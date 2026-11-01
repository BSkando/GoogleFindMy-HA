# custom_components/googlefindmy/Auth/firebase_messaging/fcmpushclient.py
#
# firebase-messaging
# https://github.com/sdb9696/firebase-messaging
#
# MIT License
#
# Copyright (c) 2017 Matthieu Lemoine
# Copyright (c) 2023 Steven Beth
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import annotations

import asyncio
import binascii
import json
import logging
import random
import ssl
import struct
import time
from base64 import b64decode
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, cast

from aiohttp import ClientSession
from cryptography.hazmat.primitives.serialization import load_der_private_key

import http_ece
from google.protobuf.message import Message as RuntimeMessage

from ..log_safety import describe_exception, exception_origin
from ._typing import (
    CredentialsUpdatedCallable,
    JSONDict,
    MutableJSONMapping,
    OnNotificationCallable,
)
from .const import (
    MCS_CONNECT_TIMEOUT_S,
    MCS_HAPPY_EYEBALLS_DELAY_S,
    MCS_HOST,
    MCS_PORT,
    MCS_SELECTIVE_ACK_ID,
    MCS_VERSION,
)
from .fcmregister import FcmRegister, FcmRegisterConfig
from .proto.mcs_pb2 import (  # pylint: disable=no-name-in-module
    Close,
    DataMessageStanza,
    HeartbeatAck,
    HeartbeatPing,
    IqStanza,
    LoginRequest,
    LoginResponse,
    SelectiveAck,
    StreamErrorStanza,
)

if TYPE_CHECKING:
    from custom_components.googlefindmy.protobuf_typing import MessageProto
else:
    MessageProto = RuntimeMessage

http_decrypt: Callable[..., bytes] = http_ece.decrypt

_logger = logging.getLogger(__name__)

_LEGACY_MCS_PROTOCOL_VERSION = 38

# A run of consecutive ECE (auth-tag / body) decrypt failures with no
# successful decrypt in between means the stored DH / auth-secret key material
# no longer matches the server-side registration: every push fails the auth tag
# even though the credential fields are still syntactically valid base64/DER. A
# SINGLE failure stays per-message poison (a corrupt individual body, ACK +
# skip); only a sustained run escalates to ``CredentialDecryptionError`` so the
# supervisor invalidates the stale tokens and re-registers, instead of ACKing
# every undecryptable push forever and silently stalling location updates (Codex
# follow-up on PR #181: "do not ACK every ECE decrypt failure"). This is a
# DEDICATED counter rather than the connectivity-oriented
# ``sequential_error_counters`` mechanism, whose escalation action is "stop the
# listener", not "re-register the credentials".
_MAX_CONSECUTIVE_DECRYPT_FAILURES = 5


class CredentialDecryptionError(ValueError):
    """Raised when stored FCM credential material cannot be decoded or parsed.

    Subclass of ``ValueError`` for backward-compatibility with external
    callers that catch broad ``ValueError``. Internal code (notably the
    listener loop's poison-message defense) discriminates this type via
    ``isinstance`` to distinguish per-message decode failures (which are
    safely ACKed and skipped) from credential-corruption failures (which
    must surface to the supervisor for re-registration).

    Why a typed exception instead of a string-prefix marker: external
    libraries such as ``cryptography`` and ``http_ece`` raise plain
    ``ValueError`` without our markers; a ``str.startswith`` catch loses
    them and silently treats credential corruption as per-message poison.
    See CA-CRED-VS-MSG-DECRYPT-001 iter-3 lesson.
    """


def _safe_urlsafe_b64decode(value: str | bytes) -> bytes:
    """Decode URL-safe base64 leniently (Python 3.14 compatibility shim).

    Strips ASCII whitespace and re-appends ``=`` padding before delegating
    to :func:`base64.b64decode` (``altchars=b"-_"`` for the URL-safe
    alphabet, ``validate=True``; see below). Python 3.14 switched
    ``binascii.a2b_base64`` to ``strict_mode=True`` by default, which
    rejects payloads containing newlines, stray whitespace, or missing
    padding -- exactly the conditions the wild FCM stream produces. This
    helper restores the pre-3.14 lenient behaviour locally without altering
    the global decoder state.

    Defense layer 3 of the three-defense hotfix for the
    ``fcmpushclient`` poison-message cascade (CA-CASCADING-FAILURE-001).
    Defense 1 wraps credential-decode errors as
    :class:`CredentialDecryptionError`; defense 2 caps the supervisor
    restart loop. This helper closes the root cause: dirty base64 input
    that the Python 3.14 strict decoder rejects unconditionally.

    Decoding runs in *validating* mode so that input containing characters
    outside the URL-safe base64 alphabet (after whitespace/padding
    normalisation) raises ``binascii.Error`` instead of being silently
    discarded. This is a correctness requirement, not just hygiene: a
    credential field corrupted to pure non-alphabet punctuation (e.g.
    ``b"!!!!"``) must reach the ``CredentialDecryptionError`` branch in
    :meth:`_decrypt_raw_data` and trigger re-registration, rather than
    decoding to garbage bytes and being mistaken for recoverable
    per-message poison.

    Raises ``binascii.Error`` when the payload remains undecodable after
    whitespace removal and padding normalisation -- including any residual
    non-alphabet character rejected by the validating decoder. Raises
    ``UnicodeError`` when ``str`` input contains non-ASCII characters
    (``str.encode("ascii")`` fails before normalisation runs); callers in
    a credential-decode path must catch both.
    """
    if isinstance(value, str):
        raw = value.encode("ascii")
    else:
        raw = bytes(value)
    # str.split() / bytes.split() with no args collapses every flavour of
    # ASCII whitespace (space, tab, newline, CR, vtab, formfeed) -- this
    # is exactly the set Python 3.14 strict mode now rejects.
    stripped = b"".join(raw.split())
    padded = stripped + b"=" * (-len(stripped) % 4)
    # Decode in *validating* mode (``validate=True``). The stdlib's
    # ``urlsafe_b64decode`` forwards to the non-validating ``b64decode``,
    # which silently DISCARDS characters outside the base64 alphabet instead
    # of raising. Corrupt credential fields that degrade to pure punctuation
    # (e.g. ``b"!!!!"``) would therefore decode to garbage bytes and slip
    # past the ``CredentialDecryptionError`` branch in ``_decrypt_raw_data``,
    # causing the listener to treat permanent credential corruption as
    # recoverable per-message poison (ACK + skip) and never trigger
    # re-registration. ``altchars=b"-_"`` keeps the URL-safe alphabet;
    # ``validate=True`` makes any non-alphabet residue raise ``binascii.Error``.
    return b64decode(padded, altchars=b"-_", validate=True)


# MCS Message Types and Tags
MCS_MESSAGE_TAG = {
    HeartbeatPing: 0,
    HeartbeatAck: 1,
    LoginRequest: 2,
    LoginResponse: 3,
    Close: 4,
    "MessageStanza": 5,
    "PresenceStanza": 6,
    IqStanza: 7,
    DataMessageStanza: 8,
    "BatchPresenceStanza": 9,
    StreamErrorStanza: 10,
    "HttpRequest": 11,
    "HttpResponse": 12,
    "BindAccountRequest": 13,
    "BindAccountResponse": 14,
    "TalkMetadata": 15,
}


class ErrorType(Enum):
    CONNECTION = 1
    READ = 2
    LOGIN = 3
    NOTIFY = 4


class FcmPushClientRunState(Enum):
    CREATED = (1,)
    STARTING_TASKS = (2,)
    STARTING_CONNECTION = (3,)
    STARTING_LOGIN = (4,)
    STARTED = (5,)
    STOPPING = (6,)
    STOPPED = (7,)


@dataclass
class FcmPushClientConfig:  # pylint:disable=too-many-instance-attributes
    """Configuration for FcmPushClient (worker-only; supervisor handles restarts)."""

    server_heartbeat_interval: int | None = 10
    """Time in seconds to request the server to send heartbeats"""

    client_heartbeat_interval: int | None = 20
    """Time in seconds to send heartbeats to the server"""

    # Kept for compatibility; used only on initial connect
    connection_retry_count: int = 5
    """Number of times to retry the initial connection before giving up."""

    # Not used by the worker anymore but kept for signature compatibility
    idle_reset_after: float = 90.0
    reset_interval: float = 3
    heartbeat_ack_timeout: float = 5
    monitor_interval: float = 1

    abort_on_sequential_error_count: int | None = 3
    """Number of sequential errors of the same type to wait before aborting.
       If set to None the client will not abort."""

    log_warn_limit: int | None = 5
    """Number of times to log specific warning messages before going silent for
       a specific warning type."""

    log_debug_verbose: bool = False
    """Set to True to log all message info including tokens."""

    # NEW: bounded writer shutdown (seconds) to avoid hanging on TLS close
    writer_close_timeout: float = 2.0


class FcmPushClient[NotificationContextT]:  # pylint:disable=too-many-instance-attributes
    """Worker-only FCM client.
    - Establishes a single connection with initial retry.
    - Listens for messages until an error occurs or stop() is called.
    - No internal reset/monitor loops; outer supervisor restarts the client.
    """

    def __init__(  # noqa: PLR0913
        self,
        callback: OnNotificationCallable[JSONDict, NotificationContextT],
        fcm_config: FcmRegisterConfig,
        credentials: MutableJSONMapping | None = None,
        credentials_updated_callback: (
            CredentialsUpdatedCallable[MutableJSONMapping] | None
        ) = None,
        *,
        callback_context: NotificationContextT | None = None,
        received_persistent_ids: list[str] | None = None,
        config: FcmPushClientConfig | None = None,
        http_client_session: ClientSession | None = None,
    ):
        """Initializes the receiver."""
        self.callback: OnNotificationCallable[JSONDict, NotificationContextT] = callback
        self.callback_context: NotificationContextT | None = callback_context
        self.fcm_config = fcm_config
        self.credentials: MutableJSONMapping | None = credentials
        self.credentials_updated_callback: (
            CredentialsUpdatedCallable[MutableJSONMapping] | None
        ) = credentials_updated_callback
        self.persistent_ids: list[str] = (
            list(received_persistent_ids) if received_persistent_ids is not None else []
        )
        self.config = config if config else FcmPushClientConfig()
        self._http_client_session: ClientSession | None = http_client_session
        self.register: FcmRegister | None = None

        # Instance-specific logger to avoid global side effects; honors log_debug_verbose.
        self.logger = logging.getLogger(f"{__name__}.FcmPushClient.{id(self)}")
        self.logger.propagate = True
        if self.config.log_debug_verbose:
            self.logger.setLevel(logging.DEBUG)

        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.do_listen = False
        self.sequential_error_counters: dict[ErrorType, int] = {}
        self.log_warn_counters: dict[str, int] = {}

        # State variables
        self.input_stream_id = 0
        self.last_input_stream_id_reported = -1
        self.first_message = True
        self.last_login_time: float = 0.0
        self.last_message_time: float = 0.0

        self.run_state: FcmPushClientRunState = FcmPushClientRunState.CREATED
        # Monotonic timestamp of this client's STARTED transition (set in
        # ``_handle_message`` on a successful login, alongside the
        # ``persistent_ids`` and ``_first_data_message_delivered`` resets).
        # Consumers measure "session age since STARTED" from this instead of the
        # supervisor's pre-start timestamp, which is recorded before ``start()``
        # and can trail the actual STARTED transition by the connection budget
        # (up to 78.6 s when every attempt fails; ``MCS_CONNECT_TIMEOUT_S`` in
        # ``const.py``). ``None`` until the client first reaches STARTED.
        self._started_monotonic: float | None = None
        # First-locate reconnect "has delivered" proof, kept *separate* from
        # ``persistent_ids``. ``persistent_ids`` must record every selective-acked
        # message for the RMQ2 login dedup (``received_persistent_id``); this flag
        # must count only genuine deliveries (decrypted + dispatched). The two
        # contracts conflict on one field, so they are split: this flag is set
        # only when ``_handle_data_message`` reports a real delivery and reset to
        # ``False`` on each successful login, alongside ``persistent_ids = []``.
        # Consumed cross-module by ``fcm_receiver_ha._needs_first_locate_reconnect``.
        self._first_data_message_delivered: bool = False
        self.tasks: list[asyncio.Task[None]] = []
        # Set by ``_listen`` when stored FCM key material is found to be
        # corrupt/undecodable. The supervisor reads this after the listener
        # stops to invalidate the bad credentials and force re-registration,
        # instead of restarting against the same poison key material until the
        # short-run crash cap fires.
        self.credential_error: CredentialDecryptionError | None = None
        # Counts consecutive ECE (auth-tag/body) decrypt failures with no
        # successful decrypt in between; reset to 0 on every successful decrypt
        # (``_handle_data_message``). When it reaches
        # ``_MAX_CONSECUTIVE_DECRYPT_FAILURES`` the poison-message arm escalates
        # a stale-key run to ``CredentialDecryptionError`` so the supervisor
        # re-registers, instead of ACKing every undecryptable push indefinitely.
        self._consecutive_decrypt_failures: int = 0

        # Locks
        self.stopping_lock: asyncio.Lock = asyncio.Lock()

    # ---- Logging helpers ----

    def _msg_str(self, msg: MessageProto) -> str:
        # Type and field names only, even in verbose mode: MCS messages carry
        # the push payload, persistent ids and the login token, none of which
        # may reach the log (AGENTS.md section 5). The structure is what a
        # protocol trace needs; the values are the payload.
        if self.config.log_debug_verbose:
            fields = [
                field.name for field, _value in cast(RuntimeMessage, msg).ListFields()
            ]
            return f"{type(msg).__name__} fields={fields}"
        return type(msg).__name__

    def _log_verbose(self, msg: str, *args: object) -> None:
        if self.config.log_debug_verbose:
            self.logger.debug(msg, *args)

    def _log_warn_with_limit(self, msg: str, *args: object) -> None:
        if msg not in self.log_warn_counters:
            self.log_warn_counters[msg] = 0
        if (
            self.config.log_warn_limit
            and self.config.log_warn_limit > self.log_warn_counters[msg]
        ):
            self.log_warn_counters[msg] += 1
            self.logger.warning(msg, *args)

    # ---- Connection helpers ----

    async def _do_writer_close(self) -> None:
        """Close the stream writer with a bounded wait; abort transport on timeout."""
        writer = self.writer
        self.writer = None
        if not writer:
            return
        try:
            try:
                writer.close()
            except Exception:
                # defensive: ignore close() errors
                pass
            try:
                # Bounded, graceful shutdown window
                await asyncio.wait_for(
                    writer.wait_closed(), timeout=self.config.writer_close_timeout
                )
            except TimeoutError:
                # TLS shutdown timed out: hard-close
                self.logger.debug("SSL shutdown timed out; aborting transport")
                transport = writer.transport if hasattr(writer, "transport") else None
                if transport is not None:
                    try:
                        transport.abort()  # Immediate abort preferred
                    except Exception:
                        try:
                            transport.close()
                        except Exception:
                            pass
        except asyncio.CancelledError:
            # On cancel, still hard-close before propagating
            transport = writer.transport if hasattr(writer, "transport") else None
            if transport is not None:
                try:
                    transport.abort()
                except Exception:
                    try:
                        transport.close()
                    except Exception:
                        pass
            raise
        except Exception:
            # Keep old behavior: swallow all other errors on close.
            pass

    # protobuf varint32 helpers
    async def _read_varint32(self) -> int:
        reader = self.reader
        if reader is None:
            # Same class as the ``_send_msg`` writer guard: a read on a
            # torn-down stream is a normal connection life-cycle event, not an
            # invariant violation. Surface it as a ConnectionError the listener
            # already handles gracefully rather than an "Unknown error" trace.
            raise ConnectionError("StreamReader is not initialized")
        res = 0
        shift = 0
        while True:
            r = await reader.readexactly(1)
            byte_value = int(struct.unpack("B", r)[0])
            res |= (byte_value & 0x7F) << shift
            if (byte_value & 0x80) == 0:
                break
            shift += 7
        return res

    @staticmethod
    def _encode_varint32(x: int) -> bytes:
        if x == 0:
            return bytes(bytearray([0]))

        res = bytearray([])
        while x != 0:
            b = x & 0x7F
            x >>= 7
            if x != 0:
                b |= 0x80
            res.append(b)
        return bytes(res)

    @staticmethod
    def _make_packet(msg: MessageProto, include_version: bool) -> bytes:
        tag = MCS_MESSAGE_TAG[type(msg)]
        header = bytearray([MCS_VERSION, tag]) if include_version else bytearray([tag])
        payload = msg.SerializeToString()
        buf = bytes(header) + FcmPushClient._encode_varint32(len(payload)) + payload
        return buf

    async def _send_msg(self, msg: MessageProto) -> None:
        self._log_verbose("Sending packet to server: %s", self._msg_str(msg))
        buf = FcmPushClient._make_packet(msg, self.first_message)
        writer = self.writer
        if writer is None:
            # A send on a torn-down stream is a normal connection life-cycle
            # event, not a programming-invariant violation: e.g. an in-flight
            # heartbeat ack racing a concurrent stop()/teardown that already
            # nulled ``self.writer`` in ``_do_writer_close``. Raise a
            # ConnectionError so the listener's connection-error handling stops
            # the worker cleanly, instead of an ``assert`` whose AssertionError
            # falls through to the generic arm and surfaces as an alarming
            # "Unknown error in listener" traceback for a benign shutdown.
            raise ConnectionError("StreamWriter is not initialized")
        writer.write(buf)
        await writer.drain()

    async def _receive_msg(self) -> MessageProto | None:
        reader = self.reader
        if reader is None:
            # Same class as the ``_send_msg`` writer guard (see there): a torn-
            # down stream is a normal life-cycle event; raise ConnectionError so
            # the listener stops cleanly instead of logging an "Unknown error".
            raise ConnectionError("StreamReader is not initialized")

        if self.first_message:
            r = await reader.readexactly(2)
            version, tag = struct.unpack("BB", r)
            if version < MCS_VERSION and version != _LEGACY_MCS_PROTOCOL_VERSION:
                raise RuntimeError(f"protocol version {version} unsupported")
            self.first_message = False
        else:
            r = await reader.readexactly(1)
            (tag,) = struct.unpack("B", r)
        size = await self._read_varint32()

        self._log_verbose("Received message with tag %s and size %s", tag, size)

        if not size >= 0:
            self._log_warn_with_limit("Unexpected message size %s", size)
            return None

        buf = await reader.readexactly(size)

        msg_class = next(iter([c for c, t in MCS_MESSAGE_TAG.items() if t == tag]))
        if not msg_class:
            self._log_warn_with_limit("Unexpected message tag %s", tag)
            return None
        if isinstance(msg_class, str):
            self._log_warn_with_limit("Unconfigured message class %s", msg_class)
            return None

        msg_type = cast(type[MessageProto], msg_class)
        payload = msg_type()
        payload.ParseFromString(buf)
        self._log_verbose("Received payload: %s", self._msg_str(payload))

        return payload

    async def _login(self) -> None:
        self.run_state = FcmPushClientRunState.STARTING_LOGIN

        now = time.time()
        self.input_stream_id = 0
        self.last_input_stream_id_reported = -1
        self.first_message = True
        self.last_login_time = now

        try:
            # Defensive access to credentials to avoid KeyError crashes
            if not isinstance(self.credentials, dict):
                raise ValueError("Missing credentials dictionary for FCM login")

            gcm_data = self.credentials.get("gcm")
            if not isinstance(gcm_data, dict):
                raise ValueError("'gcm' section is missing or invalid in credentials")

            android_id = gcm_data.get("android_id")
            security_token = gcm_data.get("security_token")
            if not android_id or not security_token:
                raise ValueError(
                    "android_id or security_token is missing from credentials"
                )

            req = LoginRequest()
            req.adaptive_heartbeat = False
            req.auth_service = LoginRequest.ANDROID_ID  # 2
            req.auth_token = security_token
            req.id = self.fcm_config.chrome_version
            req.domain = "mcs.android.com"
            req.device_id = f"android-{int(android_id):x}"
            req.network_type = 1
            req.resource = str(android_id)
            req.user = str(android_id)
            req.use_rmq2 = True
            req.setting.add(name="new_vc", value="1")
            req.received_persistent_id.extend(self.persistent_ids)
            if (
                self.config.server_heartbeat_interval
                and self.config.server_heartbeat_interval > 0
            ):
                req.heartbeat_stat.ip = ""
                req.heartbeat_stat.timeout = True
                req.heartbeat_stat.interval_ms = (
                    1000 * self.config.server_heartbeat_interval
                )

            await self._send_msg(req)
            self.logger.debug("Sent login request")
        except Exception as ex:
            self.logger.error(
                "Received an exception logging in: %s", describe_exception(ex)
            )
            if self._try_increment_error_count(ErrorType.LOGIN):
                # On worker, treat as fatal: stop listening and let supervisor restart
                self.do_listen = False

    @staticmethod
    def _decrypt_raw_data(
        credentials: Mapping[str, object],
        crypto_key_str: str,
        salt_str: str,
        raw_data: bytes,
    ) -> bytes:
        # Header decode (crypto-key / salt) -- per-message surface.
        # Failures here are per-message poison, not credential corruption:
        # they fall through to the listener's poison-message defense
        # (selective-ACK + skip) without going through the credential
        # try-block below.
        crypto_key = _safe_urlsafe_b64decode(crypto_key_str)
        salt = _safe_urlsafe_b64decode(salt_str)

        keys_section = credentials.get("keys")
        if not isinstance(keys_section, Mapping):
            raise CredentialDecryptionError("Credentials missing FCM key material")

        private_value = keys_section.get("private")
        secret_value = keys_section.get("secret")
        if not (isinstance(private_value, str) and isinstance(secret_value, str)):
            raise CredentialDecryptionError("Credentials contain invalid key values")

        try:
            der_data = _safe_urlsafe_b64decode(private_value)
            secret = _safe_urlsafe_b64decode(secret_value)
        except (binascii.Error, UnicodeError) as cred_decode_err:
            # binascii.Error: corrupt base64 payload.
            # UnicodeError (covers UnicodeEncodeError/UnicodeDecodeError):
            # non-ASCII bytes in cached keys.private/keys.secret; .encode("ascii")
            # raises before urlsafe_b64decode is reached. Both are permanent
            # credential-corruption faults, not per-message poison.
            raise CredentialDecryptionError(
                "Credentials key material failed base64 decode; re-registration required"
            ) from cred_decode_err
        try:
            privkey = load_der_private_key(der_data, password=None)
        except Exception as der_err:
            # cryptography raises plain ValueError (corrupt DER), TypeError
            # (wrong type), or UnsupportedAlgorithm — all permanent and all
            # requiring re-registration. Broad catch is intentional: the
            # only successful path is a valid DER private key.
            raise CredentialDecryptionError(
                "Credentials key material failed DER private-key parse; re-registration required"
            ) from der_err
        decrypted = http_decrypt(
            raw_data,
            salt=salt,
            private_key=privkey,
            dh=crypto_key,
            version="aesgcm",
            auth_secret=secret,
        )
        return decrypted

    def _app_data_by_key(
        self, p: DataMessageStanza, key: str, do_not_raise: bool = False
    ) -> str:
        for x in p.app_data:
            if x.key == key:
                return x.value

        if do_not_raise:
            return ""
        raise RuntimeError(f"couldn't find in app_data {key}")

    @staticmethod
    def _extract_header_param(header: str, param: str) -> str:
        """Extract a named parameter from a semicolon-separated header value.

        FCM headers like crypto-key and encryption use the format
        ``key=value;key2=value2``.  Blindly slicing off a fixed prefix
        breaks when extra parameters (e.g. ``p256ecdsa=...``) are present.
        """
        for part in header.split(";"):
            key, _, value = part.strip().partition("=")
            if key == param:
                return value
        raise ValueError(f"Parameter '{param}' not found in header: {header}")

    def _handle_data_message(
        self,
        msg: DataMessageStanza,
    ) -> bool:
        """Decrypt and dispatch one data message; report whether it was delivered.

        Returns ``True`` only when a real payload was decrypted and handed to the
        notification callback. Returns ``False`` for the non-delivering paths that
        the caller must still selective-ack but must NOT count as a delivery: a
        ``deleted_messages`` control stanza, a missing-credentials guard hit, and a
        foreign-``subtype`` drop (a push encrypted for a superseded GCM
        registration). The caller (``_handle_message``) uses this return to gate
        the *delivery proof only* (``_first_data_message_delivered``, read by
        ``fcm_receiver_ha._needs_first_locate_reconnect``); it still records
        ``persistent_ids`` for EVERY acked message, because that list is the RMQ2
        login dedup (``received_persistent_id``) and must include even dropped
        pushes to stop the server replaying them. The two consumers therefore use
        two separate signals -- their contracts conflict on a single field (all
        acked vs. only delivered), so a shared ``persistent_ids`` cannot satisfy
        both. A decrypt failure raises (caught upstream in
        ``_process_one_inbound_message``): it never counts as delivered, and the
        raise unwinds before the caller's own ``persistent_ids`` append. That
        handler still records the acked ``persistent_id`` for RMQ2 dedup when it
        selective-acks the poison push, so the append happens at the ack site
        instead of here (see ``_process_one_inbound_message``).
        """
        self.logger.debug(
            "Received data message Stream ID: %s, Last: %s, Status: %s",
            msg.stream_id,
            msg.last_stream_id_received,
            msg.status,
        )

        if (
            self._app_data_by_key(msg, "message_type", do_not_raise=True)
            == "deleted_messages"
        ):
            # The deleted_messages message does not contain data.
            return False
        crypto_key = self._extract_header_param(
            self._app_data_by_key(msg, "crypto-key"), "dh"
        )
        salt = self._extract_header_param(
            self._app_data_by_key(msg, "encryption"), "salt"
        )
        subtype = self._app_data_by_key(msg, "subtype")
        # Guard before dereferencing: ``credentials`` is ``MutableJSONMapping |
        # None`` and a missing/empty value must short-circuit *before* the
        # ``["gcm"]["app_id"]`` lookups below, mirroring the defensive check in
        # ``_decrypt_raw_data`` (see the ``isinstance(self.credentials, dict)``
        # guard). Previously this guard sat *after* the dereference and was thus
        # unreachable dead code for the ``None``/``{}`` states it was meant to
        # protect against.
        if not self.credentials:
            return False
        # Resolve our registered app_id defensively. ``credentials`` is truthy
        # here, but the ``gcm`` sub-dict or its ``app_id`` can be transiently
        # absent during a credential swap/invalidate window; a hard
        # ``["gcm"]["app_id"]`` would raise ``KeyError`` in that window. The
        # sibling read in ``_decrypt_raw_data`` already uses ``.get("gcm")`` for
        # the same reason. A ``None`` registered id simply means we cannot own
        # this push, so the mismatch check below drops it safely.
        registered_app_id = (self.credentials.get("gcm") or {}).get("app_id")
        if subtype != registered_app_id:
            # Drop, do not fall through to decrypt. A subtype mismatch means this
            # push was encrypted for a *different* (superseded) GCM registration
            # -- e.g. an old client instance still listening after an FCM
            # re-registration minted a fresh ``wp:...#<uuid>`` app_id. Its key
            # material can never match, so ``_decrypt_raw_data`` below would fail
            # with a guaranteed ``ECEException``/InvalidTag. That failure is
            # caught in ``_process_one_inbound_message`` and counts toward
            # ``_consecutive_decrypt_failures``, which at the threshold escalates
            # to a *bogus* re-registration -- minting yet another app_id and
            # provoking the next stale client. Returning here breaks that
            # amplification loop: the foreign push is simply not for us. The
            # caller (``_handle_message``) still selective-acks the message on
            # the normal path, so the server stops redelivering it.
            #
            # Log at *debug*, not warning. Dropping a foreign-subtype push is an
            # expected, benign event (Google replays orphaned/superseded
            # subscriptions under our own android_id, e.g. on reconnect); it is
            # handled correctly above and requires no user action. Surfacing it
            # at warning level only alarmed users about a non-issue. It stays
            # visible under debug logging for diagnosis.
            self.logger.debug(
                "Subtype %s in data message does not match "
                "app id client was registered with %s",
                subtype,
                registered_app_id,
            )
            return False

        decrypted = self._decrypt_raw_data(
            self.credentials, crypto_key, salt, msg.raw_data
        )
        # A successful decrypt proves the stored DH/auth-secret key material
        # still matches the server registration: clear the stale-key escalation
        # counter so a later ISOLATED corrupt body cannot accumulate toward an
        # unwarranted re-registration. Only an uninterrupted run of decrypt
        # failures (no success in between) signals stale credentials -- placing
        # the reset here, gated on a real decrypt success, means heartbeats /
        # deleted_messages / login traffic (which never reach this line) cannot
        # falsely reset the counter.
        self._consecutive_decrypt_failures = 0

        # Normalize decrypted payload to a dict (defensive against non-object JSON)
        decrypted_json: Any | None = None
        text: str | None = None
        try:
            text = decrypted.decode("utf-8")
        except Exception:
            text = None

        if text is not None:
            try:
                decrypted_json = json.loads(text)
            except (json.JSONDecodeError, ValueError):
                decrypted_json = None

        if isinstance(decrypted_json, dict):
            ret_val = cast(JSONDict, decrypted_json)
        elif decrypted_json is not None:
            self._log_warn_with_limit(
                "FCM payload JSON is not an object (%s); wrapping",
                type(decrypted_json).__name__,
            )
            ret_val = {"_raw_json": decrypted_json}
        else:
            # Not JSON → provide a stable mapping for consumers
            ret_val = (
                {"_raw_text": text}
                if text is not None
                else {"_raw_bytes": decrypted.hex()}
            )

        # Key count and size only: the decrypted push is the raw API payload,
        # its keys are wire content too (a JSON object may carry a value in a
        # key), and the persistent id is a server-assigned MCS identifier, so
        # none of them reaches the log, verbose or not (AGENTS.md section 5).
        self._log_verbose(
            "Decrypted data message: keys=%d, bytes=%d",
            len(ret_val),
            len(decrypted),
        )
        try:
            self.callback(ret_val, msg.persistent_id, self.callback_context)
            self._reset_error_count(ErrorType.NOTIFY)
        except Exception as callback_err:
            self.logger.error(
                "Unexpected exception calling notification callback: %s at %s",
                describe_exception(callback_err),
                exception_origin(callback_err),
            )
            self._try_increment_error_count(ErrorType.NOTIFY)

        # Reached only after a real decrypt + callback dispatch: a genuine
        # delivery, so the caller records ``persistent_id`` as delivered.
        return True

    def _new_input_stream_id_available(self) -> bool:
        return self.last_input_stream_id_reported != self.input_stream_id

    def _get_input_stream_id(self) -> int:
        self.last_input_stream_id_reported = self.input_stream_id
        return self.input_stream_id

    async def _handle_ping(self, p: HeartbeatPing) -> None:
        self.logger.debug(
            "Received heartbeat ping, sending ack: Stream ID: %s, Last: %s, Status: %s",
            p.stream_id,
            p.last_stream_id_received,
            p.status,
        )
        req = HeartbeatAck()

        if self._new_input_stream_id_available():
            req.last_stream_id_received = self._get_input_stream_id()

        await self._send_msg(req)

    async def _handle_iq(self, p: IqStanza) -> None:
        # `HasField`, not truthiness: an unset sub-message is still a truthy
        # default instance, so `not p.extension` never held and a stanza
        # without extension fell through to the "extension id 0" warning.
        if not p.HasField("extension"):
            # `_msg_str` names the type and fields; the whole stanza in text
            # format is a payload dump (AGENTS.md section 5). The old call
            # also had no placeholder for its argument.
            self._log_warn_with_limit(
                "Unexpected IqStanza id received with no extension: %s",
                self._msg_str(p),
            )
            return
        if p.extension.id not in (12, 13):
            self._log_warn_with_limit(
                "Unexpected extension id received: %s", p.extension.id
            )
            return

    async def _send_selective_ack(self, persistent_id: str) -> None:
        iqs = IqStanza()
        iqs.type = IqStanza.IqType.SET
        iqs.id = ""
        iqs.extension.id = MCS_SELECTIVE_ACK_ID
        sa = SelectiveAck()
        sa.id.extend([persistent_id])
        iqs.extension.data = sa.SerializeToString()
        # The persistent id is a server-assigned MCS identifier (R-1).
        self.logger.debug("Sending selective ack (id_len=%d)", len(persistent_id))
        await self._send_msg(iqs)

    async def _send_heartbeat(self) -> None:
        req = HeartbeatPing()

        if self._new_input_stream_id_available():
            req.last_stream_id_received = self._get_input_stream_id()

        await self._send_msg(req)
        self.logger.debug("Sent heartbeat ping")

    def _reset_error_count(self, error_type: ErrorType) -> None:
        self.sequential_error_counters[error_type] = 0

    def _try_increment_error_count(self, error_type: ErrorType) -> bool:
        if error_type not in self.sequential_error_counters:
            self.sequential_error_counters[error_type] = 0

        # NOTE: returns True to keep going, False when we've decided to stop
        self.sequential_error_counters[error_type] += 1

        if (
            self.config.abort_on_sequential_error_count
            and self.sequential_error_counters[error_type]
            >= self.config.abort_on_sequential_error_count
        ):
            self.logger.debug(
                "Stopping push receiver due to %d sequential errors of type %s (FCM connectivity issue)",
                self.sequential_error_counters[error_type],
                error_type,
            )
            self.do_listen = False
            return False
        return True

    async def _handle_message(self, msg: MessageProto) -> None:
        self.last_message_time = time.time()
        self.input_stream_id += 1

        if isinstance(msg, Close):
            # A server-initiated Close is a normal part of the MCS connection
            # life-cycle (the endpoint periodically rotates long-lived streams).
            # Log it at INFO, consistent with the other normal worker-stop paths
            # below ("FCM stream ended" / "FCM read ended"); the supervisor will
            # reconnect automatically. Emitting WARNING here produced a recurring
            # false alarm in the Home Assistant log panel.
            self.logger.info(
                "FCM server sent Close; worker stopping (normal, supervisor will reconnect)"
            )
            self.do_listen = False
            return

        if isinstance(msg, LoginResponse):
            if str(msg.error):
                # Structured code and type only: `message` is server-supplied
                # free text and stays out of the record (Auth AGENTS.md,
                # "Logging": raw error text and response bodies).
                self.logger.error(
                    "Received login error response: code=%s type=%s",
                    msg.error.code,
                    msg.error.type,
                )
                if self._try_increment_error_count(ErrorType.LOGIN):
                    self.do_listen = False
            else:
                self.logger.info("Successfully logged in to MCS endpoint")
                self._reset_error_count(ErrorType.LOGIN)
                # Stamp the STARTED-transition time *before* publishing the
                # STARTED state so any observer that sees STARTED also sees the
                # timestamp (single authoritative transition point, race-free).
                self._started_monotonic = time.monotonic()
                self.run_state = FcmPushClientRunState.STARTED
                self.persistent_ids = []
                # Reset the delivery proof for the new login session, mirroring
                # the ``persistent_ids`` reset above (see ``__init__``): the fresh
                # session has not delivered a data message yet.
                self._first_data_message_delivered = False
                # Refresh activity timestamp
                self.last_message_time = time.time()
            return

        if isinstance(msg, DataMessageStanza):
            delivered = self._handle_data_message(msg)
            # Record EVERY selective-acked message in ``persistent_ids``: it is the
            # sole source ``_login`` extends into ``received_persistent_id`` (RMQ2
            # dedup), so after a stream restart the server is told about all IDs we
            # already received and does not redeliver them. A dropped
            # foreign-subtype/control push is still received and acked, so it must
            # be recorded here too -- omitting it lets the server replay that same
            # orphan push across reconnects, delivering+acking it repeatedly.
            self.persistent_ids.append(msg.persistent_id)
            if delivered:
                # Delivery proof for the first-locate reconnect is a *separate*
                # signal (see ``__init__``): only a genuine decrypt+dispatch counts.
                # A dropped push must NOT set it, or a superseded push arriving
                # before the first real locate would falsely suppress the needed
                # reconnect. This is why the proof cannot share ``persistent_ids``
                # (whose RMQ2 role requires recording that very dropped push).
                self._first_data_message_delivered = True
            # Ack unconditionally (cheap): even a dropped/foreign push must be
            # selective-acked so the server stops redelivering it.
            await self._send_selective_ack(msg.persistent_id)
        elif isinstance(msg, HeartbeatPing):
            await self._handle_ping(msg)
        elif isinstance(msg, HeartbeatAck):
            # Type and field names, not the text format: the ack carries only
            # stream counters, but a message passed whole to %s is a wire dump
            # (AGENTS.md section 5), and every other MCS record uses _msg_str.
            self.logger.debug("Received heartbeat ack: %s", self._msg_str(msg))
        elif isinstance(msg, IqStanza):
            pass
        else:
            self._log_warn_with_limit("Unexpected message type %s.", type(msg).__name__)
        # Reset error count if a read has been successful
        self._reset_error_count(ErrorType.READ)
        self._reset_error_count(ErrorType.CONNECTION)

    @staticmethod
    async def _open_connection(
        host: str, port: int, ssl_context: ssl.SSLContext
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        async with asyncio.timeout(MCS_CONNECT_TIMEOUT_S):
            return await asyncio.open_connection(
                host=host,
                port=port,
                ssl=ssl_context,
                happy_eyeballs_delay=MCS_HAPPY_EYEBALLS_DELAY_S,
            )

    async def _connect(self) -> bool:
        try:
            loop = asyncio.get_running_loop()
            # create_default_context() blocks the event loop
            ssl_context = await loop.run_in_executor(None, ssl.create_default_context)
            self.logger.debug(
                "Connecting to MCS endpoint (%s,%s), limit %.1f s per attempt",
                MCS_HOST,
                MCS_PORT,
                MCS_CONNECT_TIMEOUT_S,
            )
            self.reader, self.writer = await self._open_connection(
                host=MCS_HOST, port=MCS_PORT, ssl_context=ssl_context
            )
            self.logger.debug(
                "Connected to MCS endpoint (%s,%s) over %s",
                MCS_HOST,
                MCS_PORT,
                self.writer.get_extra_info("socket").family.name,
            )
            return True
        except OSError as oex:
            # Log at DEBUG level since _connect_with_retry() handles retries
            # and will log ERROR if all attempts fail
            self.logger.debug(
                "Could not connect to MCS endpoint (%s,%s): %s",
                MCS_HOST,
                MCS_PORT,
                describe_exception(oex),
            )
            return False

    def _exp_backoff_with_jitter(self, trycount: int) -> float:
        """Exponential backoff: 0.5,1,2,4,... capped at 60s, plus 10–20% jitter."""
        base: float = min(0.5 * (2 ** (trycount - 1)), 60.0)
        jitter = base * float(random.uniform(0.10, 0.20))
        return float(base + jitter)

    async def _connect_with_retry(self) -> bool:
        self.run_state = FcmPushClientRunState.STARTING_CONNECTION

        trycount = 0
        connected = False
        while (
            trycount < self.config.connection_retry_count
            and not connected
            and self.do_listen
        ):
            trycount += 1
            connected = await self._connect()
            if not connected:
                sleep_time = self._exp_backoff_with_jitter(trycount)
                self.logger.info(
                    "Could not connect to MCS Endpoint on try %s, sleeping for %.2f seconds",
                    trycount,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)
        if not connected:
            self.logger.error(
                "Unable to connect to MCS endpoint after %s tries", trycount
            )
        return connected

    async def _ack_or_disconnect(self, persistent_id: str | None) -> bool:
        """Best-effort selective-ACK for a poison message.

        Returns ``True`` to continue the worker loop; ``False`` if the writer
        half is dead and the supervisor should reconnect (caller breaks).
        """
        if not persistent_id:
            return True
        try:
            await self._send_selective_ack(persistent_id)
            return True
        except (OSError, ssl.SSLError, ConnectionError):
            return False

    async def _process_one_inbound_message(self, msg: MessageProto) -> bool:
        """Run ``_handle_message`` with poison-message defense.

        Defense 1 (CA-CASCADING-FAILURE-001): per-message decode failures
        (``binascii.Error`` padding faults under Python 3.14 strict mode,
        ``RuntimeError`` from ``_app_data_by_key`` key lookups, generic
        ``ValueError`` from header parsing, and an ISOLATED
        ``http_ece.ECEException`` from a corrupt encrypted body / failed auth
        tag) are acknowledged and skipped instead of crashing the worker loop.
        Credential-material errors (``CredentialDecryptionError``) propagate so
        ``_listen`` records them and the supervisor invalidates the bad key
        material and prompts re-registration.

        Stale-key escalation (Codex follow-up on PR #181): a SUSTAINED run of
        ``http_ece.ECEException`` -- ``_MAX_CONSECUTIVE_DECRYPT_FAILURES`` in a
        row with no successful decrypt in between -- is no longer per-message
        poison but evidence that the stored DH/auth-secret keys are stale
        (syntactically valid yet no longer matching the server registration).
        The counter is reset on every successful decrypt
        (``_handle_data_message``); on reaching the threshold this method raises
        ``CredentialDecryptionError`` so the run escalates through the same
        re-registration channel instead of ACKing every push indefinitely.

        Returns ``True`` to continue the loop, ``False`` to break (writer
        half-dead during ack).
        """
        try:
            await self._handle_message(msg)
            return True
        except CredentialDecryptionError:
            # Credential corruption is permanent; let the supervisor see it
            # (via ``_listen``'s dedicated handler) instead of silently ACKing
            # every message that follows.
            raise
        except (ValueError, RuntimeError, http_ece.ECEException) as decrypt_err:
            # Per-message poison: the exception-hierarchy UNION of every decode
            # surface ``_handle_message`` touches.
            #   * ValueError    -- header-param parse (``_extract_header_param``)
            #                      plus ``binascii.Error`` padding faults and
            #                      ``UnicodeError``; both are ValueError
            #                      subclasses and need no separate arm.
            #   * RuntimeError  -- ``_app_data_by_key`` missing-key lookup.
            #   * ECEException  -- ``http_ece`` body decrypt / auth-tag failure.
            #                      It is a SIBLING of ValueError under Exception
            #                      (not a subclass), so it must be listed
            #                      explicitly or it bypasses this poison defense
            #                      entirely (Exception-Hierarchie-Audit lesson,
            #                      CA-CASCADING-FAILURE-001). An ISOLATED ECE is
            #                      ACKed below; a sustained run escalates to
            #                      re-registration (stale-key branch below).
            # ``CredentialDecryptionError`` is a ValueError subclass but is
            # matched by the more specific arm above and re-raised, so it never
            # reaches here. ACK + skip so the FCM server stops redelivering and
            # the supervisor's short-run crash cap is not tripped by a single
            # bad message.
            persistent_id = getattr(msg, "persistent_id", None)
            if isinstance(decrypt_err, http_ece.ECEException):
                # Auth-tag / body-decrypt failure. A single one is a corrupt
                # individual body (poison, ACK + skip). A sustained run with no
                # successful decrypt in between means the stored DH/auth-secret
                # keys no longer match the server registration -- every push
                # fails the auth tag. Escalate to credential corruption so the
                # supervisor invalidates the stale tokens and re-registers
                # (device identity preserved), rather than ACKing every push
                # forever and silently stalling location updates (Codex
                # follow-up on PR #181). ``binascii.Error`` (per-message header
                # decode) and ``RuntimeError`` (``_app_data_by_key`` lookup) are
                # genuinely per-message and never drive this counter.
                self._consecutive_decrypt_failures += 1
                if (
                    self._consecutive_decrypt_failures
                    >= _MAX_CONSECUTIVE_DECRYPT_FAILURES
                ):
                    self._consecutive_decrypt_failures = 0
                    raise CredentialDecryptionError(
                        "FCM auth-tag decrypt failed on "
                        f"{_MAX_CONSECUTIVE_DECRYPT_FAILURES} consecutive "
                        "messages; stored key material is stale and requires "
                        "re-registration"
                    ) from decrypt_err
            # The persistent id is a server-assigned MCS identifier (R-1):
            # length only, as in ``_send_selective_ack``.
            self._log_warn_with_limit(
                "Skipping FCM message that failed to decrypt (id_len=%d): %s",
                len(persistent_id or ""),
                describe_exception(decrypt_err),
            )
            acked = await self._ack_or_disconnect(persistent_id)
            if acked and persistent_id:
                # RMQ2-dedup parity with the delivered/dropped path in
                # ``_handle_message``: a poison push we selective-ack must also be
                # recorded in ``persistent_ids`` so the next ``_login`` reports it
                # in ``received_persistent_id``. The decrypt-failure raise unwinds
                # ``_handle_message`` *before* its own ``persistent_ids.append``,
                # so without recording here the server keeps replaying the same
                # undecryptable push across reconnects, re-incrementing
                # ``_consecutive_decrypt_failures`` toward a bogus re-registration.
                # Record only on a real ack: a dead-writer ``False`` means the push
                # was NOT acked (the supervisor reconnects and the server replays,
                # so a later attempt records it instead). The escalation branch
                # above raises before reaching this point, so a message that
                # escalates to re-registration is correctly never recorded.
                self.persistent_ids.append(persistent_id)
            return acked

    async def _listen(self) -> None:
        """Listens for push notifications until an error or stop() occurs."""
        try:
            if not await self._connect_with_retry():
                self.run_state = FcmPushClientRunState.STOPPED
                self.do_listen = False
                return

            try:
                await self._login()
            except Exception as ex:
                self.logger.info("Login failed: %s", describe_exception(ex))
                self.do_listen = False
                return

            while self.do_listen:
                try:
                    if msg := await self._receive_msg():
                        if not await self._process_one_inbound_message(msg):
                            self.do_listen = False
                            break

                except (ConnectionError, ssl.SSLError) as cex:
                    # Treat stream end/TLS quirks as normal stop; supervisor will restart
                    self.logger.info(
                        "FCM stream ended (%s); worker stopping.",
                        describe_exception(cex),
                    )
                    self.do_listen = False
                    break

                except (OSError, EOFError, asyncio.IncompleteReadError) as osex:
                    # Normal network life-cycle: log and stop; supervisor will restart
                    self.logger.info(
                        "FCM read ended (%s); worker stopping.",
                        describe_exception(osex),
                    )
                    self.do_listen = False
                    break

        except asyncio.CancelledError:
            self.logger.debug("Listener task cancelled")
            raise
        except CredentialDecryptionError as cred_err:
            # Stored FCM key material is corrupt/undecodable. Record it as a
            # DISTINCT credential fault instead of letting it fall through to
            # the generic "Unknown error" arm below, so the supervisor can
            # invalidate the bad key material and re-register rather than
            # restarting the listener against the same poison credentials
            # until the short-run crash cap fires.
            self.logger.error(
                "FCM registration data is corrupt and requires re-registration: %s",
                cred_err,
            )
            self.credential_error = cred_err
            self.do_listen = False
        except ConnectionResetError:
            # Log a clean warning without noisy traceback; supervisor will reconnect.
            self.logger.warning(
                "FCM connection was reset by the server (this is normal). The connection will be re-established automatically."
            )
            self.do_listen = False
        except Exception as ex:
            # Type, kind or length plus the innermost frame (Auth contract):
            # a producer's text may echo a token or a body, and so would
            # ``traceback.format_exc()`` or ``exc_info``.
            self.logger.error(
                "Unknown error in listener: %s at %s",
                describe_exception(ex),
                exception_origin(ex),
            )
        finally:
            self.run_state = FcmPushClientRunState.STOPPING
            await self._do_writer_close()
            self.run_state = FcmPushClientRunState.STOPPED

    async def checkin_or_register(self) -> str:
        """Check in if you have credentials otherwise register as a new client.

        :return: The FCM token which is used to identify you with the push end
            point application.
        """
        # Capture in a local variable so concurrent calls on the same instance
        # cannot overwrite ``self.register`` and leak the previous session.
        register = FcmRegister(
            self.fcm_config,
            self.credentials,
            self.credentials_updated_callback,
            http_client_session=self._http_client_session,
        )
        self.register = register

        try:
            credentials = await register.checkin_or_register()
            self.credentials = credentials

            fcm_section = credentials.get("fcm")
            registration_token: str | None = None
            if isinstance(fcm_section, Mapping):
                registration_section = fcm_section.get("registration")
                if isinstance(registration_section, Mapping):
                    token_candidate = registration_section.get("token")
                    if isinstance(token_candidate, str):
                        registration_token = token_candidate

            if registration_token is None:
                raise RuntimeError("FCM registration token missing from credentials.")

            return registration_token
        finally:
            await register.close()
            if self.register is register:
                self.register = None

    async def reregister_keeping_identity(self) -> str:
        """Re-register FCM tokens while preserving GCM device identity.

        :return: The FCM token.
        """
        register = FcmRegister(
            self.fcm_config,
            self.credentials,
            self.credentials_updated_callback,
            http_client_session=self._http_client_session,
        )
        self.register = register

        try:
            credentials = await register.reregister_keeping_identity()
            self.credentials = credentials

            fcm_section = credentials.get("fcm")
            registration_token: str | None = None
            if isinstance(fcm_section, Mapping):
                registration_section = fcm_section.get("registration")
                if isinstance(registration_section, Mapping):
                    token_candidate = registration_section.get("token")
                    if isinstance(token_candidate, str):
                        registration_token = token_candidate

            if registration_token is None:
                raise RuntimeError("FCM registration token missing from credentials.")

            return registration_token
        finally:
            await register.close()
            if self.register is register:
                self.register = None

    async def _start_heartbeat_sender(self) -> None:
        """Send client heartbeats at a fixed interval while started."""
        interval = self.config.client_heartbeat_interval or 0
        if interval <= 0:
            return
        try:
            while self.do_listen:
                await asyncio.sleep(interval)
                if self.run_state == FcmPushClientRunState.STARTED and self.do_listen:
                    self.logger.debug("Sending scheduled client heartbeat")
                    try:
                        await self._send_heartbeat()
                    except Exception as ex:
                        self.logger.debug(
                            "Error while sending heartbeat: %s", describe_exception(ex)
                        )
        except asyncio.CancelledError:
            self.logger.debug("Heartbeat task cancelled")
            raise

    async def _cancel_and_gather(self) -> None:
        """Cancel all running internal tasks and await their completion."""
        if not self.tasks:
            return
        for t in self.tasks:
            if t and not t.done():
                t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []

    async def start(self) -> None:
        """Connect to FCM and start listening for push notifications."""
        self.do_listen = True
        self.run_state = FcmPushClientRunState.STARTING_TASKS
        try:
            # Initialize activity clock to "now" so potential monitors (if any) don't reset
            self.last_message_time = time.time()
            self.tasks = [
                asyncio.create_task(self._listen(), name="googlefindmy.fcm_listen"),
                asyncio.create_task(
                    self._start_heartbeat_sender(), name="googlefindmy.fcm_heartbeat"
                ),
            ]
        except Exception as ex:
            self.logger.error(
                "Unexpected error running FcmPushClient: %s", describe_exception(ex)
            )

    async def stop(self) -> None:
        """Graceful stop: close writer, cancel tasks, mark stopped"""
        if (
            self.stopping_lock
            and self.stopping_lock.locked()
            or self.run_state
            in (
                FcmPushClientRunState.STOPPING,
                FcmPushClientRunState.STOPPED,
            )
        ):
            return

        async with self.stopping_lock:
            try:
                self.run_state = FcmPushClientRunState.STOPPING
                self.do_listen = False
                await self._do_writer_close()
                await self._cancel_and_gather()
            finally:
                self.run_state = FcmPushClientRunState.STOPPED

    def is_started(self) -> bool:
        return self.run_state == FcmPushClientRunState.STARTED

    async def send_message(self, raw_data: bytes, persistent_id: str) -> None:
        """Not implemented, does nothing atm."""
        dms = DataMessageStanza()
        dms.persistent_id = persistent_id
        # Not supported yet
