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
import contextlib
import json
import logging
import ssl
import struct
import time
import traceback
import random  # added: jitter for backoff
from base64 import urlsafe_b64decode
from contextlib import suppress as contextlib_suppress
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable

from aiohttp import ClientSession
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_der_private_key
from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message
from http_ece import decrypt as http_decrypt  # type: ignore[import-untyped]

from .const import (
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

_logger = logging.getLogger(__name__)

OnNotificationCallable = Callable[[dict[str, Any], str, Any], None]
CredentialsUpdatedCallable = Callable[[dict[str, Any]], None]

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
    RESETTING = (6,)
    STOPPING = (7,)
    STOPPED = (8,)


@dataclass
class FcmPushClientConfig:  # pylint:disable=too-many-instance-attributes
    """Class to provide configuration to
    :class:`firebase_messaging.FcmPushClientConfig`.FcmPushClient."""

    server_heartbeat_interval: int | None = 10
    """Time in seconds to request the server to send heartbeats"""

    client_heartbeat_interval: int | None = 20
    """Time in seconds to send heartbeats to the server"""

    send_selective_acknowledgements: bool = True
    """True to send selective acknowledgements for each message received.
        Currently if false the client does not send any acknowledgements."""

    connection_retry_count: int = 5
    """Number of times to retry the connection before giving up."""

    start_seconds_before_retry_connect: float = 3
    """Legacy parameter; retained for compatibility (ignored by new backoff)."""

    reset_interval: float = 3
    """Time in seconds to wait between resets after errors or disconnection."""

    heartbeat_ack_timeout: float = 5
    """Time in seconds to wait for a heartbeat ack before resetting."""

    abort_on_sequential_error_count: int | None = 3
    """Number of sequential errors of the same time to wait before aborting.
        If set to None the client will not abort."""

    monitor_interval: float = 1
    """Time in seconds for the monitor task to fire and check for heartbeats,
        stale connections and shut down of the main event loop."""

    log_warn_limit: int | None = 5
    """Number of times to log specific warning messages before going silent for
        a specific warning type."""

    log_debug_verbose: bool = False
    """Set to True to log all message info including tokens."""


class FcmPushClient:  # pylint:disable=too-many-instance-attributes
    """Client that connects to Firebase Cloud Messaging and receives messages.
    :param credentials: credentials object returned by register()
    :param credentials_updated_callback: callback when new credentials are
        created to allow client to store them
    :param received_persistent_ids: any persistent id's you already received.
    :param config: configuration class of
        :class:`firebase_messaging.FcmPushClientConfig`
    """

    def __init__(
        self,
        callback: Callable[[dict, str, Any | None], None],
        fcm_config: FcmRegisterConfig,
        credentials: dict | None = None,
        credentials_updated_callback: CredentialsUpdatedCallable | None = None,
        *,
        callback_context: object | None = None,
        received_persistent_ids: list[str] | None = None,
        config: FcmPushClientConfig | None = None,
        http_client_session: ClientSession | None = None,
    ):
        """Initializes the receiver."""
        self.callback = callback
        self.callback_context = callback_context
        self.fcm_config = fcm_config
        self.credentials = credentials
        self.credentials_updated_callback = credentials_updated_callback
        self.persistent_ids = received_persistent_ids if received_persistent_ids else []
        self.config = config if config else FcmPushClientConfig()
        self._http_client_session = http_client_session

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

        # Reset / state variables
        self.input_stream_id = 0
        self.last_input_stream_id_reported = -1
        self.first_message = True
        self.last_login_time: float = 0.0
        self.last_message_time: float = 0.0

        self.run_state: FcmPushClientRunState = FcmPushClientRunState.CREATED
        self.tasks: list[asyncio.Task] = []

        # Defensive initialization of locks (avoid None races)
        self.reset_lock: asyncio.Lock = asyncio.Lock()
        self.stopping_lock: asyncio.Lock = asyncio.Lock()

        # Backoff & log-throttling state
        self._last_reset_log_ts: float = 0.0
        self._suppressed_reset_logs: int = 0
        self._reset_log_window: float = 30.0  # seconds

    # ---- Logging helpers ----

    def _msg_str(self, msg: Message) -> str:
        if self.config.log_debug_verbose:
            return type(msg).__name__ + "\n" + MessageToJson(msg, indent=4)
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

    def _log_reset_by_peer(self, exc: Exception) -> None:
        """Rate-limit 'connection reset by peer' debug noise; include suppressed count."""
        now = time.time()
        if self._last_reset_log_ts == 0.0 or (now - self._last_reset_log_ts) >= self._reset_log_window:
            # Flush any suppressed count
            if self._suppressed_reset_logs > 0:
                self.logger.debug(
                    "FCM connection reset by peer (normal): %s (suppressed %d messages)",
                    exc,
                    self._suppressed_reset_logs,
                )
                self._suppressed_reset_logs = 0
            else:
                self.logger.debug("FCM connection reset by peer (normal): %s", exc)
            self._last_reset_log_ts = now
        else:
            self._suppressed_reset_logs += 1

    # ---- Connection / reset helpers ----

    async def _do_writer_close(self) -> None:
        writer = self.writer
        self.writer = None
        if writer:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _reset(self) -> None:
        if (
            (self.reset_lock and self.reset_lock.locked())
            or (self.stopping_lock and self.stopping_lock.locked())
            or not self.do_listen
        ):
            return

        async with self.reset_lock:
            self.logger.debug("Resetting connection")

            self.run_state = FcmPushClientRunState.RESETTING

            await self._do_writer_close()

            now = time.time()
            time_since_last_login = now - (self.last_login_time or 0.0)
            if time_since_last_login < self.config.reset_interval:
                self.logger.debug("%ss since last reset attempt.", time_since_last_login)
                await asyncio.sleep(self.config.reset_interval - time_since_last_login)

            self.logger.debug("Reestablishing connection")
            if not await self._connect_with_retry():
                self.logger.debug(
                    "Unable to connect to MCS endpoint after %s tries, shutting down (FCM connectivity issue)",
                    self.config.connection_retry_count,
                )
                self._terminate()
                return
            self.logger.debug("Re-connected to ssl socket")

            await self._login()

    # protobuf varint32 helpers
    async def _read_varint32(self) -> int:
        res = 0
        shift = 0
        while True:
            r = await self.reader.readexactly(1)  # type: ignore[union-attr]
            (b,) = struct.unpack("B", r)
            res |= (b & 0x7F) << shift
            if (b & 0x80) == 0:
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
    def _make_packet(msg: Message, include_version: bool) -> bytes:
        tag = MCS_MESSAGE_TAG[type(msg)]
        header = bytearray([MCS_VERSION, tag]) if include_version else bytearray([tag])
        payload = msg.SerializeToString()
        buf = bytes(header) + FcmPushClient._encode_varint32(len(payload)) + payload
        return buf

    async def _send_msg(self, msg: Message) -> None:
        self._log_verbose("Sending packet to server: %s", self._msg_str(msg))
        buf = FcmPushClient._make_packet(msg, self.first_message)
        self.writer.write(buf)  # type: ignore[union-attr]
        await self.writer.drain()  # type: ignore[union-attr]

    async def _receive_msg(self) -> Message | None:
        if self.first_message:
            r = await self.reader.readexactly(2)  # type: ignore[union-attr]
            version, tag = struct.unpack("BB", r)
            if version < MCS_VERSION and version != 38:
                raise RuntimeError(f"protocol version {version} unsupported")
            self.first_message = False
        else:
            r = await self.reader.readexactly(1)  # type: ignore[union-attr]
            (tag,) = struct.unpack("B", r)
        size = await self._read_varint32()

        self._log_verbose(
            "Received message with tag %s and size %s",
            tag,
            size,
        )

        if not size >= 0:
            self._log_warn_with_limit("Unexpected message size %s", size)
            return None

        buf = await self.reader.readexactly(size)  # type: ignore[union-attr]

        msg_class = next(iter([c for c, t in MCS_MESSAGE_TAG.items() if t == tag]))
        if not msg_class:
            self._log_warn_with_limit("Unexpected message tag %s", tag)
            return None
        if isinstance(msg_class, str):
            self._log_warn_with_limit("Unconfigured message class %s", msg_class)
            return None

        payload = msg_class()  # type: ignore[operator]
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
                raise ValueError("android_id or security_token is missing from credentials")

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
            self.logger.error("Received an exception logging in: %s", ex)
            if self._try_increment_error_count(ErrorType.LOGIN):
                await self._reset()

    @staticmethod
    def _decrypt_raw_data(
        credentials: dict[str, dict[str, str]],
        crypto_key_str: str,
        salt_str: str,
        raw_data: bytes,
    ) -> bytes:
        crypto_key = urlsafe_b64decode(crypto_key_str.encode("ascii"))
        salt = urlsafe_b64decode(salt_str.encode("ascii"))
        der_data_str = credentials["keys"]["private"]
        der_data = urlsafe_b64decode(der_data_str.encode("ascii") + b"========")
        secret_str = credentials["keys"]["secret"]
        secret = urlsafe_b64decode(secret_str.encode("ascii") + b"========")
        privkey = load_der_private_key(
            der_data, password=None, backend=default_backend()
        )
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

    def _handle_data_message(
        self,
        msg: DataMessageStanza,
    ) -> None:
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
            return
        crypto_key = self._app_data_by_key(msg, "crypto-key")[3:]  # strip dh=
        salt = self._app_data_by_key(msg, "encryption")[5:]  # strip salt=
        subtype = self._app_data_by_key(msg, "subtype")
        if TYPE_CHECKING:
            assert self.credentials
        if subtype != self.credentials["gcm"]["app_id"]:
            self._log_warn_with_limit(
                "Subtype %s in data message does not match"
                + "app id client was registered with %s",
                subtype,
                self.credentials["gcm"]["app_id"],
            )
        if not self.credentials:
            return

        decrypted = self._decrypt_raw_data(
            self.credentials, crypto_key, salt, msg.raw_data
        )

        # --- Minimal robustness patch: normalize decrypted payload to a dict ---
        # Normalize decrypted payload to a dict (defensive against non-object JSON)
        # Rationale:
        # * json.loads() may return any JSON type (object/array/string/number/etc.).
        # * Downstream callbacks often expect a mapping (dict) and may index by key.
        # * To avoid "TypeError: string indices must be integers", we normalize here:
        #   - If JSON object: pass through as-is.
        #   - If valid JSON but not an object: wrap into {"_raw_json": <value>}.
        #   - If not JSON: wrap UTF-8 text into {"_raw_text": "..."} or bytes as hex
        #     into {"_raw_bytes": "..."}.
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
            ret_val: dict[str, Any] = decrypted_json
        elif decrypted_json is not None:
            self._log_warn_with_limit(
                "FCM payload JSON is not an object (%s); wrapping",
                type(decrypted_json).__name__,
            )
            ret_val = {"_raw_json": decrypted_json}
        else:
            # Not JSON → provide a stable mapping for consumers
            ret_val = {"_raw_text": text} if text is not None else {"_raw_bytes": decrypted.hex()}

        self._log_verbose(
            "Decrypted data for message %s is: %s", msg.persistent_id, ret_val
        )
        try:
            self.callback(ret_val, msg.persistent_id, self.callback_context)
            self._reset_error_count(ErrorType.NOTIFY)
        except Exception:
            self.logger.exception("Unexpected exception calling notification callback\n")
            self._try_increment_error_count(ErrorType.NOTIFY)

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
        if not p.extension:
            self._log_warn_with_limit(
                "Unexpected IqStanza id received with no extension", str(p)
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
        self.logger.debug("Sending selective ack for message id %s", persistent_id)
        await self._send_msg(iqs)

    async def _send_heartbeat(self) -> None:
        req = HeartbeatPing()

        if self._new_input_stream_id_available():
            req.last_stream_id_received = self._get_input_stream_id()

        await self._send_msg(req)
        self.logger.debug("Sent heartbeat ping")

    def _terminate(self) -> None:
        self.run_state = FcmPushClientRunState.STOPPING

        self.do_listen = False
        current_task = asyncio.current_task()
        for task in self.tasks:
            if (
                current_task != task and not task.done()
            ):  # cancel return if task is done so no need to check
                task.cancel()

    async def _do_monitor(self) -> None:
        """Monitor task: only checks for prolonged inactivity, then resets.

        Design: sending of heartbeats is handled by a separate periodic task to avoid
        races where HeartbeatAck arrives during a sleep window.
        """
        # Inactivity timeout = max of both heartbeat intervals + small grace
        base_client = self.config.client_heartbeat_interval or 0
        base_server = self.config.server_heartbeat_interval or 0
        timeout_duration = max(base_client, base_server, 1) + 5.0

        while self.do_listen:
            await asyncio.sleep(self.config.monitor_interval)

            if self.run_state == FcmPushClientRunState.STARTED:
                now = time.time()
                last = self.last_message_time or 0.0
                if last and (now - last > timeout_duration):
                    self.logger.warning(
                        "No message received in %.1fs. Connection likely stale, resetting.",
                        timeout_duration,
                    )
                    if self._try_increment_error_count(ErrorType.CONNECTION):
                        await self._reset()

    async def _start_heartbeat_sender(self) -> None:
        """Send client heartbeats at a fixed interval while started.

        Keeps protocol behavior, but decouples ping sending from monitoring to avoid false resets.
        """
        interval = self.config.client_heartbeat_interval or 0
        if interval <= 0:
            return
        while self.do_listen:
            await asyncio.sleep(interval)
            if self.run_state == FcmPushClientRunState.STARTED:
                self.logger.debug("Sending scheduled client heartbeat")
                try:
                    await self._send_heartbeat()
                except Exception as ex:
                    self.logger.debug("Error while sending heartbeat: %s", ex)

    def _reset_error_count(self, error_type: ErrorType) -> None:
        self.sequential_error_counters[error_type] = 0

    def _try_increment_error_count(self, error_type: ErrorType) -> bool:
        if error_type not in self.sequential_error_counters:
            self.sequential_error_counters[error_type] = 0

        self.sequential_error_counters[error_type] += 1

        if (
            self.config.abort_on_sequential_error_count
            and self.sequential_error_counters[error_type]
            >= self.config.abort_on_sequential_error_count
        ):
            self.logger.debug(
                "Shutting down push receiver due to %d sequential errors of type %s (FCM connectivity issue)",
                self.sequential_error_counters[error_type],
                error_type,
            )
            self._terminate()
            return False
        return True

    async def _handle_message(self, msg: Message) -> None:
        self.last_message_time = time.time()
        self.input_stream_id += 1

        if isinstance(msg, Close):
            self._log_warn_with_limit("Server sent Close message, resetting")
            if self._try_increment_error_count(ErrorType.CONNECTION):
                await self._reset()
            return

        if isinstance(msg, LoginResponse):
            if str(msg.error):
                self.logger.error("Received login error response: %s", msg)
                if self._try_increment_error_count(ErrorType.LOGIN):
                    await self._reset()
            else:
                self.logger.info("Successfully logged in to MCS endpoint")
                self._reset_error_count(ErrorType.LOGIN)
                self.run_state = FcmPushClientRunState.STARTED
                self.persistent_ids = []
                # Refresh activity timestamp
                self.last_message_time = time.time()
            return

        if isinstance(msg, DataMessageStanza):
            self._handle_data_message(msg)
            self.persistent_ids.append(msg.persistent_id)
            if self.config.send_selective_acknowledgements:
                await self._send_selective_ack(msg.persistent_id)
        elif isinstance(msg, HeartbeatPing):
            await self._handle_ping(msg)
        elif isinstance(msg, HeartbeatAck):
            self.logger.debug("Received heartbeat ack: %s", msg)
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
        return await asyncio.open_connection(host=host, port=port, ssl=ssl_context)

    async def _connect(self) -> bool:
        try:
            loop = asyncio.get_running_loop()
            # create_default_context() blocks the event loop
            ssl_context = await loop.run_in_executor(None, ssl.create_default_context)
            self.reader, self.writer = await self._open_connection(
                host=MCS_HOST, port=MCS_PORT, ssl_context=ssl_context
            )
            self.logger.debug("Connected to MCS endpoint (%s,%s)", MCS_HOST, MCS_PORT)
            return True
        except OSError as oex:
            self.logger.error(
                "Could not connected to MCS endpoint (%s,%s): %s",
                MCS_HOST,
                MCS_PORT,
                oex,
            )
            return False

    def _exp_backoff_with_jitter(self, trycount: int) -> float:
        """Exponential backoff: 0.5,1,2,4,... capped at 60s, plus 10–20% jitter."""
        base = min(0.5 * (2 ** (trycount - 1)), 60.0)
        jitter = base * random.uniform(0.10, 0.20)
        return base + jitter

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
                "Unable to connect to MCS endpoint after %s tries, aborting", trycount
            )
        return connected

    async def _listen(self) -> None:
        """Listens for push notifications."""
        if not await self._connect_with_retry():
            return

        try:
            await self._login()

            while self.do_listen:
                try:
                    if self.run_state == FcmPushClientRunState.RESETTING:
                        await asyncio.sleep(1)
                    elif msg := await self._receive_msg():
                        await self._handle_message(msg)

                except (OSError, EOFError, asyncio.IncompleteReadError) as osex:
                    # Normal network life-cycle: treat reset by peer as debug, throttled
                    if isinstance(osex, ConnectionResetError):
                        self._log_reset_by_peer(osex)
                    else:
                        self.logger.exception("Unexpected exception during read\n")
                    if self._try_increment_error_count(ErrorType.CONNECTION):
                        await self._reset()

        except Exception as ex:
            # Avoid brittle string-matching; if we were resetting, downgrade to debug
            if self.run_state == FcmPushClientRunState.RESETTING:
                self.logger.debug("Read error during reset transition: %s", ex)
            else:
                self.logger.error(
                    "Unknown error: %s, shutting down FcmPushClient.\n%s",
                    ex,
                    traceback.format_exc(),
                )
            self._terminate()
        finally:
            await self._do_writer_close()

    async def checkin_or_register(self) -> str:
        """Check in if you have credentials otherwise register as a new client.

        :param sender_id: sender id identifying push service you are connecting to.
        :param app_id: identifier for your application.
        :return: The FCM token which is used to identify you with the push end
            point application.
        """
        self.register = FcmRegister(
            self.fcm_config,
            self.credentials,
            self.credentials_updated_callback,
            http_client_session=self._http_client_session,
        )
        self.credentials = await self.register.checkin_or_register()
        # await self.register.fcm_refresh_install()
        await self.register.close()
        return self.credentials["fcm"]["registration"]["token"]

    async def start(self) -> None:
        """Connect to FCM and start listening for push notifications."""
        self.do_listen = True
        self.run_state = FcmPushClientRunState.STARTING_TASKS
        try:
            # Initialize activity clock to "now" so the monitor doesn't instantly reset
            self.last_message_time = time.time()
            self.tasks = [
                asyncio.create_task(self._listen()),
                asyncio.create_task(self._do_monitor()),
                asyncio.create_task(self._start_heartbeat_sender()),
            ]
        except Exception as ex:
            self.logger.error("Unexpected error running FcmPushClient: %s", ex)

    async def stop(self) -> None:
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

        async with self.stopping_lock:  # type: ignore[union-attr]
            try:
                self.run_state = FcmPushClientRunState.STOPPING

                self.do_listen = False

                for task in self.tasks:
                    if not task.done():
                        task.cancel()

            finally:
                self.run_state = FcmPushClientRunState.STOPPED
                self.fcm_thread = None
                self.listen_event_loop = None

    def is_started(self) -> bool:
        return self.run_state == FcmPushClientRunState.STARTED

    async def send_message(self, raw_data: bytes, persistent_id: str) -> None:
        """Not implemented, does nothing atm."""
        dms = DataMessageStanza()
        dms.persistent_id = persistent_id

        # Not supported yet
