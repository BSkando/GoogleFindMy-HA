# custom_components/googlefindmy/beacon_actions.py
"""Read-only FMDN Beacon Actions diagnostics over GATT.

The ``read_beacon_state`` action asks a tracker directly what it holds: whether
an ephemeral identity key (EIK) is set, whether the integration's account key is
the owner account key, which EID it currently advertises, and whether it accepts
a ring key derived from the integration's EIK. That separates "the tracker holds
another key" from "the integration derives the EID differently", which passive
scanning cannot do.

Scope and guarantees
--------------------
- The set of data IDs is closed: ``0x00`` read beacon parameters, ``0x01`` read
  provisioning state, ``0x06`` read ringing state. Everything else is rejected
  before any GATT access, see ``_BeaconSession.run``.
- Nothing here runs unless the action is called: no timer, no listener, no entity.
  The only log calls are at DEBUG level. The second-by-second EID search runs only
  when the caller asks for it and stops at a fixed time budget.
- Keys never leave this module's call chain: not in logs, responses, exceptions or
  ``repr``. Bleak error texts carry the hardware address, so they are reduced to an
  ATT error code and the exception class. The raw ``0x01`` frame, which holds the
  public EID, is returned only when the caller asks for it.

Specification: https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import re
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from enum import IntEnum, StrEnum
from typing import Any, Protocol

_LOGGER = logging.getLogger(__name__)

BEACON_ACTIONS_UUID = "fe2c1238-8366-4814-8eb0-01de32100bea"
FAST_PAIR_SERVICE_DATA_UUID = "0000fe2c-0000-1000-8000-00805f9b34fb"

PROTOCOL_MAJOR_VERSION = 0x01
NONCE_LENGTH = 8
AUTH_LENGTH = 8
ACCOUNT_KEY_LENGTH = 16
EIK_LENGTH = 32

ATT_UNAUTHENTICATED = 0x80
ATT_INVALID_VALUE = 0x81
ATT_NO_USER_CONSENT = 0x82

ROTATION_SECONDS = 1024
# A tracker rotates 1 to 204 s after the calculated boundary, so the clock can be
# one window ahead of the advertised EID (and a tracker with a drifting clock one
# behind). The current window goes first.
EID_WINDOW_OFFSETS = (0, -1, 1)
MAX_COUNTER = 0xFFFFFFFF
FULL_BYTE = 0xFF
# Stop and disconnect can hang on a dead link; a hung cleanup must not hold the lock.
CLEANUP_TIMEOUT = 5.0
PARAMETERS_LENGTH = 16
RINGING_LENGTH = 3
PROVISIONING_LENGTHS = (1, 21, 33)
EID_LENGTHS = (20, 32)
MIN_FAST_PAIR_FRAME_LENGTH = 2
# A tracker can change its address while the old one is still cached, so one call
# may try several addresses. Untried addresses share the time, capped, because a dead
# address would otherwise use all of it (measured: 40 to 57 s per failed call). The
# cap stays above the 20 s bleak allows itself, so at the default limit of 45 s a lone
# address gets its full try.
MAX_CONNECT_ATTEMPTS = 3
CONNECT_ATTEMPT_LIMIT = 25.0
# Time kept for the reads after a late connection: at most this, at most a third.
READ_RESERVE_SECONDS = 10.0
# Upper bound of one EID search, however wide the requested window is.
EID_SEARCH_TIME_BUDGET = 300.0
EID_SEARCH_MAX_SECONDS = 172800
_EID_SEARCH_CHECK_EVERY = 256
_FHNA_K_BYTE = 10

# First byte forms of an account key. The stored key usually starts with 0x04; the
# advertised filter also uses 0x05 while the key is in use (measured on Pixel Buds
# Pro 2). Which form a Provider expects for HMAC and AES is tried in this order.
_KEY_FIRST_BYTES: tuple[tuple[str, int | None], ...] = (
    ("raw", None),
    ("first_byte_04", 0x04),
    ("first_byte_05", 0x05),
)

# The two text forms an ATT code is known to take: ``error=128`` from aioesphomeapi
# and ``ATT error: 0x80`` from BlueZ-style backends. Anything looser reads numbers
# out of unrelated messages.
_ATT_CODE_PATTERN = re.compile(r"\berror=(\d{1,3})\b|\bATT error: (0x[0-9a-fA-F]{2})\b")


class DataId(IntEnum):
    """Beacon Actions operations this module may send. Closed on purpose."""

    READ_BEACON_PARAMETERS = 0x00
    READ_PROVISIONING_STATE = 0x01
    READ_RINGING_STATE = 0x06


class OperationStatus(StrEnum):
    """Outcome of one Beacon Actions operation."""

    OK = "ok"
    UNAUTHENTICATED = "unauthenticated"
    INVALID_VALUE = "invalid_value"
    NO_USER_CONSENT = "no_user_consent"
    GATT_ERROR = "gatt_error"
    TIMEOUT = "timeout"
    INVALID_RESPONSE = "invalid_response"
    RESPONSE_AUTH_INVALID = "response_auth_invalid"
    SKIPPED_NO_KEY = "skipped_no_key"
    NOT_ATTEMPTED = "not_attempted"


class BeaconProtocolError(ValueError):
    """A frame violates the Beacon Actions format. Never carries key material."""


class _ResponseAuthError(BeaconProtocolError):
    """The response authentication segment does not match."""


class _NonceReadError(Exception):
    """Reading the nonce failed; ``cause`` holds the backend error."""

    def __init__(self, cause: Exception) -> None:
        super().__init__(type(cause).__name__)
        self.cause = cause


class UnsupportedDataIdError(ValueError):
    """The requested data ID is outside the closed read-only set."""


@dataclass(frozen=True)
class BeaconKeys:
    """Key material of one tracker; both fields stay out of ``repr``."""

    account_key: bytes | None = field(default=None, repr=False)
    eik: bytes | None = field(default=None, repr=False)


class BeaconGattClient(Protocol):
    """The part of a connected bleak client this module uses."""

    async def read_gatt_char(self, char_specifier: str) -> bytes | bytearray:
        """Read a characteristic."""

    async def write_gatt_char(
        self, char_specifier: str, data: bytes, response: bool = ...
    ) -> None:
        """Write a characteristic."""

    async def start_notify(
        self, char_specifier: str, callback: Callable[[Any, bytearray], Any]
    ) -> None:
        """Subscribe to notifications."""

    async def stop_notify(self, char_specifier: str) -> None:
        """Unsubscribe from notifications."""


# ---------------------------------------------------------------------------
# Codec
# ---------------------------------------------------------------------------


def _auth(key: bytes, *parts: bytes) -> bytes:
    """Return the first 8 bytes of HMAC-SHA256 over the concatenated parts."""
    return hmac.new(key, b"".join(parts), hashlib.sha256).digest()[:AUTH_LENGTH]


def parse_nonce(value: bytes | bytearray) -> tuple[int, bytes]:
    """Return version and one-time nonce from a characteristic read."""
    if len(value) != 1 + NONCE_LENGTH:
        raise BeaconProtocolError("unexpected nonce frame length")
    return value[0], bytes(value[1:])


def build_request(data_id: int, key: bytes, version: int, nonce: bytes) -> bytes:
    """Return the request frame for one read-only operation.

    The data length counts the authentication segment (FMDN table 3 gives 0x08 for
    a request that holds nothing else).
    """
    head = bytes((_checked_data_id(data_id), AUTH_LENGTH))
    return head + _auth(key, bytes((version,)), nonce, head)


def verify_response(
    frame: bytes | bytearray, data_id: int, key: bytes, version: int, nonce: bytes
) -> bytes:
    """Check a notification against its request and return the additional data."""
    if len(frame) < 2 + AUTH_LENGTH or frame[0] != data_id:
        raise BeaconProtocolError("unexpected response frame")
    if frame[1] != len(frame) - 2:
        raise BeaconProtocolError("data length does not match the frame")
    additional = bytes(frame[2 + AUTH_LENGTH :])
    expected = _auth(
        key, bytes((version,)), nonce, bytes(frame[:2]), additional, b"\x01"
    )
    if not hmac.compare_digest(expected, bytes(frame[2 : 2 + AUTH_LENGTH])):
        raise _ResponseAuthError
    return additional


def ring_key(eik: bytes) -> bytes:
    """Return the ring key derived from an EIK."""
    return hashlib.sha256(eik + b"\x02").digest()[:AUTH_LENGTH]


def decrypt_parameters(account_key: bytes, data: bytes) -> dict[str, int]:
    """Decrypt and parse the ``0x00`` response data."""
    if len(data) != PARAMETERS_LENGTH:
        raise BeaconProtocolError("unexpected beacon parameter length")
    from .FMDNCrypto._lazy_crypto import (  # noqa: PLC0415 - heavy, executor only
        get_algorithms_module,
        get_cipher_class,
        get_modes_module,
    )

    decryptor = (
        get_cipher_class()(
            get_algorithms_module().AES(account_key), get_modes_module().ECB()
        )
    ).decryptor()
    plain = decryptor.update(data) + decryptor.finalize()
    return {
        "calibrated_power": int.from_bytes(plain[0:1], "big", signed=True),
        "clock": int.from_bytes(plain[1:5], "big"),
        "curve": plain[5],
        "components": plain[6],
        "ringing_capabilities": plain[7],
    }


def parse_provisioning(data: bytes) -> tuple[bool, bool, bytes]:
    """Return EIK bit, owner bit and the current EID from the ``0x01`` data."""
    if len(data) not in PROVISIONING_LENGTHS:
        raise BeaconProtocolError("unexpected provisioning state length")
    return bool(data[0] & 0x01), bool(data[0] & 0x02), data[1:]


def parse_ringing(data: bytes) -> dict[str, int]:
    """Parse the ``0x06`` response data."""
    if len(data) != RINGING_LENGTH:
        raise BeaconProtocolError("unexpected ringing state length")
    return {
        "ringing_components": data[0],
        "ringing_remaining_ds": int.from_bytes(data[1:3], "big"),
    }


def account_key_forms(account_key: bytes) -> list[tuple[str, bytes]]:
    """Return the distinct first-byte forms of an account key with their names."""
    forms: list[tuple[str, bytes]] = []
    seen: set[bytes] = set()
    for name, first in _KEY_FIRST_BYTES:
        key = account_key if first is None else bytes((first,)) + account_key[1:]
        if key not in seen:
            seen.add(key)
            forms.append((name, key))
    return forms


# ---------------------------------------------------------------------------
# Fast Pair advertisement matching
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FastPairFrame:
    """Account key filter fields of a Fast Pair (0xFE2C) advertisement."""

    account_filter: bytes
    salt: bytes
    trailer: bytes


def parse_fast_pair_frame(data: bytes | bytearray) -> FastPairFrame | None:
    """Return the filter fields, or ``None`` for any other frame form.

    Measured form: flags byte, length/type 0x40 with a 4-byte filter, length/type
    0x21 with a 2-byte salt, optional trailing fields. Forms that do not parse are
    counted by the caller instead of being guessed at.
    """
    if len(data) < MIN_FAST_PAIR_FRAME_LENGTH:
        return None
    filter_lt = data[1]
    filter_length = filter_lt >> 4
    if filter_lt & 0x0F not in (0x00, 0x02) or filter_length == 0:
        return None
    salt_at = 2 + filter_length
    if len(data) <= salt_at:
        return None
    salt_lt = data[salt_at]
    salt_length = salt_lt >> 4
    if salt_lt & 0x0F != 0x01 or salt_length == 0:
        return None
    trailer_at = salt_at + 1 + salt_length
    if len(data) < trailer_at:
        return None
    return FastPairFrame(
        account_filter=bytes(data[2 : 2 + filter_length]),
        salt=bytes(data[salt_at + 1 : trailer_at]),
        trailer=bytes(data[trailer_at:]),
    )


def _filter_bits(key: bytes, frame: FastPairFrame) -> bytes:
    """Return the filter this key alone would set in the frame."""
    size = len(frame.account_filter)
    digest = hashlib.sha256(key + frame.salt + frame.trailer).digest()
    bits = bytearray(size)
    for index in range(8):
        value = int.from_bytes(digest[4 * index : 4 * index + 4], "big") % (size * 8)
        bits[value // 8] |= 1 << (value % 8)
    return bytes(bits)


def filter_matches(frame: FastPairFrame, account_key: bytes) -> bool:
    """Tell whether the frame's filter contains this account key.

    A filter may hold several keys of one Provider, so all bits of this key must be
    set, not the whole filter be equal. It stays probabilistic: an unrelated frame
    can match by chance, which is why the report says ``filter_checked`` and not
    more. A saturated filter matches every key and says nothing, so it never matches.
    """
    if set(frame.account_filter) == {FULL_BYTE}:
        return False
    for _name, key in account_key_forms(account_key):
        bits = _filter_bits(key, frame)
        if all(a & b == b for a, b in zip(frame.account_filter, bits, strict=True)):
            return True
    return False


@dataclass(frozen=True)
class CandidateSelection:
    """Connectable Fast Pair advertisers that match an account key."""

    addresses: tuple[str, ...]
    frames_seen: int
    frames_matched: int
    frames_unparsed: int
    seen_at: Mapping[str, float] = field(default_factory=dict)


def select_candidates(infos: Iterable[Any], account_key: bytes) -> CandidateSelection:
    """Return advertisers whose filter holds the key, newest sighting first.

    The cache keeps an address after the tracker moved on to the next one, and the
    old entry can still hold the stronger RSSI (measured on Pixel Buds Pro 2). RSSI
    only breaks ties between equally recent sightings.
    """
    matched: list[tuple[float, int, str]] = []
    seen = unparsed = 0
    for info in infos:
        service_data: Mapping[str, bytes] = info.service_data or {}
        raw = service_data.get(FAST_PAIR_SERVICE_DATA_UUID)
        if raw is None:
            continue
        seen += 1
        frame = parse_fast_pair_frame(raw)
        if frame is None:
            unparsed += 1
            continue
        if filter_matches(frame, account_key):
            seen_at = float(getattr(info, "time", 0.0) or 0.0)
            matched.append((seen_at, int(info.rssi), str(info.address)))
    matched.sort(key=lambda item: item[:2], reverse=True)
    return CandidateSelection(
        addresses=tuple(address for _seen, _rssi, address in matched),
        frames_seen=seen,
        frames_matched=len(matched),
        frames_unparsed=unparsed,
        seen_at={address: seen for seen, _rssi, address in matched},
    )


# ---------------------------------------------------------------------------
# EID comparison
# ---------------------------------------------------------------------------


def compare_eid(eik: bytes, clock: int, reported_eid: bytes) -> dict[str, Any]:
    """Compare the tracker's EID with what the integration derives from its EIK."""
    from .FMDNCrypto.eid_generator import (  # noqa: PLC0415 - heavy, executor only
        EidVariant,
        generate_eid_variant,
    )

    checked = 0
    for window_offset in EID_WINDOW_OFFSETS:
        counter = clock + window_offset * ROTATION_SECONDS
        if not 0 <= counter <= MAX_COUNTER:
            continue
        for variant in EidVariant:
            checked += 1
            try:
                derived = generate_eid_variant(eik, counter, variant)
            except ValueError:
                continue
            if hmac.compare_digest(derived, reported_eid):
                return {
                    "result": "match",
                    "variant": variant.value,
                    "window_offset": window_offset,
                    "variants_checked": checked,
                }
    return {
        "result": "no_match",
        "variant": None,
        "window_offset": None,
        "variants_checked": checked,
    }


def _unmasked_prf_input(counter: int) -> bytes:
    """Return the Table 10 block with the counter's low bits left in place.

    The specification clears them, and ``build_table10_prf_input`` always does. This
    form exists only to test whether a tracker departs from that.
    """
    raw = counter.to_bytes(4, "big")
    head = b"\xff" * 11 + bytes((_FHNA_K_BYTE,)) + raw
    return head + b"\x00" * 11 + bytes((_FHNA_K_BYTE,)) + raw


def _search_offsets(window_seconds: int) -> Iterable[int]:
    """Yield offsets outward from the clock, older counters first at each distance."""
    yield 0
    for distance in range(1, window_seconds + 1):
        yield -distance
        yield distance


def search_eid_counter(  # noqa: PLR0913
    eik: bytes,
    clock: int,
    targets: Mapping[bytes, str],
    window_seconds: int,
    stop: threading.Event | None = None,
    *,
    time_budget: float = EID_SEARCH_TIME_BUDGET,
    now: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    """Look for the counter that yields a target EID, second by second around the clock.

    Runs in an executor. ``stop`` ends it within a moment when the caller is
    cancelled, so a new call runs next to a forgotten one for a moment at most.
    ``targets`` maps an EID to the name of its source; only the name is reported. A
    hit on a
    counter whose low 10 bits are zero is what the specification prescribes, any
    other hit shows a tracker that departs from it.
    """
    from .FMDNCrypto.eid_generator import (  # noqa: PLC0415 - heavy, executor only
        EidVariant,
        _serialize_variant,
        prf_aes_256_ecb,
    )

    lengths = {len(target) for target in targets}
    variants = [
        variant
        for variant in EidVariant
        if (32 if "x32" in variant.value else 20) in lengths
    ]
    started = now()
    checked = 0
    report: dict[str, Any] = {
        "result": "no_match",
        "window_seconds": window_seconds,
        "variants": len(variants),
    }
    for offset in _search_offsets(window_seconds):
        counter = clock + offset
        if not 0 <= counter <= MAX_COUNTER:
            continue
        if checked % _EID_SEARCH_CHECK_EVERY == 0:
            if stop is not None and stop.is_set():
                report["result"] = "cancelled"
                break
            if now() - started > time_budget:
                report["result"] = "time_budget_exhausted"
                break
        checked += 1
        block = prf_aes_256_ecb(eik, _unmasked_prf_input(counter))
        for variant in variants:
            source = targets.get(_serialize_variant(block, variant))
            if source is not None:
                report.update(
                    result="match",
                    source=source,
                    variant=variant.value,
                    counter_offset=offset,
                    counter_low_bits=counter % ROTATION_SECONDS,
                )
                report["counters_checked"] = checked
                report["duration_s"] = round(now() - started, 1)
                return report
    report["counters_checked"] = checked
    report["duration_s"] = round(now() - started, 1)
    return report


# ---------------------------------------------------------------------------
# GATT flow
# ---------------------------------------------------------------------------


def att_error_code(err: BaseException | None) -> int | None:
    """Return the ATT error code an exception carries, or ``None``.

    Over an ESPHome proxy the code sits in ``BluetoothGATTAPIError.error.error``,
    wrapped by ``BleakError(str(ex)) from ex``; the text is the fallback. Only the
    explicit cause chain counts, an unrelated ``__context__`` does not. The text
    stays here, it holds the hardware address. This runs in an error path and must
    not raise itself.
    """
    seen: set[int] = set()
    current = err
    try:
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            nested = getattr(getattr(current, "error", None), "error", None)
            if isinstance(nested, int) and not isinstance(nested, bool):
                return nested
            match = _ATT_CODE_PATTERN.search(str(current))
            if match:
                return int(match.group(1) or match.group(2), 0)
            current = current.__cause__
    except Exception:  # noqa: BLE001 - a hostile __str__ or property must not escape
        return None
    return None


def _checked_data_id(data_id: int) -> DataId:
    """Return the data ID if it is in the closed read-only set."""
    try:
        return DataId(data_id)
    except ValueError as err:
        raise UnsupportedDataIdError("data ID is not in the read-only set") from err


def _status_from_att(code: int | None) -> OperationStatus:
    """Map an ATT error code to an operation status."""
    return {
        ATT_UNAUTHENTICATED: OperationStatus.UNAUTHENTICATED,
        ATT_INVALID_VALUE: OperationStatus.INVALID_VALUE,
        ATT_NO_USER_CONSENT: OperationStatus.NO_USER_CONSENT,
    }.get(code if code is not None else -1, OperationStatus.GATT_ERROR)


@dataclass
class _Operation:
    """Result of one operation; ``data`` holds verified additional data.

    ``frame`` is the notification as received, whatever became of it, and
    ``auth_valid`` tells whether its authentication segment matched under the
    request's key. ``invalid_stage`` names where an invalid response failed.
    """

    status: OperationStatus
    data: bytes = b""
    error_class: str | None = None
    att_error: int | None = None
    frame: bytes | None = field(default=None, repr=False)
    auth_valid: bool | None = None
    invalid_stage: str | None = None


@dataclass(repr=False)
class _Runs:
    """Outcomes of one connection, handed to the report builder."""

    parameters: _Operation
    parameters_key: bytes | None
    provisioning: _Operation
    ringing: _Operation
    key_form: str | None
    include_raw: bool = False


@dataclass(repr=False)
class _Reading:
    """A finished report plus what a later EID search needs from the same link."""

    report: dict[str, Any]
    clock: int | None
    eids: tuple[bytes, ...]


def _failed_operation(
    data_id: DataId, err: Exception, status: OperationStatus | None = None
) -> _Operation:
    """Return the outcome of a failed GATT call without its error text."""
    code = att_error_code(err)
    _LOGGER.debug(
        "Beacon operation 0x%02x failed: %s, ATT error %s",
        data_id,
        type(err).__name__,
        code,
    )
    return _Operation(status or _status_from_att(code), b"", type(err).__name__, code)


class _BeaconSession:
    """One subscribed connection; routes notifications to the waiting operation."""

    def __init__(
        self, client: BeaconGattClient, timeout: float, deadline: float | None = None
    ) -> None:
        self._client = client
        self._timeout = timeout
        self._deadline = deadline
        self._waiting: tuple[int, asyncio.Future[bytes]] | None = None

    def on_notify(self, _sender: Any, data: bytearray) -> None:
        """Hand a notification with the expected data ID to the waiting operation."""
        if self._waiting is None or not data or data[0] != self._waiting[0]:
            return
        if not self._waiting[1].done():
            self._waiting[1].set_result(bytes(data))

    def budget(self, loop: asyncio.AbstractEventLoop) -> float:
        """Return the time this operation may take within the overall limit."""
        if self._deadline is None:
            return self._timeout
        return min(self._timeout, self._deadline - loop.time())

    async def run(self, data_id: int, key: bytes) -> _Operation:
        """Read a fresh nonce, send one request and verify its notification.

        A rejection only counts on the write: the specification defines the
        unauthenticated error for write requests, and a failed read says nothing
        about a key.
        """
        checked = _checked_data_id(data_id)
        loop = asyncio.get_running_loop()
        budget = self.budget(loop)
        if budget <= 0:
            return _Operation(OperationStatus.NOT_ATTEMPTED)
        future: asyncio.Future[bytes] = loop.create_future()
        self._waiting = (checked, future)
        frame: bytes | None = None
        version, nonce = 0, b""
        try:
            async with asyncio.timeout(budget):
                version, nonce = await self._read_nonce()
                # The Provider notifies before the write completes, so the future
                # exists before the write goes out.
                await self._client.write_gatt_char(
                    BEACON_ACTIONS_UUID,
                    build_request(checked, key, version, nonce),
                    response=True,
                )
                frame = await future
            operation = _Operation(
                OperationStatus.OK,
                verify_response(frame, checked, key, version, nonce),
                frame=frame,
                auth_valid=True,
            )
        except TimeoutError:
            operation = _Operation(OperationStatus.TIMEOUT)
        except _ResponseAuthError:
            operation = _Operation(
                OperationStatus.RESPONSE_AUTH_INVALID,
                frame=frame,
                auth_valid=False,
                invalid_stage="auth",
            )
        except BeaconProtocolError:
            operation = _Operation(
                OperationStatus.INVALID_RESPONSE,
                frame=frame,
                auth_valid=(
                    None
                    if frame is None
                    else _frame_auth_valid(frame, key, version, nonce)
                ),
                invalid_stage="nonce" if frame is None else "frame",
            )
        except _NonceReadError as err:
            operation = _failed_operation(
                checked, err.cause, OperationStatus.GATT_ERROR
            )
        except Exception as err:  # noqa: BLE001 - the backend's error types vary
            operation = _failed_operation(checked, err)
        finally:
            self._waiting = None
            if not future.done():
                future.cancel()
        return operation

    async def _read_nonce(self) -> tuple[int, bytes]:
        """Read version and nonce; a backend failure stays apart from a write failure."""
        try:
            value = await self._client.read_gatt_char(BEACON_ACTIONS_UUID)
        except TimeoutError:
            raise
        except Exception as err:  # noqa: BLE001 - the backend's error types vary
            raise _NonceReadError(err) from err
        return parse_nonce(value)


def _frame_auth_valid(frame: bytes, key: bytes, version: int, nonce: bytes) -> bool:
    """Tell whether a frame that failed the format check still authenticates."""
    if len(frame) < 2 + AUTH_LENGTH:
        return False
    expected = _auth(
        key,
        bytes((version,)),
        nonce,
        frame[:2],
        frame[2 + AUTH_LENGTH :],
        b"\x01",
    )
    return hmac.compare_digest(expected, frame[2 : 2 + AUTH_LENGTH])


def _response_shape(operation: _Operation) -> dict[str, Any]:
    """Describe a response without its EID, for a form the parser does not know."""
    frame = operation.frame
    if frame is None:
        return {"stage": operation.invalid_stage}
    additional = frame[2 + AUTH_LENGTH :]
    shape: dict[str, Any] = {
        "stage": operation.invalid_stage,
        "frame_length": len(frame),
        "data_length_field": frame[1] if len(frame) > 1 else None,
        "additional_length": len(additional),
        "auth_valid": operation.auth_valid,
    }
    if operation.auth_valid and additional:
        shape["flags_byte"] = additional[0]
        shape["data_prefix"] = additional[:5].hex()
    return shape


def _eid_candidates(additional: bytes) -> tuple[bytes, ...]:
    """Return the EIDs an authenticated response of unknown form might carry."""
    candidates: list[bytes] = []
    for length in EID_LENGTHS:
        if len(additional) >= 1 + length:
            candidates.append(additional[1 : 1 + length])
        if len(additional) == length:
            candidates.append(additional)
    return tuple(candidates)


def _operation_report(operation: _Operation) -> dict[str, Any]:
    """Return the status fields of an operation."""
    report: dict[str, Any] = {"status": operation.status.value}
    if operation.att_error is not None:
        report["att_error"] = operation.att_error
    if operation.error_class is not None:
        report["error_class"] = operation.error_class
    return report


async def _read_with_account_key(
    session: _BeaconSession, data_id: DataId, forms: list[tuple[str, bytes]]
) -> tuple[_Operation, str | None, bytes | None]:
    """Run an account key operation, trying the next key form on rejection.

    Name and key come back only when the Provider accepted that form; a transport
    error proves nothing about it.
    """
    operation = _Operation(OperationStatus.SKIPPED_NO_KEY)
    for name, key in forms:
        operation = await session.run(data_id, key)
        if operation.status is OperationStatus.OK:
            return operation, name, key
        if operation.status is not OperationStatus.UNAUTHENTICATED:
            break
    return operation, None, None


async def read_beacon_state(
    client: BeaconGattClient,
    keys: BeaconKeys,
    *,
    operation_timeout: float = 10.0,
    deadline: float | None = None,
    include_raw: bool = False,
) -> dict[str, Any]:
    """Run the read operations on one connection and return the report."""
    reading = await _read_state(
        client,
        keys,
        operation_timeout=operation_timeout,
        deadline=deadline,
        include_raw=include_raw,
    )
    return reading.report


async def _read_state(
    client: BeaconGattClient,
    keys: BeaconKeys,
    *,
    operation_timeout: float = 10.0,
    deadline: float | None = None,
    include_raw: bool = False,
) -> _Reading:
    """Run ``0x00``, ``0x01``, ``0x06`` on one connection and report the results.

    ``eik_matches`` is ``True`` only after a verified ``0x06`` response and ``False``
    only after a rejected ``0x06`` write on a link where ``0x01`` verified. The code
    0x80 alone is not enough: over an ESP32 proxy it can also be the local
    ``ESP_GATT_NO_RESOURCES``, and the Provider returns it when not provisioned, so
    ``eik_set`` belongs next to it. Everything else is ``None``.

    ``deadline`` (event loop time) ends the run: operations that no longer fit are
    ``not_attempted``, and what was verified before stays in the report.
    """
    session = _BeaconSession(client, operation_timeout, deadline)
    # A hanging subscription must not outlive the limit and hold the caller's lock.
    loop = asyncio.get_running_loop()
    async with asyncio.timeout(session.budget(loop)):
        await client.start_notify(BEACON_ACTIONS_UUID, session.on_notify)
    try:
        forms = account_key_forms(keys.account_key) if keys.account_key else []
        parameters, form_name, key = await _read_with_account_key(
            session, DataId.READ_BEACON_PARAMETERS, forms
        )
        # An accepted form applies to the next operation too.
        (
            provisioning,
            provisioning_form,
            provisioning_key,
        ) = await _read_with_account_key(
            session,
            DataId.READ_PROVISIONING_STATE,
            [(form_name, key)] if form_name is not None and key is not None else forms,
        )
        ringing = (
            await session.run(DataId.READ_RINGING_STATE, ring_key(keys.eik))
            if keys.eik
            else _Operation(OperationStatus.SKIPPED_NO_KEY)
        )
    finally:
        try:
            async with asyncio.timeout(CLEANUP_TIMEOUT):
                await client.stop_notify(BEACON_ACTIONS_UUID)
        except Exception as err:  # noqa: BLE001 - the link may already be gone
            _LOGGER.debug("stop_notify failed: %s", type(err).__name__)

    return await asyncio.to_thread(
        _build_report,
        keys,
        _Runs(
            parameters=parameters,
            parameters_key=key,
            provisioning=provisioning,
            ringing=ringing,
            key_form=form_name or provisioning_form,
            include_raw=include_raw,
        ),
    )


def _parameters_report(runs: _Runs) -> tuple[dict[str, Any], int | None]:
    """Return the ``0x00`` report and the clock it holds."""
    parameters, parameters_key = runs.parameters, runs.parameters_key
    report = _operation_report(parameters)
    if parameters.status is not OperationStatus.OK or parameters_key is None:
        return report, None
    try:
        values = decrypt_parameters(parameters_key, parameters.data)
    except BeaconProtocolError:
        report["status"] = OperationStatus.INVALID_RESPONSE.value
        return report, None
    report.update(values)
    return report, values["clock"]


def _provisioning_report(
    runs: _Runs,
) -> tuple[dict[str, Any], bool | None, bool | None, tuple[bytes, ...], bool]:
    """Return the ``0x01`` report, both bits, the EIDs and whether the form is known."""
    provisioning = runs.provisioning
    report = _operation_report(provisioning)
    eik_set: bool | None = None
    is_owner: bool | None = None
    eids: tuple[bytes, ...] = ()
    known_form = False
    if provisioning.status is OperationStatus.OK:
        try:
            eik_set, is_owner, reported_eid = parse_provisioning(provisioning.data)
        except BeaconProtocolError:
            report["status"] = OperationStatus.INVALID_RESPONSE.value
            provisioning.invalid_stage = "length"
            eids = _eid_candidates(provisioning.data)
        else:
            known_form = True
            eids = (reported_eid,) if reported_eid else ()
            report["eik_set"] = eik_set
            report["is_owner"] = is_owner
            report["eid_prefix"] = reported_eid[:4].hex()
            report["eid_length"] = len(reported_eid)
    if provisioning.invalid_stage is not None:
        report["response"] = _response_shape(provisioning)
        if provisioning.auth_valid and provisioning.frame is not None and not eids:
            eids = _eid_candidates(provisioning.frame[2 + AUTH_LENGTH :])
    if runs.include_raw and provisioning.frame is not None:
        report["raw_frame"] = provisioning.frame.hex()
    return report, eik_set, is_owner, eids, known_form


def _build_report(keys: BeaconKeys, runs: _Runs) -> _Reading:
    """Build the response mapping; the crypto parts run off the event loop."""
    report_parameters, clock = _parameters_report(runs)
    (
        report_provisioning,
        eik_set,
        is_owner,
        eids,
        known_form,
    ) = _provisioning_report(runs)

    ringing = runs.ringing
    report_ringing = _operation_report(ringing)
    eik_matches: bool | None = None
    if ringing.status is OperationStatus.OK:
        try:
            report_ringing.update(parse_ringing(ringing.data))
            eik_matches = True
        except BeaconProtocolError:
            report_ringing["status"] = OperationStatus.INVALID_RESPONSE.value
    elif ringing.status is OperationStatus.UNAUTHENTICATED and eik_set is not None:
        eik_matches = False

    eid_check: dict[str, Any] = {"result": "not_checked"}
    if keys.eik and clock is not None and eids:
        for eid in eids:
            eid_check = compare_eid(keys.eik, clock, eid)
            if eid_check["result"] == "match":
                break
        if not known_form:
            # Read from a response form the specification does not define.
            eid_check["inferred_form"] = True

    report = {
        "key_form": runs.key_form,
        "eik_set": eik_set,
        "is_owner": is_owner,
        "eik_matches": eik_matches,
        "eid_check": eid_check,
        "operations": {
            "read_beacon_parameters": report_parameters,
            "read_provisioning_state": report_provisioning,
            "read_ringing_state": report_ringing,
        },
    }
    return _Reading(report, clock, eids)


# ---------------------------------------------------------------------------
# Home Assistant binding
# ---------------------------------------------------------------------------


def mask_address(address: str) -> str:
    """Return the last four characters of an address (AGENTS.md section 5, class c)."""
    return f"...{address[-4:]}"


def _load_ble_modules() -> tuple[Any, Any, Any]:
    """Import the Bluetooth stack; both are optional and run in the import executor."""
    from bleak_retry_connector import (  # noqa: PLC0415
        BleakClientWithServiceCache,
        establish_connection,
    )
    from homeassistant.components import bluetooth  # noqa: PLC0415

    return bluetooth, BleakClientWithServiceCache, establish_connection


@dataclass(frozen=True)
class _BleStack:
    """The Bluetooth functions of one call."""

    bluetooth: Any
    client_class: Any
    establish_connection: Any


async def _async_connect_once(  # noqa: PLR0913
    stack: _BleStack,
    device: Any,
    target: str,
    report: dict[str, Any],
    *,
    budget: float,
    max_attempts: int,
) -> Any:
    """Connect to one device; return the client, or ``None`` with the reason set."""
    try:
        async with asyncio.timeout(budget):
            client = await stack.establish_connection(
                stack.client_class,
                device,
                mask_address(target),
                max_attempts=max_attempts,
            )
    except TimeoutError:
        report["connection"] = "timeout"
        report.pop("error_class", None)
        return None
    except Exception as err:  # noqa: BLE001 - the backend's error types vary
        report["connection"] = "connection_failed"
        report["error_class"] = type(err).__name__
        _LOGGER.debug("Beacon connection failed: %s", type(err).__name__)
        return None
    report["connection"] = "connected"
    report.pop("error_class", None)
    return client


async def _async_connect_address(
    hass: Any, stack: _BleStack, target: str, report: dict[str, Any], timeout: float
) -> Any:
    """Connect to the address the caller named."""
    report["address_masked"] = mask_address(target)
    device = stack.bluetooth.async_ble_device_from_address(
        hass, target, connectable=True
    )
    if device is None:
        report["connection"] = "device_not_connectable"
        return None
    last_info = stack.bluetooth.async_last_service_info(hass, target, connectable=True)
    report["rssi"] = getattr(last_info, "rssi", None)
    return await _async_connect_once(
        stack, device, target, report, budget=timeout, max_attempts=2
    )


def _attempt_budget(remaining: float, untried: int, attempts_left: int) -> float:
    """Return the time one try may take: a share among the addresses still untried."""
    shares = min(max(untried, 1), attempts_left)
    return min(remaining / shares, CONNECT_ATTEMPT_LIMIT)


async def _async_connect_search(
    hass: Any,
    stack: _BleStack,
    account_key: bytes,
    report: dict[str, Any],
    connect_until: float,
) -> Any:
    """Connect to the newest matching advertiser, moving on when one fails.

    The selection repeats before every try, so an address that appears meanwhile is
    next in line. An address is tried again only when no untried one is left, also
    when it is the only one. Exactly one connection is held at the end, and only to
    an address that matched the filter.
    """
    loop = asyncio.get_running_loop()
    tried: list[str] = []
    attempts: list[dict[str, Any]] = []
    while len(attempts) < MAX_CONNECT_ATTEMPTS:
        selection = await hass.async_add_executor_job(
            select_candidates,
            list(stack.bluetooth.async_discovered_service_info(hass, connectable=True)),
            account_key,
        )
        report["frames_seen"] = selection.frames_seen
        report["frames_matched"] = selection.frames_matched
        report["frames_unparsed"] = selection.frames_unparsed
        if not selection.addresses:
            if not attempts:
                report["connection"] = "no_matching_frame"
            break
        remaining = connect_until - loop.time()
        if remaining <= 0:
            report["connection"] = "timeout"
            report.pop("error_class", None)
            break
        untried = [item for item in selection.addresses if item not in tried]
        target = untried[0] if untried else selection.addresses[0]
        budget = _attempt_budget(
            remaining, len(untried), MAX_CONNECT_ATTEMPTS - len(attempts)
        )
        tried.append(target)
        report["address_masked"] = mask_address(target)
        last_info = stack.bluetooth.async_last_service_info(
            hass, target, connectable=True
        )
        report["rssi"] = getattr(last_info, "rssi", None)
        seen_at = selection.seen_at.get(target, 0.0)
        attempt: dict[str, Any] = {
            "address_masked": mask_address(target),
            "seen_ago_s": round(time.monotonic() - seen_at, 1) if seen_at else None,
        }
        attempts.append(attempt)
        report["connection_attempts"] = attempts
        device = stack.bluetooth.async_ble_device_from_address(
            hass, target, connectable=True
        )
        if device is None:
            report["connection"] = attempt["result"] = "device_not_connectable"
            report.pop("error_class", None)
            continue
        client = await _async_connect_once(
            stack,
            device,
            target,
            report,
            budget=budget,
            max_attempts=1,
        )
        attempt["result"] = report["connection"]
        if "error_class" in report:
            attempt["error_class"] = report["error_class"]
        if client is not None:
            return client
    return None


async def async_probe_tracker(  # noqa: PLR0913
    hass: Any,
    keys: BeaconKeys,
    *,
    address: str | None,
    timeout: float,
    include_raw: bool = False,
    search_seconds: int = 0,
    search_eid: bytes | None = None,
) -> dict[str, Any]:
    """Find the tracker, connect and read its state, holding one connection.

    Radio outcomes are part of the response, because the counters are the diagnosis.
    An EID search runs only when ``search_seconds`` is set, after the link is closed,
    so it does not hold a proxy connection slot.
    """
    (
        bluetooth,
        client_class,
        establish_connection,
    ) = await hass.async_add_import_executor_job(_load_ble_modules)
    stack = _BleStack(bluetooth, client_class, establish_connection)

    report: dict[str, Any] = {
        "connection": "not_attempted",
        "address_masked": None,
        "rssi": None,
        "filter_checked": address is None,
        "frames_seen": 0,
        "frames_matched": 0,
        "frames_unparsed": 0,
    }
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    if address is not None:
        client = await _async_connect_address(hass, stack, address, report, timeout)
    elif keys.account_key is None:
        report["connection"] = "no_account_key_for_search"
        client = None
    else:
        client = await _async_connect_search(
            hass,
            stack,
            keys.account_key,
            report,
            deadline - min(READ_RESERVE_SECONDS, timeout / 3),
        )
    if client is None:
        if search_seconds > 0:
            report["eid_search"] = await _async_search(
                hass, keys, None, search_seconds, search_eid
            )
        return report

    reading: _Reading | None = None
    try:
        reading = await _read_state(
            client, keys, deadline=deadline, include_raw=include_raw
        )
        report.update(reading.report)
    except Exception as err:  # noqa: BLE001 - e.g. the characteristic is missing
        report["connection"] = "gatt_setup_failed"
        report["error_class"] = type(err).__name__
        _LOGGER.debug("Beacon GATT setup failed: %s", type(err).__name__)
    finally:
        try:
            async with asyncio.timeout(CLEANUP_TIMEOUT):
                await client.disconnect()
        except Exception as err:  # noqa: BLE001 - nothing left to do
            _LOGGER.debug("Beacon disconnect failed: %s", type(err).__name__)
    if search_seconds > 0:
        report["eid_search"] = await _async_search(
            hass, keys, reading, search_seconds, search_eid
        )
    return report


async def _async_search(
    hass: Any,
    keys: BeaconKeys,
    reading: _Reading | None,
    search_seconds: int,
    search_eid: bytes | None,
) -> dict[str, Any]:
    """Run the requested EID search, or say why there is nothing to search."""
    if keys.eik is None:
        return {"result": "no_eik"}
    if reading is None or reading.clock is None:
        return {"result": "no_clock"}
    targets = dict.fromkeys(reading.eids, "reported")
    if search_eid is not None:
        targets[search_eid] = "supplied"
    if not targets:
        return {"result": "no_target"}
    stop = threading.Event()
    try:
        result: dict[str, Any] = await hass.async_add_executor_job(
            search_eid_counter, keys.eik, reading.clock, targets, search_seconds, stop
        )
    except asyncio.CancelledError:
        stop.set()
        raise
    return result
