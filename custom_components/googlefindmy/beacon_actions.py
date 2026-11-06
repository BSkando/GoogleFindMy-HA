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
  before any GATT access, see ``build_request``.
- Nothing here runs unless the action is called: no timer, no listener, no entity.
  The only log calls are at DEBUG level.
- Keys never leave this module's call chain: not in logs, responses, exceptions or
  ``repr``. Bleak error texts carry the hardware address, so they are reduced to an
  ATT error code and the exception class.

Specification: https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import re
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
PARAMETERS_LENGTH = 16
RINGING_LENGTH = 3
PROVISIONING_LENGTHS = (1, 21, 33)
MIN_FAST_PAIR_FRAME_LENGTH = 2

# First byte forms of an account key. The stored key usually starts with 0x04; the
# advertised filter also uses 0x05 while the key is in use (measured on Pixel Buds
# Pro 2). Which form a Provider expects for HMAC and AES is tried in this order.
_KEY_FIRST_BYTES: tuple[tuple[str, int | None], ...] = (
    ("raw", None),
    ("first_byte_04", 0x04),
    ("first_byte_05", 0x05),
)

_ATT_CODE_PATTERN = re.compile(r"\berror[=: ]+(0x[0-9a-fA-F]{1,2}|\d{1,3})\b")


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
    try:
        checked = DataId(data_id)
    except ValueError as err:
        raise UnsupportedDataIdError("data ID is not in the read-only set") from err
    head = bytes((checked, AUTH_LENGTH))
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
    set, not the whole filter be equal.
    """
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


def select_candidates(infos: Iterable[Any], account_key: bytes) -> CandidateSelection:
    """Return matching advertisers, strongest first. Others are never connected to."""
    matched: list[tuple[int, str]] = []
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
            matched.append((int(info.rssi), str(info.address)))
    matched.sort(key=lambda item: item[0], reverse=True)
    return CandidateSelection(
        addresses=tuple(address for _rssi, address in matched),
        frames_seen=seen,
        frames_matched=len(matched),
        frames_unparsed=unparsed,
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


# ---------------------------------------------------------------------------
# GATT flow
# ---------------------------------------------------------------------------


def att_error_code(err: BaseException | None) -> int | None:
    """Return the ATT error code an exception carries, or ``None``.

    Over an ESPHome proxy the code sits in ``BluetoothGATTAPIError.error.error``,
    wrapped by ``BleakError(str(ex)) from ex``; the text is the fallback. The text
    itself stays here, it holds the hardware address.
    """
    seen: set[int] = set()
    current = err
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for candidate in (
            getattr(getattr(current, "error", None), "error", None),
            getattr(current, "error", None),
        ):
            if isinstance(candidate, int) and not isinstance(candidate, bool):
                return candidate
        match = _ATT_CODE_PATTERN.search(str(current))
        if match:
            return int(match.group(1), 0)
        current = current.__cause__ or current.__context__
    return None


def _status_from_att(code: int | None) -> OperationStatus:
    """Map an ATT error code to an operation status."""
    return {
        ATT_UNAUTHENTICATED: OperationStatus.UNAUTHENTICATED,
        ATT_INVALID_VALUE: OperationStatus.INVALID_VALUE,
        ATT_NO_USER_CONSENT: OperationStatus.NO_USER_CONSENT,
    }.get(code if code is not None else -1, OperationStatus.GATT_ERROR)


@dataclass
class _Operation:
    """Result of one operation; ``data`` holds verified additional data."""

    status: OperationStatus
    data: bytes = b""
    error_class: str | None = None
    att_error: int | None = None


@dataclass(repr=False)
class _Runs:
    """Outcomes of one connection, handed to the report builder."""

    parameters: _Operation
    parameters_key: bytes | None
    provisioning: _Operation
    ringing: _Operation
    key_form: str | None


class _BeaconSession:
    """One subscribed connection; routes notifications to the waiting operation."""

    def __init__(self, client: BeaconGattClient, timeout: float) -> None:
        self._client = client
        self._timeout = timeout
        self._waiting: tuple[int, asyncio.Future[bytes]] | None = None

    def on_notify(self, _sender: Any, data: bytearray) -> None:
        """Hand a notification with the expected data ID to the waiting operation."""
        if self._waiting is None or not data or data[0] != self._waiting[0]:
            return
        if not self._waiting[1].done():
            self._waiting[1].set_result(bytes(data))

    async def run(self, data_id: DataId, key: bytes) -> _Operation:
        """Read a fresh nonce, send one request and verify its notification."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        self._waiting = (data_id, future)
        try:
            async with asyncio.timeout(self._timeout):
                version, nonce = parse_nonce(
                    await self._client.read_gatt_char(BEACON_ACTIONS_UUID)
                )
                # The Provider notifies before the write completes, so the future
                # exists before the write goes out.
                await self._client.write_gatt_char(
                    BEACON_ACTIONS_UUID,
                    build_request(data_id, key, version, nonce),
                    response=True,
                )
                frame = await future
            return _Operation(
                OperationStatus.OK,
                verify_response(frame, data_id, key, version, nonce),
            )
        except TimeoutError:
            return _Operation(OperationStatus.TIMEOUT)
        except _ResponseAuthError:
            return _Operation(OperationStatus.RESPONSE_AUTH_INVALID)
        except BeaconProtocolError:
            return _Operation(OperationStatus.INVALID_RESPONSE)
        except UnsupportedDataIdError:
            raise
        except Exception as err:  # noqa: BLE001 - the backend's error types vary
            code = att_error_code(err)
            _LOGGER.debug(
                "Beacon operation 0x%02x failed: %s, ATT error %s",
                data_id,
                type(err).__name__,
                code,
            )
            return _Operation(_status_from_att(code), b"", type(err).__name__, code)
        finally:
            self._waiting = None
            if not future.done():
                future.cancel()


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
    client: BeaconGattClient, keys: BeaconKeys, *, operation_timeout: float = 10.0
) -> dict[str, Any]:
    """Run ``0x00``, ``0x01``, ``0x06`` on one connection and report the results.

    ``eik_matches`` is ``True`` only after a verified ``0x06`` response and ``False``
    only after an explicit unauthenticated error. That error also means "not
    provisioned", so ``eik_set`` belongs next to it. Everything else is ``None``:
    a timeout says nothing about the key.
    """
    session = _BeaconSession(client, operation_timeout)
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
        ),
    )


def _build_report(keys: BeaconKeys, runs: _Runs) -> dict[str, Any]:
    """Build the response mapping; the crypto parts run off the event loop."""
    parameters, parameters_key = runs.parameters, runs.parameters_key
    provisioning, ringing = runs.provisioning, runs.ringing
    report_parameters = _operation_report(parameters)
    clock: int | None = None
    if parameters.status is OperationStatus.OK and parameters_key is not None:
        try:
            values = decrypt_parameters(parameters_key, parameters.data)
        except BeaconProtocolError:
            report_parameters["status"] = OperationStatus.INVALID_RESPONSE.value
        else:
            report_parameters.update(values)
            clock = values["clock"]

    report_provisioning = _operation_report(provisioning)
    eik_set: bool | None = None
    is_owner: bool | None = None
    reported_eid = b""
    if provisioning.status is OperationStatus.OK:
        try:
            eik_set, is_owner, reported_eid = parse_provisioning(provisioning.data)
        except BeaconProtocolError:
            report_provisioning["status"] = OperationStatus.INVALID_RESPONSE.value
        else:
            report_provisioning["eik_set"] = eik_set
            report_provisioning["is_owner"] = is_owner
            report_provisioning["eid_prefix"] = reported_eid[:4].hex()
            report_provisioning["eid_length"] = len(reported_eid)

    report_ringing = _operation_report(ringing)
    eik_matches: bool | None = None
    if ringing.status is OperationStatus.OK:
        try:
            report_ringing.update(parse_ringing(ringing.data))
            eik_matches = True
        except BeaconProtocolError:
            report_ringing["status"] = OperationStatus.INVALID_RESPONSE.value
    elif ringing.status is OperationStatus.UNAUTHENTICATED:
        eik_matches = False

    eid_check: dict[str, Any] = {"result": "not_checked"}
    if keys.eik and clock is not None and reported_eid:
        eid_check = compare_eid(keys.eik, clock, reported_eid)

    return {
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


async def async_probe_tracker(
    hass: Any,
    keys: BeaconKeys,
    *,
    address: str | None,
    timeout: float,
) -> dict[str, Any]:
    """Find the tracker, connect once and read its state.

    Radio outcomes are part of the response, because the counters are the diagnosis.
    """
    (
        bluetooth,
        client_class,
        establish_connection,
    ) = await hass.async_add_import_executor_job(_load_ble_modules)

    report: dict[str, Any] = {
        "connection": "not_attempted",
        "address_masked": None,
        "rssi": None,
        "filter_checked": address is None,
        "frames_seen": 0,
        "frames_matched": 0,
        "frames_unparsed": 0,
    }
    target = address
    if target is None:
        if keys.account_key is None:
            report["connection"] = "no_account_key_for_search"
            return report
        selection = await asyncio.to_thread(
            select_candidates,
            list(bluetooth.async_discovered_service_info(hass, connectable=True)),
            keys.account_key,
        )
        report["frames_seen"] = selection.frames_seen
        report["frames_matched"] = selection.frames_matched
        report["frames_unparsed"] = selection.frames_unparsed
        if not selection.addresses:
            report["connection"] = "no_matching_frame"
            return report
        target = selection.addresses[0]

    report["address_masked"] = mask_address(target)
    device = bluetooth.async_ble_device_from_address(hass, target, connectable=True)
    if device is None:
        report["connection"] = "device_not_connectable"
        return report
    last_info = bluetooth.async_last_service_info(hass, target, connectable=True)
    report["rssi"] = getattr(last_info, "rssi", None)

    client: Any = None
    try:
        async with asyncio.timeout(timeout):
            client = await establish_connection(
                client_class,
                device,
                mask_address(target),
                max_attempts=2,
            )
            report["connection"] = "connected"
            report.update(await read_beacon_state(client, keys))
    except TimeoutError:
        report["connection"] = (
            "timeout"
            if report["connection"] == "not_attempted"
            else "operation_timeout"
        )
    except Exception as err:  # noqa: BLE001 - the backend's error types vary
        report["connection"] = "connection_failed"
        report["error_class"] = type(err).__name__
        _LOGGER.debug("Beacon connection failed: %s", type(err).__name__)
    finally:
        if client is not None:
            try:
                await client.disconnect()
            except Exception as err:  # noqa: BLE001 - nothing left to do
                _LOGGER.debug("Beacon disconnect failed: %s", type(err).__name__)
    return report
