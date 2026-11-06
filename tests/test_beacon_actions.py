# tests/test_beacon_actions.py
"""Tests for the read-only Beacon Actions protocol layer.

The expected values are computed here with ``hmac`` and ``hashlib``, independently of
the module. All keys are invented for these tests.
"""

from __future__ import annotations

import ast
import asyncio
import hashlib
import hmac
import logging
import secrets
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from custom_components.googlefindmy import beacon_actions as ba
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    EidVariant,
    generate_eid_variant,
)

ACCOUNT_KEY = bytes.fromhex("04112233445566778899aabbccddeeff")
EIK = bytes(range(32))
CLOCK = 1_000_000
MODULE_PATH = Path(ba.__file__)


def _sign(key: bytes, *parts: bytes) -> bytes:
    return hmac.new(key, b"".join(parts), hashlib.sha256).digest()[:8]


class ProviderError(Exception):
    """Mimics ``BleakError(...) from BluetoothGATTAPIError``."""

    def __init__(self, code: int) -> None:
        super().__init__(
            f"Bluetooth GATT Error address=AA:BB:CC:DD:EE:FF handle=1 error={code}"
        )


class FakeProvider:
    """Spec-conforming Provider: one-time nonce, notifies before the write completes."""

    def __init__(
        self,
        *,
        account_key: bytes = ACCOUNT_KEY,
        eik: bytes | None = EIK,
        owner: bool = True,
        eid_variant: EidVariant = EidVariant.LEGACY_SECP160R1_X20_BE,
        eid_clock: int = CLOCK,
    ) -> None:
        self.account_key = account_key
        self.eik = eik
        self.owner = owner
        self.eid_variant = eid_variant
        self.eid_clock = eid_clock
        self.nonce: bytes | None = None
        self.calls: list[tuple[str, Any]] = []
        self.callback: Callable[[Any, bytearray], Any] | None = None
        self.corrupt_response = False
        self.silent_ids: set[int] = set()
        self.foreign_notify_first = False

    async def start_notify(self, uuid: str, callback: Any) -> None:
        self.calls.append(("start_notify", uuid))
        self.callback = callback

    async def stop_notify(self, uuid: str) -> None:
        self.calls.append(("stop_notify", uuid))

    async def read_gatt_char(self, uuid: str) -> bytes:
        self.calls.append(("read", uuid))
        self.nonce = secrets.token_bytes(8)
        return b"\x01" + self.nonce

    async def write_gatt_char(
        self, uuid: str, data: bytes, response: bool = True
    ) -> None:
        self.calls.append(("write", bytes(data)))
        assert response is True
        nonce, self.nonce = self.nonce, None
        if nonce is None or data[1] != 8 or len(data) != 10:
            raise ProviderError(0x80)
        data_id = data[0]
        version = b"\x01"
        if data_id == 0x06:
            if self.eik is None:
                raise ProviderError(0x80)
            key = hashlib.sha256(self.eik + b"\x02").digest()[:8]
        else:
            key = self.account_key
        if not hmac.compare_digest(
            _sign(key, version, nonce, bytes(data[:2])), bytes(data[2:])
        ):
            raise ProviderError(0x80)
        if data_id in self.silent_ids:
            return
        additional = self._additional(data_id)
        head = bytes((data_id, 8 + len(additional)))
        auth = _sign(key, version, nonce, head, additional, b"\x01")
        if self.corrupt_response:
            auth = bytes(8)
        assert self.callback is not None
        if self.foreign_notify_first:
            self.callback(None, bytearray(b"\x05\x08" + bytes(8)))
        self.callback(None, bytearray(head + auth + additional))

    def _additional(self, data_id: int) -> bytes:
        if data_id == 0x00:
            plain = (
                bytes((0xF6,)) + CLOCK.to_bytes(4, "big") + b"\x00\x02\x01" + bytes(8)
            )
            enc = Cipher(algorithms.AES(self.account_key), modes.ECB()).encryptor()  # noqa: S305
            return enc.update(plain) + enc.finalize()
        if data_id == 0x01:
            bits = (0x01 if self.eik else 0) | (0x02 if self.owner else 0)
            eid = (
                generate_eid_variant(self.eik, self.eid_clock, self.eid_variant)
                if self.eik
                else b""
            )
            return bytes((bits,)) + eid
        return b"\x00\x00\x00"


async def _run(
    provider: FakeProvider, keys: ba.BeaconKeys, **kwargs: Any
) -> dict[str, Any]:
    return await ba.read_beacon_state(provider, keys, **kwargs)


# --- closed data ID set -------------------------------------------------------------


@pytest.mark.parametrize("data_id", [i for i in range(256) if i not in (0, 1, 6)])
def test_build_request_rejects_everything_outside_the_read_only_set(
    data_id: int,
) -> None:
    with pytest.raises(ba.UnsupportedDataIdError):
        ba.build_request(data_id, ACCOUNT_KEY, 1, bytes(8))


@pytest.mark.asyncio
async def test_session_rejects_a_foreign_data_id_before_any_gatt_access() -> None:
    provider = FakeProvider()

    session = ba._BeaconSession(provider, 1.0)
    with pytest.raises(ba.UnsupportedDataIdError):
        await session.run(0x05, ACCOUNT_KEY)
    assert provider.calls == []


def test_data_id_enum_holds_exactly_the_three_read_operations() -> None:
    assert sorted(int(item) for item in ba.DataId) == [0x00, 0x01, 0x06]


# --- codec ---------------------------------------------------------------------------


def test_request_authentication_matches_the_specification() -> None:
    nonce = bytes(range(8))
    frame = ba.build_request(0x01, ACCOUNT_KEY, 1, nonce)
    assert frame[:2] == b"\x01\x08"
    assert frame[2:] == _sign(ACCOUNT_KEY, b"\x01", nonce, b"\x01\x08")


def test_ring_key_is_the_truncated_hash_of_eik_and_two() -> None:
    assert ba.ring_key(EIK) == hashlib.sha256(EIK + b"\x02").digest()[:8]


@pytest.mark.parametrize("frame", [b"", b"\x02\x08" + bytes(8)])
def test_short_or_foreign_response_is_a_protocol_error(frame: bytes) -> None:
    with pytest.raises(ba.BeaconProtocolError):
        ba.verify_response(frame, 0x01, ACCOUNT_KEY, 1, bytes(8))


def test_response_with_wrong_length_field_is_a_protocol_error() -> None:
    frame = b"\x01\x09" + bytes(8)
    with pytest.raises(ba.BeaconProtocolError):
        ba.verify_response(frame, 0x01, ACCOUNT_KEY, 1, bytes(8))


@pytest.mark.parametrize("value", [b"", b"\x01", bytes(10)])
def test_nonce_frame_with_wrong_length_is_rejected(value: bytes) -> None:
    with pytest.raises(ba.BeaconProtocolError):
        ba.parse_nonce(value)


# --- flow ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_matching_provider_reports_all_fields() -> None:
    report = await _run(FakeProvider(), ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_set"] is True
    assert report["is_owner"] is True
    assert report["eik_matches"] is True
    assert report["key_form"] == "raw"
    assert report["eid_check"]["result"] == "match"
    assert report["eid_check"]["variant"] == EidVariant.LEGACY_SECP160R1_X20_BE.value
    parameters = report["operations"]["read_beacon_parameters"]
    assert parameters["clock"] == CLOCK
    assert parameters["calibrated_power"] == -10
    assert parameters["curve"] == 0
    assert parameters["components"] == 2
    assert report["operations"]["read_provisioning_state"]["eid_length"] == 20


@pytest.mark.asyncio
async def test_each_operation_uses_a_fresh_nonce() -> None:
    provider = FakeProvider()
    await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    kinds = [kind for kind, _ in provider.calls if kind in ("read", "write")]
    assert kinds == ["read", "write"] * 3


@pytest.mark.asyncio
async def test_foreign_eik_is_false_and_keeps_the_provisioning_bit() -> None:
    report = await _run(FakeProvider(), ba.BeaconKeys(ACCOUNT_KEY, bytes(32)))
    assert report["eik_matches"] is False
    assert report["eik_set"] is True
    assert report["operations"]["read_ringing_state"]["att_error"] == 0x80
    assert report["eid_check"]["result"] == "no_match"
    assert report["eid_check"]["variants_checked"] == 3 * len(EidVariant)


@pytest.mark.asyncio
async def test_unprovisioned_provider_is_false_with_eik_bit_off() -> None:
    report = await _run(FakeProvider(eik=None), ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_matches"] is False
    assert report["eik_set"] is False


@pytest.mark.asyncio
async def test_timeout_never_claims_a_foreign_key() -> None:
    provider = FakeProvider()
    provider.silent_ids = {0x06}
    report = await _run(
        provider, ba.BeaconKeys(ACCOUNT_KEY, EIK), operation_timeout=0.01
    )
    assert report["eik_matches"] is None
    assert report["operations"]["read_ringing_state"]["status"] == "timeout"


@pytest.mark.asyncio
async def test_transport_error_never_claims_a_foreign_key() -> None:
    provider = FakeProvider()
    original = provider.write_gatt_char

    async def failing(uuid: str, data: bytes, response: bool = True) -> None:
        if data[0] == 0x06:
            raise ProviderError(0x85)
        await original(uuid, data, response)

    provider.write_gatt_char = failing  # type: ignore[method-assign]
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_matches"] is None
    assert report["operations"]["read_ringing_state"]["status"] == "gatt_error"


@pytest.mark.asyncio
async def test_manipulated_response_is_not_accepted() -> None:
    provider = FakeProvider()
    provider.corrupt_response = True
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_matches"] is None
    assert report["eik_set"] is None
    for operation in report["operations"].values():
        assert operation["status"] == "response_auth_invalid"


@pytest.mark.asyncio
async def test_foreign_notification_does_not_answer_the_operation() -> None:
    provider = FakeProvider()
    provider.foreign_notify_first = True
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_matches"] is True


@pytest.mark.asyncio
async def test_rejected_key_form_moves_on_with_a_fresh_nonce() -> None:
    stored = b"\x05" + ACCOUNT_KEY[1:]
    provider = FakeProvider(account_key=stored)
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["key_form"] == "first_byte_05"
    assert report["operations"]["read_beacon_parameters"]["clock"] == CLOCK
    assert report["is_owner"] is True


@pytest.mark.asyncio
async def test_account_key_rejected_in_every_form_still_runs_the_ring_check() -> None:
    provider = FakeProvider(account_key=bytes.fromhex("04" + "ee" * 15))
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["key_form"] is None
    assert report["is_owner"] is None
    assert report["eik_matches"] is True


@pytest.mark.asyncio
async def test_missing_keys_skip_their_operations() -> None:
    report = await _run(FakeProvider(), ba.BeaconKeys(None, None))
    assert {o["status"] for o in report["operations"].values()} == {"skipped_no_key"}
    assert report["eik_matches"] is None


@pytest.mark.asyncio
async def test_previous_window_eid_matches_with_its_offset() -> None:
    provider = FakeProvider(eid_clock=CLOCK - ba.ROTATION_SECONDS)
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eid_check"]["result"] == "match"
    assert report["eid_check"]["window_offset"] == -1


@pytest.mark.asyncio
async def test_stop_notify_failure_does_not_lose_the_report() -> None:
    provider = FakeProvider()

    async def failing(uuid: str) -> None:
        raise ProviderError(0x85)

    provider.stop_notify = failing  # type: ignore[method-assign]
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_matches"] is True


# --- ATT error extraction -------------------------------------------------------------


def test_att_code_comes_from_the_wrapped_proxy_error() -> None:
    inner = Exception("x")
    inner.error = SimpleNamespace(error=0x80)  # type: ignore[attr-defined]
    outer = Exception("wrapped")
    outer.__cause__ = inner
    assert ba.att_error_code(outer) == 0x80


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("handle=3 error=128 description=x", 128),
        ("ATT error: 0x80", 0x80),
        ("nothing", None),
        ("ble error: 128 bytes short", None),
        ("error 007", None),
        ("error=09x", None),
    ],
)
def test_att_code_falls_back_to_the_text(text: str, code: int | None) -> None:
    assert ba.att_error_code(Exception(text)) == code


# --- Fast Pair frames ------------------------------------------------------------------

MEASURED_FRAME = bytes.fromhex("1040003183142 1bcac194cf56eca".replace(" ", ""))


def _frame_for(key: bytes) -> bytes:
    salt = b"\xbc\xac"
    trailer = bytes.fromhex("194cf56eca")
    digest = hashlib.sha256(key + salt + trailer).digest()
    bits = bytearray(4)
    for i in range(8):
        value = int.from_bytes(digest[4 * i : 4 * i + 4], "big") % 32
        bits[value // 8] |= 1 << (value % 8)
    return b"\x10\x40" + bytes(bits) + b"\x21" + salt + trailer


def test_measured_frame_parses_into_its_fields() -> None:
    frame = ba.parse_fast_pair_frame(MEASURED_FRAME)
    assert frame is not None
    assert frame.account_filter == bytes.fromhex("00318314")
    assert frame.salt == bytes.fromhex("bcac")
    assert frame.trailer == bytes.fromhex("194cf56eca")


def test_frame_truncated_in_the_salt_is_rejected() -> None:
    assert ba.parse_fast_pair_frame(bytes.fromhex("10400031831421bc")) is None


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"\x10",
        b"\x10\x40\x00",
        bytes.fromhex("1033aabbcc"),
        bytes(14),
        bytes.fromhex("104000318314"),
        bytes.fromhex("10400031831400bcac"),
    ],
)
def test_other_frame_forms_are_not_guessed_at(raw: bytes) -> None:
    assert ba.parse_fast_pair_frame(raw) is None


def test_filter_accepts_all_key_forms_and_rejects_foreign_keys() -> None:
    for first in (0x04, 0x05):
        key = bytes((first,)) + ACCOUNT_KEY[1:]
        frame = ba.parse_fast_pair_frame(_frame_for(key))
        assert frame is not None
        assert ba.filter_matches(frame, ACCOUNT_KEY)
    frame = ba.parse_fast_pair_frame(_frame_for(ACCOUNT_KEY))
    assert frame is not None
    assert not ba.filter_matches(frame, bytes.fromhex("04" + "77" * 15))


def test_filter_holding_a_second_key_still_matches() -> None:
    raw = bytearray(_frame_for(ACCOUNT_KEY))
    other = _frame_for(bytes.fromhex("04" + "77" * 15))
    raw[2:6] = bytes(a | b for a, b in zip(raw[2:6], other[2:6], strict=True))
    frame = ba.parse_fast_pair_frame(bytes(raw))
    assert frame is not None
    assert ba.filter_matches(frame, ACCOUNT_KEY)


def _info(address: str, rssi: int, data: bytes | None) -> SimpleNamespace:
    service_data = {} if data is None else {ba.FAST_PAIR_SERVICE_DATA_UUID: data}
    return SimpleNamespace(address=address, rssi=rssi, service_data=service_data)


def test_selection_orders_by_signal_and_drops_foreign_frames() -> None:
    mine = _frame_for(ACCOUNT_KEY)
    foreign = _frame_for(bytes.fromhex("04" + "77" * 15))
    selection = ba.select_candidates(
        [
            _info("AA:AA:AA:AA:AA:01", -80, mine),
            _info("AA:AA:AA:AA:AA:02", -50, mine),
            _info("AA:AA:AA:AA:AA:03", -40, foreign),
            _info("AA:AA:AA:AA:AA:04", -40, b"\x00"),
            _info("AA:AA:AA:AA:AA:05", -40, None),
        ],
        ACCOUNT_KEY,
    )
    assert selection.addresses == ("AA:AA:AA:AA:AA:02", "AA:AA:AA:AA:AA:01")
    assert selection.frames_seen == 4
    assert selection.frames_matched == 2
    assert selection.frames_unparsed == 1


# --- secrecy and silence ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_key_material_stays_out_of_repr_report_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    keys = ba.BeaconKeys(ACCOUNT_KEY, EIK)
    for secret in (ACCOUNT_KEY, EIK):
        assert repr(secret) not in repr(keys)
        assert secret.hex() not in repr(keys)
    with caplog.at_level(logging.DEBUG, logger=ba.__name__):
        report = await _run(FakeProvider(), keys)
        await _run(FakeProvider(account_key=bytes.fromhex("04" + "ee" * 15)), keys)
    text = f"{report!r} {caplog.text}"
    for secret in (ACCOUNT_KEY, EIK):
        assert repr(secret) not in text
        assert secret.hex() not in text
    assert "AA:BB:CC:DD:EE:FF" not in text


@pytest.mark.asyncio
async def test_a_run_writes_nothing_at_info_or_above(
    caplog: pytest.LogCaptureFixture,
) -> None:
    provider = FakeProvider()
    provider.silent_ids = {0x06}
    with caplog.at_level(logging.DEBUG, logger=ba.__name__):
        await _run(
            provider, ba.BeaconKeys(ACCOUNT_KEY, bytes(32)), operation_timeout=0.01
        )
    assert [r for r in caplog.records if r.levelno >= logging.INFO] == []


@pytest.mark.asyncio
async def test_report_is_json_serializable() -> None:
    import json

    json.dumps(await _run(FakeProvider(), ba.BeaconKeys(ACCOUNT_KEY, EIK)))


def test_mask_address_keeps_only_the_tail() -> None:
    assert ba.mask_address("AA:BB:CC:DD:EE:FF") == "...E:FF"


def test_module_stays_silent_and_light_at_import() -> None:
    tree = ast.parse(MODULE_PATH.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Import | ast.ImportFrom):
            names = [getattr(node, "module", "") or ""] + [a.name for a in node.names]
            assert not any(
                n.startswith(("homeassistant", "bleak", "cryptography")) for n in names
            )
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        receiver = node.func.value
        if isinstance(receiver, ast.Name) and receiver.id == "_LOGGER":
            assert node.func.attr == "debug"
    assert "print(" not in MODULE_PATH.read_text(encoding="utf-8")


# --- connection layer -------------------------------------------------------------------


class _Bluetooth:
    """Double of ``homeassistant.components.bluetooth``."""

    def __init__(self, infos: list[SimpleNamespace], device: object = "device") -> None:
        self.infos = infos
        self.device = device
        self.addresses: list[str] = []

    def async_discovered_service_info(
        self, _hass: Any, connectable: bool = True
    ) -> list[SimpleNamespace]:
        assert connectable is True
        return self.infos

    def async_ble_device_from_address(
        self, _hass: Any, address: str, connectable: bool = True
    ) -> object:
        assert connectable is True
        self.addresses.append(address)
        return self.device

    def async_last_service_info(
        self, _hass: Any, _address: str, connectable: bool = True
    ) -> SimpleNamespace:
        assert connectable is True
        return SimpleNamespace(rssi=-55)


class _ConnectedProvider(FakeProvider):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.disconnects = 0

    async def disconnect(self) -> None:
        self.disconnects += 1
        if self.corrupt_response:
            raise ProviderError(0x85)


class _Hass:
    """Runs import jobs inline; the job's own import is covered separately."""

    async def async_add_import_executor_job(self, target: Any) -> Any:
        return target()

    async def async_add_executor_job(self, target: Any, *args: Any) -> Any:
        return target(*args)


def _install(
    monkeypatch: pytest.MonkeyPatch, bluetooth: _Bluetooth, connect: Any
) -> None:
    monkeypatch.setattr(ba, "_load_ble_modules", lambda: (bluetooth, object, connect))


def test_ble_modules_load_through_their_public_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from homeassistant import components

    bluetooth = object()
    connector = SimpleNamespace(
        BleakClientWithServiceCache=object, establish_connection=print
    )
    monkeypatch.setattr(components, "bluetooth", bluetooth, raising=False)
    monkeypatch.setitem(sys.modules, "homeassistant.components.bluetooth", bluetooth)
    monkeypatch.setitem(sys.modules, "bleak_retry_connector", connector)
    assert ba._load_ble_modules() == (bluetooth, object, print)


@pytest.mark.asyncio
async def test_probe_connects_to_the_strongest_match_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()
    connects: list[object] = []

    async def connect(_cls: Any, device: object, name: str, **_kw: Any) -> FakeProvider:
        connects.append(device)
        assert name == "...A:02"
        return provider

    bluetooth = _Bluetooth(
        [
            _info("AA:AA:AA:AA:AA:01", -80, _frame_for(ACCOUNT_KEY)),
            _info("AA:AA:AA:AA:AA:02", -50, _frame_for(ACCOUNT_KEY)),
            _info(
                "AA:AA:AA:AA:AA:03", -40, _frame_for(bytes.fromhex("04" + "77" * 15))
            ),
        ]
    )
    _install(monkeypatch, bluetooth, connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["connection"] == "connected"
    assert report["address_masked"] == "...A:02"
    assert report["filter_checked"] is True
    assert report["frames_matched"] == 2
    assert report["rssi"] == -55
    assert report["eik_matches"] is True
    assert bluetooth.addresses == ["AA:AA:AA:AA:AA:02"]
    assert len(connects) == 1
    assert provider.disconnects == 1
    assert "AA:AA:AA:AA:AA:02" not in repr(report)


@pytest.mark.asyncio
async def test_probe_without_a_matching_frame_never_connects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*_args: Any, **_kw: Any) -> None:
        raise AssertionError("must not connect")

    bluetooth = _Bluetooth(
        [_info("AA:AA:AA:AA:AA:03", -40, _frame_for(bytes.fromhex("04" + "77" * 15)))]
    )
    _install(monkeypatch, bluetooth, connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["connection"] == "no_matching_frame"
    assert report["frames_seen"] == 1
    assert report["address_masked"] is None


@pytest.mark.asyncio
async def test_probe_search_needs_an_account_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, _Bluetooth([]), None)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(None, EIK), address=None, timeout=5
    )
    assert report["connection"] == "no_account_key_for_search"


@pytest.mark.asyncio
async def test_probe_with_address_skips_the_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()
    provider.corrupt_response = True  # disconnect failure must not lose the report

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
    )
    assert report["filter_checked"] is False
    assert report["address_masked"] == "...E:FF"
    assert report["frames_seen"] == 0
    assert provider.disconnects == 1


@pytest.mark.asyncio
async def test_probe_reports_an_address_without_a_connectable_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, _Bluetooth([], device=None), None)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
    )
    assert report["connection"] == "device_not_connectable"


@pytest.mark.asyncio
async def test_probe_reports_connection_errors_without_their_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*_args: Any, **_kw: Any) -> None:
        raise ProviderError(0x85)

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
    )
    assert report["connection"] == "connection_failed"
    assert report["error_class"] == "ProviderError"
    assert "AA:BB:CC" not in repr(report)


@pytest.mark.asyncio
async def test_probe_reports_a_connection_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*_args: Any, **_kw: Any) -> None:
        raise TimeoutError

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address="AA:BB:CC:DD:EE:FF", timeout=5
    )
    assert report["connection"] == "timeout"


# --- malformed responses ----------------------------------------------------------------


@pytest.mark.parametrize(
    "parser",
    [
        lambda: ba.decrypt_parameters(ACCOUNT_KEY, bytes(15)),
        lambda: ba.parse_provisioning(bytes(2)),
        lambda: ba.parse_ringing(bytes(2)),
    ],
)
def test_wrong_data_lengths_are_protocol_errors(parser: Callable[[], Any]) -> None:
    with pytest.raises(ba.BeaconProtocolError):
        parser()


@pytest.mark.parametrize(
    "operation",
    [
        ba._Runs(
            parameters=ba._Operation(ba.OperationStatus.OK, bytes(3)),
            parameters_key=ACCOUNT_KEY,
            provisioning=ba._Operation(ba.OperationStatus.NOT_ATTEMPTED),
            ringing=ba._Operation(ba.OperationStatus.NOT_ATTEMPTED),
            key_form="raw",
        ),
        ba._Runs(
            parameters=ba._Operation(ba.OperationStatus.NOT_ATTEMPTED),
            parameters_key=None,
            provisioning=ba._Operation(ba.OperationStatus.OK, bytes(3)),
            ringing=ba._Operation(ba.OperationStatus.NOT_ATTEMPTED),
            key_form="raw",
        ),
        ba._Runs(
            parameters=ba._Operation(ba.OperationStatus.NOT_ATTEMPTED),
            parameters_key=None,
            provisioning=ba._Operation(ba.OperationStatus.NOT_ATTEMPTED),
            ringing=ba._Operation(ba.OperationStatus.OK, bytes(1)),
            key_form=None,
        ),
    ],
)
def test_malformed_verified_data_is_reported_not_raised(operation: ba._Runs) -> None:
    report = ba._build_report(ba.BeaconKeys(ACCOUNT_KEY, EIK), operation).report
    assert "invalid_response" in {o["status"] for o in report["operations"].values()}
    assert report["eik_matches"] is None


def test_counters_outside_the_range_are_skipped() -> None:
    report = ba.compare_eid(EIK, 0, b"x" * 20)
    assert report["result"] == "no_match"
    assert report["variants_checked"] == 2 * len(EidVariant)


def test_undecodable_variant_is_counted_not_raised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from custom_components.googlefindmy.FMDNCrypto import eid_generator

    def _raise(*_args: Any, **_kw: Any) -> bytes:
        raise ValueError

    monkeypatch.setattr(eid_generator, "generate_eid_variant", _raise)
    assert ba.compare_eid(EIK, CLOCK, b"x" * 20)["result"] == "no_match"


@pytest.mark.asyncio
async def test_invalid_nonce_frame_is_an_invalid_response() -> None:
    provider = FakeProvider()

    async def short(_uuid: str) -> bytes:
        return b"\x01"

    provider.read_gatt_char = short  # type: ignore[method-assign]
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["operations"]["read_ringing_state"]["status"] == "invalid_response"


def test_notification_without_a_waiting_operation_is_ignored() -> None:
    session = ba._BeaconSession(FakeProvider(), 1.0)
    session.on_notify(None, bytearray(b"\x01"))


@pytest.mark.asyncio
async def test_second_notification_for_an_answered_operation_is_ignored() -> None:
    session = ba._BeaconSession(FakeProvider(), 1.0)
    future = asyncio.get_running_loop().create_future()
    session._waiting = (0x01, future)
    session.on_notify(None, bytearray(b"\x01first"))
    session.on_notify(None, bytearray(b"\x01second"))
    assert future.result() == b"\x01first"


# --- review findings -------------------------------------------------------------------


def test_att_code_ignores_a_flat_error_attribute_and_foreign_context() -> None:
    flat = Exception("x")
    flat.error = 128  # type: ignore[attr-defined]
    assert ba.att_error_code(flat) is None
    outer = Exception("y")
    outer.__context__ = Exception("handle=1 error=128")
    assert ba.att_error_code(outer) is None


def test_att_code_survives_a_hostile_exception() -> None:
    class Hostile(Exception):
        def __str__(self) -> str:
            raise RuntimeError("boom")

    assert ba.att_error_code(Hostile()) is None


@pytest.mark.asyncio
async def test_rejection_on_the_nonce_read_is_not_a_key_verdict() -> None:
    provider = FakeProvider()

    async def failing(_uuid: str) -> bytes:
        raise ProviderError(0x80)

    provider.read_gatt_char = failing  # type: ignore[method-assign]
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_matches"] is None
    for operation in report["operations"].values():
        assert operation["status"] == "gatt_error"
        assert operation["att_error"] == 0x80


@pytest.mark.asyncio
async def test_timeout_on_the_nonce_read_is_a_timeout() -> None:
    provider = FakeProvider()

    async def failing(_uuid: str) -> bytes:
        raise TimeoutError

    provider.read_gatt_char = failing  # type: ignore[method-assign]
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["operations"]["read_ringing_state"]["status"] == "timeout"


@pytest.mark.asyncio
async def test_all_writes_rejected_is_no_verdict_on_the_eik() -> None:
    """0x80 can be ESP_GATT_NO_RESOURCES over a proxy: without a verified 0x01 it decides nothing."""
    provider = FakeProvider()

    async def failing(_uuid: str, _data: bytes, response: bool = True) -> None:
        raise ProviderError(0x80)

    provider.write_gatt_char = failing  # type: ignore[method-assign]
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eik_set"] is None
    assert report["eik_matches"] is None
    assert report["operations"]["read_ringing_state"]["status"] == "unauthenticated"


def test_saturated_filter_matches_nothing() -> None:
    frame = ba.FastPairFrame(b"\xff\xff\xff\xff", b"\xbc\xac", b"")
    assert not ba.filter_matches(frame, ACCOUNT_KEY)


@pytest.mark.asyncio
async def test_deadline_keeps_what_was_verified_before() -> None:
    provider = FakeProvider()
    provider.silent_ids = {0x01}
    loop = asyncio.get_running_loop()
    report = await ba.read_beacon_state(
        provider,
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        operation_timeout=10.0,
        deadline=loop.time() + 0.05,
    )
    assert report["operations"]["read_beacon_parameters"]["clock"] == CLOCK
    assert report["operations"]["read_provisioning_state"]["status"] == "timeout"
    assert report["operations"]["read_ringing_state"]["status"] == "not_attempted"
    assert report["eik_matches"] is None


@pytest.mark.asyncio
async def test_hanging_cleanup_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ba, "CLEANUP_TIMEOUT", 0.01)
    provider = _ConnectedProvider()

    async def hanging(*_args: Any) -> None:
        await asyncio.Event().wait()

    provider.stop_notify = hanging  # type: ignore[method-assign]
    provider.disconnect = hanging  # type: ignore[method-assign]

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(monkeypatch, _Bluetooth([]), connect)
    async with asyncio.timeout(5):
        report = await ba.async_probe_tracker(
            _Hass(),
            ba.BeaconKeys(ACCOUNT_KEY, EIK),
            address="AA:BB:CC:DD:EE:FF",
            timeout=5,
        )
    assert report["eik_matches"] is True


@pytest.mark.asyncio
async def test_gatt_setup_failure_keeps_the_connection_phase_apart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()

    async def failing(*_args: Any) -> None:
        raise ProviderError(0x85)

    provider.start_notify = failing  # type: ignore[method-assign]

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address="AA:BB:CC:DD:EE:FF", timeout=5
    )
    assert report["connection"] == "gatt_setup_failed"
    assert report["error_class"] == "ProviderError"
    assert provider.disconnects == 1


@pytest.mark.asyncio
async def test_hanging_subscription_is_bounded_by_the_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()

    async def hanging(*_args: Any) -> None:
        await asyncio.Event().wait()

    provider.start_notify = hanging  # type: ignore[method-assign]

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(monkeypatch, _Bluetooth([]), connect)
    async with asyncio.timeout(5):
        report = await ba.async_probe_tracker(
            _Hass(),
            ba.BeaconKeys(ACCOUNT_KEY, EIK),
            address="AA:BB:CC:DD:EE:FF",
            timeout=0.01,
        )
    assert report["connection"] == "gatt_setup_failed"
    assert report["error_class"] == "TimeoutError"
    assert provider.disconnects == 1


# --- address choice -----------------------------------------------------------------


def _timed_info(address: str, rssi: int, seen: float) -> SimpleNamespace:
    info = _info(address, rssi, _frame_for(ACCOUNT_KEY))
    info.time = seen
    return info


def test_newest_sighting_beats_the_stronger_old_address() -> None:
    selection = ba.select_candidates(
        [
            _timed_info("AA:AA:AA:AA:AA:01", -40, 100.0),
            _timed_info("AA:AA:AA:AA:AA:02", -80, 200.0),
        ],
        ACCOUNT_KEY,
    )
    assert selection.addresses[0] == "AA:AA:AA:AA:AA:02"


def test_signal_breaks_a_tie_between_equally_recent_sightings() -> None:
    selection = ba.select_candidates(
        [
            _timed_info("AA:AA:AA:AA:AA:01", -80, 100.0),
            _timed_info("AA:AA:AA:AA:AA:02", -40, 100.0),
        ],
        ACCOUNT_KEY,
    )
    assert selection.addresses[0] == "AA:AA:AA:AA:AA:02"


@pytest.mark.asyncio
async def test_failed_address_moves_on_to_the_next_and_holds_one_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()
    names: list[str] = []

    async def connect(_cls: Any, _device: object, name: str, **kw: Any) -> FakeProvider:
        names.append(name)
        assert kw["max_attempts"] == 1
        if name == "...A:02":
            raise ProviderError(0x85)
        return provider

    bluetooth = _Bluetooth(
        [
            _timed_info("AA:AA:AA:AA:AA:01", -40, 100.0),
            _timed_info("AA:AA:AA:AA:AA:02", -80, 200.0),
        ]
    )
    _install(monkeypatch, bluetooth, connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert names == ["...A:02", "...A:01"]
    assert report["connection"] == "connected"
    assert report["address_masked"] == "...A:01"
    assert "error_class" not in report
    assert [a["result"] for a in report["connection_attempts"]] == [
        "connection_failed",
        "connected",
    ]
    assert provider.disconnects == 1
    assert "AA:AA:AA" not in repr(report)


@pytest.mark.asyncio
async def test_address_that_appears_after_a_failure_is_next_in_line(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()
    names: list[str] = []
    bluetooth = _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)])

    async def connect(_cls: Any, _device: object, name: str, **_kw: Any) -> Any:
        names.append(name)
        if name == "...A:01":
            bluetooth.infos.append(_timed_info("AA:AA:AA:AA:AA:02", -80, 200.0))
            raise ProviderError(0x85)
        return provider

    _install(monkeypatch, bluetooth, connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert names == ["...A:01", "...A:02"]
    assert report["connection"] == "connected"


@pytest.mark.asyncio
async def test_tries_stop_at_the_limit_and_keep_the_last_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names: list[str] = []

    async def connect(_cls: Any, _device: object, name: str, **_kw: Any) -> None:
        names.append(name)
        raise ProviderError(0x85)

    _install(
        monkeypatch,
        _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)]),
        connect,
    )
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert len(names) == ba.MAX_CONNECT_ATTEMPTS
    assert report["connection"] == "connection_failed"
    assert report["error_class"] == "ProviderError"


@pytest.mark.asyncio
async def test_a_hanging_address_gets_only_its_share_of_the_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()

    async def connect(_cls: Any, _device: object, name: str, **_kw: Any) -> Any:
        if name == "...A:02":
            await asyncio.sleep(60)
        return provider

    _install(
        monkeypatch,
        _Bluetooth(
            [
                _timed_info("AA:AA:AA:AA:AA:01", -40, 100.0),
                _timed_info("AA:AA:AA:AA:AA:02", -80, 200.0),
            ]
        ),
        connect,
    )
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=1.5
    )
    assert report["connection"] == "connected"
    assert report["connection_attempts"][0]["result"] == "timeout"


@pytest.mark.asyncio
async def test_device_gone_from_the_cache_counts_as_a_try(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*_args: Any, **_kw: Any) -> None:
        raise AssertionError("no device, no connection")

    _install(
        monkeypatch,
        _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)], device=None),
        connect,
    )
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["connection"] == "device_not_connectable"
    assert len(report["connection_attempts"]) == ba.MAX_CONNECT_ATTEMPTS


@pytest.mark.asyncio
async def test_named_address_is_tried_once_and_lists_no_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()

    async def connect(_cls: Any, _device: object, _name: str, **kw: Any) -> Any:
        assert kw["max_attempts"] == 2
        return provider

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
    )
    assert report["connection"] == "connected"
    assert "connection_attempts" not in report


# --- responses the parser does not know ---------------------------------------------


class _LongEidProvider(_ConnectedProvider):
    """Answers ``0x01`` with extra bytes behind the EID, correctly authenticated."""

    def __init__(self, trailer: bytes = b"\x07\x07", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.trailer = trailer
        self.length_field: int | None = None

    def _provisioning_frame(self, version_nonce: bytes, eid: bytes) -> bytes:
        additional = b"\x03" + eid + self.trailer
        length = (
            self.length_field if self.length_field is not None else len(additional) + 8
        )
        head = bytes((0x01, length))
        auth = _sign(self.account_key, version_nonce, head, additional, b"\x01")
        return head + auth + additional

    async def write_gatt_char(
        self, uuid: str, data: bytes, response: bool = True
    ) -> None:
        if data[0] != 0x01:
            await super().write_gatt_char(uuid, data, response)
            return
        self.calls.append(("write", data[0]))
        assert self.nonce is not None and self.callback is not None
        eid = generate_eid_variant(EIK, self.eid_clock, self.eid_variant)
        self.callback(
            None,
            bytearray(self._provisioning_frame(b"\x01" + self.nonce, eid)),
        )
        self.nonce = None


@pytest.mark.asyncio
async def test_unknown_length_reports_its_shape_and_still_checks_the_eid() -> None:
    provider = _LongEidProvider()
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    state = report["operations"]["read_provisioning_state"]
    assert state["status"] == "invalid_response"
    response = state["response"]
    assert response["stage"] == "length"
    assert response["additional_length"] == 23
    assert response["frame_length"] == 33
    assert response["auth_valid"] is True
    assert response["flags_byte"] == 0x03
    assert report["eik_set"] is None
    assert report["eid_check"]["result"] == "match"
    assert report["eid_check"]["inferred_form"] is True
    assert "raw_frame" not in state
    eid = generate_eid_variant(EIK, CLOCK, EidVariant.LEGACY_SECP160R1_X20_BE)
    assert eid.hex() not in repr(report)


@pytest.mark.asyncio
async def test_wrong_length_field_reports_stage_frame_and_field_value() -> None:
    provider = _LongEidProvider(trailer=b"")
    provider.length_field = 21
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    response = report["operations"]["read_provisioning_state"]["response"]
    assert response["stage"] == "frame"
    assert response["data_length_field"] == 21
    assert response["auth_valid"] is True
    assert report["eid_check"]["result"] == "match"
    assert report["eid_check"]["inferred_form"] is True


@pytest.mark.asyncio
async def test_forged_frame_of_unknown_form_yields_no_flags_and_no_eid() -> None:
    provider = _LongEidProvider(account_key=ACCOUNT_KEY)
    original = provider._provisioning_frame

    def forged(version_nonce: bytes, eid: bytes) -> bytes:
        frame = bytearray(original(version_nonce, eid))
        frame[2] ^= 0xFF
        frame[1] = 21
        return bytes(frame)

    provider._provisioning_frame = forged  # type: ignore[method-assign]
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    response = report["operations"]["read_provisioning_state"]["response"]
    assert response["auth_valid"] is False
    assert "flags_byte" not in response
    assert "data_prefix" not in response
    assert report["eid_check"]["result"] == "not_checked"


@pytest.mark.asyncio
async def test_raw_frame_appears_only_on_request_and_only_for_provisioning() -> None:
    report = await ba.read_beacon_state(
        FakeProvider(), ba.BeaconKeys(ACCOUNT_KEY, EIK), include_raw=True
    )
    operations = report["operations"]
    assert len(operations["read_provisioning_state"]["raw_frame"]) == 2 * (2 + 8 + 21)
    assert "raw_frame" not in operations["read_beacon_parameters"]
    assert "raw_frame" not in operations["read_ringing_state"]
    assert "response" not in operations["read_provisioning_state"]
    plain = await _run(FakeProvider(), ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert "raw_frame" not in plain["operations"]["read_provisioning_state"]


@pytest.mark.parametrize(
    ("additional", "expected"),
    [
        (b"\x03", ()),
        (b"\x03" + bytes(19), (b"\x03" + bytes(19),)),
        (bytes(20), (bytes(20),)),
        (b"\x03" + bytes(20), (bytes(20),)),
        (b"\x03" + bytes(32), (bytes(20), bytes(32))),
        (bytes(32), (bytes(20), bytes(32))),
    ],
)
def test_eid_candidates_follow_the_data_length(
    additional: bytes, expected: tuple[bytes, ...]
) -> None:
    assert ba._eid_candidates(additional) == expected


# --- second-by-second search --------------------------------------------------------


def _unmasked_eid(counter: int, variant: EidVariant) -> bytes:
    """Compute the expected EID without the module's block builder."""
    raw = counter.to_bytes(4, "big")
    block = b"\xff" * 11 + b"\x0a" + raw + b"\x00" * 11 + b"\x0a" + raw
    from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
        _serialize_variant,
        prf_aes_256_ecb,
    )

    return _serialize_variant(prf_aes_256_ecb(EIK, block), variant)


@pytest.mark.parametrize("variant", list(EidVariant))
def test_block_equals_the_specified_one_on_a_rotation_boundary(
    variant: EidVariant,
) -> None:
    counter = 1024 * 977
    target = generate_eid_variant(EIK, counter, variant)
    report = ba.search_eid_counter(EIK, counter, {target: "reported"}, 1)
    assert report["result"] == "match"
    assert report["variant"] == variant.value
    assert report["counter_offset"] == 0
    assert report["counter_low_bits"] == 0


@pytest.mark.parametrize("delta", [-37, 37])
def test_search_finds_a_counter_off_the_raster(delta: int) -> None:
    clock = 1024 * 977 + 500
    target = _unmasked_eid(clock + delta, EidVariant.LEGACY_SECP160R1_X20_BE)
    report = ba.search_eid_counter(EIK, clock, {target: "supplied"}, 40)
    assert report["result"] == "match"
    assert report["source"] == "supplied"
    assert report["variant"] == EidVariant.LEGACY_SECP160R1_X20_BE.value
    assert report["counter_offset"] == delta
    assert report["counter_low_bits"] == 500 + delta
    assert report["counters_checked"] == 2 * abs(delta) + (0 if delta < 0 else 1)
    assert target.hex() not in repr(report)


def test_search_ignores_variants_of_another_length() -> None:
    report = ba.search_eid_counter(EIK, 5000, {bytes(20): "supplied"}, 1)
    assert report["result"] == "no_match"
    assert report["variants"] == 4
    assert report["counters_checked"] == 3
    report = ba.search_eid_counter(EIK, 5000, {bytes(32): "supplied"}, 0)
    assert report["variants"] == 3


def test_search_stays_inside_the_counter_range() -> None:
    assert ba.search_eid_counter(EIK, 0, {bytes(20): "s"}, 2)["counters_checked"] == 3
    top = ba.search_eid_counter(EIK, ba.MAX_COUNTER, {bytes(20): "s"}, 2)
    assert top["counters_checked"] == 3


def test_search_stops_at_the_time_budget() -> None:
    ticks = iter(float(n) * 100 for n in range(100))
    report = ba.search_eid_counter(
        EIK,
        1_000_000,
        {bytes(20): "s"},
        100_000,
        time_budget=150.0,
        now=lambda: next(ticks),
    )
    assert report["result"] == "time_budget_exhausted"
    assert report["counters_checked"] == 256


@pytest.mark.asyncio
async def test_no_search_runs_unless_asked(monkeypatch: pytest.MonkeyPatch) -> None:
    def _forbidden(*_args: Any, **_kw: Any) -> None:
        raise AssertionError("the search must stay off")

    monkeypatch.setattr(ba, "search_eid_counter", _forbidden)
    provider = _ConnectedProvider()

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
    )
    assert "eid_search" not in report


@pytest.mark.asyncio
async def test_search_runs_after_disconnect_with_both_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()
    seen: dict[str, Any] = {}

    def _search(
        _eik: bytes, clock: int, targets: dict[bytes, str], window: int, _stop: Any
    ) -> dict[str, Any]:
        seen.update(
            clock=clock,
            sources=sorted(targets.values()),
            window=window,
            disconnects=provider.disconnects,
        )
        return {"result": "no_match"}

    monkeypatch.setattr(ba, "search_eid_counter", _search)

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
        search_seconds=9,
        search_eid=bytes(20),
    )
    assert report["eid_search"] == {"result": "no_match"}
    assert seen == {
        "clock": CLOCK,
        "sources": ["reported", "supplied"],
        "window": 9,
        "disconnects": 1,
    }


@pytest.mark.parametrize(
    ("keys", "reading", "search_eid", "result"),
    [
        (ba.BeaconKeys(ACCOUNT_KEY, None), None, None, "no_eik"),
        (ba.BeaconKeys(ACCOUNT_KEY, EIK), None, None, "no_clock"),
        (
            ba.BeaconKeys(ACCOUNT_KEY, EIK),
            ba._Reading({}, None, (bytes(20),)),
            None,
            "no_clock",
        ),
        (ba.BeaconKeys(ACCOUNT_KEY, EIK), ba._Reading({}, 5, ()), None, "no_target"),
    ],
)
@pytest.mark.asyncio
async def test_search_says_why_it_cannot_run(
    keys: ba.BeaconKeys,
    reading: ba._Reading | None,
    search_eid: bytes | None,
    result: str,
) -> None:
    report = await ba._async_search(
        _Hass(), keys, reading, ba._Target(), ba._Searches(5, search_eid)
    )
    assert report == {"result": result}


@pytest.mark.asyncio
async def test_limit_already_used_up_stops_before_any_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*_args: Any, **_kw: Any) -> None:
        raise AssertionError("no time, no connection")

    _install(
        monkeypatch,
        _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)]),
        connect,
    )
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=0
    )
    assert report["connection"] == "timeout"
    assert "connection_attempts" not in report


@pytest.mark.asyncio
async def test_frames_gone_after_a_failure_keep_the_last_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bluetooth = _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)])

    async def connect(*_args: Any, **_kw: Any) -> None:
        bluetooth.infos.clear()
        raise ProviderError(0x85)

    _install(monkeypatch, bluetooth, connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["connection"] == "connection_failed"
    assert len(report["connection_attempts"]) == 1


@pytest.mark.asyncio
async def test_too_short_frame_counts_as_not_authenticated() -> None:
    provider = _LongEidProvider()
    provider._provisioning_frame = (  # type: ignore[method-assign]
        lambda _version_nonce, _eid: b"\x01\x08\x00"
    )
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    response = report["operations"]["read_provisioning_state"]["response"]
    assert response == {
        "stage": "frame",
        "frame_length": 3,
        "data_length_field": 8,
        "additional_length": 0,
        "auth_valid": False,
    }


@pytest.mark.parametrize(
    ("remaining", "untried", "attempts_left", "budget"),
    [
        (35.0, 1, 3, ba.CONNECT_ATTEMPT_LIMIT),
        (10.0, 1, 3, 10.0),
        (35.0, 2, 3, 17.5),
        (30.0, 5, 3, 10.0),
        (9.0, 0, 2, 9.0),
        (30.0, 2, 1, ba.CONNECT_ATTEMPT_LIMIT),
    ],
)
def test_attempt_budget_shares_time_among_untried_addresses(
    remaining: float, untried: int, attempts_left: int, budget: float
) -> None:
    assert ba._attempt_budget(remaining, untried, attempts_left) == budget


def _record_budgets(
    monkeypatch: pytest.MonkeyPatch, result: Any, budgets: list[float]
) -> None:
    async def _once(*_args: Any, budget: float, **_kw: Any) -> Any:
        budgets.append(budget)
        return result

    monkeypatch.setattr(ba, "_async_connect_once", _once)


@pytest.mark.asyncio
async def test_lone_address_gets_more_than_a_third_of_the_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budgets: list[float] = []
    _record_budgets(monkeypatch, _ConnectedProvider(), budgets)
    _install(
        monkeypatch,
        _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)]),
        print,
    )
    await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=45
    )
    assert budgets == [ba.CONNECT_ATTEMPT_LIMIT]


@pytest.mark.asyncio
async def test_two_addresses_share_the_time_left_after_the_read_reserve(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budgets: list[float] = []
    _record_budgets(monkeypatch, None, budgets)
    _install(
        monkeypatch,
        _Bluetooth(
            [
                _timed_info("AA:AA:AA:AA:AA:01", -40, 100.0),
                _timed_info("AA:AA:AA:AA:AA:02", -80, 200.0),
            ]
        ),
        print,
    )
    await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=45
    )
    assert budgets[0] == pytest.approx(17.5, abs=0.5)


@pytest.mark.parametrize(("timeout", "reserve"), [(45.0, 10.0), (12.0, 4.0)])
@pytest.mark.asyncio
async def test_read_reserve_is_ten_seconds_at_most_and_a_third_at_most(
    monkeypatch: pytest.MonkeyPatch, timeout: float, reserve: float
) -> None:
    seen: list[float] = []

    async def _search(*args: Any, **_kw: Any) -> None:
        seen.append(args[4])

    monkeypatch.setattr(ba, "_async_connect_search", _search)
    _install(monkeypatch, _Bluetooth([]), print)
    loop = asyncio.get_running_loop()
    before = loop.time()
    await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=timeout
    )
    assert seen[0] - before == pytest.approx(timeout - reserve, abs=0.5)


@pytest.mark.asyncio
async def test_attempt_reports_how_old_the_sighting_is(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(
        monkeypatch,
        _Bluetooth(
            [_timed_info("AA:AA:AA:AA:AA:01", -40, time.monotonic() - 5.0)],
        ),
        connect,
    )
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert 4.5 < report["connection_attempts"][0]["seen_ago_s"] < 30


@pytest.mark.asyncio
async def test_timeout_after_an_error_does_not_keep_the_old_error_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def connect(_cls: Any, _device: object, name: str, **_kw: Any) -> None:
        calls.append(name)
        if len(calls) == 1:
            raise ProviderError(0x85)
        await asyncio.sleep(60)

    _install(
        monkeypatch,
        _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)]),
        connect,
    )
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=0.3
    )
    assert report["connection"] == "timeout"
    assert "error_class" not in report
    assert report["connection_attempts"][0]["error_class"] == "ProviderError"


@pytest.mark.asyncio
async def test_device_gone_after_an_error_drops_the_old_error_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bluetooth = _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)])

    async def connect(*_args: Any, **_kw: Any) -> None:
        bluetooth.device = None
        raise ProviderError(0x85)

    _install(monkeypatch, bluetooth, connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["connection"] == "device_not_connectable"
    assert "error_class" not in report


class _BareEidProvider(_LongEidProvider):
    """Answers ``0x01`` with the EID alone, without the flags byte."""

    def _provisioning_frame(self, version_nonce: bytes, eid: bytes) -> bytes:
        head = bytes((0x01, len(eid) + 8))
        return head + _sign(self.account_key, version_nonce, head, eid, b"\x01") + eid


@pytest.mark.asyncio
async def test_second_eid_candidate_is_checked_when_the_first_misses() -> None:
    provider = _BareEidProvider(eid_variant=EidVariant.SPEC_P256_X32_BE)
    report = await _run(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert report["eid_check"]["result"] == "match"
    assert report["eid_check"]["variant"] == EidVariant.SPEC_P256_X32_BE.value
    assert report["eid_check"]["inferred_form"] is True


def test_search_stops_when_asked() -> None:
    stop = threading.Event()
    stop.set()
    report = ba.search_eid_counter(EIK, 1_000_000, {bytes(20): "s"}, 100_000, stop)
    assert report["result"] == "cancelled"
    assert report["counters_checked"] == 0


@pytest.mark.asyncio
async def test_cancelled_caller_stops_the_search() -> None:
    seen: list[threading.Event] = []
    started = asyncio.Event()
    release = asyncio.Event()

    class _WaitingHass(_Hass):
        async def async_add_executor_job(self, _target: Any, *args: Any) -> Any:
            seen.append(args[4])
            started.set()
            await release.wait()

    task = asyncio.create_task(
        ba._async_search(
            _WaitingHass(),
            ba.BeaconKeys(ACCOUNT_KEY, EIK),
            ba._Reading({}, 5, (bytes(20),)),
            ba._Target(),
            ba._Searches(5),
        )
    )
    await started.wait()
    assert not seen[0].is_set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen[0].is_set()


@pytest.mark.asyncio
async def test_failed_connection_says_why_the_search_did_not_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*_args: Any, **_kw: Any) -> None:
        raise ProviderError(0x85)

    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
        search_seconds=9,
    )
    assert report["eid_search"] == {"result": "no_clock"}


def test_documented_limits_hold_their_values() -> None:
    assert ba.MAX_CONNECT_ATTEMPTS == 3
    assert ba.CONNECT_ATTEMPT_LIMIT == 25.0
    assert ba.READ_RESERVE_SECONDS == 10.0
    assert ba.EID_SEARCH_TIME_BUDGET == 300.0
    assert ba.EID_SEARCH_MAX_SECONDS == 172800


def test_stop_set_during_the_run_ends_the_search_at_the_next_check() -> None:
    stop = threading.Event()
    ticks: list[int] = []

    def now() -> float:
        ticks.append(1)
        if len(ticks) == 2:
            stop.set()
        return 0.0

    report = ba.search_eid_counter(
        EIK, 1_000_000, {bytes(20): "s"}, 100_000, stop, now=now
    )
    assert report["result"] == "cancelled"
    assert report["counters_checked"] == 256


@pytest.mark.asyncio
async def test_sighting_without_a_time_reports_no_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(
        monkeypatch,
        _Bluetooth([_info("AA:AA:AA:AA:AA:01", -40, _frame_for(ACCOUNT_KEY))]),
        connect,
    )
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["connection_attempts"][0]["seen_ago_s"] is None


@pytest.mark.asyncio
async def test_all_addresses_failed_retries_the_newest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names: list[str] = []

    async def connect(_cls: Any, _device: object, name: str, **_kw: Any) -> None:
        names.append(name)
        raise ProviderError(0x85)

    _install(
        monkeypatch,
        _Bluetooth(
            [
                _timed_info("AA:AA:AA:AA:AA:01", -40, 100.0),
                _timed_info("AA:AA:AA:AA:AA:02", -80, 200.0),
            ]
        ),
        connect,
    )
    await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert names == ["...A:02", "...A:01", "...A:02"]


@pytest.mark.asyncio
async def test_many_addresses_share_among_the_tries_left_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budgets: list[float] = []
    _record_budgets(monkeypatch, None, budgets)
    _install(
        monkeypatch,
        _Bluetooth(
            [_timed_info(f"AA:AA:AA:AA:AA:0{n}", -40, 100.0 + n) for n in range(5)]
        ),
        print,
    )
    await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=45
    )
    assert budgets[0] == pytest.approx(35 / 3, abs=0.5)


@pytest.mark.asyncio
async def test_used_up_time_after_an_error_drops_the_old_error_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _once(
        _stack: Any, _device: Any, _target: str, report: dict[str, Any], **_kw: Any
    ) -> None:
        report["connection"] = "connection_failed"
        report["error_class"] = "OSError"

    monkeypatch.setattr(ba, "_async_connect_once", _once)
    bluetooth = _Bluetooth([_timed_info("AA:AA:AA:AA:AA:01", -40, 100.0)])
    _install(monkeypatch, bluetooth, print)
    report: dict[str, Any] = {}
    loop = asyncio.get_running_loop()
    stack = ba._BleStack(bluetooth, object, print)
    calls = iter([1.0, -1.0])

    def budget(*_args: Any) -> float:
        return 1.0

    monkeypatch.setattr(ba, "_attempt_budget", budget)
    original = loop.time

    def later() -> float:
        return original() + (0.0 if next(calls) > 0 else 100.0)

    monkeypatch.setattr(loop, "time", later)
    try:
        await ba._async_connect_search(
            _Hass(), stack, ACCOUNT_KEY, report, original() + 10.0, seen=ba._Target()
        )
    finally:
        monkeypatch.undo()
    assert report["connection"] == "timeout"
    assert "error_class" not in report


@pytest.mark.parametrize(
    ("keys", "expected"),
    [
        (ba.BeaconKeys(None, EIK), "no_clock"),
        (ba.BeaconKeys(None, None), "no_eik"),
    ],
)
@pytest.mark.asyncio
async def test_search_without_account_key_says_why(
    monkeypatch: pytest.MonkeyPatch, keys: ba.BeaconKeys, expected: str
) -> None:
    _install(monkeypatch, _Bluetooth([]), print)
    report = await ba.async_probe_tracker(
        _Hass(), keys, address=None, timeout=5, search_seconds=3
    )
    assert report["connection"] == "no_account_key_for_search"
    assert report["eid_search"] == {"result": expected}


# --- two identities, the form with one more byte, derivations ------------------------


LOCATOR_EID = bytes(range(100, 120))


def _identity_info(  # noqa: PLR0913
    address: str,
    *,
    seen: float = 1.0,
    fmdn: bytes | None = None,
    audio: bool = False,
    tx_power: int | None = None,
    key: bytes = ACCOUNT_KEY,
) -> SimpleNamespace:
    service_data = {ba.FAST_PAIR_SERVICE_DATA_UUID: _frame_for(key)}
    if fmdn is not None:
        service_data[ba.FMDN_SERVICE_DATA_UUID] = fmdn
    if audio:
        service_data[ba.COMMON_AUDIO_SERVICE_DATA_UUID] = b""
    return SimpleNamespace(
        address=address,
        rssi=-60,
        time=seen,
        service_data=service_data,
        tx_power=tx_power,
    )


class _IdentityBluetooth(_Bluetooth):
    """Answers the last service info from the given advertisements."""

    def async_last_service_info(
        self, _hass: Any, address: str, connectable: bool = True
    ) -> SimpleNamespace:
        assert connectable is True
        return next(info for info in self.infos if info.address == address)


LOCATOR_INFO = _identity_info(
    "AA:AA:AA:AA:AA:01", seen=1.0, fmdn=b"\x40" + LOCATOR_EID + b"\x63", tx_power=-9
)
AUDIO_INFO = _identity_info("AA:AA:AA:AA:AA:02", seen=2.0, audio=True, tx_power=-10)


@pytest.mark.parametrize(
    ("data", "eid"),
    [
        (b"\x03" + bytes(range(20)) + b"\x00", bytes(range(20))),
        (b"\x03" + bytes(range(32)) + b"\x00", bytes(range(32))),
        (b"\x03" + bytes(range(20)), bytes(range(20))),
    ],
)
def test_form_with_one_more_byte_keeps_the_eid_field(data: bytes, eid: bytes) -> None:
    assert ba.parse_provisioning(data) == (True, True, eid)


@pytest.mark.asyncio
async def test_one_more_byte_is_a_verified_answer_and_stays_marked() -> None:
    report = await _run(
        _LongEidProvider(trailer=b"\x00"), ba.BeaconKeys(ACCOUNT_KEY, EIK)
    )
    state = report["operations"]["read_provisioning_state"]
    assert state["status"] == "ok"
    assert state["trailing_byte"] == 0
    assert "response" not in state
    assert report["eik_set"] is True
    assert report["is_owner"] is True
    assert report["eid_check"]["result"] == "match"
    assert report["eid_check"]["inferred_form"] is True


@pytest.mark.asyncio
async def test_specified_form_has_no_trailing_byte_and_is_not_inferred() -> None:
    report = await _run(FakeProvider(), ba.BeaconKeys(ACCOUNT_KEY, EIK))
    state = report["operations"]["read_provisioning_state"]
    assert "trailing_byte" not in state
    assert "eid_placeholder" not in state
    assert "inferred_form" not in report["eid_check"]
    assert report["protocol_version"] == 1


class _PlaceholderProvider(_LongEidProvider):
    """Answers ``0x01`` the way the locator of the measured earbuds does."""

    def _provisioning_frame(self, version_nonce: bytes, eid: bytes) -> bytes:
        return super()._provisioning_frame(version_nonce, b"\x00\xff" + bytes(18))


@pytest.mark.asyncio
async def test_placeholder_is_reported_and_is_no_eid() -> None:
    provider = _PlaceholderProvider(trailer=b"\x00")
    reading = await ba._read_state(provider, ba.BeaconKeys(ACCOUNT_KEY, EIK))
    state = reading.report["operations"]["read_provisioning_state"]
    assert state["eid_placeholder"] is True
    assert state["eid_prefix"] == "00ff0000"
    assert reading.eids == ()
    assert reading.report["eid_check"] == {"result": "not_checked"}
    assert reading.report["eik_set"] is True


@pytest.mark.parametrize(
    ("eid", "expected"),
    [
        (b"\x00\xff" + bytes(18), True),
        (b"\x00\xff" + bytes(30), True),
        (b"\x00\xff" + bytes(17) + b"\x01", False),
        (b"\x00\xfe" + bytes(18), False),
        (bytes(20), False),
    ],
)
def test_placeholder_needs_its_exact_form(eid: bytes, expected: bool) -> None:
    assert ba.is_placeholder_eid(eid) is expected


@pytest.mark.parametrize(
    ("service_data", "form"),
    [
        ({ba.FMDN_SERVICE_DATA_UUID: b"\x40"}, ba.IdentityForm.LOCATOR),
        ({ba.FMDN_SERVICE_DATA_UUID: b"\x41"}, ba.IdentityForm.LOCATOR),
        (
            {ba.FMDN_SERVICE_DATA_UUID: bytes(7) + b"\x40" + bytes(21)},
            ba.IdentityForm.LOCATOR,
        ),
        (
            {ba.FMDN_SERVICE_DATA_UUID: bytes(7) + b"\x40" + bytes(19)},
            ba.IdentityForm.OTHER,
        ),
        ({ba.FMDN_SERVICE_DATA_UUID: b"\x00" + bytes(16)}, ba.IdentityForm.OTHER),
        ({ba.FMDN_SERVICE_DATA_UUID: b""}, ba.IdentityForm.OTHER),
        (
            {
                ba.FMDN_SERVICE_DATA_UUID: b"\x00",
                ba.COMMON_AUDIO_SERVICE_DATA_UUID: b"",
            },
            ba.IdentityForm.AUDIO,
        ),
        ({ba.COMMON_AUDIO_SERVICE_DATA_UUID: b""}, ba.IdentityForm.AUDIO),
        (
            {
                ba.FMDN_SERVICE_DATA_UUID: b"\x40",
                ba.COMMON_AUDIO_SERVICE_DATA_UUID: b"",
            },
            ba.IdentityForm.LOCATOR,
        ),
        ({ba.FAST_PAIR_SERVICE_DATA_UUID: b"\x10"}, ba.IdentityForm.OTHER),
        ({}, ba.IdentityForm.OTHER),
    ],
)
def test_identity_form_follows_the_service_data(
    service_data: dict[str, bytes], form: ba.IdentityForm
) -> None:
    assert ba.identity_form(service_data) is form


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"\x40" + LOCATOR_EID, LOCATOR_EID),
        (b"\x40" + LOCATOR_EID + b"\x63", LOCATOR_EID),
        (b"\x41" + LOCATOR_EID + b"\x63", LOCATOR_EID),
        (b"\x41" + bytes(range(32)) + b"\x63", bytes(range(32))),
        (bytes(7) + b"\x40" + LOCATOR_EID, LOCATOR_EID),
        (bytes(7) + b"\x40" + LOCATOR_EID + b"\x63", LOCATOR_EID),
        (bytes(7) + b"\x41" + bytes(range(32)) + b"\x63", bytes(range(32))),
        (bytes(7) + b"\x10" + LOCATOR_EID, None),
        (b"\x10" + LOCATOR_EID, None),
        (b"\x40" + LOCATOR_EID[:19], None),
        (b"\x40" + bytes(25), None),
        (b"", None),
    ],
)
def test_advertised_eid_reads_only_the_fmdn_frame_form(
    raw: bytes, expected: bytes | None
) -> None:
    assert ba.advertised_eid({ba.FMDN_SERVICE_DATA_UUID: raw}) == expected


def test_selection_without_a_form_takes_every_identity() -> None:
    selection = ba.select_candidates([LOCATOR_INFO, AUDIO_INFO], ACCOUNT_KEY)
    assert selection.addresses == ("AA:AA:AA:AA:AA:02", "AA:AA:AA:AA:AA:01")
    assert selection.frames_other_form == 0


@pytest.mark.parametrize(
    ("form", "address"),
    [
        (ba.IdentityForm.LOCATOR, "AA:AA:AA:AA:AA:01"),
        (ba.IdentityForm.AUDIO, "AA:AA:AA:AA:AA:02"),
    ],
)
def test_selection_with_a_form_counts_the_other_identity(
    form: ba.IdentityForm, address: str
) -> None:
    selection = ba.select_candidates([LOCATOR_INFO, AUDIO_INFO], ACCOUNT_KEY, form)
    assert selection.addresses == (address,)
    assert selection.frames_matched == 1
    assert selection.frames_other_form == 1
    assert selection.frames_seen == 2


def test_selection_does_not_count_a_foreign_frame_as_the_other_identity() -> None:
    foreign = _identity_info(
        "AA:AA:AA:AA:AA:03", audio=True, key=bytes.fromhex("04" + "77" * 15)
    )
    selection = ba.select_candidates(
        [LOCATOR_INFO, foreign], ACCOUNT_KEY, ba.IdentityForm.LOCATOR
    )
    assert selection.frames_other_form == 0


def _probe_kwargs(monkeypatch: pytest.MonkeyPatch, provider: FakeProvider) -> None:
    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    _install(monkeypatch, _IdentityBluetooth([LOCATOR_INFO, AUDIO_INFO]), connect)


@pytest.mark.asyncio
async def test_probe_reports_the_identity_it_connected_to(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _probe_kwargs(monkeypatch, _ConnectedProvider())
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address=None,
        timeout=5,
        form=ba.IdentityForm.LOCATOR,
    )
    assert report["address_masked"] == "...A:01"
    assert report["identity_form"] == "locator"
    assert report["advertised_tx_power"] == -9
    assert report["advertised_eid_prefix"] == LOCATOR_EID[:4].hex()
    assert report["frames_other_form"] == 1
    assert report["advertised_eid_check"]["result"] == "no_match"
    assert LOCATOR_EID.hex() not in repr(report)


@pytest.mark.asyncio
async def test_probe_without_a_form_reports_no_skipped_frames(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _probe_kwargs(monkeypatch, _ConnectedProvider())
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["identity_form"] == "audio"
    assert report["advertised_tx_power"] == -10
    assert "frames_other_form" not in report
    assert "advertised_eid_prefix" not in report
    assert "advertised_eid_check" not in report


@pytest.mark.asyncio
async def test_advertised_eid_check_finds_the_derived_eid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eid = generate_eid_variant(EIK, CLOCK, EidVariant.LEGACY_SECP160R1_X20_BE)
    info = _identity_info("AA:AA:AA:AA:AA:01", fmdn=b"\x40" + eid)

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return _ConnectedProvider()

    _install(monkeypatch, _IdentityBluetooth([info]), connect)
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert report["advertised_eid_check"]["result"] == "match"


@pytest.mark.asyncio
async def test_a_named_address_ignores_the_form(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _probe_kwargs(monkeypatch, _ConnectedProvider())
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:AA:AA:AA:AA:02",
        timeout=5,
        form=ba.IdentityForm.LOCATOR,
    )
    assert report["address_masked"] == "...A:02"
    assert report["identity_form"] == "audio"
    assert "frames_other_form" not in report


@pytest.mark.asyncio
async def test_no_frame_of_the_form_means_no_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        raise AssertionError("must not connect")

    _install(monkeypatch, _IdentityBluetooth([AUDIO_INFO]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address=None,
        timeout=5,
        form=ba.IdentityForm.LOCATOR,
    )
    assert report["connection"] == "no_matching_frame"
    assert report["frames_other_form"] == 1


def _sha(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def _keyed(key: bytes, data: bytes) -> bytes:
    return hmac.new(key, data, hashlib.sha256).digest()


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("eik", EIK),
        ("eik_reversed", EIK[::-1]),
        ("sha256_eik", _sha(EIK)),
        ("sha256_eik_then_1", _sha(EIK + b"\x01")),
        ("sha256_3_then_eik", _sha(b"\x03" + EIK)),
        ("hmac_eik_7", _keyed(EIK, b"\x07")),
        ("account_key_raw", ACCOUNT_KEY),
        ("account_key_raw_twice", ACCOUNT_KEY + ACCOUNT_KEY),
        ("account_key_raw_zero_padded", ACCOUNT_KEY + bytes(16)),
        ("sha256_account_key_raw", _sha(ACCOUNT_KEY)),
        ("sha256_eik_then_account_key_raw", _sha(EIK + ACCOUNT_KEY)),
        ("sha256_account_key_raw_then_eik", _sha(ACCOUNT_KEY + EIK)),
        ("hmac_eik_account_key_raw", _keyed(EIK, ACCOUNT_KEY)),
        ("hmac_account_key_raw_eik", _keyed(ACCOUNT_KEY, EIK)),
        ("account_key_first_byte_05", b"\x05" + ACCOUNT_KEY[1:]),
    ],
)
def test_derivation_key_holds_what_its_name_says(name: str, expected: bytes) -> None:
    assert dict(ba.derivation_keys(ba.BeaconKeys(ACCOUNT_KEY, EIK)))[name] == expected


def test_derivation_keys_have_no_duplicates_and_valid_lengths() -> None:
    candidates = ba.derivation_keys(ba.BeaconKeys(ACCOUNT_KEY, EIK))
    assert len({key for _name, key in candidates}) == len(candidates)
    assert len({name for name, _key in candidates}) == len(candidates)
    assert {len(key) for _name, key in candidates} == {16, 32}


def test_derivation_keys_follow_the_keys_at_hand() -> None:
    assert not ba.derivation_keys(ba.BeaconKeys(None, None))
    only_eik = dict(ba.derivation_keys(ba.BeaconKeys(None, EIK)))
    assert "eik" in only_eik and not [n for n in only_eik if "account" in n]
    only_account = dict(ba.derivation_keys(ba.BeaconKeys(ACCOUNT_KEY, None)))
    assert "account_key_raw" in only_account and "eik" not in only_account
    assert not [n for n in only_account if "eik" in n]


COUNTER_BYTES = bytes.fromhex("00012000")
COUNTER_VALUE = 0x00012000


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (
            "table10",
            b"\xff" * 11
            + b"\x0a"
            + COUNTER_BYTES
            + bytes(11)
            + b"\x0a"
            + COUNTER_BYTES,
        ),
        (
            "counter_little_endian",
            b"\xff" * 11
            + b"\x0a"
            + COUNTER_BYTES[::-1]
            + bytes(11)
            + b"\x0a"
            + COUNTER_BYTES[::-1],
        ),
        (
            "pads_swapped",
            bytes(11)
            + b"\x0a"
            + COUNTER_BYTES
            + b"\xff" * 11
            + b"\x0a"
            + COUNTER_BYTES,
        ),
        (
            "pads_zero",
            bytes(11) + b"\x0a" + COUNTER_BYTES + bytes(11) + b"\x0a" + COUNTER_BYTES,
        ),
        (
            "pads_full",
            b"\xff" * 11
            + b"\x0a"
            + COUNTER_BYTES
            + b"\xff" * 11
            + b"\x0a"
            + COUNTER_BYTES,
        ),
        (
            "k_byte_9",
            b"\xff" * 11
            + b"\x09"
            + COUNTER_BYTES
            + bytes(11)
            + b"\x09"
            + COUNTER_BYTES,
        ),
        (
            "component_2_first_pad",
            b"\xff" * 10
            + b"\x02\x0a"
            + COUNTER_BYTES
            + bytes(11)
            + b"\x0a"
            + COUNTER_BYTES,
        ),
        (
            "component_3_second_pad",
            b"\xff" * 11
            + b"\x0a"
            + COUNTER_BYTES
            + bytes(10)
            + b"\x03\x0a"
            + COUNTER_BYTES,
        ),
    ],
)
def test_derivation_block_holds_what_its_name_says(name: str, expected: bytes) -> None:
    assert dict(ba.derivation_blocks(COUNTER_VALUE))[name] == expected


def test_first_derivation_block_is_the_specified_one() -> None:
    from custom_components.googlefindmy.FMDNCrypto.eid_generator import (  # noqa: PLC0415
        build_table10_prf_input,
    )

    assert ba.derivation_blocks(COUNTER_VALUE)[0] == (
        "table10",
        build_table10_prf_input(COUNTER_VALUE),
    )
    blocks = ba.derivation_blocks(COUNTER_VALUE)
    assert len({block for _name, block in blocks}) == len(blocks)
    assert {len(block) for _name, block in blocks} == {32}


def _derived_target(key: bytes, block: bytes, order: str, plus_one: bool) -> bytes:
    """Compute a target EID with the curve library, apart from the search."""
    import ecdsa  # noqa: PLC0415

    value = int.from_bytes(ba._prf_any_key(key, block), order)
    curve = ecdsa.SECP160r1
    scalar = (value % (curve.order - 1)) + 1 if plus_one else value % curve.order
    return int((curve.generator * scalar).x()).to_bytes(20, "big")


def _narrow(
    monkeypatch: pytest.MonkeyPatch, keys: list[str], blocks: list[str]
) -> None:
    all_keys = ba.derivation_keys
    all_blocks = ba.derivation_blocks
    monkeypatch.setattr(
        ba,
        "derivation_keys",
        lambda given: [item for item in all_keys(given) if item[0] in keys],
    )
    monkeypatch.setattr(
        ba,
        "derivation_blocks",
        lambda counter: [item for item in all_blocks(counter) if item[0] in blocks],
    )


@pytest.mark.parametrize(
    ("key_name", "block_name", "order", "plus_one", "scalar"),
    [
        ("eik", "table10", "big", False, "big_mod_n"),
        ("eik", "pads_swapped", "little", False, "little_mod_n"),
        ("account_key_raw", "table10", "big", True, "big_plus_one"),
        (
            "sha256_eik_then_1",
            "component_1_second_pad",
            "little",
            True,
            "little_plus_one",
        ),
    ],
)
def test_derivation_search_names_the_forms_it_hit(  # noqa: PLR0913
    monkeypatch: pytest.MonkeyPatch,
    key_name: str,
    block_name: str,
    order: str,
    plus_one: bool,
    scalar: str,
) -> None:
    keys = ba.BeaconKeys(ACCOUNT_KEY, EIK)
    boundary = CLOCK - CLOCK % 1024
    key = dict(ba.derivation_keys(keys))[key_name]
    block = dict(ba.derivation_blocks(boundary))[block_name]
    target = _derived_target(key, block, order, plus_one)
    _narrow(monkeypatch, ["eik", key_name], ["table10", block_name])
    report = ba.search_eid_derivations(keys, CLOCK, {target: "advertised"}, 1)
    assert report["result"] == "match"
    assert report["source"] == "advertised"
    assert report["key"] == key_name
    assert report["block"] == block_name
    assert report["scalar"] == scalar
    assert report["window_offset"] == 0
    assert key.hex() not in repr(report)


@pytest.mark.parametrize("window_offset", [-2, 2])
def test_derivation_search_reaches_its_outer_windows(
    monkeypatch: pytest.MonkeyPatch, window_offset: int
) -> None:
    keys = ba.BeaconKeys(ACCOUNT_KEY, EIK)
    boundary = CLOCK - CLOCK % 1024 + window_offset * 1024
    target = _derived_target(
        EIK, dict(ba.derivation_blocks(boundary))["table10"], "big", False
    )
    _narrow(monkeypatch, ["eik"], ["table10"])
    report = ba.search_eid_derivations(keys, CLOCK, {target: "supplied"}, 2)
    assert report["result"] == "match"
    assert report["window_offset"] == window_offset
    narrow = ba.search_eid_derivations(keys, CLOCK, {target: "supplied"}, 1)
    assert narrow["result"] == "no_match"
    assert narrow["combinations_checked"] == 3 * 1 * 1 * 4


def test_derivation_search_uses_the_boundary_not_the_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = ba.BeaconKeys(ACCOUNT_KEY, EIK)
    target = _derived_target(
        EIK, dict(ba.derivation_blocks(CLOCK))["table10"], "big", False
    )
    assert CLOCK % 1024
    _narrow(monkeypatch, ["eik"], ["table10"])
    report = ba.search_eid_derivations(keys, CLOCK, {target: "supplied"}, 1)
    assert report["result"] == "no_match"


def test_derivation_search_counts_what_it_tried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _narrow(monkeypatch, ["eik", "account_key_raw"], ["table10", "pads_zero"])
    report = ba.search_eid_derivations(
        ba.BeaconKeys(ACCOUNT_KEY, EIK), CLOCK, {bytes(20): "supplied"}, 0
    )
    assert report["result"] == "no_match"
    assert report["keys"] == 2
    assert report["blocks"] == 2
    assert report["scalars"] == 4
    assert report["targets"] == 1
    assert report["combinations_checked"] == 16


def test_derivation_search_needs_a_target_of_the_curve_length() -> None:
    report = ba.search_eid_derivations(
        ba.BeaconKeys(ACCOUNT_KEY, EIK), CLOCK, {bytes(32): "supplied"}, 1
    )
    assert report["result"] == "no_target"
    assert "combinations_checked" not in report


def test_derivation_search_stops_at_the_time_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ticks = iter(range(0, 10_000, 10))
    report = ba.search_eid_derivations(
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        CLOCK,
        {bytes(20): "supplied"},
        1,
        time_budget=25,
        now=lambda: float(next(ticks)),
    )
    assert report["result"] == "time_budget_exhausted"
    assert report["combinations_checked"] == 8


def test_derivation_search_stops_when_asked() -> None:
    stop = threading.Event()
    stop.set()
    report = ba.search_eid_derivations(
        ba.BeaconKeys(ACCOUNT_KEY, EIK), CLOCK, {bytes(20): "supplied"}, 1, stop
    )
    assert report["result"] == "cancelled"
    assert report["combinations_checked"] == 0


def test_derivation_search_skips_counters_out_of_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _narrow(monkeypatch, ["eik"], ["table10"])
    report = ba.search_eid_derivations(
        ba.BeaconKeys(None, EIK), 100, {bytes(20): "supplied"}, 1
    )
    assert report["combinations_checked"] == 8


@pytest.mark.asyncio
async def test_no_derivation_search_runs_unless_asked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _search(*_args: Any) -> dict[str, Any]:
        raise AssertionError("must not run")

    monkeypatch.setattr(ba, "search_eid_derivations", _search)
    _probe_kwargs(monkeypatch, _ConnectedProvider())
    report = await ba.async_probe_tracker(
        _Hass(), ba.BeaconKeys(ACCOUNT_KEY, EIK), address=None, timeout=5
    )
    assert "derivation_search" not in report


@pytest.mark.asyncio
async def test_derivation_search_runs_after_disconnect_with_every_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()
    seen: dict[str, Any] = {}

    def _search(
        _keys: ba.BeaconKeys,
        clock: int,
        targets: dict[bytes, str],
        windows: int,
        _stop: Any,
    ) -> dict[str, Any]:
        seen.update(
            clock=clock,
            sources=sorted(targets.values()),
            windows=windows,
            disconnects=provider.disconnects,
            advertised=LOCATOR_EID in targets,
        )
        return {"result": "no_match"}

    monkeypatch.setattr(ba, "search_eid_derivations", _search)
    _probe_kwargs(monkeypatch, provider)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address=None,
        timeout=5,
        form=ba.IdentityForm.LOCATOR,
        search_eid=bytes(20),
        derivation_windows=3,
    )
    assert report["derivation_search"] == {"result": "no_match"}
    assert seen == {
        "clock": CLOCK,
        "sources": ["advertised", "reported", "supplied"],
        "windows": 3,
        "disconnects": 1,
        "advertised": True,
    }


@pytest.mark.asyncio
async def test_counter_search_also_looks_for_the_advertised_eid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    def _search(
        _eik: bytes, _clock: int, targets: dict[bytes, str], _window: int, _stop: Any
    ) -> dict[str, Any]:
        seen.extend(sorted(targets.values()))
        return {"result": "no_match"}

    monkeypatch.setattr(ba, "search_eid_counter", _search)
    _probe_kwargs(monkeypatch, _ConnectedProvider())
    await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address=None,
        timeout=5,
        form=ba.IdentityForm.LOCATOR,
        search_seconds=9,
    )
    assert seen == ["advertised", "reported"]


@pytest.mark.parametrize(
    ("keys", "reading", "searches", "result"),
    [
        (ba.BeaconKeys(None, None), None, ba._Searches(), "no_key"),
        (ba.BeaconKeys(ACCOUNT_KEY, None), None, ba._Searches(), "no_clock"),
        (
            ba.BeaconKeys(ACCOUNT_KEY, EIK),
            ba._Reading({}, None, (bytes(20),)),
            ba._Searches(),
            "no_clock",
        ),
        (
            ba.BeaconKeys(ACCOUNT_KEY, EIK),
            ba._Reading({}, 5, ()),
            ba._Searches(),
            "no_target",
        ),
    ],
)
@pytest.mark.asyncio
async def test_derivation_search_says_why_it_cannot_run(
    keys: ba.BeaconKeys,
    reading: ba._Reading | None,
    searches: ba._Searches,
    result: str,
) -> None:
    report = await ba._async_derivation_search(
        _Hass(), keys, reading, ba._Target(), searches
    )
    assert report == {"result": result}


@pytest.mark.asyncio
async def test_failed_connection_says_why_the_derivation_search_did_not_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install(monkeypatch, _IdentityBluetooth([]), None)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address=None,
        timeout=5,
        derivation_windows=1,
    )
    assert report["connection"] == "no_matching_frame"
    assert report["derivation_search"] == {"result": "no_clock"}


@pytest.mark.asyncio
async def test_cancelled_caller_stops_the_derivation_search() -> None:
    stops: list[threading.Event] = []
    started = asyncio.Event()

    class _HangingHass(_Hass):
        async def async_add_executor_job(self, _target: Any, *args: Any) -> Any:
            stops.append(args[-1])
            started.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(
        ba._async_derivation_search(
            _HangingHass(),
            ba.BeaconKeys(ACCOUNT_KEY, EIK),
            ba._Reading({}, CLOCK, (bytes(20),)),
            ba._Target(),
            ba._Searches(derivation_windows=1),
        )
    )
    await started.wait()
    assert not stops[0].is_set()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stops[0].is_set()


def test_new_limits_hold_their_values() -> None:
    assert ba.DERIVATION_WINDOWS_MAX == 256
    assert ba.PROVISIONING_TRAILING_LENGTHS == (22, 34)


class _ForeignRingProvider(_LongEidProvider):
    """Holds another EIK, so a verified ``0x01`` meets a rejected ``0x06``."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(eik=bytes(32), **kwargs)


class _ForeignRingPlaceholder(_ForeignRingProvider, _PlaceholderProvider):
    """The same at the identity that answers with the placeholder."""


@pytest.mark.parametrize(
    "provider_class", [_ForeignRingProvider, _ForeignRingPlaceholder]
)
@pytest.mark.asyncio
async def test_form_with_one_more_byte_allows_the_verdict_on_the_eik(
    provider_class: type[_LongEidProvider],
) -> None:
    report = await _run(
        provider_class(trailer=b"\x00"), ba.BeaconKeys(ACCOUNT_KEY, EIK)
    )
    assert report["eik_set"] is True
    assert report["eik_matches"] is False
    state = report["operations"]["read_ringing_state"]
    assert state["status"] == "unauthenticated"


@pytest.mark.asyncio
async def test_a_second_try_does_not_keep_what_the_first_address_advertised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _ConnectedProvider()
    tried: list[str] = []
    seen: list[list[str]] = []

    async def connect(
        _cls: Any, _device: object, name: str, **_kw: Any
    ) -> FakeProvider:
        tried.append(name)
        if name == "...A:02":
            raise ProviderError(0x85)
        return provider

    def _search(
        _keys: ba.BeaconKeys,
        _clock: int,
        targets: dict[bytes, str],
        _windows: int,
        _stop: Any,
    ) -> dict[str, Any]:
        seen.append(sorted(targets.values()))
        return {"result": "no_match"}

    newest_locator = _identity_info(
        "AA:AA:AA:AA:AA:02", seen=5.0, fmdn=b"\x40" + LOCATOR_EID, tx_power=-9
    )
    older_audio = _identity_info(
        "AA:AA:AA:AA:AA:03", seen=4.0, audio=True, tx_power=-10
    )
    monkeypatch.setattr(ba, "search_eid_derivations", _search)
    _install(monkeypatch, _IdentityBluetooth([newest_locator, older_audio]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address=None,
        timeout=30,
        derivation_windows=1,
    )
    assert tried == ["...A:02", "...A:03"]
    assert report["connection"] == "connected"
    assert report["identity_form"] == "audio"
    assert report["advertised_tx_power"] == -10
    assert "advertised_eid_prefix" not in report
    assert "advertised_eid_check" not in report
    assert seen == [["reported"]]


def test_no_sighting_reports_no_identity() -> None:
    report: dict[str, Any] = {"identity_form": "locator", "advertised_eid_prefix": "x"}
    target = ba._Target(eid=LOCATOR_EID)
    ba._note_advertisement(None, report, target)
    assert report["identity_form"] is None
    assert report["advertised_tx_power"] is None
    assert "advertised_eid_prefix" not in report
    assert target.eid is None


def test_a_sighting_without_service_data_is_the_other_form() -> None:
    report: dict[str, Any] = {}
    ba._note_advertisement(SimpleNamespace(rssi=-70), report, ba._Target())
    assert report["identity_form"] == "other"
    assert report["rssi"] == -70


PLACEHOLDER = b"\x00\xff" + bytes(18)


def test_placeholder_is_never_an_eid_candidate() -> None:
    assert ba._eid_candidates(b"\x03" + PLACEHOLDER + bytes(2)) == ()
    assert ba.advertised_eid({ba.FMDN_SERVICE_DATA_UUID: b"\x40" + PLACEHOLDER}) is None


def test_placeholder_is_never_a_search_target() -> None:
    reading = ba._Reading({}, CLOCK, ())
    assert not ba._search_targets(reading, ba._Target(), ba._Searches(1, PLACEHOLDER))


@pytest.mark.asyncio
async def test_placeholder_locator_searches_its_advertised_eid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _PlaceholderProvider(trailer=b"\x00")
    seen: list[tuple[list[str], list[bytes]]] = []

    def _record(*args: Any) -> dict[str, Any]:
        seen.append((sorted(args[2].values()), list(args[2])))
        return {"result": "no_match"}

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        return provider

    monkeypatch.setattr(ba, "search_eid_counter", _record)
    monkeypatch.setattr(ba, "search_eid_derivations", _record)
    _install(monkeypatch, _IdentityBluetooth([LOCATOR_INFO]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address=None,
        timeout=5,
        search_seconds=1,
        derivation_windows=1,
    )
    state = report["operations"]["read_provisioning_state"]
    assert state["eid_placeholder"] is True
    assert report["identity_form"] == "locator"
    assert report["eid_check"] == {"result": "not_checked"}
    assert report["advertised_eid_check"]["result"] == "no_match"
    assert seen == [(["advertised"], [LOCATOR_EID])] * 2
    assert report["eid_search"] == report["derivation_search"] == {"result": "no_match"}


def test_a_zero_scalar_is_a_failed_attempt_not_a_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from custom_components.googlefindmy.FMDNCrypto import (  # noqa: PLC0415
        curve_profile,
        eid_generator,
    )

    def _no_point(_scalar: int) -> bytes:
        raise AssertionError("a zero scalar has no point")

    monkeypatch.setattr(curve_profile, "reduce_scalar", lambda *_args: 0)
    monkeypatch.setattr(eid_generator, "_serialize_legacy_x", _no_point)
    _narrow(monkeypatch, ["eik"], ["table10"])
    report = ba.search_eid_derivations(
        ba.BeaconKeys(None, EIK), CLOCK, {bytes(20): "supplied"}, 0
    )
    assert report["result"] == "no_match"
    assert report["combinations_checked"] == 4


def test_a_key_that_repeats_keeps_its_first_name() -> None:
    names = dict(ba.derivation_keys(ba.BeaconKeys(None, bytes(32))))
    assert "eik" in names
    assert "eik_reversed" not in names


def _placeholder_report(data: bytes) -> tuple[dict[str, Any], tuple[bytes, ...]]:
    idle = ba._Operation(ba.OperationStatus.NOT_ATTEMPTED)
    report, _eik, _owner, eids, _known = ba._provisioning_report(
        ba._Runs(
            parameters=idle,
            parameters_key=None,
            provisioning=ba._Operation(ba.OperationStatus.OK, data),
            ringing=idle,
            key_form=None,
        )
    )
    return report, eids


@pytest.mark.parametrize("field", [18, 30])
@pytest.mark.parametrize("trailer", [b"", b"\x00", b"\x00\x00"])
def test_placeholder_is_reported_in_every_response_length(
    field: int, trailer: bytes
) -> None:
    report, eids = _placeholder_report(b"\x03\x00\xff" + bytes(field) + trailer)
    assert report["eid_placeholder"] is True
    assert eids == ()


def test_a_real_eid_in_an_unknown_length_is_no_placeholder() -> None:
    report, eids = _placeholder_report(b"\x03" + LOCATOR_EID + b"\x00\x00")
    assert "eid_placeholder" not in report
    assert eids == (LOCATOR_EID,)


def test_a_reported_eid_that_is_also_advertised_stays_reported() -> None:
    targets = ba._search_targets(
        ba._Reading({}, CLOCK, (LOCATOR_EID,)),
        ba._Target(eid=LOCATOR_EID),
        ba._Searches(),
    )
    assert targets == {LOCATOR_EID: "reported"}


def test_derivation_search_stays_below_the_top_of_the_counter_range(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _narrow(monkeypatch, ["eik"], ["table10"])
    report = ba.search_eid_derivations(
        ba.BeaconKeys(None, EIK), 0xFFFFFFFF, {bytes(20): "supplied"}, 1
    )
    assert report["result"] == "no_match"
    assert report["combinations_checked"] == 8


def test_prf_with_a_short_key_gives_the_published_vector() -> None:
    # FIPS 197, appendix C.1 (AES-128).
    output = ba._prf_any_key(
        bytes.fromhex("000102030405060708090a0b0c0d0e0f"),
        bytes.fromhex("00112233445566778899aabbccddeeff"),
    )
    assert output.hex() == "69c4e0d86a7b0430d8cdb78070b4c55a"


def test_prf_with_a_long_key_gives_the_published_vector() -> None:
    # FIPS 197, appendix C.3 (AES-256).
    output = ba._prf_any_key(
        bytes(range(32)), bytes.fromhex("00112233445566778899aabbccddeeff")
    )
    assert output.hex() == "8ea2b7ca516745bfeafc49904b496089"
