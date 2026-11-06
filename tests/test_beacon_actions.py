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
async def test_session_rejects_a_foreign_data_id_before_it_writes() -> None:
    provider = FakeProvider()

    session = ba._BeaconSession(provider, 1.0)
    with pytest.raises(ba.UnsupportedDataIdError):
        await session.run(0x05, ACCOUNT_KEY)  # type: ignore[arg-type]
    assert [c for c in provider.calls if c[0] == "write"] == []


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
        ("error: 0x80", 0x80),
        ("nothing", None),
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


@pytest.mark.parametrize(
    ("connected", "expected"), [(False, "timeout"), (True, "operation_timeout")]
)
@pytest.mark.asyncio
async def test_probe_reports_the_phase_of_a_timeout(
    monkeypatch: pytest.MonkeyPatch, connected: bool, expected: str
) -> None:
    provider = _ConnectedProvider()

    async def connect(*_args: Any, **_kw: Any) -> FakeProvider:
        if not connected:
            raise TimeoutError
        return provider

    async def timing_out(*_args: Any, **_kw: Any) -> dict[str, Any]:
        raise TimeoutError

    monkeypatch.setattr(ba, "read_beacon_state", timing_out)
    _install(monkeypatch, _Bluetooth([]), connect)
    report = await ba.async_probe_tracker(
        _Hass(),
        ba.BeaconKeys(ACCOUNT_KEY, EIK),
        address="AA:BB:CC:DD:EE:FF",
        timeout=5,
    )
    assert report["connection"] == expected


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
    report = ba._build_report(ba.BeaconKeys(ACCOUNT_KEY, EIK), operation)
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
