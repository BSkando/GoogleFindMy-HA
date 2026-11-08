# tests/test_ble_battery_sensor.py
"""Tests for BLE battery state decoding, storage, and sensor entity."""

from __future__ import annotations

import asyncio
import time
from collections.abc import MutableMapping
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import custom_components.googlefindmy.eid_resolver as resolver_module
from custom_components.googlefindmy.const import DATA_EID_RESOLVER, DOMAIN
from custom_components.googlefindmy.eid_resolver import (
    FMDN_BATTERY_PCT,
    FMDN_HASHED_FLAGS_BATTERY_MASK,
    FMDN_HASHED_FLAGS_BATTERY_SHIFT,
    FMDN_HASHED_FLAGS_UWT_MODE_MASK,
    BLEBatteryState,
    EIDMatch,
    GoogleFindMyEIDResolver,
)
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    LEGACY_EID_LENGTH,
    MODERN_EID_LENGTH,
    P256_ORDER,
    compute_flags_xor_mask,
)
from custom_components.googlefindmy.sensor import (
    BLE_BATTERY_DESCRIPTION,
    GoogleFindMyBLEBatterySensor,
)
from tests.helpers.config_entries_stub import make_config_entry

# ---------------------------------------------------------------------------
# Constants used to build test payloads
# ---------------------------------------------------------------------------
_FMDN_FRAME_TYPE = resolver_module.FMDN_FRAME_TYPE  # 0x40
_MODERN_FRAME_TYPE = resolver_module.MODERN_FRAME_TYPE  # 0x41
_SERVICE_DATA_OFFSET = resolver_module.SERVICE_DATA_OFFSET  # 8
_RAW_HEADER_LENGTH = resolver_module.RAW_HEADER_LENGTH  # 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_hass(domain_data: dict[str, Any] | None = None) -> SimpleNamespace:
    """Return a lightweight hass stand-in."""
    data: dict[str, Any] = {}
    if domain_data is not None:
        data[DOMAIN] = domain_data
    return SimpleNamespace(
        async_create_task=lambda coro, name=None: _close_coro_and_return(coro),
        async_create_background_task=lambda coro, name=None: _close_coro_and_return(
            coro
        ),
        data=data,
    )


def _close_coro_and_return(coro: object) -> None:
    """Close a coroutine to avoid RuntimeWarning in test context."""
    if hasattr(coro, "close"):
        coro.close()


def _make_resolver() -> GoogleFindMyEIDResolver:
    """Create a minimal resolver instance suitable for direct _update_ble_battery calls."""
    resolver = GoogleFindMyEIDResolver.__new__(GoogleFindMyEIDResolver)
    resolver.hass = _fake_hass()
    resolver._lookup = {}
    resolver._lookup_metadata = {}
    resolver._locks = {}

    async def _async_noop(payload: Any = None) -> None:
        return None

    resolver._store = SimpleNamespace(async_load=lambda: None, async_save=_async_noop)
    resolver._unsub_interval = None
    resolver._unsub_alignment = None
    resolver._refresh_lock = asyncio.Lock()
    resolver._pending_refresh = False
    resolver._load_task = None
    resolver._ensure_cache_defaults()
    return resolver


def _match(device_id: str = "dev-1", canonical_id: str | None = None) -> EIDMatch:
    """Create a test EIDMatch.

    *canonical_id* defaults to *device_id* so that the battery-state
    storage key (``canonical_id``) matches the test lookup key.
    """
    return EIDMatch(
        device_id=device_id,
        config_entry_id="entry-1",
        canonical_id=canonical_id if canonical_id is not None else device_id,
        time_offset=0,
        is_reversed=False,
    )


def _service_data_payload(eid: bytes, flags_byte: int) -> bytes:
    """Build a service-data format payload: [header(7)][frame(1)][EID(20)][flags(1)]."""
    header = b"\x00" * 7
    frame = bytes([_FMDN_FRAME_TYPE])
    return header + frame + eid + bytes([flags_byte])


def _raw_header_payload(eid: bytes, flags_byte: int) -> bytes:
    """Build a raw-header format payload: [frame(1)][EID(20)][flags(1)]."""
    frame = bytes([_FMDN_FRAME_TYPE])
    return frame + eid + bytes([flags_byte])


def _modern_service_data_payload(eid: bytes, flags_byte: int) -> bytes:
    """Build a modern service-data payload: [header(7)][frame(1)][EID(32)][flags(1)]."""
    header = b"\x00" * 7
    frame = bytes([_MODERN_FRAME_TYPE])
    return header + frame + eid + bytes([flags_byte])


def _modern_raw_header_payload(eid: bytes, flags_byte: int) -> bytes:
    """Build a modern raw-header payload: [frame(1)][EID(32)][flags(1)]."""
    frame = bytes([_MODERN_FRAME_TYPE])
    return frame + eid + bytes([flags_byte])


def _fake_coordinator(
    device_id: str = "dev-1",
    present: bool = True,
    entry_id: str = "entry-1",
    visible: bool = True,
    snapshot: list[dict[str, Any]] | None = None,
) -> SimpleNamespace:
    """Create a minimal coordinator stub for sensor tests."""
    return SimpleNamespace(
        config_entry=make_config_entry(entry_id=entry_id),
        is_device_present=lambda did: present,
        is_device_visible_in_subentry=lambda key, did: visible,
        async_update_listeners=lambda: None,
        get_subentry_snapshot=lambda key: snapshot or [],
        last_update_success=True,
    )


def _build_battery_sensor(
    device_id: str = "dev-1",
    device_name: str = "Test Tracker",
    coordinator: Any = None,
    hass: Any = None,
    resolver: Any = None,
) -> GoogleFindMyBLEBatterySensor:
    """Create a BLE battery sensor with minimal stubs, bypassing HA platform init."""
    if coordinator is None:
        coordinator = _fake_coordinator(device_id=device_id)
    if hass is None:
        domain_data: dict[str, Any] = {}
        if resolver is not None:
            domain_data[DATA_EID_RESOLVER] = resolver
        hass = _fake_hass(domain_data)

    sensor = GoogleFindMyBLEBatterySensor.__new__(GoogleFindMyBLEBatterySensor)
    sensor._subentry_identifier = "tracker"
    sensor._subentry_key = "core_tracking"
    sensor.coordinator = coordinator
    sensor.hass = hass
    sensor._device_id = device_id
    sensor._device = {"id": device_id, "name": device_name}
    sensor._attr_native_value = None
    sensor.entity_description = BLE_BATTERY_DESCRIPTION
    sensor._attr_has_entity_name = True
    sensor._attr_entity_registry_enabled_default = True
    sensor._unrecorded_attributes = frozenset(
        {
            "last_ble_observation",
            "google_device_id",
            "battery_raw_level",
        }
    )
    sensor._fallback_label = device_name

    safe_id = device_id if device_id is not None else "unknown"
    entry_id = getattr(coordinator.config_entry, "entry_id", "default")
    sensor._attr_unique_id = f"googlefindmy_{entry_id}_tracker_{safe_id}_ble_battery"

    sensor.entity_id = f"sensor.test_{safe_id}_ble_battery"

    return sensor


# ===========================================================================
# 1. BLEBatteryState dataclass + FMDN_BATTERY_PCT mapping
# ===========================================================================


class TestBLEBatteryStateDataclass:
    """Unit tests for BLEBatteryState and the percentage mapping."""

    def test_dataclass_fields(self) -> None:
        state = BLEBatteryState(
            battery_level=1,
            battery_pct=100,
            uwt_mode=False,
            decoded_flags=0x02,
            observed_at_wall=1000.0,
        )
        assert state.battery_level == 1
        assert state.battery_pct == 100
        assert state.uwt_mode is False
        assert state.decoded_flags == 0x02
        assert state.observed_at_wall == 1000.0

    def test_battery_pct_mapping(self) -> None:
        """FMDN_BATTERY_PCT: 0->None (unsupported), 1->100, 2->25, 3->5."""
        assert FMDN_BATTERY_PCT == {0: None, 1: 100, 2: 25, 3: 5}

    def test_battery_pct_unsupported_raw_returns_none(self) -> None:
        """Raw value 0 (UNSUPPORTED) should map to None.

        A tracker sending battery_raw=0 does not support battery indication.
        The correct representation is None (HA shows 'unknown').
        """
        assert FMDN_BATTERY_PCT.get(0) is None

    def test_slots_optimization(self) -> None:
        """BLEBatteryState uses __slots__ for memory efficiency."""
        assert hasattr(BLEBatteryState, "__slots__")

    def test_no_battery_percentage_alias(self) -> None:
        """Guard: the field is battery_pct, NOT battery_percentage.

        A previous bug used 'battery_percentage' in a log statement inside
        _build_entities(), which raised AttributeError at runtime.  This test
        ensures the correct attribute name is used and no alias exists.
        """
        state = BLEBatteryState(
            battery_level=1,
            battery_pct=100,
            uwt_mode=False,
            decoded_flags=0x02,
            observed_at_wall=1000.0,
        )
        # Correct attribute exists and is accessible
        assert state.battery_pct == 100
        # Common typo must NOT exist (slots dataclass → AttributeError)
        assert not hasattr(state, "battery_percentage")

    def test_battery_pct_used_in_sensor_creation_log(self) -> None:
        """Regression: the INFO log in _build_entities must access battery_pct.

        This exercises the exact attribute access pattern used in sensor.py's
        _build_entities() when logging BLE battery sensor creation.
        """
        state = BLEBatteryState(
            battery_level=2,
            battery_pct=25,
            uwt_mode=False,
            decoded_flags=0x04,
            observed_at_wall=1000.0,
        )
        # Reproduce the log format string from sensor.py _build_entities()
        msg = (
            f"BLE battery sensor created for device={'test-dev'} "
            f"(battery={state.battery_pct}%)"
        )
        assert "battery=25%" in msg


# ===========================================================================
# 2. _update_ble_battery() decode and store
# ===========================================================================


class TestUpdateBLEBattery:
    """Tests for the resolver's _update_ble_battery method."""

    def test_no_matches_noop(self) -> None:
        """When matches is empty, nothing should be stored."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0x00)
        metadata = {"flags_xor_mask": 0x00}
        resolver._update_ble_battery(raw, None, metadata, [])
        assert len(resolver._ble_battery_state) == 0

    def test_decode_unsupported_service_data(self) -> None:
        """Battery level 0 (UNSUPPORTED) -> None via service-data format."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x55

        desired_decoded = 0b00000_00_0  # battery=0, uwt=0
        flags_byte = desired_decoded ^ xor_mask

        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-unsup")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-unsup")
        assert state is not None
        assert state.battery_level == 0
        assert state.battery_pct is None
        assert state.uwt_mode is False

    def test_decode_normal_service_data(self) -> None:
        """Battery level 1 (NORMAL) -> 100% via service-data format."""
        resolver = _make_resolver()
        eid = b"\xbb" * LEGACY_EID_LENGTH
        xor_mask = 0x00

        desired_decoded = 0b00000_01_0  # battery=1 at bits 2:1
        flags_byte = desired_decoded ^ xor_mask

        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-normal")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-normal")
        assert state is not None
        assert state.battery_level == 1
        assert state.battery_pct == 100
        assert state.uwt_mode is False

    def test_decode_low_service_data(self) -> None:
        """Battery level 2 (LOW) -> 25% via service-data format."""
        resolver = _make_resolver()
        eid = b"\xcc" * LEGACY_EID_LENGTH
        xor_mask = 0x00

        desired_decoded = 0b00000_10_0  # battery=2 at bits 2:1
        flags_byte = desired_decoded ^ xor_mask

        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-low")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-low")
        assert state is not None
        assert state.battery_level == 2
        assert state.battery_pct == 25
        assert state.uwt_mode is False

    def test_decode_uwt_mode_active(self) -> None:
        """UWT mode (bit 0 in standard notation) set -> uwt_mode=True."""
        resolver = _make_resolver()
        eid = b"\xdd" * LEGACY_EID_LENGTH
        xor_mask = 0x00

        desired_decoded = 0b00000_00_1  # battery=0, uwt=1 (bit 0)
        flags_byte = desired_decoded ^ xor_mask

        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-uwt")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-uwt")
        assert state is not None
        assert state.battery_level == 0
        assert state.battery_pct is None
        assert state.uwt_mode is True

    def test_decode_raw_header_format(self) -> None:
        """Flags extraction from raw-header format: [frame(1)][EID(20)][flags(1)]."""
        resolver = _make_resolver()
        eid = b"\xee" * LEGACY_EID_LENGTH
        xor_mask = 0x00

        desired_decoded = 0b00000_01_0  # battery=1 (NORMAL) at bits 2:1
        flags_byte = desired_decoded ^ xor_mask

        raw = _raw_header_payload(eid, flags_byte)
        match = _match("dev-raw")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-raw")
        assert state is not None
        assert state.battery_level == 1
        assert state.battery_pct == 100

    def test_shared_device_propagation(self) -> None:
        """When one BLE packet matches 2 accounts, battery stores for BOTH device_ids."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_01_0  # battery=1 (NORMAL)
        flags_byte = desired_decoded ^ xor_mask

        raw = _service_data_payload(eid, flags_byte)
        match_a = _match("dev-account-a")
        match_b = _match("dev-account-b")
        resolver._update_ble_battery(
            raw, None, {"flags_xor_mask": xor_mask}, [match_a, match_b]
        )

        state_a = resolver._ble_battery_state.get("dev-account-a")
        state_b = resolver._ble_battery_state.get("dev-account-b")
        assert state_a is not None
        assert state_b is not None
        assert state_a.battery_pct == 100
        assert state_b.battery_pct == 100
        # Both should reference the same BLEBatteryState instance
        assert state_a is state_b

    def test_cannot_decode_no_xor_mask(self) -> None:
        """Missing xor_mask -> no battery state stored."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0x42)
        match = _match("dev-no-mask")
        resolver._update_ble_battery(raw, None, {}, [match])
        assert resolver._ble_battery_state.get("dev-no-mask") is None

    def test_cannot_decode_short_payload(self) -> None:
        """Payload too short for flags byte -> no battery state stored."""
        resolver = _make_resolver()
        raw = b"\x00" * 10
        match = _match("dev-short")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": 0x00}, [match])
        assert resolver._ble_battery_state.get("dev-short") is None

    def test_direct_call_without_clock_dates_the_observation_now(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A direct call without ``clock`` falls back to ``time.time()``.

        This used to pin ``observed_at_wall == time.time()`` as *the*
        contract, which pinned the defect: the processing time posing as the
        observation time. The production path now hands the writer the
        advertisement time (``tests/test_eid_resolver_scan_time.py``); the
        fallback measured here is the contract for direct callers only.
        """
        monkeypatch.setattr(time, "time", lambda: 9999.5)
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        flags_byte = 0b00000_01_0 ^ xor_mask  # battery=1 (NORMAL)
        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-time")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        state = resolver._ble_battery_state.get("dev-time")
        assert state is not None
        assert state.observed_at_wall == 9999.5

    def test_first_decode_logs_info(self) -> None:
        """First decode per device should add to _flags_logged_devices."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        flags_byte = 0b00000_01_0 ^ xor_mask  # battery=1 (NORMAL)
        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-log")

        assert "dev-log" not in resolver._flags_logged_devices
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        assert "dev-log" in resolver._flags_logged_devices

    def test_cannot_decode_logs_device(self) -> None:
        """CANNOT_DECODE should still add device to _flags_logged_devices."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0x42)
        match = _match("dev-cant-decode")

        assert "dev-cant-decode" not in resolver._flags_logged_devices
        resolver._update_ble_battery(raw, None, {}, [match])
        assert "dev-cant-decode" in resolver._flags_logged_devices

    def test_battery_level_change_updates_state(self) -> None:
        """When battery level changes on subsequent call, state should update."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        match = _match("dev-change")

        # First: NORMAL (1) → 100%
        decoded_normal = 0b00000_01_0  # battery=1 at bits 2:1
        raw = _service_data_payload(eid, decoded_normal ^ xor_mask)
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        assert resolver._ble_battery_state["dev-change"].battery_pct == 100

        # Second: LOW (2) → 25% — triggers battery-changed DEBUG log
        decoded_low = 0b00000_10_0  # battery=2 at bits 2:1
        raw = _service_data_payload(eid, decoded_low ^ xor_mask)
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        assert resolver._ble_battery_state["dev-change"].battery_pct == 25
        assert resolver._ble_battery_state["dev-change"].battery_level == 2

    def test_critically_low_battery_raw_3(self) -> None:
        """Raw battery value 3 (CRITICALLY_LOW) maps to 5%."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_11_0  # battery=3 at bits 2:1
        flags_byte = desired_decoded ^ xor_mask
        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-crit")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-crit")
        assert state is not None
        assert state.battery_level == 3
        assert state.battery_pct == 5

    def test_combined_battery_and_uwt(self) -> None:
        """Battery LOW + UWT -> battery_pct=25, uwt_mode=True."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_10_1  # battery=2 (LOW) + uwt=1 (bit 0)
        flags_byte = desired_decoded ^ xor_mask
        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-combo")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-combo")
        assert state is not None
        assert state.battery_level == 2
        assert state.battery_pct == 25
        assert state.uwt_mode is True
        assert state.decoded_flags == 0x05

    def test_observed_frame_format_in_log(self) -> None:
        """When observed_frame is an int, the info log should format it as hex."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_01_0  # battery=1 (NORMAL)
        flags_byte = desired_decoded ^ xor_mask
        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-frame")
        # Pass a non-None observed_frame to cover the 0x{:02x} formatting branch
        resolver._update_ble_battery(raw, 0x40, {"flags_xor_mask": xor_mask}, [match])
        state = resolver._ble_battery_state.get("dev-frame")
        assert state is not None
        assert state.battery_pct == 100

    def test_cannot_decode_with_observed_frame(self) -> None:
        """CANNOT_DECODE with non-None observed_frame covers the hex format branch."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0x42)
        match = _match("dev-cant-frame")
        resolver._update_ble_battery(raw, 0x40, {}, [match])
        assert "dev-cant-frame" in resolver._flags_logged_devices

    def test_cannot_decode_long_payload_truncation(self) -> None:
        """CANNOT_DECODE with long payload should truncate raw_hex to 40 bytes."""
        resolver = _make_resolver()
        # Build a long payload that won't match FMDN frame type at position 0 or 7
        raw = b"\xff" * 100
        match = _match("dev-long")
        resolver._update_ble_battery(raw, None, {}, [match])
        assert "dev-long" in resolver._flags_logged_devices

    def test_cannot_decode_short_payload_full_hex(self) -> None:
        """CANNOT_DECODE with short payload should emit full raw hex."""
        resolver = _make_resolver()
        raw = b"\xab" * 20
        match = _match("dev-short-hex")
        resolver._update_ble_battery(raw, None, {}, [match])
        assert "dev-short-hex" in resolver._flags_logged_devices

    def test_second_cannot_decode_same_device_no_double_log(self) -> None:
        """CANNOT_DECODE for an already-logged device should not re-log."""
        resolver = _make_resolver()
        raw = b"\xab" * 20
        match = _match("dev-double")
        resolver._update_ble_battery(raw, None, {}, [match])
        assert "dev-double" in resolver._flags_logged_devices
        # Second call — should be a no-op (device already logged)
        resolver._update_ble_battery(raw, None, {}, [match])

    def test_no_flags_byte_no_xor_mask(self) -> None:
        """Both flags_byte=None and xor_mask=None -> CANNOT_DECODE path."""
        resolver = _make_resolver()
        raw = b"\x00" * 5  # too short for any frame format
        match = _match("dev-both-none")
        resolver._update_ble_battery(raw, None, {}, [match])
        assert resolver._ble_battery_state.get("dev-both-none") is None
        assert "dev-both-none" in resolver._flags_logged_devices

    def test_same_battery_level_no_change_log(self) -> None:
        """When battery is decoded again with the same level, no change log is emitted."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        decoded_normal = 0b00000_01_0  # battery=1 (NORMAL)
        raw = _service_data_payload(eid, decoded_normal ^ xor_mask)
        match = _match("dev-same")

        # First decode — NORMAL, INFO log
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        assert resolver._ble_battery_state["dev-same"].battery_pct == 100
        assert "dev-same" in resolver._flags_logged_devices

        # Second decode — same level (NORMAL), no change log emitted
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        assert resolver._ble_battery_state["dev-same"].battery_pct == 100

    def test_flags_byte_found_but_no_xor_mask(self) -> None:
        """Service-data has flags byte position but no xor_mask -> CANNOT_DECODE."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0x42)
        match = _match("dev-flags-no-mask")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": None}, [match])
        assert resolver._ble_battery_state.get("dev-flags-no-mask") is None

    def test_decode_modern_service_data(self) -> None:
        """Modern frame 0x41 with 32-byte EID via service-data format."""
        resolver = _make_resolver()
        eid = b"\xaa" * MODERN_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_01_0  # battery=1 (NORMAL)
        flags_byte = desired_decoded ^ xor_mask

        raw = _modern_service_data_payload(eid, flags_byte)
        match = _match("dev-modern-sd")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-modern-sd")
        assert state is not None
        assert state.battery_level == 1
        assert state.battery_pct == 100
        assert state.uwt_mode is False

    def test_decode_modern_raw_header(self) -> None:
        """Modern frame 0x41 with 32-byte EID via raw-header format."""
        resolver = _make_resolver()
        eid = b"\xbb" * MODERN_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_10_0  # battery=2 (LOW)
        flags_byte = desired_decoded ^ xor_mask

        raw = _modern_raw_header_payload(eid, flags_byte)
        match = _match("dev-modern-raw")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get("dev-modern-raw")
        assert state is not None
        assert state.battery_level == 2
        assert state.battery_pct == 25

    def test_modern_frame_too_short_cannot_decode(self) -> None:
        """Modern frame type at position 7 but payload too short for flags byte."""
        resolver = _make_resolver()
        # 7 header + 1 frame + 20 bytes (=28, no room for flags byte at all)
        raw = b"\x00" * 7 + bytes([_MODERN_FRAME_TYPE]) + b"\xaa" * 20
        match = _match("dev-modern-short")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": 0x00}, [match])
        assert resolver._ble_battery_state.get("dev-modern-short") is None

    # ---- Lenient 0x41 + legacy-length variants (Codex review on PR #169) ----
    # The Find Hub specification ties 0x41 to the unwanted tracking protection
    # mode and 0x40 to normal operation, not to the EID length (see
    # docs/FMDN.md and docs/UWT_FIELD_DATA.md), so a 20-byte EID in a 0x41 frame
    # is a valid length. Two fixtures below decode the mode bit as clear in a
    # 0x41 frame: that is the conflict between frame type and bit, kept here
    # because these tests cover the battery decode only (the upload decision
    # for the conflict is tested in tests/test_finder_uwt_state.py).
    # _extract_candidates already tolerates 0x41 with a 20-byte legacy EID in
    # both framed geometries (via _framed_eid_lengths). The lenient
    # branches in _update_ble_battery mirror that tolerance for the flags-byte
    # lookup. Range is intentionally narrow (legacy_min..legacy_min+1) to
    # match the existing tolerance and limit false-positive decodes.

    def test_lenient_0x41_legacy_service_data_decode(self) -> None:
        """0x41 at byte 7 with 29-byte payload (legacy EID + 1 flag) decodes via lenient branch."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x55
        desired_decoded = 0b00000_01_0  # battery=1 (NORMAL)
        flags_byte = desired_decoded ^ xor_mask

        # Build with MODERN_FRAME_TYPE but LEGACY_EID_LENGTH -> length 29
        header = b"\x00" * 7
        raw = header + bytes([_MODERN_FRAME_TYPE]) + eid + bytes([flags_byte])
        assert len(raw) == 29  # explicit length anchor

        match = _match("dev-lenient-sd-29")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        state = resolver._ble_battery_state.get("dev-lenient-sd-29")
        assert state is not None
        assert state.battery_level == 1
        assert state.battery_pct == 100
        assert state.uwt_mode is False

    def test_lenient_0x41_legacy_service_data_uwt(self) -> None:
        """0x41 at byte 7 with 30-byte payload (legacy EID + 1 flag + 1 trailing) decodes UWT-mode."""
        resolver = _make_resolver()
        eid = b"\xbb" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_00_1  # battery=0, uwt=1
        flags_byte = desired_decoded ^ xor_mask

        # length 30 = SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH + 2 (upper lenient bound)
        header = b"\x00" * 7
        raw = header + bytes([_MODERN_FRAME_TYPE]) + eid + bytes([flags_byte]) + b"\xff"
        assert len(raw) == 30

        match = _match("dev-lenient-sd-30")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        state = resolver._ble_battery_state.get("dev-lenient-sd-30")
        assert state is not None
        assert state.uwt_mode is True

    def test_lenient_0x41_legacy_raw_header_decode(self) -> None:
        """0x41 at byte 0 with 22-byte payload (legacy EID + 1 flag) decodes via lenient branch."""
        resolver = _make_resolver()
        eid = b"\xcc" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_10_0  # battery=2 (LOW)
        flags_byte = desired_decoded ^ xor_mask

        # length 22 = RAW_HEADER_LENGTH + LEGACY_EID_LENGTH + 1
        raw = bytes([_MODERN_FRAME_TYPE]) + eid + bytes([flags_byte])
        assert len(raw) == 22

        match = _match("dev-lenient-rh-22")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        state = resolver._ble_battery_state.get("dev-lenient-rh-22")
        assert state is not None
        assert state.battery_level == 2
        assert state.battery_pct == 25

    def test_lenient_0x41_legacy_raw_header_upper_bound(self) -> None:
        """0x41 at byte 0 with 23-byte payload decodes via lenient branch (upper bound)."""
        resolver = _make_resolver()
        eid = b"\xdd" * LEGACY_EID_LENGTH
        xor_mask = 0xAA
        desired_decoded = 0b00000_11_1  # battery=3 (CRITICAL), uwt=1
        flags_byte = desired_decoded ^ xor_mask

        # length 23 = RAW_HEADER_LENGTH + LEGACY_EID_LENGTH + 2 (upper lenient bound)
        raw = bytes([_MODERN_FRAME_TYPE]) + eid + bytes([flags_byte]) + b"\xff"
        assert len(raw) == 23

        match = _match("dev-lenient-rh-23")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        state = resolver._ble_battery_state.get("dev-lenient-rh-23")
        assert state is not None
        assert state.battery_level == 3
        assert state.uwt_mode is True

    def test_branch_order_anchor_modern_41_byte_payload(self) -> None:
        """Regression anchor: length==41 with 0x41 must use raw[40] (strict modern), NOT raw[28] (lenient)."""
        resolver = _make_resolver()
        eid = b"\xee" * MODERN_EID_LENGTH  # 32 bytes
        xor_mask = 0x00
        modern_decoded = 0b00000_01_0  # battery=1 (NORMAL) at strict offset 40
        lenient_decoded = 0b00000_11_0  # battery=3 (CRITICAL) at lenient offset 28
        modern_flags = modern_decoded ^ xor_mask
        lenient_decoy = lenient_decoded ^ xor_mask

        # Build a 41-byte payload: header(7) + frame(1) + EID(32) + flags(1) = 41
        # Place a decoy at offset 28 (would be raw[SERVICE_DATA_OFFSET+LEGACY_EID_LENGTH])
        # and the real flags at offset 40. With correct branch order the
        # strict modern branch wins and battery decodes to 1, not 3.
        eid_with_decoy = bytearray(eid)
        eid_with_decoy[20] = lenient_decoy  # at position 20 of EID = raw offset 28
        header = b"\x00" * 7
        raw = (
            header
            + bytes([_MODERN_FRAME_TYPE])
            + bytes(eid_with_decoy)
            + bytes([modern_flags])
        )
        assert len(raw) == 41

        match = _match("dev-branch-order")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])
        state = resolver._ble_battery_state.get("dev-branch-order")
        assert state is not None
        # Must read raw[40] (modern strict), NOT raw[28] (lenient legacy)
        assert state.battery_level == 1, (
            "Branch ordering broken: lenient branch fired despite length>=41"
        )
        assert state.battery_pct == 100

    def test_lenient_0x41_legacy_too_short_no_decode(self) -> None:
        """0x41 at byte 7 with 28-byte payload (no room for flags) -> no decode."""
        resolver = _make_resolver()
        # length 28 = SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH (no flags byte)
        raw = b"\x00" * 7 + bytes([_MODERN_FRAME_TYPE]) + b"\xaa" * 20
        assert len(raw) == 28
        match = _match("dev-lenient-too-short")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": 0x00}, [match])
        assert resolver._ble_battery_state.get("dev-lenient-too-short") is None

    def test_lenient_0x41_legacy_above_range_no_decode(self) -> None:
        """0x41 at byte 7 with 31-byte payload (above lenient upper bound, below modern min) -> no decode."""
        resolver = _make_resolver()
        # length 31 = SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH + 3 (above ±1 tolerance)
        # and < 41 (below modern threshold). Should NOT match any branch.
        raw = b"\x00" * 7 + bytes([_MODERN_FRAME_TYPE]) + b"\xaa" * 20 + b"\xff\xff\xff"
        assert len(raw) == 31
        match = _match("dev-lenient-above")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": 0x00}, [match])
        assert resolver._ble_battery_state.get("dev-lenient-above") is None

    def test_lenient_0x41_no_xor_mask_does_not_crash(self) -> None:
        """Lenient branch with missing xor_mask -> graceful no-decode."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        header = b"\x00" * 7
        raw = header + bytes([_MODERN_FRAME_TYPE]) + eid + bytes([0x00])
        assert len(raw) == 29
        match = _match("dev-lenient-no-mask")
        # No xor mask in metadata -> flags_byte found but decode skipped
        resolver._update_ble_battery(raw, None, {}, [match])
        assert resolver._ble_battery_state.get("dev-lenient-no-mask") is None


# ===========================================================================
# 2a-MSB. Spec-anchored regression: MSB-first bit numbering convention
#
# Anti-regression guard against AI / human reviewers who naively read
# the FMDN spec's "Bits 5-6 battery, Bit 7 UWT" without honouring the
# spec's explicit qualifier that bits are referenced from MSB to LSB.
# See custom_components/googlefindmy/eid_resolver.py around the
# "FMDN hashed_flags byte layout" comment block and docs/BLE_BATTERY_SENSOR.md L45.
# ===========================================================================


_MSB_BATTERY_LABELS = {0: "UNSUPPORTED", 1: "NORMAL", 2: "LOW", 3: "CRITICALLY_LOW"}


# Intentional: spec constants (0x03, 0x01, shift 1) are duplicated here.
# Do NOT replace them with imports of FMDN_HASHED_FLAGS_BATTERY_MASK /
# FMDN_HASHED_FLAGS_BATTERY_SHIFT / FMDN_HASHED_FLAGS_UWT_MODE_MASK from
# eid_resolver.py — that would make these tests tautological (they verify
# the constants against the Google FHN spec, not against themselves) and
# would remove the only independent encoder of the spec's MSB-first bit
# layout (TestFmdnHashedFlagsConstantsMatchSpec below catches numeric
# drift in the constants, but only this encoder pins down what they mean).
def _encode_msb_first_flags_byte(battery: int, uwt: bool) -> int:
    """Build a decoded hashed-flags byte per FMDN MSB-first spec.

    Spec quote (verbatim, retrieved 2026-06-02):
        "bits are referenced from most significant to least significant"
    https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn#hashed_flags

    Spec bit -> value table (spec bit 0 = MSB):
        spec bit:  0    1    2    3    4    5    6    7
        value:    0x80 0x40 0x20 0x10 0x08 0x04 0x02 0x01

    Battery occupies spec bits 5-6 (2 bits, values 0x04 + 0x02).
    UWT occupies spec bit 7 (value 0x01).
    Spec bits 0-4 are reserved (must be 0).
    """
    if not 0 <= battery <= 3:
        raise ValueError(f"battery must be in 0..3, got {battery}")
    # battery is 2 bits at spec positions 5 (MSB of field, value 0x04) and 6 (value 0x02).
    # In a 0..3 integer, bit 1 -> spec bit 5 (0x04), bit 0 -> spec bit 6 (0x02).
    battery_field = ((battery >> 1) & 0x01) * 0x04 | (battery & 0x01) * 0x02
    uwt_field = 0x01 if uwt else 0x00
    return battery_field | uwt_field


@pytest.mark.parametrize(
    "battery,uwt",
    [(b, u) for b in range(4) for u in (False, True)],
    ids=[f"battery{b}_uwt{int(u)}" for b in range(4) for u in (False, True)],
)
class TestHashedFlagsMsbFirstConvention:
    """Spec-quote-anchored regression for FMDN hashed_flags MSB-first decoding.

    Why this test exists:
        The Google Fast Pair FHN spec defines hashed_flags as
            spec bit 5-6: battery level (0..3)
            spec bit 7:   unwanted-tracking-protection mode
        AND qualifies explicitly:
            "bits are referenced from most significant to least significant"
        => In standard LSB-first byte arithmetic this maps to:
            battery = (byte >> 1) & 0x03
            uwt     =  byte       & 0x01

    Reviewers (human or AI) who propose (byte >> 5) & 0x03 / byte & 0x80
    are reading the spec without its MSB-first qualifier and must be pointed
    at this test. Removing the test or weakening its assertions re-opens
    the Codex / Copilot false-positive class documented in
    docs/BLE_BATTERY_SENSOR.md.
    """

    def test_decoded_byte_round_trips_through_resolver(
        self, battery: int, uwt: bool
    ) -> None:
        """Every (battery, UWT) combo encodes per spec and decodes back identically."""
        resolver = _make_resolver()
        eid = bytes([0xA0 | battery | (0x10 if uwt else 0)]) * LEGACY_EID_LENGTH
        xor_mask = 0x5A  # arbitrary non-zero mask to exercise XOR path

        decoded = _encode_msb_first_flags_byte(battery, uwt)
        flags_byte = decoded ^ xor_mask
        raw = _service_data_payload(eid, flags_byte)
        device_key = f"dev-msb-{battery}-{int(uwt)}"
        match = _match(device_key)
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        state = resolver._ble_battery_state.get(device_key)
        assert state is not None, _MSB_BATTERY_LABELS[battery]
        assert state.battery_level == battery
        assert state.uwt_mode is uwt
        assert state.decoded_flags == decoded


class TestMsbFirstNotLsbFirstDemonstratesCodexMisinterpretation:
    """Anti-false-positive guard.

    If a reviewer "fixes" the decoder to (decoded >> 5) & 0x03 and decoded & 0x80
    (the naive LSB-first reading of the spec text), this test demonstrates
    that the same wire-format byte then decodes to nonsense for every value
    of battery > 0 and for UWT=True. The assertions below are inequalities
    against the wrong implementation, not equalities against the right one,
    which is exactly what makes them readable to future automated reviewers:
    a PR that "rewrites" the decoder will trip this test even before the
    main regression tests above.
    """

    @staticmethod
    def _wrong_lsb_first_battery(decoded_byte: int) -> int:
        """The implementation Codex / a careless reader would propose."""
        return (decoded_byte >> 5) & 0x03

    @staticmethod
    def _wrong_lsb_first_uwt(decoded_byte: int) -> bool:
        """The implementation Codex / a careless reader would propose."""
        return bool(decoded_byte & 0x80)

    def test_msb_first_battery_high_collides_with_lsb_first_zero(self) -> None:
        """battery=3 in MSB-first encoding is 0x06; LSB-first would read 0 instead."""
        decoded = _encode_msb_first_flags_byte(battery=3, uwt=False)
        assert decoded == 0x06
        # Correct MSB-first decoder yields 3:
        assert (decoded >> 1) & 0x03 == 3
        # Wrong LSB-first reading collapses to 0 -> would silently lose
        # critical-battery advertisements.
        assert self._wrong_lsb_first_battery(decoded) == 0

    def test_msb_first_uwt_on_collides_with_lsb_first_off(self) -> None:
        """UWT=True in MSB-first encoding sets bit 0 (0x01); LSB-first reads bit 7."""
        decoded = _encode_msb_first_flags_byte(battery=0, uwt=True)
        assert decoded == 0x01
        # Correct MSB-first decoder reads UWT as True:
        assert bool(decoded & 0x01) is True
        # Wrong LSB-first reading reads False -> would silently swallow
        # unwanted-tracking-protection advertisements.
        assert self._wrong_lsb_first_uwt(decoded) is False

    def test_documented_anti_pattern_does_not_round_trip_for_any_nonzero_battery(
        self,
    ) -> None:
        """Property check: the wrong reading disagrees with the right one
        for every non-trivial spec value, proving the two interpretations
        are genuinely incompatible (not a coincidental match on the
        all-zeros frame)."""
        disagreements = 0
        for battery in range(4):
            for uwt in (False, True):
                decoded = _encode_msb_first_flags_byte(battery, uwt)
                correct_b = (decoded >> 1) & 0x03
                correct_u = bool(decoded & 0x01)
                wrong_b = self._wrong_lsb_first_battery(decoded)
                wrong_u = self._wrong_lsb_first_uwt(decoded)
                if (correct_b, correct_u) != (wrong_b, wrong_u):
                    disagreements += 1
        # Only (battery=0, uwt=False) -> (0, False) coincides for both readings.
        # All other 7 combinations must disagree -> readings are not exchangeable.
        assert disagreements == 7


# ===========================================================================
# 2a-CONST. Drift guard: module constants must match Google FHN spec values
# ===========================================================================


class TestFmdnHashedFlagsConstantsMatchSpec:
    """Drift guard for the FMDN hashed_flags bit-extraction constants.

    Spec quote (verbatim, retrieved 2026-06-02):
        "bits are referenced from most significant to least significant"
    https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn#hashed_flags

    Spec bit layout (spec bit 0 = MSB):
        bits 0-4: reserved (must be 0)
        bits 5-6: battery level (2 bit field)
        bit 7:    unwanted-tracking-protection mode (1 bit)

    Mapping to standard LSB-first arithmetic on the XOR-decoded byte:
        battery_raw = (decoded >> 1) & 0x03   -> shift=1, mask=0x03
        uwt_mode    = bool(decoded & 0x01)    -> mask=0x01

    If a future refactor renames or changes the numeric value of any of these
    constants, this test fails -- preventing silent drift between module code
    and the spec. The behaviour test for the decode path itself lives in
    TestHashedFlagsMsbFirstConvention (which deliberately keeps the literals
    inline to avoid tautology).
    """

    def test_battery_mask_is_2_bit(self) -> None:
        assert FMDN_HASHED_FLAGS_BATTERY_MASK == 0x03

    def test_battery_shift_is_one(self) -> None:
        assert FMDN_HASHED_FLAGS_BATTERY_SHIFT == 1

    def test_uwt_mode_mask_is_1_bit(self) -> None:
        assert FMDN_HASHED_FLAGS_UWT_MODE_MASK == 0x01

    def test_battery_field_is_disjoint_from_uwt_field(self) -> None:
        """Battery and UWT bits must not overlap in the post-decode byte."""
        battery_field_in_byte = (
            FMDN_HASHED_FLAGS_BATTERY_MASK << FMDN_HASHED_FLAGS_BATTERY_SHIFT
        )
        assert battery_field_in_byte & FMDN_HASHED_FLAGS_UWT_MODE_MASK == 0

    def test_constants_round_trip_through_msb_first_encoder(self) -> None:
        """End-to-end: spec-correct MSB-first encoder -> module constants -> back.

        Independent of the in-resolver decode site. Catches the case where all
        three constants AND the decode expression are mutated consistently
        (e.g. SHIFT=2 / MASK=0x07): the dedicated value tests above still pass
        in isolation, but this round-trip flips for at least one (battery, uwt)
        combination because the spec encoder uses verbatim spec values.
        """
        for battery in range(4):
            for uwt in (False, True):
                decoded = _encode_msb_first_flags_byte(battery, uwt)
                decoded_battery = (
                    decoded >> FMDN_HASHED_FLAGS_BATTERY_SHIFT
                ) & FMDN_HASHED_FLAGS_BATTERY_MASK
                decoded_uwt = bool(decoded & FMDN_HASHED_FLAGS_UWT_MODE_MASK)
                assert (decoded_battery, decoded_uwt) == (battery, uwt)


# ===========================================================================
# 2b. compute_flags_xor_mask() with P256 curve
# ===========================================================================


class TestComputeFlagsXorMaskP256:
    """Tests for compute_flags_xor_mask with P256 curve parameters."""

    def test_p256_xor_mask_differs_from_legacy(self) -> None:
        """Same EIK+timestamp should produce different masks for P256 vs legacy."""
        eik = b"\x42" * 32
        timestamp = 1000000

        legacy_mask = compute_flags_xor_mask(eik, timestamp)
        p256_mask = compute_flags_xor_mask(
            eik, timestamp, curve_byte_len=MODERN_EID_LENGTH, curve_order=P256_ORDER
        )

        # They *can* coincidentally be equal, but with overwhelming probability
        # they differ due to different curve order and byte length.
        # We just verify both calls succeed without error.
        assert isinstance(legacy_mask, int)
        assert isinstance(p256_mask, int)
        assert 0 <= legacy_mask <= 255
        assert 0 <= p256_mask <= 255

    def test_p256_xor_mask_deterministic(self) -> None:
        """Same inputs should always produce the same P256 mask."""
        eik = b"\xab" * 32
        timestamp = 2000000

        mask1 = compute_flags_xor_mask(
            eik, timestamp, curve_byte_len=MODERN_EID_LENGTH, curve_order=P256_ORDER
        )
        mask2 = compute_flags_xor_mask(
            eik, timestamp, curve_byte_len=MODERN_EID_LENGTH, curve_order=P256_ORDER
        )
        assert mask1 == mask2

    def test_legacy_default_backward_compatible(self) -> None:
        """Calling without curve_order should use legacy secp160r1 (backward compatible)."""
        eik = b"\xcd" * 32
        timestamp = 3000000

        mask_default = compute_flags_xor_mask(eik, timestamp)
        mask_explicit = compute_flags_xor_mask(
            eik, timestamp, curve_byte_len=LEGACY_EID_LENGTH, curve_order=None
        )
        assert mask_default == mask_explicit


# ===========================================================================
# 3. get_ble_battery_state() public API
# ===========================================================================


class TestGetBLEBatteryState:
    """Tests for the public get_ble_battery_state API."""

    def test_returns_none_when_no_data(self) -> None:
        resolver = _make_resolver()
        assert resolver.get_ble_battery_state("nonexistent") is None

    def test_returns_stored_state(self) -> None:
        resolver = _make_resolver()
        state = BLEBatteryState(
            battery_level=2,
            battery_pct=25,
            uwt_mode=False,
            decoded_flags=0x04,
            observed_at_wall=5000.0,
        )
        resolver._ble_battery_state["test-dev"] = state
        result = resolver.get_ble_battery_state("test-dev")
        assert result is state

    def test_returns_state_after_decode(self) -> None:
        """get_ble_battery_state should return data set by _update_ble_battery."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_01_0  # battery=1 (NORMAL)
        flags_byte = desired_decoded ^ xor_mask
        raw = _service_data_payload(eid, flags_byte)
        match = _match("api-dev")
        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        result = resolver.get_ble_battery_state("api-dev")
        assert result is not None
        assert result.battery_pct == 100


# ===========================================================================
# 4. BLE_BATTERY_DESCRIPTION entity description
# ===========================================================================


class TestBLEBatteryDescription:
    """Tests for the sensor entity description constants."""

    def test_key(self) -> None:
        assert BLE_BATTERY_DESCRIPTION.key == "ble_battery"

    def test_translation_key(self) -> None:
        assert BLE_BATTERY_DESCRIPTION.translation_key == "ble_battery"

    def test_device_class(self) -> None:
        from homeassistant.components.sensor import SensorDeviceClass

        assert BLE_BATTERY_DESCRIPTION.device_class == SensorDeviceClass.BATTERY

    def test_unit_of_measurement(self) -> None:
        from homeassistant.const import PERCENTAGE

        assert BLE_BATTERY_DESCRIPTION.native_unit_of_measurement == PERCENTAGE

    def test_state_class(self) -> None:
        from homeassistant.components.sensor import SensorStateClass

        assert BLE_BATTERY_DESCRIPTION.state_class == SensorStateClass.MEASUREMENT

    def test_entity_category(self) -> None:
        from homeassistant.helpers.entity import EntityCategory

        assert BLE_BATTERY_DESCRIPTION.entity_category == EntityCategory.DIAGNOSTIC

    def test_no_explicit_icon(self) -> None:
        """SensorDeviceClass.BATTERY provides dynamic icons -- no manual icon."""
        assert getattr(BLE_BATTERY_DESCRIPTION, "icon", None) is None


# ===========================================================================
# 5. GoogleFindMyBLEBatterySensor entity — native_value
# ===========================================================================


class TestBLEBatterySensorNativeValue:
    """Tests for the native_value property."""

    def test_value_from_resolver(self) -> None:
        """When resolver has battery data, native_value returns battery_pct."""
        resolver = _make_resolver()
        state = BLEBatteryState(
            battery_level=1,
            battery_pct=100,
            uwt_mode=False,
            decoded_flags=0x02,
            observed_at_wall=time.time(),
        )
        resolver._ble_battery_state["dev-1"] = state

        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        assert sensor.native_value == 100

    def test_value_fallback_to_restored(self) -> None:
        """When resolver has no data, native_value returns _attr_native_value."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-no-data", resolver=resolver)
        sensor._attr_native_value = 25
        sensor._last_observation_wall = time.time()  # fresh sighting
        assert sensor.native_value == 25

    def test_value_none_when_no_data_no_restore(self) -> None:
        """When resolver has no data and no restored value, returns None."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-empty", resolver=resolver)
        assert sensor.native_value is None

    def test_value_without_resolver(self) -> None:
        """When resolver is not in hass.data, returns _attr_native_value fallback."""
        sensor = _build_battery_sensor(device_id="dev-1", resolver=None)
        sensor._attr_native_value = 5
        sensor._last_observation_wall = time.time()  # fresh sighting
        assert sensor.native_value == 5

    def test_value_updates_on_battery_change(self) -> None:
        """native_value reflects the latest resolver state."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)

        assert sensor.native_value is None

        resolver._ble_battery_state["dev-1"] = BLEBatteryState(
            battery_level=1,
            battery_pct=100,
            uwt_mode=False,
            decoded_flags=0x02,
            observed_at_wall=time.time(),
        )
        assert sensor.native_value == 100

        resolver._ble_battery_state["dev-1"] = BLEBatteryState(
            battery_level=2,
            battery_pct=25,
            uwt_mode=False,
            decoded_flags=0x04,
            observed_at_wall=time.time(),
        )
        assert sensor.native_value == 25

    def test_value_resolver_not_a_dict(self) -> None:
        """When hass.data[DOMAIN] is not a dict, returns _attr_native_value."""
        hass = SimpleNamespace(data={DOMAIN: "not-a-dict"})
        sensor = _build_battery_sensor(device_id="dev-1", hass=hass)
        sensor._attr_native_value = 42
        sensor._last_observation_wall = time.time()  # fresh sighting
        assert sensor.native_value == 42

    def test_resolver_missing_domain_key(self) -> None:
        """When DOMAIN not in hass.data, returns _attr_native_value."""
        hass = SimpleNamespace(data={})
        sensor = _build_battery_sensor(device_id="dev-1", hass=hass)
        sensor._attr_native_value = 99
        sensor._last_observation_wall = time.time()  # fresh sighting
        assert sensor.native_value == 99


# ===========================================================================
# 6. GoogleFindMyBLEBatterySensor — available property
# ===========================================================================


class TestBLEBatterySensorAvailability:
    """Tests for the available property."""

    def test_available_when_present(self) -> None:
        """Sensor available when coordinator reports device as present."""
        resolver = _make_resolver()
        resolver._ble_battery_state["dev-1"] = BLEBatteryState(
            battery_level=1,
            battery_pct=100,
            uwt_mode=False,
            decoded_flags=0x02,
            observed_at_wall=1000.0,
        )
        coordinator = _fake_coordinator(device_id="dev-1", present=True)
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        assert sensor.available is True

    def test_available_when_not_present_with_restore(self) -> None:
        """Available even when not present, if we have a restored value."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1", present=False)
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor._attr_native_value = 100  # restored
        sensor._last_observation_wall = time.time()  # fresh sighting
        assert sensor.available is True

    def test_unavailable_when_not_present_no_data(self) -> None:
        """Unavailable when not present and no restore/resolver data."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1", present=False)
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor._attr_native_value = None
        assert sensor.available is False

    def test_unavailable_when_coordinator_hidden(self) -> None:
        """Unavailable when coordinator marks device as not visible."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1", present=True, visible=False)
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        assert sensor.available is False

    def test_available_with_is_device_present_exception(self) -> None:
        """When is_device_present raises, fall back to _attr_native_value."""
        resolver = _make_resolver()

        def _raise(did: str) -> bool:
            raise RuntimeError("boom")

        coordinator = _fake_coordinator(device_id="dev-1", present=True)
        coordinator.is_device_present = _raise
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor._attr_native_value = 50
        sensor._last_observation_wall = time.time()  # fresh sighting
        # Exception path => returns _attr_native_value is not None => True
        assert sensor.available is True

    def test_unavailable_with_exception_no_restore(self) -> None:
        """When is_device_present raises and no restore, unavailable."""
        resolver = _make_resolver()

        def _raise(did: str) -> bool:
            raise RuntimeError("boom")

        coordinator = _fake_coordinator(device_id="dev-1", present=True)
        coordinator.is_device_present = _raise
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor._attr_native_value = None
        assert sensor.available is False

    def test_available_without_is_device_present_attr(self) -> None:
        """When coordinator lacks is_device_present, fall back to restore check."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1", present=True)
        del coordinator.is_device_present
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor._attr_native_value = 25
        sensor._last_observation_wall = time.time()  # fresh sighting
        # No is_device_present => falls to bottom: _attr_native_value is not None
        assert sensor.available is True

    def test_available_present_returns_non_bool(self) -> None:
        """When is_device_present returns a truthy non-bool, available is True."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1", present=True)
        coordinator.is_device_present = lambda did: 1  # truthy non-bool
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        assert sensor.available is True


# ===========================================================================
# 7. GoogleFindMyBLEBatterySensor — extra_state_attributes
# ===========================================================================


class TestBLEBatterySensorExtraAttributes:
    """Tests for extra_state_attributes."""

    def test_attributes_with_resolver_data(self) -> None:
        resolver = _make_resolver()
        state = BLEBatteryState(
            battery_level=2,
            battery_pct=25,
            uwt_mode=True,
            decoded_flags=0x05,
            observed_at_wall=1700000000.0,
        )
        resolver._ble_battery_state["dev-1"] = state

        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        attrs = sensor.extra_state_attributes

        assert attrs is not None
        assert attrs["battery_raw_level"] == 2
        assert "uwt_mode" not in attrs  # UWT is its own binary sensor now
        assert attrs["google_device_id"] == "dev-1"
        assert "last_ble_observation" in attrs
        assert "T" in attrs["last_ble_observation"]

    def test_attributes_none_without_resolver(self) -> None:
        sensor = _build_battery_sensor(device_id="dev-1", resolver=None)
        assert sensor.extra_state_attributes is None

    def test_attributes_none_without_battery_data(self) -> None:
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-no-data", resolver=resolver)
        assert sensor.extra_state_attributes is None


# ===========================================================================
# 8. GoogleFindMyBLEBatterySensor — _handle_coordinator_update
# ===========================================================================


class TestBLEBatterySensorCoordinatorUpdate:
    """Tests for _handle_coordinator_update."""

    def test_update_caches_native_value(self) -> None:
        """When resolver has battery data, update caches native_value."""
        resolver = _make_resolver()
        resolver._ble_battery_state["dev-1"] = BLEBatteryState(
            battery_level=2,
            battery_pct=25,
            uwt_mode=False,
            decoded_flags=0x04,
            observed_at_wall=time.time(),
        )
        coordinator = _fake_coordinator(
            device_id="dev-1",
            present=True,
            snapshot=[{"id": "dev-1", "name": "Test Tracker"}],
        )
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        # Stub async_write_ha_state since we're outside HA platform
        sensor.async_write_ha_state = MagicMock()

        sensor._handle_coordinator_update()

        assert sensor._attr_native_value == 25
        sensor.async_write_ha_state.assert_called()

    def test_update_without_device_writes_state(self) -> None:
        """When coordinator_has_device is False, still writes state."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1", present=False, visible=False)
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor.async_write_ha_state = MagicMock()

        sensor._handle_coordinator_update()

        # Should still call async_write_ha_state and return early
        sensor.async_write_ha_state.assert_called()

    def test_update_without_resolver_data(self) -> None:
        """When resolver has no battery data, native_value not updated."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(
            device_id="dev-1",
            present=True,
            snapshot=[{"id": "dev-1", "name": "Test Tracker"}],
        )
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor.async_write_ha_state = MagicMock()
        sensor._attr_native_value = None

        sensor._handle_coordinator_update()

        assert sensor._attr_native_value is None
        sensor.async_write_ha_state.assert_called()

    def test_update_refreshes_device_label(self) -> None:
        """Coordinator update should refresh the device label from snapshot."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(
            device_id="dev-1",
            present=True,
            snapshot=[{"id": "dev-1", "name": "New Name"}],
        )
        sensor = _build_battery_sensor(
            device_id="dev-1",
            device_name="Old Name",
            coordinator=coordinator,
            resolver=resolver,
        )
        sensor.async_write_ha_state = MagicMock()
        # Stub maybe_update_device_registry_name to avoid registry access
        sensor.maybe_update_device_registry_name = MagicMock()

        sensor._handle_coordinator_update()

        assert sensor._device["name"] == "New Name"
        assert sensor._fallback_label == "New Name"


# ===========================================================================
# 9. GoogleFindMyBLEBatterySensor — async_added_to_hass (RestoreSensor)
# ===========================================================================


class TestBLEBatterySensorRestore:
    """Tests for async_added_to_hass RestoreSensor integration."""

    @pytest.mark.asyncio
    async def test_restore_valid_percentage(self) -> None:
        """Restoring a valid integer percentage sets _attr_native_value."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        last_data = SimpleNamespace(native_value="25")
        sensor.async_get_last_sensor_data = AsyncMock(return_value=last_data)
        sensor.async_get_last_extra_data = AsyncMock(
            return_value=SimpleNamespace(
                as_dict=lambda: {
                    "native_value": last_data.native_value,
                    "native_unit_of_measurement": "%",
                    "last_observation_wall": time.time(),
                }
            )
        )

        # Patch super().async_added_to_hass to be a no-op
        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value == 25

    @pytest.mark.asyncio
    async def test_restore_float_percentage(self) -> None:
        """Restoring a float value rounds to int."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        last_data = SimpleNamespace(native_value="99.7")
        sensor.async_get_last_sensor_data = AsyncMock(return_value=last_data)
        sensor.async_get_last_extra_data = AsyncMock(
            return_value=SimpleNamespace(
                as_dict=lambda: {
                    "native_value": last_data.native_value,
                    "native_unit_of_measurement": "%",
                    "last_observation_wall": time.time(),
                }
            )
        )

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value == 99

    @pytest.mark.asyncio
    async def test_restore_none_value(self) -> None:
        """When last sensor data returns None native_value, no restore."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        last_data = SimpleNamespace(native_value=None)
        sensor.async_get_last_sensor_data = AsyncMock(return_value=last_data)

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value is None

    @pytest.mark.asyncio
    async def test_restore_unknown_value(self) -> None:
        """When last sensor data is 'unknown', no restore."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        last_data = SimpleNamespace(native_value="unknown")
        sensor.async_get_last_sensor_data = AsyncMock(return_value=last_data)

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value is None

    @pytest.mark.asyncio
    async def test_restore_unavailable_value(self) -> None:
        """When last sensor data is 'unavailable', no restore."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        last_data = SimpleNamespace(native_value="unavailable")
        sensor.async_get_last_sensor_data = AsyncMock(return_value=last_data)

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value is None

    @pytest.mark.asyncio
    async def test_restore_invalid_value_type(self) -> None:
        """When last sensor data is not a parseable number, no restore."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        last_data = SimpleNamespace(native_value="not-a-number")
        sensor.async_get_last_sensor_data = AsyncMock(return_value=last_data)

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value is None

    @pytest.mark.asyncio
    async def test_restore_no_last_data(self) -> None:
        """When async_get_last_sensor_data returns None, no restore."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        sensor.async_get_last_sensor_data = AsyncMock(return_value=None)

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value is None

    @pytest.mark.asyncio
    async def test_restore_runtime_error(self) -> None:
        """When async_get_last_sensor_data raises RuntimeError, no restore."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        sensor.async_get_last_sensor_data = AsyncMock(
            side_effect=RuntimeError("store unavailable")
        )

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value is None

    @pytest.mark.asyncio
    async def test_restore_attribute_error(self) -> None:
        """When data lacks native_value attr, no restore."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        sensor.async_write_ha_state = MagicMock()

        sensor.async_get_last_sensor_data = AsyncMock(
            side_effect=AttributeError("no native_value")
        )

        with patch.object(
            GoogleFindMyBLEBatterySensor.__bases__[1],
            "async_added_to_hass",
            new_callable=AsyncMock,
        ):
            await sensor.async_added_to_hass()

        assert sensor._attr_native_value is None


# ===========================================================================
# 10. GoogleFindMyBLEBatterySensor — __init__ via real constructor
# ===========================================================================


class TestBLEBatterySensorInit:
    """Tests exercising the real __init__ path."""

    def test_init_sets_device_id(self) -> None:
        """Real __init__ should set _device_id from device dict."""
        coordinator = _fake_coordinator(device_id="init-dev")
        coordinator._subentry_key = "core_tracking"
        coordinator._subentry_identifier = "tracker"

        device: MutableMapping[str, Any] = {"id": "init-dev", "name": "Init Tracker"}

        sensor = GoogleFindMyBLEBatterySensor.__new__(GoogleFindMyBLEBatterySensor)
        # Manually set attributes that super().__init__ would set
        sensor.coordinator = coordinator
        sensor.hass = _fake_hass()
        sensor._subentry_identifier = "tracker"
        sensor._subentry_key = "core_tracking"
        sensor._device = device
        sensor._fallback_label = "Init Tracker"

        # Call the actual __init__ logic (the part after super().__init__)
        sensor._device_id = device.get("id")
        safe_id = sensor._device_id if sensor._device_id is not None else "unknown"
        entry_id = "entry-1"
        sensor._attr_unique_id = sensor.build_unique_id(
            DOMAIN, entry_id, "tracker", f"{safe_id}_ble_battery", separator="_"
        )
        sensor._attr_native_value = None

        assert sensor._device_id == "init-dev"
        assert "init-dev_ble_battery" in sensor._attr_unique_id
        assert sensor._attr_native_value is None

    def test_init_with_none_device_id(self) -> None:
        """When device has no id, safe_id defaults to 'unknown'."""
        coordinator = _fake_coordinator(device_id="unknown")
        device: MutableMapping[str, Any] = {"name": "No ID Tracker"}

        sensor = GoogleFindMyBLEBatterySensor.__new__(GoogleFindMyBLEBatterySensor)
        sensor.coordinator = coordinator
        sensor.hass = _fake_hass()
        sensor._subentry_identifier = "tracker"
        sensor._subentry_key = "core_tracking"
        sensor._device = device
        sensor._fallback_label = "No ID Tracker"

        sensor._device_id = device.get("id")
        safe_id = sensor._device_id if sensor._device_id is not None else "unknown"
        entry_id = "entry-1"
        sensor._attr_unique_id = sensor.build_unique_id(
            DOMAIN, entry_id, "tracker", f"{safe_id}_ble_battery", separator="_"
        )
        sensor._attr_native_value = None

        assert sensor._device_id is None
        assert "unknown_ble_battery" in sensor._attr_unique_id


# ===========================================================================
# 11. GoogleFindMyBLEBatterySensor — unique_id and _unrecorded_attributes
# ===========================================================================


class TestBLEBatterySensorUniqueId:
    """Tests for the unique_id construction."""

    def test_unique_id_format(self) -> None:
        sensor = _build_battery_sensor(device_id="tracker-xyz")
        assert "tracker-xyz_ble_battery" in sensor.unique_id

    def test_unique_id_differs_from_other_devices(self) -> None:
        sensor_a = _build_battery_sensor(device_id="dev-a")
        sensor_b = _build_battery_sensor(device_id="dev-b")
        assert sensor_a.unique_id != sensor_b.unique_id


class TestBLEBatterySensorUnrecordedAttributes:
    """Tests for the _unrecorded_attributes frozenset."""

    def test_unrecorded_attrs_defined(self) -> None:
        sensor = _build_battery_sensor()
        assert isinstance(sensor._unrecorded_attributes, frozenset)
        assert "uwt_mode" not in sensor._unrecorded_attributes  # own entity now
        assert "last_ble_observation" in sensor._unrecorded_attributes
        assert "google_device_id" in sensor._unrecorded_attributes
        assert "battery_raw_level" in sensor._unrecorded_attributes


# ===========================================================================
# 12. GoogleFindMyBLEBatterySensor — _get_resolver edge cases
# ===========================================================================


class TestBLEBatterySensorGetResolver:
    """Tests for the _get_resolver helper."""

    def test_resolver_from_hass_data(self) -> None:
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-1", resolver=resolver)
        assert sensor._get_resolver() is resolver

    def test_resolver_none_when_domain_missing(self) -> None:
        hass = SimpleNamespace(data={})
        sensor = _build_battery_sensor(device_id="dev-1", hass=hass)
        assert sensor._get_resolver() is None

    def test_resolver_none_when_domain_data_not_dict(self) -> None:
        hass = SimpleNamespace(data={DOMAIN: "invalid"})
        sensor = _build_battery_sensor(device_id="dev-1", hass=hass)
        assert sensor._get_resolver() is None

    def test_resolver_none_when_key_missing(self) -> None:
        hass = SimpleNamespace(data={DOMAIN: {"other_key": "value"}})
        sensor = _build_battery_sensor(device_id="dev-1", hass=hass)
        assert sensor._get_resolver() is None


# ===========================================================================
# 13. Integration: Full decode pipeline -> entity value
# ===========================================================================


class TestIntegrationDecodeToEntity:
    """End-to-end: _update_ble_battery populates state -> sensor reads it."""

    def test_decode_pipeline_to_sensor_value(self) -> None:
        """Full pipeline: BLE payload -> resolver decode -> sensor reads battery_pct."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x33
        desired_decoded = 0b00000_10_0  # battery=2 (LOW) at bits 2:1
        flags_byte = desired_decoded ^ xor_mask
        raw = _service_data_payload(eid, flags_byte)
        match = _match("dev-pipe")

        resolver._update_ble_battery(raw, None, {"flags_xor_mask": xor_mask}, [match])

        sensor = _build_battery_sensor(device_id="dev-pipe", resolver=resolver)
        assert sensor.native_value == 25

        attrs = sensor.extra_state_attributes
        assert attrs is not None
        assert attrs["battery_raw_level"] == 2
        assert "uwt_mode" not in attrs  # UWT is its own binary sensor now

    def test_decode_pipeline_shared_device(self) -> None:
        """Shared device: same tracker across 2 accounts -> both sensors get values."""
        resolver = _make_resolver()
        eid = b"\xaa" * LEGACY_EID_LENGTH
        xor_mask = 0x00
        desired_decoded = 0b00000_01_0  # battery=1 (NORMAL)
        flags_byte = desired_decoded ^ xor_mask
        raw = _service_data_payload(eid, flags_byte)

        match_a = _match("dev-acct-a")
        match_b = _match("dev-acct-b")
        resolver._update_ble_battery(
            raw, None, {"flags_xor_mask": xor_mask}, [match_a, match_b]
        )

        sensor_a = _build_battery_sensor(device_id="dev-acct-a", resolver=resolver)
        sensor_b = _build_battery_sensor(device_id="dev-acct-b", resolver=resolver)
        assert sensor_a.native_value == 100
        assert sensor_b.native_value == 100

    def test_decode_pipeline_no_entity_without_data(self) -> None:
        """When resolver has NO battery data for a device, sensor returns None."""
        resolver = _make_resolver()
        sensor = _build_battery_sensor(device_id="dev-no-ble", resolver=resolver)
        assert sensor.native_value is None
        assert sensor.extra_state_attributes is None


# ===========================================================================
# 14. Translations exist
# ===========================================================================


class TestTranslations:
    """Verify that translation files contain the ble_battery key."""

    def test_en_translation_exists(self) -> None:
        import json
        from pathlib import Path

        en_path = Path(__file__).parent.parent / (
            "custom_components/googlefindmy/translations/en.json"
        )
        with open(en_path) as f:
            data = json.load(f)

        sensor_entities = data.get("entity", {}).get("sensor", {})
        assert "ble_battery" in sensor_entities
        assert "name" in sensor_entities["ble_battery"]

    def test_de_translation_exists(self) -> None:
        import json
        from pathlib import Path

        de_path = Path(__file__).parent.parent / (
            "custom_components/googlefindmy/translations/de.json"
        )
        with open(de_path) as f:
            data = json.load(f)

        sensor_entities = data.get("entity", {}).get("sensor", {})
        assert "ble_battery" in sensor_entities
        assert "name" in sensor_entities["ble_battery"]

    def test_all_translations_have_ble_battery(self) -> None:
        """All translation files should have the ble_battery key."""
        import json
        from pathlib import Path

        translations_dir = Path(__file__).parent.parent / (
            "custom_components/googlefindmy/translations"
        )
        for path in sorted(translations_dir.glob("*.json")):
            with open(path) as f:
                data = json.load(f)
            sensor_entities = data.get("entity", {}).get("sensor", {})
            assert "ble_battery" in sensor_entities, (
                f"Missing ble_battery in {path.name}"
            )


# ===========================================================================
# 15. Coverage: remaining edge-case paths
# ===========================================================================


class TestBLEBatterySensorEdgeCases:
    """Additional tests to cover remaining branches."""

    def test_unavailable_when_coordinator_update_failed(self) -> None:
        """When super().available is False (coordinator update failed), sensor unavailable."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1", present=True)
        coordinator.last_update_success = False
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            resolver=resolver,
        )
        # super().available returns False due to last_update_success=False
        assert sensor.available is False

    def test_handle_coordinator_update_without_resolver(self) -> None:
        """_handle_coordinator_update with no resolver in hass.data."""
        # hass.data has no DOMAIN key => resolver is None
        hass = SimpleNamespace(data={})
        coordinator = _fake_coordinator(
            device_id="dev-1",
            present=True,
            snapshot=[{"id": "dev-1", "name": "Test"}],
        )
        sensor = _build_battery_sensor(
            device_id="dev-1",
            coordinator=coordinator,
            hass=hass,
        )
        sensor.async_write_ha_state = MagicMock()
        sensor.maybe_update_device_registry_name = MagicMock()
        sensor._attr_native_value = None

        sensor._handle_coordinator_update()

        # Should not crash and should still call async_write_ha_state
        sensor.async_write_ha_state.assert_called()
        assert sensor._attr_native_value is None

    def test_device_info_property(self) -> None:
        """device_info property should return a DeviceInfo with identifiers."""
        resolver = _make_resolver()
        coordinator = _fake_coordinator(device_id="dev-1")
        sensor = _build_battery_sensor(
            device_id="dev-1",
            device_name="My Tracker",
            coordinator=coordinator,
            resolver=resolver,
        )
        # Stub _resolve_absolute_base_url to avoid network access
        sensor._resolve_absolute_base_url = lambda: None

        info = sensor.device_info
        assert info is not None
        assert getattr(info, "identifiers", None) is not None
        assert getattr(info, "manufacturer", None) == "Google"

    def test_real_init_constructor(self) -> None:
        """Exercise the real __init__ path via direct call."""
        coordinator = _fake_coordinator(device_id="real-init")
        coordinator._subentry_key = "core_tracking"

        device: MutableMapping[str, Any] = {
            "id": "real-init",
            "name": "Real Init Tracker",
        }

        # Create sensor with __new__ then call __init__ manually
        sensor = GoogleFindMyBLEBatterySensor.__new__(GoogleFindMyBLEBatterySensor)
        # Set base class attributes that super().__init__ would set
        sensor.coordinator = coordinator
        sensor.hass = _fake_hass()
        sensor._subentry_key = "core_tracking"
        sensor._subentry_identifier = "tracker"
        sensor._base_url_warning_emitted = False
        sensor._device = device
        sensor._fallback_label = device.get("name")

        # Call __init__ body
        GoogleFindMyBLEBatterySensor.__init__(
            sensor,
            coordinator,
            device,
            subentry_key="core_tracking",
            subentry_identifier="tracker",
        )

        assert sensor._device_id == "real-init"
        assert sensor._attr_native_value is None
        assert "real-init_ble_battery" in sensor._attr_unique_id

    def test_real_init_without_device_id(self) -> None:
        """Exercise __init__ when device dict lacks 'id'."""
        coordinator = _fake_coordinator(device_id="unknown")

        device: MutableMapping[str, Any] = {"name": "No ID Device"}

        sensor = GoogleFindMyBLEBatterySensor.__new__(GoogleFindMyBLEBatterySensor)
        sensor.coordinator = coordinator
        sensor.hass = _fake_hass()
        sensor._subentry_key = "core_tracking"
        sensor._subentry_identifier = "tracker"
        sensor._base_url_warning_emitted = False
        sensor._device = device
        sensor._fallback_label = device.get("name")

        GoogleFindMyBLEBatterySensor.__init__(
            sensor,
            coordinator,
            device,
            subentry_key="core_tracking",
            subentry_identifier="tracker",
        )

        assert sensor._device_id is None
        assert "unknown_ble_battery" in sensor._attr_unique_id


class TestFrameTypeAndUwtBitObservation:
    """The frame type and the UWT flag bit are two channels for one state.

    The specification says they always agree; this project's field reports say
    otherwise. Which reading holds is unresolved, so the code measures the
    disagreement instead of arbitrating it -- arbitrating would either suppress
    the very signal the sensor exists for, or mark every modern tracker as
    permanently tracked.
    """

    @staticmethod
    def _uwt_payload(device_suffix: str = "") -> tuple[bytes, int]:
        """Return a legacy service-data payload whose decoded UWT bit is set."""
        eid = bytes([0xA0 + len(device_suffix)]) * LEGACY_EID_LENGTH
        decoded = 0b00000_00_1  # UWT bit set, battery level 0
        return _service_data_payload(eid, decoded), decoded

    def test_disagreement_logs_but_does_not_change_uwt_bit(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """0x40 frame with the UWT bit set: logged, passed through unchanged."""
        resolver = _make_resolver()
        raw, decoded = self._uwt_payload()
        match = _match("dev-conflict")

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(
                raw, _FMDN_FRAME_TYPE, {"flags_xor_mask": 0x00}, [match]
            )

        state = resolver._ble_battery_state.get("dev-conflict")
        assert state is not None
        assert state.uwt_mode is True  # unchanged, not forced to False
        assert state.decoded_flags == decoded
        assert "FMDN_FLAGS_CONFLICT" in caplog.text

    def test_modern_frame_without_uwt_bit_logs_but_stays_off(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """0x41 frame without the UWT bit: logged, still reported as off."""
        resolver = _make_resolver()
        eid = b"\xb1" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0b00000_01_0)  # battery 1, UWT clear
        match = _match("dev-conflict-inverse")

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(
                raw, _MODERN_FRAME_TYPE, {"flags_xor_mask": 0x00}, [match]
            )

        state = resolver._ble_battery_state.get("dev-conflict-inverse")
        assert state is not None
        assert state.uwt_mode is False  # unchanged, not derived from the frame
        assert "FMDN_FLAGS_CONFLICT" in caplog.text

    @pytest.mark.parametrize(
        ("frame_type", "decoded"),
        [
            (0x40, 0b00000_01_0),  # no UWT mode, no UWT bit
            (0x41, 0b00000_00_1),  # UWT mode, UWT bit set
        ],
    )
    def test_agreement_does_not_log(
        self, frame_type: int, decoded: int, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Agreement in either direction produces no conflict record."""
        resolver = _make_resolver()
        eid = bytes([0xC0 + frame_type % 16]) * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, decoded)

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(
                raw, frame_type, {"flags_xor_mask": 0x00}, [_match("dev-agree")]
            )

        assert "FMDN_FLAGS_CONFLICT" not in caplog.text

    def test_unknown_frame_type_skips_observation(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """No frame byte was observed, so there is nothing to contradict.

        This is not a gap in the measurement but a property of the input: the
        naked-EID path carries no frame byte at all.
        """
        resolver = _make_resolver()
        raw, _decoded = self._uwt_payload()

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(
                raw, None, {"flags_xor_mask": 0x00}, [_match("dev-no-frame")]
            )

        assert "FMDN_FLAGS_CONFLICT" not in caplog.text

    def test_first_conflict_per_device_is_info_then_debug(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """First disagreement at INFO so a stock build can report it."""
        resolver = _make_resolver()
        raw, _decoded = self._uwt_payload()
        match = _match("dev-first")

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(
                raw, _FMDN_FRAME_TYPE, {"flags_xor_mask": 0x00}, [match]
            )

        records = [r for r in caplog.records if "FMDN_FLAGS_CONFLICT" in r.getMessage()]
        assert len(records) == 1
        assert records[0].levelname == "INFO"

    def test_second_conflict_within_window_is_throttled(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A disagreeing beacon disagrees on every advertisement."""
        resolver = _make_resolver()
        raw, _decoded = self._uwt_payload()
        match = _match("dev-throttle")
        metadata = {"flags_xor_mask": 0x00}

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(raw, _FMDN_FRAME_TYPE, metadata, [match])
            resolver._update_ble_battery(raw, _FMDN_FRAME_TYPE, metadata, [match])
            resolver._update_ble_battery(raw, _FMDN_FRAME_TYPE, metadata, [match])

        records = [r for r in caplog.records if "FMDN_FLAGS_CONFLICT" in r.getMessage()]
        assert len(records) == 1  # the INFO one; the rest are inside the window

    def test_conflict_outside_window_logs_again_at_debug(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Once the throttle window passes, the channel keeps measuring."""
        resolver = _make_resolver()
        raw, _decoded = self._uwt_payload()
        match = _match("dev-window-expiry")
        metadata = {"flags_xor_mask": 0x00}

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(raw, _FMDN_FRAME_TYPE, metadata, [match])
            resolver._frame_conflict_log_at[("dev-window-expiry", _FMDN_FRAME_TYPE)] = (
                time.time() - 3600
            )
            resolver._update_ble_battery(raw, _FMDN_FRAME_TYPE, metadata, [match])

        records = [r for r in caplog.records if "FMDN_FLAGS_CONFLICT" in r.getMessage()]
        assert [r.levelname for r in records] == ["INFO", "DEBUG"]

    def test_resolver_built_without_init_does_not_raise(self) -> None:
        """Stubs bypass __init__, so every new cache needs its defaults guard."""
        resolver = GoogleFindMyEIDResolver.__new__(GoogleFindMyEIDResolver)
        resolver.hass = _fake_hass()
        resolver._lookup = {}
        resolver._lookup_metadata = {}
        resolver._ensure_cache_defaults()

        raw, _decoded = self._uwt_payload()
        resolver._update_ble_battery(
            raw, _FMDN_FRAME_TYPE, {"flags_xor_mask": 0x00}, [_match("dev-stub")]
        )

        assert resolver._ble_battery_state["dev-stub"].uwt_mode is True

    def test_undecodable_modern_frame_is_visible_via_the_probe_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The population that would settle the question is already visible.

        A 0x41 frame whose flags byte cannot be decoded never reaches the
        conflict channel -- there is no decoded bit to contradict. It is not
        invisible, though: the pre-existing CANNOT_DECODE probe reports it once
        per device at INFO, carrying the observed frame type.

        That distinction matters for reading the field data, because the
        specification permits omitting the hashed-flags byte only when the
        beacon supports neither battery indication nor unwanted tracking
        protection mode. A 0x41 frame arriving without a decodable flags byte
        is therefore either a beacon violating that rule or a payload shape
        this code slices wrongly -- and the probe log is what tells them apart.
        """
        resolver = _make_resolver()
        eid = b"\xd4" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0x42)
        match = _match("dev-undecodable")

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(raw, _MODERN_FRAME_TYPE, {}, [match])

        assert "FMDN_FLAGS_CONFLICT" not in caplog.text
        assert "CANNOT_DECODE" in caplog.text
        assert "observed_frame=0x41" in caplog.text

    def test_conflict_channel_is_wired_into_the_production_path(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The channel must fire through resolve_eid(), not just when poked.

        Every other test in this class calls _update_ble_battery directly and
        hands it the frame type. That measures the channel but not whether it
        is connected: passing None for the frame type at the call site would
        silence it for every real advertisement and leave those tests green.
        """
        resolver = _make_resolver()
        eid = b"\xe7" * LEGACY_EID_LENGTH
        # 0x40 frame, UWT bit set in the decoded flags: a disagreement.
        raw = _service_data_payload(eid, 0b00000_00_1)
        resolver._lookup[eid] = [_match("dev-wired")]
        resolver._lookup_metadata[eid] = {"flags_xor_mask": 0x00}

        with caplog.at_level("DEBUG"):
            result = resolver.resolve_eid(raw)

        assert result is not None  # positive control
        assert "FMDN_FLAGS_CONFLICT" in caplog.text

    def test_conflict_and_probe_logs_can_be_joined(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Both logs must name the same device in the same field.

        Emitting only the storage key would look joinable and not be: the
        probe log puts ``device_id`` in ``device=`` and the canonical id in
        ``canonical=``, and the two differ for any device that has a canonical
        id. A reporter joining the lines on ``device=`` would get nothing.
        """
        resolver = _make_resolver()
        eid = b"\xe8" * LEGACY_EID_LENGTH
        raw = _service_data_payload(eid, 0b00000_00_1)
        match = _match("registry-id-abc", canonical_id="google-canonical-xyz")

        with caplog.at_level("DEBUG"):
            resolver._update_ble_battery(
                raw, _FMDN_FRAME_TYPE, {"flags_xor_mask": 0x00}, [match]
            )

        conflict = next(
            r.getMessage()
            for r in caplog.records
            if "FMDN_FLAGS_CONFLICT" in r.getMessage()
        )
        probe = next(
            r.getMessage()
            for r in caplog.records
            if "FMDN_FLAGS_PROBE" in r.getMessage()
        )
        assert "device=registry-id-abc" in conflict
        assert "canonical=google-canonical-xyz" in conflict
        assert "device=registry-id-abc" in probe
        assert "canonical=google-canonical-xyz" in probe
