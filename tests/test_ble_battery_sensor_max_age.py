# tests/test_ble_battery_sensor_max_age.py
"""Age limit of the BLE battery value: at most 7 days after the last sighting.

Because the battery sensor is created at setup from the entity registry, it
exists in every run and Home Assistant re-saves its state on every dump, so a
restored value would never expire on its own. The sensor therefore stores the
wall-clock time of the last BLE sighting with its restore data and shows a
value only while that sighting is at most 7 days old. The same limit applies
at runtime, both to a restored value and to resolver data whose sighting is
older than 7 days (that counts as no data). Without a value the sensor shows
``unknown`` while the tracker is present and ``unavailable`` while it is absent.

Reach of these tests: they talk to the sensor only through entity attributes,
properties and the restore hooks of the ``conftest`` sensor stub; they import
no name introduced by the age-limit change, and the limit appears here as the
literal ``7 * 24 * 3600``. The clock is patched on the ``time`` module the
sensor module uses.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import custom_components.googlefindmy.sensor as sensor_module
from custom_components.googlefindmy.const import DATA_EID_RESOLVER, DOMAIN
from custom_components.googlefindmy.eid_resolver import BLEBatteryState
from custom_components.googlefindmy.sensor import (
    BLE_BATTERY_DESCRIPTION,
    GoogleFindMyBLEBatterySensor,
)
from tests.helpers.config_entries_stub import make_config_entry

MAX_AGE_S = 7 * 24 * 3600
DAY_S = 24 * 3600
NOW = 1_800_000_000.0
DEV_ID = "dev-1"


class _Clock:
    """Mutable wall clock patched over ``time.time`` of the sensor module."""

    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    """Patch the wall clock the sensor module reads."""
    fake = _Clock(NOW)
    monkeypatch.setattr(sensor_module.time, "time", fake)
    return fake


@pytest.fixture
def non_utc_local_time(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Run with a local time zone away from UTC; CI runners default to UTC.

    Without it a naive timestamp read as local time equals UTC and a test of
    the UTC fallback passes whether the fallback exists or not.
    """
    monkeypatch.setenv("TZ", "America/New_York")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


class _ResolverStub:
    """Resolver stand-in answering only ``get_ble_battery_state``."""

    def __init__(self) -> None:
        self.states: dict[str, BLEBatteryState] = {}

    def get_ble_battery_state(self, device_id: str) -> BLEBatteryState | None:
        return self.states.get(device_id)


def _state(pct: int | None, observed_at_wall: float) -> BLEBatteryState:
    return BLEBatteryState(
        battery_level=1,
        battery_pct=pct,
        uwt_mode=False,
        decoded_flags=0,
        observed_at_wall=observed_at_wall,
    )


def _build_sensor(
    resolver: _ResolverStub | None = None, *, present: bool = True
) -> tuple[GoogleFindMyBLEBatterySensor, SimpleNamespace]:
    """Return a battery sensor bypassing platform init, plus its coordinator."""
    coordinator = SimpleNamespace(
        config_entry=make_config_entry(entry_id="entry-1"),
        is_device_present=lambda _did: present,
        is_device_visible_in_subentry=lambda _key, _did: True,
        async_update_listeners=lambda: None,
        get_subentry_snapshot=lambda _key: [{"id": DEV_ID, "name": "Tag"}],
        last_update_success=True,
    )
    domain_data: dict[str, Any] = {}
    if resolver is not None:
        domain_data[DATA_EID_RESOLVER] = resolver
    hass = SimpleNamespace(data={DOMAIN: domain_data})

    entity = GoogleFindMyBLEBatterySensor.__new__(GoogleFindMyBLEBatterySensor)
    entity._subentry_identifier = "tracker"
    entity._subentry_key = "core_tracking"
    entity.coordinator = coordinator
    entity.hass = hass
    entity._device_id = DEV_ID
    entity._device = {"id": DEV_ID, "name": "Tag"}
    entity._attr_native_value = None
    entity.entity_description = BLE_BATTERY_DESCRIPTION
    entity._fallback_label = "Tag"
    entity._attr_unique_id = f"googlefindmy_entry-1_tracker_{DEV_ID}_ble_battery"
    entity.entity_id = f"sensor.test_{DEV_ID}_ble_battery"
    entity.async_write_ha_state = MagicMock()
    return entity, coordinator


async def _restore(
    entity: GoogleFindMyBLEBatterySensor,
    *,
    value: Any = "25",
    extra: dict[str, Any] | None = None,
    attributes: dict[str, Any] | None = None,
) -> None:
    """Run ``async_added_to_hass`` against the given stored data.

    Only the extra data and the legacy state are patched; the ``conftest``
    stub derives ``async_get_last_sensor_data`` from the extra data as Home
    Assistant does. Without ``extra`` the record carries value and unit only.
    """
    stored = (
        dict(extra)
        if extra is not None
        else {"native_value": value, "native_unit_of_measurement": "%"}
    )
    entity.async_get_last_extra_data = AsyncMock(
        return_value=SimpleNamespace(as_dict=lambda: dict(stored))
    )
    entity.async_get_last_state = AsyncMock(
        return_value=None
        if attributes is None
        else SimpleNamespace(state=str(value), attributes=dict(attributes))
    )
    with patch.object(
        GoogleFindMyBLEBatterySensor.__bases__[1],
        "async_added_to_hass",
        new_callable=AsyncMock,
    ):
        await entity.async_added_to_hass()


def _extra(wall: Any) -> dict[str, Any]:
    return {
        "native_value": 25,
        "native_unit_of_measurement": "%",
        "last_observation_wall": wall,
    }


def _iso(wall: float) -> str:
    return datetime.fromtimestamp(wall, tz=UTC).isoformat()


@pytest.mark.asyncio
async def test_restore_extra_fresh(clock: _Clock) -> None:
    """A stored sighting younger than 7 days keeps the restored value."""
    entity, _ = _build_sensor()
    await _restore(entity, extra=_extra(NOW - DAY_S))

    assert entity.native_value == 25
    assert entity.available is True
    stored = entity.extra_restore_state_data.as_dict()
    assert (stored["native_value"], stored["last_observation_wall"]) == (
        25,
        NOW - DAY_S,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("present", [True, False])
async def test_restore_extra_expired(clock: _Clock, present: bool) -> None:
    """A stored sighting older than 7 days hides the value."""
    entity, _ = _build_sensor(present=present)
    await _restore(entity, extra=_extra(NOW - MAX_AGE_S - 1))

    assert entity.native_value is None
    assert entity.available is present


@pytest.mark.asyncio
async def test_restore_exactly_max_age(clock: _Clock) -> None:
    """A sighting exactly 7 days old still counts."""
    entity, _ = _build_sensor()
    await _restore(entity, extra=_extra(NOW - MAX_AGE_S))

    assert entity.native_value == 25


@pytest.mark.asyncio
async def test_restore_legacy_attribute_fresh(clock: _Clock) -> None:
    """Older states without the extra field fall back to the attribute."""
    entity, _ = _build_sensor()
    legacy_extra = {"native_value": 25, "native_unit_of_measurement": "%"}
    await _restore(
        entity,
        extra=legacy_extra,
        attributes={"last_ble_observation": _iso(NOW - DAY_S)},
    )

    assert entity.native_value == 25


@pytest.mark.asyncio
@pytest.mark.usefixtures("non_utc_local_time")
async def test_restore_legacy_attribute_naive_is_utc(clock: _Clock) -> None:
    """A legacy timestamp without an offset is read as UTC."""
    entity, _ = _build_sensor()
    naive = datetime.fromtimestamp(NOW - MAX_AGE_S, tz=UTC).replace(tzinfo=None)
    await _restore(entity, attributes={"last_ble_observation": naive.isoformat()})

    assert entity.native_value == 25
    clock.now += 1
    assert entity.native_value is None


@pytest.mark.asyncio
async def test_restore_legacy_attribute_expired(clock: _Clock) -> None:
    """An attribute older than 7 days hides the value."""
    entity, _ = _build_sensor()
    await _restore(
        entity,
        attributes={"last_ble_observation": _iso(NOW - MAX_AGE_S - 60)},
    )

    assert entity.native_value is None


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["not a timestamp", 12345])
async def test_restore_legacy_attribute_unusable(clock: _Clock, raw: Any) -> None:
    """An unusable legacy attribute counts as no sighting time."""
    entity, _ = _build_sensor()
    await _restore(entity, attributes={"last_ble_observation": raw})

    assert entity.native_value is None


@pytest.mark.asyncio
async def test_restore_no_date(clock: _Clock, caplog: pytest.LogCaptureFixture) -> None:
    """Without any sighting time the restored value is discarded (DEBUG)."""
    entity, _ = _build_sensor()
    with caplog.at_level(logging.DEBUG, logger=sensor_module.__name__):
        await _restore(entity, attributes={"friendly_name": "Tag"})

    assert entity._attr_native_value is None
    assert entity.native_value is None
    records = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG
        and "no BLE sighting time" in r.getMessage()
        and DEV_ID in r.getMessage()
    ]
    assert len(records) == 1


@pytest.mark.asyncio
async def test_value_without_sighting_time_is_hidden(clock: _Clock) -> None:
    """A value that carries no sighting time is never shown."""
    entity, _ = _build_sensor(present=False)
    entity._attr_native_value = 50

    assert entity.native_value is None
    assert entity.available is False


@pytest.mark.asyncio
async def test_runtime_expiry(clock: _Clock) -> None:
    """A restored value disappears once its sighting passes 7 days at runtime."""
    entity, _ = _build_sensor()
    await _restore(entity, extra=_extra(NOW - 6 * DAY_S))
    assert entity.native_value == 25

    clock.now += 2 * DAY_S

    assert entity.native_value is None
    assert entity.available is True


@pytest.mark.parametrize("present", [True, False])
def test_runtime_expiry_with_stale_resolver_state(clock: _Clock, present: bool) -> None:
    """Resolver data older than 7 days counts as no data."""
    resolver = _ResolverStub()
    resolver.states[DEV_ID] = _state(100, NOW - MAX_AGE_S - 1)
    entity, _ = _build_sensor(resolver, present=present)

    assert entity.native_value is None
    assert entity.available is present

    entity._handle_coordinator_update()
    assert entity._attr_native_value is None
    assert getattr(entity, "_last_observation_wall", None) is None
    assert entity.native_value is None

    resolver.states[DEV_ID] = _state(100, NOW - DAY_S)
    assert entity.native_value == 100


@pytest.mark.asyncio
async def test_fresh_sighting_resets_date(clock: _Clock) -> None:
    """A coordinator update with fresh resolver data sets value and date."""
    resolver = _ResolverStub()
    entity, _ = _build_sensor(resolver)
    await _restore(entity, extra=_extra(NOW - 6 * DAY_S))

    resolver.states[DEV_ID] = _state(5, NOW)
    entity._handle_coordinator_update()
    assert entity._attr_native_value == 5

    del resolver.states[DEV_ID]
    clock.now += 3 * DAY_S
    assert entity.native_value == 5


@pytest.mark.parametrize("invalid", ["garbage", True, float("nan"), None])
def test_extra_data_roundtrip(clock: _Clock, invalid: Any) -> None:
    """The sighting time survives as_dict/from_dict; invalid values are dropped."""
    resolver = _ResolverStub()
    resolver.states[DEV_ID] = _state(25, NOW - DAY_S)
    entity, _ = _build_sensor(resolver)
    entity._handle_coordinator_update()

    stored = entity.extra_restore_state_data
    d = stored.as_dict()
    assert "last_observation_wall" in d
    assert d["native_value"] == 25
    assert d["native_unit_of_measurement"] == "%"

    parsed = type(stored).from_dict(d)
    assert parsed is not None
    assert parsed.last_observation_wall == NOW - DAY_S
    assert parsed.native_value == 25

    broken = type(stored).from_dict({**d, "last_observation_wall": invalid})
    assert broken is not None
    assert broken.last_observation_wall is None

    assert type(stored).from_dict({"last_observation_wall": NOW}) is None


@pytest.mark.asyncio
async def test_fresh_resolver_before_restore_keeps_newer_sighting(
    clock: _Clock,
) -> None:
    """A restore must not replace a newer sighting taken from the resolver.

    ``GoogleFindMyEntity.async_added_to_hass`` runs a coordinator update before
    the restore reads the stored state, so a fresh resolver state is already
    cached when the older stored pair arrives.
    """
    resolver = _ResolverStub()
    resolver.states[DEV_ID] = _state(100, NOW)
    entity, _ = _build_sensor(resolver)
    entity._handle_coordinator_update()

    await _restore(entity, extra=_extra(NOW - 6 * DAY_S))

    stored = entity.extra_restore_state_data.as_dict()
    assert stored["native_value"] == 100
    assert stored["last_observation_wall"] == NOW
    assert getattr(entity, "_last_observation_wall", None) == NOW


def test_restore_data_pairs_value_and_date_from_one_source(clock: _Clock) -> None:
    """Stored value and sighting time come from the same resolver state."""
    resolver = _ResolverStub()
    entity, _ = _build_sensor(resolver)
    resolver.states[DEV_ID] = _state(25, NOW - 2 * DAY_S)
    entity._handle_coordinator_update()

    # A new sighting arrives before the next coordinator update.
    resolver.states[DEV_ID] = _state(5, NOW)

    stored = entity.extra_restore_state_data.as_dict()
    assert (stored["native_value"], stored["last_observation_wall"]) == (5, NOW)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ahead_s", "expected"),
    [(60, 25), (3600, None)],
    ids=["within_tolerance", "far_future"],
)
async def test_restore_future_sighting_time(
    clock: _Clock, ahead_s: int, expected: int | None
) -> None:
    """A sighting time far in the future is invalid; slight clock skew is not."""
    entity, _ = _build_sensor()
    await _restore(entity, extra=_extra(NOW + ahead_s))

    assert entity.native_value == expected


@pytest.mark.parametrize(
    ("ahead_s", "expected"),
    [(60, 100), (3600, None)],
    ids=["within_tolerance", "far_future"],
)
def test_resolver_future_sighting_time(
    clock: _Clock, ahead_s: int, expected: int | None
) -> None:
    """Resolver data with a sighting time far in the future counts as no data."""
    resolver = _ResolverStub()
    resolver.states[DEV_ID] = _state(100, NOW + ahead_s)
    entity, _ = _build_sensor(resolver)

    assert entity.native_value == expected
