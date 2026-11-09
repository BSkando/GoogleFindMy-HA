# tests/test_uwt_mode_binary_sensor_setup.py
"""Setup-level tests for creating the unwanted tracking mode binary sensor.

The UWT binary sensor follows the same two creation paths as the BLE battery
sensor: at platform setup when the entity registry already holds its
``unique_id``, and on the first decoded hashed-flags state for a device
without a registry entry. A device that has neither still gets no UWT
sensor. The sensor restores nothing: until the first sighting it shows
``unknown`` (``is_on is None``) for a present device and ``unavailable`` for
an absent one.

The tests drive ``binary_sensor.async_setup_entry`` with the ``conftest``
coordinator stub, the ``conftest`` entity registry stub and a resolver
stand-in that only answers ``get_ble_battery_state``. The expected
``unique_id`` is written out as a literal, so a drift of the production
formula fails these tests instead of being mirrored by them.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from custom_components.googlefindmy import binary_sensor
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    DATA_EID_RESOLVER,
    DATA_SECRET_BUNDLE,
    DOMAIN,
)
from custom_components.googlefindmy.eid_resolver import BLEBatteryState
from tests.helpers.config_entries_stub import make_config_entry

ENTRY_ID = "entry-uwt-setup"
TRACKER_KEY = "tracker"
TRACKER_SUBENTRY_ID = "tracker-subentry"
SECOND_KEY = "secondary"
SECOND_SUBENTRY_ID = "secondary-subentry"
DEV_ID = "device-1"


def _uwt_uid(subentry_id: str, dev_id: str, suffix: str = "uwt_mode") -> str:
    """Return the expected unique_id, built from the test's own model."""
    return f"googlefindmy_{ENTRY_ID}_{subentry_id}_{dev_id}_{suffix}"


def _flags_state(uwt_mode: bool = False) -> BLEBatteryState:
    """Return a fresh decoded hashed-flags state."""
    return BLEBatteryState(
        battery_level=1,
        battery_pct=100,
        uwt_mode=uwt_mode,
        decoded_flags=0,
        observed_at_wall=time.time(),
    )


class _CacheStub:
    """Coordinator cache stand-in; it carries only the owning entry id."""

    def __init__(self, entry_id: str) -> None:
        self.entry_id = entry_id


class _ResolverStub:
    """Resolver stand-in answering only ``get_ble_battery_state``."""

    def __init__(self) -> None:
        self.states: dict[str, BLEBatteryState] = {}

    def get_ble_battery_state(self, device_id: str) -> BLEBatteryState | None:
        return self.states.get(device_id)


def _make_hass(loop: asyncio.AbstractEventLoop) -> HomeAssistant:
    hass = HomeAssistant()
    hass.loop = loop
    hass.data = {"core.uuid": "test-instance"}
    hass.bus = SimpleNamespace(
        async_listen=lambda *_args, **_kwargs: lambda: None,
        async_listen_once=lambda *_args, **_kwargs: lambda: None,
    )
    hass.async_create_task = lambda coro, *, name=None: loop.create_task(
        coro, name=name
    )
    hass.async_run_hass_job = lambda job, *args: getattr(
        job, "target", lambda *_: None
    )(*args)
    hass.verify_event_loop_thread = lambda *_args, **_kwargs: None
    return hass


class _Harness:
    """Run the binary_sensor platform setup and record what it adds."""

    def __init__(
        self,
        stub_coordinator_factory: Callable[..., type[Any]],
        *,
        devices_by_key: dict[str, list[dict[str, Any]]],
        resolver: _ResolverStub | None,
        hidden: frozenset[str] = frozenset(),
        second_subentry: bool = False,
    ) -> None:
        loop = asyncio.get_running_loop()
        self.hass = _make_hass(loop)
        self.added: list[tuple[Any, str | None]] = []
        self.unloads: list[Any] = []

        subentries = {
            TRACKER_SUBENTRY_ID: ConfigSubentry(
                data={"group_key": TRACKER_KEY, "features": ("binary_sensor",)},
                subentry_type="tracker",
                title="Tracker",
                subentry_id=TRACKER_SUBENTRY_ID,
            )
        }
        if second_subentry:
            subentries[SECOND_SUBENTRY_ID] = ConfigSubentry(
                data={"group_key": SECOND_KEY, "features": ("binary_sensor",)},
                subentry_type="tracker",
                title="Secondary",
                subentry_id=SECOND_SUBENTRY_ID,
            )

        self.entry = make_config_entry(
            entry_id=ENTRY_ID,
            data={
                DATA_SECRET_BUNDLE: {"username": "user@example.com"},
                CONF_GOOGLE_EMAIL: "user@example.com",
            },
            subentries=MappingProxyType(subentries),
            async_on_unload=self.unloads.append,
        )

        coordinator_cls = stub_coordinator_factory(
            snapshot_callback=lambda key, _feature: devices_by_key.get(key or "", []),
        )
        self.coordinator = coordinator_cls(self.hass, cache=_CacheStub(ENTRY_ID))
        self.coordinator.config_entry = self.entry
        self.coordinator.is_device_visible_in_subentry = lambda _key, dev_id: (
            dev_id not in hidden
        )
        self.entry.runtime_data = SimpleNamespace(coordinator=self.coordinator)

        if resolver is not None:
            self.hass.data[DOMAIN] = {DATA_EID_RESOLVER: resolver}

        self.registry = er.async_get(self.hass)

    def register(self, entity_id: str, unique_id: str, subentry_id: str) -> None:
        """Record a registry row owned by the integration."""
        self.registry.record_entity(
            entity_id,
            platform=DOMAIN,
            unique_id=unique_id,
            config_entry_id=ENTRY_ID,
            config_entry_subentry_id=subentry_id,
        )

    def _add_entities(self, entities: list[Any], **kwargs: Any) -> None:
        config_subentry_id = kwargs.get("config_subentry_id")
        for entity in entities:
            entity.hass = self.hass
            if not getattr(entity, "entity_id", None):
                entity.entity_id = f"binary_sensor.{entity.unique_id}"
            self.added.append((entity, config_subentry_id))

    async def setup(self) -> None:
        await binary_sensor.async_setup_entry(self.hass, self.entry, self._add_entities)
        await asyncio.sleep(0)

    async def fire_listeners(self) -> None:
        """Run the coordinator listeners, as a coordinator update would."""
        for listener in list(self.coordinator._listeners):
            listener()
        await asyncio.sleep(0)

    def uwt(self) -> list[tuple[Any, str | None]]:
        return [
            (entity, config)
            for entity, config in self.added
            if isinstance(entity, binary_sensor.GoogleFindMyUWTModeSensor)
        ]

    def uwt_uids(self) -> list[str]:
        return [entity.unique_id for entity, _ in self.uwt()]


def _one_device() -> dict[str, list[dict[str, Any]]]:
    return {TRACKER_KEY: [{"id": DEV_ID, "name": "Tag"}]}


@pytest.mark.asyncio
async def test_registry_entry_no_data(stub_coordinator_factory: Any) -> None:
    """A registry row without BLE data creates the UWT sensor at setup."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
    )
    uid = _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID)
    harness.register("binary_sensor.tag_uwt_mode", uid, TRACKER_SUBENTRY_ID)

    await harness.setup()

    assert harness.uwt_uids() == [uid]
    assert [config for _, config in harness.uwt()] == [TRACKER_SUBENTRY_ID]


@pytest.mark.asyncio
async def test_data_no_registry(
    stub_coordinator_factory: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Decoded flags without a registry row create the UWT sensor (INFO)."""
    resolver = _ResolverStub()
    resolver.states[DEV_ID] = _flags_state(uwt_mode=True)
    harness = _Harness(
        stub_coordinator_factory, devices_by_key=_one_device(), resolver=resolver
    )

    with caplog.at_level(logging.INFO, logger=binary_sensor.__name__):
        await harness.setup()

    assert harness.uwt_uids() == [_uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID)]
    info = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.INFO
        and "UWT-Mode binary sensor created" in r.getMessage()
    ]
    assert info == [
        f"UWT-Mode binary sensor created for device={DEV_ID} (uwt_mode=True)"
    ]


@pytest.mark.asyncio
async def test_neither(stub_coordinator_factory: Any) -> None:
    """Without a registry row and without data no UWT sensor is created."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
    )

    await harness.setup()
    await harness.fire_listeners()

    assert harness.uwt_uids() == []


@pytest.mark.asyncio
async def test_registry_then_first_sighting(stub_coordinator_factory: Any) -> None:
    """A later first sighting does not add a second sensor for the same uid."""
    resolver = _ResolverStub()
    harness = _Harness(
        stub_coordinator_factory, devices_by_key=_one_device(), resolver=resolver
    )
    uid = _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID)
    harness.register("binary_sensor.tag_uwt_mode", uid, TRACKER_SUBENTRY_ID)

    await harness.setup()
    resolver.states[DEV_ID] = _flags_state()
    await harness.fire_listeners()
    await harness.fire_listeners()

    assert harness.uwt_uids() == [uid]


@pytest.mark.asyncio
async def test_no_registry_then_first_sighting(stub_coordinator_factory: Any) -> None:
    """The first sighting creates the UWT sensor exactly once via the listener."""
    resolver = _ResolverStub()
    harness = _Harness(
        stub_coordinator_factory, devices_by_key=_one_device(), resolver=resolver
    )

    await harness.setup()
    assert harness.uwt_uids() == []

    resolver.states[DEV_ID] = _flags_state()
    await harness.fire_listeners()
    await harness.fire_listeners()

    assert harness.uwt_uids() == [_uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID)]


@pytest.mark.asyncio
async def test_hidden(stub_coordinator_factory: Any) -> None:
    """A registry row of a hidden device does not revive the UWT sensor."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
        hidden=frozenset({DEV_ID}),
    )
    harness.register(
        "binary_sensor.tag_uwt_mode",
        _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID),
        TRACKER_SUBENTRY_ID,
    )

    await harness.setup()

    assert harness.uwt_uids() == []


@pytest.mark.asyncio
async def test_not_in_snapshot(stub_coordinator_factory: Any) -> None:
    """A registry row of a device missing from the snapshot stays dead."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
    )
    harness.register(
        "binary_sensor.gone_uwt_mode",
        _uwt_uid(TRACKER_SUBENTRY_ID, "device-gone"),
        TRACKER_SUBENTRY_ID,
    )

    await harness.setup()

    assert harness.uwt_uids() == []


@pytest.mark.asyncio
async def test_other_subentry_uid(stub_coordinator_factory: Any) -> None:
    """A registry row carrying another subentry's uid does not match."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
    )
    harness.register(
        "binary_sensor.tag_uwt_mode",
        _uwt_uid("foreign-subentry", DEV_ID),
        "foreign-subentry",
    )

    await harness.setup()

    assert harness.uwt_uids() == []


@pytest.mark.asyncio
async def test_two_tracker_subentries(stub_coordinator_factory: Any) -> None:
    """Each subentry gets only the sensors whose uid it registered.

    Both subentries see the same devices: the tracker scope of this platform
    reads the snapshot of the first tracker subentry's group key for every
    tracker subentry (existing behaviour, not changed here), so a per-key
    device split would test that, not the registry arm.
    """
    devices = [{"id": DEV_ID, "name": "Tag"}, {"id": "device-2", "name": "B"}]
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key={TRACKER_KEY: devices, SECOND_KEY: devices},
        resolver=_ResolverStub(),
        second_subentry=True,
    )
    second_uid = _uwt_uid(SECOND_SUBENTRY_ID, "device-2")
    harness.register("binary_sensor.b_uwt_mode", second_uid, SECOND_SUBENTRY_ID)
    tracker_uid = _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID)
    harness.register("binary_sensor.tag_uwt_mode", tracker_uid, TRACKER_SUBENTRY_ID)

    await harness.setup()

    assert sorted((entity.unique_id, config) for entity, config in harness.uwt()) == (
        sorted([(tracker_uid, TRACKER_SUBENTRY_ID), (second_uid, SECOND_SUBENTRY_ID)])
    )


@pytest.mark.asyncio
async def test_wrong_suffix_or_domain(stub_coordinator_factory: Any) -> None:
    """Rows with the battery suffix or the sensor domain create no UWT sensor."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
    )
    harness.register(
        "sensor.tag_ble_battery",
        _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID, "ble_battery"),
        TRACKER_SUBENTRY_ID,
    )
    harness.register(
        "sensor.tag_uwt_mode",
        _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID),
        TRACKER_SUBENTRY_ID,
    )

    await harness.setup()

    assert harness.uwt_uids() == []


@pytest.mark.asyncio
async def test_resolver_none_registry_entry(stub_coordinator_factory: Any) -> None:
    """Without any resolver a registry row still creates exactly one sensor."""
    harness = _Harness(
        stub_coordinator_factory, devices_by_key=_one_device(), resolver=None
    )
    uid = _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID)
    harness.register("binary_sensor.tag_uwt_mode", uid, TRACKER_SUBENTRY_ID)

    await harness.setup()
    await harness.fire_listeners()

    assert harness.uwt_uids() == [uid]


@pytest.mark.asyncio
async def test_registry_path_logs_debug(
    stub_coordinator_factory: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """The registry path logs at DEBUG with the device id, never at INFO."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
    )
    harness.register(
        "binary_sensor.tag_uwt_mode",
        _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID),
        TRACKER_SUBENTRY_ID,
    )

    with caplog.at_level(logging.DEBUG, logger=binary_sensor.__name__):
        await harness.setup()

    records = [
        r for r in caplog.records if "UWT-Mode binary sensor created" in r.getMessage()
    ]
    assert [r.levelno for r in records] == [logging.DEBUG]
    assert records[0].getMessage() == (
        f"UWT-Mode binary sensor created from entity registry for device={DEV_ID}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("present", "available"),
    [(True, True), (False, False)],
    ids=["present_is_unknown", "absent_is_unavailable"],
)
async def test_registry_sensor_without_data_state(
    stub_coordinator_factory: Any, present: bool, available: bool
) -> None:
    """Until the first sighting: unknown while present, unavailable while absent.

    Covers the cases ``present_is_unknown`` and ``absent_is_unavailable``.
    """
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_ResolverStub(),
    )
    harness.coordinator.is_device_present = lambda _dev_id: present
    harness.register(
        "binary_sensor.tag_uwt_mode",
        _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID),
        TRACKER_SUBENTRY_ID,
    )

    await harness.setup()

    created = harness.uwt()
    assert len(created) == 1
    entity = created[0][0]
    assert entity.is_on is None
    assert entity.available is available


class _FailingResolver(_ResolverStub):
    """Resolver stand-in whose lookup raises."""

    def get_ble_battery_state(self, device_id: str) -> BLEBatteryState | None:
        raise RuntimeError("resolver lookup failed")


@pytest.mark.asyncio
@pytest.mark.parametrize("registered", [True, False], ids=["registry", "no_registry"])
async def test_resolver_error_counts_as_no_data(
    stub_coordinator_factory: Any, registered: bool
) -> None:
    """A failing resolver lookup counts as no data; the registry arm still works."""
    harness = _Harness(
        stub_coordinator_factory,
        devices_by_key=_one_device(),
        resolver=_FailingResolver(),
    )
    uid = _uwt_uid(TRACKER_SUBENTRY_ID, DEV_ID)
    if registered:
        harness.register("binary_sensor.tag_uwt_mode", uid, TRACKER_SUBENTRY_ID)

    await harness.setup()

    assert harness.uwt_uids() == ([uid] if registered else [])
