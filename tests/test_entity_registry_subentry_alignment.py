# tests/test_entity_registry_subentry_alignment.py
"""Ensure entity registry entries inherit the coordinator's subentry mapping."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from custom_components.googlefindmy import binary_sensor, device_tracker, sensor
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    DATA_SECRET_BUNDLE,
    DOMAIN,
    OPT_ENABLE_STATS_ENTITIES,
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
)


class _ConfigEntryStub:
    """Minimal config entry stub for platform setup tests."""

    def __init__(self) -> None:
        self.entry_id = "entry-registry"
        self.data: dict[str, Any] = {
            DATA_SECRET_BUNDLE: {"username": "user@example.com"},
            CONF_GOOGLE_EMAIL: "user@example.com",
            OPT_ENABLE_STATS_ENTITIES: True,
        }
        self.options: dict[str, Any] = {OPT_ENABLE_STATS_ENTITIES: True}
        self.title = "Registry Alignment"
        self.runtime_data: Any | None = None
        self.subentries: dict[str, Any] = {}
        self._unload_callbacks: list[Callable[[], None]] = []

    def async_on_unload(self, callback: Callable[[], None]) -> None:
        self._unload_callbacks.append(callback)


def _make_entity_id(domain: str, unique_id: str, *, fallback_index: int) -> str:
    """Return a sanitized entity_id derived from the provided unique_id."""

    if not unique_id:
        return f"{domain}.generated_{fallback_index}"
    safe = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in unique_id.lower())
    if not safe:
        safe = f"generated_{fallback_index}"
    return f"{domain}.{safe}"


def _make_add_entities(
    hass: HomeAssistant,
    entry: _ConfigEntryStub,
    registry: Any,
    *,
    domain: str,
    loop: asyncio.AbstractEventLoop,
) -> tuple[Callable[[Iterable[Any], bool], None], list[asyncio.Task[Any]]]:
    """Return an async_add_entities callback that records registry assignments."""

    pending: list[asyncio.Task[Any]] = []

    def _async_add_entities(
        entities: Iterable[Any], update_before_add: bool = False
    ) -> None:
        del update_before_add
        for index, entity in enumerate(entities):
            unique_id = getattr(entity, "unique_id", None)
            if not isinstance(unique_id, str):
                unique_id = f"{domain}-entity-{index}"
            entity_id = _make_entity_id(domain, unique_id, fallback_index=index)
            entity.entity_id = entity_id
            entity.hass = hass
            registry.record_entity(
                entity_id,
                platform=DOMAIN,
                unique_id=unique_id,
                config_entry_id=entry.entry_id,
                config_entry_subentry_id=getattr(entity, "subentry_identifier", None),
            )
            if hasattr(entity, "async_added_to_hass"):
                pending.append(loop.create_task(entity.async_added_to_hass()))

    return _async_add_entities, pending


@pytest.mark.asyncio
async def test_entity_registry_subentry_alignment(
    stub_coordinator_factory: Callable[..., type[Any]],
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> None:
    """Platforms should register entities with the correct subentry identifiers."""

    del (
        deterministic_config_subentry_id
    )  # fixture side effects patch ensure_config_subentry_id

    loop = asyncio.get_running_loop()
    hass = HomeAssistant()
    hass.loop = loop
    hass.data = {"core.uuid": "test-instance"}
    hass.async_create_task = lambda coro, *, name=None: loop.create_task(
        coro, name=name
    )
    hass.bus = SimpleNamespace(
        async_listen=lambda *_args, **_kwargs: lambda: None,
        async_listen_once=lambda *_args, **_kwargs: lambda: None,
    )

    entry = _ConfigEntryStub()

    def _get_device_location_data_for_subentry(
        self: Any, subentry_key: str, device_id: str
    ) -> dict[str, Any] | None:
        for device in self.get_subentry_snapshot(subentry_key):
            if device.get("id") == device_id:
                return device
        return None

    coordinator_cls = stub_coordinator_factory(
        data=[{"id": "tracker-1", "name": "Tracker One"}],
        stats={"background_updates": 5},
        methods={
            "get_device_location_data_for_subentry": _get_device_location_data_for_subentry,
        },
    )
    coordinator = coordinator_cls(
        hass,
        cache=SimpleNamespace(entry_id=entry.entry_id),
    )
    coordinator.config_entry = entry
    entry.runtime_data = SimpleNamespace(coordinator=coordinator)

    registry = er.async_get(hass)

    binary_add, binary_tasks = _make_add_entities(
        hass,
        entry,
        registry,
        domain="binary_sensor",
        loop=loop,
    )
    await binary_sensor.async_setup_entry(hass, entry, binary_add)

    sensor_add, sensor_tasks = _make_add_entities(
        hass,
        entry,
        registry,
        domain="sensor",
        loop=loop,
    )
    await sensor.async_setup_entry(hass, entry, sensor_add)

    tracker_add, tracker_tasks = _make_add_entities(
        hass,
        entry,
        registry,
        domain="device_tracker",
        loop=loop,
    )
    await device_tracker.async_setup_entry(hass, entry, tracker_add)

    await asyncio.gather(*binary_tasks, *sensor_tasks, *tracker_tasks)

    service_identifier = coordinator.stable_subentry_identifier(
        key=SERVICE_SUBENTRY_KEY
    )
    tracker_identifier = coordinator.stable_subentry_identifier(
        key=TRACKER_SUBENTRY_KEY
    )

    entries = list(getattr(registry, "entities", {}).values())
    assert entries, "platform setup should register at least one entity"

    # Per-device tracker-scope sensors are identified by their unique_id suffix:
    # last_seen (timestamp), plus_code (Open Location Code), location_status
    # (age class) and coarse_fix (discarded coarse fix) belong to the tracker
    # subentry, not the diagnostic service scope.
    tracker_sensor_suffixes = (
        "_last_seen",
        "_plus_code",
        "_location_status",
        "_coarse_fix",
    )

    service_entries = [
        item
        for item in entries
        if item.domain == "binary_sensor"
        or (
            item.domain == "sensor"
            and not str(item.unique_id).endswith(tracker_sensor_suffixes)
        )
    ]
    tracker_entries = [
        item
        for item in entries
        if item.domain == "device_tracker"
        or str(item.unique_id).endswith(tracker_sensor_suffixes)
    ]

    assert service_entries, "diagnostic entities must be present for validation"
    assert tracker_entries, "tracker entities must be present for validation"

    for entry_record in service_entries:
        assert entry_record.config_entry_subentry_id == service_identifier, (
            f"service entity {entry_record.entity_id} lost its subentry mapping"
        )

    for entry_record in tracker_entries:
        assert entry_record.config_entry_subentry_id == tracker_identifier, (
            f"tracker entity {entry_record.entity_id} should map to tracker subentry"
        )
