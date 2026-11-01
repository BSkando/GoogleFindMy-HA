# tests/test_services_rebuild_registry.py
"""Regression tests for the googlefindmy.rebuild_registry service."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from homeassistant.core import ServiceCall

from custom_components import googlefindmy as integration
from custom_components.googlefindmy import _async_unload_parent_entry, dr, er, services
from custom_components.googlefindmy.const import (
    DOMAIN,
    SERVICE_REBUILD_REGISTRY,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_SUBENTRY_KEY,
    service_device_identifier,
)
from tests.helpers import (
    FakeConfigEntriesManager,
    FakeConfigEntry,
    FakeDeviceEntry,
    FakeDeviceRegistry,
    FakeEntityRegistry,
    FakeHass,
    config_entry_with_runtime_managed_subentries,
    device_registry_async_entries_for_config_entry,
    service_device_stub,
)

UPDATED_COUNT = 2


async def _register_rebuild_service(hass: FakeHass, ctx: dict[str, Any]) -> Any:
    """Helper to register the rebuild service and return its handler."""

    await services.async_register_services(hass, ctx)
    return hass.services.handlers[(DOMAIN, SERVICE_REBUILD_REGISTRY)]


@pytest.mark.asyncio
async def test_rebuild_registry_reloads_primary_entry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When no entry IDs are provided, reload the first config entry."""

    manager = FakeConfigEntriesManager(
        [
            FakeConfigEntry(entry_id="primary"),
            FakeConfigEntry(entry_id="secondary"),
        ]
    )
    hass = FakeHass(manager)

    handler = await _register_rebuild_service(hass, {})

    caplog.set_level(logging.INFO)
    await handler(ServiceCall({}))

    assert manager.reload_calls == ["primary"]
    assert any(
        "Reloading config entry: primary" in record.message for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rebuild_registry_runs_migration_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invoke migration helpers when the mode is set to ``migrate``."""

    manager = FakeConfigEntriesManager([FakeConfigEntry(entry_id="primary")])
    hass = FakeHass(manager)

    soft_migrate = AsyncMock()
    migrate_unique_ids = AsyncMock()
    relink_buttons = AsyncMock()
    relink_entities = AsyncMock()

    handler = await _register_rebuild_service(
        hass,
        {
            "soft_migrate_entry": soft_migrate,
            "migrate_unique_ids": migrate_unique_ids,
            "relink_button_devices": relink_buttons,
            "relink_subentry_entities": relink_entities,
        },
    )

    await handler(ServiceCall({"mode": "migrate"}))

    assert manager.reload_calls == ["primary"]
    entry = manager.async_entries()[0]
    soft_migrate.assert_awaited_once_with(hass, entry)
    migrate_unique_ids.assert_awaited_once_with(hass, entry)
    relink_buttons.assert_awaited_once_with(hass, entry)
    relink_entities.assert_awaited_once_with(hass, entry)


@pytest.mark.asyncio
async def test_rebuild_registry_reloads_specific_ids(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Reload only the config entries explicitly requested by ID."""

    manager = FakeConfigEntriesManager(
        [
            FakeConfigEntry(entry_id="primary"),
            FakeConfigEntry(entry_id="secondary"),
            FakeConfigEntry(entry_id="tertiary"),
        ]
    )
    hass = FakeHass(manager)

    handler = await _register_rebuild_service(hass, {})

    caplog.set_level(logging.INFO)
    await handler(
        ServiceCall({services.ATTR_ENTRY_ID: ["secondary", "missing", "primary"]})
    )

    assert manager.reload_calls == ["secondary", "primary"]
    assert any(
        "Reloading config entries: ['secondary', 'primary']" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rebuild_registry_filters_by_device_ids(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Select config entries based on the provided device IDs when reloading."""

    manager = FakeConfigEntriesManager(
        [
            FakeConfigEntry(entry_id="primary"),
            FakeConfigEntry(entry_id="secondary"),
        ]
    )
    hass = FakeHass(manager)

    device_registry = FakeDeviceRegistry(
        [
            FakeDeviceEntry(
                id="dev-secondary",
                config_entries={"secondary"},
            )
        ]
    )
    monkeypatch.setattr(dr, "async_get", lambda hass: device_registry)

    handler = await _register_rebuild_service(hass, {})

    caplog.set_level(logging.INFO)
    await handler(ServiceCall({"device_ids": ["dev-secondary"]}))

    assert manager.reload_calls == ["secondary"]
    assert any(
        "Reloading config entry: secondary" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rebuild_registry_accepts_single_entry_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Treat a lone entry ID string as a single-item reload request."""

    manager = FakeConfigEntriesManager(
        [
            FakeConfigEntry(entry_id="primary"),
            FakeConfigEntry(entry_id="secondary"),
        ]
    )
    hass = FakeHass(manager)

    handler = await _register_rebuild_service(hass, {})

    caplog.set_level(logging.INFO)
    await handler(ServiceCall({services.ATTR_ENTRY_ID: "primary"}))

    assert manager.reload_calls == ["primary"]
    assert any(
        "Reloading config entry: primary" in record.message for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rebuild_registry_logs_warning_for_invalid_ids(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Warn and exit when none of the provided entry IDs are valid."""

    manager = FakeConfigEntriesManager(
        [
            FakeConfigEntry(entry_id="primary"),
        ]
    )
    hass = FakeHass(manager)

    handler = await _register_rebuild_service(hass, {})

    caplog.set_level(logging.INFO)
    await handler(ServiceCall({services.ATTR_ENTRY_ID: ["missing-1", "missing-2"]}))

    assert manager.reload_calls == []
    assert any(
        "No valid config entries found for IDs" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rebuild_registry_handles_missing_entries(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Gracefully warn when the integration has no config entries to reload."""

    manager = FakeConfigEntriesManager([])
    hass = FakeHass(manager)

    handler = await _register_rebuild_service(hass, {})

    caplog.set_level(logging.INFO)
    await handler(ServiceCall({}))

    assert manager.reload_calls == []
    assert any(
        "No config entries available to reload." in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rebuild_registry_detaches_orphaned_tracker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove hub links when a tracker lacks the matching subentry."""

    entry = FakeConfigEntry(entry_id="hub-entry", title="Hub Entry")
    tracker_subentry_id = "tracker-subentry"
    tracker_entry_id = "tracker-config-entry"
    service_subentry_id = "service-subentry"

    service_device = service_device_stub(
        entry_id=entry.entry_id,
        service_subentry_id=service_subentry_id,
        device_id="service-device",
        name="Service Device",
        include_hub_link=True,
    )
    orphan_tracker = SimpleNamespace(
        id="orphan-tracker",
        identifiers={(DOMAIN, "tracker-orphan")},
        config_entries={entry.entry_id},
        name="Orphan Tracker",
        config_entries_subentries={entry.entry_id: {None}},
        config_subentry_id=None,
    )

    class RecordingDeviceRegistry:
        """Minimal registry stub tracking update calls for assertions."""

        def __init__(self, devices: Iterable[Any]) -> None:
            self._devices = {device.id: device for device in devices}
            self._identifier_index = {
                identifier: device
                for device in devices
                for identifier in device.identifiers
            }
            self.updated: list[tuple[str, dict[str, Any]]] = []

        def async_get_device(
            self, *, identifiers: set[tuple[str, str]] | None = None, **_: Any
        ) -> Any | None:
            if not identifiers:
                return None
            for identifier in identifiers:
                device = self._identifier_index.get(identifier)
                if device is not None:
                    return device
            return None

        def async_entries_for_config_entry(self, entry_id: str) -> tuple[Any, ...]:
            return tuple(
                device
                for device in self._devices.values()
                if entry_id in device.config_entries
            )

        def async_update_device(  # noqa: PLR0912
            self, device_id: str, **changes: Any
        ) -> None:
            device = self._devices[device_id]
            if "remove_config_entry_id" in changes:
                entry_to_remove = changes["remove_config_entry_id"]
                mapping = getattr(device, "config_entries_subentries", None)
                removed_entry = False
                if isinstance(mapping, dict):
                    subset = mapping.get(entry_to_remove)
                    if subset is not None:
                        if changes.get("remove_config_subentry_id") is None:
                            if None in subset:
                                subset.discard(None)
                            if not subset:
                                mapping.pop(entry_to_remove, None)
                                removed_entry = True
                        else:
                            subset.discard(changes["remove_config_subentry_id"])
                            if not subset:
                                mapping.pop(entry_to_remove, None)
                                removed_entry = True
                    else:
                        removed_entry = True
                else:
                    removed_entry = True

                if removed_entry:
                    device.config_entries.discard(entry_to_remove)
                elif isinstance(mapping, dict):
                    subset = mapping.get(entry_to_remove)
                    if subset:
                        non_null = [item for item in subset if item is not None]
                        if non_null and len(subset - {None}) == 1:
                            device.config_subentry_id = non_null[0]
                        elif len(subset) == 1 and None in subset:
                            device.config_subentry_id = None
                if removed_entry and not getattr(
                    device, "config_entries_subentries", {}
                ):
                    device.config_subentry_id = None
            self.updated.append((device_id, dict(changes)))

    registry = RecordingDeviceRegistry([service_device, orphan_tracker])

    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)

    tracker_metadata = SimpleNamespace(
        config_subentry_id=tracker_subentry_id,
        entry_id=tracker_entry_id,
    )
    service_metadata = SimpleNamespace(
        config_subentry_id=service_subentry_id,
        entry_id="service-config-entry",
    )

    def _metadata(*, key: str) -> SimpleNamespace | None:
        if key == TRACKER_SUBENTRY_KEY:
            return tracker_metadata
        if key == SERVICE_SUBENTRY_KEY:
            return service_metadata
        return None

    coordinator = SimpleNamespace(
        config_entry=entry,
        data=[],
        name="Coordinator",
        _ensure_registry_for_devices=lambda devices, ignored: 0,
        _get_ignored_set=set,
        _ensure_service_device_exists=lambda: None,
        get_subentry_metadata=_metadata,
    )

    runtime = SimpleNamespace(coordinator=coordinator)
    entry.runtime_data = runtime
    hass.data.setdefault(DOMAIN, {}).setdefault("entries", {})[entry.entry_id] = runtime

    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_get",
        lambda hass: registry,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_entries_for_config_entry",
        device_registry_async_entries_for_config_entry,
        raising=False,
    )

    entity_registry = FakeEntityRegistry()
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.er.async_get",
        lambda hass: entity_registry,
    )

    await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert len(registry.updated) == UPDATED_COUNT
    assert (
        "service-device",
        {
            "remove_config_entry_id": entry.entry_id,
            "remove_config_subentry_id": None,
        },
    ) in registry.updated
    assert (
        "orphan-tracker",
        {
            "remove_config_entry_id": entry.entry_id,
            "remove_config_subentry_id": None,
        },
    ) in registry.updated
    assert entry.entry_id not in orphan_tracker.config_entries
    assert service_device.config_entries_subentries[entry.entry_id] == {
        service_subentry_id
    }
    assert service_device.config_subentry_id == service_subentry_id
    assert entry.entry_id in service_device.config_entries


@pytest.mark.asyncio
async def test_rebuild_registry_reload_skips_subentry_keyword_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Treat missing ``config_subentry_id`` support as a successful unload.

    T8: unloading a loaded entry keeps its registry entries, so the ``rebuild``
    mode removes them itself between unload and setup (``services.yaml``:
    "remove entities/devices and reload").
    """

    subentry_id = "tracker-subentry"
    subentry = SimpleNamespace(
        subentry_id=subentry_id,
        config_subentry_id=subentry_id,
        subentry_type=SUBENTRY_TYPE_TRACKER,
        data={"subentry_type": SUBENTRY_TYPE_TRACKER},
    )
    entry = config_entry_with_runtime_managed_subentries(
        entry_id="parent-entry", subentries=[subentry]
    )

    class RecordingEntityRegistry:
        def __init__(self, entries: Iterable[Any]) -> None:
            self._entries = {entry.entity_id: entry for entry in entries}
            self.removed: list[str] = []

        def async_entries_for_config_entry(self, entry_id: str) -> tuple[Any, ...]:
            return tuple(
                entity
                for entity in self._entries.values()
                if getattr(entity, "config_entry_id", None) == entry_id
            )

        def async_remove(self, entity_id: str) -> None:
            self.removed.append(entity_id)
            self._entries.pop(entity_id, None)

    class RecordingDeviceRegistry:
        def __init__(self, devices: Iterable[Any]) -> None:
            self._devices = {device.id: device for device in devices}
            self.updated: list[tuple[str, dict[str, Any]]] = []

        def async_entries_for_config_entry(self, entry_id: str) -> tuple[Any, ...]:
            return tuple(
                device
                for device in self._devices.values()
                if entry_id in getattr(device, "config_entries", set())
            )

        def async_get(self, device_id: str) -> Any | None:
            return self._devices.get(device_id)

        def async_update_device(self, device_id: str, **changes: Any) -> None:
            device = self._devices[device_id]
            mapping = getattr(device, "config_entries_subentries", {})
            if "remove_config_entry_id" in changes:
                entry_to_remove = changes["remove_config_entry_id"]
                subset = mapping.get(entry_to_remove)
                subentry_to_remove = changes.get("remove_config_subentry_id")
                if isinstance(subset, set):
                    if subentry_to_remove is None:
                        subset.discard(None)
                        subset.clear()
                    else:
                        subset.discard(subentry_to_remove)
                    if not subset:
                        mapping.pop(entry_to_remove, None)
                getattr(device, "config_entries", set()).discard(entry_to_remove)
                if not mapping.get(entry_to_remove):
                    device.config_subentry_id = None

            self.updated.append((device_id, dict(changes)))

    class LegacyForwardUnloadManager(FakeConfigEntriesManager):
        async def async_forward_entry_unload(self, entry: Any, domain: str) -> bool:
            raise ValueError("unexpected fallback execution")

        async def async_unload_platforms(
            self, entry: FakeConfigEntry, platforms: Iterable[str]
        ) -> bool:
            return True

    manager = LegacyForwardUnloadManager([entry])
    hass = FakeHass(manager)
    hass.data.setdefault(DOMAIN, {}).setdefault("entries", {})[entry.entry_id] = (
        entry.runtime_data
    )

    tracker_device = SimpleNamespace(
        id="tracker-device",
        identifiers={(DOMAIN, "tracker-device")},
        config_entries={entry.entry_id},
        config_entries_subentries={entry.entry_id: {subentry_id}},
        config_subentry_id=subentry_id,
    )
    entity_entry = SimpleNamespace(
        entity_id="device_tracker.tracker",
        config_entry_id=entry.entry_id,
        config_subentry_id=subentry_id,
    )

    entity_registry = RecordingEntityRegistry([entity_entry])
    device_registry = RecordingDeviceRegistry([tracker_device])

    monkeypatch.setattr(er, "async_get", lambda hass: entity_registry)
    monkeypatch.setattr(
        er,
        "async_entries_for_config_entry",
        lambda registry, entry_id: registry.async_entries_for_config_entry(entry_id),
    )
    monkeypatch.setattr(dr, "async_get", lambda hass: device_registry)
    monkeypatch.setattr(
        dr,
        "async_entries_for_config_entry",
        device_registry_async_entries_for_config_entry,
        raising=False,
    )

    steps: list[str] = []

    async def _unload(entry_id: str) -> bool:
        steps.append("unload")
        return await _async_unload_parent_entry(hass, entry)

    async def _setup(entry_id: str) -> bool:
        steps.append("setup")
        return True

    purge_entry = integration._async_purge_entry_registrations

    async def _purge(hass_obj: Any, entry_obj: Any) -> tuple[int, int]:
        steps.append("purge")
        return await purge_entry(hass_obj, entry_obj)

    manager.async_unload = _unload  # type: ignore[attr-defined]
    manager.async_setup = _setup  # type: ignore[method-assign]

    handler = await _register_rebuild_service(
        hass, {"purge_entry_registrations": _purge}
    )
    await handler(ServiceCall({services.ATTR_ENTRY_ID: entry.entry_id}))

    assert steps == ["unload", "purge", "setup"]
    assert manager.reload_calls == []
    assert entity_registry.removed == [entity_entry.entity_id]
    assert not tracker_device.config_entries_subentries.get(entry.entry_id)
    assert device_registry.updated == [
        (
            tracker_device.id,
            {
                "remove_config_entry_id": entry.entry_id,
                "remove_config_subentry_id": subentry_id,
            },
        )
    ]


@pytest.mark.asyncio
async def test_rebuild_registry_migrate_mode_does_not_purge() -> None:
    """T8: the ``migrate`` mode keeps registry entries and only reloads."""

    entry = FakeConfigEntry(entry_id="migrate-entry")
    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)
    purge = AsyncMock(return_value=(0, 0))

    handler = await _register_rebuild_service(
        hass, {"purge_entry_registrations": purge}
    )
    await handler(
        ServiceCall({services.ATTR_ENTRY_ID: entry.entry_id, "mode": "migrate"})
    )

    purge.assert_not_awaited()
    assert manager.reload_calls == [entry.entry_id]


@pytest.mark.asyncio
async def test_rebuild_registry_keeps_entries_when_unload_fails(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T8: no purge and no setup when the entry could not be unloaded."""

    entry = FakeConfigEntry(entry_id="stuck-entry")
    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)
    manager.async_unload = AsyncMock(return_value=False)  # type: ignore[attr-defined]
    purge = AsyncMock(return_value=(0, 0))

    handler = await _register_rebuild_service(
        hass, {"purge_entry_registrations": purge}
    )
    caplog.set_level(logging.ERROR)
    await handler(ServiceCall({services.ATTR_ENTRY_ID: entry.entry_id}))

    purge.assert_not_awaited()
    assert manager.setup_calls == []
    assert manager.reload_calls == []
    assert any(
        "could not be unloaded" in record.getMessage() for record in caplog.records
    )


@pytest.mark.asyncio
async def test_rebuild_registry_stops_when_entry_vanished_after_unload() -> None:
    """T8: an entry removed during its unload is neither purged nor set up."""

    entry = FakeConfigEntry(entry_id="vanishing-entry")
    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)

    async def _unload(entry_id: str) -> bool:
        manager._entries = [e for e in manager._entries if e.entry_id != entry_id]
        return True

    manager.async_unload = _unload  # type: ignore[attr-defined]
    purge = AsyncMock(return_value=(0, 0))

    handler = await _register_rebuild_service(
        hass, {"purge_entry_registrations": purge}
    )
    await handler(ServiceCall({services.ATTR_ENTRY_ID: entry.entry_id}))

    purge.assert_not_awaited()
    assert manager.setup_calls == []


@pytest.mark.asyncio
async def test_rebuild_registry_purges_but_does_not_set_up_disabled_entry() -> None:
    """T8: a disabled entry is purged and stays unloaded, as a reload would leave it."""

    class _DisabledEntry(FakeConfigEntry):
        disabled_by = "user"

    entry = _DisabledEntry(entry_id="disabled-entry")
    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)
    manager.async_unload = AsyncMock(return_value=True)  # type: ignore[attr-defined]
    purge = AsyncMock(return_value=(1, 1))

    handler = await _register_rebuild_service(
        hass, {"purge_entry_registrations": purge}
    )
    await handler(ServiceCall({services.ATTR_ENTRY_ID: entry.entry_id}))

    purge.assert_awaited_once_with(hass, entry)
    assert manager.setup_calls == []
    assert manager.reload_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled", [False, True], ids=["active", "disabled"])
async def test_failed_purge_still_sets_the_entry_up_again(
    caplog: pytest.LogCaptureFixture, disabled: bool
) -> None:
    """B9 (R2-8): a purge that fails after the unload does not strand the entry.

    The unload has already happened, so an active entry must be set up again,
    or it stays unloaded until someone reloads it by hand or HA restarts. A
    disabled entry stays unloaded as before. The failure is logged without
    the exception text (AGENTS.md section 5).
    """

    class _Entry(FakeConfigEntry):
        disabled_by = "user" if disabled else None

    entry = _Entry(entry_id="purge-fails")
    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)
    manager.async_unload = AsyncMock(return_value=True)  # type: ignore[attr-defined]
    purge = AsyncMock(side_effect=RuntimeError("owner someone@example.com"))

    handler = await _register_rebuild_service(
        hass, {"purge_entry_registrations": purge}
    )
    caplog.set_level(logging.DEBUG)
    await handler(ServiceCall({services.ATTR_ENTRY_ID: entry.entry_id}))

    purge.assert_awaited_once_with(hass, entry)
    assert manager.setup_calls == ([] if disabled else [entry.entry_id])
    assert not any("someone@example.com" in r.getMessage() for r in caplog.records)
    assert any("RuntimeError" in r.getMessage() for r in caplog.records)


@pytest.mark.asyncio
async def test_rebuild_registry_detaches_redundant_hub_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Remove hub links even when the tracker subentry is already linked."""

    entry = FakeConfigEntry(entry_id="hub-entry", title="Hub Entry")
    tracker_subentry_id = "tracker-subentry"
    tracker_entry_id = "tracker-config-entry"
    service_subentry_id = "service-subentry"

    service_device = service_device_stub(
        entry_id=entry.entry_id,
        service_subentry_id=service_subentry_id,
        device_id="service-device",
        name="Service Device",
        include_hub_link=True,
    )
    redundant_tracker = SimpleNamespace(
        id="tracker-device",
        identifiers={(DOMAIN, "tracker-device")},
        config_entries={entry.entry_id, tracker_entry_id},
        name="Tracker Device",
        config_subentry_id=tracker_subentry_id,
        config_entries_subentries={
            entry.entry_id: {None},
            tracker_entry_id: {tracker_subentry_id},
        },
    )

    class RecordingDeviceRegistry:
        """Minimal registry stub tracking update calls for assertions."""

        def __init__(self, devices: Iterable[Any]) -> None:
            self._devices = {device.id: device for device in devices}
            self._identifier_index = {
                identifier: device
                for device in devices
                for identifier in device.identifiers
            }
            self.updated: list[tuple[str, dict[str, Any]]] = []

        def async_get_device(
            self, *, identifiers: set[tuple[str, str]] | None = None, **_: Any
        ) -> Any | None:
            if not identifiers:
                return None
            for identifier in identifiers:
                device = self._identifier_index.get(identifier)
                if device is not None:
                    return device
            return None

        def async_entries_for_config_entry(self, entry_id: str) -> tuple[Any, ...]:
            return tuple(
                device
                for device in self._devices.values()
                if entry_id in device.config_entries
            )

        def async_update_device(  # noqa: PLR0912
            self, device_id: str, **changes: Any
        ) -> None:
            device = self._devices[device_id]
            if "remove_config_entry_id" in changes:
                entry_to_remove = changes["remove_config_entry_id"]
                mapping = getattr(device, "config_entries_subentries", None)
                removed_entry = False
                if isinstance(mapping, dict):
                    subset = mapping.get(entry_to_remove)
                    if subset is not None:
                        if changes.get("remove_config_subentry_id") is None:
                            if None in subset:
                                subset.discard(None)
                            if not subset:
                                mapping.pop(entry_to_remove, None)
                                removed_entry = True
                        else:
                            subset.discard(changes["remove_config_subentry_id"])
                            if not subset:
                                mapping.pop(entry_to_remove, None)
                                removed_entry = True
                    else:
                        removed_entry = True
                else:
                    removed_entry = True

                if removed_entry:
                    device.config_entries.discard(entry_to_remove)
                elif isinstance(mapping, dict):
                    subset = mapping.get(entry_to_remove)
                    if subset:
                        non_null = [item for item in subset if item is not None]
                        if non_null and len(subset - {None}) == 1:
                            device.config_subentry_id = non_null[0]
                        elif len(subset) == 1 and None in subset:
                            device.config_subentry_id = None
                if removed_entry and not getattr(
                    device, "config_entries_subentries", {}
                ):
                    device.config_subentry_id = None
            self.updated.append((device_id, dict(changes)))

    registry = RecordingDeviceRegistry([service_device, redundant_tracker])

    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)

    tracker_metadata = SimpleNamespace(
        config_subentry_id=tracker_subentry_id,
        entry_id=tracker_entry_id,
    )
    service_metadata = SimpleNamespace(
        config_subentry_id=service_subentry_id,
        entry_id="service-config-entry",
    )

    def _metadata(*, key: str) -> SimpleNamespace | None:
        if key == TRACKER_SUBENTRY_KEY:
            return tracker_metadata
        if key == SERVICE_SUBENTRY_KEY:
            return service_metadata
        return None

    coordinator = SimpleNamespace(
        config_entry=entry,
        data=[],
        name="Coordinator",
        _ensure_registry_for_devices=lambda devices, ignored: 0,
        _get_ignored_set=set,
        _ensure_service_device_exists=lambda: None,
        get_subentry_metadata=_metadata,
    )

    runtime = SimpleNamespace(coordinator=coordinator)
    entry.runtime_data = runtime
    hass.data.setdefault(DOMAIN, {}).setdefault("entries", {})[entry.entry_id] = runtime

    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_get",
        lambda hass: registry,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_entries_for_config_entry",
        device_registry_async_entries_for_config_entry,
        raising=False,
    )

    entity_registry = FakeEntityRegistry()
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.er.async_get",
        lambda hass: entity_registry,
    )

    await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert len(registry.updated) == UPDATED_COUNT
    assert (
        "service-device",
        {
            "remove_config_entry_id": entry.entry_id,
            "remove_config_subentry_id": None,
        },
    ) in registry.updated
    assert (
        "tracker-device",
        {
            "remove_config_entry_id": entry.entry_id,
            "remove_config_subentry_id": None,
        },
    ) in registry.updated
    assert entry.entry_id not in redundant_tracker.config_entries
    mapping = getattr(redundant_tracker, "config_entries_subentries", {})
    assert entry.entry_id not in mapping
    assert service_device.config_entries_subentries[entry.entry_id] == {
        service_subentry_id
    }
    assert entry.entry_id in service_device.config_entries


@pytest.mark.asyncio
async def test_rebuild_registry_handles_legacy_remove_config_subentry_kwarg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry hub-detach updates when legacy cores reject the new keyword."""

    entry = FakeConfigEntry(entry_id="hub-entry", title="Hub Entry")
    tracker_subentry_id = "tracker-subentry"
    tracker_entry_id = "tracker-config-entry"
    service_subentry_id = "service-subentry"

    service_device = service_device_stub(
        entry_id=entry.entry_id,
        service_subentry_id=service_subentry_id,
        device_id="service-device",
        name="Service Device",
        include_hub_link=True,
    )
    redundant_tracker = SimpleNamespace(
        id="tracker-device",
        identifiers={(DOMAIN, "tracker-device")},
        config_entries={entry.entry_id, tracker_entry_id},
        name="Tracker Device",
        config_subentry_id=tracker_subentry_id,
        config_entries_subentries={
            entry.entry_id: {None},
            tracker_entry_id: {tracker_subentry_id},
        },
    )

    class RaisingDeviceRegistry:
        """Registry stub that raises on ``remove_config_subentry_id`` usage."""

        def __init__(self, devices: Iterable[Any]) -> None:
            self._devices = {device.id: device for device in devices}
            self._identifier_index = {
                identifier: device
                for device in devices
                for identifier in device.identifiers
            }
            self.calls: list[tuple[str, dict[str, Any]]] = []
            self.updated: list[tuple[str, dict[str, Any]]] = []

        def async_get_device(
            self, *, identifiers: set[tuple[str, str]] | None = None, **_: Any
        ) -> Any | None:
            if not identifiers:
                return None
            for identifier in identifiers:
                device = self._identifier_index.get(identifier)
                if device is not None:
                    return device
            return None

        def async_entries_for_config_entry(self, entry_id: str) -> tuple[Any, ...]:
            return tuple(
                device
                for device in self._devices.values()
                if entry_id in device.config_entries
            )

        def async_get(self, device_id: str) -> Any | None:
            return self._devices.get(device_id)

        def async_update_device(  # noqa: PLR0912
            self, device_id: str, **changes: Any
        ) -> None:
            self.calls.append((device_id, dict(changes)))
            if "remove_config_subentry_id" in changes:
                raise TypeError(
                    "unexpected keyword argument 'remove_config_subentry_id'"
                )

            device = self._devices[device_id]
            if "remove_config_entry_id" in changes:
                entry_to_remove = changes["remove_config_entry_id"]
                mapping = getattr(device, "config_entries_subentries", None)
                removed_entry = False
                if isinstance(mapping, dict):
                    subset = mapping.get(entry_to_remove)
                    if subset is not None:
                        if changes.get("remove_config_subentry_id") is None:
                            if None in subset:
                                subset.discard(None)
                            if not subset:
                                mapping.pop(entry_to_remove, None)
                                removed_entry = True
                        else:
                            subset.discard(changes["remove_config_subentry_id"])
                            if not subset:
                                mapping.pop(entry_to_remove, None)
                                removed_entry = True
                    else:
                        removed_entry = True
                else:
                    removed_entry = True

                if removed_entry:
                    device.config_entries.discard(entry_to_remove)
                elif isinstance(mapping, dict):
                    subset = mapping.get(entry_to_remove)
                    if subset:
                        non_null = [item for item in subset if item is not None]
                        if non_null and len(subset - {None}) == 1:
                            device.config_subentry_id = non_null[0]
                        elif len(subset) == 1 and None in subset:
                            device.config_subentry_id = None
                if removed_entry and not getattr(
                    device, "config_entries_subentries", {}
                ):
                    device.config_subentry_id = None

            self.updated.append((device_id, dict(changes)))

    registry = RaisingDeviceRegistry([service_device, redundant_tracker])

    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)

    tracker_metadata = SimpleNamespace(
        config_subentry_id=tracker_subentry_id,
        entry_id=tracker_entry_id,
    )
    service_metadata = SimpleNamespace(
        config_subentry_id=service_subentry_id,
        entry_id="service-config-entry",
    )

    def _metadata(*, key: str) -> SimpleNamespace | None:
        if key == TRACKER_SUBENTRY_KEY:
            return tracker_metadata
        if key == SERVICE_SUBENTRY_KEY:
            return service_metadata
        return None

    coordinator = SimpleNamespace(
        config_entry=entry,
        data=[],
        name="Coordinator",
        _ensure_registry_for_devices=lambda devices, ignored: 0,
        _get_ignored_set=set,
        _ensure_service_device_exists=lambda: None,
        get_subentry_metadata=_metadata,
    )

    runtime = SimpleNamespace(coordinator=coordinator)
    entry.runtime_data = runtime
    hass.data.setdefault(DOMAIN, {}).setdefault("entries", {})[entry.entry_id] = runtime

    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_get",
        lambda hass: registry,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_entries_for_config_entry",
        device_registry_async_entries_for_config_entry,
        raising=False,
    )

    entity_registry = FakeEntityRegistry()
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.er.async_get",
        lambda hass: entity_registry,
    )

    await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert registry.calls == [
        (
            "service-device",
            {
                "remove_config_entry_id": entry.entry_id,
                "remove_config_subentry_id": None,
            },
        ),
        ("service-device", {"remove_config_entry_id": entry.entry_id}),
        (
            "tracker-device",
            {
                "remove_config_entry_id": entry.entry_id,
                "remove_config_subentry_id": None,
            },
        ),
        ("tracker-device", {"remove_config_entry_id": entry.entry_id}),
    ]
    assert registry.updated == [
        ("service-device", {"remove_config_entry_id": entry.entry_id}),
        ("tracker-device", {"remove_config_entry_id": entry.entry_id}),
    ]
    assert entry.entry_id not in redundant_tracker.config_entries
    assert (
        redundant_tracker.config_entries_subentries.get(entry.entry_id, set()) == set()
    )
    assert service_device.config_entries_subentries[entry.entry_id] == {
        service_subentry_id
    }
    assert service_device.config_subentry_id == service_subentry_id


# ---------------------------------------------------------------------------
# AP-13: the hub detach on a single-owner core (Home Assistant 2026.8+)
#
# The tests above run against a registry double that models the *pre*-2026.8
# world, where a device holds a set of links and detaching one leaves the
# others. From 2026.8 a device has a single owning entry, so giving that entry
# up removes the device and every entity on it. These tests exercise the same
# service against ``SingleOwnerDeviceRegistry``, the double that is checked
# against real core in ``tests/test_device_registry_single_owner_contract.py``.
# ---------------------------------------------------------------------------


def _single_owner_hub_environment(
    *,
    entry_id: str = "hub-entry",
    tracker_subentry_id: str = "tracker-subentry",
    service_subentry_id: str = "service-subentry",
) -> tuple[Any, Any]:
    """Build the hass/coordinator scaffolding the cleanup service needs."""

    entry = FakeConfigEntry(entry_id=entry_id, title="Hub Entry")
    manager = FakeConfigEntriesManager([entry])
    hass = FakeHass(manager)

    tracker_metadata = SimpleNamespace(
        config_subentry_id=tracker_subentry_id, entry_id=entry_id
    )
    service_metadata = SimpleNamespace(
        config_subentry_id=service_subentry_id, entry_id=entry_id
    )

    def _metadata(*, key: str) -> SimpleNamespace | None:
        if key == TRACKER_SUBENTRY_KEY:
            return tracker_metadata
        if key == SERVICE_SUBENTRY_KEY:
            return service_metadata
        return None

    coordinator = SimpleNamespace(
        config_entry=entry,
        data=[],
        name="Coordinator",
        _ensure_registry_for_devices=lambda devices, ignored: 0,
        _get_ignored_set=set,
        _ensure_service_device_exists=lambda: None,
        get_subentry_metadata=_metadata,
    )
    runtime = SimpleNamespace(coordinator=coordinator)
    entry.runtime_data = runtime
    hass.data.setdefault(DOMAIN, {}).setdefault("entries", {})[entry_id] = runtime
    return hass, entry


def _patch_registries(monkeypatch: pytest.MonkeyPatch, registry: Any) -> None:
    """Point the service at ``registry`` and at an empty entity registry."""

    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_get",
        lambda hass: registry,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.dr.async_entries_for_config_entry",
        lambda reg, entry_id: reg.async_entries_for_config_entry(entry_id),
        raising=False,
    )
    entity_registry = FakeEntityRegistry()
    monkeypatch.setattr(
        "custom_components.googlefindmy.services.er.async_get",
        lambda hass: entity_registry,
    )


@pytest.mark.asyncio
async def test_single_owner_device_on_the_hub_link_is_given_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A device sitting on the entry root with no tracker link is detached.

    On this core giving up the owning entry *is* the removal, so the planner
    emits ``async_remove_device`` rather than a keyword pair. That the device is
    gone afterwards is the assertion; the keywords are not.
    """
    from tests.helpers.single_owner_device_registry import SingleOwnerDeviceRegistry

    registry = SingleOwnerDeviceRegistry()
    registry.add_config_entry("hub-entry", {"tracker-subentry", "service-subentry"})
    orphan = registry.add_device(
        identifiers={(DOMAIN, "orphan-device")},
        config_entry_id="hub-entry",
        config_subentry_id=None,
        name="Orphan",
    )
    registry.add_device(
        identifiers={service_device_identifier("hub-entry")},
        config_entry_id="hub-entry",
        config_subentry_id="service-subentry",
        name="Service",
    )

    hass, _entry = _single_owner_hub_environment()
    _patch_registries(monkeypatch, registry)

    await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert orphan.id not in registry.devices
    assert ("remove_device", {"device_id": orphan.id}) in registry.operations


@pytest.mark.asyncio
async def test_single_owner_device_in_a_subentry_is_left_alone(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A device that does not sit on the hub link is not touched.

    The planner answers "no-op" rather than "remove": the link the caller wants
    to give up is not the link the device sits on. Without that distinction this
    is the call that deletes a healthy tracker.
    """
    from tests.helpers.single_owner_device_registry import SingleOwnerDeviceRegistry

    registry = SingleOwnerDeviceRegistry()
    registry.add_config_entry("hub-entry", {"tracker-subentry", "service-subentry"})
    tracker = registry.add_device(
        identifiers={(DOMAIN, "tracker-device")},
        config_entry_id="hub-entry",
        config_subentry_id="other-subentry",
        name="Tracker",
    )
    registry.add_device(
        identifiers={service_device_identifier("hub-entry")},
        config_entry_id="hub-entry",
        config_subentry_id="service-subentry",
        name="Service",
    )

    hass, _entry = _single_owner_hub_environment()
    _patch_registries(monkeypatch, registry)

    with caplog.at_level(logging.INFO):
        await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert tracker.id in registry.devices
    assert not [op for op in registry.operations if op[0] == "remove_device"]
    # An empty plan still counts as a handled device. That is what the old code
    # did -- it sent the removal, core did nothing, and the caller counted it --
    # and the count feeds the summary the user sees. This assertion pins the
    # return value of the "nothing to detach" branch, which is otherwise free to
    # flip. It also pins an inaccuracy: the summary says "Removed 1" for a run
    # that removed nothing. That is pre-existing and out of scope here; making
    # the count follow the executed operations means changing the return type of
    # the detach helper and of ``_apply_device_ownership`` beside it.
    assert "Removed 1 orphaned device links" in caplog.text


class _ModernRegistryWithBlindDevices:
    """Single-owner signature, device entries that describe no ownership.

    Not a core: the combination cannot occur in Home Assistant, where the
    signature and the device shape ship together. It is the shape a partially
    migrated test double or a patched fork can present, and it is the only way
    to reach the planner's refusal from this service.
    """

    def __init__(self, devices: list[Any]) -> None:
        self._devices = {device.id: device for device in devices}
        self.updated: list[tuple[str, dict[str, Any]]] = []
        self.removed: list[str] = []

    def async_entries_for_config_entry(self, entry_id: str) -> list[Any]:
        return list(self._devices.values())

    def async_get(self, device_id: str) -> Any | None:
        return self._devices.get(device_id)

    def async_get_device_by_identifier(
        self, identifier: tuple[str, str], config_entry_id: str
    ) -> Any | None:
        for device in self._devices.values():
            if identifier in device.identifiers:
                return device
        return None

    def async_update_device(
        self,
        device_id: str,
        *,
        new_config_entry_id: Any = None,
        new_config_subentry_id: Any = None,
        **changes: Any,
    ) -> Any:
        self.updated.append((device_id, dict(changes)))
        return self._devices.get(device_id)

    def async_remove_device(self, device_id: str) -> None:
        self.removed.append(device_id)
        self._devices.pop(device_id, None)


@pytest.mark.asyncio
async def test_unknown_ownership_is_reported_instead_of_guessed(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The detach refuses when it cannot read the current ownership.

    Deleting on a guess is the failure this whole work package exists to avoid,
    so the refusal is a logged error and an untouched device, not a removal.
    """
    blind_device = SimpleNamespace(
        id="blind-device",
        identifiers={(DOMAIN, "blind-device")},
        config_entries={"hub-entry"},
        config_entries_subentries={"hub-entry": {None}},
        name="Blind",
    )
    service_device = service_device_stub(
        entry_id="hub-entry",
        service_subentry_id="service-subentry",
        name="Service",
    )
    registry = _ModernRegistryWithBlindDevices([service_device, blind_device])

    hass, _entry = _single_owner_hub_environment()
    _patch_registries(monkeypatch, registry)

    with caplog.at_level(logging.ERROR):
        await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert registry.removed == []
    assert registry.updated == []
    # The wording is the planner's own refusal, forwarded by the service through
    # its error template. "device entry not resolvable" is a different message,
    # raised by the coordinator's executor, and is deliberately not accepted
    # here: an ``or`` over two messages passes when neither path is the one
    # under test.
    assert "DETACH needs the current ownership" in caplog.text


@pytest.mark.asyncio
async def test_half_known_ownership_with_an_entry_is_treated_as_the_hub_link(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A device naming only its entry sits on the hub link, and is given up.

    One of the two half-known shapes the ``or`` in ``describes_ownership``
    admits. ``config_entry_id`` names us and ``config_subentry_id`` is absent,
    which is exactly the hub link, so the planner removes. Reading this as
    "unknown" instead would refuse and leave the orphan behind.
    """
    half_known = SimpleNamespace(
        id="half-known-device",
        identifiers={(DOMAIN, "half-known-device")},
        config_entry_id="hub-entry",
        name="Half known",
    )
    service_device = service_device_stub(
        entry_id="hub-entry",
        service_subentry_id="service-subentry",
        name="Service",
    )
    registry = _ModernRegistryWithBlindDevices([service_device, half_known])

    hass, _entry = _single_owner_hub_environment()
    _patch_registries(monkeypatch, registry)

    await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert registry.removed == ["half-known-device"]


@pytest.mark.asyncio
async def test_half_known_ownership_with_a_subentry_only_is_a_no_op(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other half-known shape names no entry, so it is not our hub link.

    ``config_subentry_id`` alone yields owner ``None``, which does not match this
    entry; the planner answers with an empty plan. The asymmetry between this
    test and its twin is why the comment beside the predicate no longer claims a
    single outcome for both shapes.
    """
    half_known = SimpleNamespace(
        id="half-known-device",
        identifiers={(DOMAIN, "half-known-device")},
        config_entries={"hub-entry"},
        config_subentry_id="some-subentry",
        name="Half known",
    )
    service_device = service_device_stub(
        entry_id="hub-entry",
        service_subentry_id="service-subentry",
        name="Service",
    )
    registry = _ModernRegistryWithBlindDevices([service_device, half_known])

    hass, _entry = _single_owner_hub_environment()
    _patch_registries(monkeypatch, registry)

    await services.async_rebuild_device_registry(hass, ServiceCall({}))

    assert registry.removed == []
    assert registry.updated == []


def test_attr_entry_id_matches_rebuild_registry_service_field() -> None:
    """``ATTR_ENTRY_ID`` is the ``rebuild_registry`` field name in ``services.yaml``.

    Home Assistant core exposes no ``ATTR_ENTRY_ID`` constant, so the module
    defines it locally; this binds the local value to the documented schema.
    """

    from pathlib import Path

    import yaml

    services_yaml = (
        Path(services.__file__).with_name("services.yaml").read_text(encoding="utf-8")
    )
    schema = yaml.safe_load(services_yaml)

    assert services.ATTR_ENTRY_ID == "entry_id"
    assert services.ATTR_ENTRY_ID in schema[SERVICE_REBUILD_REGISTRY]["fields"]


def test_docs_describe_what_the_rebuild_mode_does() -> None:
    """F2/F6: README and AGENTS.md describe the purge of mode ``rebuild``.

    The handler purges entities and devices between unload and setup through
    ``purge_entry_registrations`` and leaves a disabled entry unloaded
    (``test_rebuild_registry_purges_but_does_not_set_up_disabled_entry``).
    Both documents used to describe a plain reload.
    """
    import inspect
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    handler = inspect.getsource(services)
    assert '"purge_entry_registrations"' in handler, "doc premise no longer holds"

    readme_rows = [
        line
        for line in (root / "README.md").read_text(encoding="utf-8").splitlines()
        if line.startswith("| googlefindmy.rebuild_registry |")
    ]
    assert len(readme_rows) == 1
    # R2-8: the purge covers what is registered to the entry's subentries.
    assert (
        "removes the entities and devices registered to its subentries"
        in (readme_rows[0])
    )
    assert "removes all entities" not in readme_rows[0]
    assert "set up again even when that removal fails" in readme_rows[0]
    assert "stays disabled" in readme_rows[0]

    agents = (root / "AGENTS.md").read_text(encoding="utf-8")
    bullet = next(
        line
        for line in agents.splitlines()
        if line.startswith("* **Config entry reload states:**")
    )
    assert "purge_entry_registrations" in bullet
    assert "disabled entry is purged but not set up again" in bullet
