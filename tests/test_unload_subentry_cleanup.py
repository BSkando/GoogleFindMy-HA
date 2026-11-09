# tests/test_unload_subentry_cleanup.py
"""Tests verifying unload removes subentries and registry assignments."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Sequence
from types import MappingProxyType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.helpers import entity_registry as er

import custom_components.googlefindmy as integration
from custom_components.googlefindmy.const import DOMAIN, SUBENTRY_TYPE_TRACKER
from tests.helpers.config_flow import ConfigEntriesDomainUniqueIdLookupMixin
from tests.helpers.single_owner_device_registry import SingleOwnerDeviceRegistry

pytestmark = pytest.mark.asyncio


def _platform_names(platforms: tuple[object, ...]) -> tuple[str, ...]:
    """Return normalized platform names for recorded calls."""

    names: list[str] = []
    for platform in platforms:
        if isinstance(platform, str):
            names.append(platform)
        else:
            value = getattr(platform, "value", None)
            if isinstance(value, str):
                names.append(value)
            else:
                names.append(str(platform))
    return tuple(names)


class _RegistryTracker:
    """Track registry cleanup operations."""

    def __init__(self) -> None:
        self.by_subentry: dict[str, tuple[str, ...]] = {}
        self.removals: list[str] = []

    def apply(self, subentry_id: str, device_ids: tuple[str, ...]) -> None:
        self.by_subentry[subentry_id] = device_ids

    def remove_for_subentry(self, subentry_id: str) -> None:
        self.by_subentry.pop(subentry_id, None)
        self.removals.append(subentry_id)


class _SubentryManagerStub:
    """Stub for ConfigEntrySubEntryManager capturing cleanup calls."""

    def __init__(
        self,
        entry: _EntryStub,
        entity_registry: _RegistryTracker,
        device_registry: _RegistryTracker,
    ) -> None:
        self._entry = entry
        self.entity_registry = entity_registry
        self.device_registry = device_registry
        self.removed: list[str] = []

    async def async_remove_all(self) -> None:
        for subentry_id in list(self._entry.subentries):
            self._entry.subentries.pop(subentry_id, None)
            self.entity_registry.remove_for_subentry(subentry_id)
            self.device_registry.remove_for_subentry(subentry_id)
            self.removed.append(subentry_id)


class _AsyncLock:
    """Minimal async lock stub used by the unload test."""

    async def __aenter__(self) -> _AsyncLock:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _ConfigEntriesHelper(ConfigEntriesDomainUniqueIdLookupMixin):
    """Subset of hass.config_entries used during unload."""

    def __init__(self, entry: _EntryStub) -> None:
        self._entry = entry
        self.removed_subentries: list[str] = []
        self.unloaded_subentries: list[str] = []
        self.setup_calls: list[str] = []
        self.forward_unload_calls: list[tuple[_EntryStub, tuple[object, ...]]] = []
        self.unload_platform_calls: list[tuple[_EntryStub, tuple[object, ...]]] = []
        self.forward_setup_calls: list[tuple[_EntryStub, tuple[object, ...]]] = []
        self.parent_unload_invocations = 0
        self._subentries_removed = False

    def async_entries(self, domain: str | None = None) -> list[_EntryStub]:
        if domain is not None and domain != DOMAIN:
            return []
        return [self._entry]

    async def async_unload(self, entry_id: str) -> bool:
        self.unloaded_subentries.append(entry_id)
        return True

    async def async_unload_platforms(
        self, entry: _EntryStub, platforms: Sequence[object]
    ) -> bool:
        assert entry is self._entry
        self.parent_unload_invocations += 1
        self.unload_platform_calls.append((entry, tuple(platforms)))
        return True

    def async_forward_entry_unload(
        self,
        entry: _EntryStub,
        platforms: object,
    ) -> bool | Awaitable[bool]:
        assert entry is self._entry
        if not self._subentries_removed:
            runtime = getattr(self._entry, "runtime_data", None)
            manager = getattr(runtime, "subentry_manager", None)
            if manager is not None:
                removal_result = manager.async_remove_all()
                if inspect.isawaitable(removal_result):
                    self._subentries_removed = True
                    return removal_result
            self._subentries_removed = True
        return True

    async def async_forward_entry_setups(
        self,
        entry: _EntryStub,
        platforms: Sequence[object],
    ) -> None:
        assert entry is self._entry
        self.forward_setup_calls.append((entry, tuple(platforms)))

    def async_remove_subentry(self, entry: _EntryStub, subentry_id: str) -> bool:  # noqa: FBT001
        assert entry is self._entry
        self.removed_subentries.append(subentry_id)
        return True

    def async_get_entry(self, entry_id: str) -> _EntryStub | None:
        if entry_id == self._entry.entry_id:
            return self._entry
        return None

    def async_get_subentries(self, entry_id: str) -> list[ConfigSubentry]:
        entry = self.async_get_entry(entry_id)
        if entry is None:
            return []
        return list(entry.subentries.values())

    async def async_setup(self, entry_id: str) -> bool:
        self.setup_calls.append(entry_id)
        return True


class _HassStub:
    """Minimal Home Assistant stub for async_unload_entry."""

    def __init__(
        self,
        entry: _EntryStub,
        runtime_data: integration.RuntimeData,
        entity_registry: _RegistryTracker,
        device_registry: _RegistryTracker,
    ) -> None:
        self.config_entries = _ConfigEntriesHelper(entry)
        self.data: dict[str, Any] = {
            DOMAIN: {
                "entries": {entry.entry_id: runtime_data},
                "fcm_lock": _AsyncLock(),
                "fcm_refcount": 1,
                "fcm_receiver": SimpleNamespace(async_stop=lambda: asyncio.sleep(0)),
            }
        }

    async def async_add_executor_job(self, func: Any, *args: Any) -> Any:
        return func(*args)


class _TokenCacheStub:
    """Token cache stub capturing close operations."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _CoordinatorStub:
    """Coordinator stub exposing async_shutdown hook."""

    def __init__(self) -> None:
        self.shutdown_called = False

    async def async_shutdown(self) -> None:
        self.shutdown_called = True


class _EntryStub:
    """Config entry stub for unload tests."""

    def __init__(self) -> None:
        self.entry_id = "entry-unload"
        self.data: dict[str, Any] = {}
        self.options: dict[str, Any] = {}
        self.title = "Find My"
        self.subentries: dict[str, ConfigSubentry] = {}
        self.runtime_data: integration.RuntimeData | None = None

    def add_subentry(self, key: str, device_ids: tuple[str, ...]) -> ConfigSubentry:
        subentry = ConfigSubentry(
            data=MappingProxyType({"group_key": key, "visible_device_ids": device_ids}),
            subentry_type=SUBENTRY_TYPE_TRACKER,
            title=key.title(),
            unique_id=f"{self.entry_id}-{key}",
        )
        self.subentries[subentry.subentry_id] = subentry
        return subentry


class _SubentryConfigEntriesHelper:
    """Config entries helper tracking subentry unload requests."""

    def __init__(self) -> None:
        self.unload_platform_calls: list[tuple[Any, tuple[object, ...]]] = []
        self.forward_unload_calls: list[tuple[Any, tuple[object, ...]]] = []

    async def async_unload_platforms(self, entry: Any, platforms: list[object]) -> bool:  # noqa: FBT001 - Home Assistant signature
        self.unload_platform_calls.append((entry, tuple(platforms)))
        return True

    async def async_forward_entry_unload(
        self,
        entry: Any,
        platforms: object,
    ) -> bool:  # noqa: FBT001 - Home Assistant signature
        if isinstance(platforms, (list, tuple, set)):
            payload = tuple(platforms)
        else:
            payload = (platforms,)
        self.forward_unload_calls.append((entry, payload))
        return True


async def test_async_purge_unloaded_subentry_registrations_removes_registries() -> None:
    """Purge helper should drop orphaned registry entries for missing platforms."""

    hass = SimpleNamespace()
    ent_reg = er.async_get(hass)
    # The shared single-owner double, per ``tests/AGENTS.md``: this path gives up
    # an ownership link, and on a single-owner core that *is* the deletion. Only
    # a double carrying the Core 2026.8 rules can show that; the generic stub
    # would answer "link dropped" and never remove the device.
    #
    # This case is green on the previous state too -- there the core-side
    # deletion happened inside ``async_update_device``. It is a regression
    # guard for the outcome, and the two tests below it are the ones that pin
    # the planner path itself.
    dev_reg = SingleOwnerDeviceRegistry()
    hass._device_registry_stub = dev_reg  # noqa: SLF001 - conftest lookup key

    parent_entry_id = "parent-entry"
    config_subentry_id = "tracker-subentry"

    ent_reg.record_entity(
        "device_tracker.tracker_one",
        platform=DOMAIN,
        unique_id="entity-1",
        config_entry_id=parent_entry_id,
        config_entry_subentry_id=config_subentry_id,
    )
    ent_reg.record_entity(
        "device_tracker.tracker_two",
        platform=DOMAIN,
        unique_id="entity-2",
        config_entry_id=parent_entry_id,
        config_entry_subentry_id="other-subentry",
    )

    dev_reg.add_config_entry(parent_entry_id, {config_subentry_id, "other-subentry"})
    purged_device = dev_reg.add_device(
        identifiers={(DOMAIN, "device-to-purge")},
        config_entry_id=parent_entry_id,
        config_subentry_id=config_subentry_id,
        name="Tracker",
    )
    dev_reg.add_device(
        identifiers={(DOMAIN, "device-keep")},
        config_entry_id=parent_entry_id,
        config_subentry_id="other-subentry",
        name="Other Tracker",
    )

    (
        removed_entities,
        removed_devices,
    ) = await integration._async_purge_unloaded_subentry_registrations(  # type: ignore[arg-type]
        hass,
        parent_entry_id=parent_entry_id,
        config_subentry_id=config_subentry_id,
        entry_type=SUBENTRY_TYPE_TRACKER,
    )

    assert removed_entities == 1
    assert removed_devices == 1
    assert ent_reg.async_get("device_tracker.tracker_one") is None
    assert ent_reg.async_get("device_tracker.tracker_two") is not None
    # The resulting ownership is the assertion, not the keywords the planner
    # picked (``tests/AGENTS.md``, registry stub checklist item 5). The device
    # that gave up its only ownership link is gone; the one in the other
    # subentry is untouched.
    assert dev_reg.async_get(purged_device.id) is None
    remaining_devices = list(dev_reg.devices.values())
    assert len(remaining_devices) == 1
    assert remaining_devices[0].config_subentry_id == "other-subentry"
    assert remaining_devices[0].config_entry_id == parent_entry_id


async def test_async_unload_subentry_purges_never_loaded_platforms() -> None:
    """Subentry unload should purge registry links when platforms never load."""

    hass = SimpleNamespace()
    ent_reg = er.async_get(hass)
    # Shared single-owner double, see the purge test above.
    dev_reg = SingleOwnerDeviceRegistry()
    hass._device_registry_stub = dev_reg  # noqa: SLF001 - conftest lookup key

    parent_entry_id = "parent-entry"
    config_subentry_id = "tracker-subentry"

    ent_reg.record_entity(
        "device_tracker.tracker_one",
        platform=DOMAIN,
        unique_id="entity-1",
        config_entry_id=parent_entry_id,
        config_entry_subentry_id=config_subentry_id,
    )
    dev_reg.add_config_entry(parent_entry_id, {config_subentry_id})
    device_entry = dev_reg.add_device(
        identifiers={(DOMAIN, "device-to-purge")},
        config_entry_id=parent_entry_id,
        config_subentry_id=config_subentry_id,
        name="Tracker",
    )

    class _ConfigEntriesStub:
        def __init__(self) -> None:
            self.unload_calls: list[tuple[Any, tuple[object, ...]]] = []

        async def async_unload_platforms(
            self, entry: Any, platforms: tuple[object, ...]
        ) -> bool:  # noqa: FBT001
            self.unload_calls.append((entry, platforms))
            raise ValueError("never loaded")

    entry_runtime = SimpleNamespace()
    subentry = SimpleNamespace(
        entry_id="child-entry",
        data={"group_key": "tracker", "subentry_type": SUBENTRY_TYPE_TRACKER},
        config_subentry_id=config_subentry_id,
        subentry_id=config_subentry_id,
        runtime_data=entry_runtime,
        parent_entry_id=parent_entry_id,
    )

    hass.config_entries = _ConfigEntriesStub()

    result = await integration._async_unload_subentry(hass, subentry)  # type: ignore[arg-type]

    assert result is True
    assert subentry.runtime_data is None
    assert ent_reg.async_get("device_tracker.tracker_one") is None
    assert dev_reg.async_get(device_entry.id) is None
    assert hass.config_entries.unload_calls == [(subentry, integration.PLATFORMS)]


async def test_async_unload_subentry_clears_runtime_data_and_preserves_parent_cache() -> (
    None
):
    """Subentry unload should clear runtime data without touching the parent cache."""

    runtime_data = integration.RuntimeData(
        coordinator=SimpleNamespace(),
        token_cache=SimpleNamespace(),
        subentry_manager=SimpleNamespace(),
        fcm_receiver=None,
    )

    parent_entry_id = "parent-entry"
    hass = SimpleNamespace(
        config_entries=_SubentryConfigEntriesHelper(),
        data={DOMAIN: {"entries": {parent_entry_id: runtime_data}}},
    )

    child_entry = SimpleNamespace(
        entry_id="child-entry",
        data={"group_key": "tracker"},
        runtime_data=runtime_data,
        parent_entry_id=parent_entry_id,
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    result = await integration._async_unload_subentry(  # type: ignore[arg-type]
        hass, child_entry
    )

    assert result is True
    assert child_entry.runtime_data is None
    entries_bucket = hass.data[DOMAIN]["entries"]
    assert entries_bucket == {parent_entry_id: runtime_data}
    assert hass.config_entries.unload_platform_calls
    recorded_entry, recorded_platforms = hass.config_entries.unload_platform_calls[0]
    assert recorded_entry == child_entry
    assert _platform_names(recorded_platforms) == tuple(
        platform.value if hasattr(platform, "value") else str(platform)
        for platform in integration.PLATFORMS
    )


async def test_async_unload_entry_removes_subentries_and_registries(
    monkeypatch: Any,
) -> None:
    """Unload should drop subentries and clear registry assignments."""

    entry = _EntryStub()
    first = entry.add_subentry("core", ("dev-1", "dev-2"))
    second = entry.add_subentry("extra", ("dev-3",))

    entity_registry = _RegistryTracker()
    device_registry = _RegistryTracker()
    entity_registry.apply(first.subentry_id, first.data["visible_device_ids"])
    entity_registry.apply(second.subentry_id, second.data["visible_device_ids"])
    device_registry.apply(first.subentry_id, first.data["visible_device_ids"])
    device_registry.apply(second.subentry_id, second.data["visible_device_ids"])

    token_cache = _TokenCacheStub()
    coordinator = _CoordinatorStub()
    subentry_manager = _SubentryManagerStub(entry, entity_registry, device_registry)
    runtime_data = integration.RuntimeData(
        coordinator=coordinator,
        token_cache=token_cache,
        subentry_manager=subentry_manager,
        fcm_receiver=None,
    )
    entry.runtime_data = runtime_data
    entry._gfm_parent_platforms_forwarded = True

    hass = _HassStub(entry, runtime_data, entity_registry, device_registry)

    async def _fake_release_fcm(hass_obj: Any, entry_obj: Any | None = None) -> None:
        hass_obj.data[DOMAIN]["fcm_refcount"] = 0
        assert entry_obj is entry

    monkeypatch.setattr(integration, "_async_release_shared_fcm", _fake_release_fcm)
    monkeypatch.setattr(integration, "_unregister_instance", lambda _entry_id: None)
    monkeypatch.setattr(integration, "loc_unregister_fcm_provider", lambda: None)
    monkeypatch.setattr(integration, "api_unregister_fcm_provider", lambda: None)

    result = await integration.async_unload_entry(hass, entry)

    assert result is True
    assert coordinator.shutdown_called is True
    assert token_cache.closed is True
    assert subentry_manager.removed == [first.subentry_id, second.subentry_id]
    assert entity_registry.removals == [first.subentry_id, second.subentry_id]
    assert device_registry.removals == [first.subentry_id, second.subentry_id]
    assert not entry.subentries
    calls = hass.config_entries.forward_unload_calls
    assert calls == []
    assert hass.config_entries.parent_unload_invocations == 1
    assert hass.config_entries.unload_platform_calls == [
        (entry, tuple(integration.PLATFORMS))
    ]
    assert hass.config_entries.removed_subentries == []


async def test_async_unload_entry_defaults_parent_forward_flag(
    monkeypatch: Any,
) -> None:
    """Unload should assume parent platforms forwarded when flag is missing."""

    entry = _EntryStub()
    subentry = entry.add_subentry("core", ("dev-1",))

    entity_registry = _RegistryTracker()
    device_registry = _RegistryTracker()
    entity_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])
    device_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])

    token_cache = _TokenCacheStub()
    coordinator = _CoordinatorStub()
    subentry_manager = _SubentryManagerStub(entry, entity_registry, device_registry)
    runtime_data = integration.RuntimeData(
        coordinator=coordinator,
        token_cache=token_cache,
        subentry_manager=subentry_manager,
        fcm_receiver=None,
    )
    entry.runtime_data = runtime_data

    hass = _HassStub(entry, runtime_data, entity_registry, device_registry)

    async def _fake_release_fcm(hass_obj: Any, entry_obj: Any | None = None) -> None:
        hass_obj.data[DOMAIN]["fcm_refcount"] = 0
        assert entry_obj is entry

    monkeypatch.setattr(integration, "_async_release_shared_fcm", _fake_release_fcm)
    monkeypatch.setattr(integration, "_unregister_instance", lambda _entry_id: None)
    monkeypatch.setattr(integration, "loc_unregister_fcm_provider", lambda: None)
    monkeypatch.setattr(integration, "api_unregister_fcm_provider", lambda: None)

    result = await integration.async_unload_entry(hass, entry)

    assert result is True
    assert hass.config_entries.parent_unload_invocations == 1
    assert hass.config_entries.unload_platform_calls == [
        (entry, tuple(integration.PLATFORMS))
    ]
    assert hass.config_entries.removed_subentries == []


async def test_async_unload_entry_preserves_registry_and_user_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reload/unload must not drop registry entries or user-defined labels."""

    class _DeviceStub:
        def __init__(
            self,
            *,
            device_id: str,
            config_entry_id: str,
            identifiers: set[tuple[str, str]],
            manufacturer: str,
            model: str,
            name: str,
            config_subentry_id: str,
        ) -> None:
            self.id = device_id
            self.config_entry_id = config_entry_id
            self.identifiers = identifiers
            self.manufacturer = manufacturer
            self.model = model
            self.name = name
            self.config_subentry_id = config_subentry_id
            self.name_by_user: str | None = None

    class _DeviceRegistryStub:
        def __init__(self) -> None:
            self.devices: dict[str, _DeviceStub] = {}

        def async_get_or_create(
            self,
            *,
            config_entry_id: str,
            identifiers: set[tuple[str, str]],
            manufacturer: str,
            model: str,
            name: str,
            config_subentry_id: str,
        ) -> _DeviceStub:
            device_id = f"device-{len(self.devices)}"
            device = _DeviceStub(
                device_id=device_id,
                config_entry_id=config_entry_id,
                identifiers=identifiers,
                manufacturer=manufacturer,
                model=model,
                name=name,
                config_subentry_id=config_subentry_id,
            )
            self.devices[device_id] = device
            return device

        def async_get(self, device_id: str) -> _DeviceStub | None:
            return self.devices.get(device_id)

    entry = _EntryStub()
    subentry = entry.add_subentry("core", ("dev-1",))
    entry._gfm_parent_platforms_forwarded = True

    coordinator = AsyncMock()
    coordinator.async_shutdown = AsyncMock()
    token_cache = AsyncMock()
    token_cache.close = AsyncMock()
    subentry_manager = AsyncMock()

    runtime_data = integration.RuntimeData(
        coordinator=coordinator,
        token_cache=token_cache,
        subentry_manager=subentry_manager,
        fcm_receiver=None,
    )
    entry.runtime_data = runtime_data

    hass = _HassStub(entry, runtime_data, _RegistryTracker(), _RegistryTracker())
    hass.config_entries.async_forward_entry_unload = AsyncMock(return_value=True)

    # Record the purge instead of replacing it with a no-op: a reload of a
    # loaded entry must not call it at all (the registry stubs here would not
    # show a removal, so the call itself is the observable).
    purge_calls: list[dict[str, object]] = []

    async def _recording_purge(*_: object, **kwargs: object) -> tuple[int, int]:
        purge_calls.append(dict(kwargs))
        return (0, 0)

    monkeypatch.setattr(
        integration,
        "_async_purge_unloaded_subentry_registrations",
        _recording_purge,
    )

    device_registry = _DeviceRegistryStub()
    with patch(
        "homeassistant.helpers.device_registry.async_get", return_value=device_registry
    ):
        device = device_registry.async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={(DOMAIN, "device-to-keep")},
            manufacturer="Google",
            model="Find My Device",
            name="API Name",
            config_subentry_id=subentry.subentry_id,
        )
        device.name_by_user = "Custom Label"

        result = await integration.async_unload_entry(hass, entry)

    assert result is True
    assert entry.subentries == {subentry.subentry_id: subentry}
    subentry_manager.async_remove_all.assert_not_called()
    coordinator.async_shutdown.assert_awaited_once()
    token_cache.close.assert_awaited_once()

    refreshed = device_registry.async_get(device.id)
    assert refreshed is not None
    assert refreshed.name_by_user == "Custom Label"
    assert refreshed.name == "API Name"
    assert purge_calls == []


async def test_async_unload_entry_passes_entry_to_fcm_release(
    monkeypatch: Any,
) -> None:
    """Unload should release FCM with the active entry context."""

    entry = _EntryStub()
    subentry = entry.add_subentry("core", ("dev-1",))

    entity_registry = _RegistryTracker()
    device_registry = _RegistryTracker()
    entity_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])
    device_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])

    token_cache = _TokenCacheStub()
    coordinator = _CoordinatorStub()
    subentry_manager = _SubentryManagerStub(entry, entity_registry, device_registry)
    runtime_data = integration.RuntimeData(
        coordinator=coordinator,
        token_cache=token_cache,
        subentry_manager=subentry_manager,
        fcm_receiver=None,
    )
    entry.runtime_data = runtime_data
    entry._gfm_parent_platforms_forwarded = True

    hass = _HassStub(entry, runtime_data, entity_registry, device_registry)

    release_calls: list[tuple[Any, Any | None]] = []

    async def _fake_release_fcm(hass_obj: Any, entry_obj: Any | None = None) -> None:
        hass_obj.data[DOMAIN]["fcm_refcount"] = 0
        release_calls.append((hass_obj, entry_obj))

    monkeypatch.setattr(integration, "_async_release_shared_fcm", _fake_release_fcm)
    monkeypatch.setattr(integration, "_unregister_instance", lambda _entry_id: None)
    monkeypatch.setattr(integration, "loc_unregister_fcm_provider", lambda: None)
    monkeypatch.setattr(integration, "api_unregister_fcm_provider", lambda: None)

    result = await integration.async_unload_entry(hass, entry)

    assert result is True
    assert release_calls == [(hass, entry)]


async def test_async_unload_entry_handles_legacy_forward_signature(
    monkeypatch: Any,
) -> None:
    """Unload should skip fallback when Home Assistant lacks subentry keyword support."""

    entry = _EntryStub()
    subentry = entry.add_subentry("legacy", ("dev-legacy",))

    entity_registry = _RegistryTracker()
    device_registry = _RegistryTracker()
    entity_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])
    device_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])

    token_cache = _TokenCacheStub()
    coordinator = _CoordinatorStub()
    subentry_manager = _SubentryManagerStub(entry, entity_registry, device_registry)
    runtime_data = integration.RuntimeData(
        coordinator=coordinator,
        token_cache=token_cache,
        subentry_manager=subentry_manager,
        fcm_receiver=None,
    )
    entry.runtime_data = runtime_data
    entry._gfm_parent_platforms_forwarded = True

    hass = _HassStub(entry, runtime_data, entity_registry, device_registry)

    legacy_calls: list[tuple[_EntryStub, tuple[object, ...]]] = []
    purge_calls: list[dict[str, object]] = []

    def legacy_forward(entry_obj: _EntryStub, platforms: object) -> bool:
        if isinstance(platforms, (list, tuple, set)):
            payload = tuple(platforms)
        else:
            payload = (platforms,)
        legacy_calls.append((entry_obj, payload))
        return True

    hass.config_entries.async_forward_entry_unload = legacy_forward  # type: ignore[attr-defined]

    async def _record_purge(*args: object, **kwargs: object) -> tuple[int, int]:
        purge_calls.append({"args": args, "kwargs": kwargs})
        return (0, 0)

    async def _fake_release_fcm(hass_obj: Any, entry_obj: Any | None = None) -> None:
        hass_obj.data[DOMAIN]["fcm_refcount"] = 0
        assert entry_obj is entry

    monkeypatch.setattr(integration, "_async_release_shared_fcm", _fake_release_fcm)
    monkeypatch.setattr(integration, "_unregister_instance", lambda _entry_id: None)
    monkeypatch.setattr(integration, "loc_unregister_fcm_provider", lambda: None)
    monkeypatch.setattr(integration, "api_unregister_fcm_provider", lambda: None)
    monkeypatch.setattr(
        integration,
        "_async_purge_unloaded_subentry_registrations",
        _record_purge,
    )

    result = await integration.async_unload_entry(hass, entry)

    assert result is True
    assert legacy_calls == [
        (
            entry,
            (platform.value if hasattr(platform, "value") else str(platform),),
        )
        for platform in integration.PLATFORMS
    ]
    assert hass.config_entries.parent_unload_invocations in (0, 1)
    assert hass.config_entries.unload_platform_calls == [
        (entry, tuple(integration.PLATFORMS))
    ]
    assert purge_calls == []


async def test_async_unload_entry_rolls_back_when_parent_unload_fails(
    monkeypatch: Any,
) -> None:
    """Parent platform unload failure should keep subentries online."""

    entry = _EntryStub()
    subentry = entry.add_subentry("core", ("dev-1", "dev-2"))

    entity_registry = _RegistryTracker()
    device_registry = _RegistryTracker()
    entity_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])
    device_registry.apply(subentry.subentry_id, subentry.data["visible_device_ids"])

    token_cache = _TokenCacheStub()
    coordinator = _CoordinatorStub()
    subentry_manager = _SubentryManagerStub(entry, entity_registry, device_registry)
    runtime_data = integration.RuntimeData(
        coordinator=coordinator,
        token_cache=token_cache,
        subentry_manager=subentry_manager,
        fcm_receiver=None,
    )
    entry.runtime_data = runtime_data
    entry._gfm_parent_platforms_forwarded = True

    hass = _HassStub(entry, runtime_data, entity_registry, device_registry)

    async def _fail_parent_unload(
        entry_obj: _EntryStub, platforms: Sequence[object]
    ) -> bool:
        hass.config_entries.parent_unload_invocations += 1
        hass.config_entries.unload_platform_calls.append((entry_obj, tuple(platforms)))
        return False

    hass.config_entries.async_unload_platforms = _fail_parent_unload  # type: ignore[assignment]

    async def _fake_release_fcm(hass_obj: Any, entry_obj: Any | None = None) -> None:
        hass_obj.data[DOMAIN]["fcm_refcount"] = 0
        assert entry_obj is entry

    monkeypatch.setattr(integration, "_async_release_shared_fcm", _fake_release_fcm)
    monkeypatch.setattr(integration, "_unregister_instance", lambda _entry_id: None)
    monkeypatch.setattr(integration, "loc_unregister_fcm_provider", lambda: None)
    monkeypatch.setattr(integration, "api_unregister_fcm_provider", lambda: None)

    result = await integration.async_unload_entry(hass, entry)

    assert result is False or hass.config_entries.parent_unload_invocations == 0
    # Subentries and registries should remain untouched because the unload aborted.
    assert entry.subentries == {subentry.subentry_id: subentry}
    assert hass.config_entries.forward_unload_calls == []
    assert hass.config_entries.removed_subentries == []
    assert hass.config_entries.parent_unload_invocations == 1
    assert hass.config_entries.unload_platform_calls == [
        (entry, tuple(integration.PLATFORMS))
    ]
    # Parent unload failures must not trigger manual subentry forwarding; Home
    # Assistant will re-run subentry setup as needed.
    assert hass.config_entries.forward_setup_calls == []
    # Runtime data is reattached to the bucket so the entry keeps running.
    assert hass.data[DOMAIN]["entries"][entry.entry_id] is runtime_data
    assert entry.runtime_data is runtime_data
    # Coordinator and token cache must not shut down on abort.
    assert coordinator.shutdown_called is False
    assert token_cache.closed is False


async def test_purge_refuses_a_device_whose_ownership_it_cannot_read() -> None:
    """A planner refusal ends this device, not the purge and not by guessing.

    ``plan_device_ownership`` raises when it would have to guess at an ownership
    state it cannot read. On a single-owner core a guess here means deleting a
    device, so the purge logs and moves on to the next device rather than
    forcing the call through.
    """

    hass = SimpleNamespace()
    er.async_get(hass)
    dev_reg = SingleOwnerDeviceRegistry()
    hass._device_registry_stub = dev_reg  # noqa: SLF001 - conftest lookup key

    parent_entry_id = "parent-entry"
    config_subentry_id = "tracker-subentry"
    dev_reg.add_config_entry(parent_entry_id, {config_subentry_id})
    device = dev_reg.add_device(
        identifiers={(DOMAIN, "device-to-purge")},
        config_entry_id=parent_entry_id,
        config_subentry_id=config_subentry_id,
        name="Tracker",
    )

    def _refuse(*_args: Any, **_kwargs: Any) -> Any:
        raise ValueError("ownership unknown")

    with patch.object(
        integration.coordinator.helpers.registry,
        "plan_device_ownership",
        _refuse,
    ):
        (
            removed_entities,
            removed_devices,
        ) = await integration._async_purge_unloaded_subentry_registrations(  # type: ignore[arg-type]
            hass,
            parent_entry_id=parent_entry_id,
            config_subentry_id=config_subentry_id,
            entry_type=SUBENTRY_TYPE_TRACKER,
        )

    assert (removed_entities, removed_devices) == (0, 0)
    assert dev_reg.async_get(device.id) is not None


async def test_purge_counts_nothing_when_the_plan_is_empty() -> None:
    """An empty plan is "nothing to do", and must not be counted as a removal.

    The planner answers with an empty plan when the link the caller wants to give
    up is not the link the device sits on. Counting that as a removed device
    would put a number into the log that no registry operation backs.
    """

    hass = SimpleNamespace()
    er.async_get(hass)
    dev_reg = SingleOwnerDeviceRegistry()
    hass._device_registry_stub = dev_reg  # noqa: SLF001 - conftest lookup key

    parent_entry_id = "parent-entry"
    config_subentry_id = "tracker-subentry"
    dev_reg.add_config_entry(parent_entry_id, {config_subentry_id})
    device = dev_reg.add_device(
        identifiers={(DOMAIN, "device-to-purge")},
        config_entry_id=parent_entry_id,
        config_subentry_id=config_subentry_id,
        name="Tracker",
    )

    with patch.object(
        integration.coordinator.helpers.registry,
        "plan_device_ownership",
        lambda *_args, **_kwargs: (),
    ):
        (
            removed_entities,
            removed_devices,
        ) = await integration._async_purge_unloaded_subentry_registrations(  # type: ignore[arg-type]
            hass,
            parent_entry_id=parent_entry_id,
            config_subentry_id=config_subentry_id,
            entry_type=SUBENTRY_TYPE_TRACKER,
        )

    assert (removed_entities, removed_devices) == (0, 0)
    assert dev_reg.async_get(device.id) is not None


class _LegacyPurgeRegistry:
    """A pre-2026.8 device registry, as Core ``2025.9.1`` presents it.

    Two properties decide this test and neither is in the shared single-owner
    double, which models the *newer* core:

    * The signature carries ``add_config_subentry_id`` and no ``new_*`` pair, so
      ``detect_device_registry_capabilities`` reads ``single_owner_model=False``
      and the planner takes its legacy branch (`registry.py`, the DETACH branch).
    * ``async_update_device`` with the removal pair drops the named subentry link
      and deletes the device **only** when this was its last owning entry, which
      is Core's rule at tag ``2025.9.1``, lines 1174-1177. A device another entry
      still owns survives.

    Device entries carry ``config_entries`` and ``config_entries_subentries``,
    the ownership fields of that core; they carry neither ``config_entry_id`` nor
    ``config_subentry_id``, which that core does not have.
    """

    def __init__(self) -> None:
        self.devices: dict[str, SimpleNamespace] = {}
        self.removed: list[str] = []
        self._next = 0

    def add_device(
        self, *, owners: dict[str, set[str | None]], identifiers: set[Any]
    ) -> SimpleNamespace:
        """Register a device owned by ``owners`` (entry id -> subentry links)."""
        self._next += 1
        device = SimpleNamespace(
            id=f"legacy-device-{self._next}",
            identifiers=set(identifiers),
            config_entries=set(owners),
            config_entries_subentries={k: set(v) for k, v in owners.items()},
            name=None,
        )
        self.devices[device.id] = device
        return device

    def async_get(self, device_id: str) -> SimpleNamespace | None:
        return self.devices.get(device_id)

    def async_entries_for_config_entry(self, entry_id: str) -> list[SimpleNamespace]:
        return [d for d in self.devices.values() if entry_id in d.config_entries]

    def async_remove_device(self, device_id: str) -> None:
        self.devices.pop(device_id, None)
        self.removed.append(device_id)

    def async_update_device(
        self,
        *,
        device_id: str,
        add_config_entry_id: Any = None,
        add_config_subentry_id: Any = None,
        remove_config_entry_id: Any = None,
        remove_config_subentry_id: Any = None,
        **_extra: Any,
    ) -> SimpleNamespace | None:
        device = self.devices[device_id]
        if remove_config_entry_id is None:
            return device
        links = device.config_entries_subentries.get(remove_config_entry_id, set())
        links.discard(remove_config_subentry_id)
        if not links:
            device.config_entries_subentries.pop(remove_config_entry_id, None)
            device.config_entries.discard(remove_config_entry_id)
        if not device.config_entries:
            self.async_remove_device(device_id)
            return None
        return device


async def test_purge_on_a_legacy_core_keeps_a_device_another_entry_owns() -> None:
    """On Core 2025.9.1 the purge drops a link; Core decides about deletion.

    This is the branch the declared minimum runs, and the one the production
    comment calls load-bearing: no direct ``async_remove_device`` is written for
    it, because a device that a second config entry still owns must survive
    losing ours. The shared single-owner double cannot show this -- there a
    device has one owner by construction.
    """

    hass = SimpleNamespace()
    er.async_get(hass)
    dev_reg = _LegacyPurgeRegistry()
    hass._device_registry_stub = dev_reg  # noqa: SLF001 - conftest lookup key

    parent = "parent-entry"
    subentry = "tracker-subentry"

    shared = dev_reg.add_device(
        owners={parent: {subentry}, "other-entry": {None}},
        identifiers={(DOMAIN, "shared-device")},
    )
    only_ours = dev_reg.add_device(
        owners={parent: {subentry}},
        identifiers={(DOMAIN, "our-device")},
    )

    _, removed_devices = await integration._async_purge_unloaded_subentry_registrations(  # type: ignore[arg-type]
        hass,
        parent_entry_id=parent,
        config_subentry_id=subentry,
        entry_type=SUBENTRY_TYPE_TRACKER,
    )

    assert removed_devices == 2
    # The shared device lost our link and survives on the other entry.
    surviving = dev_reg.async_get(shared.id)
    assert surviving is not None
    assert surviving.config_entries == {"other-entry"}
    # The device only we owned is gone, and Core removed it, not us.
    assert dev_reg.async_get(only_ours.id) is None
    assert dev_reg.removed == [only_ours.id]
