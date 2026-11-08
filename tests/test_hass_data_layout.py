# tests/test_hass_data_layout.py
"""Regression tests for the hass.data layout used by the integration."""

from __future__ import annotations

import asyncio
import functools
import importlib
import inspect
import json
import logging
import sys
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType, ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
from pytest import importorskip

importorskip(
    "homeassistant",
    reason="homeassistant test stubs must be installed",
)
importorskip(
    "pytest_homeassistant_custom_component",
    reason="pytest-homeassistant-custom-component must be installed",
)

from homeassistant.config_entries import ConfigEntryState, ConfigSubentry
from homeassistant.const import Platform
from homeassistant.exceptions import ConfigEntryNotReady, ServiceValidationError

from custom_components.googlefindmy import _platform_value, config_flow
from custom_components.googlefindmy.const import (
    ATTR_MODE,
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    DATA_AAS_TOKEN,
    DATA_AUTH_METHOD,
    DATA_SECRET_BUNDLE,
    DOMAIN,
    MODE_MIGRATE,
    OPT_LOCATION_POLL_INTERVAL,
    OPT_MAP_VIEW_ENABLED,
    OPT_OPTIONS_SCHEMA_VERSION,
    SECRETS_EXTRA_WATCH_PATHS,
    SERVICE_FEATURE_PLATFORMS,
    SERVICE_LOCATE_DEVICE,
    SERVICE_REBUILD_REGISTRY,
    SERVICE_SUBENTRY_KEY,
    SERVICE_SUBENTRY_TRANSLATION_KEY,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_FEATURE_PLATFORMS,
    TRACKER_SUBENTRY_KEY,
    TRACKER_SUBENTRY_TRANSLATION_KEY,
)
from tests.conftest import import_coordinator_consumers
from tests.helpers import drain_loop
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.config_flow import ConfigEntriesDomainUniqueIdLookupMixin
from tests.helpers.homeassistant import (
    FakeDeviceEntry,
    FakeDeviceRegistry,
    FakeEntityRegistry,
    resolve_config_entry_lookup,
)
from tests.helpers.service_doubles import FakeServiceCall, FakeServiceRegistry

if TYPE_CHECKING:
    from custom_components.googlefindmy import RuntimeData


class _StubCache:
    """Lightweight token cache stub used for setup tests."""

    def __init__(self) -> None:
        self.values: dict[str, Any] = {}
        self.closed = False

    async def async_get_cached_value(self, key: str) -> Any:
        """Return the cached value for ``key`` if present."""

        return self.values.get(key)

    async def async_set_cached_value(self, key: str, value: Any) -> None:
        """Store ``value``, or remove ``key`` when it is ``None``.

        ``TokenCache.set`` treats ``None`` as a removal (``self._data.pop(name)``),
        so a stub that stored the ``None`` instead would let a test pass on a
        key that is still readable through ``all()`` -- exactly the kind of
        difference that lets a resurrected credential through unnoticed.
        """

        if value is None:
            self.values.pop(key, None)
            return
        self.values[key] = value

    async def all(self) -> dict[str, Any]:
        return dict(self.values)

    async def flush(self) -> None:  # pragma: no cover - compatibility hook
        return None

    async def close(self) -> None:
        self.closed = True


class _StubConfigEntry:
    """Minimal ConfigEntry-like stub capturing unload callbacks."""

    def __init__(self) -> None:
        self.entry_id: str = "entry-test"
        # ``ConfigEntry.unique_id`` is ``str | None``; Home Assistant copies the
        # flow's unique id onto the entry verbatim
        # (``ConfigEntry(unique_id=flow.unique_id)`` in
        # ``ConfigEntriesFlowManager.async_finish_flow``). The integration uses
        # a normalized Google address, so the stub mirrors its own entry data.
        self.unique_id: str | None = "user@example.com"
        self.data: dict[str, Any] = {
            DATA_SECRET_BUNDLE: {"username": "user@example.com"},
            CONF_GOOGLE_EMAIL: "user@example.com",
        }
        self.options: dict[str, Any] = {}
        self.title: str = "Test Entry"
        self.runtime_data: RuntimeData | None = None
        self.subentries: dict[str, ConfigSubentry] = {}
        self.state: ConfigEntryState = ConfigEntryState.LOADED
        self.disabled_by: str | None = None
        self._unload_callbacks: list[Callable[[], None]] = []
        self._update_listeners: list[Callable[..., Any]] = []
        self.updated_at = datetime(2024, 1, 1, 0, 0, 0)
        self.created_at = datetime(2024, 1, 1, 0, 0, 0)
        self._hass: _StubHass | None = None
        self._background_tasks: list[asyncio.Task[Any]] = []

    def async_on_unload(self, callback: Callable[[], None]) -> None:
        self._unload_callbacks.append(callback)

    def add_update_listener(self, listener: Callable[..., Any]) -> Callable[[], None]:
        """Register an options-update listener; return a no-op unsub.

        Mirrors ``homeassistant.config_entries.ConfigEntry.add_update_listener``
        so ``async_setup_entry``'s ``async_on_unload(add_update_listener(...))``
        watch-path refresh wiring works against the stub.
        """
        self._update_listeners.append(listener)
        return lambda: None

    def _attach_hass(self, hass: _StubHass) -> None:
        self._hass = hass

    def async_create_background_task(
        self,
        hass: _StubHass,
        target: Awaitable[Any],
        *,
        name: str | None = None,
        eager_start: bool = True,
    ) -> asyncio.Task[Any]:
        if self._hass is None:
            msg = "ConfigEntry is not attached to a hass instance"
            raise RuntimeError(msg)
        if hass is not self._hass:
            msg = "ConfigEntry is attached to a different hass instance"
            raise RuntimeError(msg)
        task = self._hass.async_create_task(target, name=name)
        self._background_tasks.append(task)
        return task


class _StubBus:
    """Event bus stub providing async_listen_once."""

    def async_listen_once(
        self, _event: str, _callback: Callable[..., Any]
    ) -> Callable[[], None]:
        return lambda: None


class _StubHttp:
    """Stub HTTP component capturing registered views."""

    def __init__(self) -> None:
        self.registered: list[Any] = []

    def register_view(self, view: Any) -> None:
        self.registered.append(view)


class _StubConfigEntries:
    """Align subentry lookup/registration with FakeConfigEntriesManager for retry coverage."""

    def __init__(self, entry: _StubConfigEntry) -> None:
        self._entries: list[_StubConfigEntry] = [entry]
        self.forward_calls: list[tuple[_StubConfigEntry, tuple[str, ...]]] = []
        self.reload_calls: list[str] = []
        self.added_subentries: list[tuple[_StubConfigEntry, ConfigSubentry]] = []
        self.updated_subentries: list[tuple[_StubConfigEntry, ConfigSubentry]] = []
        self.removed_subentries: list[tuple[_StubConfigEntry, str]] = []
        self.entry_update_calls: list[tuple[_StubConfigEntry, dict[str, Any]]] = []
        self.scheduled_reloads: list[str] = []
        self.unload_calls: list[str] = []
        self.set_disabled_by_calls: list[tuple[str, object | None]] = []
        self.setup_calls: list[str] = []
        self._registered_subentry_ids: set[str] = set()

    def _known_entry_ids(self) -> set[str]:
        known_ids = {entry.entry_id for entry in self._entries}
        known_ids.update(self._registered_subentry_ids)
        return known_ids

    @staticmethod
    def _normalize_subentry_id(subentry: ConfigSubentry) -> str | None:
        subentry_id = getattr(subentry, "entry_id", None)
        if isinstance(subentry_id, str) and subentry_id:
            return subentry_id
        subentry_id = getattr(subentry, "subentry_id", None)
        if isinstance(subentry_id, str) and subentry_id:
            return subentry_id
        return None

    def async_entries(self, _domain: str) -> list[_StubConfigEntry]:
        return list(self._entries)

    async def async_forward_entry_setups(
        self,
        entry: _StubConfigEntry,
        platforms: Iterable[Platform],
    ) -> None:
        platform_names = tuple(_platform_value(platform) for platform in platforms)
        self.forward_calls.append((entry, platform_names))

    async def async_unload_platforms(
        self, entry: _StubConfigEntry, _platforms: list[str]
    ) -> bool:
        return True

    def async_forward_entry_unload(
        self, entry: _StubConfigEntry, platforms: object
    ) -> bool:
        del platforms
        runtime = getattr(entry, "runtime_data", None)
        manager = getattr(runtime, "subentry_manager", None)
        if manager is not None:
            removal_result = manager.async_remove_all()
            if inspect.isawaitable(removal_result):
                return removal_result  # type: ignore[return-value]
            managed = getattr(manager, "managed_subentries", None)
            if isinstance(managed, dict):
                managed.clear()

        for subentry_id in list(entry.subentries):
            self.async_remove_subentry(entry, subentry_id)

        return True

    def async_add_subentry(
        self, entry: _StubConfigEntry, subentry: ConfigSubentry
    ) -> bool:
        entry.subentries[subentry.subentry_id] = subentry
        self.added_subentries.append((entry, subentry))
        normalized_id = self._normalize_subentry_id(subentry)
        if normalized_id is not None:
            self._registered_subentry_ids.add(normalized_id)
        return True

    def async_update_subentry(
        self,
        entry: _StubConfigEntry,
        subentry: ConfigSubentry,
        *,
        data: dict[str, Any] | None = None,
        title: str | None = None,
        unique_id: str | None = None,
        translation_key: str | None = None,
    ) -> bool:
        changed = False
        if data is not None:
            subentry.data = MappingProxyType(dict(data))
            changed = True
        if title is not None and subentry.title != title:
            subentry.title = title
            changed = True
        if unique_id is not None and subentry.unique_id != unique_id:
            subentry.unique_id = unique_id
            changed = True
        if translation_key is not None and subentry.translation_key != translation_key:
            subentry.translation_key = translation_key
            changed = True
        entry.subentries[subentry.subentry_id] = subentry
        self.updated_subentries.append((entry, subentry))
        normalized_id = self._normalize_subentry_id(subentry)
        if normalized_id is not None:
            self._registered_subentry_ids.add(normalized_id)
        return changed

    def async_remove_subentry(self, entry: _StubConfigEntry, subentry_id: str) -> bool:
        entry.subentries.pop(subentry_id, None)
        self.removed_subentries.append((entry, subentry_id))
        self._registered_subentry_ids.discard(subentry_id)
        return True

    def async_get_entry(
        self, entry_id: str
    ) -> _StubConfigEntry | ConfigSubentry | None:
        # Mirror tests.helpers.homeassistant.FakeConfigEntriesManager so
        # subentry retries observe registered children before exhausting.
        return resolve_config_entry_lookup(self._entries, entry_id)

    def async_get_subentries(self, entry_id: str) -> list[ConfigSubentry]:
        entry = self.async_get_entry(entry_id)
        if entry is None:
            return []
        return list(entry.subentries.values())

    async def async_setup(self, entry_id: str) -> bool:
        if entry_id not in self._known_entry_ids():
            raise LookupError(f"Config entry '{entry_id}' not registered")
        self.setup_calls.append(entry_id)
        return True

    def async_schedule_reload(self, entry_id: str) -> None:
        """Record a scheduled reload instead of performing one."""

        self.scheduled_reloads.append(entry_id)

    def async_update_entry(self, entry: _StubConfigEntry, **kwargs: Any) -> None:
        self.entry_update_calls.append((entry, dict(kwargs)))

        options = kwargs.get("options")
        if isinstance(options, Mapping):
            entry.options = dict(options)

        data = kwargs.get("data")
        if isinstance(data, Mapping):
            entry.data = dict(data)

        title = kwargs.get("title")
        if isinstance(title, str):
            entry.title = title

        unique_id = kwargs.get("unique_id")
        if isinstance(unique_id, str):
            setattr(entry, "unique_id", unique_id)

        version_value = kwargs.get("version")
        if isinstance(version_value, int):
            entry.version = version_value

    async def async_reload(self, entry_id: str) -> None:
        self.reload_calls.append(entry_id)

    async def async_unload(self, entry_id: str) -> bool:
        self.unload_calls.append(entry_id)
        return True

    async def async_set_disabled_by(
        self, entry_id: str, disabled_by: object | None
    ) -> None:
        for entry in self._entries:
            if entry.entry_id == entry_id:
                entry.disabled_by = disabled_by
                self.set_disabled_by_calls.append((entry_id, disabled_by))
                break


_StubServices = FakeServiceRegistry


class _StubHass:
    """Home Assistant core stub with just enough surface for setup."""

    def __init__(
        self, entry: _StubConfigEntry, loop: asyncio.AbstractEventLoop
    ) -> None:
        from homeassistant.core import CoreState

        self.data: dict[str, Any] = {DOMAIN: {}, "core.uuid": "ha-uuid"}
        self.loop = loop
        self.state = CoreState.running
        self.bus = _StubBus()
        self.http = _StubHttp()
        self.config_entries = _StubConfigEntries(entry)
        self._tasks: list[asyncio.Task[Any]] = []
        self.services = _StubServices()
        entry._attach_hass(self)

    def async_create_task(
        self, coro: Awaitable[Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        task = self.loop.create_task(coro, name=name)
        self._tasks.append(task)
        return task

    async def async_add_executor_job(self, func: Callable[..., Any], *args: Any) -> Any:
        return func(*args)

    def verify_event_loop_thread(self, _action: str | None = None) -> None:
        return


@dataclass(slots=True)
class AsyncSetupEntryHarness:
    """Shared scaffolding for tests exercising async_setup_entry."""

    integration: ModuleType
    coordinator_module: ModuleType
    button_module: ModuleType
    map_view_module: ModuleType
    services_module: ModuleType
    hass: _StubHass
    entry: _StubConfigEntry
    cache: _StubCache
    coordinator_cls: type[Any]
    dummy_fcm: SimpleNamespace


def _prepare_async_setup_entry_harness(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
    loop: asyncio.AbstractEventLoop,
    *,
    entry: _StubConfigEntry | None = None,
    hass: _StubHass | None = None,
) -> AsyncSetupEntryHarness:
    """Apply common integration patches and return the prepared context."""

    # Harness attribute usage expectations:
    # * test_hass_data_layout relies on integration, button_module,
    #   map_view_module, hass, entry, and cache.
    # * test_async_setup_entry_propagates_subentry_registration consumes
    #   integration, hass, and entry.

    integration = importlib.import_module("custom_components.googlefindmy")
    coordinator_module = importlib.import_module(
        "custom_components.googlefindmy.coordinator"
    )
    button_module = importlib.import_module("custom_components.googlefindmy.button")
    sys.modules.pop("custom_components.googlefindmy.map_view", None)
    map_view_module = importlib.import_module("custom_components.googlefindmy.map_view")
    services_module = importlib.import_module("custom_components.googlefindmy.services")

    # Import every module that copies ``GoogleFindMyCoordinator`` into its own
    # namespace *before* the first monkeypatch below.  A platform setup driven
    # from inside the patch window imports some of them lazily for the first
    # time; the copy they take is then the stub, and monkeypatch cannot undo a
    # binding it never made.  Importing up front makes them copy the production
    # class, which is the state the suite already has whenever an earlier test
    # file happened to import them first.  Guarded statically by
    # ``tests/test_guard_coordinator_identity.py`` (list completeness and the
    # ordering below) and at runtime by ``detect_coordinator_identity_leaks()``
    # in ``tests/conftest.py``.
    import_coordinator_consumers()

    cache = _StubCache()
    monkeypatch.setattr(integration.TokenCache, "create", AsyncMock(return_value=cache))
    monkeypatch.setattr(integration, "_register_instance", lambda *_: None)
    monkeypatch.setattr(integration, "_unregister_instance", lambda *_: cache)

    async_defaults: dict[str, AsyncMock] = {
        "_async_soft_migrate_data_to_options": AsyncMock(return_value=None),
        "_async_migrate_unique_ids": AsyncMock(return_value=None),
        "_async_relink_button_devices": AsyncMock(return_value=None),
        "_async_relink_subentry_entities": AsyncMock(return_value=None),
        "_async_save_secrets_data": AsyncMock(return_value=None),
        "_async_seed_manual_credentials": AsyncMock(return_value=None),
        "_async_normalize_device_names": AsyncMock(return_value=None),
        "_async_release_shared_fcm": AsyncMock(return_value=None),
        "_async_self_heal_duplicate_entities": AsyncMock(return_value=None),
        "_ensure_post_migration_consistency": AsyncMock(
            return_value=(True, "user@example.com")
        ),
    }

    for attribute, mock in async_defaults.items():
        monkeypatch.setattr(integration, attribute, mock, raising=False)

    monkeypatch.setattr(integration, "_self_heal_device_registry", lambda *_: None)

    class _RegisterViewStub:
        def __init__(self, hass_obj: Any) -> None:
            self.hass = hass_obj

    monkeypatch.setattr(integration, "GoogleFindMyMapView", _RegisterViewStub)
    monkeypatch.setattr(integration, "GoogleFindMyMapRedirectView", _RegisterViewStub)

    dummy_fcm = SimpleNamespace(
        register_coordinator=lambda *_: None,
        unregister_coordinator=lambda *_: None,
        _start_listening=AsyncMock(return_value=None),
        request_stop=lambda: None,
    )
    monkeypatch.setattr(
        integration,
        "_async_acquire_shared_fcm",
        AsyncMock(return_value=dummy_fcm),
    )

    coordinator_cls = stub_coordinator_factory()
    monkeypatch.setattr(coordinator_module, "GoogleFindMyCoordinator", coordinator_cls)
    monkeypatch.setattr(integration, "GoogleFindMyCoordinator", coordinator_cls)
    monkeypatch.setattr(button_module, "GoogleFindMyCoordinator", coordinator_cls)
    monkeypatch.setattr(
        map_view_module, "GoogleFindMyCoordinator", coordinator_cls, raising=False
    )

    entry_obj = entry or _StubConfigEntry()
    hass_obj = hass or _StubHass(entry_obj, loop)

    return AsyncSetupEntryHarness(
        integration=integration,
        coordinator_module=coordinator_module,
        button_module=button_module,
        map_view_module=map_view_module,
        services_module=services_module,
        hass=hass_obj,
        entry=entry_obj,
        cache=cache,
        coordinator_cls=coordinator_cls,
        dummy_fcm=dummy_fcm,
    )


@pytest.mark.asyncio
async def test_async_setup_entry_hijacks_legacy_credentials_when_cache_empty(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Legacy shapes with empty caches are migrated and cached once."""

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    legacy_secrets = json.dumps(
        {
            "email": "LegacyUser@Example.com ",
            CONF_OAUTH_TOKEN: "legacy-oauth-token",
            DATA_AAS_TOKEN: "legacy-aas-token",
        }
    )

    entry.data = {
        DATA_SECRET_BUNDLE: legacy_secrets,
        CONF_OAUTH_TOKEN: "outer-oauth-token",
    }
    entry.options = {"tracked_devices": ["legacy-device"]}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    update_calls = hass.config_entries.entry_update_calls
    assert update_calls, "Migration should rewrite the entry once"

    _, payload = update_calls[0]
    migrated_data = payload["data"]
    assert DATA_SECRET_BUNDLE not in migrated_data
    assert "scanned_data" not in migrated_data
    assert CONF_OAUTH_TOKEN not in migrated_data
    assert migrated_data[CONF_GOOGLE_EMAIL] == "legacyuser@example.com"
    assert migrated_data[DATA_AUTH_METHOD] == "secrets_json"
    assert payload["options"] == {}

    cache = harness.cache
    username_key = integration.username_string
    assert cache.values[username_key] == "legacyuser@example.com"
    # The secrets-bundle normalizer edge-trims non-credential fields (case is
    # preserved, only the pasted trailing space is removed).
    assert cache.values[DATA_SECRET_BUNDLE]["email"] == "LegacyUser@Example.com"
    assert cache.values[CONF_OAUTH_TOKEN] == "outer-oauth-token"
    assert cache.values[DATA_AAS_TOKEN] == "legacy-aas-token"


@pytest.mark.asyncio
async def test_async_setup_entry_keeps_the_initial_options_of_a_fresh_entry(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A fresh entry keeps the options chosen during the initial setup.

    The config flow stores ``CONF_OAUTH_TOKEN`` in ``entry.data`` while the
    entry's token cache is still empty, so the first setup runs the legacy
    migration. That migration used to replace the options with ``{}`` and
    turned a declined Map View back on. The user's settings and the schema
    version survive; removed keys and migration markers are still dropped.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data = {
        DATA_AUTH_METHOD: "oauth",
        CONF_OAUTH_TOKEN: "fresh-oauth-token",
        CONF_GOOGLE_EMAIL: "user@example.com",
    }
    initial_options = {
        OPT_MAP_VIEW_ENABLED: False,
        OPT_LOCATION_POLL_INTERVAL: 600,
        OPT_OPTIONS_SCHEMA_VERSION: 2,
        SECRETS_EXTRA_WATCH_PATHS: "/config/extra-secrets.json",
    }
    entry.options = {
        **initial_options,
        "tracked_devices": ["stale-device"],
        "unique_id_migrated": True,
    }

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    _, payload = hass.config_entries.entry_update_calls[0]
    assert CONF_OAUTH_TOKEN not in payload["data"]
    assert payload["options"] == initial_options
    assert entry.options[OPT_MAP_VIEW_ENABLED] is False
    assert hass.http.registered == []


@pytest.mark.asyncio
async def test_async_setup_entry_leaves_modern_entries_intact(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Primed caches prevent destructive rewrites for modern entries."""

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.options = {"tracked_devices": ["existing"]}
    entry.data[DATA_SECRET_BUNDLE] = {"username": "cached@example.com"}
    harness.cache.values = {integration.username_string: "cached@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    assert hass.config_entries.entry_update_calls == []
    assert entry.options == {"tracked_devices": ["existing"]}
    assert (
        harness.cache.values.get(integration.username_string)
        == entry.data[CONF_GOOGLE_EMAIL]
    )


@pytest.mark.asyncio
async def test_async_setup_entry_registers_the_map_tiles_token_view(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Setup registers the token view next to the two map views.

    The harness stubs the two map views; the token view is the real class, so
    the registered route is the one the map page fetches.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    hass = harness.hass

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, harness.entry) is True

    assert len(hass.http.registered) == 3
    token_views = [
        view
        for view in hass.http.registered
        if getattr(view, "url", None) == "/api/googlefindmy/map_tiles_token"
    ]
    assert len(token_views) == 1
    assert type(token_views[0]).__name__ == "GoogleFindMyMapTilesTokenView"


@pytest.mark.asyncio
async def test_async_setup_entry_skips_view_registration_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A single entry with ``map_view_enabled`` off registers no views at all."""

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    hass = harness.hass
    # A "modern" shape (cache already primed) skips the legacy migration path;
    # that path keeps the option (see
    # ``test_async_setup_entry_keeps_the_initial_options_of_a_fresh_entry``),
    # but this test is about view registration, not about the migration.
    harness.entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}
    harness.entry.options[OPT_MAP_VIEW_ENABLED] = False

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, harness.entry) is True

    assert hass.http.registered == []


@pytest.mark.asyncio
async def test_async_setup_entry_disabled_entry_does_not_block_another_entry(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """One entry opting out must not stop a second entry from registering the views.

    Registration is gated per-entry but the "already registered" bucket is
    shared process/domain-wide (HA core has no view-unregister API), so the
    disabled entry setting up first must leave the door open for an enabled
    entry to register afterwards.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    hass = harness.hass
    disabled_entry = harness.entry
    # A "modern" shape (cache already primed) skips the legacy migration path;
    # that path keeps the option (see
    # ``test_async_setup_entry_keeps_the_initial_options_of_a_fresh_entry``),
    # but this test is about view registration, not about the migration.
    disabled_entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}
    disabled_entry.options[OPT_MAP_VIEW_ENABLED] = False

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, disabled_entry) is True
    assert hass.http.registered == []

    enabled_entry = _StubConfigEntry()
    enabled_entry.entry_id = "entry-test-2"
    enabled_entry.unique_id = "user2@example.com"
    enabled_entry.data = {
        DATA_SECRET_BUNDLE: {"username": "user2@example.com"},
        CONF_GOOGLE_EMAIL: "user2@example.com",
    }
    enabled_entry._attach_hass(hass)
    hass.config_entries._entries.append(enabled_entry)

    assert await integration.async_setup_entry(hass, enabled_entry) is True

    assert len(hass.http.registered) == 3


@pytest.mark.asyncio
async def test_changed_credentials_reload_the_entry_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """The update listener carries the reload the config flow no longer does.

    Home Assistant turns "update listener plus reloading config-flow method"
    into an error in 2026.12, so the flow stores credentials without reloading.
    Storing alone would leave them ineffective: the token cache is seeded in
    ``async_setup_entry``, and a running coordinator does not pick up new
    tokens. The listener therefore has to reload on a credential change, once,
    and stay quiet for everything else.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_AAS_TOKEN] = "aas_et/OLD_TOKEN_VALUE"
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    assert len(entry._update_listeners) == 1
    notify = entry._update_listeners[0]

    # Anything that leaves the credentials alone must not reload.
    entry.options = {"tracked_devices": ["existing"]}
    await notify(hass, entry)
    assert hass.config_entries.scheduled_reloads == []

    # New credentials: one reload, and only one even if the notification
    # arrives twice before it takes effect.
    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/NEW_TOKEN_VALUE"}
    await notify(hass, entry)
    await notify(hass, entry)

    assert hass.config_entries.scheduled_reloads == [entry.entry_id], (
        "changed credentials have to become effective, and a second "
        "notification must not schedule a second reload"
    )


@pytest.mark.asyncio
async def test_the_listener_gives_the_latch_back_when_scheduling_fails(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A claim is a promise to reload; a broken promise has to be given back.

    The listener claims the shared latch immediately before scheduling. If the
    scheduling call raises, keeping the claim would be the worse of the two
    failures: the reload that failed is gone either way, but a latch left behind
    silently swallows every later reload of that entry as well.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_AAS_TOKEN] = "aas_et/OLD_TOKEN_VALUE"
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    notify = entry._update_listeners[0]

    def _boom(_entry_id: str) -> None:
        raise RuntimeError("no event loop to schedule on")

    monkeypatch.setattr(
        hass.config_entries, "async_schedule_reload", _boom, raising=False
    )

    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/NEW_TOKEN_VALUE"}
    await notify(hass, entry)

    pending = hass.data[integration.DOMAIN]["pending_entry_reloads"]
    assert entry.entry_id not in pending, (
        "a latch kept after a failed schedule would block every later reload"
    )


@pytest.mark.asyncio
async def test_the_listener_stands_down_when_a_flow_already_reloads(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Two schedulers, one reload.

    A flow that writes credentials and reloads the entry itself also notifies
    this listener, which reloads for the very same change. Home Assistant's
    ``async_schedule_reload`` does not coalesce, so without an agreement on one
    owner the entry would be unloaded and set up twice in a row.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_AAS_TOKEN] = "aas_et/OLD_TOKEN_VALUE"
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    notify = entry._update_listeners[0]

    # The writing flow got there first and reloads on its own behalf.
    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True

    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/NEW_TOKEN_VALUE"}
    await notify(hass, entry)

    assert hass.config_entries.scheduled_reloads == [], (
        "a reload is already on its way; the listener must not add a second one"
    )

    # The reload arrives: its setup releases the latch, so the next credential
    # change reloads again instead of being swallowed forever.
    assert await integration.async_setup_entry(hass, entry) is True
    notify = entry._update_listeners[-1]
    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/THIRD_TOKEN_VALUE"}
    await notify(hass, entry)

    assert hass.config_entries.scheduled_reloads == [entry.entry_id], (
        "the released latch has to let a later change reload again"
    )


@pytest.mark.asyncio
async def test_a_listener_invocation_from_a_bygone_setup_does_nothing(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """An invocation queued before the unload must not reload the rebuilt entry.

    ``async_update_entry`` creates the listener task right away, while
    ``async_on_unload`` only takes the listener out of the entry. Such a call
    still runs afterwards, carrying the fingerprint of a setup that is over.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_AAS_TOKEN] = "aas_et/OLD_TOKEN_VALUE"
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    notify = entry._update_listeners[0]

    # Home Assistant keeps the registered listeners here; the stub does not, so
    # the attribute is supplied for this test the way the core would expose it.
    entry.update_listeners = [notify]
    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/NEW_TOKEN_VALUE"}
    await notify(hass, entry)
    assert hass.config_entries.scheduled_reloads == [entry.entry_id]

    # The reload has run: its unload released the latch, so nothing but the
    # staleness itself keeps this invocation from scheduling another one.
    integration.discard_pending_entry_reload(hass, entry.entry_id)

    # After the unload the listener is gone from the entry, but the queued task
    # still runs with the credentials of the entry it no longer belongs to.
    entry.update_listeners = []
    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/FOURTH_TOKEN_VALUE"}
    await notify(hass, entry)

    assert hass.config_entries.scheduled_reloads == [entry.entry_id], (
        "an invocation from a bygone setup must not reload the rebuilt entry"
    )


@pytest.mark.asyncio
async def test_the_listener_stays_out_of_an_unload_that_is_under_way(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """The latch is free for a while inside a reload; the state is not.

    ``async_unload_entry`` releases the latch as its first act, but Home
    Assistant removes the listener only *after* ``async_unload_entry`` returns
    (``_async_process_on_unload``). The whole platform unload lies between the
    two, with its awaits, so an invocation queued before the reload can wake up
    there, pass the identity check and find the latch free: a second teardown of
    an entry that is already being torn down. What separates the window from a
    genuine change is the entry state, which the core sets to
    ``UNLOAD_IN_PROGRESS`` *before* calling ``async_unload_entry`` (verified in
    dev and in the declared floor 2025.9.1).
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_AAS_TOKEN] = "aas_et/OLD_TOKEN_VALUE"
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    notify = entry._update_listeners[0]
    # The core exposes the registered listeners here; the stub does not, so the
    # attribute is supplied the way the core would, which keeps the identity
    # check from being the reason this test passes.
    entry.update_listeners = [notify]

    # Inside the window: the reload released the latch at the start of the
    # unload, the listener is still registered, and the entry is being torn down.
    integration.discard_pending_entry_reload(hass, entry.entry_id)
    entry.state = ConfigEntryState.UNLOAD_IN_PROGRESS
    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/NEW_TOKEN_VALUE"}
    await notify(hass, entry)

    assert hass.config_entries.scheduled_reloads == [], (
        "an unload that is already under way must not be answered with a second "
        "reload of the same entry"
    )

    # Counter-direction: once the entry is loaded again, a changed credential is
    # the listener's business as before.
    entry.state = ConfigEntryState.LOADED
    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/THIRD_TOKEN_VALUE"}
    await notify(hass, entry)

    assert hass.config_entries.scheduled_reloads == [entry.entry_id], (
        "a loaded entry with changed credentials still has to be reloaded"
    )


@pytest.mark.asyncio
async def test_a_core_without_schedule_reload_stays_quiet_about_it(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """An older core simply applies the credentials on the next restart.

    ``async_schedule_reload`` is resolved defensively because the declared
    minimum version is 2025.9.1; missing it must not raise out of an update
    listener, where an exception would surface as an unrelated error.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_AAS_TOKEN] = "aas_et/OLD_TOKEN_VALUE"
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    monkeypatch.setattr(
        hass.config_entries, "async_schedule_reload", None, raising=False
    )
    entry.data = {**entry.data, DATA_AAS_TOKEN: "aas_et/NEW_TOKEN_VALUE"}

    await entry._update_listeners[0](hass, entry)

    assert hass.config_entries.scheduled_reloads == []


@pytest.mark.asyncio
async def test_options_update_listener_never_touches_map_views_directly(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """The per-entry update listener leaves map-view (de)registration alone.

    ``map_view_enabled`` is a setup-time option, exactly like the poll
    interval or a feature-group toggle: the Settings form's own
    ``async_step_settings`` already calls ``_schedule_claimed_reload`` for any
    options change, and that reload re-runs ``async_setup_entry`` end to end,
    which registers the views and refreshes every device's
    ``configuration_url`` through the ordinary setup path. The listener
    installed on the entry (``_async_refresh_watch_paths``) must therefore
    never call ``_async_ensure_map_views_registered`` or
    ``_async_refresh_device_urls`` itself -- whether the change flips
    ``map_view_enabled`` on, off, or touches an unrelated option -- or it
    would race the reload the settings step already owns.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    # A "modern" shape (cache already primed) skips the legacy migration path;
    # that path keeps the option (see
    # ``test_async_setup_entry_keeps_the_initial_options_of_a_fresh_entry``),
    # but this test is about view registration, not about the migration.
    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}
    entry.options[OPT_MAP_VIEW_ENABLED] = False

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    assert hass.http.registered == []

    notify = entry._update_listeners[0]

    ensure_calls: list[tuple[Any, Any]] = []
    monkeypatch.setattr(
        integration,
        "_async_ensure_map_views_registered",
        lambda hass_arg, bucket: ensure_calls.append((hass_arg, bucket)),
    )
    refresh_mock = AsyncMock(return_value=None)
    monkeypatch.setattr(integration, "_async_refresh_device_urls", refresh_mock)

    for new_options in (
        {**entry.options, OPT_MAP_VIEW_ENABLED: True},
        {**entry.options, OPT_MAP_VIEW_ENABLED: False},
        {**entry.options, integration.OPT_LOCATION_POLL_INTERVAL: 999},
    ):
        entry.options = new_options
        await notify(hass, entry)

    assert ensure_calls == [], (
        "the update listener must never register map views itself; that is "
        "the reload's job"
    )
    refresh_mock.assert_not_awaited()


def test_the_reload_latch_is_a_no_op_without_an_entry_id() -> None:
    """No entry, no claim, and nothing to release.

    Both helpers are called from lifecycle hooks that read ``entry.entry_id``
    from partially initialised entries, so an empty id must neither claim a latch
    under the empty key nor create the domain bucket on the way out.
    """

    integration = importlib.import_module("custom_components.googlefindmy")

    hass = SimpleNamespace(data={})

    assert integration.claim_pending_entry_reload(hass, "") is False
    integration.discard_pending_entry_reload(hass, "")
    assert hass.data == {}, "neither helper may create the domain bucket here"

    # A release without a bucket is equally quiet.
    integration.discard_pending_entry_reload(SimpleNamespace(data={}), "entry-1")


def test_the_credential_fingerprint_keeps_no_plaintext() -> None:
    """The value held for the entry's lifetime must not carry the tokens."""

    integration = importlib.import_module("custom_components.googlefindmy")

    secret = "aas_et/VERY_SECRET_TOKEN"
    data = {
        CONF_GOOGLE_EMAIL: "user@example.com",
        DATA_AAS_TOKEN: secret,
        DATA_SECRET_BUNDLE: {"aas_token": secret, "username": "user@example.com"},
    }

    fingerprint = integration._credential_fingerprint(data)

    assert secret not in fingerprint
    assert "user@example.com" not in fingerprint
    assert fingerprint == integration._credential_fingerprint(dict(data))
    assert fingerprint != integration._credential_fingerprint(
        {**data, DATA_AAS_TOKEN: "aas_et/OTHER"}
    )
    # A missing container must not raise on the setup path.
    assert integration._credential_fingerprint(None) == (
        integration._credential_fingerprint({})
    )


@pytest.mark.asyncio
async def test_a_receiver_self_write_leaves_no_plaintext_in_hass_data(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """The self-write receipt in ``hass.data`` is a digest, never the value."""

    from custom_components.googlefindmy.Auth.fcm_receiver_ha import FcmReceiverHA
    from custom_components.googlefindmy.entry_self_write import (
        SELF_WRITE_RECEIPTS_KEY,
    )

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    secret = "VERY_SECRET_FCM_TOKEN"
    harness.entry.data = {
        CONF_GOOGLE_EMAIL: "user@example.com",
        DATA_SECRET_BUNDLE: {"fcm_credentials": {"fcm": {"token": "old"}}},
    }
    harness.cache.values = {integration.username_string: "user@example.com"}
    assert await integration.async_setup(harness.hass, {}) is True
    assert await integration.async_setup_entry(harness.hass, harness.entry)

    receiver = FcmReceiverHA()
    receiver.attach_hass(harness.hass)
    receiver._persist_credentials_to_entry(
        harness.entry.entry_id, {"fcm": {"token": secret}}
    )

    receipts = harness.hass.data[integration.DOMAIN][SELF_WRITE_RECEIPTS_KEY]
    assert list(receipts) == [harness.entry.entry_id]
    assert secret not in repr(receipts)
    assert secret not in repr(harness.hass.data[integration.DOMAIN])


# ---------------------------------------------------------------------------
# Deferred container-login cleanup (P2)
#
# ``ConfigFlow.async_create_entry`` only builds a FlowResult; Home Assistant
# creates and stores the entry afterwards in
# ``ConfigEntriesFlowManager.async_finish_flow`` (``await
# self.config_entries.async_add(entry)``). The flow therefore only *stages* the
# irreversible cleanups in ``hass.data[DOMAIN]["pending_container_cleanup"]``
# (in-memory, never HA storage).
#
# ``async_setup_entry`` only *claims* a ticket and arms a background task; the
# jobs run once the entry is provably in Home Assistant's storage, because
# ``ConfigEntries.async_add`` awaits ``async_setup_entry`` and schedules the
# (debounced) save only afterwards.
#
# The paths that update an *existing* entry stage through the same area, but
# their tickets name the entry and carry a ``modified_at`` watermark, because
# for them the entry id was in storage long before the update. Those semantics
# are covered in ``tests/test_config_flow_cleanup_tickets.py`` and
# ``tests/test_container_cleanup_persist_probe.py``; the tests here stay on the
# ``async_setup_entry`` side of the seam.
# ---------------------------------------------------------------------------


def _install_cleanup_recorder(
    monkeypatch: pytest.MonkeyPatch, recorded: list[str]
) -> None:
    """Record every deferred watched-secrets delete the cleanup runs.

    Recorded by digest, which is what tells two staged jobs apart in the tests
    below; the delete itself touches the filesystem and is covered where it
    lives.
    """

    async def _fake_delete(
        _hass: Any,
        *,
        imported_stable_key: str | None = None,
        imported_digest: str | None = None,
    ) -> None:
        recorded.append(imported_digest or "")

    monkeypatch.setattr(config_flow, "_async_delete_watched_secrets", _fake_delete)


def _install_persistence_probe(
    monkeypatch: pytest.MonkeyPatch, *, persisted: bool
) -> None:
    """Pin the durability gate's storage observation.

    The production probe reads Home Assistant's own config-entry store; the
    setup stub here has no such storage, so the observation is pinned instead
    of faked at the filesystem level. The gate logic around it stays real.

    ``min_modified_at`` is accepted and ignored: what the watermark *means* is
    pinned against real Home Assistant storage in
    ``tests/test_container_cleanup_persist_probe.py``. Accepting it here is not
    cosmetic -- a stub that rejected the keyword would turn every gate call into
    a ``TypeError``, which the runner swallows as "could not verify", i.e. these
    tests would silently stop exercising the path they are about.
    """

    async def _probe(
        _hass: Any, _entry_id: str, *, min_modified_at: Any = None
    ) -> bool:
        return persisted

    monkeypatch.setattr(config_flow, "_async_config_entry_is_persisted", _probe)
    # Keep the give-up path fast for the negative case.
    monkeypatch.setattr(config_flow, "PERSIST_PROOF_TIMEOUT", 0.0)


async def _drain_cleanup_tasks(entry: _StubConfigEntry) -> None:
    """Await the background tasks ``async_setup_entry`` armed on ``entry``."""

    while entry._background_tasks:
        pending = list(entry._background_tasks)
        entry._background_tasks.clear()
        await asyncio.gather(*pending)


def _stage_import_cleanup(
    hass: Any, unique_id: str | None, digest: str, *, flow_id: str = "flow-1"
) -> None:
    """Stage one delete-after-import cleanup job exactly as the flow does."""

    config_flow._async_stage_container_cleanup(
        hass,
        flow_id=flow_id,
        unique_id=unique_id,
        job=config_flow.PendingContainerCleanup(
            imported_stable_key="email:user@example.com",
            imported_digest=digest,
        ),
    )


@pytest.mark.asyncio
async def test_two_jobs_of_one_flow_share_a_ticket_and_upgrade_the_account() -> None:
    """A flow that stages twice gets ONE ticket, and its account is filled in later.

    Both halves of a container login are staged by the same flow: the discovery
    confirm step parks the delete of the imported copy, ``device_selection``
    parks the ack. They must land on one ticket, because a single
    ``async_setup_entry`` claims exactly one ticket and would otherwise leave the
    other half behind for an unrelated entry to pick up.

    The account upgrade is the second half of that: the first job can be staged
    before the flow resolved its unique id, so a ticket that starts out
    account-less has to adopt the account as soon as it is known. Without it the
    ticket stays claimable by *any* entry of this integration.
    """

    hass = SimpleNamespace(data={})

    config_flow._async_stage_container_cleanup(
        hass,
        flow_id="flow-two-halves",
        unique_id=None,
        job=config_flow.PendingContainerCleanup(imported_digest="first"),
    )
    config_flow._async_stage_container_cleanup(
        hass,
        flow_id="flow-two-halves",
        unique_id="user@example.com",
        job=config_flow.PendingContainerCleanup(imported_digest="second"),
    )

    tickets = hass.data[DOMAIN][config_flow.PENDING_CONTAINER_CLEANUP_KEY]
    assert len(tickets) == 1, "a second job of the same flow opened a second ticket"
    assert [job.imported_digest for job in tickets[0].jobs] == ["first", "second"]
    assert tickets[0].unique_id == "user@example.com"

    # A foreign entry must no longer be able to claim it now that the account is
    # known: that is the whole point of the upgrade.
    assert (
        config_flow._async_claim_container_cleanup(hass, unique_id="other@example.com")
        == []
    )
    claimed = config_flow._async_claim_container_cleanup(
        hass, unique_id="user@example.com"
    )
    assert [job.imported_digest for job in claimed] == ["first", "second"]


@pytest.mark.asyncio
async def test_cleanup_is_dropped_when_the_storage_probe_raises(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A probe that raises must drop the jobs, not run them.

    The fail-safe direction of the whole subsystem: without a positive durability
    proof the credentials stay where they are. An exception is *less* evidence
    than a plain ``False``, so it must never be treated as permission to run an
    irreversible cleanup.
    """

    hass = SimpleNamespace(data={})
    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)

    async def _exploding_probe(
        _hass: Any, _entry_id: str, *, min_modified_at: Any = None
    ) -> bool:
        raise OSError("storage unreadable")

    monkeypatch.setattr(
        config_flow, "_async_config_entry_is_persisted", _exploding_probe
    )

    _stage_import_cleanup(hass, "user@example.com", "delete-token-xyz")
    jobs = config_flow._async_claim_container_cleanup(
        hass, unique_id="user@example.com"
    )
    assert jobs, "precondition: a job must be claimed for this to say anything"

    with caplog.at_level(logging.WARNING):
        await config_flow._async_run_container_cleanup_when_persisted(
            hass, "entry-id", jobs
        )

    assert acked == [], "an unverifiable entry must not authorise the ack"
    assert "could not verify" in caplog.text.lower() or "kept on disk" in caplog.text
    # The delete token must not leak into the log.
    assert "delete-token-xyz" not in caplog.text


@pytest.mark.asyncio
async def test_async_setup_entry_runs_staged_container_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A job staged under the entry's unique id runs once setup succeeded."""

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)
    _install_persistence_probe(monkeypatch, persisted=True)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, entry.unique_id, "delete-token-xyz")
    # Still staged, not executed, before setup runs.
    assert acked == []

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    # Setup itself only arms the gate; the job runs from the background task.
    await _drain_cleanup_tasks(entry)

    assert acked == ["delete-token-xyz"]
    # The staging area is drained, so nothing lingers in hass.data.
    assert config_flow.PENDING_CONTAINER_CLEANUP_KEY not in hass.data[DOMAIN]


@pytest.mark.asyncio
async def test_async_setup_entry_reload_does_not_repeat_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A reload must not re-run an already executed cleanup (pop, not get).

    ``async_setup_entry`` runs again on every reload (an options change alone
    triggers one), so reading the staged jobs must consume them. Otherwise a
    single container login would ack on every reload for the rest of the
    Home Assistant process lifetime.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)
    _install_persistence_probe(monkeypatch, persisted=True)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, entry.unique_id, "delete-token-xyz")

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    await _drain_cleanup_tasks(entry)
    assert acked == ["delete-token-xyz"]

    # Second setup (reload): no job left, so no second ack.
    assert await integration.async_setup_entry(hass, entry) is True
    await _drain_cleanup_tasks(entry)
    assert acked == ["delete-token-xyz"]


@pytest.mark.asyncio
async def test_async_setup_entry_survives_failing_cleanup_job(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A cleanup job that raises must not turn a good setup into a failed one.

    The secrets watcher re-imports a surviving file, so a failed cleanup is
    recoverable while a failed setup is not.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    async def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("delete exploded")

    monkeypatch.setattr(config_flow, "_async_delete_watched_secrets", _boom)
    _install_persistence_probe(monkeypatch, persisted=True)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, entry.unique_id, "delete-token-xyz")

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    await _drain_cleanup_tasks(entry)
    # Consumed despite the failure: retrying the delete forever would be worse
    # than leaving the file for the watcher's next scan.
    assert config_flow.PENDING_CONTAINER_CLEANUP_KEY not in hass.data[DOMAIN]


@pytest.mark.asyncio
async def test_async_setup_entry_survives_failing_cleanup_scheduler(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A broken cleanup *scheduler* must leave ``async_setup_entry`` at True.

    Deliberately narrower than the name it used to carry: since the jobs run
    from a background task, a failing *runner* can no longer reach
    ``async_setup_entry`` at all. What still can is the arming itself, and that
    is what this pins. The runner's own failure paths are covered by
    ``test_cleanup_is_dropped_when_the_storage_probe_raises``.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    def _boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("scheduler exploded")

    monkeypatch.setattr(config_flow, "async_schedule_pending_container_cleanup", _boom)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True


@pytest.mark.asyncio
async def test_async_setup_entry_claims_account_less_ticket(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A ticket staged without a unique id is claimed too, never stranded.

    The flow sets its unique id before creating the entry, so this is the
    defensive branch: an account-less ticket must not accumulate jobs that no
    setup ever claims. The cleanup itself is self-validating (the delete
    re-checks account and content, the ack is bound to its own delete token),
    so running it on the next setup is safe.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)
    _install_persistence_probe(monkeypatch, persisted=True)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, None, "orphan-delete-token")
    staged = hass.data[DOMAIN][config_flow.PENDING_CONTAINER_CLEANUP_KEY]
    assert [ticket.unique_id for ticket in staged] == [None]

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    await _drain_cleanup_tasks(entry)

    assert acked == ["orphan-delete-token"]
    assert config_flow.PENDING_CONTAINER_CLEANUP_KEY not in hass.data[DOMAIN]


@pytest.mark.asyncio
async def test_setup_does_not_clean_up_before_the_entry_is_stored(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """P1: no irreversible cleanup while the entry is only in memory.

    ``ConfigEntries.async_add`` awaits ``async_setup_entry`` and calls
    ``_async_schedule_save`` only afterwards, which then saves *debounced*
    (``SAVE_DELAY``). Reaching the end of setup therefore proves the entry
    exists in memory, not that it survived to storage. If Home Assistant is
    stopped or crashes in that window, an ack or a bundle delete would leave
    neither the entry nor the credentials behind, forcing a full re-login.

    The gate must therefore fail towards "credentials survive, cleanup is
    lost": without a positive storage observation, nothing irreversible runs.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)
    _install_persistence_probe(monkeypatch, persisted=False)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, entry.unique_id, "delete-token-xyz")

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    await _drain_cleanup_tasks(entry)

    # The container was never told to drop its copy of the credentials.
    assert acked == []


@pytest.mark.asyncio
async def test_cleanup_task_cancellation_keeps_the_credentials(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """P1: a shutdown during the durability wait must not ack the container.

    Home Assistant cancels an entry's background tasks on shutdown and on
    unload. That cancellation is the structural half of the guarantee: it turns
    "Home Assistant stopped right after setup" into a dropped cleanup instead of
    a destroyed credential.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)

    probe_reached = asyncio.Event()

    async def _never_persisted(
        _hass: Any, _entry_id: str, *, min_modified_at: Any = None
    ) -> bool:
        probe_reached.set()
        await asyncio.sleep(3600)
        return True  # pragma: no cover - the sleep is always cancelled

    monkeypatch.setattr(
        config_flow, "_async_config_entry_is_persisted", _never_persisted
    )

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, entry.unique_id, "delete-token-xyz")

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    tasks = list(entry._background_tasks)
    entry._background_tasks.clear()
    assert tasks, "async_setup_entry must arm the cleanup as a background task"
    await probe_reached.wait()

    for task in tasks:
        task.cancel()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)

    assert acked == []


@pytest.mark.asyncio
async def test_setup_claims_only_its_own_flow_ticket(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """P2: overlapping same-account flows must not share one cleanup list.

    Two create flows for the same account stage two tickets under the same
    unique id. Bucketing them by account merged both job lists, so the first
    entry that reached ``async_setup_entry`` executed the second flow's
    irreversible cleanup as well -- for an entry that may never materialise.
    Each setup claims exactly one ticket, in staging order.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)
    _install_persistence_probe(monkeypatch, persisted=True)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, entry.unique_id, "token-flow-a", flow_id="flow-a")
    _stage_import_cleanup(hass, entry.unique_id, "token-flow-b", flow_id="flow-b")

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    await _drain_cleanup_tasks(entry)

    # Only the first flow's job ran; the second flow's job is still waiting for
    # its own entry.
    assert acked == ["token-flow-a"]
    staged = hass.data[DOMAIN][config_flow.PENDING_CONTAINER_CLEANUP_KEY]
    assert [ticket.flow_id for ticket in staged] == ["flow-b"]


@pytest.mark.asyncio
async def test_update_listener_adopts_newly_configured_watch_path(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
    tmp_path: Path,
) -> None:
    """The registered update listener adopts the entry's new extra watch path.

    ``async_setup_entry`` registers a listener adapter that forwards to
    ``_async_refresh_discovery_watch_paths``. The adapter must NOT exclude the
    entry that was just updated: that entry is precisely the one whose freshly
    configured ``SECRETS_EXTRA_WATCH_PATHS`` has to be picked up without a Home
    Assistant restart. The test drives the listener that production registered,
    not a hand-built copy of it.
    """

    discovery = importlib.import_module("custom_components.googlefindmy.discovery")

    default_path = tmp_path / "defaults" / "secrets.json"
    extra_path = tmp_path / "extra" / "secrets.json"

    async def _fake_translations(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    async def _fake_trigger(_hass: Any, **_kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(discovery, "_default_watch_paths", lambda: [default_path])
    monkeypatch.setattr(discovery, "_trigger_cloud_discovery", _fake_trigger)
    monkeypatch.setattr(discovery, "async_track_time_interval", lambda *_: lambda: None)
    monkeypatch.setattr(discovery.cf, "_find_entry_by_email", lambda *_: None)
    monkeypatch.setattr(
        discovery.translation, "async_get_translations", _fake_translations
    )

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True

    manager = hass.data[DOMAIN]["discovery_manager"]
    assert manager.watch_paths == (default_path,)

    listeners = list(entry._update_listeners)
    assert len(listeners) == 1, "async_setup_entry must register exactly one listener"

    entry.options = dict(entry.options)
    entry.options[discovery.SECRETS_EXTRA_WATCH_PATHS] = [str(extra_path)]
    await listeners[0](hass, entry)

    assert extra_path in manager.watch_paths

    await manager.async_stop()


@pytest.mark.asyncio
async def test_setup_entry_adopts_watch_path_armed_after_the_manager(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
    tmp_path: Path,
) -> None:
    """Setting an entry up adopts an extra watch path the manager never saw.

    The discovery manager is armed once per Home Assistant instance, and
    ``_collect_extra_watch_paths`` skips disabled entries. Home Assistant fires
    no update listener when an entry is enabled again, so without a refresh at
    the end of ``async_setup_entry`` the path of a re-enabled (or later set up)
    entry would stay unobserved until the next options update or a restart.
    The test arms the manager first and only then makes the option visible,
    which is exactly the state a re-enabled entry is in.
    """

    discovery = importlib.import_module("custom_components.googlefindmy.discovery")

    default_path = tmp_path / "defaults" / "secrets.json"
    extra_path = tmp_path / "reenabled" / "secrets.json"  # codespell:ignore reenabled

    async def _fake_translations(*_args: Any, **_kwargs: Any) -> dict[str, str]:
        return {}

    async def _fake_trigger(_hass: Any, **_kwargs: Any) -> bool:
        return True

    monkeypatch.setattr(discovery, "_default_watch_paths", lambda: [default_path])
    monkeypatch.setattr(discovery, "_trigger_cloud_discovery", _fake_trigger)
    monkeypatch.setattr(discovery, "async_track_time_interval", lambda *_: lambda: None)
    monkeypatch.setattr(discovery.cf, "_find_entry_by_email", lambda *_: None)
    monkeypatch.setattr(
        discovery.translation, "async_get_translations", _fake_translations
    )

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    # Arm the singleton while the extra path is still invisible.
    assert await integration.async_setup(hass, {}) is True
    manager = hass.data[DOMAIN]["discovery_manager"]
    assert manager.watch_paths == (default_path,)

    # Now the option becomes visible, as it does when an entry is re-enabled.
    entry.options = dict(entry.options)
    entry.options[discovery.SECRETS_EXTRA_WATCH_PATHS] = [str(extra_path)]

    assert await integration.async_setup_entry(hass, entry) is True

    assert extra_path in manager.watch_paths

    await manager.async_stop()


@pytest.mark.asyncio
async def test_duplicate_account_abort_discards_staged_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
    tmp_path: Path,
) -> None:
    """The duplicate-account abort drops the staged job instead of leaking it.

    That branch leaves ``async_setup_entry`` with a *final* ``return False``,
    far above the cleanup runner at the end of the function, and Home Assistant
    does not retry it. Without an explicit discard the job would sit in
    ``hass.data`` for the rest of the process lifetime and the bucket could grow
    without bound.

    Discarded, not executed: nothing about this account was set up, so the
    fail-safe direction applies. The watched credential file stays on disk (the
    secrets watcher re-imports it on its next scan) and the login container is
    left to its own TTL delete rather than being acked.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)

    watched = tmp_path / "data" / "secrets.json"
    watched.parent.mkdir(parents=True, exist_ok=True)
    watched.write_text(
        json.dumps({"google_email": "user@example.com", "shared_key": "DDEEFF"}),
        encoding="utf-8",
    )
    hass.data.setdefault(DOMAIN, {})["discovery_manager"] = SimpleNamespace(
        watch_paths=(watched,)
    )

    # A staged delete of the imported copy.
    config_flow._async_stage_container_cleanup(
        hass,
        flow_id="flow-duplicate",
        unique_id=entry.unique_id,
        job=config_flow.PendingContainerCleanup(
            imported_stable_key="email:user@example.com",
            imported_digest="deadbeef",
        ),
    )

    # This entry duplicates an account that is already configured.
    monkeypatch.setattr(
        integration,
        "_ensure_post_migration_consistency",
        AsyncMock(return_value=(False, "user@example.com")),
    )

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is False

    # Nothing was executed on the way out.
    assert acked == []
    assert watched.exists()
    # ... but the bucket is empty, so the nonce/token do not linger.
    assert config_flow.PENDING_CONTAINER_CLEANUP_KEY not in hass.data[DOMAIN]


@pytest.mark.asyncio
async def test_config_entry_not_ready_keeps_staged_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """A retryable setup failure must leave the staged job alone.

    The counterpart of the duplicate-account discard: ``ConfigEntryNotReady``
    means Home Assistant will try this entry again, so the job has to survive
    until a setup actually succeeds. Consuming it here would silently drop the
    ack and the delete for good.
    """

    loop = asyncio.get_running_loop()
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    acked: list[str] = []
    _install_cleanup_recorder(monkeypatch, acked)

    entry.data[DATA_SECRET_BUNDLE] = {"username": "user@example.com"}
    harness.cache.values = {integration.username_string: "user@example.com"}

    _stage_import_cleanup(hass, entry.unique_id, "retry-delete-token")

    async def _not_ready(*_args: Any, **_kwargs: Any) -> None:
        raise ConfigEntryNotReady("try again later")

    # Fails inside the setup core, i.e. before the cleanup runner at the end.
    monkeypatch.setattr(integration, "_async_refresh_device_urls", _not_ready)

    assert await integration.async_setup(hass, {}) is True
    with pytest.raises(ConfigEntryNotReady):
        await integration.async_setup_entry(hass, entry)

    assert acked == []
    staged = hass.data[DOMAIN][config_flow.PENDING_CONTAINER_CLEANUP_KEY]
    assert [ticket.unique_id for ticket in staged] == [entry.unique_id]
    assert [job.imported_digest for job in staged[0].jobs] == ["retry-delete-token"]


def test_service_stats_unique_id_migration_prefers_service_subentry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tracker-prefixed stats sensor IDs collapse to the service identifier."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        entry = _StubConfigEntry()
        entry.entry_id = "entry-test"

        tracker_subentry = ConfigSubentry(
            data={
                "group_key": TRACKER_SUBENTRY_KEY,
                "features": ("device_tracker", "sensor"),
            },
            subentry_type=SUBENTRY_TYPE_TRACKER,
            title="Devices",
            unique_id=f"{entry.entry_id}-{TRACKER_SUBENTRY_KEY}",
            subentry_id="tracker-subentry",
        )
        service_subentry = ConfigSubentry(
            data={
                "group_key": SERVICE_SUBENTRY_KEY,
                "features": ("binary_sensor",),
            },
            subentry_type=SUBENTRY_TYPE_SERVICE,
            title="Service",
            unique_id=f"{entry.entry_id}-{SERVICE_SUBENTRY_KEY}",
            subentry_id="service-subentry",
        )
        entry.subentries = {
            tracker_subentry.subentry_id: tracker_subentry,
            service_subentry.subentry_id: service_subentry,
        }

        hass = _StubHass(entry, loop)

        class _RegistryStub:
            def __init__(self) -> None:
                self.entities: dict[str, SimpleNamespace] = {}
                self._by_key: dict[tuple[str, str, str], str] = {}
                self.updated: list[str] = []

            def add(
                self,
                *,
                entity_id: str,
                domain: str,
                platform: str,
                unique_id: str,
                config_entry_id: str,
            ) -> None:
                entry_obj = SimpleNamespace(
                    entity_id=entity_id,
                    domain=domain,
                    platform=platform,
                    unique_id=unique_id,
                    config_entry_id=config_entry_id,
                )
                self.entities[entity_id] = entry_obj
                self._by_key[(domain, platform, unique_id)] = entity_id

            def async_get_entity_id(
                self, domain: str, platform: str, unique_id: str
            ) -> str | None:
                return self._by_key.get((domain, platform, unique_id))

            def async_update_entity(
                self,
                entity_id: str,
                *,
                new_unique_id: str | None = None,
                **_: Any,
            ) -> None:
                entry_obj = self.entities[entity_id]
                if new_unique_id:
                    self._by_key.pop(
                        (entry_obj.domain, entry_obj.platform, entry_obj.unique_id),
                        None,
                    )
                    entry_obj.unique_id = new_unique_id
                    self._by_key[
                        (entry_obj.domain, entry_obj.platform, new_unique_id)
                    ] = entity_id
                self.updated.append(entity_id)

        class _DeviceRegistryStub:
            def __init__(self) -> None:
                self.devices: dict[str, Any] = {}

            def async_update_device(self, **_: Any) -> None:  # pragma: no cover - stub
                return None

        entity_registry = _RegistryStub()
        entity_registry.add(
            entity_id="sensor.googlefindmy_api_updates",
            domain="sensor",
            platform=integration.DOMAIN,
            unique_id=(
                f"{integration.DOMAIN}_{entry.entry_id}_"
                f"{tracker_subentry.subentry_id}_{service_subentry.subentry_id}_api_updates_total"
            ),
            config_entry_id=entry.entry_id,
        )
        device_registry = _DeviceRegistryStub()

        monkeypatch.setattr(integration.er, "async_get", lambda _hass: entity_registry)
        monkeypatch.setattr(integration.dr, "async_get", lambda _hass: device_registry)

        loop.run_until_complete(integration._async_migrate_unique_ids(hass, entry))

        migrated = entity_registry.entities["sensor.googlefindmy_api_updates"]
        assert migrated.unique_id == (
            f"{integration.DOMAIN}_{entry.entry_id}_"
            f"{service_subentry.subentry_id}_api_updates_total"
        )
        assert entity_registry.updated == ["sensor.googlefindmy_api_updates"]
    finally:
        drain_loop(loop)


def test_unique_id_migration_rewrites_legacy_tracker_entities(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy tracker IDs are namespaced and scoped to subentries without collisions."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        entry = _StubConfigEntry()
        entry.entry_id = "entry-legacy"

        tracker_subentry = ConfigSubentry(
            data={
                "group_key": TRACKER_SUBENTRY_KEY,
                "features": ("device_tracker", "sensor"),
            },
            subentry_type=SUBENTRY_TYPE_TRACKER,
            title="Devices",
            unique_id=f"{entry.entry_id}-{TRACKER_SUBENTRY_KEY}",
            subentry_id="tracker-subentry",
        )
        entry.subentries = {tracker_subentry.subentry_id: tracker_subentry}

        hass = _StubHass(entry, loop)

        class _RegistryStub:
            def __init__(self) -> None:
                self.entities: dict[str, SimpleNamespace] = {}
                self._by_key: dict[tuple[str, str, str], str] = {}
                self.updated: list[str] = []

            def add(
                self,
                *,
                entity_id: str,
                domain: str,
                platform: str,
                unique_id: str,
                config_entry_id: str,
            ) -> None:
                entry_obj = SimpleNamespace(
                    entity_id=entity_id,
                    domain=domain,
                    platform=platform,
                    unique_id=unique_id,
                    config_entry_id=config_entry_id,
                )
                self.entities[entity_id] = entry_obj
                self._by_key[(domain, platform, unique_id)] = entity_id

            def async_get_entity_id(
                self, domain: str, platform: str, unique_id: str
            ) -> str | None:
                return self._by_key.get((domain, platform, unique_id))

            def async_update_entity(
                self,
                entity_id: str,
                *,
                new_unique_id: str | None = None,
                **_: Any,
            ) -> None:
                entry_obj = self.entities[entity_id]
                if new_unique_id:
                    self._by_key.pop(
                        (entry_obj.domain, entry_obj.platform, entry_obj.unique_id),
                        None,
                    )
                    entry_obj.unique_id = new_unique_id
                    self._by_key[
                        (entry_obj.domain, entry_obj.platform, new_unique_id)
                    ] = entity_id
                self.updated.append(entity_id)

        class _DeviceRegistryStub:
            def __init__(self) -> None:
                self.devices: dict[str, Any] = {}

            def async_update_device(self, **_: Any) -> None:  # pragma: no cover - stub
                return None

        entity_registry = _RegistryStub()
        entity_registry.add(
            entity_id="device_tracker.googlefindmy_device_one",
            domain="device_tracker",
            platform=integration.DOMAIN,
            unique_id=f"{integration.DOMAIN}_device-1",
            config_entry_id=entry.entry_id,
        )
        entity_registry.add(
            entity_id="device_tracker.googlefindmy_device_two",
            domain="device_tracker",
            platform=integration.DOMAIN,
            unique_id=f"{integration.DOMAIN}_{entry.entry_id}_device-2",
            config_entry_id=entry.entry_id,
        )
        device_registry = _DeviceRegistryStub()

        monkeypatch.setattr(integration.er, "async_get", lambda _hass: entity_registry)
        monkeypatch.setattr(integration.dr, "async_get", lambda _hass: device_registry)

        loop.run_until_complete(integration._async_migrate_unique_ids(hass, entry))

        migrated_one = entity_registry.entities[
            "device_tracker.googlefindmy_device_one"
        ]
        migrated_two = entity_registry.entities[
            "device_tracker.googlefindmy_device_two"
        ]
        expected_one = f"{entry.entry_id}:{tracker_subentry.subentry_id}:device-1"
        expected_two = f"{entry.entry_id}:{tracker_subentry.subentry_id}:device-2"

        assert migrated_one.unique_id == expected_one
        assert migrated_two.unique_id == expected_two
        assert (
            entity_registry.async_get_entity_id(
                "device_tracker", integration.DOMAIN, expected_one
            )
            == "device_tracker.googlefindmy_device_one"
        )
        assert (
            entity_registry.async_get_entity_id(
                "device_tracker", integration.DOMAIN, expected_two
            )
            == "device_tracker.googlefindmy_device_two"
        )

        assert entry.options["unique_id_migrated"] is True
        assert entry.options["unique_id_subentry_migrated"] is True
        assert hass.config_entries.entry_update_calls[-1][1]["options"][
            "unique_id_migrated"
        ]
        assert hass.config_entries.entry_update_calls[-1][1]["options"][
            "unique_id_subentry_migrated"
        ]

        entity_registry.updated.clear()

        def _fail_migration(*_: Any, **__: Any) -> None:
            msg = "Migration helpers should not run once options flags are set"
            raise AssertionError(msg)

        monkeypatch.setattr(integration, "_migrate_legacy_unique_ids", _fail_migration)
        monkeypatch.setattr(
            integration, "_migrate_unique_ids_to_subentry", _fail_migration
        )

        loop.run_until_complete(integration._async_migrate_unique_ids(hass, entry))

        assert entity_registry.updated == []
        assert len(hass.config_entries.entry_update_calls) == 1
    finally:
        drain_loop(loop)


def test_hass_data_layout(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """The integration stores runtime state only under hass.data[DOMAIN]["entries"]."""

    loop = asyncio.new_event_loop()

    try:
        if "homeassistant.components.button" not in sys.modules:
            homeassistant_root = sys.modules.get("homeassistant")
            if homeassistant_root is None:
                homeassistant_root = ModuleType("homeassistant")
                homeassistant_root.__path__ = []  # type: ignore[attr-defined]
                sys.modules["homeassistant"] = homeassistant_root

            components_pkg = sys.modules.get("homeassistant.components")
            if components_pkg is None:
                components_pkg = ModuleType("homeassistant.components")
                components_pkg.__path__ = []  # type: ignore[attr-defined]
                sys.modules["homeassistant.components"] = components_pkg

            helpers_pkg = sys.modules.get("homeassistant.helpers")
            if helpers_pkg is None:
                helpers_pkg = ModuleType("homeassistant.helpers")
                helpers_pkg.__path__ = []  # type: ignore[attr-defined]
                sys.modules["homeassistant.helpers"] = helpers_pkg

            setattr(homeassistant_root, "components", components_pkg)
            setattr(homeassistant_root, "helpers", helpers_pkg)

            button_component = ModuleType("homeassistant.components.button")

            class _ButtonEntity:  # pragma: no cover - structural stub
                pass

            class _ButtonEntityDescription:  # pragma: no cover - structural stub
                def __init__(self, **kwargs: Any) -> None:
                    for key, value in kwargs.items():
                        setattr(self, key, value)

            button_component.ButtonEntity = _ButtonEntity
            button_component.ButtonEntityDescription = _ButtonEntityDescription
            sys.modules["homeassistant.components.button"] = button_component
            setattr(components_pkg, "button", button_component)

        if "homeassistant.components.http" not in sys.modules:
            http_component = ModuleType("homeassistant.components.http")

            class _HomeAssistantView:  # pragma: no cover - structural stub
                url: str = ""
                name: str = ""
                requires_auth = False

                def __init__(self, hass=None) -> None:
                    self.hass = hass

            http_component.HomeAssistantView = _HomeAssistantView
            sys.modules["homeassistant.components.http"] = http_component
            components_pkg = sys.modules.get("homeassistant.components")
            if components_pkg is not None:
                setattr(components_pkg, "http", http_component)

        if "homeassistant.loader" not in sys.modules:
            homeassistant_root = sys.modules.setdefault(
                "homeassistant", ModuleType("homeassistant")
            )
            if not hasattr(homeassistant_root, "__path__"):
                homeassistant_root.__path__ = []  # type: ignore[attr-defined]
            loader_module = ModuleType("homeassistant.loader")

            async def _async_get_integration(
                _hass: Any, _domain: str
            ) -> SimpleNamespace:
                return SimpleNamespace(name="googlefindmy", version="0.0.0")

            loader_module.async_get_integration = _async_get_integration
            sys.modules["homeassistant.loader"] = loader_module
            setattr(homeassistant_root, "loader", loader_module)

        helpers_pkg = sys.modules.setdefault(
            "homeassistant.helpers", ModuleType("homeassistant.helpers")
        )
        if not hasattr(helpers_pkg, "__path__"):
            helpers_pkg.__path__ = []  # type: ignore[attr-defined]
        entity_module = sys.modules.get("homeassistant.helpers.entity")
        if entity_module is None:
            entity_module = ModuleType("homeassistant.helpers.entity")
            sys.modules["homeassistant.helpers.entity"] = entity_module
            setattr(helpers_pkg, "entity", entity_module)

        if not hasattr(entity_module, "DeviceInfo"):

            class _DeviceInfo:
                def __init__(self, **kwargs: Any) -> None:
                    for key, value in kwargs.items():
                        setattr(self, key, value)

            entity_module.DeviceInfo = _DeviceInfo

        entity_platform_module = sys.modules.get(
            "homeassistant.helpers.entity_platform"
        )
        if entity_platform_module is None:
            entity_platform_module = ModuleType("homeassistant.helpers.entity_platform")
            sys.modules["homeassistant.helpers.entity_platform"] = (
                entity_platform_module
            )
            setattr(helpers_pkg, "entity_platform", entity_platform_module)

        if not hasattr(entity_platform_module, "AddEntitiesCallback"):
            entity_platform_module.AddEntitiesCallback = Callable[[list[Any]], None]

        class _StubPlatform:
            def async_register_platform_entity_service(
                self, *_: Any, **__: Any
            ) -> None:
                return None

            def async_register_entity_service(self, *_: Any, **__: Any) -> None:
                return None

        monkeypatch.setattr(
            entity_platform_module,
            "async_get_current_platform",
            _StubPlatform,
            raising=False,
        )
        monkeypatch.setattr(
            "homeassistant.helpers.entity_platform.async_get_current_platform",
            _StubPlatform,
            raising=False,
        )

        helpers_pkg = sys.modules.setdefault(
            "homeassistant.helpers", ModuleType("homeassistant.helpers")
        )
        if not hasattr(helpers_pkg, "__path__"):
            helpers_pkg.__path__ = []  # type: ignore[attr-defined]
        update_coordinator_module = sys.modules.get(
            "homeassistant.helpers.update_coordinator"
        )
        if update_coordinator_module is None:
            update_coordinator_module = ModuleType(
                "homeassistant.helpers.update_coordinator"
            )
            sys.modules["homeassistant.helpers.update_coordinator"] = (
                update_coordinator_module
            )
            setattr(helpers_pkg, "update_coordinator", update_coordinator_module)

        if not hasattr(update_coordinator_module, "CoordinatorEntity"):

            class _CoordinatorEntity:
                def __init__(self, coordinator: Any | None = None) -> None:
                    self.coordinator = coordinator

            update_coordinator_module.CoordinatorEntity = _CoordinatorEntity

        if not hasattr(update_coordinator_module, "DataUpdateCoordinator"):

            class _DataUpdateCoordinator:
                def __init__(
                    self,
                    hass: Any,
                    logger: Any | None = None,
                    *,
                    name: str | None = None,
                    update_interval: Any | None = None,
                ) -> None:  # noqa: D401 - stub signature
                    self.hass = hass
                    self.logger = logger
                    self.name = name
                    self.update_interval = update_interval

                async def async_config_entry_first_refresh(self) -> None:
                    return None

            update_coordinator_module.DataUpdateCoordinator = _DataUpdateCoordinator

        if not hasattr(update_coordinator_module, "UpdateFailed"):

            class _UpdateFailed(Exception):
                pass

            update_coordinator_module.UpdateFailed = _UpdateFailed

        config_entries_module = importlib.import_module("homeassistant.config_entries")
        state_cls = config_entries_module.ConfigEntryState
        if not hasattr(state_cls, "SETUP_IN_PROGRESS"):
            setattr(state_cls, "SETUP_IN_PROGRESS", "setup_in_progress")
        if not hasattr(state_cls, "SETUP_RETRY"):
            setattr(state_cls, "SETUP_RETRY", "setup_retry")

        harness = _prepare_async_setup_entry_harness(
            monkeypatch, stub_coordinator_factory, loop
        )
        integration = harness.integration
        button_module = harness.button_module
        map_view_module = harness.map_view_module
        monkeypatch.setattr(
            button_module.entity_platform,
            "async_get_current_platform",
            _StubPlatform,
            raising=False,
        )
        entry = harness.entry
        hass = harness.hass
        cache = harness.cache

        # Recorder history module stub required by the map view handler.
        history_module = ModuleType("homeassistant.components.recorder.history")
        history_module.get_significant_states = lambda *_args, **_kwargs: {}
        sys.modules["homeassistant.components.recorder.history"] = history_module

        async def _exercise() -> None:
            assert await integration.async_setup(hass, {}) is True
            setup_ok = await integration.async_setup_entry(hass, entry)
            assert setup_ok is True

            if hass._tasks:
                await asyncio.gather(*hass._tasks)

            runtime_data = getattr(entry, "runtime_data", None)
            coordinator = getattr(runtime_data, "coordinator", None)
            assert getattr(coordinator, "first_refresh_calls", 0) == 1

            expected_subentries = {
                subentry.subentry_id for subentry in entry.subentries.values()
            }
            assert len(expected_subentries) == len(entry.subentries)

            # Subentry platforms are forwarded once for the parent entry; Home
            # Assistant attaches per-subentry identifiers when it schedules the
            # child platforms.
            assert hass.config_entries.forward_calls
            for forwarded_entry, platform_names in hass.config_entries.forward_calls:
                assert forwarded_entry is entry
                assert set(platform_names) == set(
                    TRACKER_FEATURE_PLATFORMS + SERVICE_FEATURE_PLATFORMS
                )

            domain_bucket = hass.data[DOMAIN]
            assert entry.entry_id not in domain_bucket
            runtime_bucket = domain_bucket["entries"]
            assert entry.entry_id in runtime_bucket

            runtime_data = runtime_bucket[entry.entry_id]
            assert runtime_data is entry.runtime_data
            assert isinstance(runtime_data, integration.RuntimeData)
            assert runtime_data.coordinator is entry.runtime_data.coordinator
            assert runtime_data.token_cache is cache
            assert runtime_data.cache is cache
            assert runtime_data.subentry_manager is not None

            subentry_manager = runtime_data.subentry_manager
            managed = subentry_manager.managed_subentries
            assert TRACKER_SUBENTRY_KEY in managed
            assert SERVICE_SUBENTRY_KEY in managed
            service_subentry = managed[SERVICE_SUBENTRY_KEY]
            core_subentry = managed[TRACKER_SUBENTRY_KEY]
            assert core_subentry.data["group_key"] == TRACKER_SUBENTRY_KEY
            tracker_features = core_subentry.data["features"]
            assert tracker_features == sorted(TRACKER_FEATURE_PLATFORMS)
            assert all(isinstance(feature, str) for feature in tracker_features)
            assert all(feature == feature.lower() for feature in tracker_features)
            assert core_subentry.data["fcm_push_enabled"] is True
            assert core_subentry.data["has_google_home_filter"] is False
            assert core_subentry.unique_id.endswith(TRACKER_SUBENTRY_KEY)

            assert service_subentry.data["group_key"] == SERVICE_SUBENTRY_KEY
            service_features = service_subentry.data["features"]
            assert service_features == sorted(SERVICE_FEATURE_PLATFORMS)
            assert all(isinstance(feature, str) for feature in service_features)
            assert all(feature == feature.lower() for feature in service_features)
            assert service_subentry.data["fcm_push_enabled"] is True
            assert service_subentry.data["has_google_home_filter"] is False
            assert (
                service_subentry.unique_id == f"{entry.entry_id}-{SERVICE_SUBENTRY_KEY}"
            )

            added_entities: list[Any] = []

            def _collect_entities(
                entities: list[Any], _update_before_add: bool = False, **kwargs: Any
            ) -> None:
                del _update_before_add, kwargs
                added_entities.extend(entities)

            await button_module.async_setup_entry(hass, entry, _collect_entities)
            assert not added_entities

            monkeypatch.setattr(
                map_view_module,
                "_resolve_entry_by_token",
                lambda _hass, _token: (entry, {"token"}),
            )

            class _StubEntityRegistry:
                def async_get_entity_id(
                    self, _domain: str, _platform: str, _unique_id: str
                ) -> str | None:
                    return None

                def async_get(self, _entity_id: str) -> Any | None:
                    return None

            monkeypatch.setattr(
                map_view_module.er,
                "async_get",
                lambda _hass: _StubEntityRegistry(),
            )

            view = map_view_module.GoogleFindMyMapView(hass)
            request = SimpleNamespace(query={"token": "token"})
            response = await view.get(request, "device-1")
            assert response.status == 200

            migrate_handler = hass.services.registered[
                (DOMAIN, SERVICE_REBUILD_REGISTRY)
            ]
            await migrate_handler(SimpleNamespace(data={ATTR_MODE: MODE_MIGRATE}))
            assert integration._async_soft_migrate_data_to_options.await_count == 2
            assert integration._async_migrate_unique_ids.await_count == 2
            assert integration._async_relink_button_devices.await_count == 2
            assert integration._async_relink_subentry_entities.await_count == 2
            assert hass.config_entries.reload_calls == [entry.entry_id]

            assert await integration.async_unload_entry(hass, entry) is True
            assert not entry.subentries
            assert not subentry_manager.managed_subentries
            assert hass.config_entries.removed_subentries

        loop.run_until_complete(_exercise())
    finally:
        drain_loop(loop)


def test_setup_entry_reactivates_disabled_button_entities(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Disabled button entities are re-enabled during setup."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        entry = _StubConfigEntry()
        hass = _StubHass(entry, loop)

        dummy_cache = _StubCache()

        async def _fake_create(cls, hass_obj, entry_id, legacy_path=None) -> _StubCache:  # type: ignore[override]
            assert hass_obj is hass
            assert entry_id == entry.entry_id
            return dummy_cache

        monkeypatch.setattr(
            integration.TokenCache,
            "create",
            classmethod(_fake_create),
        )
        monkeypatch.setattr(
            integration,
            "_async_soft_migrate_data_to_options",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            integration,
            "_async_migrate_unique_ids",
            AsyncMock(return_value=None),
        )
        monkeypatch.setattr(
            integration,
            "_register_instance",
            lambda *_: None,
        )
        monkeypatch.setattr(
            integration,
            "_unregister_instance",
            lambda *_: None,
        )

        dummy_fcm = SimpleNamespace(
            register_coordinator=lambda *_: None,
            unregister_coordinator=lambda *_: None,
            _start_listening=AsyncMock(return_value=None),
            request_stop=lambda: None,
        )

        monkeypatch.setattr(
            integration,
            "_async_acquire_shared_fcm",
            AsyncMock(return_value=dummy_fcm),
        )

        class _RegisterViewStub:
            def __init__(self, hass_obj: Any) -> None:
                self.hass = hass_obj

        monkeypatch.setattr(integration, "GoogleFindMyMapView", _RegisterViewStub)
        monkeypatch.setattr(
            integration, "GoogleFindMyMapRedirectView", _RegisterViewStub
        )

        coordinator_cls = stub_coordinator_factory()
        monkeypatch.setattr(integration, "GoogleFindMyCoordinator", coordinator_cls)

        disabled_marker = integration.RegistryEntryDisabler.INTEGRATION

        registry_entries = [
            SimpleNamespace(
                entity_id="button.googlefindmy_disabled",
                platform=DOMAIN,
                domain="button",
                disabled_by=disabled_marker,
                config_entry_id=entry.entry_id,
            ),
            SimpleNamespace(
                entity_id="button.googlefindmy_enabled",
                platform=DOMAIN,
                domain="button",
                disabled_by=None,
                config_entry_id=entry.entry_id,
            ),
            SimpleNamespace(
                entity_id="button.other_integration",
                platform="other",
                domain="button",
                disabled_by=disabled_marker,
                config_entry_id="other-entry",
            ),
        ]

        class _RegistryStub:
            def __init__(self) -> None:
                self.updated: list[str] = []

            def async_update_entity(self, entity_id: str, **changes: Any) -> None:
                self.updated.append(entity_id)
                for entry_obj in registry_entries:
                    if entry_obj.entity_id == entity_id and "disabled_by" in changes:
                        entry_obj.disabled_by = changes["disabled_by"]

        registry = _RegistryStub()

        def _entries_for_config_entry(
            _registry: Any, config_entry_id: str
        ) -> list[SimpleNamespace]:
            assert _registry is registry
            if config_entry_id != entry.entry_id:
                return []
            return list(registry_entries)

        monkeypatch.setattr(integration.er, "async_get", lambda _hass: registry)
        monkeypatch.setattr(
            integration.er,
            "async_entries_for_config_entry",
            _entries_for_config_entry,
            raising=False,
        )

        loop.run_until_complete(integration.async_setup_entry(hass, entry))

        assert registry_entries[0].disabled_by is None
        assert registry.updated == ["button.googlefindmy_disabled"]
    finally:
        drain_loop(loop)


@pytest.mark.asyncio
async def test_async_setup_entry_propagates_subentry_registration(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Yielding before subentry setup lets HA finish registering children."""

    loop = asyncio.get_running_loop()

    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    class _EntityRegistryStub:
        def __init__(self) -> None:
            self.entities: dict[str, Any] = {}

        def async_update_entity(self, *_, **__) -> None:
            return None

    class _DeviceRegistryStub:
        def __init__(self) -> None:
            self.devices: dict[str, Any] = {}

        def async_update_device(self, *_, **__) -> None:
            return None

    entity_registry = _EntityRegistryStub()
    device_registry = _DeviceRegistryStub()
    monkeypatch.setattr(integration.er, "async_get", lambda _hass: entity_registry)
    monkeypatch.setattr(integration.dr, "async_get", lambda _hass: device_registry)

    entry = _StubConfigEntry()
    entry.unique_id = entry.entry_id
    hass = _StubHass(entry, loop)

    forward_calls: list[tuple[str, ...]] = []

    async def _forward_entry_setups(
        entry_obj: _StubConfigEntry,
        platforms: Iterable[Platform],
    ) -> None:
        assert entry_obj is entry
        platform_names = tuple(_platform_value(platform) for platform in platforms)
        forward_calls.append(platform_names)

    hass.config_entries.async_forward_entry_setups = (  # type: ignore[assignment]
        _forward_entry_setups
    )

    ConfigSubentry = importlib.import_module(
        "homeassistant.config_entries"
    ).ConfigSubentry

    class _SubentryManagerStub:
        async def async_add(
            self,
            entry_obj: _StubConfigEntry,
            *,
            subentry_type: str,
            data: Mapping[str, Any],
            title: str,
            unique_id: str,
        ) -> ConfigSubentry:
            subentry = ConfigSubentry(
                data=data,
                subentry_type=subentry_type,
                title=title,
                unique_id=unique_id,
            )
            entry_obj.subentries[subentry.subentry_id] = subentry
            if not getattr(subentry, "config_subentry_id", None):
                setattr(subentry, "config_subentry_id", subentry.subentry_id)
            return subentry

    hass.config_entries.subentries = _SubentryManagerStub()  # type: ignore[attr-defined]

    assert await integration.async_setup(hass, {}) is True
    setup_ok = await integration.async_setup_entry(hass, entry)
    assert setup_ok is True

    if hass._tasks:
        await asyncio.gather(*hass._tasks)

    created_subentries = {
        subentry.subentry_id for subentry in entry.subentries.values()
    }
    assert len(created_subentries) == len(entry.subentries)

    assert forward_calls
    for platform_names in forward_calls:
        assert set(platform_names) == set(
            TRACKER_FEATURE_PLATFORMS + SERVICE_FEATURE_PLATFORMS
        )

    runtime_data = getattr(entry, "runtime_data", None)
    coordinator = getattr(runtime_data, "coordinator", None)
    assert getattr(coordinator, "first_refresh_calls", 0) == 1


@pytest.mark.asyncio
async def test_programmatic_subentry_creation_triggers_setup_and_entities(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Programmatic subentries should trigger setup and create tracker entities."""

    loop = asyncio.get_running_loop()

    def _metadata(
        self: Any, *, key: str | None = None, feature: str | None = None
    ) -> Any:
        resolved_key = (
            key or feature or getattr(self, "_subentry_key", TRACKER_SUBENTRY_KEY)
        )
        device_ids = tuple(device.get("id") for device in getattr(self, "data", ()))
        return SimpleNamespace(
            key=resolved_key,
            config_subentry_id=None,
            features=(feature,) if feature else ("device_tracker",),
            stable_identifier=lambda: resolved_key,
            title=None,
            poll_intervals={},
            filters={},
            feature_flags={},
            visible_device_ids=device_ids,
            enabled_device_ids=device_ids,
        )

    def _stable_identifier(
        self: Any, *, key: str | None = None, feature: str | None = None
    ) -> str:
        return key or feature or getattr(self, "_subentry_key", TRACKER_SUBENTRY_KEY)

    harness = _prepare_async_setup_entry_harness(
        monkeypatch,
        functools.partial(
            stub_coordinator_factory,
            methods={
                "get_subentry_metadata": _metadata,
                "stable_subentry_identifier": _stable_identifier,
            },
        ),
        loop,
    )

    integration = harness.integration
    entry = harness.entry
    hass = harness.hass

    device_registry = FakeDeviceRegistry()
    entity_registry = FakeEntityRegistry()
    monkeypatch.setattr(integration.dr, "async_get", lambda _hass: device_registry)
    monkeypatch.setattr(integration.er, "async_get", lambda _hass: entity_registry)

    from custom_components.googlefindmy import device_tracker

    added_entities: list[tuple[Any, str | None]] = []

    async def _async_add_entities(
        entities: Iterable[Any],
        update_before_add: bool = False,
        *,
        config_subentry_id: str | None = None,
    ) -> None:
        del update_before_add
        for entity in entities:
            added_entities.append((entity, config_subentry_id))
            entity_registry.entities[entity.entity_id] = SimpleNamespace(
                entity_id=entity.entity_id,
                config_entry_id=entry.entry_id,
                config_subentry_id=config_subentry_id,
            )
            device_registry.add_device(
                FakeDeviceEntry(
                    id=entity.device_info.id,
                    identifiers=set(entity.device_info.identifiers),
                    config_entries={entry.entry_id},
                    config_subentry_id=config_subentry_id,
                )
            )

    def _schedule_add_entities(
        hass_obj: _StubHass,
        async_add: Callable[..., Awaitable[None]],
        *,
        entities: Iterable[Any],
        update_before_add: bool,
        config_subentry_id: str | None,
        log_owner: str,
        logger: Any,
    ) -> None:
        del log_owner, logger
        hass_obj.async_create_task(
            async_add(
                list(entities),
                update_before_add=update_before_add,
                config_subentry_id=config_subentry_id,
            ),
            name=f"{DOMAIN}.add_entities.{config_subentry_id}",
        )

    class _StubDeviceTracker:
        def __init__(
            self,
            coordinator: Any,
            device: dict[str, Any],
            *,
            subentry_key: str,
            subentry_identifier: str,
        ) -> None:
            del subentry_key
            device_id = device.get("id", "device")
            self.entity_id = f"device_tracker.{device_id}"
            self._attr_unique_id = (
                f"{coordinator.config_entry.entry_id}:{subentry_identifier}:{device_id}"
            )
            self._device_info = SimpleNamespace(
                id=f"{coordinator.config_entry.entry_id}:{device_id}",
                identifiers={
                    (DOMAIN, f"{coordinator.config_entry.entry_id}:{device_id}")
                },
                config_entries={coordinator.config_entry.entry_id},
                config_subentry_id=subentry_identifier,
            )

        @property
        def unique_id(self) -> str:
            return self._attr_unique_id

        @property
        def device_info(self) -> Any:
            return self._device_info

    class _StubLastLocationTracker:
        def __init__(
            self,
            coordinator: Any,
            device: dict[str, Any],
            *,
            subentry_key: str,
            subentry_identifier: str,
        ) -> None:
            del subentry_key
            device_id = device.get("id", "device")
            self.entity_id = f"device_tracker.{device_id}_last_location"
            self._attr_unique_id = f"{coordinator.config_entry.entry_id}:{subentry_identifier}:{device_id}:last_location"
            self._device_info = SimpleNamespace(
                id=f"{coordinator.config_entry.entry_id}:{device_id}",
                identifiers={
                    (DOMAIN, f"{coordinator.config_entry.entry_id}:{device_id}")
                },
                config_entries={coordinator.config_entry.entry_id},
                config_subentry_id=subentry_identifier,
            )

        @property
        def unique_id(self) -> str:
            return self._attr_unique_id

        @property
        def device_info(self) -> Any:
            return self._device_info

    monkeypatch.setattr(device_tracker, "schedule_add_entities", _schedule_add_entities)
    monkeypatch.setattr(device_tracker, "GoogleFindMyDeviceTracker", _StubDeviceTracker)
    monkeypatch.setattr(
        device_tracker, "GoogleFindMyLastLocationTracker", _StubLastLocationTracker
    )

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry)

    if hass._tasks:
        await asyncio.gather(*hass._tasks)

    initial_subentries = set(entry.subentries)

    tracker_definition = integration.ConfigEntrySubentryDefinition(
        key=TRACKER_SUBENTRY_KEY,
        title="Google Find My devices",
        data={
            "features": sorted(TRACKER_FEATURE_PLATFORMS),
            "fcm_push_enabled": False,
            "has_google_home_filter": False,
            "entry_title": entry.title,
        },
        subentry_type=SUBENTRY_TYPE_TRACKER,
        unique_id=f"{entry.entry_id}-{TRACKER_SUBENTRY_KEY}",
        translation_key=TRACKER_SUBENTRY_TRANSLATION_KEY,
    )

    service_definition = integration.ConfigEntrySubentryDefinition(
        key=SERVICE_SUBENTRY_KEY,
        title="Google Find Hub Service",
        data={
            "features": sorted(SERVICE_FEATURE_PLATFORMS),
            "fcm_push_enabled": False,
            "has_google_home_filter": False,
            "entry_title": entry.title,
        },
        subentry_type=SUBENTRY_TYPE_SERVICE,
        unique_id=f"{entry.entry_id}-{SERVICE_SUBENTRY_KEY}",
        translation_key=SERVICE_SUBENTRY_TRANSLATION_KEY,
    )

    new_tracker_definition = integration.ConfigEntrySubentryDefinition(
        key="dynamic",  # programmatic tracker group created at runtime
        title="Dynamic tracker group",
        data={
            "features": ["device_tracker"],
            "fcm_push_enabled": False,
            "has_google_home_filter": False,
            "entry_title": entry.title,
        },
        subentry_type=SUBENTRY_TYPE_TRACKER,
        unique_id=f"{entry.entry_id}-dynamic",
    )

    await entry.runtime_data.subentry_manager.async_sync(
        [tracker_definition, service_definition, new_tracker_definition]
    )

    created_subentries = set(entry.subentries) - initial_subentries
    assert created_subentries
    new_subentry_id = next(iter(created_subentries))

    assert any(
        "device_tracker" in platforms
        for _, platforms in hass.config_entries.forward_calls
    )

    await device_tracker.async_setup_entry(
        hass,
        entry,
        _async_add_entities,
        config_subentry_id=new_subentry_id,
    )

    if hass._tasks:
        await asyncio.gather(*hass._tasks)

    assert added_entities
    entity_entry = next(iter(entity_registry.entities.values()))
    assert entity_entry.config_entry_id == entry.entry_id
    assert entity_entry.config_subentry_id == new_subentry_id

    registry_entry = next(iter(device_registry.devices.values()))
    assert registry_entry.config_subentry_id == new_subentry_id
    assert entry.entry_id in registry_entry.config_entries


def test_setup_entry_failure_does_not_register_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Setup failures must not leave a TokenCache registered in the facade."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        entry = _StubConfigEntry()
        hass = _StubHass(entry, loop)

        monkeypatch.setattr(
            integration.ir, "async_delete_issue", lambda *args, **kwargs: None
        )
        monkeypatch.setattr(
            integration.ir, "async_create_issue", lambda *args, **kwargs: None
        )
        monkeypatch.setattr(
            integration,
            "_async_migrate_unique_ids",
            AsyncMock(return_value=None),
        )

        dummy_cache = _StubCache()

        async def _fake_create(cls, hass_obj, entry_id, legacy_path=None) -> _StubCache:  # type: ignore[override]
            assert hass_obj is hass
            assert entry_id == entry.entry_id
            return dummy_cache

        monkeypatch.setattr(
            integration.TokenCache,
            "create",
            classmethod(_fake_create),
        )

        register_calls: list[tuple[str, Any]] = []
        monkeypatch.setattr(
            integration,
            "_register_instance",
            lambda entry_id, cache: register_calls.append((entry_id, cache)),
        )

        dummy_fcm = SimpleNamespace(
            register_coordinator=lambda *_: None,
            unregister_coordinator=lambda *_: None,
            _start_listening=AsyncMock(return_value=None),
            request_stop=lambda: None,
        )
        monkeypatch.setattr(
            integration,
            "_async_acquire_shared_fcm",
            AsyncMock(return_value=dummy_fcm),
        )

        def _boom(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("boom during coordinator init")

        monkeypatch.setattr(integration, "GoogleFindMyCoordinator", _boom)

        with pytest.raises(RuntimeError):
            loop.run_until_complete(integration.async_setup_entry(hass, entry))

        assert register_calls == []
    finally:
        drain_loop(loop)


def test_duplicate_account_issue_translated(monkeypatch: pytest.MonkeyPatch) -> None:
    """A duplicate-account repair issue renders with translated placeholders."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        existing_entry = _StubConfigEntry()
        existing_entry.entry_id = "entry-existing"
        existing_entry.title = "Primary Account"
        existing_entry.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        existing_entry.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"
        existing_entry.updated_at = datetime(2024, 2, 2, 0, 0, 0)

        new_entry = _StubConfigEntry()
        new_entry.entry_id = "entry-new"
        new_entry.title = "Duplicate Account"
        new_entry.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        new_entry.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"

        legacy_duplicate = _StubConfigEntry()
        legacy_duplicate.entry_id = "entry-legacy"
        legacy_duplicate.title = "Legacy Duplicate"
        legacy_duplicate.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        legacy_duplicate.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"

        hass = _StubHass(new_entry, loop)
        hass.config_entries._entries.extend([existing_entry, legacy_duplicate])

        async def _legacy_set_disabled_by(
            self: _StubConfigEntries, entry_id: str, disabled_by: object | None
        ) -> None:
            raise TypeError("async_set_disabled_by is not supported")

        monkeypatch.setattr(
            hass.config_entries.__class__,
            "async_set_disabled_by",
            _legacy_set_disabled_by,
        )

        recorded_issues: list[dict[str, Any]] = []

        monkeypatch.setattr(
            integration.ir, "async_delete_issue", lambda *_, **__: None, raising=False
        )

        def _record_issue(
            _hass: Any, _domain: str, issue_id: str, **kwargs: Any
        ) -> None:
            recorded_issues.append({"id": issue_id, **kwargs})

        monkeypatch.setattr(
            integration.ir, "async_create_issue", _record_issue, raising=False
        )

        async def _exercise() -> bool:
            return await integration.async_setup_entry(hass, new_entry)

        result = loop.run_until_complete(_exercise())
        assert result is False
        assert recorded_issues, "Expected duplicate-account issue to be recorded"
        issue = recorded_issues[0]
        assert issue["id"] == f"duplicate_account_{legacy_duplicate.entry_id}"
        placeholders = issue.get("translation_placeholders", {})
        assert placeholders.get("email") == "dup@example.com"
        entries_placeholder = str(placeholders.get("entries", ""))
        assert existing_entry.entry_id in entries_placeholder
        assert legacy_duplicate.entry_id in entries_placeholder
        assert placeholders.get("cause") == "setup_duplicate"

        issue = recorded_issues[-1]
        assert issue["translation_key"] == "duplicate_account_entries"
        placeholders = issue["translation_placeholders"]
        assert placeholders["email"] == "dup@example.com"
        assert "Primary Account" in placeholders["entries"]

        translation = json.loads(
            Path("custom_components/googlefindmy/translations/en.json").read_text(
                encoding="utf-8"
            )
        )
        template = translation["issues"]["duplicate_account_entries"]["description"]
        rendered = template.format(**placeholders)
        assert "dup@example.com" in rendered
        assert "Primary Account" in rendered
    finally:
        loop.close()


def test_duplicate_account_issue_cleanup_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolved duplicate-account issues are cleared during normal setup."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        config_entries_module = importlib.import_module("homeassistant.config_entries")
        state_cls = config_entries_module.ConfigEntryState
        if not hasattr(state_cls, "SETUP_IN_PROGRESS"):
            setattr(state_cls, "SETUP_IN_PROGRESS", "setup_in_progress")
        if not hasattr(state_cls, "SETUP_RETRY"):
            setattr(state_cls, "SETUP_RETRY", "setup_retry")

        entry = _StubConfigEntry()
        hass = _StubHass(entry, loop)

        delete_calls: list[tuple[Any, str, str]] = []

        def _delete_issue(hass_arg: Any, domain: str, issue_id: str, **_: Any) -> None:
            delete_calls.append((hass_arg, domain, issue_id))

        monkeypatch.setattr(
            integration.ir, "async_delete_issue", _delete_issue, raising=False
        )

        create_calls: list[tuple[Any, str, str]] = []

        def _record_create(hass_arg: Any, domain: str, issue_id: str, **_: Any) -> None:
            create_calls.append((hass_arg, domain, issue_id))

        monkeypatch.setattr(
            integration.ir, "async_create_issue", _record_create, raising=False
        )

        def _fail_domain_data(_hass: Any) -> None:
            raise RuntimeError("stop after duplicate cleanup")

        monkeypatch.setattr(integration, "_domain_data", _fail_domain_data)

        with pytest.raises(RuntimeError):
            loop.run_until_complete(integration.async_setup_entry(hass, entry))

        issue_ids = [issue_id for *_hass, _domain, issue_id in delete_calls]
        assert f"duplicate_account_{entry.entry_id}" in issue_ids, (
            "Expected duplicate-account cleanup to delete stale issue"
        )
        cleanup_index = issue_ids.index(f"duplicate_account_{entry.entry_id}")
        cleanup_call = delete_calls[cleanup_index]
        assert cleanup_call[0] is hass
        assert cleanup_call[1] == DOMAIN
        assert create_calls == []
    finally:
        loop.close()


def test_duplicate_account_mixed_states_prefer_loaded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Loaded duplicates remain authoritative; others auto-disable and clean up."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        config_entries_module = importlib.import_module("homeassistant.config_entries")
        state_cls = config_entries_module.ConfigEntryState
        if not hasattr(state_cls, "SETUP_RETRY"):
            setattr(state_cls, "SETUP_RETRY", "setup_retry")

        loaded_entry = _StubConfigEntry()
        loaded_entry.entry_id = "entry-loaded"
        loaded_entry.title = "Loaded Account"
        loaded_entry.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        loaded_entry.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"
        loaded_entry.state = ConfigEntryState.LOADED
        loaded_entry.updated_at = datetime(2024, 1, 2, 12, 0, 0)

        retry_entry = _StubConfigEntry()
        retry_entry.entry_id = "entry-retry"
        retry_entry.title = "Retry Account"
        retry_entry.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        retry_entry.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"
        retry_entry.state = getattr(
            ConfigEntryState, "SETUP_RETRY", ConfigEntryState.NOT_LOADED
        )
        retry_entry.updated_at = datetime(2024, 1, 3, 12, 0, 0)

        create_calls: list[tuple[Any, str, str, dict[str, Any]]] = []
        delete_calls: list[tuple[Any, str, str]] = []

        def _record_create(
            hass_arg: Any, domain: str, issue_id: str, **kwargs: Any
        ) -> None:
            create_calls.append((hass_arg, domain, issue_id, kwargs))

        def _record_delete(hass_arg: Any, domain: str, issue_id: str, **_: Any) -> None:
            delete_calls.append((hass_arg, domain, issue_id))

        monkeypatch.setattr(
            integration.ir, "async_create_issue", _record_create, raising=False
        )
        monkeypatch.setattr(
            integration.ir, "async_delete_issue", _record_delete, raising=False
        )

        hass_loaded = _StubHass(loaded_entry, loop)
        hass_loaded.config_entries._entries.append(retry_entry)

        should_setup_loaded, normalized_email = loop.run_until_complete(
            integration._ensure_post_migration_consistency(  # type: ignore[attr-defined]
                hass_loaded,
                loaded_entry,
            )
        )
        assert should_setup_loaded is True
        assert normalized_email == "dup@example.com"

        if hass_loaded._tasks:
            loop.run_until_complete(asyncio.gather(*hass_loaded._tasks))

        delete_issue_ids = [issue_id for *_hass, _domain, issue_id in delete_calls]
        assert f"duplicate_account_{loaded_entry.entry_id}" in delete_issue_ids, (
            "Authoritative entry should clear its repair issue"
        )
        assert f"duplicate_account_{retry_entry.entry_id}" in delete_issue_ids, (
            "Duplicate entry repair issue should be cleared after auto-disable"
        )

        assert (
            hass_loaded.config_entries.unload_calls.count(retry_entry.entry_id) >= 1
        ), "Duplicate entry should be unloaded when disabled"
        assert not create_calls, "Auto-disabled duplicates must not raise new issues"
        assert "integration" in str(retry_entry.disabled_by).lower()

        create_calls.clear()
        delete_calls.clear()

        hass_retry = _StubHass(retry_entry, loop)
        hass_retry.config_entries._entries.append(loaded_entry)

        should_setup_retry, normalized_retry = loop.run_until_complete(
            integration._ensure_post_migration_consistency(  # type: ignore[attr-defined]
                hass_retry,
                retry_entry,
            )
        )
        assert should_setup_retry is False
        assert normalized_retry == "dup@example.com"
        assert "integration" in str(retry_entry.disabled_by).lower()
        assert not create_calls, "Duplicate should remain disabled without new issues"

        if hass_retry._tasks:
            loop.run_until_complete(asyncio.gather(*hass_retry._tasks))

        delete_issue_ids = [issue_id for *_hass, _domain, issue_id in delete_calls]
        assert f"duplicate_account_{retry_entry.entry_id}" in delete_issue_ids, (
            "Duplicate entry issues should stay cleared"
        )
    finally:
        loop.close()


def test_duplicate_account_auto_disables_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-authoritative entries are disabled, unloaded, and cleaned up."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        authoritative = _StubConfigEntry()
        authoritative.entry_id = "entry-authoritative"
        authoritative.title = "Authoritative"
        authoritative.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        authoritative.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"
        authoritative.state = ConfigEntryState.LOADED

        duplicate_loaded = _StubConfigEntry()
        duplicate_loaded.entry_id = "entry-duplicate-loaded"
        duplicate_loaded.title = "Loaded Duplicate"
        duplicate_loaded.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        duplicate_loaded.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"
        duplicate_loaded.state = ConfigEntryState.LOADED

        duplicate_error = _StubConfigEntry()
        duplicate_error.entry_id = "entry-duplicate-error"
        duplicate_error.title = "Error Duplicate"
        duplicate_error.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        duplicate_error.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"
        duplicate_error.state = getattr(
            ConfigEntryState, "SETUP_ERROR", ConfigEntryState.LOADED
        )

        duplicate_user = _StubConfigEntry()
        duplicate_user.entry_id = "entry-duplicate-user"
        duplicate_user.title = "User Disabled"
        duplicate_user.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        duplicate_user.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"
        duplicate_user.state = ConfigEntryState.LOADED
        duplicate_user.disabled_by = "user"

        create_calls: list[tuple[Any, str, str, dict[str, Any]]] = []
        delete_calls: list[tuple[Any, str, str]] = []

        def _record_create(
            hass_arg: Any, domain: str, issue_id: str, **kwargs: Any
        ) -> None:
            create_calls.append((hass_arg, domain, issue_id, kwargs))

        def _record_delete(hass_arg: Any, domain: str, issue_id: str, **_: Any) -> None:
            delete_calls.append((hass_arg, domain, issue_id))

        monkeypatch.setattr(
            integration.ir, "async_create_issue", _record_create, raising=False
        )
        monkeypatch.setattr(
            integration.ir, "async_delete_issue", _record_delete, raising=False
        )

        hass = _StubHass(authoritative, loop)
        hass.config_entries._entries.extend(
            [duplicate_loaded, duplicate_error, duplicate_user]
        )

        should_setup, normalized_email = loop.run_until_complete(
            integration._ensure_post_migration_consistency(  # type: ignore[attr-defined]
                hass,
                authoritative,
            )
        )
        assert should_setup is True
        assert normalized_email == "dup@example.com"

        if hass._tasks:
            loop.run_until_complete(asyncio.gather(*hass._tasks))

        assert "integration" in str(duplicate_loaded.disabled_by).lower()
        assert "integration" in str(duplicate_error.disabled_by).lower()
        assert "user" in str(duplicate_user.disabled_by).lower()

        for duplicate in (duplicate_loaded, duplicate_error, duplicate_user):
            assert hass.config_entries.unload_calls.count(duplicate.entry_id) >= 1, (
                "Every duplicate should be unloaded"
            )

        delete_issue_ids = [issue_id for *_hass, _domain, issue_id in delete_calls]
        assert f"duplicate_account_{duplicate_loaded.entry_id}" in delete_issue_ids
        assert f"duplicate_account_{duplicate_error.entry_id}" in delete_issue_ids
        assert f"duplicate_account_{duplicate_user.entry_id}" in delete_issue_ids
        assert f"duplicate_account_{authoritative.entry_id}" in delete_issue_ids

        assert not create_calls
    finally:
        loop.close()


def test_duplicate_account_legacy_core_disable_fallback(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Legacy cores raise TypeError but still unload and raise repair issues."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        authoritative = _StubConfigEntry()
        authoritative.entry_id = "entry-authoritative"
        authoritative.title = "Authoritative"
        authoritative.data[CONF_GOOGLE_EMAIL] = "legacy@example.com"
        authoritative.data[DATA_SECRET_BUNDLE]["username"] = "legacy@example.com"
        authoritative.state = ConfigEntryState.LOADED

        duplicate_legacy = _StubConfigEntry()
        duplicate_legacy.entry_id = "entry-duplicate-legacy"
        duplicate_legacy.title = "Legacy Duplicate"
        duplicate_legacy.data[CONF_GOOGLE_EMAIL] = "legacy@example.com"
        duplicate_legacy.data[DATA_SECRET_BUNDLE]["username"] = "legacy@example.com"
        duplicate_legacy.state = ConfigEntryState.LOADED

        hass = _StubHass(authoritative, loop)
        hass.config_entries._entries.append(duplicate_legacy)

        async def _legacy_set_disabled_by(
            self: _StubConfigEntries, entry_id: str, disabled_by: object | None
        ) -> None:
            raise TypeError("async_set_disabled_by is not supported")

        monkeypatch.setattr(
            hass.config_entries.__class__,
            "async_set_disabled_by",
            _legacy_set_disabled_by,
        )

        create_calls: list[tuple[Any, str, str, dict[str, Any]]] = []
        delete_calls: list[tuple[Any, str, str]] = []

        def _record_create(
            hass_arg: Any, domain: str, issue_id: str, **kwargs: Any
        ) -> None:
            create_calls.append((hass_arg, domain, issue_id, kwargs))

        def _record_delete(hass_arg: Any, domain: str, issue_id: str, **_: Any) -> None:
            delete_calls.append((hass_arg, domain, issue_id))

        monkeypatch.setattr(
            integration.ir, "async_create_issue", _record_create, raising=False
        )
        monkeypatch.setattr(
            integration.ir, "async_delete_issue", _record_delete, raising=False
        )

        caplog.set_level(logging.INFO)

        should_setup, normalized_email = loop.run_until_complete(
            integration._ensure_post_migration_consistency(  # type: ignore[attr-defined]
                hass,
                authoritative,
            )
        )
        assert should_setup is True
        assert normalized_email == "legacy@example.com"

        if hass._tasks:
            loop.run_until_complete(asyncio.gather(*hass._tasks))

        assert hass.config_entries.unload_calls.count(duplicate_legacy.entry_id) >= 1
        assert duplicate_legacy.disabled_by is None

        warning_messages = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.WARNING
        ]
        assert any(
            "could not be disabled via API" in message for message in warning_messages
        ), "Fallback warning should be emitted for legacy disable handling"

        info_messages = [
            record.getMessage()
            for record in caplog.records
            if record.levelno == logging.INFO
        ]
        assert any(
            "manual_action_required=['entry-duplicate-legacy']" in message
            for message in info_messages
        ), "Manual action list should log the legacy duplicate entry"

        create_issue_ids = [
            issue_id for *_hass, _domain, issue_id, _kwargs in create_calls
        ]
        assert f"duplicate_account_{duplicate_legacy.entry_id}" in create_issue_ids, (
            "Repair issue should be created for the legacy duplicate"
        )

        delete_issue_ids = [issue_id for *_hass, _domain, issue_id in delete_calls]
        assert (
            f"duplicate_account_{duplicate_legacy.entry_id}" not in delete_issue_ids
        ), "Legacy duplicate issues should remain open for manual action"
    finally:
        loop.close()


def test_duplicate_account_all_not_loaded_prefers_newest_timestamp() -> None:
    """Among inactive duplicates, the freshest update wins."""

    integration = importlib.import_module("custom_components.googlefindmy")

    primary_entry = _StubConfigEntry()
    primary_entry.entry_id = "entry-primary"
    primary_entry.state = ConfigEntryState.NOT_LOADED
    primary_entry.updated_at = datetime(2024, 1, 1, 12, 0, 0)
    primary_entry.created_at = datetime(2024, 1, 1, 11, 0, 0)

    newer_entry = _StubConfigEntry()
    newer_entry.entry_id = "entry-newer"
    newer_entry.state = ConfigEntryState.NOT_LOADED
    newer_entry.updated_at = datetime(2024, 1, 2, 12, 0, 0)
    newer_entry.created_at = datetime(2024, 1, 2, 11, 0, 0)

    authoritative = integration._select_authoritative_entry_id(  # type: ignore[attr-defined]
        primary_entry,
        [newer_entry],
    )
    assert authoritative == "entry-newer"


def test_duplicate_account_tie_breaker_by_entry_id() -> None:
    """Equal states and timestamps fall back to entry_id ordering."""

    integration = importlib.import_module("custom_components.googlefindmy")

    candidate_a = _StubConfigEntry()
    candidate_a.entry_id = "entry-a"
    candidate_a.state = ConfigEntryState.NOT_LOADED
    candidate_a.updated_at = datetime(2024, 1, 2, 12, 0, 0)
    candidate_a.created_at = datetime(2024, 1, 1, 12, 0, 0)

    candidate_b = _StubConfigEntry()
    candidate_b.entry_id = "entry-b"
    candidate_b.state = ConfigEntryState.NOT_LOADED
    candidate_b.updated_at = datetime(2024, 1, 2, 12, 0, 0)
    candidate_b.created_at = datetime(2024, 1, 1, 12, 0, 0)

    authoritative = integration._select_authoritative_entry_id(  # type: ignore[attr-defined]
        candidate_b,
        [candidate_a],
    )
    assert authoritative == "entry-a"


def test_duplicate_account_clear_stale_issues_for_all() -> None:
    """When duplicates are gone, all related issues are purged."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        entry = _StubConfigEntry()
        entry.entry_id = "entry-authoritative"
        entry.data[CONF_GOOGLE_EMAIL] = "solo@example.com"
        entry.data[DATA_SECRET_BUNDLE]["username"] = "solo@example.com"

        hass = _StubHass(entry, loop)

        registry = integration.ir.async_get(hass)
        registry.async_create_issue(  # type: ignore[attr-defined]
            DOMAIN,
            "duplicate_account_entry-removed",
            translation_key="duplicate_account_entries",
            translation_placeholders={"email": "solo@example.com"},
        )

        loop.run_until_complete(
            integration._ensure_post_migration_consistency(  # type: ignore[attr-defined]
                hass,
                entry,
            )
        )

        assert (
            registry.async_get_issue(  # type: ignore[attr-defined]
                DOMAIN, "duplicate_account_entry-removed"
            )
            is None
        )
    finally:
        loop.close()


def test_duplicate_account_cleanup_keeps_active_tuple_key_issues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only stale duplicate-account issues are removed for tuple-key registries."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        authoritative = _StubConfigEntry()
        authoritative.entry_id = "entry-authoritative"
        authoritative.state = ConfigEntryState.LOADED
        email = "duo@example.com"
        authoritative.data[CONF_GOOGLE_EMAIL] = email
        authoritative.data[DATA_SECRET_BUNDLE]["username"] = email

        duplicate = _StubConfigEntry()
        duplicate.entry_id = "entry-duplicate"
        duplicate.state = ConfigEntryState.NOT_LOADED
        duplicate.data[CONF_GOOGLE_EMAIL] = email
        duplicate.data[DATA_SECRET_BUNDLE]["username"] = email

        hass = _StubHass(authoritative, loop)
        hass.config_entries._entries.append(duplicate)

        async def _legacy_set_disabled_by(
            self: _StubConfigEntries, entry_id: str, disabled_by: object | None
        ) -> None:
            raise TypeError("async_set_disabled_by is not supported")

        monkeypatch.setattr(
            hass.config_entries.__class__,
            "async_set_disabled_by",
            _legacy_set_disabled_by,
        )

        registry = integration.ir.async_get(hass)
        registry.async_create_issue(  # type: ignore[attr-defined]
            DOMAIN,
            "duplicate_account_entry-stale",
            translation_placeholders={"email": email},
        )
        registry.async_create_issue(  # type: ignore[attr-defined]
            DOMAIN,
            f"duplicate_account_{duplicate.entry_id}",
            translation_placeholders={"email": email},
        )

        should_setup, normalized_email = loop.run_until_complete(
            integration._ensure_post_migration_consistency(  # type: ignore[attr-defined]
                hass,
                authoritative,
            )
        )

        assert should_setup is True
        assert normalized_email == email
        assert (
            registry.async_get_issue(  # type: ignore[attr-defined]
                DOMAIN,
                "duplicate_account_entry-stale",
            )
            is None
        )
        active_issue = registry.async_get_issue(  # type: ignore[attr-defined]
            DOMAIN,
            f"duplicate_account_{duplicate.entry_id}",
        )
        assert active_issue is not None
        placeholders = active_issue.get("translation_placeholders", {})
        assert placeholders.get("email") == email
        assert authoritative.entry_id in str(placeholders.get("entries", ""))
    finally:
        loop.close()


def test_duplicate_account_cleanup_respects_string_key_issue_registries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stale cleanup handles registries that expose string-key issue mappings."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        class _StringKeyIssueRegistry:
            def __init__(self) -> None:
                self.issues: dict[str, dict[str, Any]] = {}

            def async_get_issue(
                self, domain: str, issue_id: str
            ) -> dict[str, Any] | None:
                issue = self.issues.get(issue_id)
                if issue and issue.get("domain") == domain:
                    return issue
                return None

            def async_create_issue(
                self,
                domain: str,
                issue_id: str,
                **data: Any,
            ) -> None:
                self.issues[issue_id] = {
                    **data,
                    "domain": domain,
                    "issue_id": issue_id,
                }

            def async_delete_issue(self, domain: str, issue_id: str) -> None:
                self.issues.pop(issue_id, None)

        registry = _StringKeyIssueRegistry()

        monkeypatch.setattr(
            integration.ir,
            "async_get",
            lambda hass: registry,
        )
        monkeypatch.setattr(
            integration.ir,
            "async_create_issue",
            lambda hass, domain, issue_id, **data: registry.async_create_issue(
                domain, issue_id, **data
            ),
        )
        monkeypatch.setattr(
            integration.ir,
            "async_delete_issue",
            lambda hass, domain, issue_id: registry.async_delete_issue(
                domain, issue_id
            ),
        )

        authoritative = _StubConfigEntry()
        authoritative.entry_id = "string-key-authoritative"
        authoritative.state = ConfigEntryState.LOADED
        email = "mapped@example.com"
        authoritative.data[CONF_GOOGLE_EMAIL] = email
        authoritative.data[DATA_SECRET_BUNDLE]["username"] = email

        duplicate = _StubConfigEntry()
        duplicate.entry_id = "string-key-duplicate"
        duplicate.state = ConfigEntryState.NOT_LOADED
        duplicate.data[CONF_GOOGLE_EMAIL] = email
        duplicate.data[DATA_SECRET_BUNDLE]["username"] = email

        hass = _StubHass(authoritative, loop)
        hass.config_entries._entries.append(duplicate)

        async def _legacy_set_disabled_by(
            self: _StubConfigEntries, entry_id: str, disabled_by: object | None
        ) -> None:
            raise TypeError("async_set_disabled_by is not supported")

        monkeypatch.setattr(
            hass.config_entries.__class__,
            "async_set_disabled_by",
            _legacy_set_disabled_by,
        )

        registry.async_create_issue(
            DOMAIN,
            "duplicate_account_retired-entry",
            translation_placeholders={"email": email},
        )
        registry.async_create_issue(
            DOMAIN,
            f"duplicate_account_{duplicate.entry_id}",
            translation_placeholders={"email": email},
        )

        should_setup, normalized_email = loop.run_until_complete(
            integration._ensure_post_migration_consistency(  # type: ignore[attr-defined]
                hass,
                authoritative,
            )
        )

        assert should_setup is True
        assert normalized_email == email
        assert (
            registry.async_get_issue(
                DOMAIN,
                "duplicate_account_retired-entry",
            )
            is None
        )
        active_issue = registry.async_get_issue(
            DOMAIN,
            f"duplicate_account_{duplicate.entry_id}",
        )
        assert active_issue is not None
        placeholders = active_issue.get("translation_placeholders", {})
        assert placeholders.get("email") == email
        assert authoritative.entry_id in str(placeholders.get("entries", ""))
    finally:
        loop.close()


def test_issue_exists_helper_is_synchronous() -> None:
    """_issue_exists interacts with the registry helpers without awaiting."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        entry = _StubConfigEntry()
        hass = _StubHass(entry, loop)

        assert (
            integration._issue_exists(  # type: ignore[attr-defined]
                hass,
                "missing_issue",
            )
            is False
        )
        registry = integration.ir.async_get(hass)
        registry.async_create_issue(  # type: ignore[attr-defined]
            DOMAIN,
            "duplicate_account_entry-test",
            is_fixable=False,
            severity="warning",
            translation_key="duplicate_account_entries",
            translation_placeholders={"email": "user@example.com"},
        )
        assert (
            integration._issue_exists(  # type: ignore[attr-defined]
                hass,
                "duplicate_account_entry-test",
            )
            is True
        )
    finally:
        loop.close()


def test_duplicate_account_issue_log_level_downgrades_when_existing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Existing repair issues cause duplicate detection logs to drop to DEBUG."""

    loop = asyncio.new_event_loop()

    try:
        integration = importlib.import_module("custom_components.googlefindmy")

        entry = _StubConfigEntry()
        entry.entry_id = "entry-dup"
        entry.data[CONF_GOOGLE_EMAIL] = "dup@example.com"
        entry.data[DATA_SECRET_BUNDLE]["username"] = "dup@example.com"

        hass = _StubHass(entry, loop)

        caplog.set_level(logging.DEBUG)

        caplog.clear()
        integration._log_duplicate_and_raise_repair_issue(  # type: ignore[attr-defined]
            hass,
            entry,
            "dup@example.com",
            cause="setup_duplicate",
            conflicts=[],
        )
        warning_records = [
            record
            for record in caplog.records
            if "duplicate account" in record.getMessage()
        ]
        assert warning_records
        assert warning_records[-1].levelno == logging.WARNING

        caplog.clear()
        integration._log_duplicate_and_raise_repair_issue(  # type: ignore[attr-defined]
            hass,
            entry,
            "dup@example.com",
            cause="setup_duplicate",
            conflicts=[],
        )
        debug_records = [
            record
            for record in caplog.records
            if "duplicate account" in record.getMessage()
        ]
        assert debug_records
        assert debug_records[-1].levelno == logging.DEBUG
    finally:
        loop.close()


def test_service_no_active_entry_placeholders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Service validation exposes counts/list placeholders for inactive setups."""

    loop = asyncio.new_event_loop()

    try:
        services_module = importlib.import_module(
            "custom_components.googlefindmy.services"
        )

        class _StrictServiceValidationError(Exception):
            def __init__(
                self,
                *args: Any,
                translation_domain: str | None = None,
                translation_key: str | None = None,
                translation_placeholders: Mapping[str, Any] | None = None,
            ) -> None:
                message = args[0] if args else "Service validation error"
                super().__init__(message)
                self.translation_domain = translation_domain
                self.translation_key = translation_key
                self.translation_placeholders = (
                    None
                    if translation_placeholders is None
                    else dict(translation_placeholders)
                )

        monkeypatch.setattr(
            services_module, "ServiceValidationError", _StrictServiceValidationError
        )
        monkeypatch.setattr(
            sys.modules[__name__],
            "ServiceValidationError",
            _StrictServiceValidationError,
        )

        entries = [
            SimpleNamespace(title="Account One", entry_id="entry-1", active=False),
            SimpleNamespace(title="Account Two", entry_id="entry-2", active=False),
        ]

        class _ConfigEntriesStub(ConfigEntriesDomainUniqueIdLookupMixin):
            def async_entries(self, domain: str) -> list[Any]:
                assert domain == DOMAIN
                return list(entries)

        hass = SimpleNamespace(
            data={},
            services=FakeServiceRegistry(),
            config_entries=_ConfigEntriesStub(),
        )

        ctx: dict[str, Any] = {
            "domain": DOMAIN,
            "resolve_canonical": lambda _hass, device_id: (device_id, device_id),
            "is_active_entry": lambda entry: getattr(entry, "active", False),
            "primary_active_entry": lambda entries_list: None,
            "opt": lambda entry, key, default=None: default,
            "default_map_view_token_expiration": False,
            "opt_map_view_token_expiration_key": "map_view_token_expiration",
            "redact_url_token": lambda token: token,
            "soft_migrate_entry": AsyncMock(),
        }

        loop.run_until_complete(services_module.async_register_services(hass, ctx))

        handler = hass.services.registered[(DOMAIN, SERVICE_LOCATE_DEVICE)]
        call = FakeServiceCall({"device_id": "device-1"})

        with pytest.raises(ServiceValidationError) as errinfo:
            loop.run_until_complete(handler(call))

        error = errinfo.value
        placeholders = error.translation_placeholders
        assert placeholders["active_count"] == "0"
        assert placeholders["total_count"] == "2"
        assert "Account One" in placeholders["entries"]

        translation = json.loads(
            Path("custom_components/googlefindmy/translations/en.json").read_text(
                encoding="utf-8"
            )
        )
        template = translation["exceptions"]["no_active_entry"]["message"]
        rendered = template.format(**placeholders)
        assert "0/2" in rendered
        assert "Account One" in rendered
    finally:
        loop.close()


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


def _make_latch_watching_entry(
    integration: ModuleType, hass: Any, recording: list[bool], seen: list[bool]
) -> _StubConfigEntry:
    """Return an entry stub that samples the reload latch on every ``data`` read.

    Deliberately samples the **state** of the latch rather than watching for a
    call to ``discard_pending_entry_reload``. Watching the call would pin the
    name of a helper: a behaviour-preserving refactor that inlines the release,
    or renames it, would fail the assertion although the invariant holds. The
    state survives both.
    """

    class _LatchWatchingEntry(_StubConfigEntry):
        @property
        def data(self) -> dict[str, Any]:
            if recording[0]:
                bucket = hass.data.get(integration.DOMAIN, {})
                pending = bucket.get("pending_entry_reloads", set())
                seen.append(self.entry_id in pending)
            return self._data

        @data.setter
        def data(self, value: dict[str, Any]) -> None:
            self._data = value

    return _LatchWatchingEntry()


@pytest.mark.asyncio
async def test_the_setup_releases_the_reload_latch_before_it_reads_the_entry_data(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """The release has to be the first act of the setup half, not merely one of them.

    Two independent reasons, both from the production code:

    First, for an entry that was not loaded Home Assistant's ``async_reload``
    skips the unload half entirely, so this release is then the **only** one that
    runs. A latch nobody releases swallows every later reload of that entry.

    Second, it is the reason a writer that finds the latch taken may stand down
    without losing its change. Standing down is safe because the reload holding
    the latch has not finished its unload half -- that release sits in the
    ``finally`` at its very end -- so the replacement setup still reads what the
    stander-down wrote. Move this release behind the first entry read and the
    guarantee inverts: the window in which a writer sees a free latch now
    overlaps a setup that has already read the entry, and the value written into
    it goes nowhere until something else reloads.

    Scope, stated so the name is not read as more than it is: only ``entry.data``
    is instrumented. ``entry.entry_id`` is by construction read earlier -- it is
    the argument of the release call itself.

    The sibling ``test_the_reload_latch_survives_the_unload_phase`` pins the other
    end of the same invariant, deliberately in the opposite direction.
    """

    loop = asyncio.get_running_loop()

    recording = [False]
    latch_held_at_read: list[bool] = []

    integration = importlib.import_module("custom_components.googlefindmy")
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    hass = harness.hass
    entry = _make_latch_watching_entry(integration, hass, recording, latch_held_at_read)
    entry._attach_hass(hass)
    hass.config_entries._entries = [entry]

    entry.data[DATA_AAS_TOKEN] = "aas_et/OLD_TOKEN_VALUE"
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True

    # The claimed reload arrives at its setup half.
    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True

    recording[0] = True
    try:
        assert await integration.async_setup_entry(hass, entry) is True
    finally:
        recording[0] = False

    assert latch_held_at_read, (
        "the setup has to read the entry data at all -- without a read the "
        "assertion below would hold vacuously and pin nothing"
    )
    assert latch_held_at_read[0] is False, (
        "the latch was still held when the setup first read the entry data; the "
        "release has to run before that, not somewhere inside the setup"
    )

    pending = hass.data[integration.DOMAIN]["pending_entry_reloads"]
    assert entry.entry_id not in pending, (
        "the arrived reload has to leave the latch free for the next change"
    )


@pytest.mark.asyncio
async def test_the_subentry_branch_is_behind_the_reload_latch_release(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """The early exit for a subentry must not overtake the release either.

    ``async_setup_entry`` routes a subentry away after two lines. A release moved
    just one step down -- behind that ``if`` -- would leave every subentry setup
    with a latch it never hands back, and the previous test would stay green
    because it never takes this branch. Unreachable today (claims are made for
    parent entries), which is exactly why nothing but a test keeps it that way.
    """

    loop = asyncio.get_running_loop()

    recording = [False]
    latch_held_at_read: list[bool] = []

    integration = importlib.import_module("custom_components.googlefindmy")
    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, loop
    )
    hass = harness.hass
    entry = _make_latch_watching_entry(integration, hass, recording, latch_held_at_read)
    entry._attach_hass(hass)
    entry.parent_entry_id = "parent-entry"  # type: ignore[attr-defined]

    async def _subentry_setup(_hass: Any, subentry: Any) -> bool:
        # Stands in for the real subentry setup; the only thing that matters here
        # is that it touches the entry, so the sample above has something to see.
        assert subentry.data is not None
        return True

    monkeypatch.setattr(integration, "_async_setup_subentry", _subentry_setup)

    assert await integration.async_setup(hass, {}) is True
    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True

    recording[0] = True
    try:
        assert await integration.async_setup_entry(hass, entry) is True
    finally:
        recording[0] = False

    assert latch_held_at_read, "the subentry branch has to touch the entry at all"
    assert latch_held_at_read[0] is False, (
        "the release has to run before the subentry branch takes over"
    )


@pytest.mark.asyncio
async def test_the_reload_latch_survives_the_unload_phase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The claim must outlive the teardown, not the first line of the router.

    The platform teardown is the longest stretch of a reload. Releasing the latch
    before it lets the schedulers that do not wait for ``LOADED`` -- the
    credential-writing config flows and the tracker registry probe -- claim it
    again and queue a second reload, even though the replacement setup that is
    already on its way reads the newest ``entry.data`` anyway. That is the
    consecutive teardown the latch exists to prevent. Both do consult
    ``entry.state`` by now, through ``entry_reload_gate``, but only for the
    *terminal* states, and ``UNLOAD_IN_PROGRESS`` is deliberately not one of
    them: this position stays necessary for exactly that reason.

    Beyond that, this position is what makes standing down safe at all: a writer
    that finds the latch taken keeps its change only because the reload holding it
    has not finished its unload half, so the setup half still ahead of it reads
    what the stander-down wrote. Releasing at the head of the router would take
    that guarantee away. This is not a tidiness test, and deleting it in a later
    cleanup would silently unpin the assumption the whole single-owner design
    rests on. The opposite end is pinned by
    ``test_the_setup_releases_the_reload_latch_before_it_reads_the_entry_data``.
    """

    integration = importlib.import_module("custom_components.googlefindmy")

    hass = SimpleNamespace(data={})
    entry = make_config_entry(entry_id="entry-unload", parent_entry_id=None)

    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True

    seen_during_unload: list[bool] = []

    async def _unload_probe(
        _hass: object, _entry: object
    ) -> bool:  # pragma: no cover - trivial stub
        # A second owner asking for the latch mid-teardown must be told "no".
        seen_during_unload.append(
            integration.claim_pending_entry_reload(hass, entry.entry_id)
        )
        return True

    monkeypatch.setattr(integration, "_async_unload_parent_entry", _unload_probe)

    assert await integration.async_unload_entry(hass, entry) is True

    assert seen_during_unload == [False], (
        "the latch must still be held while the platforms are being torn down"
    )
    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True, (
        "once the unload has run the latch is free again, so a later change can "
        "schedule the next reload"
    )


@pytest.mark.asyncio
async def test_a_failed_unload_hands_the_reload_latch_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No setup half follows a failed unload, so the release cannot wait for one."""

    integration = importlib.import_module("custom_components.googlefindmy")

    hass = SimpleNamespace(data={})
    entry = make_config_entry(entry_id="entry-unload-fails", parent_entry_id=None)

    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True

    async def _boom(_hass: object, _entry: object) -> bool:
        raise RuntimeError("teardown exploded")

    monkeypatch.setattr(integration, "_async_unload_parent_entry", _boom)

    with pytest.raises(RuntimeError):
        await integration.async_unload_entry(hass, entry)

    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True, (
        "a latch kept by a failed unload would swallow every later reload"
    )


@pytest.mark.asyncio
async def test_an_unload_that_returns_false_hands_the_reload_latch_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A refused unload reports through the return value, not an exception."""

    integration = importlib.import_module("custom_components.googlefindmy")

    hass = SimpleNamespace(data={})
    entry = make_config_entry(entry_id="entry-unload-refused", parent_entry_id=None)

    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True

    async def _refuse(_hass: object, _entry: object) -> bool:
        return False

    monkeypatch.setattr(integration, "_async_unload_parent_entry", _refuse)

    assert await integration.async_unload_entry(hass, entry) is False
    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True, (
        "no setup half follows a refused unload, so the latch has to come back here"
    )


@pytest.mark.asyncio
async def test_the_subentry_unload_runs_through_the_same_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both router branches share the release, and each frees its own entry id."""

    integration = importlib.import_module("custom_components.googlefindmy")

    hass = SimpleNamespace(data={})
    entry = make_config_entry(entry_id="sub-entry", parent_entry_id="parent-entry")

    assert integration.claim_pending_entry_reload(hass, "sub-entry") is True
    assert integration.claim_pending_entry_reload(hass, "parent-entry") is True

    async def _unload_sub(_hass: object, _entry: object) -> bool:
        return True

    monkeypatch.setattr(integration, "_async_unload_subentry", _unload_sub)

    assert await integration.async_unload_entry(hass, entry) is True

    assert integration.claim_pending_entry_reload(hass, "sub-entry") is True
    assert integration.claim_pending_entry_reload(hass, "parent-entry") is False, (
        "a subentry unload must not release the parent entry's latch"
    )
