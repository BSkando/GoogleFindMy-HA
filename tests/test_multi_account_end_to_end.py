# tests/test_multi_account_end_to_end.py
"""End-to-end regression coverage for multi-account setups."""

from __future__ import annotations

import asyncio
import importlib
import sys
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType, ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest
from homeassistant.config_entries import ConfigEntryState, ConfigSubentry
from homeassistant.const import Platform
from homeassistant.core import ServiceCall

from custom_components.googlefindmy import _platform_value
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    DATA_SECRET_BUNDLE,
    DOMAIN,
    SERVICE_LOCATE_DEVICE,
    SERVICE_PLAY_SOUND,
    PlaySoundOutcome,
    StopSoundOutcome,
)
from tests.helpers import drain_loop
from tests.helpers.homeassistant import resolve_config_entry_lookup
from tests.helpers.service_doubles import FakeServiceRegistry

if TYPE_CHECKING:
    from custom_components.googlefindmy import RuntimeData


@dataclass
class _StubTokenCache:
    """Token cache stub storing entry-scoped values and call history."""

    entry_id: str
    values: dict[str, Any] = field(default_factory=dict)
    set_calls: list[tuple[str, Any]] = field(default_factory=list)

    async def async_set_cached_value(self, key: str, value: Any) -> None:
        self.set_calls.append((key, value))
        self.values[key] = value

    async def async_get_cached_value(self, key: str) -> Any:
        return self.values.get(key)

    async def all(self) -> dict[str, Any]:
        return dict(self.values)

    async def flush(self) -> None:  # pragma: no cover - exercised indirectly
        return None

    async def close(self) -> None:  # pragma: no cover - exercised indirectly
        return None


class _StubFcm:
    """Shared FCM receiver stub tracking coordinator registrations."""

    def __init__(self) -> None:
        self.registered: list[Any] = []
        self.tokens: dict[str, str] = {}

    def register_coordinator(self, coordinator: Any) -> None:
        assert coordinator.cache is not None
        token = f"fcm-token-{coordinator.cache.entry_id}"
        self.tokens[coordinator.cache.entry_id] = token
        self.registered.append(coordinator)

    def unregister_coordinator(self, coordinator: Any) -> None:
        self.registered = [c for c in self.registered if c is not coordinator]

    def request_stop(self) -> None:  # pragma: no cover - no state change
        return None

    async def _start_listening(self) -> None:
        return None


class _StubBus:
    def async_listen_once(
        self, _event: str, _callback: Callable[..., Any]
    ) -> Callable[[], None]:
        return lambda: None


class _StubHttp:
    def __init__(self) -> None:
        self.registered: list[Any] = []

    def register_view(self, view: Any) -> None:
        self.registered.append(view)


_StubServices = FakeServiceRegistry


class _StubConfigEntry:
    def __init__(self, entry_id: str, email: str) -> None:
        self.entry_id: str = entry_id
        self.data: dict[str, Any] = {
            DATA_SECRET_BUNDLE: {"username": email, "oauth_token": f"oauth-{entry_id}"},
            CONF_GOOGLE_EMAIL: email,
        }
        self.options: dict[str, Any] = {}
        self.title: str = f"Account {email}"
        self.runtime_data: RuntimeData | None = None
        self.subentries: dict[str, ConfigSubentry] = {}
        self.state: ConfigEntryState = ConfigEntryState.LOADED
        self.disabled_by: str | None = None
        self._unload_callbacks: list[Callable[[], None]] = []
        self._update_listeners: list[Callable[..., Any]] = []
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


class _StubConfigEntries:
    """Mirror FakeConfigEntriesManager lookups for multi-entry harnesses.

    See ``tests/AGENTS.md`` for the shared alignment reminders.
    """

    def __init__(self, entries: list[_StubConfigEntry]) -> None:
        self._entries: list[_StubConfigEntry] = entries
        self.forward_calls: list[tuple[str, tuple[str, ...]]] = []
        self.added_subentries: list[tuple[str, ConfigSubentry]] = []
        self.updated_subentries: list[tuple[str, ConfigSubentry]] = []
        self.removed_subentries: list[tuple[str, str]] = []
        self.setup_calls: list[str] = []

    def async_entries(self, domain: str) -> list[_StubConfigEntry]:
        if domain != DOMAIN:
            return []
        return list(self._entries)

    def async_get_entry(
        self, entry_id: str
    ) -> _StubConfigEntry | ConfigSubentry | None:
        return resolve_config_entry_lookup(self._entries, entry_id)

    def async_get_subentries(self, entry_id: str) -> list[ConfigSubentry]:
        entry = self.async_get_entry(entry_id)
        if entry is None:
            return []
        return list(entry.subentries.values())

    async def async_forward_entry_setups(
        self,
        entry: _StubConfigEntry,
        platforms: Iterable[Platform],
    ) -> None:
        platform_names = tuple(_platform_value(platform) for platform in platforms)
        self.forward_calls.append((entry.entry_id, platform_names))

    async def async_unload_platforms(
        self, _entry: _StubConfigEntry, _platforms: list[str]
    ) -> bool:
        return True

    def async_add_subentry(
        self, entry: _StubConfigEntry, subentry: ConfigSubentry
    ) -> bool:
        entry.subentries[subentry.subentry_id] = subentry
        self.added_subentries.append((entry.entry_id, subentry))
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
        self.updated_subentries.append((entry.entry_id, subentry))
        return changed

    def async_remove_subentry(self, entry: _StubConfigEntry, subentry_id: str) -> bool:
        entry.subentries.pop(subentry_id, None)
        self.removed_subentries.append((entry.entry_id, subentry_id))
        return True

    def async_update_entry(self, entry: _StubConfigEntry, **kwargs: Any) -> None:
        data = kwargs.get("data")
        if isinstance(data, Mapping):
            entry.data = dict(data)

        options = kwargs.get("options")
        if isinstance(options, Mapping):
            entry.options = dict(options)

        title = kwargs.get("title")
        if isinstance(title, str):
            entry.title = title

        unique_id = kwargs.get("unique_id")
        if isinstance(unique_id, str):
            setattr(entry, "unique_id", unique_id)

    async def async_reload(
        self, _entry_id: str
    ) -> None:  # pragma: no cover - not triggered
        return None

    async def async_setup(self, entry_id: str) -> bool:
        self.setup_calls.append(entry_id)
        return True


class _StubHass:
    def __init__(
        self, entries: list[_StubConfigEntry], loop: asyncio.AbstractEventLoop
    ) -> None:
        from homeassistant.core import CoreState

        self.loop = loop
        self.data: dict[str, Any] = {DOMAIN: {}, "core.uuid": "ha-uuid"}
        self.state = CoreState.running
        self.bus = _StubBus()
        self.http = _StubHttp()
        self.services = _StubServices()
        self.config_entries: _StubConfigEntries = _StubConfigEntries(entries)
        self._tasks: list[asyncio.Task[Any]] = []
        for entry in entries:
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


def test_multi_account_end_to_end(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """Two entries can coexist with isolated caches, services, and FCM tokens."""

    loop = asyncio.new_event_loop()

    try:
        if "homeassistant.loader" not in sys.modules:
            loader_module = ModuleType("homeassistant.loader")

            async def _async_get_integration(_domain: str) -> SimpleNamespace:
                return SimpleNamespace(name="googlefindmy", version="0.0.0")

            loader_module.async_get_integration = _async_get_integration  # type: ignore[attr-defined]
            sys.modules["homeassistant.loader"] = loader_module

        integration = importlib.import_module("custom_components.googlefindmy")
        coordinator_module = importlib.import_module(
            "custom_components.googlefindmy.coordinator"
        )
        map_view_module = importlib.import_module(
            "custom_components.googlefindmy.map_view"
        )
        nova_module = importlib.import_module(
            "custom_components.googlefindmy.NovaApi.nova_request"
        )

        register_calls: list[Any] = []
        unregister_calls: list[Any] = []
        session_unreg_calls: list[Any] = []

        original_register = getattr(nova_module, "register_hass", None)
        original_unregister = getattr(nova_module, "unregister_hass", None)
        original_unreg_provider = getattr(
            nova_module, "unregister_session_provider", None
        )

        def _spy_register(hass: Any) -> None:
            register_calls.append(hass)
            if callable(original_register):
                original_register(hass)

        def _spy_unregister() -> None:
            unregister_calls.append(True)
            if callable(original_unregister):
                original_unregister()

        def _spy_unreg_provider() -> None:
            session_unreg_calls.append(True)
            if callable(original_unreg_provider):
                original_unreg_provider()

        monkeypatch.setattr(nova_module, "register_hass", _spy_register)
        monkeypatch.setattr(nova_module, "unregister_hass", _spy_unregister)
        monkeypatch.setattr(
            nova_module,
            "unregister_session_provider",
            _spy_unreg_provider,
            raising=False,
        )

        config_entries_module = importlib.import_module("homeassistant.config_entries")
        state_cls = config_entries_module.ConfigEntryState
        if not hasattr(state_cls, "SETUP_IN_PROGRESS"):
            setattr(state_cls, "SETUP_IN_PROGRESS", "setup_in_progress")
        if not hasattr(state_cls, "SETUP_RETRY"):
            setattr(state_cls, "SETUP_RETRY", "setup_retry")

        caches: dict[str, _StubTokenCache] = {}

        async def _fake_cache_create(
            cls: Any, hass: Any, entry_id: str, legacy_path: str | None = None
        ) -> _StubTokenCache:  # type: ignore[override]
            cache = _StubTokenCache(entry_id)
            caches[entry_id] = cache
            return cache

        monkeypatch.setattr(
            integration.TokenCache,
            "create",
            classmethod(_fake_cache_create),
        )

        async def _noop_async(*_args: Any, **_kwargs: Any) -> None:
            return None

        monkeypatch.setattr(
            integration, "_register_instance", lambda *args, **kwargs: None
        )
        monkeypatch.setattr(
            integration, "_unregister_instance", lambda *args, **kwargs: None
        )
        monkeypatch.setattr(
            integration, "_async_soft_migrate_data_to_options", _noop_async
        )
        monkeypatch.setattr(integration, "_async_migrate_unique_ids", _noop_async)
        monkeypatch.setattr(integration, "_async_normalize_device_names", _noop_async)

        stub_fcm = _StubFcm()

        async def _acquire_shared_fcm(
            _hass: Any,
            *,
            entry: Any | None = None,
            cache: Any | None = None,
            entry_resolver: Callable[[], str | None] | None = None,
        ) -> _StubFcm:
            return stub_fcm

        monkeypatch.setattr(
            integration, "_async_acquire_shared_fcm", _acquire_shared_fcm
        )

        issue_calls: list[tuple[str, str]] = []
        monkeypatch.setattr(
            integration.ir,
            "async_create_issue",
            lambda hass, domain, issue_id, **kwargs: issue_calls.append(
                (domain, issue_id)
            ),
        )

        def _init_coordinator(
            coordinator: Any,
            *,
            cache: _StubTokenCache,
            hass: Any,
            **_: Any,
        ) -> None:
            canonical = f"{cache.entry_id}-device"
            coordinator.data = [{"id": canonical, "name": f"Device {cache.entry_id}"}]
            coordinator._display = {canonical: f"Device {cache.entry_id}"}
            coordinator.locate_calls = []
            coordinator.play_calls = []
            coordinator.refresh_calls = 0

        def _get_device_display_name(self: Any, canonical_id: str) -> str | None:
            return self._display.get(canonical_id)

        def _can_request_location(self: Any, _device_id: str) -> bool:
            return True

        def _can_play_sound(self: Any, _device_id: str) -> bool:
            return True

        async def _async_locate_device(self: Any, canonical_id: str) -> dict[str, Any]:
            self.locate_calls.append(canonical_id)
            return {"canonical_id": canonical_id, "entry_id": self.cache.entry_id}

        async def _async_play_sound(self: Any, canonical_id: str) -> PlaySoundOutcome:
            token = f"fcm-token-{self.cache.entry_id}"
            self.play_calls.append((canonical_id, token))
            # Same reason as the stop stub below: production returns the enum,
            # and the service handler treats anything that is not ACCEPTED as a
            # failure. A bool here used to read as success; it now surfaces as
            # the contract breach it is.
            return PlaySoundOutcome.ACCEPTED

        async def _async_stop_sound(self: Any, _canonical_id: str) -> StopSoundOutcome:
            # Production returns the enum; a bool would slip through the
            # service handler's default branch unnoticed.
            return StopSoundOutcome.CANCELLED

        def _force_poll_due(self: Any) -> None:
            self.refresh_calls += 1

        async def _async_refresh(self: Any) -> None:
            self.refresh_calls += 1

        coordinator_cls = stub_coordinator_factory(
            init_hook=_init_coordinator,
            methods={
                "get_device_display_name": _get_device_display_name,
                "can_request_location": _can_request_location,
                "can_play_sound": _can_play_sound,
                "async_locate_device": _async_locate_device,
                "async_play_sound": _async_play_sound,
                "async_stop_sound": _async_stop_sound,
                "force_poll_due": _force_poll_due,
                "async_refresh": _async_refresh,
            },
        )

        monkeypatch.setattr(
            coordinator_module, "GoogleFindMyCoordinator", coordinator_cls
        )
        monkeypatch.setattr(integration, "GoogleFindMyCoordinator", coordinator_cls)
        monkeypatch.setattr(
            map_view_module, "GoogleFindMyCoordinator", coordinator_cls, raising=False
        )

        class _DummyView:
            def __init__(self, hass: Any) -> None:
                self.hass = hass

            async def get(self, *_args: Any, **_kwargs: Any) -> SimpleNamespace:
                return SimpleNamespace(status=200)

        monkeypatch.setattr(
            map_view_module, "GoogleFindMyMapView", _DummyView, raising=False
        )
        monkeypatch.setattr(
            map_view_module, "GoogleFindMyMapRedirectView", _DummyView, raising=False
        )
        monkeypatch.setattr(integration, "GoogleFindMyMapView", _DummyView)
        monkeypatch.setattr(integration, "GoogleFindMyMapRedirectView", _DummyView)

        entry_one = _StubConfigEntry("entry-one", "alpha@example.com")
        entry_two = _StubConfigEntry("entry-two", "beta@example.com")
        entries = [entry_one, entry_two]
        hass = _StubHass(entries, loop)

        async def _exercise() -> None:
            assert await integration.async_setup(hass, {})
            assert await integration.async_setup_entry(hass, entry_one)
            assert await integration.async_setup_entry(hass, entry_two)

            if hass._tasks:
                await asyncio.gather(*hass._tasks)

            # Home Assistant core forwards subentry platforms automatically; the
            # parent entry no longer schedules background forwarding tasks.
            assert entry_one._background_tasks == []
            assert entry_two._background_tasks == []

            locate_service = hass.services.registered[(DOMAIN, SERVICE_LOCATE_DEVICE)]
            play_service = hass.services.registered[(DOMAIN, SERVICE_PLAY_SOUND)]

            canonical_one = f"{entry_one.entry_id}-device"
            canonical_two = f"{entry_two.entry_id}-device"

            await locate_service(ServiceCall({"device_id": canonical_one}))
            await locate_service(ServiceCall({"device_id": canonical_two}))
            await play_service(ServiceCall({"device_id": canonical_one}))
            await play_service(ServiceCall({"device_id": canonical_two}))

        loop.run_until_complete(_exercise())

        runtime_bucket = hass.data[DOMAIN]["entries"]
        assert set(runtime_bucket) == {"entry-one", "entry-two"}

        coord_one = runtime_bucket["entry-one"].coordinator
        coord_two = runtime_bucket["entry-two"].coordinator

        assert coord_one.locate_calls == ["entry-one-device"]
        assert coord_two.locate_calls == ["entry-two-device"]
        assert coord_one.play_calls == [("entry-one-device", "fcm-token-entry-one")]
        assert coord_two.play_calls == [("entry-two-device", "fcm-token-entry-two")]

        assert caches["entry-one"] is not caches["entry-two"]
        assert caches["entry-one"].values["username"] == "alpha@example.com"
        assert caches["entry-two"].values["username"] == "beta@example.com"

        assert stub_fcm.tokens == {
            "entry-one": "fcm-token-entry-one",
            "entry-two": "fcm-token-entry-two",
        }
        assert issue_calls == []

        assert len(register_calls) == 2
        bucket = hass.data[DOMAIN]
        assert bucket["nova_refcount"] == 2

        def _drain_unload_callbacks(entry: _StubConfigEntry) -> None:
            callbacks = list(entry._unload_callbacks)
            entry._unload_callbacks.clear()
            for callback in callbacks:
                callback()

        _drain_unload_callbacks(entry_one)
        assert bucket["nova_refcount"] == 1
        assert not unregister_calls
        assert not session_unreg_calls

        _drain_unload_callbacks(entry_two)
        assert bucket["nova_refcount"] == 0
        assert len(unregister_calls) == 1
        assert len(session_unreg_calls) == 1
    finally:
        drain_loop(loop)
