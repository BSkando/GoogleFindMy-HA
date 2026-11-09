# tests/test_subentry_setup_trigger.py
from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable, Iterable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.exceptions import ConfigEntryNotReady

import custom_components.googlefindmy as gfm
from custom_components.googlefindmy import (
    _SUBENTRY_SETUP_MAX_ATTEMPTS,
    ConfigEntrySubentryDefinition,
    ConfigEntrySubEntryManager,
    RuntimeData,
    _async_create_task,
    _async_setup_new_subentries,
    _async_setup_subentry,
    _async_unload_parent_entry,
)
from custom_components.googlefindmy.const import (
    DATA_EID_RESOLVER,
    DOMAIN,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_FEATURE_PLATFORMS,
)
from custom_components.googlefindmy.eid_resolver import GoogleFindMyEIDResolver
from custom_components.googlefindmy.entity import schedule_add_entities
from custom_components.googlefindmy.fmdn_finder.ble_scanner import (
    DATA_BLE_SCANNER_UNSUB,
)
from tests.helpers.homeassistant import (
    FakeConfigEntriesManager,
    FakeConfigEntry,
    FakeDeviceEntry,
    FakeDeviceRegistry,
    FakeEntityRegistry,
    FakeHass,
    capture_async_dispatcher_send,
)


def _attach_runtime(entry: FakeConfigEntry) -> RuntimeData:
    """Attach a minimal runtime data container to ``entry``."""

    runtime_data = RuntimeData(
        coordinator=SimpleNamespace(),  # type: ignore[arg-type]
        token_cache=object(),  # type: ignore[arg-type]
        subentry_manager=SimpleNamespace(),  # type: ignore[arg-type]
        fcm_receiver=None,
    )
    entry.runtime_data = runtime_data
    return runtime_data


def _resolver_stub() -> GoogleFindMyEIDResolver:
    """Build a resolver double without invoking the real initializer."""

    class _ResolverDouble(GoogleFindMyEIDResolver):
        __slots__ = ("stop_called", "refresh_called")

        def stop(self) -> None:  # type: ignore[override]
            self.stop_called = True

        async def async_refresh(self, *, wait: bool = False) -> None:
            self.refresh_called = True

    resolver = _ResolverDouble.__new__(_ResolverDouble)
    resolver.stop_called = False
    resolver.refresh_called = False
    return resolver


@pytest.mark.asyncio
async def test_async_setup_subentry_inherits_parent_runtime_data() -> None:
    """Legacy child entries should continue to inherit the parent runtime data."""

    hass = FakeHass(config_entries=FakeConfigEntriesManager())
    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})

    parent_entry_id = "parent-entry"
    coordinator = object()
    runtime_data = RuntimeData(
        coordinator=coordinator,  # type: ignore[arg-type]
        token_cache=object(),  # type: ignore[arg-type]
        subentry_manager=SimpleNamespace(),  # type: ignore[arg-type]
        fcm_receiver=None,
    )
    entries_bucket[parent_entry_id] = runtime_data

    forward_called = False

    async def forward(*_: object) -> None:
        nonlocal forward_called
        forward_called = True

    hass.config_entries.async_forward_entry_setups = forward  # type: ignore[attr-defined]

    child_entry = SimpleNamespace(
        entry_id="child-entry",
        data={"group_key": "child"},
        parent_entry_id=parent_entry_id,
        config_subentry_id="child-subentry",
        subentry_type="tracker",
        runtime_data=None,
    )

    assert await _async_setup_subentry(hass, child_entry) is True
    assert child_entry.runtime_data is runtime_data
    assert forward_called is False


@pytest.mark.asyncio
async def test_async_create_task_schedules_from_worker_thread() -> None:
    """Task scheduling should marshal worker threads back to the HA loop."""

    loop = asyncio.get_running_loop()
    hass = FakeHass(config_entries=FakeConfigEntriesManager([]))
    setattr(hass, "loop", loop)

    thread_names: list[str] = []

    def _recording_async_create_task(
        coro: Any, *, name: str | None = None
    ) -> asyncio.Task[Any]:
        thread_names.append(threading.current_thread().name)
        assert name == "googlefindmy.threadsafe_test"
        return loop.create_task(coro)

    setattr(hass, "async_create_task", _recording_async_create_task)

    async def _marker() -> None:
        thread_names.append(f"coro:{threading.current_thread().name}")

    task = await loop.run_in_executor(
        None,
        lambda: _async_create_task(
            hass,
            _marker(),
            name="googlefindmy.threadsafe_test",
        ),
    )

    await task

    main_thread_name = threading.main_thread().name
    assert thread_names[0] != main_thread_name
    assert thread_names[1] == f"coro:{main_thread_name}"


@pytest.mark.asyncio
async def test_async_setup_subentry_defers_when_parent_runtime_missing() -> None:
    """Modern subentry setup should defer until the parent runtime data bucket exists."""

    hass = FakeHass(config_entries=FakeConfigEntriesManager())

    subentry = SimpleNamespace(
        entry_id="child-entry",
        parent_entry_id="missing-parent",
        config_subentry_id="tracker-subentry",
        data={"features": TRACKER_FEATURE_PLATFORMS},
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    with pytest.raises(ConfigEntryNotReady):
        await _async_setup_subentry(hass, subentry)


@pytest.mark.asyncio
async def test_async_setup_subentry_defers_when_coordinator_missing() -> None:
    """Modern subentry setup should defer until the parent coordinator is ready."""

    hass = FakeHass(config_entries=FakeConfigEntriesManager())
    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})

    parent_entry_id = "parent-entry"
    entries_bucket[parent_entry_id] = SimpleNamespace(coordinator=None)

    subentry = SimpleNamespace(
        entry_id="child-entry",
        parent_entry_id=parent_entry_id,
        config_subentry_id="tracker-subentry",
        data={"features": TRACKER_FEATURE_PLATFORMS},
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    with pytest.raises(ConfigEntryNotReady):
        await _async_setup_subentry(hass, subentry)


@pytest.mark.asyncio
async def test_async_setup_legacy_subentry_attaches_bucket_runtime_data() -> None:
    """Legacy subentry setup should pull runtime data from the parent bucket."""

    hass = FakeHass(config_entries=FakeConfigEntriesManager())
    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})

    parent_entry_id = "parent-entry"
    coordinator = object()
    runtime_data = RuntimeData(
        coordinator=coordinator,  # type: ignore[arg-type]
        token_cache=object(),  # type: ignore[arg-type]
        subentry_manager=SimpleNamespace(),  # type: ignore[arg-type]
        fcm_receiver=None,
    )
    entries_bucket[parent_entry_id] = runtime_data

    child_entry = SimpleNamespace(
        entry_id="child-entry",
        data={"group_key": "child"},
        parent_entry_id=parent_entry_id,
        subentry_type="tracker",
        runtime_data=None,
    )

    assert await _async_setup_subentry(hass, child_entry) is True
    assert child_entry.runtime_data is runtime_data


@pytest.mark.asyncio
async def test_async_setup_subentry_errors_when_unregistered(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Modern subentry setup should fail loudly if the subentry is unknown."""

    async def _async_refresh() -> None:
        return None

    caplog.set_level(logging.ERROR)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = asyncio.create_task
    _attach_runtime(parent_entry)

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    runtime_data = RuntimeData(
        coordinator=SimpleNamespace(
            _refresh_subentry_index=lambda: None,
            async_request_refresh=_async_refresh,
        ),  # type: ignore[arg-type]
        token_cache=object(),  # type: ignore[arg-type]
        subentry_manager=SimpleNamespace(_refresh_from_entry=lambda: None),  # type: ignore[arg-type]
        fcm_receiver=None,
    )
    entries_bucket[parent_entry.entry_id] = runtime_data

    subentry = SimpleNamespace(
        entry_id="child-entry",
        parent_entry_id=parent_entry.entry_id,
        config_subentry_id="child-entry",
        data={"features": TRACKER_FEATURE_PLATFORMS},
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    with pytest.raises(ConfigEntryNotReady):
        await _async_setup_subentry(hass, subentry, subentry)

    assert "not registered under parent" in " ".join(caplog.messages)


@pytest.mark.asyncio
async def test_async_setup_new_subentries_retries_unknown_and_reschedules(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """UnknownEntry should clear the tracker and auto-schedule a retry."""

    caplog.set_level(logging.WARNING)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )
    parent_entry.subentries[subentry.subentry_id] = subentry

    config_entries = FakeConfigEntriesManager([parent_entry])
    config_entries.set_transient_unknown_entry(subentry.entry_id, setup_failures=1)
    hass = FakeHass(config_entries=config_entries)

    runtime_data = _attach_runtime(parent_entry)

    scheduled_callbacks: list[Callable[[Any], None]] = []

    class _Handle:
        def __init__(self) -> None:
            self._cancelled = False

        def cancel(self) -> None:
            self._cancelled = True

    def _fake_async_call_later(
        _hass: FakeHass, _delay: float, callback: Callable[[Any], None]
    ) -> _Handle:
        scheduled_callbacks.append(callback)
        return _Handle()

    monkeypatch.setattr(
        "custom_components.googlefindmy.async_call_later", _fake_async_call_later
    )

    tasks: list[asyncio.Task[Any]] = []

    def _capture_task(coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task

    hass.async_create_task = _capture_task

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        [subentry],
    )

    assert runtime_data.subentry_retry_attempts == {}
    assert runtime_data.subentry_retry_handles == {}
    assert scheduled_callbacks == []
    assert config_entries.setup_calls == [subentry.entry_id]


@pytest.mark.asyncio
async def test_async_setup_new_subentries_stops_after_retry_limit(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Retries should stop after the configured attempt limit."""

    caplog.set_level(logging.WARNING)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )
    parent_entry.subentries[subentry.subentry_id] = subentry

    config_entries = FakeConfigEntriesManager([parent_entry])
    config_entries.set_transient_unknown_entry(
        subentry.entry_id, setup_failures=_SUBENTRY_SETUP_MAX_ATTEMPTS + 2
    )
    hass = FakeHass(config_entries=config_entries)

    runtime_data = _attach_runtime(parent_entry)

    scheduled_callbacks: list[Callable[[Any], None]] = []

    def _fake_async_call_later(
        _hass: FakeHass, _delay: float, callback: Callable[[Any], None]
    ) -> SimpleNamespace:
        scheduled_callbacks.append(callback)
        return SimpleNamespace(cancel=lambda: None)

    monkeypatch.setattr(
        "custom_components.googlefindmy.async_call_later", _fake_async_call_later
    )

    tasks: list[asyncio.Task[Any]] = []

    def _capture_task(coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task

    hass.async_create_task = _capture_task

    await _async_setup_new_subentries(hass, parent_entry, [subentry])

    assert scheduled_callbacks == []
    assert config_entries.setup_calls == [subentry.entry_id]
    assert runtime_data.subentry_retry_attempts == {}
    assert runtime_data.subentry_retry_handles == {}


@pytest.mark.asyncio
async def test_subentry_manager_retries_unknown_entry_without_enforcement() -> None:
    """ConfigEntrySubEntryManager should swallow UnknownEntry and retry later."""

    tracker_key = "tracker"
    parent_entry = FakeConfigEntry(entry_id="parent-entry")

    class _RetryingConfigEntriesManager(FakeConfigEntriesManager):
        def __init__(self) -> None:
            super().__init__([parent_entry])

        def async_add_subentry(self, entry: FakeConfigEntry, subentry: Any) -> Any:
            subentry_id = getattr(subentry, "subentry_id", None)
            if not isinstance(subentry_id, str) or not subentry_id:
                subentry_id = f"{entry.entry_id}:{tracker_key}"
                setattr(subentry, "subentry_id", subentry_id)
            entry.subentries[subentry_id] = subentry
            entry._registered_subentry_ids.add(subentry_id)
            self.set_transient_unknown_entry(subentry_id, setup_failures=1)
            return subentry

    config_entries = _RetryingConfigEntriesManager()
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = asyncio.create_task
    _attach_runtime(parent_entry)
    manager = ConfigEntrySubEntryManager(hass, parent_entry)

    definition = ConfigEntrySubentryDefinition(
        key=tracker_key,
        title="Tracker devices",
        data={"group_key": tracker_key},
    )

    await manager.async_sync([definition])
    assert tracker_key in manager._managed
    assert len(config_entries.setup_calls) == 1
    scheduled_subentry_id = config_entries.setup_calls[0]
    assert isinstance(scheduled_subentry_id, str) and scheduled_subentry_id

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        list(manager._managed.values()),
    )

    assert config_entries.setup_calls == [
        scheduled_subentry_id,
        scheduled_subentry_id,
    ]


@pytest.mark.asyncio
async def test_async_setup_new_subentries_aggregates_platforms_and_dispatches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forward subentry platforms once and dispatch setup signals per child."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    tracker_subentry = SimpleNamespace(
        entry_id="child-entry",  # legacy attribute names used by stubs
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )
    service_subentry = SimpleNamespace(
        entry_id="service-entry",
        subentry_id="service-entry",
        unique_id="service-entry",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )

    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    _attach_runtime(parent_entry)

    monkeypatch.setattr(
        "homeassistant.helpers.dispatcher.async_dispatcher_send",
        capture_async_dispatcher_send,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.async_dispatcher_send",
        capture_async_dispatcher_send,
    )

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        [tracker_subentry, service_subentry],
    )

    assert len(config_entries.forward_setup_calls) == 1
    forwarded_entry, platform_names = config_entries.forward_setup_calls[0]
    assert forwarded_entry is parent_entry
    assert set(platform_names) == {
        "binary_sensor",
        "button",
        "device_tracker",
        "sensor",
    }

    expected_signal = f"googlefindmy_subentry_setup_{parent_entry.entry_id}"
    assert hass.dispatcher_signals == [
        (expected_signal, (tracker_subentry,)),
        (expected_signal, (service_subentry,)),
    ]


@pytest.mark.asyncio
async def test_async_setup_new_subentries_ignores_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ValueError from forward_entry_setups should not block dispatchers."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    _attach_runtime(parent_entry)

    monkeypatch.setattr(
        "homeassistant.helpers.dispatcher.async_dispatcher_send",
        capture_async_dispatcher_send,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.async_dispatcher_send",
        capture_async_dispatcher_send,
    )

    forward_calls: list[tuple[FakeConfigEntry, tuple[str, ...]]] = []

    def _raise_value_error(
        entry: FakeConfigEntry, platforms: Iterable[str | Any]
    ) -> None:
        forward_calls.append((entry, tuple(platforms)))
        raise ValueError("already setup")

    monkeypatch.setattr(
        config_entries, "async_forward_entry_setups", _raise_value_error
    )

    await _async_setup_new_subentries(hass, parent_entry, [subentry])

    assert forward_calls == [
        (
            parent_entry,
            (
                "button",
                "device_tracker",
                "sensor",
            ),
        )
    ]
    assert hass.dispatcher_signals == [
        (f"googlefindmy_subentry_setup_{parent_entry.entry_id}", (subentry,))
    ]
    assert config_entries.setup_calls == [subentry.subentry_id]


@pytest.mark.asyncio
async def test_async_setup_new_subentries_enforces_registered_subentries(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Subentry scheduling should warn when the parent entry lacks the subentry."""

    caplog.set_level(logging.WARNING)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    _attach_runtime(parent_entry)

    orphan_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        [orphan_subentry],
        enforce_registration=True,
    )

    assert config_entries.setup_calls == [orphan_subentry.entry_id]


@pytest.mark.asyncio
async def test_async_setup_new_subentries_warns_but_schedules_when_unregistered(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Missing parent membership should not block scheduling when enforcement is off."""

    caplog.set_level(logging.WARNING)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)

    orphan_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    await _async_setup_new_subentries(hass, parent_entry, [orphan_subentry])

    assert orphan_subentry.entry_id in config_entries.setup_calls


@pytest.mark.asyncio
async def test_async_setup_new_subentries_requires_registered_subentries(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing registry visibility should warn even when enforcement is enabled."""

    caplog.set_level(logging.WARNING)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)

    orphan_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )
    parent_entry.subentries[orphan_subentry.subentry_id] = orphan_subentry

    monkeypatch.setattr(
        "custom_components.googlefindmy._registered_subentry_ids", lambda *_: set()
    )

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        [orphan_subentry],
        enforce_registration=True,
    )

    assert orphan_subentry.entry_id in config_entries.setup_calls


@pytest.mark.asyncio
async def test_async_setup_new_subentries_requires_fallback_parent_membership(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Fallback identifiers should not block scheduling valid parent subentries."""

    caplog.set_level(logging.WARNING)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    parent_entry.subentries["child-subentry"] = {"subentry_id": "child-subentry"}

    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    _attach_runtime(parent_entry)

    mixed_identifier_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-subentry",
        unique_id="child-subentry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )

    await _async_setup_new_subentries(hass, parent_entry, [mixed_identifier_subentry])

    assert "child-subentry" in config_entries.setup_calls
    assert not any(
        "child-entry" in message and "skipping" not in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_async_setup_new_subentries_requires_registration_when_not_enforced(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing registered subentries should warn without blocking scheduling."""

    caplog.set_level(logging.WARNING)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    registered_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )
    parent_entry.subentries[registered_subentry.subentry_id] = registered_subentry

    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    _attach_runtime(parent_entry)

    monkeypatch.setattr(
        "custom_components.googlefindmy._registered_subentry_ids",
        lambda _hass, _entry: set(),
    )

    await _async_setup_new_subentries(hass, parent_entry, [registered_subentry])

    assert registered_subentry.entry_id in config_entries.setup_calls


@pytest.mark.asyncio
async def test_async_setup_new_subentries_forwards_placeholder_with_skip_registered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Forward placeholder subentries lacking IDs even when skipping registered ones."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    placeholder_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        data={"features": TRACKER_FEATURE_PLATFORMS},
    )
    parent_entry.subentries[placeholder_subentry.entry_id] = placeholder_subentry

    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    _attach_runtime(parent_entry)

    monkeypatch.setattr(
        "homeassistant.helpers.dispatcher.async_dispatcher_send",
        capture_async_dispatcher_send,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.async_dispatcher_send",
        capture_async_dispatcher_send,
    )

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        [placeholder_subentry],
        skip_registered=True,
    )

    assert config_entries.forward_setup_calls == [
        (parent_entry, ("button", "device_tracker", "sensor")),
    ]
    assert hass.dispatcher_signals == [
        (
            f"googlefindmy_subentry_setup_{parent_entry.entry_id}",
            (placeholder_subentry,),
        ),
    ]


@pytest.mark.asyncio
async def test_async_setup_new_subentries_requires_registration_when_enforced() -> None:
    """Subentry scheduling should proceed when the registry exposes the subentry."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    registered_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )
    parent_entry.subentries[registered_subentry.subentry_id] = registered_subentry

    config_entries = FakeConfigEntriesManager([parent_entry])
    hass = FakeHass(config_entries=config_entries)
    _attach_runtime(parent_entry)

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        [registered_subentry],
        enforce_registration=True,
    )

    assert registered_subentry.entry_id in config_entries.setup_calls


@pytest.mark.asyncio
async def test_async_setup_new_subentries_links_entities_and_devices(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Schedule setup for registered subentries and propagate config_subentry_id."""

    caplog.set_level(logging.DEBUG)
    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    registered_subentry = SimpleNamespace(
        entry_id="child-entry",
        subentry_id="child-entry",
        unique_id="child-entry",
        subentry_type=SUBENTRY_TYPE_TRACKER,
    )
    parent_entry.subentries[registered_subentry.subentry_id] = registered_subentry

    config_entries = FakeConfigEntriesManager([parent_entry])
    _attach_runtime(parent_entry)
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = lambda coro, name=None: asyncio.create_task(coro)

    device_registry = FakeDeviceRegistry(
        [
            FakeDeviceEntry(
                id="device-id",
                identifiers={(DOMAIN, "device-id")},
                config_entries={parent_entry.entry_id},
            )
        ]
    )
    entity_registry = FakeEntityRegistry()

    async def _async_add_entities(
        entities: Iterable[Any],
        update_before_add: bool = True,
        *,
        config_subentry_id: str | None = None,
    ) -> None:
        del update_before_add
        for entity in entities:
            entity_registry.entities[entity.entity_id] = SimpleNamespace(
                entity_id=entity.entity_id,
                config_entry_id=parent_entry.entry_id,
                config_subentry_id=config_subentry_id,
            )
            device_registry.async_update_device(
                entity.device_info.id,
                config_subentry_id=config_subentry_id,
            )

    class _StubEntity:
        def __init__(self, entity_id: str) -> None:
            self.entity_id = entity_id
            self._device_info = SimpleNamespace(
                id="device-id",
                identifiers={(DOMAIN, "device-id")},
                config_entries={parent_entry.entry_id},
            )

        @property
        def device_info(self) -> Any:
            return self._device_info

    await _async_setup_new_subentries(
        hass,
        parent_entry,
        [registered_subentry],
        enforce_registration=True,
    )

    schedule_add_entities(
        hass,
        _async_add_entities,
        entities=[_StubEntity("sensor.child")],
        update_before_add=True,
        config_subentry_id=registered_subentry.subentry_id,
        log_owner="Subentry schedule test",
        logger=logging.getLogger(__name__),
    )
    await asyncio.sleep(0)

    entity_entry = entity_registry.entities.get("sensor.child")
    device_entry = device_registry.devices.get("device-id")

    assert registered_subentry.entry_id in config_entries.setup_calls
    assert entity_entry is not None
    assert entity_entry.config_subentry_id == registered_subentry.subentry_id
    assert device_entry is not None
    assert device_entry.config_subentry_id == registered_subentry.subentry_id
    assert any(
        "Forwarded setup for config subentries" in message
        for message in caplog.messages
    )


@pytest.mark.asyncio
async def test_async_unload_entry_cancels_retry_handles() -> None:
    """Scheduled retry handles should be cancelled during parent unload."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    runtime_data = _attach_runtime(parent_entry)
    runtime_data.subentry_retry_attempts[parent_entry.entry_id] = {"child-entry": 2}

    class _Handle:
        def __init__(self) -> None:
            self.cancelled = False

        def cancel(self) -> None:
            self.cancelled = True

    handle = _Handle()
    runtime_data.subentry_retry_handles[parent_entry.entry_id] = handle

    config_entries = FakeConfigEntriesManager([parent_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: True  # type: ignore[attr-defined]

    def _forward_unload(entry: Any, platform: Any, **kwargs: Any) -> bool:
        assert kwargs == {}
        return True

    config_entries.async_forward_entry_unload = _forward_unload  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)

    assert await _async_unload_parent_entry(hass, parent_entry) is True
    assert handle.cancelled is True
    assert runtime_data.subentry_retry_handles == {}
    assert runtime_data.subentry_retry_attempts == {}


@pytest.mark.asyncio
async def test_parent_unload_failure_preserves_resolver() -> None:
    """Resolver should remain running when parent unload aborts."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    runtime_data = _attach_runtime(parent_entry)
    config_entries = FakeConfigEntriesManager([parent_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: False  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = (
        lambda coro, name=None: asyncio.create_task(coro)  # noqa: ARG005
    )

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    resolver = _resolver_stub()
    entries_bucket[parent_entry.entry_id] = runtime_data
    bucket[DATA_EID_RESOLVER] = resolver

    assert await _async_unload_parent_entry(hass, parent_entry) is False
    assert entries_bucket[parent_entry.entry_id] is runtime_data
    assert bucket[DATA_EID_RESOLVER] is resolver
    assert resolver.stop_called is False  # type: ignore[attr-defined]
    assert resolver.refresh_called is False  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_parent_unload_success_stops_resolver_when_last_entry() -> None:
    """Resolver should stop only after a successful unload of the final entry."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    runtime_data = _attach_runtime(parent_entry)
    config_entries = FakeConfigEntriesManager([parent_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: True  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = (
        lambda coro, name=None: asyncio.create_task(coro)  # noqa: ARG005
    )

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    resolver = _resolver_stub()
    entries_bucket[parent_entry.entry_id] = runtime_data
    bucket[DATA_EID_RESOLVER] = resolver

    assert await _async_unload_parent_entry(hass, parent_entry) is True
    assert parent_entry.entry_id not in entries_bucket
    assert resolver.stop_called is True  # type: ignore[attr-defined]
    assert bucket.get(DATA_EID_RESOLVER) is None


@pytest.mark.asyncio
async def test_resolver_stub_accepts_the_setup_refresh_call() -> None:
    """The double keeps the real ``async_refresh`` signature (``wait`` keyword).

    Entry setup reuses a resolver from the bucket and calls
    ``async_refresh(wait=True)``; a double with a narrower signature would
    fail there with TypeError instead of standing in for the real class.
    """

    resolver = _resolver_stub()
    bucket: dict[str, Any] = {DATA_EID_RESOLVER: resolver}

    hass = FakeHass(config_entries=FakeConfigEntriesManager())
    await gfm._async_ensure_shared_eid_resolver(hass, bucket)  # type: ignore[arg-type]

    assert bucket[DATA_EID_RESOLVER] is resolver
    assert resolver.refresh_called is True  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_parent_unload_success_refreshes_resolver_when_entries_remain() -> None:
    """Resolver should refresh only after a successful unload when others remain."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    other_entry = FakeConfigEntry(entry_id="other-entry")
    # The resolver stays only while another entry is LOADED.
    other_entry.state = ConfigEntryState.LOADED
    runtime_data = _attach_runtime(parent_entry)
    _attach_runtime(other_entry)
    config_entries = FakeConfigEntriesManager([parent_entry, other_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: True  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)

    tasks: list[asyncio.Task[object]] = []

    def _capture_task(
        coro: Awaitable[object], name: str | None = None
    ) -> asyncio.Task[object]:  # noqa: ARG001
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task

    hass.async_create_task = _capture_task

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    resolver = _resolver_stub()
    entries_bucket[parent_entry.entry_id] = runtime_data
    entries_bucket[other_entry.entry_id] = other_entry.runtime_data  # type: ignore[index]
    bucket[DATA_EID_RESOLVER] = resolver

    assert await _async_unload_parent_entry(hass, parent_entry) is True
    for task in tasks:
        await task

    assert parent_entry.entry_id not in entries_bucket
    assert other_entry.entry_id in entries_bucket
    assert resolver.stop_called is False  # type: ignore[attr-defined]
    assert resolver.refresh_called is True  # type: ignore[attr-defined]
    assert bucket.get(DATA_EID_RESOLVER) is resolver


@pytest.mark.asyncio
async def test_parent_unload_stops_resolver_when_other_entry_is_not_loaded() -> None:
    """An entry left in SETUP_RETRY does not keep the shared resolver alive.

    Its setup raised ``ConfigEntryNotReady`` after it joined the entries bucket,
    so it is still listed there, but Home Assistant never unloads it.
    """

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    other_entry = FakeConfigEntry(entry_id="other-entry")
    other_entry.state = ConfigEntryState.SETUP_RETRY
    runtime_data = _attach_runtime(parent_entry)
    _attach_runtime(other_entry)
    config_entries = FakeConfigEntriesManager([parent_entry, other_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: True  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = (
        lambda coro, name=None: asyncio.create_task(coro)  # noqa: ARG005
    )

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    resolver = _resolver_stub()
    entries_bucket[parent_entry.entry_id] = runtime_data
    entries_bucket[other_entry.entry_id] = other_entry.runtime_data  # type: ignore[index]
    bucket[DATA_EID_RESOLVER] = resolver

    assert await _async_unload_parent_entry(hass, parent_entry) is True
    assert other_entry.entry_id in entries_bucket
    assert resolver.stop_called is True  # type: ignore[attr-defined]
    assert resolver.refresh_called is False  # type: ignore[attr-defined]
    assert bucket.get(DATA_EID_RESOLVER) is None


@pytest.mark.asyncio
async def test_parent_unload_of_one_of_two_entries_keeps_ble_callback() -> None:
    """Unloading one of two loaded entries keeps the shared BLE callback."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    other_entry = FakeConfigEntry(entry_id="other-entry")
    other_entry.state = ConfigEntryState.LOADED
    runtime_data = _attach_runtime(parent_entry)
    _attach_runtime(other_entry)
    config_entries = FakeConfigEntriesManager([parent_entry, other_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: True  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = (
        lambda coro, name=None: asyncio.create_task(coro)  # noqa: ARG005
    )

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    entries_bucket[parent_entry.entry_id] = runtime_data
    entries_bucket[other_entry.entry_id] = other_entry.runtime_data  # type: ignore[index]
    unsub = Mock(name="unsub")
    bucket[DATA_BLE_SCANNER_UNSUB] = unsub

    assert await _async_unload_parent_entry(hass, parent_entry) is True
    assert unsub.call_count == 0
    assert bucket[DATA_BLE_SCANNER_UNSUB] is unsub


def _arm_global_timers(bucket: dict[str, Any]) -> dict[str, Mock]:
    """Store mock handles for the restart watchdog and the URL refresh timer."""
    handles = {
        "restart_check_initial_unsub": Mock(name="restart_initial"),
        "restart_check_unsub": Mock(name="restart_interval"),
        "url_refresh_unsub": Mock(name="url_refresh"),
    }
    bucket.update(handles)
    bucket["restart_check_registered"] = True
    bucket["url_refresh_registered"] = True
    return handles


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("other_state", "released", "spot_closed"),
    [
        (ConfigEntryState.SETUP_RETRY, True, True),
        (ConfigEntryState.LOADED, False, False),
        (ConfigEntryState.SETUP_IN_PROGRESS, True, False),
    ],
)
async def test_parent_unload_releases_global_timers_and_spot_unless_other_loaded(
    monkeypatch: pytest.MonkeyPatch, other_state: Any, released: bool, spot_closed: bool
) -> None:
    """Watchdog, URL timer and SPOT transport follow the LOADED predicate.

    An entry in SETUP_RETRY stays in the entries bucket, but Home Assistant
    never unloads it, so it must not keep the global singletons alive. An
    entry still setting up may already use the lazy SPOT channel, so the
    channel stays open for it (its setup end restores the other singletons).
    """

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    other_entry = FakeConfigEntry(entry_id="other-entry")
    other_entry.state = other_state
    runtime_data = _attach_runtime(parent_entry)
    _attach_runtime(other_entry)
    config_entries = FakeConfigEntriesManager([parent_entry, other_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: True  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = (
        lambda coro, name=None: asyncio.create_task(coro)  # noqa: ARG005
    )
    spot_close = AsyncMock()
    monkeypatch.setattr(gfm.SPOT_GRPC_TRANSPORT, "async_close", spot_close)

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    entries_bucket[parent_entry.entry_id] = runtime_data
    entries_bucket[other_entry.entry_id] = other_entry.runtime_data  # type: ignore[index]
    handles = _arm_global_timers(bucket)

    assert await _async_unload_parent_entry(hass, parent_entry) is True

    assert other_entry.entry_id in entries_bucket
    for name, handle in handles.items():
        assert handle.call_count == (1 if released else 0), name
        assert (name in bucket) is (not released), name
    assert ("restart_check_registered" in bucket) is (not released)
    assert spot_close.await_count == (1 if spot_closed else 0)


@pytest.mark.asyncio
async def test_parent_unload_surfaces_unexpected_ble_unsub_error() -> None:
    """The scanner release absorbs only the documented ValueError."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    runtime_data = _attach_runtime(parent_entry)
    config_entries = FakeConfigEntriesManager([parent_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: True  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)
    hass.async_create_task = (
        lambda coro, name=None: asyncio.create_task(coro)  # noqa: ARG005
    )

    bucket = hass.data.setdefault(DOMAIN, {})
    bucket.setdefault("entries", {})[parent_entry.entry_id] = runtime_data
    bucket[DATA_BLE_SCANNER_UNSUB] = Mock(side_effect=KeyError("unexpected"))

    with pytest.raises(KeyError):
        await _async_unload_parent_entry(hass, parent_entry)


@pytest.mark.asyncio
async def test_parent_unload_failure_does_not_refresh_resolver() -> None:
    """Resolver refresh should not be scheduled when unload fails."""

    parent_entry = FakeConfigEntry(entry_id="parent-entry")
    other_entry = FakeConfigEntry(entry_id="other-entry")
    runtime_data = _attach_runtime(parent_entry)
    _attach_runtime(other_entry)
    config_entries = FakeConfigEntriesManager([parent_entry, other_entry])
    config_entries.async_unload_platforms = lambda entry, platforms: False  # type: ignore[attr-defined]
    hass = FakeHass(config_entries=config_entries)

    hass.async_create_task = (
        lambda coro, name=None: asyncio.create_task(coro)  # noqa: ARG005
    )

    bucket = hass.data.setdefault(DOMAIN, {})
    entries_bucket = bucket.setdefault("entries", {})
    resolver = _resolver_stub()
    entries_bucket[parent_entry.entry_id] = runtime_data
    entries_bucket[other_entry.entry_id] = other_entry.runtime_data  # type: ignore[index]
    bucket[DATA_EID_RESOLVER] = resolver

    assert await _async_unload_parent_entry(hass, parent_entry) is False
    assert parent_entry.entry_id in entries_bucket
    assert resolver.stop_called is False  # type: ignore[attr-defined]
    assert resolver.refresh_called is False  # type: ignore[attr-defined]
    assert bucket.get(DATA_EID_RESOLVER) is resolver
