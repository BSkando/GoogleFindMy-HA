# tests/helpers/homeassistant.py
"""Reusable Home Assistant-style stubs for integration tests."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import FrozenInstanceError, dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import Platform

from custom_components.googlefindmy import UnknownEntry
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    DATA_SECRET_BUNDLE,
    DOMAIN,
    service_device_identifier,
)
from custom_components.googlefindmy.ha_typing import CloudDiscoveryRuntime

from .config_flow import attach_config_entries_flow_manager
from .service_doubles import FakeServiceRegistry

__all__ = [
    "FakeConfigEntry",
    "FakeConfigEntriesManager",
    "DeferredRegistryConfigEntriesManager",
    "FakeServiceRegistry",
    "FakeDeviceEntry",
    "FakeDeviceRegistry",
    "device_registry_async_entries_for_config_entry",
    "FakeEntityRegistry",
    "FakeHass",
    "runtime_subentry_manager",
    "runtime_data_with_subentries",
    "config_entry_with_subentries",
    "config_entry_with_runtime_managed_subentries",
    "config_entry_with_cloud_runtime",
    "resolve_config_entry_lookup",
    "deferred_subentry_entry_id_assignment",
    "service_device_stub",
    "GoogleFindMyConfigEntryStub",
    "capture_async_dispatcher_send",
]

try:  # pragma: no cover - fallback runs only when HA stubs are absent
    from homeassistant.helpers import device_registry as dr
except ModuleNotFoundError:  # pragma: no cover - exercised when helpers unavailable
    dr = SimpleNamespace(DeviceEntryType=None)  # type: ignore[assignment]

_DEFAULT_SERVICE_ENTRY_TYPE = getattr(dr.DeviceEntryType, "SERVICE", "service")


def service_device_stub(
    *,
    entry_id: str,
    service_subentry_id: str | None,
    device_id: str = "service-device",
    name: str | None = None,
    identifiers: Iterable[tuple[str, str]] | None = None,
    include_service_subentry_identifier: bool = True,
    include_hub_link: bool = False,
    config_entries_subentries: Mapping[str, Iterable[str | None]] | None = None,
    entry_type: Any | None = _DEFAULT_SERVICE_ENTRY_TYPE,
    extra_attributes: Mapping[str, Any] | None = None,
) -> SimpleNamespace:
    """Return a service-device stub aligned with registry expectations.

    Parameters
    ----------
    entry_id:
        Parent config entry identifier.
    service_subentry_id:
        Subentry identifier associated with the service device. When ``None``
        the returned namespace omits ``config_subentry_id``.
    device_id:
        Stable device registry identifier.
    name:
        Optional display name attached to the stub.
    identifiers:
        Additional identifier tuples merged with the canonical service
        identifier.
    include_service_subentry_identifier:
        When ``True`` (default) append the derived service-subentry identifier
        used by the coordinator for diagnostics.
    include_hub_link:
        Append ``None`` to the config-subentry mapping for ``entry_id`` to
        emulate legacy hub links when tests require the pre-cleanup state.
    config_entries_subentries:
        Optional explicit mapping. When omitted the helper constructs a
        ``{entry_id: {service_subentry_id}}`` mapping (plus ``None`` when
        ``include_hub_link`` is ``True``).
    entry_type:
        The registry ``entry_type`` assigned to the stub. Defaults to the
        service entry type exposed by Home Assistant's stubs.
    extra_attributes:
        Additional attribute/value pairs copied onto the namespace before
        returning.
    """

    resolved_identifiers: set[tuple[str, str]] = set(identifiers or ())
    resolved_identifiers.add(service_device_identifier(entry_id))
    if include_service_subentry_identifier and service_subentry_id is not None:
        resolved_identifiers.add((DOMAIN, f"{entry_id}:{service_subentry_id}:service"))

    config_entries = set()
    if entry_id:
        config_entries.add(entry_id)

    if config_entries_subentries is None:
        subentries_map: dict[str, set[str | None]] = {entry_id: set()}
        if service_subentry_id is not None:
            subentries_map[entry_id].add(service_subentry_id)
        if include_hub_link:
            subentries_map[entry_id].add(None)
    else:
        subentries_map = {}
        for key, values in config_entries_subentries.items():
            if values is None:
                subentries_map[key] = {None}
                continue
            if isinstance(values, str):
                subentries_map[key] = {values}
                continue
            subentries_map[key] = set(values)

    payload: dict[str, Any] = {
        "id": device_id,
        "identifiers": resolved_identifiers,
        "config_entries": config_entries,
        "config_entries_subentries": subentries_map,
    }

    if service_subentry_id is not None:
        payload["config_subentry_id"] = service_subentry_id
    if name is not None:
        payload["name"] = name
    if entry_type is not None:
        payload["entry_type"] = entry_type
    if extra_attributes:
        payload.update(extra_attributes)

    return SimpleNamespace(**payload)


class GoogleFindMyConfigEntryStub:
    """Config-entry double with Google Find My defaults and unload tracking."""

    __slots__ = (
        "entry_id",
        "domain",
        "data",
        "options",
        "title",
        "unique_id",
        "state",
        "runtime_data",
        "subentries",
        "pref_disable_new_entities",
        "pref_disable_polling",
        "disabled_by",
        "source",
        "version",
        "minor_version",
        "async_start_reauth",
        "_unload_callbacks",
    )

    def __init__(
        self,
        *,
        entry_id: str = "entry-test",
        email: str = "user@example.com",
        data: Mapping[str, Any] | None = None,
        options: Mapping[str, Any] | None = None,
        title: str | None = None,
        unique_id: str | None = None,
        state: ConfigEntryState = ConfigEntryState.NOT_LOADED,
        source: str | None = "user",
    ) -> None:
        self.entry_id = entry_id
        self.domain = DOMAIN
        self.title = title or "Google Find My"
        default_data = {
            DATA_SECRET_BUNDLE: {"username": email},
            CONF_GOOGLE_EMAIL: email,
        }
        if data:
            merged = dict(default_data)
            merged.update(data)
            self.data = merged
        else:
            self.data = dict(default_data)
        self.options = dict(options or {})
        self.unique_id = unique_id
        self.state = state
        self.runtime_data: Any | None = None
        self.subentries: dict[str, Any] = {}
        self.pref_disable_new_entities = False
        self.pref_disable_polling = False
        self.disabled_by: object | None = None
        self.source = source
        self.version = 1
        self.minor_version = 1
        # Mirror the real ``ConfigEntry.async_start_reauth`` (synchronous @callback
        # returning None). A Mock lets poll-cycle tests assert the reauth flow was
        # started; tests may also re-assign their own Mock to this slot.
        self.async_start_reauth = Mock()
        self._unload_callbacks: list[Callable[[], None]] = []

    def async_on_unload(self, callback: Callable[[], None]) -> None:
        """Record ``callback`` for later invocation during unload."""

        self._unload_callbacks.append(callback)

    def invoke_unload_callbacks(self) -> None:
        """Invoke and clear tracked unload callbacks."""

        for callback in self.pop_unload_callbacks():
            callback()

    def pop_unload_callbacks(self) -> list[Callable[[], None]]:
        """Return and clear the recorded unload callbacks."""

        callbacks = list(self._unload_callbacks)
        self._unload_callbacks.clear()
        return callbacks


def _assign_if_present(target: Any, attribute: str, value: Any) -> None:
    """Assign ``value`` to ``attribute`` when the target exposes the field."""

    if not hasattr(target, attribute):
        return
    try:
        setattr(target, attribute, value)
    except (AttributeError, TypeError, FrozenInstanceError):
        object.__setattr__(target, attribute, value)


@dataclass(slots=True)
class FakeConfigEntry:
    """Minimal config entry representation used across service tests."""

    entry_id: str
    domain: str = DOMAIN
    state: ConfigEntryState = ConfigEntryState.NOT_LOADED
    title: str | None = None
    subentries: dict[str, Any] = field(default_factory=dict)
    runtime_data: Any | None = None
    _registered_subentry_ids: set[str] = field(default_factory=set)


@dataclass(slots=True)
class _TransientUnknownEntryConfig:
    """Configuration describing transient UnknownEntry behavior."""

    lookup_misses: int = 0
    setup_failures: int = 0


TransientUnknownConfigInput = _TransientUnknownEntryConfig | Mapping[str, int] | int


def resolve_config_entry_lookup(entries: Iterable[Any], entry_id: str) -> Any | None:
    """Return an entry or subentry matching ``entry_id``.

    This mirrors the lookup contract exercised by
    ``FakeConfigEntriesManager`` so purpose-built test stubs can reuse the
    logic without duplicating it.
    """

    for entry in entries:
        if getattr(entry, "entry_id", None) == entry_id:
            return entry

    for entry in entries:
        runtime_data = getattr(entry, "runtime_data", None)
        manager = getattr(runtime_data, "subentry_manager", None)
        managed = getattr(manager, "managed_subentries", None)
        if isinstance(managed, dict):
            candidate = managed.get(entry_id)
            if candidate is not None:
                return candidate
            for subentry in managed.values():
                candidate_entry_id = getattr(subentry, "entry_id", None)
                if (
                    isinstance(candidate_entry_id, str)
                    and candidate_entry_id == entry_id
                ):
                    return subentry
                candidate_subentry_id = getattr(subentry, "subentry_id", None)
                if (
                    isinstance(candidate_subentry_id, str)
                    and candidate_subentry_id == entry_id
                ):
                    return subentry

    for entry in entries:
        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, dict):
            for subentry in subentries.values():
                candidate_entry_id = getattr(subentry, "entry_id", None)
                if (
                    isinstance(candidate_entry_id, str)
                    and candidate_entry_id == entry_id
                ):
                    return subentry
                candidate_subentry_id = getattr(subentry, "subentry_id", None)
                if (
                    isinstance(candidate_subentry_id, str)
                    and candidate_subentry_id == entry_id
                ):
                    return subentry

    return None


class FakeConfigEntriesManager:
    """Provide config entry access and capture reload/update attempts."""

    # Keep this manager aligned with `_StubConfigEntries` in
    # `tests/test_hass_data_layout.py`; see `tests/AGENTS.md` for the shared
    # synchronization guidance.

    def __init__(
        self,
        entries: Iterable[FakeConfigEntry] | None = None,
        *,
        migration_success: bool = True,
        supports_migrate: bool = True,
        transient_unknown_entries: Mapping[str, TransientUnknownConfigInput]
        | None = None,
    ) -> None:
        self._entries: list[FakeConfigEntry] = list(entries or [])
        self.reload_calls: list[str] = []
        self.update_calls: list[tuple[FakeConfigEntry, dict[str, Any]]] = []
        self.migrate_calls: list[str] = []
        self.migration_success = migration_success
        attach_config_entries_flow_manager(self)
        if not supports_migrate:
            # Mirror Home Assistant instances that omit async_migrate helpers.
            self.async_migrate_entry = None  # type: ignore[assignment]
            self.async_migrate = None  # type: ignore[assignment]
        self.setup_calls: list[str] = []
        self.forward_setup_calls: list[tuple[FakeConfigEntry, tuple[str, ...]]] = []
        self.lookup_attempts: dict[str, int] = defaultdict(int)
        self._transient_unknown: dict[str, _TransientUnknownEntryConfig] = {}
        if transient_unknown_entries:
            for entry_id, config in transient_unknown_entries.items():
                self.set_transient_unknown_entry(entry_id, config=config)

    def add_entry(self, entry: FakeConfigEntry) -> None:
        """Register another entry for subsequent lookups."""

        self._entries.append(entry)

    def set_transient_unknown_entry(
        self,
        entry_id: str,
        *,
        lookup_misses: int | None = None,
        setup_failures: int | None = None,
        config: TransientUnknownConfigInput | None = None,
    ) -> None:
        """Configure transient UnknownEntry behavior for a child entry.

        Parameters
        ----------
        entry_id:
            Identifier of the entry whose lookups or setup should simulate
            transient UnknownEntry races.
        lookup_misses:
            Number of initial ``async_get_entry`` calls that should return
            ``None`` for ``entry_id``.
        setup_failures:
            Number of initial ``async_setup`` calls that should raise
            :class:`UnknownEntry` for ``entry_id``.
        config:
            Optional aggregate configuration. When provided, ``lookup_misses``
            and ``setup_failures`` overrides still take precedence.
        """

        resolved = self._coerce_transient_unknown_config(config)
        if lookup_misses is not None:
            resolved.lookup_misses = lookup_misses
        if setup_failures is not None:
            resolved.setup_failures = setup_failures
        self._transient_unknown[entry_id] = resolved

    @staticmethod
    def _coerce_transient_unknown_config(
        config: TransientUnknownConfigInput | None,
    ) -> _TransientUnknownEntryConfig:
        if isinstance(config, _TransientUnknownEntryConfig):
            return _TransientUnknownEntryConfig(
                lookup_misses=max(0, config.lookup_misses),
                setup_failures=max(0, config.setup_failures),
            )
        if isinstance(config, Mapping):
            return _TransientUnknownEntryConfig(
                lookup_misses=max(0, int(config.get("lookup_misses", 0))),
                setup_failures=max(0, int(config.get("setup_failures", 0))),
            )
        if isinstance(config, int):
            return _TransientUnknownEntryConfig(lookup_misses=max(0, config))
        return _TransientUnknownEntryConfig()

    def async_entries(self, domain: str | None = None) -> list[FakeConfigEntry]:
        """Return entries optionally filtered by domain."""

        if domain is None:
            return list(self._entries)
        return [entry for entry in self._entries if entry.domain == domain]

    def async_get_entry(self, entry_id: str) -> Any | None:
        """Return the entry or subentry matching ``entry_id`` if available."""

        self.lookup_attempts[entry_id] += 1
        config = self._transient_unknown.get(entry_id)
        if config is not None and config.lookup_misses > 0:
            config.lookup_misses -= 1
            return None
        return resolve_config_entry_lookup(self._entries, entry_id)

    def async_get_subentries(self, entry_id: str) -> list[Any]:
        """Return child subentries registered on the provided entry."""

        entry = self.async_get_entry(entry_id)
        if entry is None:
            return []
        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, dict):
            return list(subentries.values())
        if isinstance(subentries, (list, tuple)):
            return list(subentries)
        return []

    async def async_setup(self, entry_id: str) -> bool:
        """Record setup attempts for config subentries."""

        self.setup_calls.append(entry_id)
        config = self._transient_unknown.get(entry_id)
        if config is not None and config.setup_failures > 0:
            config.setup_failures -= 1
            raise UnknownEntry(entry_id)
        return True

    async def async_forward_entry_setups(
        self,
        entry: FakeConfigEntry,
        platforms: Iterable[Platform | str],
    ) -> None:
        """Record forwarded platform setups for assertions."""

        platform_names = tuple(
            platform.value
            if isinstance(platform, Platform) and isinstance(platform.value, str)
            else str(platform)
            for platform in platforms
        )
        self.forward_setup_calls.append((entry, platform_names))

    def async_update_entry(self, entry: FakeConfigEntry, **kwargs: Any) -> None:
        """Capture entry updates in ``update_calls`` for assertions."""

        self.update_calls.append((entry, dict(kwargs)))

    async def async_reload(self, entry_id: str) -> bool:
        """Record reload attempts made by the integration.

        Returns the core's ``bool``, not ``None``: ``_release_claim_when_reload
        _fails`` reads the task's result, and a falsy one means "ended without
        reloading". A double answering ``None`` would report every successful
        reload as a dead end (tests/AGENTS.md, checklist item 8).
        """

        self.reload_calls.append(entry_id)
        return True

    async def async_migrate_entry(self, entry: FakeConfigEntry) -> bool:
        """Record migration attempts and optionally mark the entry as reloadable."""

        self.migrate_calls.append(entry.entry_id)
        if self.migration_success:
            entry.state = ConfigEntryState.NOT_LOADED
        return self.migration_success

    async def async_migrate(self, entry_id: str) -> bool:
        """Backwards-compatible alias that delegates to ``async_migrate_entry``."""

        entry = self.async_get_entry(entry_id)
        if entry is None:
            return False
        return await self.async_migrate_entry(entry)


class DeferredRegistryConfigEntriesManager(FakeConfigEntriesManager):
    """Simulate delayed registry publication when ``async_create_subentry`` is absent."""

    def __init__(self, parent_entry: FakeConfigEntry, resolved_child: Any) -> None:
        super().__init__([parent_entry])
        self._resolved_child = resolved_child
        self.provisional_subentry: Any | None = None
        self._defer_publication = False
        # Mirror Home Assistant cores that do not expose async_create_subentry.
        self.async_create_subentry = None  # type: ignore[assignment]

    def async_add_subentry(self, entry: FakeConfigEntry, subentry: Any) -> None:
        """Stage a provisional subentry and defer registry visibility."""

        registered = getattr(entry, "_registered_subentry_ids", None)
        if not isinstance(registered, set):
            registered = set()
            entry._registered_subentry_ids = registered

        self.provisional_subentry = subentry
        _assign_if_present(subentry, "entry_id", None)
        _assign_if_present(
            subentry,
            "subentry_id",
            getattr(self._resolved_child, "subentry_id", None),
        )
        entry.subentries[self._resolved_child.subentry_id] = self._resolved_child
        resolved_subentry_id = getattr(self._resolved_child, "subentry_id", None)
        if isinstance(resolved_subentry_id, str) and resolved_subentry_id:
            registered.add(resolved_subentry_id)

        resolved_entry_id = getattr(self._resolved_child, "entry_id", None)
        if isinstance(resolved_entry_id, str) and resolved_entry_id:
            registered.add(resolved_entry_id)
        self._defer_publication = True

    def async_get_entry(self, entry_id: str) -> Any | None:
        """Delay lookups until the resolved child becomes visible."""

        provisional = self.provisional_subentry
        provisional_id = (
            getattr(provisional, "entry_id", None) if provisional is not None else None
        )
        resolved_id = getattr(self._resolved_child, "entry_id", None)
        if (
            self._defer_publication
            and isinstance(provisional_id, str)
            and provisional_id
            and provisional_id != resolved_id
            and entry_id == provisional_id
        ):
            self.lookup_attempts[entry_id] += 1
            return None

        if self._defer_publication and entry_id == resolved_id:
            self.lookup_attempts[entry_id] += 1
            self._defer_publication = False
            return None

        return super().async_get_entry(entry_id)


async def deferred_subentry_entry_id_assignment(
    subentry: Any,
    *,
    entry_id: str,
    manager: FakeConfigEntriesManager,
    delay: float = 0.0,
    registered_entry: FakeConfigEntry | None = None,
) -> None:
    """Assign ``entry_id`` after ``delay`` seconds and register the child entry."""

    await asyncio.sleep(delay)
    _assign_if_present(subentry, "entry_id", entry_id)
    if registered_entry is not None:
        manager.add_entry(registered_entry)


def runtime_subentry_manager(
    subentries: Mapping[str, Any] | Iterable[Any],
) -> SimpleNamespace:
    """Return a runtime-style manager exposing ``managed_subentries``.

    Parameters
    ----------
    subentries:
        Either a mapping of identifiers to subentry objects or an iterable of
        objects that define an ``entry_id`` attribute.
    """

    if isinstance(subentries, Mapping):
        managed = dict(subentries)
    else:
        managed = {
            subentry.entry_id: subentry
            for subentry in subentries
            if getattr(subentry, "entry_id", None) is not None
        }

    class _RuntimeSubentryManager:
        """Expose a mutable mapping mirroring Home Assistant's manager."""

        def __init__(self, mapping: dict[str, Any]) -> None:
            self._managed = dict(mapping)

        @property
        def managed_subentries(self) -> dict[str, Any]:
            """Return a shallow copy of the managed subentry mapping."""

            return dict(self._managed)

    return _RuntimeSubentryManager(managed)


def runtime_data_with_subentries(
    subentries: Mapping[str, Any] | Iterable[Any],
) -> SimpleNamespace:
    """Create a runtime data namespace exposing the provided subentries."""

    return SimpleNamespace(subentry_manager=runtime_subentry_manager(subentries))


def config_entry_with_subentries(
    *,
    entry_id: str,
    domain: str = DOMAIN,
    state: ConfigEntryState = ConfigEntryState.NOT_LOADED,
    title: str | None = None,
    subentries: Mapping[str, Any] | Iterable[Any] | None = None,
    runtime_data: Any | None = None,
) -> FakeConfigEntry:
    """Create a config entry that exposes normalized ``subentries``."""

    entry = FakeConfigEntry(
        entry_id=entry_id,
        domain=domain,
        state=state,
        title=title,
        runtime_data=runtime_data,
    )
    if subentries is None:
        return entry

    if isinstance(subentries, Mapping):
        entry.subentries = dict(subentries)
        return entry

    normalized: dict[str, Any] = {}
    for subentry in subentries:
        identifier = getattr(subentry, "subentry_id", None) or getattr(
            subentry, "entry_id", None
        )
        if identifier is None:
            raise ValueError("Subentries must define 'subentry_id' or 'entry_id'")
        normalized[str(identifier)] = subentry

    entry.subentries = normalized
    return entry


def config_entry_with_runtime_managed_subentries(
    *,
    entry_id: str,
    domain: str = DOMAIN,
    state: ConfigEntryState = ConfigEntryState.NOT_LOADED,
    title: str | None = None,
    subentries: Mapping[str, Any] | Iterable[Any] | None = None,
) -> FakeConfigEntry:
    """Create a config entry with a runtime subentry manager attached."""

    entry = config_entry_with_subentries(
        entry_id=entry_id,
        domain=domain,
        state=state,
        title=title,
        subentries=subentries,
    )
    existing = getattr(entry, "subentries", None)
    if isinstance(existing, Mapping):
        mapping = dict(existing)
    elif existing is None:
        mapping = {}
    else:
        mapping = {}
        for subentry in existing:
            identifier = getattr(subentry, "subentry_id", None)
            if not identifier:
                identifier = getattr(subentry, "entry_id", None)
            if not identifier:
                continue
            mapping[str(identifier)] = subentry
    entry.runtime_data = runtime_data_with_subentries(mapping)
    return entry


def config_entry_with_cloud_runtime(
    *,
    entry_id: str = "entry-id",
    domain: str = DOMAIN,
    state: ConfigEntryState = ConfigEntryState.NOT_LOADED,
    runtime_data: Any | None = None,
    data: Mapping[str, Any] | None = None,
    entry: Any | None = None,
) -> Any:
    """Create an entry with a populated cloud discovery runtime container."""

    runtime_container = runtime_data or SimpleNamespace()
    cloud_container = getattr(runtime_container, "cloud_discovery", None)
    if not isinstance(cloud_container, CloudDiscoveryRuntime):
        cloud_container = CloudDiscoveryRuntime()
    runtime_container.cloud_discovery = cloud_container

    cloud_entry = entry or FakeConfigEntry(
        entry_id=entry_id,
        domain=domain,
        state=state,
    )
    _assign_if_present(cloud_entry, "entry_id", entry_id)
    _assign_if_present(cloud_entry, "domain", domain)
    _assign_if_present(cloud_entry, "state", state)
    _assign_if_present(cloud_entry, "data", data)
    cloud_entry.runtime_data = runtime_container
    return cloud_entry


@dataclass(slots=True)
class FakeDeviceEntry:
    """Lightweight device entry model mirroring Home Assistant's registry."""

    id: str
    identifiers: set[tuple[str, str]] = field(default_factory=set)
    config_entries: set[str] = field(default_factory=set)
    config_subentry_id: str | None = None
    via_device_id: str | None = None
    name: str | None = None


class FakeDeviceRegistry:
    """Expose the subset of device registry behaviour required by tests."""

    def __init__(self, devices: Iterable[FakeDeviceEntry] | None = None) -> None:
        self.devices: dict[str, FakeDeviceEntry] = {
            device.id: device for device in devices or ()
        }
        self.updated: list[tuple[str, dict[str, Any]]] = []

    def add_device(self, device: FakeDeviceEntry) -> None:
        """Register another device entry for subsequent lookups."""

        self.devices[device.id] = device

    def async_get(self, device_id: str) -> FakeDeviceEntry | None:
        return self.devices.get(device_id)

    def async_update_device(self, device_id: str, **changes: Any) -> None:
        """Record updates to a device entry and apply them immediately."""

        entry = self.devices[device_id]
        for attribute, value in changes.items():
            setattr(entry, attribute, value)
        self.updated.append((device_id, dict(changes)))

    def async_entries_for_config_entry(
        self, entry_id: str
    ) -> tuple[FakeDeviceEntry, ...]:
        """Return all device entries associated with ``entry_id``."""

        return tuple(
            device
            for device in self.devices.values()
            if entry_id in device.config_entries
        )

    def async_remove_device(
        self, device_id: str
    ) -> None:  # pragma: no cover - defensive
        raise AssertionError(f"Unexpected device removal for {device_id}")


def device_registry_async_entries_for_config_entry(
    registry: FakeDeviceRegistry, entry_id: str
) -> tuple[FakeDeviceEntry, ...]:
    """Return devices for ``entry_id`` mirroring Home Assistant's helper."""

    return registry.async_entries_for_config_entry(entry_id)


class FakeEntityRegistry:
    """Expose the subset of entity registry behaviour required by tests."""

    def __init__(self) -> None:
        self.entities: dict[str, Any] = {}

    def async_remove(self, entity_id: str) -> None:  # pragma: no cover - defensive
        raise AssertionError(f"Unexpected entity removal for {entity_id}")


@dataclass(slots=True)
class FakeHass:
    """Home Assistant stub exposing the services and config entry manager."""

    config_entries: FakeConfigEntriesManager
    services: FakeServiceRegistry = field(default_factory=FakeServiceRegistry)
    data: dict[str, Any] = field(default_factory=dict)
    loop: asyncio.AbstractEventLoop | None = None
    loop_thread_id: int | None = None
    async_create_task: (
        Callable[[Awaitable[Any], str | None], asyncio.Task[Any]] | None
    ) = None
    dispatcher_send_hook: Callable[[str, tuple[Any, ...]], None] | None = None
    dispatcher_signals: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def verify_event_loop_thread(self, _action: str | None = None) -> None:
        """Mirror Home Assistant's dispatcher guard with a no-op for tests."""

        return


def capture_async_dispatcher_send(
    hass: Any, signal: str, *args: Any
) -> None:  # pragma: no cover - exercised via tests
    """Record dispatcher signals on FakeHass instances for assertions."""

    if isinstance(hass, FakeHass):
        hass.dispatcher_signals.append((signal, args))
        if hass.dispatcher_send_hook is not None:
            hass.dispatcher_send_hook(signal, args)
            return

    # Preserve default no-op behaviour for unrelated hass doubles
    return
