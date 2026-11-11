# custom_components/googlefindmy/switch.py
"""Switch entities for Google Find My tracker subentries.

One configuration switch per tracker: **Automatic location polling**. When it
is off, the coordinator sends no automatic location request for the tracker
(regular tick, cold start, requested refresh, ``force_poll_due``). A manual
locate (button, ``googlefindmy.locate_device``) stays possible, and the device
list is still fetched for the whole account.

Design notes
------------
* The state lives in the entry options (``automatic_polling_disabled``); the
  coordinator owns reading and writing them. The entity holds no state of its
  own, so a restart needs no ``RestoreEntity``.
* Writing the options never reloads the entry: the options update listener
  only reloads on changed credentials.
* The entity logs nothing. The coordinator writes the INFO line
  ``AUTOMATIC_POLLING_CHANGE`` for every stored change, without the name.
* Availability is not tied to presence: the switch stays usable while the
  tracker is absent from the latest Google list, as long as it is known in the
  subentry snapshot.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any, NamedTuple

from homeassistant.components.switch import SwitchEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import EntityRecoveryManager
from .const import DOMAIN, SUBENTRY_TYPE_TRACKER, TRACKER_SUBENTRY_KEY
from .entity import (
    GoogleFindMyDeviceEntity,
    ensure_config_subentry_id,
    ensure_dispatcher_dependencies,
    resolve_coordinator,
    schedule_add_entities,
)
from .ha_typing import SwitchEntity, callback

if TYPE_CHECKING:
    # Annotation only: a module-level binding would make this module one more
    # holder of the coordinator class (tests/test_guard_coordinator_identity.py).
    from .coordinator import GoogleFindMyCoordinator

_LOGGER = logging.getLogger(__name__)

# Toggling writes the entry options; serialize writes per platform.
PARALLEL_UPDATES = 1

_FEATURE = "switch"
AUTOMATIC_POLLING_KEY = "automatic_location_polling"

AUTOMATIC_POLLING_DESCRIPTION = SwitchEntityDescription(
    key=AUTOMATIC_POLLING_KEY,
    translation_key=AUTOMATIC_POLLING_KEY,
    entity_category=EntityCategory.CONFIG,
)


class _TrackerScope(NamedTuple):
    """Resolved tracker subentry scope for entity creation."""

    subentry_key: str
    config_subentry_id: str | None
    identifier: str


def _subentry_type(subentry: Any | None) -> str | None:
    """Return the declared subentry type for dispatcher filtering."""

    if subentry is None or isinstance(subentry, str):
        return None

    declared_type = getattr(subentry, "subentry_type", None)
    if isinstance(declared_type, str):
        return declared_type

    data = getattr(subentry, "data", None)
    if isinstance(data, Mapping):
        fallback_type = data.get("subentry_type") or data.get("type")
        if isinstance(fallback_type, str):
            return fallback_type
    return None


def _derive_device_label(device: Mapping[str, Any]) -> str | None:
    """Return a stable device label without mutating the coordinator snapshot."""

    for candidate in (device.get("name"), device.get("device_id"), device.get("id")):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    *,
    config_subentry_id: str | None = None,
) -> None:
    """Set up the per-tracker automatic polling switches."""
    _LOGGER.debug(
        "Switch setup invoked for entry=%s config_subentry_id=%s",
        getattr(config_entry, "entry_id", None),
        config_subentry_id,
    )
    coordinator = resolve_coordinator(config_entry)
    ensure_dispatcher_dependencies(hass)
    if getattr(coordinator, "config_entry", None) is None:
        coordinator.config_entry = config_entry

    def _known_tracker_ids() -> set[str]:
        ids: set[str] = set()
        sources: list[Iterable[Any]] = []
        subentries = getattr(config_entry, "subentries", None)
        if isinstance(subentries, Mapping):
            sources.append(subentries.values())
        runtime_data = getattr(config_entry, "runtime_data", None)
        manager = getattr(runtime_data, "subentry_manager", None)
        managed = getattr(manager, "managed_subentries", None)
        if isinstance(managed, Mapping):
            sources.append(managed.values())
        for source in sources:
            for subentry in source:
                if _subentry_type(subentry) != SUBENTRY_TYPE_TRACKER:
                    continue
                candidate = getattr(subentry, "subentry_id", None) or getattr(
                    subentry, "entry_id", None
                )
                if isinstance(candidate, str) and candidate:
                    ids.add(candidate)
        return ids

    def _collect_tracker_scopes(
        hint_subentry_id: str | None = None,
        forwarded_config_id: str | None = None,
    ) -> list[_TrackerScope]:
        scopes: dict[str, _TrackerScope] = {}

        subentry_metas = getattr(coordinator, "_subentry_metadata", None)
        if isinstance(subentry_metas, Mapping):
            for key, meta in subentry_metas.items():
                if _FEATURE not in getattr(meta, "features", ()):
                    continue
                stable_identifier = getattr(meta, "stable_identifier", None)
                identifier = (
                    stable_identifier()
                    if callable(stable_identifier)
                    else None
                    or getattr(meta, "config_subentry_id", None)
                    or coordinator.stable_subentry_identifier(key=key)
                )
                scopes[identifier] = _TrackerScope(
                    key, getattr(meta, "config_subentry_id", None), identifier
                )

        subentries = getattr(config_entry, "subentries", None)
        if isinstance(subentries, Mapping):
            for subentry in subentries.values():
                data = getattr(subentry, "data", {})
                group_key = TRACKER_SUBENTRY_KEY
                subentry_features: Iterable[Any] = ()
                if isinstance(data, Mapping):
                    group_key = data.get("group_key", group_key)
                    subentry_features = data.get("features", ())
                if _FEATURE not in subentry_features:
                    continue
                config_id = getattr(subentry, "subentry_id", None) or getattr(
                    subentry, "entry_id", None
                )
                identifier = (
                    config_id
                    or coordinator.stable_subentry_identifier(key=group_key)
                    or TRACKER_SUBENTRY_KEY
                )
                scopes.setdefault(
                    identifier,
                    _TrackerScope(
                        group_key or TRACKER_SUBENTRY_KEY, config_id, identifier
                    ),
                )

        if hint_subentry_id:
            scopes.setdefault(
                hint_subentry_id,
                _TrackerScope(
                    TRACKER_SUBENTRY_KEY,
                    forwarded_config_id or hint_subentry_id,
                    hint_subentry_id,
                ),
            )

        if scopes:
            return list(scopes.values())

        fallback_identifier = coordinator.stable_subentry_identifier(
            key=TRACKER_SUBENTRY_KEY
        )
        return [
            _TrackerScope(
                TRACKER_SUBENTRY_KEY, forwarded_config_id, fallback_identifier
            )
        ]

    def _is_visible(subentry_key: str, device_id: str) -> bool:
        is_visible = getattr(coordinator, "is_device_visible_in_subentry", None)
        if callable(is_visible):
            try:
                return bool(is_visible(subentry_key, device_id))
            except Exception:  # pragma: no cover - defensive fallback for stubs
                return True
        return True

    added_unique_ids: set[str] = set()
    primary_scope: _TrackerScope | None = None
    primary_scheduler: Callable[[Iterable[SwitchEntity], bool], None] | None = None

    def _add_scope(scope: _TrackerScope, forwarded_config_id: str | None) -> None:
        nonlocal primary_scope, primary_scheduler

        tracker_ids = _known_tracker_ids()
        sanitized_config_id = ensure_config_subentry_id(
            config_entry,
            _FEATURE,
            scope.config_subentry_id or forwarded_config_id,
            known_ids=tracker_ids,
        )
        if sanitized_config_id is None:
            if tracker_ids:
                _LOGGER.debug(
                    "Switch setup: skipping subentry '%s' because the config_subentry_id is unknown",
                    forwarded_config_id
                    or scope.config_subentry_id
                    or scope.subentry_key,
                )
                return
            sanitized_config_id = (
                scope.identifier or forwarded_config_id or scope.subentry_key
            )

        tracker_identifier = (
            scope.identifier or sanitized_config_id or scope.subentry_key
        )

        def _schedule(
            new_entities: Iterable[SwitchEntity], update_before_add: bool = True
        ) -> None:
            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=new_entities,
                update_before_add=update_before_add,
                config_subentry_id=sanitized_config_id,
                log_owner="Switch setup",
                logger=_LOGGER,
            )

        if primary_scope is None:
            primary_scope = scope
        if primary_scheduler is None:
            primary_scheduler = _schedule

        known_device_ids: set[str] = set()

        def _build_entities(devices: Iterable[dict[str, Any]]) -> list[SwitchEntity]:
            entities: list[SwitchEntity] = []
            for device in devices:
                dev_id = device.get("id") if isinstance(device, Mapping) else None
                if not isinstance(dev_id, str) or not dev_id:
                    continue
                if dev_id in known_device_ids:
                    continue
                if not _is_visible(scope.subentry_key, dev_id):
                    _LOGGER.debug(
                        "Switch setup: skipping hidden device id %s for subentry %s",
                        dev_id,
                        scope.subentry_key,
                    )
                    continue
                entity = GoogleFindMyAutomaticPollingSwitch(
                    coordinator,
                    device,
                    _derive_device_label(device),
                    subentry_key=scope.subentry_key,
                    subentry_identifier=tracker_identifier,
                )
                unique_id = getattr(entity, "unique_id", None)
                if isinstance(unique_id, str):
                    if unique_id in added_unique_ids:
                        continue
                    added_unique_ids.add(unique_id)
                entities.append(entity)
                known_device_ids.add(dev_id)
            return entities

        _schedule(
            _build_entities(coordinator.get_subentry_snapshot(scope.subentry_key)),
            True,
        )

        @callback
        def _add_new_devices() -> None:
            new_entities = _build_entities(
                coordinator.get_subentry_snapshot(scope.subentry_key)
            )
            if new_entities:
                _schedule(new_entities, True)

        config_entry.async_on_unload(coordinator.async_add_listener(_add_new_devices))

    seen_subentries: set[str | None] = set()

    @callback
    def async_add_subentry(subentry: Any | None = None) -> None:
        if isinstance(subentry, str):
            subentry_identifier: str | None = subentry
        else:
            subentry_identifier = getattr(subentry, "subentry_id", None) or getattr(
                subentry, "entry_id", None
            )
        subentry_type = _subentry_type(subentry)
        if subentry_type is not None and subentry_type != SUBENTRY_TYPE_TRACKER:
            return
        if subentry_identifier in seen_subentries:
            return
        seen_subentries.add(subentry_identifier)
        for scope in _collect_tracker_scopes(
            subentry_identifier, forwarded_config_id=subentry_identifier
        ):
            _add_scope(scope, subentry_identifier)

    runtime_data = getattr(config_entry, "runtime_data", None)
    subentry_manager = getattr(runtime_data, "subentry_manager", None)
    managed_subentries = getattr(subentry_manager, "managed_subentries", None)
    known_subentries: Iterable[Any] = ()
    if isinstance(managed_subentries, Mapping) and managed_subentries:
        known_subentries = managed_subentries.values()
    elif (
        isinstance(getattr(config_entry, "subentries", None), Mapping)
        and config_entry.subentries
    ):
        known_subentries = config_entry.subentries.values()

    if config_subentry_id is None and known_subentries and subentry_manager is not None:
        _LOGGER.debug(
            "Switch setup: deferring subentry processing until dispatcher signal for entry %s",
            getattr(config_entry, "entry_id", None),
        )
        schedule_add_entities(
            coordinator.hass,
            async_add_entities,
            entities=[],
            update_before_add=False,
            log_owner="Switch setup",
            logger=_LOGGER,
        )
    elif known_subentries:
        for managed_subentry in known_subentries:
            async_add_subentry(managed_subentry)
    else:
        async_add_subentry(config_subentry_id)

    config_entry.async_on_unload(
        async_dispatcher_connect(
            hass,
            f"{DOMAIN}_subentry_setup_{config_entry.entry_id}",
            async_add_subentry,
        )
    )

    recovery_manager = getattr(runtime_data, "entity_recovery_manager", None)
    if not isinstance(recovery_manager, EntityRecoveryManager):
        return

    entry_id = getattr(config_entry, "entry_id", None)
    recovery_identifier = (
        primary_scope.identifier if primary_scope is not None else None
    )
    recovery_subentry_key = (
        primary_scope.subentry_key
        if primary_scope is not None
        else TRACKER_SUBENTRY_KEY
    )

    def _recovery_unique_id(dev_id: str) -> str:
        return f"{DOMAIN}_{entry_id}_{recovery_identifier}_{dev_id}_{AUTOMATIC_POLLING_KEY}"

    def _recovery_devices() -> list[dict[str, Any]]:
        if not isinstance(entry_id, str) or not entry_id:
            return []
        if not isinstance(recovery_identifier, str) or not recovery_identifier:
            return []
        return [
            device
            for device in coordinator.get_subentry_snapshot(recovery_subentry_key)
            if isinstance(device, Mapping)
            and isinstance(device.get("id"), str)
            and device.get("id")
            and _is_visible(recovery_subentry_key, device["id"])
        ]

    def _expected_unique_ids() -> set[str]:
        return {_recovery_unique_id(device["id"]) for device in _recovery_devices()}

    def _build_missing(missing: set[str]) -> list[SwitchEntity]:
        if not missing or not isinstance(recovery_identifier, str):
            return []
        return [
            GoogleFindMyAutomaticPollingSwitch(
                coordinator,
                device,
                _derive_device_label(device),
                subentry_key=recovery_subentry_key,
                subentry_identifier=recovery_identifier,
            )
            for device in _recovery_devices()
            if _recovery_unique_id(device["id"]) in missing
        ]

    def _recovery_add_entities(
        new_entities: Iterable[SwitchEntity], update_before_add: bool = True
    ) -> None:
        if primary_scheduler is None:
            return
        primary_scheduler(new_entities, update_before_add)

    recovery_manager.register_switch_platform(
        expected_unique_ids=_expected_unique_ids,
        entity_factory=_build_missing,
        add_entities=_recovery_add_entities,
    )


class GoogleFindMyAutomaticPollingSwitch(GoogleFindMyDeviceEntity, SwitchEntity):
    """Turn automatic location requests for one tracker on or off."""

    entity_description = AUTOMATIC_POLLING_DESCRIPTION
    _attr_entity_registry_enabled_default = True
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        fallback_label: str | None,
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        super().__init__(
            coordinator,
            device,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
            fallback_label=fallback_label,
        )
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            self.entry_id,
            subentry_identifier,
            self.device_id,
            AUTOMATIC_POLLING_KEY,
            separator="_",
        )

    @property
    def available(self) -> bool:
        """Available while the tracker is known, present or not."""
        return self.coordinator_has_device()

    @property
    def is_on(self) -> bool:
        """Return True while automatic location requests are enabled."""
        return bool(self.coordinator.is_automatic_polling_enabled(self.device_id))

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Enable automatic location requests for the tracker."""
        self.coordinator.async_set_automatic_polling(self.device_id, True)
        self.async_write_ha_state()

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Disable automatic location requests for the tracker."""
        self.coordinator.async_set_automatic_polling(self.device_id, False)
        self.async_write_ha_state()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Refresh device metadata and propagate state updates."""
        self.refresh_device_label_from_coordinator(log_prefix="AutomaticPolling")
        self.async_write_ha_state()
