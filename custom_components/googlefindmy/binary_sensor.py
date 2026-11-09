# custom_components/googlefindmy/binary_sensor.py
"""Binary sensor entities for the Google Find My Device integration.

This module provides *diagnostic* binary sensors that live under the
per-entry **service device** (see `const.service_device_identifier`).
Sensors are intentionally light-weight and derive their state from the
integration's central `GoogleFindMyCoordinator` and, where appropriate,
from Home Assistant's system facilities (e.g., the Repairs issue registry).

Design goals:
- Keep all network I/O **out** of entity code; entities are consumers of
  coordinator state and HA registries only.
- Use stable, entry-scoped `unique_id`s to support multi-account setups:
  "<entry_id>:<sensor_key>" (e.g., "abcd1234:polling").
- Route all sensors to the **service device** so users find diagnostics in
  one place.
- Prefer translation keys over hardcoded names/icons where supported.

Provided sensors:
- `polling` (diagnostic): `on` while a sequential polling cycle runs.
- `auth_status` (diagnostic): `on` when an authentication problem exists
  (active Repairs issue or recent auth-error event for this entry).
- `connectivity` (diagnostic): `on` when the FCM push transport is connected and
  ready for Nova responses.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import Event, HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import EntityRecoveryManager
from .const import (
    DATA_EID_RESOLVER,
    DOMAIN,
    EVENT_AUTH_ERROR,
    EVENT_AUTH_OK,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_SUBENTRY_KEY,
    TRANSLATION_KEY_AUTH_STATUS,
    issue_id_for,
)
from .coordinator import GoogleFindMyCoordinator, format_epoch_utc
from .entity import (
    GoogleFindMyDeviceEntity,
    GoogleFindMyEntity,
    ensure_config_subentry_id,
    ensure_dispatcher_dependencies,
    known_ids_for_subentry_type,
    resolve_coordinator,
    sanitize_state_text,
    schedule_add_entities,
)
from .entity import (
    subentry_type as _subentry_type,
)
from .ha_typing import BinarySensorEntity, callback

if TYPE_CHECKING:
    from .eid_resolver import GoogleFindMyEIDResolver

_LOGGER = logging.getLogger(__name__)

# Read-only mirror of coordinator state; no I/O performed per-entity.
PARALLEL_UPDATES = 0

CONNECTIVITY_DEVICE_CLASS = getattr(BinarySensorDeviceClass, "CONNECTIVITY", None)


class _ServiceScope(NamedTuple):
    """Resolved service subentry scope for entity creation."""

    subentry_key: str
    config_subentry_id: str | None
    identifier: str


# --------------------------------------------------------------------------------------
# Entity descriptions
# --------------------------------------------------------------------------------------
POLLING_DESC = BinarySensorEntityDescription(
    key="polling",
    translation_key="polling",
    icon="mdi:refresh",
    entity_category=EntityCategory.DIAGNOSTIC,
)

AUTH_STATUS_DESC = BinarySensorEntityDescription(
    key="auth_status",
    translation_key=TRANSLATION_KEY_AUTH_STATUS,
    device_class=BinarySensorDeviceClass.PROBLEM,  # True => problem present
    icon="mdi:account-alert",
    entity_category=EntityCategory.DIAGNOSTIC,
)

CONNECTIVITY_DESC = BinarySensorEntityDescription(
    key="connectivity",
    translation_key="connectivity",
    device_class=CONNECTIVITY_DEVICE_CLASS,
    entity_category=EntityCategory.DIAGNOSTIC,
)

UWT_MODE_DESC = BinarySensorEntityDescription(
    key="uwt_mode",
    translation_key="uwt_mode",
    entity_category=EntityCategory.DIAGNOSTIC,
)


# --------------------------------------------------------------------------------------
# Platform setup
# --------------------------------------------------------------------------------------
async def async_setup_entry(  # noqa: PLR0915
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    *,
    config_subentry_id: str | None = None,
) -> None:
    """Set up Google Find My Device binary sensor entities (per config entry).

    Registers both diagnostic sensors under the per-entry service device.
    """
    coordinator = resolve_coordinator(entry)
    ensure_dispatcher_dependencies(hass)
    if getattr(coordinator, "config_entry", None) is None:
        coordinator.config_entry = entry

    def _collect_service_scopes(
        hint_subentry_id: str | None = None,
        forwarded_config_id: str | None = None,
    ) -> list[_ServiceScope]:
        scopes: dict[str, _ServiceScope] = {}

        subentry_metas = getattr(coordinator, "_subentry_metadata", None)
        if isinstance(subentry_metas, Mapping):
            for key, meta in subentry_metas.items():
                meta_features = getattr(meta, "features", ())
                if "binary_sensor" not in meta_features:
                    continue

                stable_identifier = getattr(meta, "stable_identifier", None)
                identifier = (
                    stable_identifier()
                    if callable(stable_identifier)
                    else None
                    or getattr(meta, "config_subentry_id", None)
                    or coordinator.stable_subentry_identifier(key=key)
                )
                scopes[identifier] = _ServiceScope(
                    key,
                    getattr(meta, "config_subentry_id", None),
                    identifier,
                )

        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, Mapping):
            for subentry in subentries.values():
                data = getattr(subentry, "data", {})
                group_key = SERVICE_SUBENTRY_KEY
                subentry_features: Iterable[Any] = ()
                if isinstance(data, Mapping):
                    group_key = data.get("group_key", group_key)
                    subentry_features = data.get("features", ())

                if "binary_sensor" not in subentry_features:
                    continue

                config_id = getattr(subentry, "subentry_id", None) or getattr(
                    subentry, "entry_id", None
                )
                identifier = (
                    config_id
                    or coordinator.stable_subentry_identifier(key=group_key)
                    or SERVICE_SUBENTRY_KEY
                )
                scopes.setdefault(
                    identifier,
                    _ServiceScope(
                        group_key or SERVICE_SUBENTRY_KEY, config_id, identifier
                    ),
                )

        if hint_subentry_id:
            identifier = hint_subentry_id
            scopes.setdefault(
                identifier,
                _ServiceScope(
                    SERVICE_SUBENTRY_KEY,
                    forwarded_config_id or hint_subentry_id,
                    identifier,
                ),
            )

        if scopes:
            return list(scopes.values())

        fallback_identifier = coordinator.stable_subentry_identifier(
            feature="binary_sensor"
        )
        return [
            _ServiceScope(
                SERVICE_SUBENTRY_KEY,
                forwarded_config_id,
                fallback_identifier,
            )
        ]

    added_unique_ids: set[str] = set()
    primary_scope: _ServiceScope | None = None
    primary_scheduler: Callable[[Iterable[BinarySensorEntity], bool], None] | None = (
        None
    )

    def _add_scope(scope: _ServiceScope, forwarded_config_id: str | None) -> None:
        nonlocal primary_scope, primary_scheduler
        service_ids = known_ids_for_subentry_type(entry, SUBENTRY_TYPE_SERVICE)
        sanitized_config_id = ensure_config_subentry_id(
            entry,
            "binary_sensor",
            scope.config_subentry_id or forwarded_config_id,
            known_ids=service_ids,
        )
        if sanitized_config_id is None:
            if service_ids:
                _LOGGER.debug(
                    "Binary sensor setup: skipping subentry '%s' because the config_subentry_id is unknown",
                    forwarded_config_id
                    or scope.config_subentry_id
                    or scope.subentry_key,
                )
                return
            sanitized_config_id = scope.identifier or scope.subentry_key
            _LOGGER.debug(
                "Binary sensor setup: synthesized config_subentry_id '%s' for key '%s'",
                sanitized_config_id,
                scope.subentry_key,
            )

        subentry_identifier = scope.identifier or sanitized_config_id

        def _schedule_service_entities(
            new_entities: Iterable[BinarySensorEntity],
            update_before_add: bool = True,
        ) -> None:
            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=new_entities,
                update_before_add=update_before_add,
                config_subentry_id=sanitized_config_id,
                log_owner="Binary sensor setup",
                logger=_LOGGER,
            )

        if primary_scope is None:
            primary_scope = scope
        if primary_scheduler is None:
            primary_scheduler = _schedule_service_entities

        entities: list[BinarySensorEntity] = [
            GoogleFindMyPollingSensor(
                coordinator,
                entry,
                subentry_key=scope.subentry_key,
                subentry_identifier=subentry_identifier,
            ),
            GoogleFindMyAuthStatusSensor(
                coordinator,
                entry,
                subentry_key=scope.subentry_key,
                subentry_identifier=subentry_identifier,
            ),
            GoogleFindMyConnectivitySensor(
                coordinator,
                entry,
                subentry_key=scope.subentry_key,
                subentry_identifier=subentry_identifier,
            ),
        ]

        deduped_entities: list[BinarySensorEntity] = []
        for entity in entities:
            unique_id = getattr(entity, "unique_id", None)
            if isinstance(unique_id, str) and unique_id in added_unique_ids:
                continue
            if isinstance(unique_id, str):
                added_unique_ids.add(unique_id)
            deduped_entities.append(entity)

        if not deduped_entities:
            _schedule_service_entities([], True)
            return

        _LOGGER.debug(
            "Binary sensor setup: subentry_key=%s, config_subentry_id=%s",
            scope.subentry_key,
            sanitized_config_id,
        )
        _schedule_service_entities(deduped_entities, True)

    # ---- Per-device tracker scope: UWT-Mode binary sensors ----
    processed_tracker_identifiers: set[str] = set()
    known_uwt_ids: set[str] = set()

    def _get_ble_resolver() -> GoogleFindMyEIDResolver | None:
        """Return the EID resolver from hass.data, or None."""
        domain_data = hass.data.get(DOMAIN)
        if not isinstance(domain_data, dict):
            return None
        return cast(
            "GoogleFindMyEIDResolver | None", domain_data.get(DATA_EID_RESOLVER)
        )

    def _add_tracker_scope(  # noqa: PLR0915
        tracker_key: str,
        forwarded_config_id: str | None,
    ) -> None:
        """Create per-device UWT binary sensors for a tracker subentry."""
        tracker_ids = known_ids_for_subentry_type(entry, SUBENTRY_TYPE_TRACKER)
        sanitized_config_id = ensure_config_subentry_id(
            entry,
            "binary_sensor_tracker",
            forwarded_config_id,
            known_ids=tracker_ids,
        )
        if sanitized_config_id is None:
            if tracker_ids:
                return
            sanitized_config_id = forwarded_config_id or tracker_key

        tracker_identifier = sanitized_config_id or tracker_key
        if tracker_identifier in processed_tracker_identifiers:
            return
        processed_tracker_identifiers.add(tracker_identifier)

        def _schedule_tracker_entities(
            new_entities: Iterable[BinarySensorEntity],
            update_before_add: bool = True,
        ) -> None:
            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=new_entities,
                update_before_add=update_before_add,
                config_subentry_id=sanitized_config_id,
                log_owner="Binary sensor setup (tracker)",
                logger=_LOGGER,
            )

        def _build_uwt_entities() -> list[BinarySensorEntity]:
            """Build UWT-Mode binary sensors for devices with BLE data.

            A device whose UWT sensor the entity registry already holds gets it
            at once, even without BLE data or a resolver.
            """
            entities: list[BinarySensorEntity] = []
            resolver = _get_ble_resolver()
            for device in coordinator.get_subentry_snapshot(tracker_key):
                dev_id = device.get("id") if isinstance(device, Mapping) else None
                dev_name = device.get("name") if isinstance(device, Mapping) else None
                if not dev_id or not dev_name:
                    continue
                if dev_id in known_uwt_ids:
                    continue

                visible = True
                is_visible = getattr(coordinator, "is_device_visible_in_subentry", None)
                if callable(is_visible):
                    try:
                        visible = bool(is_visible(tracker_key, dev_id))
                    except Exception:  # pragma: no cover
                        visible = True
                if not visible:
                    continue

                battery_state = None
                if resolver is not None:
                    try:
                        battery_state = resolver.get_ble_battery_state(dev_id)
                    except Exception:  # noqa: BLE001
                        pass

                uwt_entity = GoogleFindMyUWTModeSensor(
                    coordinator,
                    device,
                    subentry_key=tracker_key,
                    subentry_identifier=tracker_identifier,
                )
                uwt_uid = getattr(uwt_entity, "unique_id", None)
                in_registry = (
                    isinstance(uwt_uid, str)
                    and er.async_get(hass).async_get_entity_id(
                        "binary_sensor", DOMAIN, uwt_uid
                    )
                    is not None
                )
                if (
                    (battery_state is not None or in_registry)
                    and isinstance(uwt_uid, str)
                    and uwt_uid not in added_unique_ids
                ):
                    added_unique_ids.add(uwt_uid)
                    known_uwt_ids.add(dev_id)
                    entities.append(uwt_entity)
                    if battery_state is not None:
                        _LOGGER.info(
                            "UWT-Mode binary sensor created for device=%s "
                            "(uwt_mode=%s)",
                            dev_id,
                            battery_state.uwt_mode,
                        )
                    else:
                        _LOGGER.debug(
                            "UWT-Mode binary sensor created from entity registry "
                            "for device=%s",
                            dev_id,
                        )
            return entities

        initial = _build_uwt_entities()
        if initial:
            _schedule_tracker_entities(initial, True)
        else:
            _schedule_tracker_entities([], True)

        @callback
        def _add_new_uwt_devices() -> None:
            new_entities = _build_uwt_entities()
            if new_entities:
                _LOGGER.debug(
                    "Binary sensor: dynamically adding %d UWT entity(ies)",
                    len(new_entities),
                )
                _schedule_tracker_entities(new_entities, True)

        unsub = coordinator.async_add_listener(_add_new_uwt_devices)
        entry.async_on_unload(unsub)

    seen_subentries: set[str | None] = set()

    @callback
    def async_add_subentry(subentry: Any | None = None) -> None:
        subentry_identifier = None
        if isinstance(subentry, str):
            subentry_identifier = subentry
        else:
            subentry_identifier = getattr(subentry, "subentry_id", None) or getattr(
                subentry, "entry_id", None
            )

        subentry_type = _subentry_type(subentry)
        if subentry_type is not None and subentry_type not in ("service", "tracker"):
            _LOGGER.debug(
                "Binary sensor setup skipped for unrelated subentry '%s' (type '%s')",
                subentry_identifier,
                subentry_type,
            )
            return

        if subentry_identifier in seen_subentries:
            return
        seen_subentries.add(subentry_identifier)

        if subentry_type != "tracker":
            for scope in _collect_service_scopes(
                subentry_identifier, forwarded_config_id=subentry_identifier
            ):
                _add_scope(scope, subentry_identifier)

        # Per-device UWT sensors for tracker subentries (or untyped).
        if subentry_type in (None, "tracker"):
            tracker_key = TRACKER_SUBENTRY_KEY
            subentries = getattr(entry, "subentries", None)
            # Read the group key of the subentry being set up; a bare id is
            # resolved through the entry. Only an untyped call falls back to
            # the first tracker subentry.
            own = subentry
            if isinstance(subentry, str) and isinstance(subentries, Mapping):
                own = subentries.get(subentry)
            own_data = getattr(own, "data", None)
            if _subentry_type(own) == "tracker" and isinstance(own_data, Mapping):
                tracker_key = own_data.get("group_key", TRACKER_SUBENTRY_KEY)
            elif isinstance(subentries, Mapping):
                for sub in subentries.values():
                    if _subentry_type(sub) == "tracker":
                        data = getattr(sub, "data", None)
                        if isinstance(data, Mapping):
                            tracker_key = data.get("group_key", TRACKER_SUBENTRY_KEY)
                        break
            _add_tracker_scope(tracker_key, subentry_identifier)

    runtime_data = getattr(entry, "runtime_data", None)

    subentry_manager = getattr(runtime_data, "subentry_manager", None)
    managed_subentries = getattr(subentry_manager, "managed_subentries", None)
    if isinstance(managed_subentries, Mapping) and managed_subentries:
        for managed_subentry in managed_subentries.values():
            async_add_subentry(managed_subentry)
    elif isinstance(getattr(entry, "subentries", None), Mapping):
        if entry.subentries:
            for managed_subentry in entry.subentries.values():
                async_add_subentry(managed_subentry)
        else:
            async_add_subentry(config_subentry_id)
    else:
        async_add_subentry(config_subentry_id)

    entry.async_on_unload(
        async_dispatcher_connect(
            hass, f"{DOMAIN}_subentry_setup_{entry.entry_id}", async_add_subentry
        )
    )

    recovery_manager = getattr(runtime_data, "entity_recovery_manager", None)

    if isinstance(recovery_manager, EntityRecoveryManager):
        entry_id = getattr(entry, "entry_id", None)
        service_subentry_identifier = (
            primary_scope.identifier if primary_scope is not None else None
        )
        service_subentry_key = (
            primary_scope.subentry_key
            if primary_scope is not None
            else SERVICE_SUBENTRY_KEY
        )

        def _recovery_add_entities(
            new_entities: Iterable[BinarySensorEntity],
            update_before_add: bool = True,
        ) -> None:
            if primary_scheduler is None:
                return
            primary_scheduler(new_entities, update_before_add)

        def _expected_unique_ids() -> set[str]:
            if not isinstance(entry_id, str) or not entry_id:
                return set()
            if (
                not isinstance(service_subentry_identifier, str)
                or not service_subentry_identifier
            ):
                return set()
            return {
                f"{entry_id}:{service_subentry_identifier}:polling",
                f"{entry_id}:{service_subentry_identifier}:auth_status",
                f"{entry_id}:{service_subentry_identifier}:connectivity",
            }

        def _build_entities(missing: set[str]) -> list[BinarySensorEntity]:
            if not missing:
                return []
            built: list[BinarySensorEntity] = []
            if not isinstance(entry_id, str) or not entry_id:
                return built
            if (
                not isinstance(service_subentry_identifier, str)
                or not service_subentry_identifier
            ):
                return built
            mapping: dict[str, Callable[[], BinarySensorEntity]] = {
                f"{entry_id}:{service_subentry_identifier}:polling": lambda: (
                    GoogleFindMyPollingSensor(
                        coordinator,
                        entry,
                        subentry_key=service_subentry_key,
                        subentry_identifier=service_subentry_identifier,
                    )
                ),
                f"{entry_id}:{service_subentry_identifier}:auth_status": lambda: (
                    GoogleFindMyAuthStatusSensor(
                        coordinator,
                        entry,
                        subentry_key=service_subentry_key,
                        subentry_identifier=service_subentry_identifier,
                    )
                ),
                f"{entry_id}:{service_subentry_identifier}:connectivity": lambda: (
                    GoogleFindMyConnectivitySensor(
                        coordinator,
                        entry,
                        subentry_key=service_subentry_key,
                        subentry_identifier=service_subentry_identifier,
                    )
                ),
            }
            for unique_id, factory in mapping.items():
                if unique_id in missing:
                    built.append(factory())
            return built

        recovery_manager.register_binary_sensor_platform(
            expected_unique_ids=_expected_unique_ids,
            entity_factory=_build_entities,
            add_entities=_recovery_add_entities,
        )


# --------------------------------------------------------------------------------------
# Polling sensor
# --------------------------------------------------------------------------------------
class GoogleFindMyPollingSensor(GoogleFindMyEntity, BinarySensorEntity):
    """Binary sensor indicating whether a background sequential polling cycle is active.

    Semantics:
        - `on`  → a sequential device polling cycle is currently in progress.
        - `off` → no sequential poll is running at the moment.
    """

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    entity_description = POLLING_DESC

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        entry: ConfigEntry,
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the polling sensor."""
        super().__init__(
            coordinator,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        self._entry_id = entry.entry_id
        entry_id = self.entry_id
        # Entry-scoped unique_id: "<entry_id>:<subentry_identifier>:polling"
        self._attr_unique_id = self.build_unique_id(
            entry_id,
            subentry_identifier,
            "polling",
        )

    @property
    def is_on(self) -> bool:
        """Return True if a polling cycle is currently running."""
        public_val = getattr(self.coordinator, "is_polling", None)
        if isinstance(public_val, bool):
            return public_val
        # Legacy fallback (older coordinator builds)
        return bool(getattr(self.coordinator, "_is_polling", False))

    @property
    def icon(self) -> str:
        """Return a dynamic icon reflecting the state (visual feedback in UI)."""
        return "mdi:sync" if self.is_on else "mdi:sync-off"

    @property
    def available(self) -> bool:
        """Polling diagnostic sensor stays online to expose status information."""

        return bool(super().available)

    @callback
    def _handle_coordinator_update(self) -> None:
        """Refresh Home Assistant state when coordinator data changes."""

        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Attach the sensor to the per-entry service device."""
        # Single service device per config entry:
        # identifiers -> (DOMAIN, f"integration_<entry_id>")
        return self.service_device_info(include_subentry_identifier=True)


# --------------------------------------------------------------------------------------
# Authentication status sensor
# --------------------------------------------------------------------------------------
class GoogleFindMyAuthStatusSensor(GoogleFindMyEntity, BinarySensorEntity):
    """Binary sensor indicating whether user action is required to re-authenticate.

    Semantics (device_class=problem):
        - `on`  → Authentication problem detected for this config entry
                  (e.g., invalid/expired token). User action is required.
        - `off` → No active authentication problem known.
    """

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    entity_description = AUTH_STATUS_DESC

    # Internal event-driven state (None -> unknown, True -> problem, False -> ok)
    _event_state: bool | None
    _unsub_err: Callable[[], None] | None
    _unsub_ok: Callable[[], None] | None

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        entry: ConfigEntry,
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the authentication status sensor."""
        super().__init__(
            coordinator,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        self._entry_id = entry.entry_id
        entry_id = self.entry_id
        # Entry-scoped unique_id: "<entry_id>:<subentry_identifier>:auth_status"
        self._attr_unique_id = self.build_unique_id(
            entry_id,
            subentry_identifier,
            "auth_status",
        )
        self._event_state = None
        self._unsub_err = None
        self._unsub_ok = None

    # ----------------------- HA lifecycle hooks -----------------------
    async def async_added_to_hass(self) -> None:
        """Subscribe to auth events when the entity is added to Home Assistant."""
        await super().async_added_to_hass()

        @callback
        def _on_auth_error(event: Event) -> None:
            # Only process events for *this* config entry
            if event.data.get("entry_id") == self._entry_id:
                self._event_state = True
                _LOGGER.debug(
                    "Auth error event received for entry %s; setting problem=True",
                    self._entry_id,
                )
                self.async_write_ha_state()

        @callback
        def _on_auth_ok(event: Event) -> None:
            if event.data.get("entry_id") == self._entry_id:
                self._event_state = False
                _LOGGER.debug(
                    "Auth ok event received for entry %s; setting problem=False",
                    self._entry_id,
                )
                self.async_write_ha_state()

        # Register listeners and keep unsubscribe callables
        self._unsub_err = self.hass.bus.async_listen(EVENT_AUTH_ERROR, _on_auth_error)
        self._unsub_ok = self.hass.bus.async_listen(EVENT_AUTH_OK, _on_auth_ok)

    async def async_will_remove_from_hass(self) -> None:
        """Unsubscribe from auth events when the entity is removed."""
        await super().async_will_remove_from_hass()
        for unsub in (self._unsub_err, self._unsub_ok):
            if unsub:
                try:
                    unsub()
                except Exception:  # defensive
                    pass
        self._unsub_err = None
        self._unsub_ok = None

    # ----------------------- State calculation ------------------------
    @property
    def is_on(self) -> bool:
        """Return True if an authentication problem exists.

        Resolution order:
        1) If we have an event-driven state (_event_state is not None), prefer it.
        2) Otherwise, query the Repairs issue registry for the per-entry issue.
           Existence of the issue is interpreted as "problem = True".
        """
        # 1) Event-driven fast path
        if self._event_state is not None:
            return bool(self._event_state)

        # 2) Repairs issue registry fallback (persistent source of truth)
        reg = ir.async_get(self.hass)
        issue = reg.async_get_issue(DOMAIN, issue_id_for(self._entry_id))
        return issue is not None

    @property
    def icon(self) -> str:
        """Return a dynamic icon to communicate the current auth state."""
        # Keep explicit icons for clarity, even with device_class=problem.
        return "mdi:account-alert" if self.is_on else "mdi:account-check"

    @property
    def available(self) -> bool:
        """Auth status diagnostics remain available even if polling fails."""

        return True

    @property
    def extra_state_attributes(self) -> dict[str, str | None] | None:
        """Expose Nova API and push transport health snapshots."""

        attributes: dict[str, str | None] = {}

        status = getattr(self.coordinator, "api_status", None)
        state = getattr(status, "state", None)
        if isinstance(state, str):
            attributes["nova_api_status"] = state
        reason = getattr(status, "reason", None)
        if isinstance(reason, str) and reason:
            attributes["nova_api_status_reason"] = sanitize_state_text(reason)
        changed_at = getattr(status, "changed_at", None)
        changed_at_iso = format_epoch_utc(changed_at)
        if changed_at_iso is not None:
            attributes["nova_api_status_changed_at"] = changed_at_iso

        fcm_status = getattr(self.coordinator, "fcm_status", None)
        fcm_state = getattr(fcm_status, "state", None)
        if isinstance(fcm_state, str):
            attributes["nova_fcm_status"] = fcm_state
        fcm_reason = getattr(fcm_status, "reason", None)
        if isinstance(fcm_reason, str) and fcm_reason:
            attributes["nova_fcm_status_reason"] = sanitize_state_text(fcm_reason)
        fcm_changed_at = getattr(fcm_status, "changed_at", None)
        fcm_changed_at_iso = format_epoch_utc(fcm_changed_at)
        if fcm_changed_at_iso is not None:
            attributes["nova_fcm_status_changed_at"] = fcm_changed_at_iso

        return attributes or None

    @property
    def device_info(self) -> DeviceInfo:
        """Attach the sensor to the per-entry service device."""
        return self.service_device_info(include_subentry_identifier=True)


# --------------------------------------------------------------------------------------
# Connectivity sensor
# --------------------------------------------------------------------------------------
class GoogleFindMyConnectivitySensor(GoogleFindMyEntity, BinarySensorEntity):
    """Binary sensor reflecting push transport connectivity for Nova responses."""

    _attr_has_entity_name = True
    _attr_device_class = CONNECTIVITY_DEVICE_CLASS
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    entity_description = CONNECTIVITY_DESC

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        entry: ConfigEntry,
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the connectivity sensor."""

        super().__init__(
            coordinator,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        entry_id = self.entry_id or entry.entry_id
        self._attr_unique_id = self.build_unique_id(
            entry_id,
            subentry_identifier,
            "connectivity",
        )

    @property
    def is_on(self) -> bool:
        """Return True when the push transport reports as ready."""

        api = getattr(self.coordinator, "api", None)
        fn = getattr(api, "is_push_ready", None)
        if callable(fn):
            try:
                return bool(fn())
            except Exception:
                return False

        ready_flag = getattr(self.coordinator, "is_fcm_connected", None)
        if isinstance(ready_flag, bool):
            return ready_flag

        return False

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Expose Nova polling and FCM connection diagnostics."""

        attributes: dict[str, Any] = {}

        last_result = getattr(self.coordinator, "last_poll_result", None)
        if isinstance(last_result, str):
            attributes["last_poll_result"] = last_result

        timeouts = getattr(self.coordinator, "consecutive_timeouts", None)
        if isinstance(timeouts, int):
            attributes["consecutive_timeouts"] = timeouts

        api = getattr(self.coordinator, "api", None)
        fcm = getattr(api, "fcm", None)
        connected_at = None
        entry_id = self.entry_id
        getter = getattr(fcm, "get_last_connected_wall_time", None)
        if callable(getter):
            try:
                connected_at = getter(entry_id)
            except Exception:
                connected_at = None

        if connected_at is None and bool(
            getattr(self.coordinator, "is_fcm_connected", False)
        ):
            fcm_status = getattr(self.coordinator, "fcm_status", None)
            connected_at = getattr(fcm_status, "changed_at", None)

        connected_at_iso = format_epoch_utc(connected_at)
        if connected_at_iso is not None:
            attributes["fcm_connected_at"] = connected_at_iso

        fatal_error: str | None = None
        fatal_by_entry = getattr(fcm, "_fatal_errors", None)
        if isinstance(fatal_by_entry, Mapping) and entry_id:
            fatal_error = fatal_by_entry.get(entry_id)
        fatal_error = fatal_error or getattr(fcm, "_fatal_error", None)
        if isinstance(fatal_error, str) and fatal_error:
            attributes["fcm_fatal_error"] = sanitize_state_text(fatal_error)

        return attributes or None

    @property
    def device_info(self) -> DeviceInfo:
        """Attach the sensor to the per-entry service device."""

        return self.service_device_info(include_subentry_identifier=True)


# --------------------------------------------------------------------------------------
# Per-device UWT-Mode sensor
# --------------------------------------------------------------------------------------
class GoogleFindMyUWTModeSensor(GoogleFindMyDeviceEntity, BinarySensorEntity):
    """Per-device binary sensor for the FMDN unwanted tracking protection mode.

    Semantics (per the Find Hub Network Accessory Specification, sections
    "Hashed flags" and "Unwanted tracking prevention", retrieved 2026-09-03:
    https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn):
        - ``on``  → The beacon reports that unwanted tracking protection mode
          is active: spec bit 7 of the hashed-flags byte is set. Note that the
          specification numbers hashed-flags bits MSB-first, so spec bit 7 is
          standard bit 0; the resolver mask is therefore ``0x01`` and not
          ``0x80`` (see ``eid_resolver.py``, which marks ``0x80`` as an
          anti-pattern).
        - ``off`` → The beacon reports normal operation.
        - ``None`` → No hashed-flags byte has been decoded for this device.
          The byte is optional: a beacon without battery indication that is not
          in unwanted tracking protection mode may omit it entirely.

    How the mode is entered and left. The specification maps it onto DULT
    verbatim: '"Unwanted tracking protection mode" defined in this document
    maps to the "separated state" defined by the DULT spec', and certified
    devices "must also meet the requirements in the implementation version of
    the cross-platform specification for Detecting Unwanted Location Trackers
    (DULT)". Who switches the mode is stated in the Find Hub specification
    itself (section "Unwanted tracking protection mode", retrieved 2026-10-11): "The
    Find Hub service can relay an unwanted tracking protection mode activation
    request through the Find Hub network". Activation (Data ID 0x07 on the
    Beacon Actions characteristic) is authenticated with the unwanted tracking
    protection key; deactivation (0x08) additionally requires that "The hashed
    ephemeral identity key matches the current ephemeral identity key". With
    Find Hub the service can therefore turn the mode on (0x07, relayed through
    the Find Hub network), and only a holder of the identity key can turn it
    off (0x08). In practice that holder is the owner's phone; that is an
    INFERENCE, not a specification statement. DULT's 30 minutes bound the
    separation criterion: the accessory "SHALL transition from near-owner mode
    to separated mode" once it is "physically separated from the owner device
    for more than 30 minutes", and back on reunion
    (``draft-detecting-unwanted-location-trackers-01``, sections 3.4.4 and
    3.4.5). Google's server criteria for sending the activation are not
    published, and other vendors' firmware is not public. Two consequences for
    automations. The mode can appear with no command sent from this
    installation, because the Find Hub service can request it. And the bit
    carries no duration: it cannot tell how long a tag has been away from its
    owner.
    Field data from real installations: ``docs/UWT_FIELD_DATA.md``.

    Two 24-hour figures exist and they are not the same figure:
        - DULT ``T_(SEPARATED_UT_TIMEOUT)``, a "random value between 8-24 hours
          chosen from a uniform distribution", after which the accessory "MUST
          enable the motion detector" while separated and then plays a sound on
          detected motion (DULT section 3.12.2.1 with table 16; section 3.4.4
          carries the state transition only, not the detector). This is
          normative, and it gates the motion-triggered chime, not the setting of
          this bit. Two earlier revisions of this docstring misplaced the figure
          in opposite directions; both are withdrawn, and the correction of
          record is the attribution stated here (BSkando#210). Neither is
          restated, so the guards in
          ``tests/test_uwt_mode_binary_sensor.py`` need no exception for this
          paragraph.
        - The Find Hub specification's own "24 hours", which is the reduced MAC
          address rotation period WHILE the mode is active.

    A separation-based anti-stalking chime is therefore device behaviour in
    exactly the state this bit reports, so the two are related rather than
    unrelated. What the bit does not carry is whether the motion detector is
    already armed: that depends on ``T_(SEPARATED_UT_TIMEOUT)``, which is not
    observable from the advertisement. This sensor is consequently not a
    separation timer and cannot be turned into one.

    Why the mode is nonetheless relevant to reports of spurious ringing:
    activation (Data ID 0x07) takes an optional control flag, ``0x01``
    "Skip ringing authentication", specified as "When set, ringing requests
    aren't authenticated while in unwanted tracking protection mode" (section
    "Beacon Actions", retrieved 2026-08-05). A beacon activated with that flag
    set still expects authentication data on a ring request but no longer
    verifies it, so while the mode is active the OWNER ring path is reachable
    without the ring key by any party in Bluetooth range. Two caveats, both
    load-bearing:

        - The likelier explanation for the existing reports is the DULT
          non-owner sound path, which is unauthenticated by design and needs no
          such flag (see ``docs/PLAY_SOUND_ARCHITECTURE.md``). The flag is
          documented here because it is the only mechanism that also removes
          authentication from the OWNER path, which the DULT explanation does
          not. Neither is asserted as the cause of any particular report.
        - The bit itself is not yet trustworthy as a trigger. Episodes of 38
          and 57 seconds and same-second flips across two devices at home are
          on record (BSkando#210). Treat this sensor as context for a report,
          never as an alerting signal.

    What this does establish: a chime with no cloud request behind it is
    specification-conformant and does not imply that this integration sent
    anything (BSkando#195, BSkando#108). This integration has no GATT write
    path at all. The advertisement carries the mode, never the flag, so the
    sensor can narrow such a report down but cannot confirm a cause.

    Data source: spec bit 7 (standard bit 0) of the FMDN hashed-flags byte,
    decoded by the EID resolver as :pyattr:`BLEBatteryState.uwt_mode`.

    Created at setup when the entity registry already holds its unique_id,
    otherwise alongside the BLE battery sensor when the resolver first decodes
    hashed-flags data for a device; a new device without BLE data gets no UWT
    sensor. Nothing is restored: until the first decoded flags byte the sensor
    reports ``None`` (``unknown``) while the device is present and is
    unavailable while it is absent.
    """

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    entity_description = UWT_MODE_DESC

    _unrecorded_attributes = frozenset(
        {
            "last_ble_observation",
            "google_device_id",
        }
    )

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the UWT-Mode binary sensor."""
        super().__init__(
            coordinator,
            device,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
            fallback_label=device.get("name"),
        )
        self._device_id: str | None = device.get("id")
        safe_id = self._device_id if self._device_id is not None else "unknown"
        entry_id = self.entry_id or "default"
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            entry_id,
            subentry_identifier,
            f"{safe_id}_uwt_mode",
            separator="_",
        )

    def _get_resolver(self) -> GoogleFindMyEIDResolver | None:
        """Return the EID resolver from hass.data, or None."""
        domain_data = self.hass.data.get(DOMAIN)
        if not isinstance(domain_data, dict):
            return None
        return cast(
            "GoogleFindMyEIDResolver | None", domain_data.get(DATA_EID_RESOLVER)
        )

    @property
    def is_on(self) -> bool | None:
        """Return True when the beacon reports unwanted tracking protection mode.

        ``None`` means "no hashed-flags byte decoded yet", not "off": the byte
        is optional per specification.

        No BLE staleness TTL: the last decoded value is reported for as long as
        the coordinator's TTL-smoothed presence holds. Rationale, and why this
        is a decision rather than an oversight: the hashed-flags byte is
        optional and may be sent rarely, so a short TTL would flap; and there is
        no measurement in this project that would justify any particular TTL
        value. Ageing is therefore left to the consumer, which is why
        ``last_ble_observation`` is exposed as an attribute. Revisit when field
        data from FMDN_FLAGS_PROBE exists (BSkando#210).
        """
        resolver = self._get_resolver()
        if resolver is None or self._device_id is None:
            return None
        state = resolver.get_ble_battery_state(self._device_id)
        if state is None:
            return None
        return state.uwt_mode

    @property
    def icon(self) -> str:
        """Return the icon for the reported unwanted tracking protection mode.

        ``mdi:shield-alert`` when the mode is reported active, ``mdi:shield-check``
        otherwise. The undecoded ``None`` case is deliberately rendered as "no
        alert" rather than as an alarm.
        """
        return "mdi:shield-alert" if self.is_on else "mdi:shield-check"

    @property
    def available(self) -> bool:
        """Return True when the coordinator considers the device present.

        Availability follows the coordinator's TTL-smoothed presence only; it
        carries no separate staleness rule for the decoded flags byte. See
        :pyattr:`is_on` for why that absence is a decision rather than an
        oversight.
        """
        if not super().available:
            return False
        if not self.coordinator_has_device():
            return False
        try:
            if self._device_id is not None and hasattr(
                self.coordinator, "is_device_present"
            ):
                return bool(self.coordinator.is_device_present(self._device_id))
        except Exception:
            pass
        return True

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return diagnostic attributes (excluded from recorder)."""
        resolver = self._get_resolver()
        if resolver is None or self._device_id is None:
            return None
        state = resolver.get_ble_battery_state(self._device_id)
        if state is None:
            return None
        return {
            "last_ble_observation": datetime.fromtimestamp(
                state.observed_at_wall, tz=UTC
            ).isoformat(),
            "google_device_id": self._device_id,
        }

    @callback
    def _handle_coordinator_update(self) -> None:
        """Refresh Home Assistant state when coordinator data changes."""
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Attach the sensor to the per-device tracker device."""
        return super().device_info
