# custom_components/googlefindmy/sensor.py
"""Sensor entities for Google Find My Device integration.

Exposes:
- Per-device `last_seen` timestamp sensors (restore-friendly).
- Optional integration diagnostic counters (stats), toggled via options.

Best practices:
- Device names are synced from the coordinator once known; user-assigned names are never overwritten.
- No placeholder names are written to the device registry on cold boot.
- End devices rely on the coordinator's tracker subentry metadata; do not set
  manual `via_device` links.
- Sensors default to **enabled** so a fresh installation is immediately functional.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NamedTuple, Self, cast

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import ATTR_LATITUDE, ATTR_LONGITUDE, PERCENTAGE
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import EntityRecoveryManager
from .const import (
    DATA_EID_RESOLVER,
    DEFAULT_ENABLE_STATS_ENTITIES,
    DOMAIN,
    OPT_ENABLE_STATS_ENTITIES,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_SUBENTRY_KEY,
    TRANSLATION_KEY_ENCRYPTION_KEY_STATUS,
)
from .coordinator import (
    LOCATION_STATUS_OPTIONS,
    CryptoStatus,
    GoogleFindMyCoordinator,
    SemanticLabelRecord,
    _as_ha_attributes,
    classify_location_status,
    encode_plus_code_for_row,
    location_age_seconds,
    resolve_stale_threshold,
)
from .entity import (
    GoogleFindMyDeviceEntity,
    GoogleFindMyEntity,
    ensure_config_subentry_id,
    ensure_dispatcher_dependencies,
    known_ids_for_subentry_type,
    resolve_coordinator,
    schedule_add_entities,
)
from .entity import (
    subentry_type as _subentry_type,
)
from .ha_typing import RestoreSensor, SensorEntity, SensorExtraStoredData, callback

if TYPE_CHECKING:
    from .eid_resolver import BLEBatteryState, GoogleFindMyEIDResolver

_LOGGER = logging.getLogger(__name__)

# Read-only mirror of coordinator state; no I/O performed per-entity.
PARALLEL_UPDATES = 0


class _Scope(NamedTuple):
    """Resolved subentry scope for entity creation."""

    subentry_key: str
    config_subentry_id: str | None
    identifier: str


# ----------------------------- Entity Descriptions -----------------------------

LAST_SEEN_DESCRIPTION = SensorEntityDescription(
    key="last_seen",
    translation_key="last_seen",
    icon="mdi:clock-outline",
    device_class=SensorDeviceClass.TIMESTAMP,
)

PLUS_CODE_DESCRIPTION = SensorEntityDescription(
    key="plus_code",
    translation_key="plus_code",
    icon="mdi:map-marker",
)

# Icons come from icons.json (default plus one per state), not from here.
LOCATION_STATUS_DESCRIPTION = SensorEntityDescription(
    key="location_status",
    translation_key="location_status",
    device_class=SensorDeviceClass.ENUM,
    options=list(LOCATION_STATUS_OPTIONS),
)

# A TIMESTAMP sensor has no state_class (none is allowed), so no long-term
# statistics. Icon from icons.json. Diagnostic: the state is ``unknown`` most of
# the time (only a discarded coarse fix that is still fresh sets it), so it sits
# with the diagnostics instead of next to the location entities, where an
# ``unknown`` reads like a missing position. It stays enabled by default; see
# GoogleFindMyCoarseFixSensor.
COARSE_FIX_DESCRIPTION = SensorEntityDescription(
    key="coarse_fix",
    translation_key="coarse_fix",
    device_class=SensorDeviceClass.TIMESTAMP,
    entity_category=EntityCategory.DIAGNOSTIC,
)

SEMANTIC_LABEL_DESCRIPTION = SensorEntityDescription(
    key="semantic_labels",
    translation_key="semantic_labels",
    icon="mdi:format-list-text",
)

ENCRYPTION_KEY_STATUS_DESCRIPTION = SensorEntityDescription(
    key=TRANSLATION_KEY_ENCRYPTION_KEY_STATUS,
    translation_key=TRANSLATION_KEY_ENCRYPTION_KEY_STATUS,
    device_class=SensorDeviceClass.ENUM,
    icon="mdi:key-alert",
)

# The concrete enum states exposed by the encryption-key-status sensor. HA
# requires ``native_value`` to be one of ``options`` or ``None``; CryptoStatus
# UNKNOWN is deliberately omitted because it maps to HA's "unknown" (None), not
# an explicit option. Single source for the sensor and its tests.
ENCRYPTION_KEY_STATUS_OPTIONS: list[str] = [
    CryptoStatus.OK,
    CryptoStatus.SHARED_KEY_INVALID,
    CryptoStatus.SHARED_KEY_MISSING,
    CryptoStatus.TRACKER_KEY_OUTDATED,
]

BLE_BATTERY_DESCRIPTION = SensorEntityDescription(
    key="ble_battery",
    translation_key="ble_battery",
    device_class=SensorDeviceClass.BATTERY,
    native_unit_of_measurement=PERCENTAGE,
    state_class=SensorStateClass.MEASUREMENT,
    entity_category=EntityCategory.DIAGNOSTIC,
    suggested_display_precision=0,
    # No icon: SensorDeviceClass.BATTERY provides dynamic icons automatically
    # based on percentage value (mdi:battery, mdi:battery-20, mdi:battery-alert).
)

# Longest span after the last BLE sighting for which the battery sensor still
# shows a value: 7 days, the span of Home Assistant's ``STATE_EXPIRATION``. Wall
# clock seconds, the unit of ``BLEBatteryState.observed_at_wall``.
BLE_BATTERY_RESTORE_MAX_AGE_S = 7 * 24 * 3600

# A sighting time may lie this far in the future (clock skew between the BLE
# scanner and Home Assistant); anything later is invalid, otherwise a stored
# future date would keep a value beyond the 7-day limit.
BLE_BATTERY_FUTURE_TOLERANCE_S = 300


def _sighting_within_limit(observed_wall: float) -> bool:
    """Return True if a sighting time is neither too old nor in the future."""
    age = time.time() - observed_wall
    return -BLE_BATTERY_FUTURE_TOLERANCE_S <= age <= BLE_BATTERY_RESTORE_MAX_AGE_S


def _coerce_observation_wall(raw: Any) -> float | None:
    """Return ``raw`` as a finite wall-clock timestamp, or ``None``."""
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    value = float(raw)
    return value if math.isfinite(value) else None


@dataclass
class BLEBatteryExtraStoredData(SensorExtraStoredData):
    """Restore data of the BLE battery sensor: value, unit and sighting time.

    ``last_observation_wall`` is the wall-clock time of the BLE sighting the
    value came from. It survives a restart, so the 7-day limit
    (``BLE_BATTERY_RESTORE_MAX_AGE_S``) keeps counting from the sighting, not
    from the restart.
    """

    last_observation_wall: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """Return the sensor restore data plus the sighting time."""
        data = super().as_dict()
        data["last_observation_wall"] = self.last_observation_wall
        return data

    @classmethod
    def from_dict(cls, restored: dict[str, Any]) -> Self | None:
        """Read the restore data; an invalid sighting time becomes ``None``."""
        base = SensorExtraStoredData.from_dict(restored)
        if base is None:
            return None
        return cls(
            base.native_value,
            base.native_unit_of_measurement,
            _coerce_observation_wall(restored.get("last_observation_wall")),
        )


# NOTE:
# - Translation keys are aligned with en.json (entity.sensor.*), keeping the set in sync.
# - `skipped_duplicates` is intentionally absent (removed upstream).
STATS_DESCRIPTIONS: dict[str, SensorEntityDescription] = {
    "background_updates": SensorEntityDescription(
        key="background_updates",
        translation_key="stat_background_updates",
        icon="mdi:cloud-download",
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    "polled_updates": SensorEntityDescription(
        key="polled_updates",
        translation_key="stat_polled_updates",
        icon="mdi:download-network",
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    "crowd_sourced_updates": SensorEntityDescription(
        key="crowd_sourced_updates",
        translation_key="stat_crowd_sourced_updates",
        icon="mdi:account-group",
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    "history_fallback_used": SensorEntityDescription(
        key="history_fallback_used",
        translation_key="stat_history_fallback_used",
        icon="mdi:history",
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    "timeouts": SensorEntityDescription(
        key="timeouts",
        translation_key="stat_timeouts",
        icon="mdi:timer-off",
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    "invalid_coords": SensorEntityDescription(
        key="invalid_coords",
        translation_key="stat_invalid_coords",
        icon="mdi:map-marker-alert",
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    "fused_updates": SensorEntityDescription(
        key="fused_updates",
        translation_key="stat_fused_updates",
        icon="mdi:map-marker-path",
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    *,
    config_subentry_id: str | None = None,
) -> None:
    """Set up Google Find My Device sensor entities.

    Behavior:
    - Create per-device last_seen sensors for devices in the current snapshot.
    - Optionally create diagnostic stat sensors when enabled via options.
    - Dynamically add per-device sensors when new devices appear.
    """
    _LOGGER.debug(
        "Sensor setup invoked for entry=%s config_subentry_id=%s",
        getattr(entry, "entry_id", None),
        config_subentry_id,
    )
    coordinator = resolve_coordinator(entry)
    ensure_dispatcher_dependencies(hass)
    if getattr(coordinator, "config_entry", None) is None:
        coordinator.config_entry = entry

    def _collect_scopes(
        *,
        feature: str,
        default_key: str,
        hint_subentry_id: str | None = None,
        forwarded_config_id: str | None = None,
    ) -> list[_Scope]:
        scopes: dict[str, _Scope] = {}

        subentry_metas = getattr(coordinator, "_subentry_metadata", None)
        if isinstance(subentry_metas, Mapping):
            for key, meta in subentry_metas.items():
                meta_features = getattr(meta, "features", ())
                if feature not in meta_features:
                    continue

                stable_identifier = getattr(meta, "stable_identifier", None)
                identifier = (
                    stable_identifier()
                    if callable(stable_identifier)
                    else None
                    or getattr(meta, "config_subentry_id", None)
                    or coordinator.stable_subentry_identifier(key=key)
                )
                scopes[identifier] = _Scope(
                    key,
                    getattr(meta, "config_subentry_id", None),
                    identifier,
                )

        subentries = getattr(entry, "subentries", None)
        if isinstance(subentries, Mapping):
            for subentry in subentries.values():
                data = getattr(subentry, "data", {})
                group_key = default_key
                subentry_features: Iterable[Any] = ()
                if isinstance(data, Mapping):
                    group_key = data.get("group_key", group_key)
                    subentry_features = data.get("features", ())

                if isinstance(group_key, str) and group_key:
                    if default_key == TRACKER_SUBENTRY_KEY and group_key in (
                        SERVICE_SUBENTRY_KEY,
                        "service",
                    ):
                        continue
                    if default_key == SERVICE_SUBENTRY_KEY and group_key not in (
                        SERVICE_SUBENTRY_KEY,
                        "service",
                    ):
                        continue

                if feature not in subentry_features:
                    continue

                config_id = getattr(subentry, "subentry_id", None) or getattr(
                    subentry, "entry_id", None
                )
                identifier = (
                    config_id
                    or coordinator.stable_subentry_identifier(key=group_key)
                    or default_key
                )
                scopes.setdefault(
                    identifier,
                    _Scope(group_key or default_key, config_id, identifier),
                )

        if scopes:
            return list(scopes.values())

        fallback_identifier = coordinator.stable_subentry_identifier(
            key=default_key, feature=feature
        )
        fallback_config_id = forwarded_config_id or hint_subentry_id

        return [
            _Scope(
                default_key,
                fallback_config_id,
                fallback_identifier or fallback_config_id or default_key,
            )
        ]

    enable_stats_raw = entry.options.get(
        OPT_ENABLE_STATS_ENTITIES,
        entry.data.get(OPT_ENABLE_STATS_ENTITIES, DEFAULT_ENABLE_STATS_ENTITIES),
    )
    enable_stats = bool(enable_stats_raw)

    added_unique_ids: set[str] = set()
    created_stats: dict[str, list[str]] = {}
    service_scopes: dict[str, _Scope] = {}
    tracker_scopes: list[_Scope] = []
    processed_tracker_identifiers: set[str] = set()
    primary_tracker_scope: _Scope | None = None
    tracker_scheduler: Callable[[Iterable[SensorEntity], bool], None] | None = None

    def _scope_matches_forwarded(
        scope: _Scope, forwarded_config_id: str | None
    ) -> bool:
        if forwarded_config_id is None:
            return True
        return forwarded_config_id in (
            scope.config_subentry_id,
            scope.identifier,
            scope.subentry_key,
        )

    def _add_service_scope(scope: _Scope, forwarded_config_id: str | None) -> None:
        service_ids = known_ids_for_subentry_type(entry, SUBENTRY_TYPE_SERVICE)
        sanitized_config_id = ensure_config_subentry_id(
            entry,
            "sensor_service",
            scope.config_subentry_id or forwarded_config_id or scope.identifier,
            known_ids=service_ids,
        )
        if sanitized_config_id is None:
            if not service_ids:
                sanitized_config_id = (
                    scope.config_subentry_id
                    or forwarded_config_id
                    or scope.identifier
                    or SERVICE_SUBENTRY_KEY
                )
            else:
                _LOGGER.debug(
                    "Sensor setup (service): skipping subentry '%s' because the config_subentry_id is unknown",
                    forwarded_config_id or scope.config_subentry_id or scope.identifier,
                )
                return

        identifier = scope.identifier or sanitized_config_id or scope.subentry_key
        service_scopes[identifier] = _Scope(
            scope.subentry_key, sanitized_config_id, identifier
        )

        def _schedule_service_entities(
            new_entities: Iterable[SensorEntity],
            update_before_add: bool = True,
        ) -> None:
            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=new_entities,
                update_before_add=update_before_add,
                config_subentry_id=sanitized_config_id,
                log_owner="Sensor setup (service)",
                logger=_LOGGER,
            )

        service_entities: list[SensorEntity] = []

        semantic_sensor = GoogleFindMySemanticLabelSensor(
            coordinator,
            subentry_key=scope.subentry_key,
            subentry_identifier=identifier,
        )
        semantic_unique_id = getattr(semantic_sensor, "unique_id", None)
        if not isinstance(semantic_unique_id, str) or (
            isinstance(semantic_unique_id, str)
            and semantic_unique_id not in added_unique_ids
        ):
            if isinstance(semantic_unique_id, str):
                added_unique_ids.add(semantic_unique_id)
            service_entities.append(semantic_sensor)

        # Third diagnostic axis (E2EE key health). Always created alongside the
        # semantic-label sensor, independent of the stats toggle, because the
        # encryption-key status is a core diagnostic like Nova auth / FCM
        # connectivity, not an optional counter.
        crypto_sensor = GoogleFindMyEncryptionKeyStatusSensor(
            coordinator,
            subentry_key=scope.subentry_key,
            subentry_identifier=identifier,
        )
        crypto_unique_id = getattr(crypto_sensor, "unique_id", None)
        if not isinstance(crypto_unique_id, str) or (
            isinstance(crypto_unique_id, str)
            and crypto_unique_id not in added_unique_ids
        ):
            if isinstance(crypto_unique_id, str):
                added_unique_ids.add(crypto_unique_id)
            service_entities.append(crypto_sensor)

        if not enable_stats:
            _schedule_service_entities(service_entities, True)
            return

        if hasattr(coordinator, "stats"):
            created_for_scope: list[str] = []
            for stat_key, desc in STATS_DESCRIPTIONS.items():
                if stat_key not in coordinator.stats:
                    continue
                entity = GoogleFindMyStatsSensor(
                    coordinator,
                    stat_key,
                    desc,
                    subentry_key=scope.subentry_key,
                    subentry_identifier=identifier,
                )
                unique_id = getattr(entity, "unique_id", None)
                if isinstance(unique_id, str) and unique_id in added_unique_ids:
                    continue
                if isinstance(unique_id, str):
                    added_unique_ids.add(unique_id)
                service_entities.append(entity)
                created_for_scope.append(stat_key)

            if created_for_scope:
                created_stats[identifier] = created_for_scope

        if service_entities:
            _LOGGER.debug(
                "Sensor setup: service_key=%s, config_subentry_id=%s (entities=%d)",
                scope.subentry_key,
                sanitized_config_id,
                len(service_entities),
            )
            _schedule_service_entities(service_entities, True)
        else:
            _schedule_service_entities([], True)

    def _add_tracker_scope(scope: _Scope, forwarded_config_id: str | None) -> None:
        nonlocal primary_tracker_scope, tracker_scheduler

        candidate_subentry_id = scope.config_subentry_id
        if candidate_subentry_id is None and forwarded_config_id in (
            scope.identifier,
            scope.subentry_key,
        ):
            candidate_subentry_id = forwarded_config_id
        candidate_subentry_id = candidate_subentry_id or scope.identifier

        tracker_ids = known_ids_for_subentry_type(entry, SUBENTRY_TYPE_TRACKER)
        sanitized_config_id = ensure_config_subentry_id(
            entry,
            "sensor_tracker",
            candidate_subentry_id,
            known_ids=tracker_ids,
        )
        if sanitized_config_id is None:
            if tracker_ids:
                _LOGGER.debug(
                    "Sensor setup: skipping tracker subentry '%s' because the config_subentry_id is unknown",
                    candidate_subentry_id or forwarded_config_id,
                )
                return
            sanitized_config_id = candidate_subentry_id or scope.subentry_key
            _LOGGER.debug(
                "Sensor setup: synthesized config_subentry_id '%s' for tracker key '%s'",
                sanitized_config_id,
                scope.subentry_key,
            )

        tracker_identifier = (
            scope.identifier or sanitized_config_id or scope.subentry_key
        )
        if tracker_identifier in processed_tracker_identifiers:
            return
        processed_tracker_identifiers.add(tracker_identifier)
        tracker_scope = _Scope(
            scope.subentry_key, sanitized_config_id, tracker_identifier
        )
        tracker_scopes.append(tracker_scope)

        if primary_tracker_scope is None:
            primary_tracker_scope = tracker_scope

        known_ids: set[str] = set()
        known_battery_ids: set[str] = set()
        entities_added = False

        def _schedule_tracker_entities(
            new_entities: Iterable[SensorEntity],
            update_before_add: bool = True,
        ) -> None:
            nonlocal entities_added
            entity_list = list(new_entities)
            entities_added |= bool(entity_list)

            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=entity_list,
                update_before_add=update_before_add,
                config_subentry_id=sanitized_config_id,
                log_owner="Sensor setup (tracker)",
                logger=_LOGGER,
            )

        if tracker_scheduler is None:
            tracker_scheduler = _schedule_tracker_entities

        def _get_ble_resolver() -> GoogleFindMyEIDResolver | None:
            """Return the EID resolver from hass.data, or None."""
            domain_data = hass.data.get(DOMAIN)
            if not isinstance(domain_data, dict):
                return None
            return cast(
                "GoogleFindMyEIDResolver | None", domain_data.get(DATA_EID_RESOLVER)
            )

        def _build_entities() -> list[SensorEntity]:
            """Build sensor entities for visible devices in the current subentry."""
            entities: list[SensorEntity] = []
            resolver = _get_ble_resolver()
            for device in coordinator.get_subentry_snapshot(tracker_scope.subentry_key):
                dev_id = device.get("id") if isinstance(device, Mapping) else None
                dev_name = device.get("name") if isinstance(device, Mapping) else None
                if not dev_id or not dev_name:
                    _LOGGER.debug("Skipping device without id/name: %s", device)
                    continue

                visible = True
                is_visible = getattr(coordinator, "is_device_visible_in_subentry", None)
                if callable(is_visible):
                    try:
                        visible = bool(is_visible(tracker_scope.subentry_key, dev_id))
                    except Exception:  # pragma: no cover - defensive fallback for stubs
                        visible = True

                if not visible:
                    _LOGGER.debug(
                        "Sensor setup: skipping hidden device id %s for subentry %s",
                        dev_id,
                        tracker_scope.subentry_key,
                    )
                    continue

                # --- LastSeen sensor (always) ---
                if dev_id not in known_ids:
                    entity = GoogleFindMyLastSeenSensor(
                        coordinator,
                        device,
                        subentry_key=tracker_scope.subentry_key,
                        subentry_identifier=tracker_identifier,
                    )
                    unique_id = getattr(entity, "unique_id", None)
                    if isinstance(unique_id, str):
                        if unique_id in added_unique_ids:
                            continue
                        added_unique_ids.add(unique_id)
                    known_ids.add(dev_id)
                    entities.append(entity)

                    # --- Plus Code sensor (always, alongside LastSeen) ---
                    plus_code_entity = GoogleFindMyPlusCodeSensor(
                        coordinator,
                        device,
                        subentry_key=tracker_scope.subentry_key,
                        subentry_identifier=tracker_identifier,
                    )
                    pc_uid = getattr(plus_code_entity, "unique_id", None)
                    if not isinstance(pc_uid, str) or pc_uid not in added_unique_ids:
                        if isinstance(pc_uid, str):
                            added_unique_ids.add(pc_uid)
                        entities.append(plus_code_entity)

                    # --- Location status and coarse-fix sensors (always,
                    # alongside LastSeen) ---
                    # In this setup path they are built only here, right after
                    # LastSeen was added for this device for the first time,
                    # and their unique_ids are derived from the same device id:
                    # a duplicate cannot reach this point, so no unique-id
                    # check of their own. The recovery factory below builds
                    # them separately, only for missing unique_ids.
                    entities.append(
                        GoogleFindMyLocationStatusSensor(
                            coordinator,
                            device,
                            subentry_key=tracker_scope.subentry_key,
                            subentry_identifier=tracker_identifier,
                        )
                    )
                    entities.append(
                        GoogleFindMyCoarseFixSensor(
                            coordinator,
                            device,
                            subentry_key=tracker_scope.subentry_key,
                            subentry_identifier=tracker_identifier,
                        )
                    )

                # --- BLE Battery sensor (when the entity registry already
                # holds it, or when the resolver has data) ---
                if dev_id not in known_battery_ids:
                    battery_state = None
                    if resolver is not None:
                        try:
                            battery_state = resolver.get_ble_battery_state(dev_id)
                        except Exception:  # noqa: BLE001
                            pass
                    battery_entity = GoogleFindMyBLEBatterySensor(
                        coordinator,
                        device,
                        subentry_key=tracker_scope.subentry_key,
                        subentry_identifier=tracker_identifier,
                    )
                    bat_uid = getattr(battery_entity, "unique_id", None)
                    in_registry = (
                        isinstance(bat_uid, str)
                        and er.async_get(hass).async_get_entity_id(
                            "sensor", DOMAIN, bat_uid
                        )
                        is not None
                    )
                    if (
                        (battery_state is not None or in_registry)
                        and isinstance(bat_uid, str)
                        and bat_uid not in added_unique_ids
                    ):
                        added_unique_ids.add(bat_uid)
                        known_battery_ids.add(dev_id)
                        entities.append(battery_entity)
                        if battery_state is not None:
                            _LOGGER.info(
                                "BLE battery sensor created for device=%s "
                                "(battery=%s%%)",
                                dev_id,
                                battery_state.battery_pct,
                            )
                        else:
                            _LOGGER.debug(
                                "BLE battery sensor created from entity registry "
                                "for device=%s",
                                dev_id,
                            )

            return entities

        initial_entities = _build_entities()
        if initial_entities:
            _LOGGER.debug(
                "Sensor setup: tracker_key=%s, config_subentry_id=%s (initial=%d)",
                tracker_scope.subentry_key,
                sanitized_config_id,
                len(initial_entities),
            )
            _schedule_tracker_entities(initial_entities, True)
        else:
            _schedule_tracker_entities([], True)

        @callback
        def _add_new_devices() -> None:
            new_entities = _build_entities()
            if new_entities:
                _LOGGER.debug(
                    "Sensor setup: dynamically adding %d entity(ies) for tracker subentry %s",
                    len(new_entities),
                    tracker_scope.subentry_key,
                )
                _schedule_tracker_entities(new_entities, True)
            elif not entities_added:
                _schedule_tracker_entities([], True)

        unsub = coordinator.async_add_listener(_add_new_devices)
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
        _LOGGER.debug(
            "Sensor setup: processing subentry '%s' (type=%s)",
            subentry_identifier,
            subentry_type,
        )
        if subentry_type not in (None, "service", "tracker"):
            _LOGGER.debug(
                "Sensor setup skipped for unrelated subentry '%s' (type '%s')",
                subentry_identifier,
                subentry_type,
            )
            return

        if subentry_identifier in seen_subentries:
            return
        seen_subentries.add(subentry_identifier)

        service_config_id = next(
            (
                getattr(candidate, "subentry_id", None)
                for candidate in getattr(entry, "subentries", {}).values()
                if getattr(candidate, "subentry_type", None) == "service"
                or (
                    isinstance(getattr(candidate, "data", None), Mapping)
                    and candidate.data.get("group_key")
                    in (SERVICE_SUBENTRY_KEY, "service")
                )
            ),
            None,
        )
        service_subentries_exist = service_config_id is not None

        processed_ids: set[str | None] = set()
        if subentry_type == "tracker" and not service_subentries_exist:
            _add_service_scope(
                _Scope(
                    SERVICE_SUBENTRY_KEY,
                    service_config_id or SERVICE_SUBENTRY_KEY,
                    SERVICE_SUBENTRY_KEY,
                ),
                service_config_id or SERVICE_SUBENTRY_KEY,
            )
        service_forward_id = (
            service_config_id
            if (subentry_type == "tracker" and service_subentries_exist)
            else SERVICE_SUBENTRY_KEY
            if subentry_type == "tracker"
            else subentry_identifier
        )

        if not (subentry_type == "tracker" and service_subentries_exist):
            for scope in _collect_scopes(
                feature="sensor",
                default_key=SERVICE_SUBENTRY_KEY,
                hint_subentry_id=service_forward_id,
                forwarded_config_id=service_forward_id,
            ):
                scope_identifier = (
                    scope.config_subentry_id or scope.identifier or scope.subentry_key
                )
                if scope_identifier in processed_ids and subentry_type != "tracker":
                    continue
                if subentry_type != "tracker":
                    processed_ids.add(scope_identifier)
                if _scope_matches_forwarded(scope, subentry_identifier):
                    _add_service_scope(scope, subentry_identifier)

        if subentry_type in (None, "tracker"):
            for scope in _collect_scopes(
                feature="sensor",
                default_key=TRACKER_SUBENTRY_KEY,
                hint_subentry_id=subentry_identifier,
                forwarded_config_id=subentry_identifier,
            ):
                scope_identifier = (
                    scope.config_subentry_id or scope.identifier or scope.subentry_key
                )
                if scope_identifier in processed_ids:
                    continue
                processed_ids.add(scope_identifier)
                if _scope_matches_forwarded(scope, subentry_identifier):
                    _add_tracker_scope(scope, subentry_identifier)

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
        service_identifier = next(iter(created_stats.keys()), None)
        service_scope = (
            service_scopes.get(service_identifier) if service_identifier else None
        )
        tracker_scope = primary_tracker_scope

        def _recovery_add_entities(
            new_entities: Iterable[SensorEntity],
            update_before_add: bool = True,
        ) -> None:
            service_batch: list[SensorEntity] = []
            tracker_batch: list[SensorEntity] = []
            for entity in new_entities:
                if isinstance(entity, GoogleFindMyStatsSensor):
                    service_batch.append(entity)
                else:
                    tracker_batch.append(entity)

            if service_batch and service_scope is not None:
                schedule_add_entities(
                    coordinator.hass,
                    async_add_entities,
                    entities=service_batch,
                    update_before_add=update_before_add,
                    config_subentry_id=service_scope.config_subentry_id,
                    log_owner="Sensor setup (service)",
                    logger=_LOGGER,
                )
            if tracker_batch and tracker_scheduler is not None:
                tracker_scheduler(tracker_batch, update_before_add)

        def _is_visible_device(device_id: str) -> bool:
            if tracker_scope is None:
                return True
            is_visible = getattr(coordinator, "is_device_visible_in_subentry", None)
            if callable(is_visible):
                try:
                    return bool(is_visible(tracker_scope.subentry_key, device_id))
                except Exception:  # pragma: no cover - defensive fallback for stubs
                    return True
            return True

        def _expected_unique_ids() -> set[str]:
            if not isinstance(entry_id, str) or not entry_id:
                return set()
            expected: set[str] = set()
            if (
                service_identifier
                and isinstance(service_identifier, str)
                and service_identifier
            ):
                for stat_key in created_stats.get(service_identifier, []):
                    expected.add(f"{DOMAIN}_{entry_id}_{service_identifier}_{stat_key}")
            if tracker_scope and isinstance(tracker_scope.identifier, str):
                for device in coordinator.get_subentry_snapshot(
                    tracker_scope.subentry_key
                ):
                    dev_id = device.get("id")
                    dev_name = device.get("name")
                    if (
                        not isinstance(dev_id, str)
                        or not dev_id
                        or not isinstance(dev_name, str)
                        or not dev_name
                    ):
                        continue
                    if not _is_visible_device(dev_id):
                        continue
                    expected.add(
                        f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_last_seen"
                    )
                    expected.add(
                        f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_plus_code"
                    )
                    expected.add(
                        f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_location_status"
                    )
                    expected.add(
                        f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_coarse_fix"
                    )
            return expected

        def _build_entities(missing: set[str]) -> list[SensorEntity]:
            if not missing:
                return []
            built: list[SensorEntity] = []
            if not isinstance(entry_id, str) or not entry_id:
                return built
            if (
                service_scope
                and isinstance(service_identifier, str)
                and service_identifier
            ):
                for stat_key in created_stats.get(service_identifier, []):
                    unique_id = f"{DOMAIN}_{entry_id}_{service_identifier}_{stat_key}"
                    if unique_id not in missing:
                        continue
                    description = STATS_DESCRIPTIONS.get(stat_key)
                    if description is None:
                        continue
                    built.append(
                        GoogleFindMyStatsSensor(
                            coordinator,
                            stat_key,
                            description,
                            subentry_key=service_scope.subentry_key,
                            subentry_identifier=service_identifier,
                        )
                    )
            if tracker_scope and isinstance(tracker_scope.identifier, str):
                for device in coordinator.get_subentry_snapshot(
                    tracker_scope.subentry_key
                ):
                    dev_id = device.get("id")
                    dev_name = device.get("name")
                    if (
                        not isinstance(dev_id, str)
                        or not dev_id
                        or not isinstance(dev_name, str)
                        or not dev_name
                    ):
                        continue
                    if not _is_visible_device(dev_id):
                        continue
                    ls_uid = f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_last_seen"
                    pc_uid = f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_plus_code"
                    st_uid = f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_location_status"
                    cf_uid = f"{DOMAIN}_{entry_id}_{tracker_scope.identifier}_{dev_id}_coarse_fix"
                    if ls_uid in missing:
                        built.append(
                            GoogleFindMyLastSeenSensor(
                                coordinator,
                                device,
                                subentry_key=tracker_scope.subentry_key,
                                subentry_identifier=tracker_scope.identifier,
                            )
                        )
                    if pc_uid in missing:
                        built.append(
                            GoogleFindMyPlusCodeSensor(
                                coordinator,
                                device,
                                subentry_key=tracker_scope.subentry_key,
                                subentry_identifier=tracker_scope.identifier,
                            )
                        )
                    if st_uid in missing:
                        built.append(
                            GoogleFindMyLocationStatusSensor(
                                coordinator,
                                device,
                                subentry_key=tracker_scope.subentry_key,
                                subentry_identifier=tracker_scope.identifier,
                            )
                        )
                    if cf_uid in missing:
                        built.append(
                            GoogleFindMyCoarseFixSensor(
                                coordinator,
                                device,
                                subentry_key=tracker_scope.subentry_key,
                                subentry_identifier=tracker_scope.identifier,
                            )
                        )
            return built

        recovery_manager.register_sensor_platform(
            expected_unique_ids=_expected_unique_ids,
            entity_factory=_build_entities,
            add_entities=_recovery_add_entities,
        )


# ------------------------------- Stats Sensor ---------------------------------


class GoogleFindMySemanticLabelSensor(GoogleFindMyEntity, SensorEntity):
    """Expose observed semantic labels for mapping and diagnostics."""

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_has_entity_name = True
    entity_description = SEMANTIC_LABEL_DESCRIPTION

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        super().__init__(
            coordinator,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        entry_id = self.entry_id or "default"
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            entry_id,
            subentry_identifier,
            "semantic_labels",
            separator="_",
        )

    @staticmethod
    def _as_iso(ts: float | None) -> str | None:
        """Render epoch seconds as an ISO 8601 string."""

        if not isinstance(ts, (int, float)) or ts <= 0:
            return None
        try:
            return datetime.fromtimestamp(float(ts), tz=UTC).isoformat()
        except Exception:
            return None

    def _observations(self) -> list[SemanticLabelRecord]:
        getter = getattr(self.coordinator, "get_observed_semantic_labels", None)
        if not callable(getter):
            return []
        try:
            result = getter()
        except Exception as err:  # pragma: no cover - defensive logging path
            _LOGGER.debug("Semantic label snapshot failed: %s", err)
            return []

        observations: list[SemanticLabelRecord] = []
        for record in result:
            if isinstance(record, SemanticLabelRecord):
                observations.append(record)

        return observations

    @property
    def native_value(self) -> int:
        """Return the number of observed semantic labels."""

        return len(self._observations())

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Expose observed semantic labels and their metadata."""

        observations = self._observations()
        attrs: list[dict[str, Any]] = []
        for record in observations:
            attrs.append(
                {
                    "label": record.label,
                    "first_seen": self._as_iso(record.first_seen),
                    "last_seen": self._as_iso(record.last_seen),
                    "devices": sorted(record.devices),
                }
            )

        return {"labels": [item["label"] for item in attrs], "observations": attrs}

    @property
    def device_info(self) -> DeviceInfo:
        """Attach to the service device for diagnostic grouping."""

        return self.service_device_info(include_subentry_identifier=True)


class GoogleFindMyEncryptionKeyStatusSensor(GoogleFindMyEntity, SensorEntity):
    """Diagnostic ENUM sensor exposing end-to-end location decryption health.

    This is the third diagnostic axis next to Nova auth (outgoing) and the FCM
    connection (incoming): the E2EE key used to decrypt location reports. Its
    state is read straight from ``coordinator.crypto_status``, which the poll,
    push and manual-locate paths all feed through the single
    ``note_decrypt_failure`` writer, so the sensor never diverges from the reauth
    escalation behaviour it visualizes. Lives on the per-entry service device
    (account-wide, like the shared key it reflects).
    """

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_options = ENCRYPTION_KEY_STATUS_OPTIONS
    entity_description = ENCRYPTION_KEY_STATUS_DESCRIPTION

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the encryption-key-status sensor."""

        super().__init__(
            coordinator,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        entry_id = self.entry_id or "default"
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            entry_id,
            subentry_identifier,
            TRANSLATION_KEY_ENCRYPTION_KEY_STATUS,
            separator="_",
        )

    @property
    def native_value(self) -> str | None:
        """Return the current decryption state, or None for HA's "unknown".

        HA's ENUM device class requires the value to be one of ``_attr_options``
        or ``None``. CryptoStatus.UNKNOWN (no decryption attempted yet) is mapped
        to ``None`` so HA renders the native "unknown" state instead of a fifth
        explicit option.
        """

        snapshot = getattr(self.coordinator, "crypto_status", None)
        state = getattr(snapshot, "state", None)
        if not isinstance(state, str) or state == CryptoStatus.UNKNOWN:
            return None
        return state

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Expose how close the account is to a decrypt-driven reauth.

        Sourced from the same coordinator fields that drive the escalation, so
        there is no second, divergent state. ``threshold`` is intentionally not
        exposed here to avoid coupling the entity to a private coordinator
        constant; ``consecutive_decrypt_failures`` already shows the progress.
        """

        attributes: dict[str, Any] = {}

        snapshot = getattr(self.coordinator, "crypto_status", None)
        changed_at = getattr(snapshot, "changed_at", None)
        if isinstance(changed_at, (int, float)):
            attributes["changed_at"] = changed_at

        failures = getattr(self.coordinator, "_consecutive_decrypt_failures", None)
        if isinstance(failures, int):
            attributes["consecutive_decrypt_failures"] = failures

        last_error = getattr(self.coordinator, "_last_decrypt_error", None)
        if isinstance(last_error, str) and last_error:
            # Stored as "ClassName: message"; expose just the class for triage.
            attributes["last_decrypt_error_class"] = last_error.split(":", 1)[0]

        return attributes or None

    @property
    def device_info(self) -> DeviceInfo:
        """Attach to the service device for diagnostic grouping."""

        return self.service_device_info(include_subentry_identifier=True)


class GoogleFindMyStatsSensor(GoogleFindMyEntity, SensorEntity):
    """Diagnostic counters for the integration (entry-scoped).

    Naming policy (HA Quality Scale – Platinum):
    - Do **not** set a hard-coded `_attr_name`. We rely on `entity_description.translation_key`
      and `_attr_has_entity_name = True` so HA composes the visible name as
      "<device name> <translated entity name>". This ensures locale-correct UI strings.
    """

    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_has_entity_name = True  # allow translated entity name to be used

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        stat_key: str,
        description: SensorEntityDescription,
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the stats sensor.

        Args:
            coordinator: The integration's data coordinator.
            stat_key: Name of the counter in coordinator.stats.
            description: Home Assistant entity description (icon, translation_key, etc.).
        """
        super().__init__(
            coordinator,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        self._stat_key = stat_key
        self.entity_description = description
        entry_id = self.entry_id or "default"
        # Entry-scoped unique_id avoids collisions in multi-account setups.
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            entry_id,
            subentry_identifier,
            stat_key,
            separator="_",
        )
        # Plain unit label; TOTAL_INCREASING counters represent event counts.
        self._attr_native_unit_of_measurement = "updates"

    @property
    def native_value(self) -> int | None:
        """Return the current counter value."""
        stats = getattr(self.coordinator, "stats", None)
        if stats is None:
            return None
        raw = stats.get(self._stat_key)
        if isinstance(raw, bool):
            return int(raw)
        if isinstance(raw, (int, float)):
            return int(raw)
        return None

    @property
    def available(self) -> bool:
        """Stats sensors stay available even when polling fails."""
        return bool(super().available)

    @callback
    def _handle_coordinator_update(self) -> None:
        """Propagate coordinator updates to Home Assistant state."""

        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Expose a single integration service device for diagnostic sensors.

        All counters live on the per-entry SERVICE device to keep the UI tidy.
        """
        return self.service_device_info(include_subentry_identifier=True)


# ----------------------------- Per-Device Last Seen ---------------------------


class GoogleFindMyLastSeenSensor(GoogleFindMyDeviceEntity, RestoreSensor):
    """Per-device sensor exposing the last_seen timestamp.

    Behavior:
    - Restores the last native value on startup and seeds the coordinator cache.
    - Updates on coordinator ticks and keeps the registry name aligned with Google's label.
    - Never writes a placeholder name to the device registry.
    """

    # Best practice: let HA compose "<Device Name> <translated entity name>"
    _attr_has_entity_name = True
    # Entities should be enabled by default on fresh installs
    _attr_entity_registry_enabled_default = True
    entity_description = LAST_SEEN_DESCRIPTION

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the sensor."""
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
        # Namespace unique_id by entry for safety (keeps multi-entry setups clean).
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            entry_id,
            subentry_identifier,
            f"{safe_id}_last_seen",
            separator="_",
        )
        self._attr_native_value: datetime | None = None

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return extra state attributes for diagnostics/UX (sanitized & stable).

        Delegates to the coordinator helper `_as_ha_attributes`, which:
        - Adds a normalized UTC timestamp mirror (`last_seen_utc`).
        - Uses `accuracy_m` (float meters) rather than `gps_accuracy` for stability.
        - Includes source labeling (`source_label`/`source_rank`) for transparency.

        Map-marker self-heal: `latitude`/`longitude` are stripped here, at the
        consumer, because the shared helper is also used by the device_tracker,
        which legitimately carries coordinates. The map marker is a live-state
        attribute (not a registry entry), so once this property stops emitting the
        pair, HA drops the spurious marker on the next state write after upgrade.
        No entity deletion, registry migration, or reconfigure is required.
        """
        dev_id = self._device_id
        if not dev_id:
            return None
        row = self.coordinator.get_device_location_data_for_subentry(
            self.subentry_key, dev_id
        )
        attrs = _as_ha_attributes(row) if row else None
        if not attrs:
            return None
        # Defensive copy: the shared helper output is also consumed by the
        # device_tracker (which legitimately carries coordinates); never mutate it.
        attrs = dict(attrs)
        # A TIMESTAMP sensor must not advertise map coordinates, otherwise HA
        # plots it as a third, spurious marker. Strip via the HA constants the
        # device_tracker already uses (not string literals).
        attrs.pop(ATTR_LATITUDE, None)
        attrs.pop(ATTR_LONGITUDE, None)
        return attrs

    @property
    def available(self) -> bool:
        """Mirror device presence in availability (TTL-smoothed by coordinator).

        Fallback: If coordinator presence is unavailable, consider the sensor available
        when we have at least a restored/known last_seen value.
        """
        if not super().available:
            return False
        dev_id = self._device_id
        if not dev_id:
            return False
        if not self.coordinator_has_device():
            return False

        present: bool | None = None
        try:
            if hasattr(self.coordinator, "is_device_present"):
                raw = self.coordinator.is_device_present(dev_id)
                if isinstance(raw, bool):
                    present = raw
                else:
                    present = bool(raw)
        except Exception:
            # Be tolerant if a different coordinator build is used.
            present = None

        if present is True:
            return True
        if present is False:
            # Presence expired; fall back to restored values if available.
            return self._attr_native_value is not None

        # Unknown presence: consider available if we have any known last_seen value
        return self._attr_native_value is not None

    @callback
    def _handle_coordinator_update(self) -> None:
        """Update native timestamp and keep the device label in sync.

        - Synchronize the raw Google label into the Device Registry unless user-renamed.
        - Keep the existing last_seen when no fresh value is available to avoid churn.
        """
        # 1) Keep the raw device name synchronized with the coordinator snapshot.
        if not self.coordinator_has_device():
            self._attr_native_value = None
            self.async_write_ha_state()
            return

        self.refresh_device_label_from_coordinator(log_prefix="LastSeen")
        current_name = self._device.get("name")
        if isinstance(current_name, str):
            self.maybe_update_device_registry_name(current_name)

        # 2) Update last_seen when a valid value is available; otherwise keep the previous value.
        previous = self._attr_native_value
        new_dt: datetime | None = None
        try:
            value = (
                self.coordinator.get_device_last_seen_for_subentry(
                    self._subentry_key, self._device_id
                )
                if self._device_id
                else None
            )
            if isinstance(value, datetime):
                new_dt = value
            elif isinstance(value, (int, float)):
                new_dt = datetime.fromtimestamp(float(value), tz=UTC)
            elif isinstance(value, str):
                v = value.strip()
                if v.endswith("Z"):
                    v = v.replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(v)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    new_dt = dt
                except ValueError:
                    new_dt = None
        except (AttributeError, TypeError, ValueError) as e:  # noqa: BLE001
            _LOGGER.debug(
                "Invalid last_seen for %s: %s",
                self._device.get("name", self._device_id),
                e,
            )
            new_dt = None

        if new_dt is not None:
            self._attr_native_value = new_dt
        elif previous is not None:
            _LOGGER.debug(
                "Keeping previous last_seen for %s (no update available)",
                self._device.get("name", self._device_id),
            )

        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Restore last_seen from HA's persistent store and seed coordinator cache.

        We only seed `last_seen` (epoch seconds) — coordinates are handled by the tracker.
        """
        await super().async_added_to_hass()

        # Use RestoreSensor API to get the last native value (may be datetime/str/number)
        try:
            data = await self.async_get_last_sensor_data()
            value = getattr(data, "native_value", None) if data else None
        except (RuntimeError, AttributeError) as e:  # noqa: BLE001
            _LOGGER.debug(
                "Failed to restore sensor state for %s: %s", self.entity_id, e
            )
            value = None

        if value in (None, "unknown", "unavailable"):
            return

        # Parse restored value -> epoch seconds for coordinator cache
        ts: float | None = None
        try:
            if isinstance(value, (int, float)):
                ts = float(value)
            elif isinstance(value, str):
                v = value.strip()
                if v.endswith("Z"):
                    v = v.replace("Z", "+00:00")
                try:
                    dt = datetime.fromisoformat(v)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=UTC)
                    ts = dt.timestamp()
                except ValueError:
                    ts = float(v)  # numeric string fallback
            elif isinstance(value, datetime):
                ts = value.timestamp()
        except (ValueError, TypeError) as ex:  # noqa: BLE001
            _LOGGER.debug(
                "Could not parse restored value '%s' for %s: %s",
                value,
                self.entity_id,
                ex,
            )
            ts = None

        if ts is None or not self._device_id:
            return

        # Seed coordinator cache using its public API (no private access).
        try:
            self.coordinator.seed_device_last_seen(self._device_id, ts)
        except (AttributeError, TypeError) as e:  # noqa: BLE001
            _LOGGER.debug(
                "Failed to seed coordinator cache for %s: %s", self.entity_id, e
            )
            return

        # Set our native value now (no need to wait for next coordinator tick)
        self._attr_native_value = datetime.fromtimestamp(ts, tz=UTC)
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Expose DeviceInfo using the shared entity helper."""

        return super().device_info


# ------------------------------- Per-Device Plus Code -------------------------


class GoogleFindMyPlusCodeSensor(GoogleFindMyDeviceEntity, RestoreSensor):
    """Per-device sensor exposing the Google Plus Code (Open Location Code).

    The value is a deterministic function of the device's latest known
    latitude/longitude (offline, no API key, no network). It is derived live
    from the coordinator's current location row:

    - A valid fix -> the full 10-digit Plus Code (11 characters incl. the ``+``
      separator, e.g. ``8FVC9G8F+6X``), directly usable in Google Maps.
    - No fix / invalid coordinates -> ``None`` (no stale "last value"). The
      underlying coordinate cache is restored by the device_tracker across
      restarts, so this derived text needs no separate restore wiring; it
      subclasses ``RestoreSensor`` for parity with the other per-device sensors.

    Unlike the device_tracker, this is a plain text sensor and deliberately
    carries no ``latitude``/``longitude`` attributes, so Home Assistant does not
    plot a spurious extra map marker for it.
    """

    _attr_has_entity_name = True
    _attr_entity_registry_enabled_default = True
    entity_description = PLUS_CODE_DESCRIPTION
    # HA flattens this to the ``attribution`` state attribute (not into
    # extra_state_attributes, no map marker). Static end-user source credit; the
    # divergence from the tracker's dynamic ``FMDN (email)`` attribution is
    # deliberate (maintenance-free) and documented in the PR body.
    _attr_attribution = "Data provided by Google"

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the sensor."""
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
        # Namespace unique_id by entry for safety (keeps multi-entry setups clean).
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            entry_id,
            subentry_identifier,
            f"{safe_id}_plus_code",
            separator="_",
        )

    def _current_plus_code(self) -> str | None:
        """Compute the Plus Code from the device's last known published location.

        The value follows the ``last_known`` semantics of the device_tracker's
        Last Location entity: it reports the last *reliable* position and never
        goes stale. Both the coordinate row and the encoding come from shared
        sources:

        - the row is the coordinator display row
          (``get_display_location_data_for_subentry``): the current fix when it
          carries a usable accuracy, otherwise the last accuracy-bearing fix
          (coordinator last-good), otherwise ``None``;
        - the encoding is ``encode_plus_code_for_row`` (shared with the tracker
          ``plus_code`` attribute, single source, no duplicated logic).

        The accuracy gate is inherent in the display-row selection, so an
        accuracy-less coordinate the tracker/map deliberately hide is still never
        published (Codex #202 / PR #1179): such a fix is skipped in favour of the
        last good row, and if none exists the result is ``None`` (only for a
        device that has never had a usable fix).
        """
        dev_id = self._device_id
        if not dev_id:
            return None
        row = self.coordinator.get_display_location_data_for_subentry(
            self.subentry_key, dev_id
        )
        return encode_plus_code_for_row(row)

    @property
    def native_value(self) -> str | None:
        """Return the current Plus Code (11 chars incl. ``+``) or ``None``."""
        return self._current_plus_code()

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return a copyable ``coordinates`` string for the last known position.

        The string uses the map popup's format (``"lat, lon"``, dot decimal
        separator, raw values, see ``map_view.py``) so the user can copy the
        tracker's last known coordinates straight into a maps app while searching
        for it. Deliberately no numeric ``latitude``/``longitude``: a text sensor
        carrying both would be auto-plotted by HA as a spurious extra map marker.

        Coupling invariant: ``coordinates`` is present exactly when
        ``native_value`` yields a code (both derive from the same display row);
        otherwise the attributes are empty. ``attribution`` is set independently
        via ``_attr_attribution`` and always present.
        """
        dev_id = self._device_id
        if not dev_id:
            return None
        row = self.coordinator.get_display_location_data_for_subentry(
            self.subentry_key, dev_id
        )
        if row is None or encode_plus_code_for_row(row) is None:
            return None
        return {"coordinates": f"{row['latitude']}, {row['longitude']}"}

    @property
    def available(self) -> bool:
        """Mirror device presence, falling back to coordinator availability."""
        if not super().available:
            return False
        dev_id = self._device_id
        if not dev_id:
            return False
        return self.coordinator_has_device()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Keep the device label in sync and push the recomputed state."""
        if not self.coordinator_has_device():
            self.async_write_ha_state()
            return
        self.refresh_device_label_from_coordinator(log_prefix="PlusCode")
        current_name = self._device.get("name")
        if isinstance(current_name, str):
            self.maybe_update_device_registry_name(current_name)
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Expose DeviceInfo using the shared entity helper."""

        return super().device_info


# ----------------------------- Per-Device Location Status ----------------------


class GoogleFindMyLocationStatusSensor(GoogleFindMyDeviceEntity, SensorEntity):
    """Per-device enum sensor with the live age class of the published fix.

    Takes over the live part of the device_tracker ``location_status``
    attribute, which holds the value of the last observation so that a tick
    without a new fix writes nothing new on the tracker. The value is derived
    live on every coordinator tick from the coordinator display row
    (``get_display_location_data_for_subentry``) and the shared
    ``classify_location_status``. The tracker uses the same function and the
    same row selection (``select_display_row`` over the current row and the
    last reliable fix). The two last-good caches can still differ: after the
    device was hidden the tracker clears its own while the coordinator keeps
    its until the device is purged, and a tracker entity that was disabled
    starts again without one:

    - ``current``, ``aging`` or ``stale`` against the configured stale threshold;
    - ``None`` (HA state ``unknown``) when there is no row or no usable age.

    Every tick writes; HA records a change only when the value changes, so an
    unchanged class across ticks is a report, not a change (``force_update``
    stays ``False``).
    """

    _attr_has_entity_name = True
    _attr_entity_registry_enabled_default = True
    entity_description = LOCATION_STATUS_DESCRIPTION

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the sensor."""
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
            f"{safe_id}_location_status",
            separator="_",
        )

    @property
    def native_value(self) -> str | None:
        """Return ``current``/``aging``/``stale`` or ``None`` when unknown."""
        dev_id = self._device_id
        if not dev_id:
            return None
        row = self.coordinator.get_display_location_data_for_subentry(
            self.subentry_key, dev_id
        )
        status = classify_location_status(
            location_age_seconds(row, time.time()),
            resolve_stale_threshold(self.coordinator),
        )
        return None if status == "unknown" else status

    @property
    def available(self) -> bool:
        """Mirror device presence, falling back to coordinator availability."""
        if not super().available:
            return False
        if not self._device_id:
            return False
        return self.coordinator_has_device()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Keep the device label in sync and push the recomputed state."""
        if not self.coordinator_has_device():
            self.async_write_ha_state()
            return
        self.refresh_device_label_from_coordinator(log_prefix="LocationStatus")
        current_name = self._device.get("name")
        if isinstance(current_name, str):
            self.maybe_update_device_registry_name(current_name)
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Expose DeviceInfo using the shared entity helper."""

        return super().device_info


# ----------------------------- Per-Device BLE Battery ---------------------------


# ----------------------------- Per-Device Coarse Fix ---------------------------


# Marks "no write in progress" for GoogleFindMyCoarseFixSensor; ``None`` is
# a valid snapshot (no coarse fix), so it cannot serve as the marker.
_NO_SNAPSHOT = object()


class GoogleFindMyCoarseFixSensor(GoogleFindMyDeviceEntity, SensorEntity):
    """Per-device timestamp of a coarse fix the accuracy gate discarded (#216).

    The gate refuses a wide fix as a POSITION, so Home Assistant's zone logic
    never sees it; its information (a newer, but imprecise sighting) stays
    useful. The main device_tracker holds its ``coarse_*`` attributes with the
    observation and the ``_last_location`` tracker carries none, so this
    sensor is the live carrier:

    - state: the time of the coarse fix (``coarse_last_seen``) while the
      coordinator still offers a fresh one, read through
      ``get_fresh_coarse_fix`` (the accessor the contract reserves for
      publishers); otherwise ``None`` (HA state ``unknown``, not
      ``unavailable``). A coarse fix exists only while it is newer than the
      adopted fix, so ``state > sensor.*_last_seen`` means "a newer, imprecise
      sighting exists";
    - attributes: ``coarse_accuracy`` in metres and the coordinates only as
      ``coarse_latitude``/``coarse_longitude`` (the names redacted in
      diagnostics), never ``latitude``/``longitude``, so no map marker and no
      zone logic.
    """

    _attr_has_entity_name = True
    # Enabled by default: this is the only live carrier of the coarse fix, and
    # disabled it would vanish silently for every user. Recorder cost is about
    # two state changes per coarse fix (appear, expire); measured about 12 rows
    # a day for a whole installation.
    _attr_entity_registry_enabled_default = True
    entity_description = COARSE_FIX_DESCRIPTION

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(
            coordinator,
            device,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
            fallback_label=device.get("name"),
        )
        self._device_id: str | None = device.get("id")
        self._write_snapshot: object = _NO_SNAPSHOT
        safe_id = self._device_id if self._device_id is not None else "unknown"
        entry_id = self.entry_id or "default"
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            entry_id,
            subentry_identifier,
            f"{safe_id}_coarse_fix",
            separator="_",
        )

    def _coarse_fix(self) -> Mapping[str, Any] | None:
        """Return the coarse fix for the state being built.

        During a state write the state and the attributes come from one read
        (see ``_async_write_ha_state``), so a fix that expires between the two reads
        cannot give a timestamp state with no attributes, or the reverse.
        Outside a write every call reads afresh.
        """
        if self._write_snapshot is not _NO_SNAPSHOT:
            return cast("Mapping[str, Any] | None", self._write_snapshot)
        return self._read_coarse_fix()

    def _read_coarse_fix(self) -> Mapping[str, Any] | None:
        """Return the fresh coarse fix of this device, if the coordinator has one."""
        dev_id = self._device_id
        get_fresh = getattr(self.coordinator, "get_fresh_coarse_fix", None)
        if not dev_id or not callable(get_fresh):
            return None
        coarse = get_fresh(dev_id)
        return coarse if isinstance(coarse, Mapping) and coarse else None

    @property
    def native_value(self) -> datetime | None:
        """Return the time of the fresh coarse fix, or ``None`` without one."""
        coarse = self._coarse_fix()
        if coarse is None:
            return None
        try:
            return datetime.fromtimestamp(float(coarse["last_seen"]), tz=UTC)
        except (KeyError, TypeError, ValueError, OverflowError, OSError):
            return None

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return accuracy and coordinates under the ``coarse_*`` names only."""
        coarse = self._coarse_fix()
        if coarse is None:
            return None
        attrs: dict[str, Any] = {}
        lat, lon = coarse.get("latitude"), coarse.get("longitude")
        if lat is not None and lon is not None:
            attrs["coarse_latitude"] = lat
            attrs["coarse_longitude"] = lon
        if coarse.get("accuracy") is not None:
            attrs["coarse_accuracy"] = coarse["accuracy"]
        return attrs or None

    @property
    def available(self) -> bool:
        """Mirror device presence, falling back to coordinator availability."""
        if not super().available:
            return False
        if not self._device_id:
            return False
        return self.coordinator_has_device()

    @callback
    def _async_write_ha_state(self) -> None:
        """Write the state with one read of the coarse fix for state and attributes.

        On every Core line the CI tests, each state write goes
        through this method (``async_write_ha_state``, ``async_update_ha_state``
        and ``schedule_update_ha_state``). From Core 2026.5 on,
        ``async_write_ha_state`` is ``@final`` and its docstring names this
        method as the one to override.
        """
        self._write_snapshot = self._read_coarse_fix()
        try:
            super()._async_write_ha_state()
        finally:
            self._write_snapshot = _NO_SNAPSHOT

    @callback
    def _handle_coordinator_update(self) -> None:
        """Keep the device label in sync and push the recomputed state."""
        if not self.coordinator_has_device():
            self.async_write_ha_state()
            return
        self.refresh_device_label_from_coordinator(log_prefix="CoarseFix")
        current_name = self._device.get("name")
        if isinstance(current_name, str):
            self.maybe_update_device_registry_name(current_name)
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Expose DeviceInfo using the shared entity helper."""

        return super().device_info


class GoogleFindMyBLEBatterySensor(GoogleFindMyDeviceEntity, RestoreSensor):
    """Per-device battery sensor from FMDN hashed-flags BLE advertisement.

    Reports a percentage (100/25/5) mapped from the FMDN 2-bit battery level
    (NORMAL/LOW/CRITICALLY_LOW).  Uses ``SensorDeviceClass.BATTERY`` for automatic
    dynamic icons and HA battery grouping.

    Behavior:
    - Created at setup when the entity registry already holds its unique_id,
      otherwise on the first decoded battery data from the EID resolver. A new
      device without BLE data gets no battery sensor.
    - Restores state across HA restarts via RestoreSensor, together with the
      wall-clock time of the last BLE sighting.
    - Shows a value only while the last BLE sighting is at most 7 days old
      (``BLE_BATTERY_RESTORE_MAX_AGE_S``), for a restored value as well as for
      resolver data, at restore time and while running. A restored value
      without a sighting time is discarded, and a sighting time more than
      ``BLE_BATTERY_FUTURE_TOLERANCE_S`` in the future counts as invalid.
    - Available while the coordinator considers the device present; while it
      is absent, only as long as a value within the age limit is known.
    - Reads battery state directly from the EID resolver (no coordinator proxy).
    """

    _attr_has_entity_name = True
    _attr_entity_registry_enabled_default = True
    entity_description = BLE_BATTERY_DESCRIPTION
    _last_observation_wall: float | None = None

    _unrecorded_attributes = frozenset(
        {
            "last_ble_observation",
            "google_device_id",
            "battery_raw_level",
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
        """Initialize the BLE battery sensor."""
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
            f"{safe_id}_ble_battery",
            separator="_",
        )
        self._attr_native_value: int | None = None
        self._last_observation_wall = None

    def _get_resolver(self) -> GoogleFindMyEIDResolver | None:
        """Return the EID resolver from hass.data, or None."""
        domain_data = self.hass.data.get(DOMAIN)
        if not isinstance(domain_data, dict):
            return None
        return cast(
            "GoogleFindMyEIDResolver | None", domain_data.get(DATA_EID_RESOLVER)
        )

    def _fresh_resolver_state(self) -> BLEBatteryState | None:
        """Return the resolver state if its sighting is at most 7 days old."""
        resolver = self._get_resolver()
        if resolver is None or self._device_id is None:
            return None
        state = resolver.get_ble_battery_state(self._device_id)
        if state is None or not _sighting_within_limit(state.observed_at_wall):
            return None
        return state

    def _restored_value_if_fresh(self) -> int | None:
        """Return the cached value if its sighting is at most 7 days old."""
        wall = self._last_observation_wall
        if (
            self._attr_native_value is None
            or wall is None
            or not _sighting_within_limit(wall)
        ):
            return None
        return self._attr_native_value

    @property
    def native_value(self) -> int | None:
        """Return battery percentage from resolver, or restored value.

        Both count only while their BLE sighting is at most 7 days old
        (``BLE_BATTERY_RESTORE_MAX_AGE_S``); resolver data older than that
        counts as no data, and then the restored value under the same limit
        applies.
        """
        state = self._fresh_resolver_state()
        if state is None:
            return self._restored_value_if_fresh()
        return state.battery_pct

    @property
    def available(self) -> bool:
        """Return True when the coordinator considers the device present.

        No BLE staleness TTL for presence — availability follows the
        coordinator's TTL-smoothed presence, which avoids flapping for trackers
        that transmit the flags byte infrequently. The value itself is limited:
        while the device is absent, the sensor stays available only with a value
        whose BLE sighting is at most 7 days old.
        """
        if not super().available:
            return False
        if not self.coordinator_has_device():
            return False

        try:
            if self._device_id is not None and hasattr(
                self.coordinator, "is_device_present"
            ):
                raw = self.coordinator.is_device_present(self._device_id)
                present = bool(raw) if not isinstance(raw, bool) else raw
                if present:
                    return True
                # Presence expired → available only with a restored value
                return self._restored_value_if_fresh() is not None
        except Exception:  # noqa: BLE001
            pass

        # Unknown presence → available if we have any known value
        return self._restored_value_if_fresh() is not None

    @property
    def extra_restore_state_data(self) -> BLEBatteryExtraStoredData:
        """Return value, unit and the time of the last BLE sighting.

        Value and sighting time always come from the same source: the fresh
        resolver state if there is one, otherwise the cached value with its
        own sighting time.
        """
        state = self._fresh_resolver_state()
        if state is not None:
            return BLEBatteryExtraStoredData(
                state.battery_pct,
                self.native_unit_of_measurement,
                state.observed_at_wall,
            )
        return BLEBatteryExtraStoredData(
            self._restored_value_if_fresh(),
            self.native_unit_of_measurement,
            self._last_observation_wall,
        )

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
            "battery_raw_level": state.battery_level,
            "last_ble_observation": datetime.fromtimestamp(
                state.observed_at_wall, tz=UTC
            ).isoformat(),
            "google_device_id": self._device_id,
        }

    @callback
    def _handle_coordinator_update(self) -> None:
        """Propagate coordinator updates and keep device label in sync."""
        if not self.coordinator_has_device():
            self.async_write_ha_state()
            return

        self.refresh_device_label_from_coordinator(log_prefix="BLEBattery")

        # Update cached native_value and sighting time from resolver data
        # within the age limit, for restore persistence
        state = self._fresh_resolver_state()
        if state is not None:
            self._attr_native_value = state.battery_pct
            self._last_observation_wall = state.observed_at_wall

        self.async_write_ha_state()

    async def async_added_to_hass(self) -> None:
        """Restore battery percentage from HA's persistent store."""
        await super().async_added_to_hass()

        try:
            data = await self.async_get_last_sensor_data()
            value = getattr(data, "native_value", None) if data else None
        except (RuntimeError, AttributeError) as e:  # noqa: BLE001
            _LOGGER.debug(
                "Failed to restore BLE battery state for %s: %s",
                self.entity_id,
                e,
            )
            value = None

        if value is None or value in ("unknown", "unavailable"):
            return

        try:
            restored_pct = int(float(value))
        except (ValueError, TypeError):
            return

        observed_wall = await self._async_restored_observation_wall()
        if observed_wall is None:
            _LOGGER.debug(
                "Discarding restored BLE battery value for device=%s: "
                "no BLE sighting time stored",
                self._device_id,
            )
            return

        if (
            self._last_observation_wall is not None
            and observed_wall <= self._last_observation_wall
        ):
            # A coordinator update during setup already cached a newer sighting.
            return

        self._attr_native_value = restored_pct
        self._last_observation_wall = observed_wall
        self.async_write_ha_state()

    async def _async_restored_observation_wall(self) -> float | None:
        """Return the stored time of the last BLE sighting, or ``None``.

        Reads ``last_observation_wall`` from the extra restore data; states
        saved before that field existed fall back to the
        ``last_ble_observation`` attribute of the restored state.
        """
        extra = await self.async_get_last_extra_data()
        if extra is not None:
            stored = BLEBatteryExtraStoredData.from_dict(extra.as_dict())
            if stored is not None and stored.last_observation_wall is not None:
                return stored.last_observation_wall

        last_state = await self.async_get_last_state()
        attributes = getattr(last_state, "attributes", None)
        raw = (
            attributes.get("last_ble_observation")
            if isinstance(attributes, Mapping)
            else None
        )
        if not isinstance(raw, str):
            return None
        try:
            observed = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        return observed.timestamp()

    @property
    def device_info(self) -> DeviceInfo:
        """Expose DeviceInfo using the shared entity helper."""
        return super().device_info
