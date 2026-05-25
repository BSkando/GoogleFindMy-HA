# custom_components/googlefindmy/device_tracker.py
"""Device tracker platform for Google Find My Device.

Notes on design and consistency with the coordinator:
- Entities are created from the coordinator's full snapshot and added dynamically later.
- Location significance gating and stale-timestamp guard are enforced by the coordinator,
  not here. This entity simply reflects the coordinator's sanitized cache.
- Extra attributes come from `_as_ha_attributes(...)` and intentionally use stable keys
  like `accuracy_m` for recorder friendliness, while the entity's built-in accuracy
  property exposes an integer `gps_accuracy` to HA Core.
- End devices rely on tracker subentry identifiers to associate with their
  registry entry; avoid manual `via_device` linkage.

Entry-scope guarantees (C2):
- Unique IDs are entry-scoped using the subentry-aware schema:
  "<entry_id>:<subentry_identifier>:<device_id>" (or "<subentry_identifier>:<device_id>"
  during bootstrap before the entry ID attaches).
- Device Registry identifiers are also entry-scoped to avoid cross-account merges.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from homeassistant.components.device_tracker import SourceType
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    ATTR_GPS_ACCURACY,
    ATTR_LATITUDE,
    ATTR_LONGITUDE,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import EntityRecoveryManager, _extract_email_from_entry
from .const import (
    CONF_OAUTH_TOKEN,
    DATA_SECRET_BUNDLE,
    DEFAULT_STALE_THRESHOLD,
    DOMAIN,
    OPT_STALE_THRESHOLD,
    TRACKER_SUBENTRY_KEY,
)
from .coordinator import GoogleFindMyCoordinator, _as_ha_attributes
from .discovery import (
    CLOUD_DISCOVERY_NAMESPACE,
    _cloud_discovery_stable_key,
    _redact_account_for_log,
    _trigger_cloud_discovery,
)
from .entity import (
    GoogleFindMyDeviceEntity,
    ensure_config_subentry_id,
    ensure_dispatcher_dependencies,
    known_config_subentry_ids,
    resolve_coordinator,
    schedule_add_entities,
)
from .ha_typing import RestoreEntity, TrackerEntity, callback

_LOGGER = logging.getLogger(__name__)


@dataclass
class _TrackerScope:
    """Tracker subentry details for entity creation."""

    subentry_key: str
    config_subentry_id: str | None
    identifier: str | None


def _subentry_type(subentry: Any | None) -> str | None:
    """Return the declared subentry type, if present."""

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


def _normalize_identifier_set(candidate: Iterable[Any] | None) -> list[str]:
    """Return a sorted list of identifier strings extracted from an iterable."""

    if not candidate:
        return []

    normalized: set[str] = set()
    for item in candidate:
        if isinstance(item, str) and item:
            normalized.add(item)
    return sorted(normalized)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    *,
    config_subentry_id: str | None = None,
) -> None:
    """Set up Google Find My Device tracker entities.

    Behavior:
    - On setup, create entities for all devices in the coordinator snapshot (if any).
    - Listen for coordinator updates and add entities for newly discovered devices.
    """
    _LOGGER.debug(
        "Device tracker setup invoked for entry=%s config_subentry_id=%s",
        getattr(config_entry, "entry_id", None),
        config_subentry_id,
    )
    coordinator = resolve_coordinator(config_entry)
    ensure_dispatcher_dependencies(hass)
    if getattr(coordinator, "config_entry", None) is None:
        coordinator.config_entry = config_entry

    def _known_subentry_ids() -> set[str]:
        return known_config_subentry_ids(config_entry)

    def _collect_tracker_scopes(
        hint_subentry_id: str | None = None,
        forwarded_config_id: str | None = None,
    ) -> list[_TrackerScope]:
        """Collect tracker scopes from metadata, subentries, and dispatcher hints.

        The coordinator may expose tracker subentries via `_subentry_metadata` or the
        config-entry `subentries` mapping. Because some subentries arrive without a
        config_subentry_id (for example, placeholders surfaced through dispatcher
        hints), scopes are indexed by a stable composite key of the subentry key and
        identifier rather than the identifier alone. This prevents deduplication of
        multiple tracker placeholders that have no identifier yet.

        `hint_subentry_id` carries the dispatcher-provided subentry identifier, while
        `forwarded_config_id` mirrors Home Assistant's forwarded config_subentry_id so
        placeholder scopes can inherit the forwarded value when present.
        """

        scopes: dict[tuple[str | None, str | None], _TrackerScope] = {}

        subentry_metas = getattr(coordinator, "_subentry_metadata", None)
        if isinstance(subentry_metas, Mapping):
            for key, meta in subentry_metas.items():
                meta_features = getattr(meta, "features", ())
                if "device_tracker" not in meta_features:
                    continue

                stable_identifier = getattr(meta, "stable_identifier", None)
                identifier = (
                    stable_identifier()
                    if callable(stable_identifier)
                    else None
                    or getattr(meta, "config_subentry_id", None)
                    or coordinator.stable_subentry_identifier(key=key)
                )
                scope_key = (key, identifier)
                scopes[scope_key] = _TrackerScope(
                    key,
                    getattr(meta, "config_subentry_id", None),
                    identifier,
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

                if "device_tracker" not in subentry_features:
                    continue

                config_id = getattr(subentry, "subentry_id", None) or getattr(
                    subentry, "entry_id", None
                )
                identifier = (
                    config_id
                    or coordinator.stable_subentry_identifier(key=group_key)
                    or TRACKER_SUBENTRY_KEY
                )
                scope_key = (group_key, identifier)
                scopes.setdefault(
                    scope_key,
                    _TrackerScope(
                        group_key or TRACKER_SUBENTRY_KEY, config_id, identifier
                    ),
                )

        if hint_subentry_id:
            scope_key = (TRACKER_SUBENTRY_KEY, hint_subentry_id)
            scopes.setdefault(
                scope_key,
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
                TRACKER_SUBENTRY_KEY,
                forwarded_config_id or fallback_identifier,
                fallback_identifier or TRACKER_SUBENTRY_KEY,
            )
        ]

    added_unique_ids: set[str] = set()
    scope_states: dict[str, dict[str, Any]] = {}

    def _add_scope(scope: _TrackerScope, forwarded_config_id: str | None) -> None:
        """Initialize entity listeners for a tracker scope and dedupe re-entrance.

        Scopes are keyed by identifier/config_subentry_id/subentry_key to avoid
        re-registering listeners when dispatcher hints replay or multiple subentry
        placeholders collapse to the same identifier. Each successful registration
        attaches a coordinator listener, schedules entity recovery hooks, and
        ensures `async_add_entities` receives the forwarded config_subentry_id for
        the scope (when available).
        """

        scope_identifier = (
            scope.identifier or scope.config_subentry_id or scope.subentry_key
        )
        if scope_identifier in scope_states:
            scope_states[scope_identifier]["scan"]()
            return

        tracker_subentry_key = scope.subentry_key or TRACKER_SUBENTRY_KEY

        candidate_subentry_id = scope.config_subentry_id or forwarded_config_id
        tracker_config_subentry_id = (
            ensure_config_subentry_id(
                config_entry,
                "device_tracker",
                candidate_subentry_id,
                known_ids=_known_subentry_ids(),
            )
            if candidate_subentry_id is not None
            else None
        )

        if (
            candidate_subentry_id is not None
            and tracker_config_subentry_id is None
            and _known_subentry_ids()
        ):
            _LOGGER.debug(
                "Device tracker setup: skipping subentry '%s' because the config_subentry_id is unknown",
                candidate_subentry_id,
            )
            return

        tracker_identifier = (
            tracker_config_subentry_id
            or scope.identifier
            or coordinator.stable_subentry_identifier(key=tracker_subentry_key)
            or tracker_subentry_key
        )
        tracker_config_subentry_for_entities = (
            tracker_config_subentry_id or tracker_identifier
        )

        expected_config_subentry_id = (
            scope.config_subentry_id or tracker_config_subentry_id
        )

        _LOGGER.debug(
            "Device tracker setup: subentry_key=%s, config_subentry_id=%s",
            tracker_subentry_key,
            tracker_config_subentry_id,
        )

        if (
            forwarded_config_id
            and expected_config_subentry_id
            and forwarded_config_id != expected_config_subentry_id
        ):
            current_known_ids = _known_subentry_ids()
            if current_known_ids and forwarded_config_id not in current_known_ids:
                _LOGGER.debug(
                    "Device tracker setup skipped for unknown forwarded subentry '%s' (known: %s)",
                    forwarded_config_id,
                    ", ".join(sorted(current_known_ids)),
                )
                return
            _LOGGER.debug(
                "Device tracker setup ignored for unrelated subentry '%s' (expected '%s')",
                forwarded_config_id,
                expected_config_subentry_id,
            )
            return

        if tracker_config_subentry_for_entities is None:
            _LOGGER.debug(
                "Device tracker setup: awaiting config_subentry_id for key '%s'; skipping",
                tracker_subentry_key,
            )
            return

        tracker_config_subentry_for_entities_str = tracker_config_subentry_for_entities
        tracker_subentry_identifier_str = (
            tracker_identifier
            or tracker_config_subentry_for_entities_str
            or tracker_subentry_key
        )

        tracker_meta = coordinator.get_subentry_metadata(key=tracker_subentry_key)
        known_ids: set[str] = set()

        coordinator.get_subentry_snapshot(tracker_subentry_key)

        tracker_entities_added = False

        def _schedule_tracker_entities(
            new_entities: Iterable[GoogleFindMyDeviceTracker],
            update_before_add: bool = True,
        ) -> None:
            nonlocal tracker_entities_added

            entity_list = list(new_entities)
            tracker_entities_added |= bool(entity_list)

            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=entity_list,
                update_before_add=update_before_add,
                config_subentry_id=tracker_config_subentry_for_entities_str,
                log_owner="Device tracker setup",
                logger=_LOGGER,
            )

        def _build_entities(
            snapshot: Sequence[Mapping[str, Any]],
        ) -> list[GoogleFindMyDeviceTracker]:
            to_add: list[GoogleFindMyDeviceTracker] = []
            for device in snapshot:
                dev_id = device.get("id")
                name = device.get("name")
                if not dev_id or not name:
                    continue
                if dev_id in known_ids:
                    continue

                # Create main tracker entity (with stale detection)
                entity = GoogleFindMyDeviceTracker(
                    coordinator,
                    dict(device),
                    subentry_key=tracker_subentry_key,
                    subentry_identifier=tracker_subentry_identifier_str,
                )
                unique_id = getattr(entity, "unique_id", None)
                if isinstance(unique_id, str):
                    if unique_id in added_unique_ids:
                        continue
                    added_unique_ids.add(unique_id)

                # Add main tracker entity first
                known_ids.add(dev_id)
                to_add.append(entity)

                # Create last location tracker entity (always shows last known location)
                last_location_entity = GoogleFindMyLastLocationTracker(
                    coordinator,
                    dict(device),
                    subentry_key=tracker_subentry_key,
                    subentry_identifier=tracker_subentry_identifier_str,
                )
                last_location_unique_id = getattr(
                    last_location_entity, "unique_id", None
                )
                if isinstance(last_location_unique_id, str):
                    if last_location_unique_id not in added_unique_ids:
                        added_unique_ids.add(last_location_unique_id)
                        to_add.append(last_location_entity)
            return to_add

        @callback
        def _scan_available_trackers_from_coordinator() -> None:
            snapshot = coordinator.get_subentry_snapshot(tracker_subentry_key)
            if not snapshot:
                meta = coordinator.get_subentry_metadata(key=tracker_subentry_key)
                visible_ids = _normalize_identifier_set(
                    getattr(meta, "visible_device_ids", None)
                )
                enabled_ids = _normalize_identifier_set(
                    getattr(meta, "enabled_device_ids", None)
                )
                snapshot_map = getattr(coordinator, "_subentry_snapshots", {})
                snapshot_keys = _normalize_identifier_set(snapshot_map.keys())
                reconfigure_marker = getattr(coordinator, "recent_reconfigure_at", None)
                _LOGGER.debug(
                    (
                        "Device tracker setup: no coordinator snapshot for subentry %s "
                        "(config_subentry_id=%s, visible_device_ids=%s, "
                        "enabled_device_ids=%s, snapshot_keys=%s, snapshot_cache_size=%d, "
                        "initial_discovery_done=%s, post_reconfigure=%s)"
                    ),
                    tracker_subentry_key,
                    getattr(meta, "config_subentry_id", None),
                    ", ".join(visible_ids) or "<empty>",
                    ", ".join(enabled_ids) or "<empty>",
                    ", ".join(snapshot_keys) or "<empty>",
                    len(snapshot_map) if isinstance(snapshot_map, Mapping) else 0,
                    getattr(coordinator, "_initial_discovery_done", None),
                    bool(reconfigure_marker),
                )
                _schedule_tracker_entities([], True)
                return

            to_add = _build_entities(snapshot)
            if to_add:
                _LOGGER.info(
                    "Adding %d newly discovered Find My tracker(s)", len(to_add)
                )
                _schedule_tracker_entities(to_add, True)

                registry_lookup = getattr(
                    coordinator, "find_tracker_entity_entry", None
                )
                if callable(registry_lookup):
                    all_registered = True

                    for entity in to_add:
                        dev_id = getattr(entity, "device_id", None)

                        try:
                            if not dev_id or registry_lookup(dev_id) is None:
                                all_registered = False
                                break
                        except (
                            Exception
                        ):  # pragma: no cover - best effort registry probe
                            _LOGGER.debug(
                                "Registry lookup failed for tracker %s", dev_id
                            )
                            all_registered = False
                            break

                    if all_registered:
                        _LOGGER.debug(
                            "Device tracker setup: all %d tracker(s) already registered; skipping discovery",
                            len(to_add),
                        )
                        return
                else:
                    _LOGGER.debug(
                        "Device tracker setup: registry helper unavailable; treating trackers as new"
                    )

                email = _extract_email_from_entry(config_entry) or None
                token = config_entry.data.get(CONF_OAUTH_TOKEN)
                token_value = token if isinstance(token, str) and token else None
                secrets_raw = config_entry.data.get(DATA_SECRET_BUNDLE)
                secrets_bundle: Mapping[str, Any] | None
                if isinstance(secrets_raw, Mapping):
                    secrets_bundle = secrets_raw
                else:
                    secrets_bundle = None

                discovery_ns = (
                    f"{CLOUD_DISCOVERY_NAMESPACE}.{config_entry.entry_id}"
                    if config_entry.entry_id
                    else CLOUD_DISCOVERY_NAMESPACE
                )
                stable_key = _cloud_discovery_stable_key(
                    email,
                    token_value,
                    secrets_bundle,
                )

                async def _async_trigger_cloud_scan(new_count: int) -> None:
                    triggered = await _trigger_cloud_discovery(
                        hass,
                        email=email,
                        token=token_value,
                        secrets_bundle=secrets_bundle,
                        discovery_ns=discovery_ns,
                        discovery_stable_key=stable_key,
                        source="cloud_scanner",
                        entry=config_entry,
                    )
                    account_ref = _redact_account_for_log(email, stable_key)
                    if triggered:
                        _LOGGER.info(
                            "Cloud tracker scanner queued discovery for %s after %d newly available tracker(s)",
                            account_ref,
                            new_count,
                        )
                    else:
                        _LOGGER.debug(
                            "Cloud tracker scanner deduplicated discovery for %s",
                            account_ref,
                        )

                hass_async_create_task = getattr(hass, "async_create_task", None)
                if callable(hass_async_create_task):
                    pending = hass_async_create_task(
                        _async_trigger_cloud_scan(len(to_add))
                    )
                    if asyncio.iscoroutine(pending):
                        asyncio.create_task(pending)
                else:
                    _LOGGER.debug(
                        "Device tracker setup: hass missing async_create_task; skipping cloud discovery trigger"
                    )

        unsub = coordinator.async_add_listener(
            _scan_available_trackers_from_coordinator
        )
        config_entry.async_on_unload(unsub)

        _scan_available_trackers_from_coordinator()

        if not tracker_entities_added:
            _schedule_tracker_entities((), True)

        runtime_data = getattr(config_entry, "runtime_data", None)
        recovery_manager = getattr(runtime_data, "entity_recovery_manager", None)

        if isinstance(recovery_manager, EntityRecoveryManager):
            entry_id = getattr(config_entry, "entry_id", None)

            def _is_visible(device_id: str) -> bool:
                try:
                    return bool(
                        device_id
                        and tracker_meta
                        and device_id in getattr(tracker_meta, "visible_device_ids", [])
                    )
                except (
                    TypeError
                ):  # pragma: no cover - fallback for misconfigured metadata
                    return False

            def _is_enabled(device_id: str) -> bool:
                try:
                    return bool(
                        device_id
                        and tracker_meta
                        and device_id in getattr(tracker_meta, "enabled_device_ids", [])
                    )
                except (
                    TypeError
                ):  # pragma: no cover - fallback for misconfigured metadata
                    return False

            def _expected_unique_ids() -> set[str]:
                if not isinstance(entry_id, str) or not entry_id:
                    return set()
                expected: set[str] = set()
                for device in coordinator.get_subentry_snapshot(tracker_subentry_key):
                    if not isinstance(device, Mapping):
                        continue
                    dev_id = device.get("id")
                    if not isinstance(dev_id, str) or not dev_id:
                        continue
                    if not _is_visible(dev_id) or not _is_enabled(dev_id):
                        continue
                    # Main tracker entity
                    expected.add(
                        GoogleFindMyDeviceEntity.join_parts(
                            entry_id,
                            tracker_subentry_identifier_str,
                            dev_id,
                        )
                    )
                    # Last location tracker entity
                    expected.add(
                        GoogleFindMyDeviceEntity.join_parts(
                            entry_id,
                            tracker_subentry_identifier_str,
                            f"{dev_id}:last_location",
                        )
                    )
                return expected

            def _build_recovery_entities(
                missing: set[str],
            ) -> list[GoogleFindMyDeviceTracker]:
                if not missing:
                    return []
                built: list[GoogleFindMyDeviceTracker] = []
                if not isinstance(entry_id, str) or not entry_id:
                    return built
                for device in coordinator.get_subentry_snapshot(tracker_subentry_key):
                    if not isinstance(device, Mapping):
                        continue
                    dev_id = device.get("id")
                    name = device.get("name")
                    if (
                        not isinstance(dev_id, str)
                        or not isinstance(name, str)
                        or not dev_id
                        or not name
                    ):
                        continue
                    if not _is_visible(dev_id) or not _is_enabled(dev_id):
                        continue

                    # Main tracker entity
                    unique_id = GoogleFindMyDeviceEntity.join_parts(
                        entry_id,
                        tracker_subentry_identifier_str,
                        dev_id,
                    )
                    if unique_id in missing:
                        built.append(
                            GoogleFindMyDeviceTracker(
                                coordinator,
                                dict(device),
                                subentry_key=tracker_subentry_key,
                                subentry_identifier=tracker_subentry_identifier_str,
                            )
                        )

                    # Last location tracker entity
                    last_location_unique_id = GoogleFindMyDeviceEntity.join_parts(
                        entry_id,
                        tracker_subentry_identifier_str,
                        f"{dev_id}:last_location",
                    )
                    if last_location_unique_id in missing:
                        built.append(
                            GoogleFindMyLastLocationTracker(
                                coordinator,
                                dict(device),
                                subentry_key=tracker_subentry_key,
                                subentry_identifier=tracker_subentry_identifier_str,
                            )
                        )
                return built

            recovery_manager.register_device_tracker_platform(
                expected_unique_ids=_expected_unique_ids,
                entity_factory=_build_recovery_entities,
                add_entities=_schedule_tracker_entities,
            )

        scope_states[scope_identifier] = {
            "scan": _scan_available_trackers_from_coordinator
        }

    seen_subentries: set[str | None] = set()
    seen_subentry_keys: set[str] = set()
    placeholder_subentries: set[str | None] = set()
    placeholder_subentry_keys: set[str] = set()

    @callback
    def async_add_subentry(subentry: Any | None = None) -> None:
        """Process a subentry or placeholder and register tracker scopes.

        Dispatcher callbacks may supply only a `subentry_id` string while Home
        Assistant is still finalizing the subentry record. Placeholder identifiers
        enter the `placeholder_*` sets so later calls with full subentry objects can
        replace them. Deduplication happens per subentry identifier and group key,
        and each call forwards the dispatcher-provided `config_subentry_id` to
        `_collect_tracker_scopes` so listeners can inherit the forwarded config ID
        when Home Assistant supplies one.
        """

        subentry_key = TRACKER_SUBENTRY_KEY
        subentry_identifier = None
        if isinstance(subentry, str):
            subentry_identifier = subentry
        else:
            subentry_identifier = getattr(subentry, "subentry_id", None) or getattr(
                subentry, "entry_id", None
            )
            data = getattr(subentry, "data", None)
            if isinstance(data, Mapping):
                data_group_key = data.get("group_key")
                if isinstance(data_group_key, str) and data_group_key.strip():
                    subentry_key = data_group_key.strip()

        if not subentry_identifier:
            try:
                stable_identifier = coordinator.stable_subentry_identifier(
                    key=subentry_key
                )
            except Exception:  # pragma: no cover - best effort fallback
                stable_identifier = None

            subentry_identifier = (
                stable_identifier
                if isinstance(stable_identifier, str) and stable_identifier
                else subentry_key
            )

        subentry_type = _subentry_type(subentry)
        _LOGGER.debug(
            "Device tracker setup: processing subentry '%s' (type=%s)",
            subentry_identifier,
            subentry_type,
        )
        if subentry_type is not None and subentry_type != "tracker":
            _LOGGER.debug(
                "Device tracker setup skipped for unrelated subentry '%s' (type '%s')",
                subentry_identifier,
                subentry_type,
            )
            return

        has_full_context = hasattr(subentry, "subentry_id") or hasattr(subentry, "data")
        if subentry_identifier in seen_subentries or subentry_key in seen_subentry_keys:
            if has_full_context and (
                subentry_identifier in placeholder_subentries
                or subentry_key in placeholder_subentry_keys
            ):
                placeholder_subentries.discard(subentry_identifier)
                placeholder_subentry_keys.discard(subentry_key)
                seen_subentries.discard(subentry_identifier)
                seen_subentry_keys.discard(subentry_key)
            else:
                return

        if has_full_context:
            seen_subentries.add(subentry_identifier)
            seen_subentry_keys.add(subentry_key)
        else:
            placeholder_subentries.add(subentry_identifier)
            placeholder_subentry_keys.add(subentry_key)

        for scope in _collect_tracker_scopes(
            subentry_identifier, forwarded_config_id=subentry_identifier
        ):
            _add_scope(scope, subentry_identifier)

    @callback
    def _handle_subentry_setup(subentry: Any | None = None) -> None:
        """Schedule subentry setup from dispatcher callbacks."""

        hass_async_create_task = getattr(hass, "async_create_task", None)
        if callable(hass_async_create_task):
            result = async_add_subentry(subentry)
            if inspect.isawaitable(result):
                hass_async_create_task(result)
            return

        hass_add_job = getattr(hass, "add_job", None)
        if callable(hass_add_job):
            result = async_add_subentry(subentry)
            if inspect.isawaitable(result):
                hass_add_job(result)
            return

        async_add_subentry(subentry)

    runtime_data = getattr(config_entry, "runtime_data", None)
    subentry_manager = getattr(runtime_data, "subentry_manager", None)
    managed_subentries = getattr(subentry_manager, "managed_subentries", None)
    entry_subentries = (
        config_entry.subentries
        if isinstance(getattr(config_entry, "subentries", None), Mapping)
        else None
    )

    if isinstance(managed_subentries, Mapping) and managed_subentries:
        for managed_subentry in managed_subentries.values():
            async_add_subentry(managed_subentry)
    elif isinstance(managed_subentries, Mapping):
        async_add_subentry(config_subentry_id)
    elif isinstance(entry_subentries, Mapping) and entry_subentries:
        for managed_subentry in entry_subentries.values():
            async_add_subentry(managed_subentry)
    else:
        async_add_subentry(config_subentry_id)

    config_entry.async_on_unload(
        async_dispatcher_connect(
            hass,
            f"{DOMAIN}_subentry_setup_{config_entry.entry_id}",
            _handle_subentry_setup,
        )
    )


# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------


class GoogleFindMyDeviceTracker(GoogleFindMyDeviceEntity, TrackerEntity, RestoreEntity):
    """Representation of a Google Find My Device tracker."""

    # Convention: trackers represent the device itself; the entity name
    # inherits from the device name via has_entity_name=True.
    _attr_has_entity_name = True
    _attr_source_type = SourceType.GPS
    _attr_entity_category: EntityCategory | None = (
        None  # ensure tracker is not diagnostic
    )
    # Default to enabled in the registry for per-device trackers
    _attr_entity_registry_enabled_default = True
    _attr_translation_key = "device"
    _attr_attribution: str | None = None  # Set in __init__ with account email

    # ---- Display-name policy (strip legacy prefixes, no new prefixes) ----
    @staticmethod
    def _display_name(raw: str | None) -> str:
        """Return the UI display name without legacy prefixes."""
        name = (raw or "").strip()
        if name.lower().startswith("find my - "):
            name = name[10:].strip()
        return name or "Google Find My Device"

    def device_label(self) -> str:
        """Return the sanitized device label used for DeviceInfo."""

        return self._display_name(super().device_label())

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the tracker entity."""
        super().__init__(
            coordinator,
            device,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
            fallback_label=device.get("name"),
        )

        entry_id = self.entry_id
        dev_id = self.device_id

        self._attr_unique_id = self.build_unique_id(
            entry_id,
            subentry_identifier,
            dev_id,
        )

        # With has_entity_name=True, setting name to None means the entity
        # inherits only the device name (no suffix). The translation_key "device"
        # is used for state attributes but not for the entity name itself.
        self._attr_name = None

        # Attribution for data source identification (helps distinguish from Bermuda etc.)
        email = _extract_email_from_entry(coordinator.config_entry)
        self._attr_attribution = f"FMDN ({email})" if email else "FMDN"

        # Persist a "last good" fix to keep map position usable when current accuracy is filtered
        self._last_good_accuracy_data: dict[str, Any] | None = None
        self._logged_visibility_block = False

    async def async_added_to_hass(self) -> None:
        """Restore last known location and seed the coordinator cache.

        On cold boots where the coordinator hasn't polled yet, we restore the last
        coordinates from the state machine to provide a better initial UX. We also
        prime the coordinator's cache via its public priming API (no private access).
        """
        await super().async_added_to_hass()

        try:
            last_state = await self.async_get_last_state()
        except (RuntimeError, AttributeError) as err:
            _LOGGER.debug("Failed to get last state for %s: %s", self.entity_id, err)
            return

        if not last_state:
            return

        # Standard device_tracker attributes (with safe fallbacks for legacy keys)
        lat = last_state.attributes.get(
            ATTR_LATITUDE, last_state.attributes.get("latitude")
        )
        lon = last_state.attributes.get(
            ATTR_LONGITUDE, last_state.attributes.get("longitude")
        )
        acc = last_state.attributes.get(
            ATTR_GPS_ACCURACY, last_state.attributes.get("gps_accuracy")
        )

        restored: dict[str, Any] = {}
        try:
            if lat is not None and lon is not None:
                restored["latitude"] = float(lat)
                restored["longitude"] = float(lon)
            if acc is not None:
                # HA core accuracy attribute is an int (meters).
                restored["accuracy"] = int(float(acc))
        except (TypeError, ValueError) as ex:
            _LOGGER.debug("Invalid restored coordinates for %s: %s", self.entity_id, ex)
            restored = {}

        if restored:
            self._last_good_accuracy_data = {**restored}
            # Prime coordinator cache using its public API (no private access).
            dev_id = self.device_id
            try:
                self.coordinator.prime_device_location_cache(dev_id, restored)
            except (AttributeError, TypeError) as err:
                _LOGGER.debug(
                    "Failed to seed coordinator cache for %s: %s", self.entity_id, err
                )

            self._sync_location_attrs()
            self.async_write_ha_state()

    # ---------------- Device Info + Map Link ----------------
    @property
    def device_info(self) -> DeviceInfo:
        """Expose DeviceInfo using the shared entity helper."""

        return super().device_info

    def _current_row(self) -> dict[str, Any] | None:
        """Get current device data from the coordinator's public cache API."""

        dev_id = self.device_id
        try:
            data = self.coordinator.get_device_location_data_for_subentry(
                self.subentry_key, dev_id
            )
        except (AttributeError, TypeError):
            return None
        if isinstance(data, dict):
            return data
        return None

    @property
    def available(self) -> bool:
        """Return True if the device is currently present according to the coordinator.

        Presence has priority over restored coordinates: if the device is no
        longer present in the Google list (TTL-smoothed by the coordinator),
        the entity becomes unavailable and the user may delete it via HA UI.
        """
        if not super().available:
            return False
        if not self.coordinator.is_device_visible_in_subentry(
            self.subentry_key, self.device_id
        ):
            if not self._logged_visibility_block:
                meta = self.coordinator.get_subentry_metadata(key=self.subentry_key)
                visible_ids = _normalize_identifier_set(
                    getattr(meta, "visible_device_ids", None)
                )
                _LOGGER.debug(
                    (
                        "Tracker '%s' unavailable: device '%s' is not visible in subentry %s "
                        "(config_subentry_id=%s, visible_device_ids=%s)"
                    ),
                    self.entity_id,
                    self.device_id,
                    self.subentry_key,
                    getattr(meta, "config_subentry_id", None),
                    ", ".join(visible_ids) or "<empty>",
                )
                self._logged_visibility_block = True
            return False
        self._logged_visibility_block = False
        if not self.coordinator_has_device():
            return False
        # Prefer coordinator presence; fall back to previous behavior if API is missing.
        try:
            if hasattr(self.coordinator, "is_device_present"):
                if not self.coordinator.is_device_present(self.device_id):
                    return False
        except Exception:
            # Be tolerant in case of older coordinator builds
            pass

        device_data = self._current_row()
        if device_data:
            if (
                device_data.get("latitude") is not None
                and device_data.get("longitude") is not None
            ) or device_data.get("semantic_name") is not None:
                return True
        return self._last_good_accuracy_data is not None

    def _is_stale_threshold_enabled(self) -> bool:
        """Return True - stale threshold is always enabled.

        Users who need the last known location should use the
        "Last Location" entity instead of disabling stale detection.
        """
        return True

    def _get_stale_threshold(self) -> int:
        """Return the configured stale threshold in seconds."""
        entry = getattr(self.coordinator, "config_entry", None)
        if entry is None:
            return DEFAULT_STALE_THRESHOLD
        options = getattr(entry, "options", {})
        if not isinstance(options, Mapping):
            return DEFAULT_STALE_THRESHOLD
        threshold = options.get(OPT_STALE_THRESHOLD, DEFAULT_STALE_THRESHOLD)
        try:
            return int(threshold)
        except (TypeError, ValueError):
            return DEFAULT_STALE_THRESHOLD

    def _get_location_age(self) -> float | None:
        """Return the age of the location data in seconds, or None if unknown."""
        data = self._current_row() or self._last_good_accuracy_data
        if not data:
            return None
        last_seen = data.get("last_seen")
        if last_seen is None:
            return None
        try:
            last_seen_epoch = float(last_seen)
            return time.time() - last_seen_epoch
        except (TypeError, ValueError):
            return None

    def _is_location_stale(self) -> bool:
        """Return True if the location data is considered stale."""
        age = self._get_location_age()
        if age is None:
            return False  # No age information, assume not stale
        threshold = self._get_stale_threshold()
        return age > threshold

    def _get_location_status(self) -> str:
        """Return the location status string based on data age."""
        age = self._get_location_age()
        if age is None:
            return "unknown"
        threshold = self._get_stale_threshold()
        if age > threshold:
            return "stale"
        elif age > threshold / 2:
            return "aging"
        else:
            return "current"

    # ------------------------------------------------------------------
    # Location attribute synchronisation (HA CachedProperties pattern)
    # ------------------------------------------------------------------
    # HA's TrackerEntity uses CachedProperties (backed by propcache) for
    # latitude, longitude, location_accuracy, and location_name.  The
    # Entity base class caches extra_state_attributes the same way.
    # Setting the corresponding _attr_* values is the ONLY reliable way
    # to invalidate those caches across all HA versions (including
    # 2026.2+).  We therefore no longer override the properties directly
    # but recompute all values in _sync_location_attrs() and write the
    # _attr_* attributes before every async_write_ha_state() call.
    # ------------------------------------------------------------------

    def _sync_location_attrs(self) -> None:
        """Recompute and publish every TrackerEntity attribute via _attr_*.

        Must be called before *every* ``async_write_ha_state()`` so that
        HA's ``CachedProperties`` mechanism picks up fresh values.
        """
        stale = self._is_location_stale()

        # --- latitude / longitude / location_accuracy ---
        if stale:
            self._attr_latitude = None
            self._attr_longitude = None
            self._attr_location_accuracy = 0.0
            self._attr_location_name = None
        else:
            data = self._current_row() or self._last_good_accuracy_data
            if not data or data.get("accuracy") is None:
                # Accuracy must be present for a valid GPS fix; without it
                # HA's zone engine raises TypeError on comparison.
                self._attr_latitude = None
                self._attr_longitude = None
                self._attr_location_accuracy = 0.0
            else:
                self._attr_latitude = data.get("latitude")
                self._attr_longitude = data.get("longitude")
                acc = data.get("accuracy")
                try:
                    self._attr_location_accuracy = (
                        float(acc) if acc is not None else 0.0
                    )
                except (TypeError, ValueError):
                    self._attr_location_accuracy = 0.0

            # --- location_name ---
            name_data = self._current_row()
            if not name_data:
                self._attr_location_name = None
            else:
                lat = name_data.get("latitude")
                lon = name_data.get("longitude")
                sem = name_data.get("semantic_name")
                if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                    # Coordinates present -> let HA zone engine decide.
                    self._attr_location_name = None
                elif isinstance(sem, str) and sem.strip().casefold() in {
                    "home",
                    "zuhause",
                }:
                    self._attr_location_name = None
                else:
                    self._attr_location_name = sem

        # --- extra_state_attributes ---
        row = self._current_row()
        attributes: dict[str, Any] = _as_ha_attributes(row) or {}

        attributes["google_device_id"] = self.device_id

        location_age = self._get_location_age()
        if location_age is not None:
            # Round location_age to the nearest 5 minutes (300s) to suppress database writes
            # from redundant sub-second/minute updates when the device is stationary.
            attributes["location_age"] = int(round(location_age / 300.0) * 300)
        attributes["location_status"] = self._get_location_status()

        if stale:
            stale_data = self._current_row() or self._last_good_accuracy_data
            if stale_data:
                last_lat = stale_data.get("latitude")
                last_lon = stale_data.get("longitude")
                if last_lat is not None:
                    attributes["last_latitude"] = last_lat
                if last_lon is not None:
                    attributes["last_longitude"] = last_lon

        self._attr_extra_state_attributes = attributes

    @callback
    def _handle_coordinator_update(self) -> None:
        """React to coordinator updates.

        - Keep the device's human-readable name in sync with the coordinator snapshot.
        - Rely on the coordinator's filtered snapshot for accuracy gating while
          preserving the last known coordinates when new fixes omit location data.
        - Recompute _attr_* values so HA's CachedProperties caches are invalidated.
        """
        if not self.coordinator_has_device():
            self._last_good_accuracy_data = None
            self._sync_location_attrs()
            self.async_write_ha_state()
            return

        self.refresh_device_label_from_coordinator(log_prefix="DeviceTracker")
        # With has_entity_name=True, the entity name is derived from the device
        # registry name. No need to manually update _attr_name here.

        device_data = self._current_row()
        if not device_data:
            self._sync_location_attrs()
            self.async_write_ha_state()
            return

        lat = device_data.get("latitude")
        lon = device_data.get("longitude")

        if lat is not None and lon is not None:
            self._last_good_accuracy_data = device_data.copy()
        elif self._last_good_accuracy_data is None:
            # Preserve semantic-only updates when no prior location is available.
            self._last_good_accuracy_data = device_data.copy()

        self._sync_location_attrs()
        self.async_write_ha_state()


class GoogleFindMyLastLocationTracker(GoogleFindMyDeviceTracker):
    """Tracker that always shows last known location, never goes stale.

    This entity is useful for:
    - Automations that need the last known position even when the device is offline
    - Map visualizations that should always show the device's last position
    - Users who want to track "where was it last seen" instead of "is it here now"
    """

    _attr_translation_key = "last_location"
    _attr_entity_registry_enabled_default = True

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
        *,
        subentry_key: str,
        subentry_identifier: str,
    ) -> None:
        """Initialize the last location tracker entity."""
        super().__init__(
            coordinator,
            device,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )

        # Override unique_id with :last_location suffix
        entry_id = self.entry_id
        dev_id = self.device_id
        self._attr_unique_id = self.build_unique_id(
            entry_id,
            subentry_identifier,
            f"{dev_id}:last_location",
        )

        # CRITICAL: Remove the _attr_name = None that was set by the parent class.
        # With has_entity_name=True:
        #   - _attr_name = None → entity inherits ONLY device name (no suffix)
        #   - _attr_name not set → name comes from translation_key ("last_location")
        # We need the latter behavior so HA composes "<device_name> <translation_suffix>"
        # e.g. "Galaxy S25 Ultra Letzter Standort"
        del self._attr_name

    def _is_location_stale(self) -> bool:
        """Never stale - always show last known location."""
        return False

    def _get_location_status(self) -> str:
        """Return location status - always shows 'last_known' when data is aged."""
        age = self._get_location_age()
        if age is None:
            return "unknown"
        threshold = self._get_stale_threshold()
        if age > threshold:
            return "last_known"
        elif age > threshold / 2:
            return "aging"
        else:
            return "current"
