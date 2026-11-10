# custom_components/googlefindmy/button.py
"""Button entities for Google Find My tracker subentries.

This module exposes per-device buttons that trigger actions on Google Find My
devices via the integration coordinator:

- **Play Sound**: request the device to play a sound.
- **Stop Sound**: request the device to stop a playing sound.
- **Locate now**: trigger an immediate manual locate through the integration's
  service call.

Quality & design notes (HA Platinum guidelines)
-----------------------------------------------
* Async-first: no blocking calls on the event loop.
* Availability mirrors device presence and capability gates from the coordinator.
* Device registry naming respects user overrides; we never write placeholders.
* Entities default to **enabled** (see `_attr_entity_registry_enabled_default = True`).
* Tracker entities rely on per-device identifiers managed by the coordinator;
  do not link them to the service device via manual `via_device` tuples.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, NamedTuple

from homeassistant.components.button import ButtonEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import (
    ServiceValidationError,
    Unauthorized,
    UnknownUser,
)
from homeassistant.helpers import entity_platform
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.restore_state import RestoreEntity as HARestoreEntity
from homeassistant.util import dt as dt_util

if TYPE_CHECKING:
    from homeassistant.core import Context

    from .ha_typing import RestoreEntity as RestoreEntityType
else:
    RestoreEntityType = HARestoreEntity

from . import EntityRecoveryManager
from .const import (
    DOMAIN,
    SERVICE_LOCATE_DEVICE,
    SERVICE_PLAY_SOUND,
    SERVICE_STOP_SOUND,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TOKEN_REFRESH_COOLDOWN_S,
    TRACKER_SUBENTRY_KEY,
)
from .coordinator import GoogleFindMyCoordinator
from .entity import (
    GoogleFindMyDeviceEntity,
    GoogleFindMyEntity,
    ensure_config_subentry_id,
    ensure_dispatcher_dependencies,
    resolve_coordinator,
    schedule_add_entities,
)
from .ha_typing import ButtonEntity, callback
from .util_services import register_entity_service

_LOGGER = logging.getLogger(__name__)

# Buttons trigger coordinator actions (refresh, FCM restart); serialize them.
PARALLEL_UPDATES = 1


class _TrackerScope(NamedTuple):
    """Resolved tracker subentry scope for entity creation."""

    subentry_key: str
    config_subentry_id: str | None
    identifier: str


class _ServiceScope(NamedTuple):
    """Resolved service subentry scope for entity creation."""

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


def _derive_device_label(device: dict[str, Any]) -> str | None:
    """Return a stable device label without mutating the coordinator snapshot."""

    name = device.get("name")
    if isinstance(name, str):
        stripped = name.strip()
        if stripped:
            return stripped

    fallback = device.get("device_id") or device.get("id")
    if isinstance(fallback, str):
        stripped = fallback.strip()
        if stripped:
            return stripped

    return None


# Reusable entity description with translations in en.json
PLAY_SOUND_DESCRIPTION = ButtonEntityDescription(
    key="play_sound",
    translation_key="play_sound",
    icon="mdi:volume-high",
    entity_category=EntityCategory.DIAGNOSTIC,
)

# Entity description for stopping a sound manually
STOP_SOUND_DESCRIPTION = ButtonEntityDescription(
    key="stop_sound",
    translation_key="stop_sound",
    icon="mdi:volume-off",
    entity_category=EntityCategory.DIAGNOSTIC,
)

# Entity description for the manual "Locate now" action
LOCATE_DEVICE_DESCRIPTION = ButtonEntityDescription(
    key="locate_device",
    translation_key="locate_device",
    icon="mdi:radar",
    entity_category=EntityCategory.DIAGNOSTIC,
)

RESET_STATISTICS_DESCRIPTION = ButtonEntityDescription(
    key="reset_statistics",
    translation_key="reset_statistics",
    icon="mdi:restart",
    entity_category=EntityCategory.CONFIG,
)

# Entity description for regenerating FCM token
REGENERATE_FCM_TOKEN_DESCRIPTION = ButtonEntityDescription(
    key="regenerate_fcm_token",
    translation_key="regenerate_fcm_token",
    icon="mdi:cellphone-wireless",
    entity_category=EntityCategory.CONFIG,
)

# Entity description for regenerating ADM token
REGENERATE_ADM_TOKEN_DESCRIPTION = ButtonEntityDescription(
    key="regenerate_adm_token",
    translation_key="regenerate_adm_token",
    icon="mdi:key-change",
    entity_category=EntityCategory.CONFIG,
)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
    *,
    config_subentry_id: str | None = None,
) -> None:
    """Set up Google Find My Device button entities."""
    _LOGGER.debug(
        "Button setup invoked for entry=%s config_subentry_id=%s",
        getattr(config_entry, "entry_id", None),
        config_subentry_id,
    )
    coordinator = resolve_coordinator(config_entry)
    ensure_dispatcher_dependencies(hass)
    if getattr(coordinator, "config_entry", None) is None:
        coordinator.config_entry = config_entry

    def _known_ids_for_type(expected_type: str) -> set[str]:
        ids: set[str] = set()
        subentries = getattr(config_entry, "subentries", None)
        if isinstance(subentries, Mapping):
            for subentry in subentries.values():
                if _subentry_type(subentry) == expected_type:
                    candidate = getattr(subentry, "subentry_id", None) or getattr(
                        subentry, "entry_id", None
                    )
                    if isinstance(candidate, str) and candidate:
                        ids.add(candidate)

        runtime_data = getattr(config_entry, "runtime_data", None)
        subentry_manager = getattr(runtime_data, "subentry_manager", None)
        managed_subentries = getattr(subentry_manager, "managed_subentries", None)
        if isinstance(managed_subentries, Mapping):
            for subentry in managed_subentries.values():
                if _subentry_type(subentry) == expected_type:
                    candidate = getattr(subentry, "subentry_id", None) or getattr(
                        subentry, "entry_id", None
                    )
                    if isinstance(candidate, str) and candidate:
                        ids.add(candidate)

        return ids

    platform_getter = getattr(entity_platform, "async_get_current_platform", None)
    if callable(platform_getter):
        platform = platform_getter()
        if platform is not None:
            register_entity_service(
                platform,
                "trigger_device_refresh",
                None,
                "async_trigger_coordinator_refresh",
            )

    def _collect_tracker_scopes(
        hint_subentry_id: str | None = None,
        forwarded_config_id: str | None = None,
    ) -> list[_TrackerScope]:
        scopes: dict[str, _TrackerScope] = {}

        subentry_metas = getattr(coordinator, "_subentry_metadata", None)
        if isinstance(subentry_metas, Mapping):
            for key, meta in subentry_metas.items():
                meta_features = getattr(meta, "features", ())
                if "button" not in meta_features:
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

                if "button" not in subentry_features:
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
                TRACKER_SUBENTRY_KEY,
                forwarded_config_id,
                fallback_identifier,
            )
        ]

    def _collect_service_scopes(
        hint_subentry_id: str | None = None,
        forwarded_config_id: str | None = None,
    ) -> list[_ServiceScope]:
        scopes: dict[str, _ServiceScope] = {}

        subentry_metas = getattr(coordinator, "_subentry_metadata", None)
        if isinstance(subentry_metas, Mapping):
            for key, meta in subentry_metas.items():
                if key not in (SERVICE_SUBENTRY_KEY, "service"):
                    continue
                meta_features = getattr(meta, "features", ())
                if "button" not in meta_features:
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

        subentries = getattr(config_entry, "subentries", None)
        if isinstance(subentries, Mapping):
            for subentry in subentries.values():
                data = getattr(subentry, "data", {})
                group_key = SERVICE_SUBENTRY_KEY
                subentry_features: Iterable[Any] = ()
                if isinstance(data, Mapping):
                    group_key = data.get("group_key", group_key)
                    subentry_features = data.get("features", ())

                if group_key not in (SERVICE_SUBENTRY_KEY, "service"):
                    continue
                if "button" not in subentry_features:
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

        return list(scopes.values())

    added_unique_ids: set[str] = set()
    seen_service_subentries: set[str | None] = set()
    primary_scope: _TrackerScope | None = None
    primary_scheduler: Callable[[Iterable[ButtonEntity], bool], None] | None = None

    def _add_service_scope(
        scope: _ServiceScope, forwarded_config_id: str | None
    ) -> None:
        service_ids = _known_ids_for_type(SUBENTRY_TYPE_SERVICE)
        sanitized_config_id = ensure_config_subentry_id(
            config_entry,
            "button_service",
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
                    "Button setup (service): skipping subentry '%s' because the config_subentry_id is unknown",
                    forwarded_config_id or scope.config_subentry_id or scope.identifier,
                )
                return

        identifier = scope.identifier or sanitized_config_id or scope.subentry_key

        def _schedule_service_entities(
            new_entities: Iterable[ButtonEntity],
            update_before_add: bool = True,
        ) -> None:
            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=new_entities,
                update_before_add=update_before_add,
                config_subentry_id=sanitized_config_id,
                log_owner="Button setup (service)",
                logger=_LOGGER,
            )

        entities: list[ButtonEntity] = []

        # Stats reset button
        entity = GoogleFindMyStatsResetButton(
            coordinator,
            subentry_key=scope.subentry_key,
            subentry_identifier=identifier,
        )
        unique_id = getattr(entity, "unique_id", None)
        if isinstance(unique_id, str):
            if unique_id in added_unique_ids:
                _LOGGER.debug(
                    "Button setup (service): skipping duplicate unique_id %s", unique_id
                )
            else:
                added_unique_ids.add(unique_id)
                entities.append(entity)
        else:
            entities.append(entity)

        # Token regeneration buttons (disabled by default)
        for button_cls in (
            GoogleFindMyRegenerateFcmTokenButton,
            GoogleFindMyRegenerateAdmTokenButton,
        ):
            token_button = button_cls(
                coordinator,
                subentry_key=scope.subentry_key,
                subentry_identifier=identifier,
            )
            btn_unique_id = getattr(token_button, "unique_id", None)
            if isinstance(btn_unique_id, str):
                if btn_unique_id in added_unique_ids:
                    _LOGGER.debug(
                        "Button setup (service): skipping duplicate unique_id %s",
                        btn_unique_id,
                    )
                else:
                    added_unique_ids.add(btn_unique_id)
                    entities.append(token_button)
            else:
                entities.append(token_button)

        if entities:
            _LOGGER.debug(
                "Button setup (service): subentry_key=%s, config_subentry_id=%s (count=%d)",
                scope.subentry_key,
                sanitized_config_id,
                len(entities),
            )
            _schedule_service_entities(entities, True)
        else:
            _schedule_service_entities([], True)

    def _add_scope(scope: _TrackerScope, forwarded_config_id: str | None) -> None:
        nonlocal primary_scope, primary_scheduler

        tracker_ids = _known_ids_for_type(SUBENTRY_TYPE_TRACKER)
        sanitized_config_id = ensure_config_subentry_id(
            config_entry,
            "button",
            scope.config_subentry_id or forwarded_config_id,
            known_ids=tracker_ids,
        )
        if sanitized_config_id is None:
            if tracker_ids:
                _LOGGER.debug(
                    "Button setup: skipping subentry '%s' because the config_subentry_id is unknown",
                    forwarded_config_id
                    or scope.config_subentry_id
                    or scope.subentry_key,
                )
                return
            sanitized_config_id = (
                scope.identifier or forwarded_config_id or scope.subentry_key
            )
            _LOGGER.debug(
                "Button setup: synthesized config_subentry_id '%s' for key '%s'",
                sanitized_config_id,
                scope.subentry_key,
            )

        tracker_identifier = (
            scope.identifier or sanitized_config_id or scope.subentry_key
        )

        def _schedule_button_entities(
            new_entities: Iterable[ButtonEntity],
            update_before_add: bool = True,
        ) -> None:
            schedule_add_entities(
                coordinator.hass,
                async_add_entities,
                entities=new_entities,
                update_before_add=update_before_add,
                config_subentry_id=sanitized_config_id,
                log_owner="Button setup",
                logger=_LOGGER,
            )

        if primary_scope is None:
            primary_scope = scope
        if primary_scheduler is None:
            primary_scheduler = _schedule_button_entities

        known_device_ids: set[str] = set()

        def _build_entities(devices: Iterable[dict[str, Any]]) -> list[ButtonEntity]:
            entities: list[ButtonEntity] = []
            for device in devices:
                dev_id = device.get("id") if isinstance(device, Mapping) else None
                if not dev_id or dev_id in known_device_ids:
                    continue

                visible = True
                is_visible = getattr(coordinator, "is_device_visible_in_subentry", None)
                if callable(is_visible):
                    try:
                        visible = bool(is_visible(scope.subentry_key, dev_id))
                    except Exception:  # pragma: no cover - defensive fallback for stubs
                        visible = True

                if not visible:
                    _LOGGER.debug(
                        "Button setup: skipping hidden device id %s for subentry %s",
                        dev_id,
                        scope.subentry_key,
                    )
                    continue

                label = _derive_device_label(device)
                for entity_cls in (
                    GoogleFindMyPlaySoundButton,
                    GoogleFindMyStopSoundButton,
                    GoogleFindMyLocateButton,
                ):
                    entity = entity_cls(
                        coordinator,
                        device,
                        label,
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

        initial_entities = _build_entities(
            coordinator.get_subentry_snapshot(scope.subentry_key)
        )
        if initial_entities:
            _LOGGER.debug(
                "Button setup: subentry_key=%s, config_subentry_id=%s (initial=%d)",
                scope.subentry_key,
                sanitized_config_id,
                len(initial_entities),
            )
            _schedule_button_entities(initial_entities, True)
        else:
            _LOGGER.debug(
                "Button setup: no devices available for subentry %s (config_subentry_id=%s)",
                scope.subentry_key,
                sanitized_config_id,
            )
            _schedule_button_entities([], True)

        @callback
        def _add_new_devices() -> None:
            new_entities = _build_entities(
                coordinator.get_subentry_snapshot(scope.subentry_key)
            )
            if new_entities:
                _LOGGER.debug(
                    "Button setup: dynamically adding %d entity(ies) for subentry %s",
                    len(new_entities),
                    scope.subentry_key,
                )
                _schedule_button_entities(new_entities, True)

        unsub = coordinator.async_add_listener(_add_new_devices)
        config_entry.async_on_unload(unsub)

    seen_subentries: set[str | None] = set()

    @callback
    def async_add_service_subentry(subentry: Any | None = None) -> None:
        subentry_identifier = None
        if isinstance(subentry, str):
            subentry_identifier = subentry
        else:
            subentry_identifier = getattr(subentry, "subentry_id", None) or getattr(
                subentry, "entry_id", None
            )

        subentry_type = _subentry_type(subentry)
        _LOGGER.debug(
            "Button setup (service): processing subentry '%s' (type=%s)",
            subentry_identifier,
            subentry_type,
        )
        if subentry_type is not None and subentry_type != "service":
            _LOGGER.debug(
                "Button setup (service) skipped for unrelated subentry '%s' (type '%s')",
                subentry_identifier,
                subentry_type,
            )
            return

        scopes = _collect_service_scopes(
            subentry_identifier, forwarded_config_id=subentry_identifier
        )
        if not scopes:
            return

        if subentry_identifier in seen_service_subentries:
            return
        seen_service_subentries.add(subentry_identifier)

        for scope in scopes:
            _add_service_scope(scope, subentry_identifier)

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
            "Button setup: processing subentry '%s' (type=%s)",
            subentry_identifier,
            subentry_type,
        )
        if subentry_type is not None and subentry_type != "tracker":
            _LOGGER.debug(
                "Button setup skipped for unrelated subentry '%s' (type '%s')",
                subentry_identifier,
                subentry_type,
            )
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
            "Button setup: deferring subentry processing until dispatcher signal for entry %s",
            getattr(config_entry, "entry_id", None),
        )
        schedule_add_entities(
            coordinator.hass,
            async_add_entities,
            entities=[],
            update_before_add=False,
            log_owner="Button setup",
            logger=_LOGGER,
        )
    elif known_subentries:
        for managed_subentry in known_subentries:
            async_add_service_subentry(managed_subentry)
            async_add_subentry(managed_subentry)
    elif isinstance(getattr(config_entry, "subentries", None), Mapping):
        async_add_service_subentry(config_subentry_id)
        async_add_subentry(config_subentry_id)
    else:
        async_add_service_subentry(config_subentry_id)
        async_add_subentry(config_subentry_id)

    config_entry.async_on_unload(
        async_dispatcher_connect(
            hass,
            f"{DOMAIN}_subentry_setup_{config_entry.entry_id}",
            async_add_subentry,
        )
    )
    config_entry.async_on_unload(
        async_dispatcher_connect(
            hass,
            f"{DOMAIN}_subentry_setup_{config_entry.entry_id}",
            async_add_service_subentry,
        )
    )

    recovery_manager = getattr(runtime_data, "entity_recovery_manager", None)

    if isinstance(recovery_manager, EntityRecoveryManager):
        entry_id = getattr(config_entry, "entry_id", None)
        tracker_identifier = (
            primary_scope.identifier if primary_scope is not None else None
        )
        tracker_subentry_key = (
            primary_scope.subentry_key
            if primary_scope is not None
            else TRACKER_SUBENTRY_KEY
        )

        def _recovery_add_entities(
            new_entities: Iterable[ButtonEntity],
            update_before_add: bool = True,
        ) -> None:
            if primary_scheduler is None:
                return
            primary_scheduler(new_entities, update_before_add)

        def _is_visible_device(device_id: str) -> bool:
            is_visible = getattr(coordinator, "is_device_visible_in_subentry", None)
            if callable(is_visible):
                try:
                    return bool(is_visible(tracker_subentry_key, device_id))
                except Exception:  # pragma: no cover - defensive fallback for stubs
                    return True
            return True

        def _expected_unique_ids() -> set[str]:
            if not isinstance(entry_id, str) or not entry_id:
                return set()
            if not isinstance(tracker_identifier, str) or not tracker_identifier:
                return set()
            return {
                f"{DOMAIN}_{entry_id}_{tracker_identifier}_{device.get('id')}_{action}"
                for device in coordinator.get_subentry_snapshot(tracker_subentry_key)
                for action in ("play_sound", "stop_sound", "locate_device")
                if isinstance(device, Mapping)
                and isinstance(device.get("id"), str)
                and device.get("id")
                and _is_visible_device(device["id"])
            }

        def _build_entities(missing: set[str]) -> list[ButtonEntity]:
            if not missing:
                return []
            built: list[ButtonEntity] = []
            if not isinstance(entry_id, str) or not entry_id:
                return built
            if not isinstance(tracker_identifier, str) or not tracker_identifier:
                return built
            snapshot = coordinator.get_subentry_snapshot(tracker_subentry_key)
            for device in snapshot:
                dev_id = device.get("id") if isinstance(device, Mapping) else None
                if not isinstance(dev_id, str) or not dev_id:
                    continue
                if not _is_visible_device(dev_id):
                    continue
                label = _derive_device_label(device)
                mapping: dict[str, type[GoogleFindMyButtonEntity]] = {
                    "play_sound": GoogleFindMyPlaySoundButton,
                    "stop_sound": GoogleFindMyStopSoundButton,
                    "locate_device": GoogleFindMyLocateButton,
                }
                for action, entity_cls in mapping.items():
                    unique_id = (
                        f"{DOMAIN}_{entry_id}_{tracker_identifier}_{dev_id}_{action}"
                    )
                    if unique_id not in missing:
                        continue
                    built.append(
                        entity_cls(
                            coordinator,
                            device,
                            label,
                            subentry_key=tracker_subentry_key,
                            subentry_identifier=tracker_identifier,
                        )
                    )
            return built

        recovery_manager.register_button_platform(
            expected_unique_ids=_expected_unique_ids,
            entity_factory=_build_entities,
            add_entities=_recovery_add_entities,
        )


async def _async_require_admin(hass: HomeAssistant, context: Context | None) -> None:
    """Reject a maintenance button press by a user who is not an administrator.

    Home Assistant offers no administrator check for entities, so this mirrors
    the check of its admin action wrapper
    (``homeassistant.helpers.service.async_register_admin_service``), which
    already guards the maintenance actions of this integration: a press without
    a user (automation, script) passes, an unknown user raises ``UnknownUser``,
    and a user who is not an administrator raises ``Unauthorized``.
    """

    user_id = context.user_id if context is not None else None
    if not user_id:
        return
    user = await hass.auth.async_get_user(user_id)
    if user is None:
        raise UnknownUser(context=context)
    if not user.is_admin:
        _LOGGER.warning(
            "Rejected maintenance button press: caller is not an administrator"
        )
        raise Unauthorized(context=context)


class _CoordinatorRefreshServiceMixin:
    """Target of the ``trigger_device_refresh`` entity service.

    The service's target filter can only narrow by integration and domain, so
    Home Assistant offers every button of this integration; each button class
    therefore carries the method the service calls.
    """

    coordinator: GoogleFindMyCoordinator

    async def async_trigger_coordinator_refresh(self) -> None:
        """Request an immediate refresh of the coordinator of this entry."""

        await self.coordinator.async_request_refresh()


class GoogleFindMyStatsResetButton(
    _CoordinatorRefreshServiceMixin, GoogleFindMyEntity, ButtonEntity, RestoreEntityType
):
    """Button to reset integration statistics counters."""

    entity_description = RESET_STATISTICS_DESCRIPTION
    _attr_has_entity_name = True
    _attr_icon = RESET_STATISTICS_DESCRIPTION.icon
    _attr_translation_key = RESET_STATISTICS_DESCRIPTION.translation_key

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
            "reset_statistics",
            separator="_",
        )

    async def async_added_to_hass(self) -> None:
        """Restore last press timestamp to avoid 'Unknown' state."""

        await super().async_added_to_hass()
        if (state := await self.async_get_last_state()) is None:
            return

        restored_value = (
            state.attributes.get("last_press") if hasattr(state, "attributes") else None
        )
        if restored_value is None:
            restored_value = state.state

        parse_datetime = getattr(dt_util, "parse_datetime", None)
        restored = parse_datetime(restored_value) if callable(parse_datetime) else None
        if restored is None:
            try:
                restored = datetime.fromisoformat(restored_value)
            except (TypeError, ValueError):
                return

        self._attr_last_pressed = restored
        self.async_write_ha_state()

    def _update_last_pressed(self) -> None:
        """Record the current timestamp and push state to Home Assistant."""

        self._attr_last_pressed = dt_util.utcnow()
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Attach the reset control to the per-entry service device."""

        return self.service_device_info(include_subentry_identifier=True)

    async def async_press(self) -> None:
        """Reset coordinator statistics and refresh listeners."""

        # Maintenance is for administrators, as for the maintenance actions.
        # Home Assistant records the press timestamp before calling this
        # method, so the button state changes even when the press is rejected.
        await _async_require_admin(self.hass, self._context)

        hass = self.coordinator.hass
        # Counters, tally claims and the generation marker in one call: they are
        # halves of one fact, and the marker is what keeps a stats load that is
        # still in flight from adding the discarded generation back. Source order
        # against the persist below does NOT matter - that schedule only creates a
        # task which sleeps first, and the writer reads every half live.
        begin_epoch = getattr(self.coordinator, "begin_new_stats_epoch", None)
        if callable(begin_epoch):
            try:
                if not begin_epoch():
                    _LOGGER.debug("Stats reset: coordinator stats missing or invalid")
            except Exception as err:  # pragma: no cover - defensive logging
                # WARNING, unlike the DEBUG of the neighbours below: their failure
                # is benign, this one produces exactly the state the reset forbids.
                _LOGGER.warning("Stats reset: failed to start a new epoch: %s", err)
        else:
            _LOGGER.debug("Stats reset skipped: coordinator cannot start an epoch")

        diag_buffer = getattr(self.coordinator, "_diag", None)
        if diag_buffer is not None:
            warnings_bucket = getattr(diag_buffer, "warnings", None)
            errors_bucket = getattr(diag_buffer, "errors", None)
            if isinstance(warnings_bucket, dict):
                warnings_bucket.clear()
            if isinstance(errors_bucket, dict):
                errors_bucket.clear()

        domain_data = hass.data.get(DOMAIN)
        if isinstance(domain_data, dict):
            if "fcm_lock_contention_count" in domain_data:
                domain_data["fcm_lock_contention_count"] = 0
            if "services_lock_contention_count" in domain_data:
                domain_data["services_lock_contention_count"] = 0

        registry = ir.async_get(hass)
        issues_attr = getattr(registry, "issues", None)
        if isinstance(issues_attr, Mapping):
            issues_iterable = list(issues_attr.items())
        else:
            private_issues = getattr(registry, "_issues", None)
            issues_iterable = (
                list(private_issues.items())
                if isinstance(private_issues, Mapping)
                else []
            )

        self._update_last_pressed()

        expected_issue_key_size = 2

        for key, payload in issues_iterable:
            domain = DOMAIN
            issue_id: str | None = None
            if isinstance(key, tuple) and len(key) == expected_issue_key_size:
                domain = str(key[0])
                issue_id = str(key[1])
            else:
                issue_id = str(key)
                if isinstance(payload, Mapping):
                    domain = str(payload.get("domain", domain))

            if domain != DOMAIN or issue_id is None:
                continue

            try:
                ir.async_delete_issue(hass, DOMAIN, issue_id)
            except Exception as err:  # pragma: no cover - defensive cleanup
                _LOGGER.debug(
                    "Stats reset: failed to delete issue %s: %s", issue_id, err
                )

        schedule_persist = getattr(self.coordinator, "_schedule_stats_persist", None)
        if callable(schedule_persist):
            try:
                schedule_persist()
            except Exception as err:  # pragma: no cover - defensive logging
                _LOGGER.debug(
                    "Stats persistence scheduling failed after reset: %s", err
                )

        try:
            self.coordinator.async_update_listeners()
        except Exception as err:  # pragma: no cover - defensive logging
            _LOGGER.debug("Stats listener notification failed after reset: %s", err)

        entry_id = self.entry_id or getattr(
            getattr(self.coordinator, "config_entry", None), "entry_id", None
        )
        _LOGGER.info(
            "Statistics reset requested via button for entry %s",
            entry_id or "unknown",
        )


class GoogleFindMyButtonEntity(
    _CoordinatorRefreshServiceMixin,
    GoogleFindMyDeviceEntity,
    ButtonEntity,
    RestoreEntityType,
):
    """Common helpers for all per-device buttons."""

    _attr_entity_registry_enabled_default = True
    _attr_has_entity_name = True
    _attr_should_poll = False
    log_prefix = "Button"

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

    async def async_added_to_hass(self) -> None:
        """Restore last press timestamp to avoid 'Unknown' state."""

        await super().async_added_to_hass()
        if (state := await self.async_get_last_state()) is None:
            return

        restored_value = (
            state.attributes.get("last_press") if hasattr(state, "attributes") else None
        )
        if restored_value is None:
            restored_value = state.state

        parse_datetime = getattr(dt_util, "parse_datetime", None)
        restored = parse_datetime(restored_value) if callable(parse_datetime) else None
        if restored is None:
            try:
                restored = datetime.fromisoformat(restored_value)
            except (TypeError, ValueError):
                return

        self._attr_last_pressed = restored
        self.async_write_ha_state()

    def _update_last_pressed(self) -> None:
        """Record the current timestamp and push state to Home Assistant."""

        self._attr_last_pressed = dt_util.utcnow()
        self.async_write_ha_state()

    @callback
    def _handle_coordinator_update(self) -> None:
        """Refresh device metadata and propagate state updates."""

        self.refresh_device_label_from_coordinator(log_prefix=self.log_prefix)
        self.async_write_ha_state()


# ----------------------------- Play Sound -----------------------------------
class GoogleFindMyPlaySoundButton(GoogleFindMyButtonEntity):
    """Button to trigger 'Play Sound' on a Google Find My Device."""

    entity_description = PLAY_SOUND_DESCRIPTION
    log_prefix = "PlaySound"

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
            fallback_label,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        dev_id = self.device_id
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            self.entry_id,
            subentry_identifier,
            dev_id,
            "play_sound",
            separator="_",
        )

    @property
    def available(self) -> bool:
        """Return True only if the device is present AND can likely play a sound.

        Presence has priority: if the device is absent from the latest Google list,
        the button is unavailable regardless of capability/push readiness.
        """
        dev_id = self.device_id
        device_label = self.device_label()
        try:
            # Presence gate
            if not self.coordinator_has_device():
                return False
            if hasattr(
                self.coordinator, "is_device_present"
            ) and not self.coordinator.is_device_present(dev_id):
                return False
            # Capability / push readiness gate
            return bool(self.coordinator.can_play_sound(dev_id))
        except (AttributeError, TypeError) as err:
            _LOGGER.debug(
                "PlaySound availability check for %s (%s) raised %s; defaulting to True",
                device_label,
                dev_id,
                err,
            )
            return True  # Optimistic fallback

    async def async_press(self) -> None:
        """Handle the button press."""
        device_id = self.device_id
        device_name = self.device_label()

        if not self.available:
            _LOGGER.warning(
                "Play Sound not available for %s (%s) — push not ready, device not capable, or absent",
                device_name,
                device_id,
            )
            return

        _LOGGER.debug("Play Sound: attempting on %s (%s)", device_name, device_id)
        try:
            await self.hass.services.async_call(
                DOMAIN,
                SERVICE_PLAY_SOUND,
                {"device_id": device_id},
                blocking=True,
                context=self._context,
            )
            self._update_last_pressed()
            _LOGGER.info(
                "Successfully submitted Play Sound request for %s", device_name
            )
        except (ServiceValidationError, Unauthorized):
            # The service layer raises this to *tell the user something*, with a
            # translated message. Swallowing it into a log line is how a
            # deliberately written explanation never reaches the person pressing
            # the button. Let HA surface it; it is not an unexpected crash. A
            # permission rejection is an answer to the person pressing, not a
            # crash either.
            raise
        except Exception as err:  # Avoid crashing the update loop
            _LOGGER.error("Error playing sound on %s: %s", device_name, err)


# ----------------------------- Stop Sound -----------------------------------
class GoogleFindMyStopSoundButton(GoogleFindMyButtonEntity):
    """Button to trigger 'Stop Sound' on a Google Find My Device."""

    entity_description = STOP_SOUND_DESCRIPTION
    log_prefix = "StopSound"

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
            fallback_label,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        dev_id = self.device_id
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            self.entry_id,
            subentry_identifier,
            dev_id,
            "stop_sound",
            separator="_",
        )

    @property
    def available(self) -> bool:
        """Return True if the device is present; do not couple to Play gating.

        Rationale:
        - The Play button may be intentionally unavailable during in-flight/cooldown.
        - Stopping must remain possible in that phase.
        We therefore:
          1) require presence, and
          2) prefer a dedicated `can_stop_sound()` if provided by the coordinator,
             otherwise assume stopping is allowed when the device is present.
        """
        dev_id = self.device_id
        device_label = self.device_label()
        try:
            if not self.coordinator_has_device():
                return False
            if hasattr(
                self.coordinator, "is_device_present"
            ) and not self.coordinator.is_device_present(dev_id):
                return False
            can_stop = getattr(self.coordinator, "can_stop_sound", None)
            if callable(can_stop):
                return bool(can_stop(dev_id))
            # Do NOT fall back to can_play_sound(): Stop should stay available even if Play is gated.
            return True
        except (AttributeError, TypeError) as err:
            _LOGGER.debug(
                "StopSound availability check for %s (%s) raised %s; defaulting to True",
                device_label,
                dev_id,
                err,
            )
            return True

    async def async_press(self) -> None:
        """Handle the button press (stop sound)."""
        device_id = self.device_id
        device_name = self.device_label()

        if not self.available:
            _LOGGER.warning(
                "Stop Sound not available for %s (%s) — device absent or not eligible",
                device_name,
                device_id,
            )
            return

        _LOGGER.debug("Stop Sound: attempting on %s (%s)", device_name, device_id)
        try:
            await self.hass.services.async_call(
                DOMAIN,
                SERVICE_STOP_SOUND,
                {"device_id": device_id},
                blocking=True,
                context=self._context,
            )
            self._update_last_pressed()
            _LOGGER.info(
                "Successfully submitted Stop Sound request for %s", device_name
            )
        except (ServiceValidationError, Unauthorized):
            # This button is the primary path for BSkando#195: it is where a
            # user learns that the ring could not be correlated and may keep
            # playing. That message exists in ten languages; a log line reaches
            # none of them. Let HA show it. A permission rejection is an answer
            # to the person pressing, not a crash either.
            raise
        except Exception as err:
            _LOGGER.error("Error stopping sound on %s: %s", device_name, err)


# ----------------------------- Locate now -----------------------------------
class GoogleFindMyLocateButton(GoogleFindMyButtonEntity):
    """Button to trigger an immediate 'Locate now' request (manual location update)."""

    entity_description = LOCATE_DEVICE_DESCRIPTION
    log_prefix = "Locate"

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
            fallback_label,
            subentry_key=subentry_key,
            subentry_identifier=subentry_identifier,
        )
        dev_id = self.device_id
        self._attr_unique_id = self.build_unique_id(
            DOMAIN,
            self.entry_id,
            subentry_identifier,
            dev_id,
            "locate_device",
            separator="_",
        )

    @property
    def available(self) -> bool:
        """Return True only if the device is present AND a manual locate is currently allowed.

        Presence has priority: if absent from Google's list, the button is unavailable.
        """
        dev_id = self.device_id
        device_label = self.device_label()
        try:
            # Presence gate
            if not self.coordinator_has_device():
                return False
            if hasattr(
                self.coordinator, "is_device_present"
            ) and not self.coordinator.is_device_present(dev_id):
                return False
            # Locate gating
            return bool(self.coordinator.can_request_location(dev_id))
        except Exception as err:
            _LOGGER.debug(
                "Locate availability check for %s (%s) failed: %s; defaulting to False",
                device_label,
                dev_id,
                err,
            )
            return False

    async def async_press(self) -> None:
        """Invoke the `googlefindmy.locate_device` service for this device.

        The service path keeps UI and logic decoupled and ensures that all
        manual triggers (buttons, automations, scripts) share the same code path.
        """
        device_id = self.device_id
        device_name = self.device_label()

        if not self.available:
            _LOGGER.warning(
                "Locate now not available for %s (%s) — push not ready, in-flight/cooldown, or absent",
                device_name,
                device_id,
            )
            return

        _LOGGER.debug("Locate now: attempting on %s (%s)", device_name, device_id)
        try:
            # Fire-and-forget for responsive UI; coordinator handles gating & updates
            await self.hass.services.async_call(
                DOMAIN,
                SERVICE_LOCATE_DEVICE,
                {"device_id": device_id},
                blocking=False,  # non-blocking: avoid UI stall
                context=self._context,
            )
            self._update_last_pressed()
            # blocking=False by design (responsive UI), so the service result is
            # never observed here. This call site therefore cannot claim
            # success: a ServiceValidationError raised inside the handler is
            # reported by HA core, never by the except-branch below, and a
            # gated locate returns quietly. Say what is known -- dispatched --
            # and nothing more. An Unauthorized raised by the service is logged
            # by HA core as well and never arrives here.
            _LOGGER.info(
                "Manual locate dispatched for %s (fire-and-forget, outcome not awaited)",
                device_name,
            )
        except Exception as err:  # Avoid crashing the update loop
            _LOGGER.error("Error submitting manual locate for %s: %s", device_name, err)


# ----------------------------- Token Regeneration Buttons -----------------------------------


class GoogleFindMyTokenRefreshButtonBase(
    _CoordinatorRefreshServiceMixin, GoogleFindMyEntity, ButtonEntity, RestoreEntityType
):
    """Base class for token regeneration buttons with per-type cooldown logic.

    Token regeneration buttons are disabled by default. The refresh cooldown
    is scoped per token type (see ``_token_type``), so regenerating one token
    only rate-limits its own button and leaves the sibling button available.
    """

    _attr_has_entity_name = True
    _attr_entity_registry_enabled_default = False  # Disabled by default
    _token_type: str = "token"  # Override in subclasses

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

    async def async_added_to_hass(self) -> None:
        """Restore last press timestamp to avoid 'Unknown' state."""
        await super().async_added_to_hass()
        if (state := await self.async_get_last_state()) is None:
            return

        restored_value = (
            state.attributes.get("last_press") if hasattr(state, "attributes") else None
        )
        if restored_value is None:
            restored_value = state.state

        parse_datetime = getattr(dt_util, "parse_datetime", None)
        restored = parse_datetime(restored_value) if callable(parse_datetime) else None
        if restored is None:
            try:
                restored = datetime.fromisoformat(restored_value)
            except (TypeError, ValueError):
                return

        self._attr_last_pressed = restored
        self.async_write_ha_state()

    def _update_last_pressed(self) -> None:
        """Record the current timestamp and push state to Home Assistant."""
        self._attr_last_pressed = dt_util.utcnow()
        self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Attach the token refresh button to the per-entry service device."""
        return self.service_device_info(include_subentry_identifier=True)

    @property
    def available(self) -> bool:
        """Return True if the button is not on cooldown.

        The cooldown is scoped per token type, so a refresh of one token
        (e.g. FCM) does not disable the sibling button (e.g. ADM).
        """
        from .Auth.token_refresh import is_refresh_on_cooldown

        entry_id = self.entry_id or "default"
        on_cooldown, _ = is_refresh_on_cooldown(entry_id, self._token_type)
        return not on_cooldown

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return additional state attributes including cooldown info."""
        from .Auth.token_refresh import get_cooldown_remaining

        entry_id = self.entry_id or "default"
        remaining = get_cooldown_remaining(entry_id, self._token_type)

        attrs: dict[str, Any] = {
            "cooldown_seconds": TOKEN_REFRESH_COOLDOWN_S,
        }
        if remaining > 0:
            attrs["cooldown_remaining"] = round(remaining, 1)
        return attrs


class GoogleFindMyRegenerateFcmTokenButton(GoogleFindMyTokenRefreshButtonBase):
    """Button to regenerate FCM/GCM tokens.

    Preserves the GCM device identity (android_id/security_token) and
    obtains fresh GCM + FCM registration tokens via re-registration.
    """

    entity_description = REGENERATE_FCM_TOKEN_DESCRIPTION
    _attr_icon = REGENERATE_FCM_TOKEN_DESCRIPTION.icon
    _attr_translation_key = REGENERATE_FCM_TOKEN_DESCRIPTION.translation_key
    _token_type = "FCM"

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
            "regenerate_fcm_token",
            separator="_",
        )

    async def async_press(self) -> None:
        """Handle the button press to regenerate FCM token."""
        # Maintenance is for administrators, as for the maintenance actions.
        # Home Assistant records the press timestamp before calling this
        # method, so the button state changes even when the press is rejected.
        await _async_require_admin(self.hass, self._context)
        from .Auth.token_refresh import async_regenerate_fcm_token

        # Fall back to "default", matching ``available``/``extra_state_attributes``
        # and ``__init__`` above. The value becomes the cooldown key
        # (``async_regenerate_fcm_token`` records under it); a divergent fallback
        # here (e.g. "unknown") would record the cooldown under a different key
        # than ``available`` reads, so an ``entry_id is None`` button could never
        # see its own cooldown. Keep every fallback identical.
        entry_id = self.entry_id or "default"

        if not self.available:
            _LOGGER.info(
                "FCM token regeneration button pressed but cooldown active (entry: %s)",
                entry_id,
            )
            return

        _LOGGER.info(
            "FCM token regeneration button pressed (entry: %s)",
            entry_id,
        )

        hass = self.hass
        if hass is None:
            _LOGGER.error(
                "FCM token regeneration failed: no hass instance available (entry: %s)",
                entry_id,
            )
            return

        success = await async_regenerate_fcm_token(hass=hass, entry_id=entry_id)

        if success:
            self._update_last_pressed()
            _LOGGER.info(
                "FCM token regeneration completed successfully (entry: %s)",
                entry_id,
            )
        else:
            _LOGGER.warning(
                "FCM token regeneration failed (entry: %s)",
                entry_id,
            )


class GoogleFindMyRegenerateAdmTokenButton(GoogleFindMyTokenRefreshButtonBase):
    """Button to regenerate the ADM (Android Device Manager) token.

    Regenerating the ADM token will use the existing AAS token if valid,
    otherwise it will trigger AAS regeneration as well.
    """

    entity_description = REGENERATE_ADM_TOKEN_DESCRIPTION
    _attr_icon = REGENERATE_ADM_TOKEN_DESCRIPTION.icon
    _attr_translation_key = REGENERATE_ADM_TOKEN_DESCRIPTION.translation_key
    _token_type = "ADM"

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
            "regenerate_adm_token",
            separator="_",
        )

    async def async_press(self) -> None:
        """Handle the button press to regenerate ADM token."""
        # Maintenance is for administrators, as for the maintenance actions.
        # Home Assistant records the press timestamp before calling this
        # method, so the button state changes even when the press is rejected.
        await _async_require_admin(self.hass, self._context)
        from .Auth.token_refresh import async_regenerate_adm_token

        # Fall back to "default", matching ``available`` above. This value is the
        # cooldown key; a divergent fallback (e.g. "unknown") would record the
        # cooldown under a different key than ``available`` reads, so an
        # ``entry_id is None`` button could never see its own cooldown.
        entry_id = self.entry_id or "default"

        if not self.available:
            _LOGGER.info(
                "ADM token regeneration button pressed but cooldown active (entry: %s)",
                entry_id,
            )
            return

        _LOGGER.info(
            "ADM token regeneration button pressed (entry: %s)",
            entry_id,
        )

        # Get the token cache from runtime data
        config_entry = getattr(self.coordinator, "config_entry", None)
        runtime_data = getattr(config_entry, "runtime_data", None)
        cache = getattr(runtime_data, "token_cache", None)
        if cache is None:
            cache = getattr(self.coordinator, "cache", None)

        if cache is None:
            _LOGGER.error(
                "ADM token regeneration failed: no token cache available (entry: %s)",
                entry_id,
            )
            return

        success = await async_regenerate_adm_token(cache=cache)

        if success:
            self._update_last_pressed()
            _LOGGER.info(
                "ADM token regeneration completed successfully (entry: %s)",
                entry_id,
            )
        else:
            _LOGGER.warning(
                "ADM token regeneration failed (entry: %s)",
                entry_id,
            )
