# custom_components/googlefindmy/services.py

"""Service handlers & registration for Google Find My Device (Home Assistant).

Design goals
------------
- services.yaml is the single source of truth for service metadata (names, descriptions, selectors).
- This module only registers handlers and implements business logic.
- No circular imports: required helpers (e.g. canonical resolver, option reader) are passed via a context.
- PII-safe logs: do not log secrets or coordinates; redact tokens in URLs on info/debug paths.

Compatibility
-------------
- HA 2025.5+; depends on services.yaml for UI/schema validation.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Awaitable, Callable, Iterable, Mapping
from typing import Any

import voluptuous as vol
from homeassistant.auth.permissions.const import POLICY_CONTROL
from homeassistant.components.device_tracker import DOMAIN as DEVICE_TRACKER_DOMAIN
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse
from homeassistant.exceptions import (
    HomeAssistantError,
    ServiceValidationError,
    Unauthorized,
    UnknownUser,
)
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.network import NoURLAvailableError, get_url
from homeassistant.helpers.service import async_register_admin_service

from .Auth.log_safety import describe_exception
from .beacon_actions import BeaconKeys, async_probe_tracker
from .const import (
    DATA_EID_RESOLVER,
    DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
    DOMAIN,
    LEGACY_SERVICE_IDENTIFIER,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,  # ctx provides the key but we keep a local fallback constant
    SERVICE_DEVICE_IDENTIFIER_PREFIX,
    SERVICE_LOCATE_DEVICE,
    SERVICE_LOCATE_EXTERNAL,
    SERVICE_PLAY_SOUND,
    SERVICE_REBUILD_REGISTRY,
    SERVICE_REFRESH_DEVICE_URLS,
    SERVICE_STOP_SOUND,
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
    PlaySoundOutcome,
    StopSoundOutcome,
    StopSoundUncorrelatedReason,
    map_token_hex_digest,
    map_token_secret_seed,
    service_device_identifier,
)
from .coordinator.helpers.registry import (
    OwnershipIntent,
    detect_device_registry_capabilities,
    device_owning_entry_ids,
    execute_ownership_plan,
    extract_subentry_links,
    plan_device_ownership,
    read_device_ownership,
    resolve_device_by_identifiers,
)
from .eid_resolver import _normalize_encrypted_blob
from .map_view import _is_map_view_enabled_for_entry

_LOGGER = logging.getLogger(__name__)

# K10: one message per reason a stop is UNCORRELATED. English fallback text and
# translation key; the translated texts live in strings.json / translations.
_STOP_UNCORRELATED_MESSAGES: dict[
    StopSoundUncorrelatedReason | None, tuple[str, str]
] = {
    StopSoundUncorrelatedReason.NO_KEY: (
        "Stop sound for '{device_id}' was submitted, but this instance holds no "
        "key for a ring on this device; it may keep playing.",
        "stop_sound_no_cancel_key",
    ),
    StopSoundUncorrelatedReason.AGED_KEY: (
        "Stop sound for '{device_id}' was submitted with a key older than 30 "
        "minutes; it may keep playing.",
        "stop_sound_cancel_key_aged",
    ),
    StopSoundUncorrelatedReason.FOREIGN_KEY: (
        "Stop sound for '{device_id}' was submitted with a request UUID that "
        "does not belong to a ring started by this instance; it may keep "
        "playing.",
        "stop_sound_cancel_key_foreign",
    ),
}

# Service field name of ``rebuild_registry`` (see ``services.yaml``); Home
# Assistant core has no ``ATTR_ENTRY_ID`` constant in ``homeassistant.const``.
ATTR_ENTRY_ID: str = "entry_id"

try:
    from homeassistant.exceptions import ConfigEntryError
except ImportError:  # pragma: no cover - ConfigEntryError introduced in newer cores
    ConfigEntryError = HomeAssistantError


def _service_validation_error(
    message: str,
    *,
    translation_key: str,
    translation_placeholders: Mapping[str, Any] | None = None,
    translation_domain: str = DOMAIN,
) -> ServiceValidationError:
    """Create a ServiceValidationError compatible across HA releases."""

    placeholders = (
        None if translation_placeholders is None else dict(translation_placeholders)
    )
    kwargs = {
        "translation_domain": translation_domain,
        "translation_key": translation_key,
        "translation_placeholders": placeholders,
    }
    return ServiceValidationError(message, **kwargs)


SERVICE_REBUILD_DEVICE_REGISTRY: str = "rebuild_device_registry"
SERVICE_READ_BEACON_STATE: str = "read_beacon_state"

_BLE_ADDRESS_PATTERN = re.compile(r"^(?:[0-9A-F]{2}:){5}[0-9A-F]{2}$")
_READ_BEACON_STATE_SCHEMA = vol.Schema(
    {
        vol.Required("device_id"): str,
        vol.Optional("address"): str,
        vol.Optional("timeout", default=45): vol.All(
            vol.Coerce(float), vol.Range(min=5, max=120)
        ),
    }
)


async def async_rebuild_device_registry(hass: HomeAssistant, call: ServiceCall) -> None:
    """Synchronize and clean Device Registry entries for Google Find My hubs.

    This service iterates over all googlefindmy config entries, identifies
    the correct service device and tracker subentry, and removes any
    orphaned devices (trackers) that are incorrectly linked to the parent
    config entry instead of the tracker subentry.
    """

    _LOGGER.info(
        "Service '%s' called: rebuilding and cleaning device registry.",
        SERVICE_REBUILD_DEVICE_REGISTRY,
    )

    dev_reg = dr.async_get(hass)
    domain_bucket = hass.data.get(DOMAIN, {})
    if not isinstance(domain_bucket, Mapping):
        _LOGGER.warning("Device registry cleanup skipped: Domain data not found.")
        return

    entries_bucket = domain_bucket.get("entries")
    if not isinstance(entries_bucket, Mapping):
        _LOGGER.warning("Device registry cleanup skipped: No entries bucket found.")
        return

    capability_cache: dict[int, Any] = {}

    def _registry_capabilities(update_call: Any) -> Any:
        """Return the capability profile for ``update_call``, probed once.

        The contract calls this choice one that is "made once, behind a
        signature probe" (``docs/AI_DEPRECATIONS_GUIDE.md``, section VI), and
        the coordinator caches it for the same reason. Without the cache this
        service runs ``inspect.signature`` once per device of every entry.
        """
        key = id(update_call)
        cached = capability_cache.get(key)
        if cached is None:
            cached = detect_device_registry_capabilities(update_call)
            capability_cache[key] = cached
        return cached

    def _detach_hub_link_from_device(
        device_id: str,
        *,
        device: Any,
        context_label: str,
        error_log_template: str,
    ) -> bool:
        """Give up this entry's hub link on ``device_id``.

        The intent is DETACH on the hub link, that is the link of a device
        sitting directly on the config entry rather than in one of its
        subentries. ``plan_device_ownership`` turns it into the keywords the
        installed core understands; this function never names them. See
        ``AGENTS.md``, "Registry updates", and ``docs/AI_DEPRECATIONS_GUIDE.md``,
        section VI.

        ``device`` is not optional and not a convenience. From Core 2026.8 a
        device has a single owning entry, so giving that entry up removes the
        device and every entity on it. The planner therefore refuses to act on
        an ownership state it cannot read, and the device entry is where that
        state is read from. Both call sites already hold it.

        Args:
            device_id: The device to detach.
            device: The device registry entry for ``device_id``.
            context_label: What the device is, for the debug log.
            error_log_template: Log template taking (entry_id, device_id, error).

        Returns:
            True when the detach was carried out or the core had nothing to do,
            False when the registry call failed.
        """

        update_call = getattr(dev_reg, "async_update_device", None)
        if not callable(update_call):  # pragma: no cover - defensive guard
            _LOGGER.error(
                error_log_template,
                entry_id,
                device_id,
                "device registry has no async_update_device",
            )
            return False

        # Unknown ownership and "owned by nobody" are two different statements,
        # and on a single-owner core the difference decides between a refusal
        # and a deletion. ``read_device_ownership`` draws that line and carries
        # the reasoning about the two half-known shapes.
        current = read_device_ownership(device)

        try:
            plan = plan_device_ownership(
                OwnershipIntent.DETACH,
                caps=_registry_capabilities(update_call),
                device_id=device_id,
                entry_id=entry_id,
                # Named rather than omitted so the intent is readable here. On
                # the DETACH path the two are equivalent: the planner maps an
                # omitted argument onto ``None`` right away. The distinction
                # carries weight on the legacy MOVE path, not on this one.
                detach_subentry_id=None,
                current=current,
            )
        except ValueError as err:
            # The planner refuses to guess. Reported, never attempted.
            _LOGGER.error(error_log_template, entry_id, device_id, err)
            return False

        if not plan:
            _LOGGER.debug(
                "[%s] Hub Cleanup: %s %s does not sit on the hub link; nothing to detach",
                entry_id,
                context_label,
                device_id,
            )
            return True

        try:
            execute_ownership_plan(dev_reg, plan)
        except Exception as err:  # pragma: no cover - defensive guard
            _LOGGER.error(error_log_template, entry_id, device_id, err)
            return False

        return True

    def _extract_hub_details(candidate: Any) -> tuple[str, Mapping[Any, Any]] | None:
        """Normalize hub container details from the runtime bucket."""

        entry_id: str | None
        coordinators: Any

        if isinstance(candidate, Mapping):
            raw_entry_id = candidate.get("entry_id")
            if isinstance(raw_entry_id, str) and raw_entry_id:
                entry_id = raw_entry_id
            elif raw_entry_id is not None:
                entry_id = str(raw_entry_id)
            else:
                entry_id = None
            coordinators = candidate.get("coordinators")
        else:
            raw_entry_id = getattr(candidate, "entry_id", None)
            if isinstance(raw_entry_id, str) and raw_entry_id:
                entry_id = raw_entry_id
            elif raw_entry_id is not None:
                entry_id = str(raw_entry_id)
            else:
                entry_id = None
            coordinators = getattr(candidate, "coordinators", None)

        if not entry_id:
            return None

        if isinstance(coordinators, Mapping):
            return entry_id, coordinators

        return None

    def _iter_hubs() -> list[tuple[str, Mapping[Any, Any]]]:
        """Yield hub entry_id and coordinator mapping tuples."""

        hubs: list[tuple[str, Mapping[Any, Any]]] = []
        seen: set[int] = set()

        hub_bucket = (
            domain_bucket.get("hubs") if isinstance(domain_bucket, Mapping) else None
        )
        if isinstance(hub_bucket, Mapping):
            for item in hub_bucket.values():
                if item is None or id(item) in seen:
                    continue
                seen.add(id(item))
                details = _extract_hub_details(item)
                if details is not None:
                    hubs.append(details)

        for value in domain_bucket.values():
            if value is None or id(value) in seen:
                continue
            details = _extract_hub_details(value)
            if details is not None:
                hubs.append(details)

        return hubs

    def _coordinator_devices(candidate: Any) -> list[Any]:
        """Normalize device payloads exposed by a coordinator."""

        data = getattr(candidate, "data", [])
        if isinstance(data, list):
            return data
        if isinstance(data, Iterable) and not isinstance(data, (str, bytes)):
            try:
                return list(data)
            except Exception:  # pragma: no cover - defensive guard
                return []
        return []

    def _coordinator_ignored(candidate: Any) -> set[str]:
        """Return the coordinator's ignored device ids, if exposed."""

        getter = getattr(candidate, "_get_ignored_set", None)
        if not callable(getter):
            return set()
        try:
            ignored = getter()
        except Exception:  # pragma: no cover - defensive guard
            return set()
        if isinstance(ignored, set):
            return ignored
        if isinstance(ignored, Iterable) and not isinstance(ignored, (str, bytes)):
            return {item for item in ignored if isinstance(item, str) and item}
        return set()

    def _entry_links_for_device(device: Any, target_entry_id: str) -> set[str | None]:
        """Return normalized subentry identifiers for ``target_entry_id``.

        Delegates to the shared accessor, which reads the scalar ownership
        fields before the ``config_entries_subentries`` shim that Core 2026.10
        reports on every read.
        """

        if not isinstance(target_entry_id, str) or not target_entry_id:
            return set()
        return extract_subentry_links(device, target_entry_id)

    processed_coordinators = 0
    seen_coordinators: set[int] = set()

    for _hub_entry_id, coordinators in _iter_hubs():
        for coordinator in coordinators.values():
            if coordinator is None or id(coordinator) in seen_coordinators:
                continue
            ensure_devices = getattr(coordinator, "_ensure_registry_for_devices", None)
            if not callable(ensure_devices):
                continue
            devices = _coordinator_devices(coordinator)
            ignored = _coordinator_ignored(coordinator)
            try:
                # Until Home Assistant exposes a public API for this workflow,
                # rely on the coordinator's private helper to mirror runtime behavior.
                created = ensure_devices(devices, ignored)
            except Exception as err:  # noqa: BLE001 - defensive logging
                _LOGGER.warning(
                    "Coordinator %s failed during registry rebuild: %s",
                    getattr(coordinator, "name", "<unknown>"),
                    err,
                )
                continue
            processed_coordinators += int(created or 0)
            seen_coordinators.add(id(coordinator))

    for runtime in entries_bucket.values():
        coordinator = getattr(runtime, "coordinator", None)
        if coordinator is None or id(coordinator) in seen_coordinators:
            continue
        ensure_devices = getattr(coordinator, "_ensure_registry_for_devices", None)
        if not callable(ensure_devices):
            continue
        try:
            # Until Home Assistant exposes a public API for this workflow,
            # rely on the coordinator's private helper to mirror runtime behavior.
            created = ensure_devices(
                _coordinator_devices(coordinator),
                _coordinator_ignored(coordinator),
            )
        except Exception as err:  # noqa: BLE001 - defensive logging
            _LOGGER.warning(
                "Runtime coordinator %s failed during registry rebuild: %s",
                getattr(coordinator, "name", "<unknown>"),
                err,
            )
            continue
        processed_coordinators += int(created or 0)
        seen_coordinators.add(id(coordinator))

    _LOGGER.info(
        "Completed device registry ensure phase; processed %d coordinators.",
        processed_coordinators,
    )

    cleaned_devices_total = 0

    # Iterate over all active googlefindmy RuntimeData instances
    for runtime in entries_bucket.values():
        coordinator = getattr(runtime, "coordinator", None)
        if coordinator is None:
            continue

        entry = getattr(coordinator, "config_entry", None)
        if entry is None or not hasattr(entry, "entry_id"):
            continue

        entry_id = entry.entry_id
        # The entry title is never logged: the config flow sets it to the account
        # e-mail. The entry ID identifies the entry just as well.
        _LOGGER.info("[%s] Hub Cleanup: Processing entry", entry_id)

        # 1. Find the correct Service Device ID
        service_device_ident = service_device_identifier(entry_id)
        service_device = resolve_device_by_identifiers(
            dev_reg, (service_device_ident,), entry_id=entry_id
        )
        service_device_id = getattr(service_device, "id", None)
        service_meta = None
        get_metadata = getattr(coordinator, "get_subentry_metadata", None)
        if callable(get_metadata):
            try:
                service_meta = get_metadata(key=SERVICE_SUBENTRY_KEY)
            except TypeError:
                service_meta = None
        service_subentry_id = None
        if service_meta is not None:
            candidate_service_id = getattr(service_meta, "config_subentry_id", None)
            if isinstance(candidate_service_id, str) and candidate_service_id:
                service_subentry_id = candidate_service_id
        if not isinstance(service_subentry_id, str) or not service_subentry_id:
            fallback_service_id = getattr(entry, "service_subentry_id", None)
            if isinstance(fallback_service_id, str) and fallback_service_id:
                service_subentry_id = fallback_service_id
        if not service_device_id:
            _LOGGER.debug("[%s] Hub Cleanup: Service device not found.", entry_id)
            # Try to ensure it exists before continuing
            try:
                coordinator._ensure_service_device_exists()
                service_device = resolve_device_by_identifiers(
                    dev_reg, (service_device_ident,), entry_id=entry_id
                )
                service_device_id = getattr(service_device, "id", None)
                if not service_device_id:
                    _LOGGER.warning(
                        "[%s] Hub Cleanup: Could not find or create service device. Skipping entry.",
                        entry_id,
                    )
                    continue
            except Exception as e:
                _LOGGER.error(
                    "[%s] Hub Cleanup: Error ensuring service device: %s", entry_id, e
                )
                continue

        service_cleanup_applied = False
        service_links = _entry_links_for_device(service_device, entry_id)
        if (
            isinstance(service_subentry_id, str)
            and service_subentry_id
            and service_links
            and None in service_links
            and service_subentry_id in service_links
            and isinstance(service_device_id, str)
            and service_device_id
        ):
            _LOGGER.info(
                "[%s] Hub Cleanup: Detaching redundant hub link from service device '%s' (device_id=%s). Links prior to cleanup: %s",
                entry_id,
                getattr(service_device, "name", "<unknown>"),
                service_device_id,
                sorted(link for link in service_links if link is not None)
                + (["<hub>"] if None in service_links else []),
            )
            if _detach_hub_link_from_device(
                service_device_id,
                device=service_device,
                context_label="service device",
                error_log_template="[%s] Hub Cleanup: Failed to detach hub entry from service device %s: %s",
            ):
                service_cleanup_applied = True
                refresh_service = getattr(dev_reg, "async_get", None)
                if callable(refresh_service):
                    refreshed = refresh_service(service_device_id)
                    if refreshed is not None:
                        service_device = refreshed

        # 2. Find the correct Tracker Subentry ID
        # We access the coordinator's metadata which was refreshed during setup
        tracker_meta = coordinator.get_subentry_metadata(key=TRACKER_SUBENTRY_KEY)
        if not tracker_meta or not tracker_meta.config_subentry_id:
            _LOGGER.warning(
                "[%s] Hub Cleanup: Tracker subentry metadata not found. "
                "Reloading integration may be required. Skipping entry.",
                entry_id,
            )
            continue

        correct_tracker_subentry_id = tracker_meta.config_subentry_id
        tracker_entry_id: str | None = None

        subentries_iterable: tuple[Any, ...]
        subentries_container = getattr(entry, "subentries", None)
        if isinstance(subentries_container, Mapping):
            subentries_iterable = tuple(subentries_container.values())
        elif isinstance(subentries_container, Iterable) and not isinstance(
            subentries_container, (str, bytes)
        ):
            subentries_iterable = tuple(subentries_container)
        else:
            subentries_iterable = ()

        for subentry in subentries_iterable:
            subentry_identifier = getattr(subentry, "subentry_id", None)
            if subentry_identifier is None:
                subentry_identifier = getattr(subentry, "config_subentry_id", None)
            if subentry_identifier != correct_tracker_subentry_id:
                continue

            raw_tracker_entry_id = getattr(subentry, "entry_id", None)
            if isinstance(raw_tracker_entry_id, str) and raw_tracker_entry_id:
                tracker_entry_id = raw_tracker_entry_id
                break
            if raw_tracker_entry_id is not None:
                tracker_entry_id = str(raw_tracker_entry_id)
                if tracker_entry_id:
                    break

        if not tracker_entry_id:
            fallback_tracker_entry_id = getattr(tracker_meta, "entry_id", None)
            if isinstance(fallback_tracker_entry_id, str) and fallback_tracker_entry_id:
                tracker_entry_id = fallback_tracker_entry_id

        tracker_identifier_for_log = tracker_entry_id or correct_tracker_subentry_id
        _LOGGER.debug(
            "[%s] Hub Cleanup: Found Service Device ID: %s",
            entry_id,
            service_device_id,
        )
        _LOGGER.debug(
            "[%s] Hub Cleanup: Found Tracker Subentry ID: %s",
            entry_id,
            correct_tracker_subentry_id,
        )
        if tracker_entry_id:
            _LOGGER.debug(
                "[%s] Hub Cleanup: Tracker subentry link resolves to ConfigEntry %s",
                entry_id,
                tracker_entry_id,
            )
        else:
            _LOGGER.debug(
                "[%s] Hub Cleanup: Tracker subentry link falling back to identifier %s",
                entry_id,
                tracker_identifier_for_log,
            )

        # 3. Find and remove orphaned devices
        devices_for_entry = dr.async_entries_for_config_entry(dev_reg, entry_id)
        entry_cleaned_devices = 1 if service_cleanup_applied else 0

        for device in devices_for_entry:
            if device is None or not hasattr(device, "id"):
                continue

            # Skip the Service Device itself
            if device.id == service_device_id:
                continue

            # Check if the device is correctly linked to the tracker subentry
            # One accessor for every core: the owning entries come from the
            # scalar ``config_entry_id`` where it exists (2026.8+) and from the
            # legacy set below that, and the subentry links per entry come from
            # ``extract_subentry_links``. Neither reads the
            # ``config_entries_subentries`` shim that Core 2026.10 reports.
            # Only a real subentry id can confirm a tracker link; ``None`` in
            # the link set is the hub link and must not match a missing id.
            tracker_linked_entry_ids: set[str] = (
                {
                    owner_entry_id
                    for owner_entry_id in device_owning_entry_ids(device)
                    if owner_entry_id
                    and correct_tracker_subentry_id
                    in extract_subentry_links(device, owner_entry_id)
                }
                if isinstance(correct_tracker_subentry_id, str)
                and correct_tracker_subentry_id
                else set()
            )

            linked_entry_ids = {
                link_entry_id
                for link_entry_id in device_owning_entry_ids(device)
                if link_entry_id
            }

            has_hub_link = entry_id in linked_entry_ids
            confirmed_tracker_entry_ids = {
                candidate
                for candidate in tracker_linked_entry_ids
                if candidate in linked_entry_ids
            }
            if tracker_entry_id and tracker_entry_id in linked_entry_ids:
                confirmed_tracker_entry_ids.add(tracker_entry_id)
            device_config_subentry_id = getattr(device, "config_subentry_id", None)
            is_correctly_linked_tracker = (
                isinstance(device_config_subentry_id, str)
                and device_config_subentry_id == correct_tracker_subentry_id
            )
            has_tracker_link = bool(confirmed_tracker_entry_ids) or (
                is_correctly_linked_tracker and bool(linked_entry_ids)
            )

            if not has_hub_link:
                _LOGGER.debug(
                    "[%s] Hub Cleanup: Skipping device '%s' (device_id=%s); hub config entry '%s' not linked.",
                    entry_id,
                    device.name or "<unknown>",
                    device.id,
                    entry_id,
                )
                continue

            if has_tracker_link:
                _LOGGER.info(
                    "[%s] Hub Cleanup: Detaching redundant hub config entry from device '%s' (device_id=%s); tracker link '%s' already active. Linked entries prior to cleanup: %s",
                    entry_id,
                    device.name or "<unknown>",
                    device.id,
                    tracker_identifier_for_log,
                    sorted(linked_entry_ids),
                )
            else:
                _LOGGER.info(
                    "[%s] Hub Cleanup: Detaching hub config entry from device '%s' (device_id=%s); tracker link '%s' not found. Linked entries prior to cleanup: %s",
                    entry_id,
                    device.name or "<unknown>",
                    device.id,
                    tracker_identifier_for_log,
                    sorted(linked_entry_ids),
                )
            if _detach_hub_link_from_device(
                device.id,
                device=device,
                context_label="device",
                error_log_template="[%s] Hub Cleanup: Failed to detach hub entry from device %s: %s",
            ):
                entry_cleaned_devices += 1

        if entry_cleaned_devices > 0:
            _LOGGER.info(
                "[%s] Hub Cleanup: Removed %d orphaned device links.",
                entry_id,
                entry_cleaned_devices,
            )
            cleaned_devices_total += entry_cleaned_devices

    _LOGGER.info(
        "Device registry cleanup phase complete. Removed %d total orphaned device links.",
        cleaned_devices_total,
    )

    # --- Phase 3 (from the original code): clean up legacy tracker entities ---
    # This logic is independent and should be preserved.
    ent_reg = er.async_get(hass)

    managed_entry_ids: set[str] = set(entries_bucket.keys())

    removed_entities = 0
    entities_container = getattr(ent_reg, "entities", None)
    if isinstance(entities_container, Mapping):
        for entry in list(entities_container.values()):
            if getattr(entry, "platform", None) != DOMAIN:
                continue
            if getattr(entry, "domain", None) != DEVICE_TRACKER_DOMAIN:
                continue

            config_entry_id = getattr(entry, "config_entry_id", None)
            if (
                not isinstance(config_entry_id, str)
                or config_entry_id not in managed_entry_ids
            ):
                continue

            unique_id = getattr(entry, "unique_id", None)
            if not isinstance(unique_id, str) or not unique_id:
                continue

            is_canonical = (
                unique_id.startswith(f"{config_entry_id}:")
                and unique_id.count(":") >= 2
            )
            if is_canonical:
                continue

            _LOGGER.info(
                "[%s] Tracker cleanup: removing legacy entity '%s' (unique_id=%s); it no longer matches the canonical entry-scoped schema.",
                config_entry_id,
                getattr(entry, "entity_id", "<unknown>"),
                unique_id,
            )
            try:
                ent_reg.async_remove(entry.entity_id)
                removed_entities += 1
            except Exception as err:  # noqa: BLE001 - defensive logging
                _LOGGER.error(
                    "[%s] Tracker cleanup: failed to remove entity %s: %s",
                    config_entry_id,
                    getattr(entry, "entity_id", "<unknown>"),
                    err,
                )

    _LOGGER.info(
        "Entity registry cleanup phase complete. Removed %d legacy tracker entities.",
        removed_entities,
    )


# Home Assistant's admin helper validates with an empty schema that rejects
# every field unless told otherwise. ``services.yaml`` only describes the
# fields for the UI and does not validate at runtime; the maintenance handlers
# check their fields themselves, so this schema accepts any field and passes
# the data through unchanged.
_ADMIN_PASSTHROUGH_SCHEMA = vol.Schema({}, extra=vol.ALLOW_EXTRA)


def _control_entity_ids_for_target(
    hass: HomeAssistant, raw_device_id: object, ctx: Mapping[str, Any]
) -> set[str]:
    """Return the entity ids a caller needs ``control`` on to act on a target.

    The target is the value of the ``device_id`` field: a device registry id,
    an entity id of this integration or a canonical tracker id. The result is
    the union of the enabled entities of this integration on the device the
    value names directly (or the device of the named entity) and on every
    device of this integration whose canonical id equals the one the value
    resolves to. The second group is needed because the action falls back to
    the first coordinator that knows the canonical id, which can belong to
    another account than the named device.

    A value that names a device, directly or through one of its entities,
    whose entities of this integration are all disabled yields an empty set,
    even when another account holds the same tracker with enabled entities.
    Disabling every entity of a device therefore keeps non-admins off that
    device; the tracker stays reachable through its canonical id.

    Membership in the integration comes from the entity registry
    (``platform == DOMAIN``) only; device entries are not inspected for their
    config entries. An empty set means "no entity to check against" and is
    rejected by the caller.

    Limit: a device with the same canonical id but without an enabled entity of
    this integration (or known only to a coordinator) adds nothing to the set,
    although the fallback may still run against it. It is the same physical
    tracker, so the caller already holds ``control`` on that tracker elsewhere.
    """

    if not isinstance(raw_device_id, str) or not raw_device_id:
        return set()

    ent_reg = er.async_get(hass)
    dev_reg = dr.async_get(hass)
    entities_by_device: dict[str, set[str]] = {}
    for entity in ent_reg.entities.values():
        if entity.platform != DOMAIN or not entity.device_id or entity.disabled:
            continue
        entities_by_device.setdefault(entity.device_id, set()).add(entity.entity_id)

    resolve = ctx["resolve_canonical"]
    try:
        canonical_id = resolve(hass, raw_device_id)[0]
    except HomeAssistantError:
        return set()

    target_devices: set[str] = set()
    device = dev_reg.async_get(raw_device_id)
    if device is not None:
        target_devices.add(device.id)
    else:
        entity_entry = ent_reg.async_get(raw_device_id)
        if (
            entity_entry is not None
            and entity_entry.platform == DOMAIN
            and entity_entry.device_id
        ):
            target_devices.add(entity_entry.device_id)

    if any(named not in entities_by_device for named in target_devices):
        return set()

    for candidate_device_id in entities_by_device:
        if candidate_device_id in target_devices:
            continue
        try:
            candidate_canonical = resolve(hass, candidate_device_id)[0]
        except HomeAssistantError:
            continue
        if candidate_canonical == canonical_id:
            target_devices.add(candidate_device_id)

    result: set[str] = set()
    for target_device_id in target_devices:
        result.update(entities_by_device.get(target_device_id, set()))
    return result


async def _async_authorize_device_call(
    hass: HomeAssistant,
    call: ServiceCall,
    raw_device_id: object,
    ctx: Mapping[str, Any],
) -> None:
    """Reject a user call that lacks ``control`` on the target's entities.

    Calls without a user (automations, scripts not started by a user) and calls
    by admins pass. For every other user, all entities returned by
    ``_control_entity_ids_for_target`` must grant ``control``; an empty set is
    rejected. The rejection is the same for an unknown device and for a missing
    permission and names neither the device nor an entity.

    Invariant: between building the target set here and the coordinator call
    after ``_resolve_runtime_for_device_id`` there is no await that yields to
    the event loop, so the check and the resolution see the same registry
    state. Do not add one.
    """

    user_id = call.context.user_id
    if not user_id:
        return
    user = await hass.auth.async_get_user(user_id)
    if user is None:
        raise UnknownUser(context=call.context, permission=POLICY_CONTROL)
    if user.is_admin:
        return

    entity_ids = _control_entity_ids_for_target(hass, raw_device_id, ctx)
    if not entity_ids or not all(
        user.permissions.check_entity(entity_id, POLICY_CONTROL)
        for entity_id in entity_ids
    ):
        _LOGGER.warning(
            "Rejected %s.%s: caller lacks control permission on the target",
            call.domain,
            call.service,
        )
        raise Unauthorized(context=call.context, permission=POLICY_CONTROL)


def _decrypt_account_key(owner_key: bytes, blob: bytes) -> bytes:
    """Decrypt an account key; the crypto import and the work stay off the loop."""
    from .KeyBackup.cloud_key_decryptor import decrypt_account_key  # noqa: PLC0415

    return bytes(decrypt_account_key(owner_key, blob))


async def _async_beacon_keys(
    hass: HomeAssistant, runtime: Any, canonical_id: str
) -> BeaconKeys:
    """Return the tracker's account key and EIK, or raise a translated error.

    Uses the resolver's own key path, so a key mismatch found by the diagnostic is
    about the key the resolver works with. Keys stay out of every error text.
    """
    unavailable = _service_validation_error(
        "No key material is available for this device.",
        translation_key="beacon_keys_unavailable",
    )
    coordinator = getattr(runtime, "coordinator", None)
    resolver = hass.data.get(DOMAIN, {}).get(DATA_EID_RESOLVER)
    if coordinator is None or resolver is None:
        raise unavailable
    identity = next(
        (
            item
            for item in coordinator.get_active_device_identities()
            if str(item.canonical_id).rsplit(":", 1)[-1]
            == canonical_id.rsplit(":", 1)[-1]
        ),
        None,
    )
    if identity is None:
        raise unavailable

    cache = getattr(coordinator, "cache", None)
    normalized = await resolver._normalize_identities([identity], cache=cache)
    eik = _normalize_encrypted_blob(normalized[0].identity_key) if normalized else None

    account_key: bytes | None = None
    blob = _normalize_encrypted_blob(identity.encrypted_account_key)
    if blob is not None:
        owner = await resolver._resolve_owner_key(identity, cache=cache)
        if owner is not None:
            try:
                account_key = await asyncio.to_thread(
                    _decrypt_account_key, owner.key, blob
                )
            except ValueError as err:
                _LOGGER.debug("Account key not usable: %s", type(err).__name__)

    keys = BeaconKeys(
        account_key=account_key if account_key and len(account_key) == 16 else None,
        eik=eik if eik and len(eik) == 32 else None,
    )
    if keys.account_key is None and keys.eik is None:
        raise unavailable
    return keys


async def async_register_services(hass: HomeAssistant, ctx: dict[str, Any]) -> None:
    """Register integration-wide services, using services.yaml for metadata.

    Parameters
    ----------
    hass : HomeAssistant
        The running HA instance.
    ctx : dict[str, Any]
        A context passed by __init__.py to avoid import cycles. Expected keys:
        - "domain": str
        - "resolve_canonical": Callable[[HomeAssistant, str], Tuple[str, str]]
        - "is_active_entry": Callable[[ConfigEntry], bool]
        - "primary_active_entry": Callable[[list[ConfigEntry]], ConfigEntry | None]
        - "opt": Callable[[ConfigEntry, str, Any], Any]
        - "default_map_view_token_expiration": bool
        - "opt_map_view_token_expiration_key": str
        - "redact_url_token": Callable[[str], str]
        - "soft_migrate_entry": Callable[[HomeAssistant, Any], Any]  # awaited per entry
        - "migrate_unique_ids": Callable[[HomeAssistant, Any], Any]
        - "relink_button_devices": Callable[[HomeAssistant, Any], Any]
        - "relink_subentry_entities": Callable[[HomeAssistant, Any], Any]
        - "purge_entry_registrations": Callable[[HomeAssistant, ConfigEntry], Awaitable[tuple[int, int]]]
          # rebuild mode, between unload and setup
        - "coalesce_account_entries": Callable[[HomeAssistant, ConfigEntry], Awaitable[ConfigEntry]]
        - "extract_normalized_email": Callable[[ConfigEntry], str | None]

    Permissions
    -----------
    ``locate_device``, ``locate_external``, ``play_sound`` and ``stop_sound``
    check, as their first statement, that a calling user holds ``control`` on
    every enabled entity of this integration at the target device
    (``_async_authorize_device_call``); calls without a user and calls by
    admins pass. ``refresh_device_urls``, ``rebuild_device_registry`` and
    ``rebuild_registry`` are registered with ``async_register_admin_service``
    and reject non-admin users.
    """

    # ---- Small local helpers (no circular imports) ---------------------------

    def _iter_runtimes(hass: HomeAssistant) -> Iterable[Any]:
        """Yield active runtime containers, preferring entry.runtime_data."""

        seen: set[int] = set()
        manager = getattr(hass, "config_entries", None)
        async_entries = getattr(manager, "async_entries", None)
        if callable(async_entries):
            try:
                for entry in async_entries(DOMAIN):
                    runtime = getattr(entry, "runtime_data", None)
                    if runtime is None:
                        continue
                    seen.add(id(runtime))
                    yield runtime
            except Exception:  # pragma: no cover - defensive guard
                pass

        # Fallback: scan domain-level entries bucket for runtimes not yet
        # discovered via entry.runtime_data.  hass.data[DOMAIN] is compatible
        # with the HassKey-based DATA_DOMAIN defined in __init__.py.
        entries: dict[str, Any] = hass.data.setdefault(DOMAIN, {}).setdefault(
            "entries", {}
        )
        for runtime in entries.values():
            if id(runtime) not in seen:
                yield runtime

    def _entry_for_id(hass: HomeAssistant, entry_id: str) -> Any | None:
        """Return the config entry with the given id, if available."""

        manager = getattr(hass, "config_entries", None)
        if manager is None:
            return None

        getter = getattr(manager, "async_get_entry", None)
        if callable(getter):
            try:
                return getter(entry_id)
            except Exception:  # pragma: no cover - defensive guard
                return None

        async_entries = getattr(manager, "async_entries", None)
        if callable(async_entries):
            try:
                for entry in async_entries(DOMAIN):
                    if entry.entry_id == entry_id:
                        return entry
            except Exception:  # pragma: no cover - defensive guard
                return None
        return None

    async def _resolve_runtime_for_device_id(device_id: str) -> tuple[Any, str]:
        """Return the runtime and canonical_id for a device_id or raise translated error.

        Robustness:
        - If no runtime is available at all, fail early with a clear, translated error.
        - Prefer Device Registry mapping from the provided device_id.
        - Fall back to scanning active coordinators for the device's canonical id presence.
        """
        # Early exit: no active runtimes at all
        runtimes = list(_iter_runtimes(hass))
        if not runtimes:
            configured_entries = []
            active_count = 0
            total_count = 0

            config_entries = getattr(hass, "config_entries", None)
            if config_entries is not None:
                try:
                    configured_entries = list(config_entries.async_entries(DOMAIN))
                except Exception:  # pragma: no cover - defensive guard
                    configured_entries = []

            if configured_entries:
                total_count = len(configured_entries)
                is_active = ctx.get("is_active_entry")
                if callable(is_active):
                    active_count = sum(
                        1 for entry in configured_entries if is_active(entry)
                    )
                entry_titles = [
                    entry.title or entry.entry_id for entry in configured_entries
                ]
                entries_placeholder = ", ".join(entry_titles) or "—"
            else:
                entries_placeholder = "—"

            placeholders = {
                "entries": entries_placeholder,
                "active_count": str(active_count),
                "total_count": str(total_count),
            }
            raise _service_validation_error(
                (
                    "No active Google Find My entries (active {active_count}/{total_count};"
                    " entries: {entries}).".format(**placeholders)
                ),
                translation_key="no_active_entry",
                translation_placeholders=placeholders,
            )

        # Resolve canonical id & friendly name via context resolver (device_id/entity_id/canonical_id)
        try:
            canonical_id, _friendly = ctx["resolve_canonical"](hass, device_id)
        except HomeAssistantError as err:
            # Pass through as translated validation error
            placeholders = {"device_id": str(device_id)}
            raise _service_validation_error(
                "Device '{device_id}' was not found.".format(**placeholders),
                translation_key="device_not_found",
                translation_placeholders=placeholders,
            ) from err

        # 1) Preferred mapping: device registry relation for the passed device_id
        dev_reg = dr.async_get(hass)
        dev = dev_reg.async_get(device_id)
        if dev:
            for entry_id in device_owning_entry_ids(dev):
                entry = _entry_for_id(hass, entry_id)
                # Prefer entry.runtime_data (2026 standard), then entries bucket.
                runtime = getattr(entry, "runtime_data", None)
                if runtime:
                    return runtime, canonical_id
                runtime = (
                    hass.data.setdefault(DOMAIN, {})
                    .setdefault("entries", {})
                    .get(entry_id)
                )
                if runtime:
                    return runtime, canonical_id

        # 2) Fallback: scan known coordinators for the canonical id
        for runtime in runtimes:
            coord = getattr(runtime, "coordinator", None)
            if not coord:
                continue

            display_lookup = getattr(coord, "get_device_display_name", None)
            if callable(display_lookup):
                try:
                    display_name = display_lookup(canonical_id)
                except Exception:
                    display_name = None
                if display_name is not None:
                    return runtime, canonical_id

            location_lookup = getattr(coord, "get_device_location_data", None)
            if callable(location_lookup):
                try:
                    location_data = location_lookup(canonical_id)
                except Exception:
                    continue
                if location_data is not None:
                    return runtime, canonical_id

        # 3) Not found -> translated error
        placeholders = {"device_id": str(device_id)}
        raise _service_validation_error(
            "Device '{device_id}' was not found.".format(**placeholders),
            translation_key="device_not_found",
            translation_placeholders=placeholders,
        )

    # ---- Service handlers ----------------------------------------------------

    async def async_locate_device_service(call: ServiceCall) -> None:
        """Handle locate device service call (metadata in services.yaml)."""
        await _async_authorize_device_call(hass, call, call.data.get("device_id"), ctx)
        raw_device_id = call.data.get("device_id")
        if not isinstance(raw_device_id, str) or not raw_device_id:
            placeholders = {"device_id": str(raw_device_id)}
            raise _service_validation_error(
                "Device '{device_id}' was not found.".format(**placeholders),
                translation_key="device_not_found",
                translation_placeholders=placeholders,
            )
        try:
            runtime, canonical_id = await _resolve_runtime_for_device_id(raw_device_id)
            await runtime.coordinator.async_locate_device(canonical_id)
        except ServiceValidationError:
            raise
        except Exception as err:
            placeholders = {
                "device_id": str(raw_device_id),
                "error": str(err),
            }
            raise _service_validation_error(
                "Failed to locate device '{device_id}': {error}".format(**placeholders),
                translation_key="locate_failed",
                translation_placeholders=placeholders,
            ) from err

    async def async_play_sound_service(call: ServiceCall) -> None:
        """Handle play sound service call."""
        await _async_authorize_device_call(hass, call, call.data.get("device_id"), ctx)
        raw_device_id = call.data.get("device_id")
        if not isinstance(raw_device_id, str) or not raw_device_id:
            placeholders = {"device_id": str(raw_device_id)}
            raise _service_validation_error(
                "Device '{device_id}' was not found.".format(**placeholders),
                translation_key="device_not_found",
                translation_placeholders=placeholders,
            )
        try:
            runtime, canonical_id = await _resolve_runtime_for_device_id(raw_device_id)
            outcome = await runtime.coordinator.async_play_sound(canonical_id)
            placeholders = {"device_id": str(raw_device_id)}
            if outcome is PlaySoundOutcome.SUPPRESSED:
                raise _service_validation_error(
                    "Play sound suppressed for device '{device_id}'".format(
                        **placeholders
                    ),
                    # NOT play_sound_failed: that template carries an {error}
                    # placeholder this call site cannot fill, so HA would render
                    # the literal "{error}" (it formats under suppress(KeyError)).
                    # Same defect class as the stop path, same fix.
                    translation_key="play_sound_suppressed",
                    translation_placeholders=placeholders,
                )
            if outcome is not PlaySoundOutcome.ACCEPTED:
                # Closed set, closed handling, and written in the negative form
                # for the same reason the stop path is: the safe default here is
                # failure. A value from outside the enum -- a bool from a stale
                # test double, a member added later without visiting this site --
                # must not be reported to the user as a ring that started.
                #
                # FAILED and that stray value share one message on purpose. The
                # play path has exactly two user-facing outcomes, and the
                # difference between them is what the user should DO: wait, or
                # look at the log and act. An unknown value belongs to the
                # second group; splitting it off would need a third message in
                # eleven files that says the same thing. The contract breach
                # itself is not silent, it goes to the log below.
                if outcome is not PlaySoundOutcome.FAILED:
                    # No device id: AGENTS.md section 5 forbids logging them, and
                    # this line adds nothing by naming one -- the breach is in the
                    # coordinator's contract, not in a particular tracker.
                    _LOGGER.error(
                        "async_play_sound returned %r, which is not a "
                        "PlaySoundOutcome; reporting it as a failure",
                        outcome,
                    )
                raise _service_validation_error(
                    "Play sound for '{device_id}' was rejected".format(**placeholders),
                    translation_key="play_sound_rejected",
                    translation_placeholders=placeholders,
                )
        except ServiceValidationError:
            raise
        except Exception as err:
            placeholders = {
                "device_id": str(raw_device_id),
                "error": str(err),
            }
            raise _service_validation_error(
                "Failed to play sound on device '{device_id}': {error}".format(
                    **placeholders
                ),
                translation_key="play_sound_failed",
                translation_placeholders=placeholders,
            ) from err

    async def async_stop_sound_service(call: ServiceCall) -> None:
        """Handle stop sound service call."""
        await _async_authorize_device_call(hass, call, call.data.get("device_id"), ctx)
        raw_device_id = call.data.get("device_id")
        if not isinstance(raw_device_id, str) or not raw_device_id:
            placeholders = {"device_id": str(raw_device_id)}
            raise _service_validation_error(
                "Device '{device_id}' was not found.".format(**placeholders),
                translation_key="device_not_found",
                translation_placeholders=placeholders,
            )
        request_uuid_raw = call.data.get("request_uuid")
        if request_uuid_raw is not None and not isinstance(request_uuid_raw, str):
            placeholders = {
                "device_id": str(raw_device_id),
                "request_uuid": str(request_uuid_raw),
            }
            raise _service_validation_error(
                "Stop sound request UUID for '{device_id}' is invalid ({request_uuid}).".format(
                    **placeholders
                ),
                # Own key, not `stop_sound_failed`: that template carries an
                # `{error}` placeholder this call site cannot fill, and HA
                # swallows the resulting KeyError, leaving the user with the raw
                # template. Same defect the suppressed-stop branch already
                # avoids; fixed here as a class, not as an instance.
                translation_key="stop_sound_invalid_uuid",
                translation_placeholders=placeholders,
            )
        # Blank is normalised to None one layer down, in
        # `coordinator.async_stop_sound`, so that every caller of that method
        # (not just this handler) gets the cached-key fallback.
        request_uuid: str | None = request_uuid_raw
        try:
            runtime, canonical_id = await _resolve_runtime_for_device_id(raw_device_id)
            # Read the case BEFORE the stop and with nothing awaited in
            # between, so it describes the same cache state the stop decides
            # on. Only used if the outcome is UNCORRELATED (K10).
            uncorrelated_reason = runtime.coordinator.classify_stop_correlation(
                canonical_id, request_uuid
            )
            outcome = await runtime.coordinator.async_stop_sound(
                canonical_id, request_uuid
            )
            placeholders = {"device_id": str(raw_device_id)}
            if outcome is StopSoundOutcome.SUPPRESSED:
                raise _service_validation_error(
                    "Stop sound suppressed for device '{device_id}'".format(
                        **placeholders
                    ),
                    translation_key="stop_sound_suppressed",
                    translation_placeholders=placeholders,
                )
            if outcome is StopSoundOutcome.FAILED:
                # Distinct from SUPPRESSED, and not by distance travelled:
                # auth failures, 401/403, server rejections, rate limits,
                # network errors, empty replies AND a missing action token all
                # arrive here, because none of them is answered by waiting a
                # moment. SUPPRESSED is reserved for the one condition that is
                # -- a push transport that has not come up yet. See
                # StopSoundOutcome.FAILED for the full rule. So this branch
                # gets its own message and points at the log, which does carry
                # the specific cause.
                raise _service_validation_error(
                    "Stop sound for '{device_id}' was rejected".format(**placeholders),
                    translation_key="stop_sound_rejected",
                    translation_placeholders=placeholders,
                )
            if outcome is StopSoundOutcome.UNCORRELATED:
                # Submitted, but nothing proves an effect: reporting plain
                # success here would be misinformation (BSkando#195). It stays
                # an error in every case; only the text says which case it is,
                # because each one has a different remedy (K10). A reason the
                # table does not know falls back to the generic text.
                message, translation_key = _STOP_UNCORRELATED_MESSAGES.get(
                    uncorrelated_reason,
                    (
                        "Stop sound for '{device_id}' could not be matched to a "
                        "running ring; it may keep playing.",
                        "stop_sound_uncorrelated",
                    ),
                )
                raise _service_validation_error(
                    message.format(**placeholders),
                    translation_key=translation_key,
                    translation_placeholders=placeholders,
                )
            if outcome is not StopSoundOutcome.CANCELLED:
                # Closed set, closed handling. Silence here would mean that any
                # value outside the enum -- a bool from a stale test double, a
                # future member added without visiting this site -- is reported
                # to the user as a successful stop. That is the BSkando#195
                # failure mode reintroduced by omission, so the default is an
                # error, not success.
                raise _service_validation_error(
                    "Stop sound for '{device_id}' returned an unknown outcome "
                    "({outcome}); treating it as unproven.".format(
                        outcome=outcome, **placeholders
                    ),
                    translation_key="stop_sound_uncorrelated",
                    translation_placeholders=placeholders,
                )
        except ServiceValidationError:
            raise
        except Exception as err:
            placeholders = {
                "device_id": str(raw_device_id),
                "error": str(err),
            }
            raise _service_validation_error(
                "Failed to stop sound on device '{device_id}': {error}".format(
                    **placeholders
                ),
                translation_key="stop_sound_failed",
                translation_placeholders=placeholders,
            ) from err

    async def async_locate_external_service(call: ServiceCall) -> None:
        """External locate device service (delegates to locate)."""
        await _async_authorize_device_call(hass, call, call.data.get("device_id"), ctx)
        raw_device_id = call.data.get("device_id")
        if not isinstance(raw_device_id, str) or not raw_device_id:
            placeholders = {"device_id": str(raw_device_id)}
            raise _service_validation_error(
                "Device '{device_id}' was not found.".format(**placeholders),
                translation_key="device_not_found",
                translation_placeholders=placeholders,
            )
        # device_name is optional; currently used only for logging on caller side.
        try:
            runtime, canonical_id = await _resolve_runtime_for_device_id(raw_device_id)
            await runtime.coordinator.async_locate_device(canonical_id)
        except ServiceValidationError:
            raise
        except Exception as err:
            placeholders = {
                "device_id": str(raw_device_id),
                "error": str(err),
            }
            raise _service_validation_error(
                "Failed to locate device '{device_id}': {error}".format(**placeholders),
                translation_key="locate_failed",
                translation_placeholders=placeholders,
            ) from err

    async def async_refresh_device_urls_service(call: ServiceCall) -> None:
        """Refresh configuration URLs for integration devices (absolute URL).

        Security:
            The token is a short-lived (weekly) or static gate derived from the HA UUID.
            All tokens are redacted in logs; the view must validate tokens server-side.

        Clearing a disabled entry's stale ``configuration_url`` needs no base
        URL, so that pass runs before the base-URL lookup below, mirroring
        ``_async_refresh_device_urls`` in ``__init__.py``.
        """
        entries = hass.config_entries.async_entries(DOMAIN)
        entries_by_id = {entry.entry_id: entry for entry in entries}

        default_expiration = bool(
            ctx.get(
                "default_map_view_token_expiration",
                DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
            )
        )
        opt_reader = ctx.get("opt")
        opt_key = ctx.get(
            "opt_map_view_token_expiration_key", OPT_MAP_VIEW_TOKEN_EXPIRATION
        )

        map_view_enabled_cache: dict[str, bool] = {}
        token_cache: dict[str, str] = {}
        ha_uuid = str(hass.data.get("core.uuid", "ha"))
        now = int(time.time())

        def _read_bool_option(entry_id: str | None, key: str, default: bool) -> bool:
            """Read one bool option for ``entry_id``, via the shared ``opt`` reader.

            Used for the token-expiration option. It needs no cache of its own:
            its only caller, ``_expiration_enabled``, is called only from
            ``_token_for_entry``, which caches per entry, so the option is read
            once per entry and refresh regardless of device count. Map View
            enablement is read through
            ``map_view._is_map_view_enabled_for_entry`` so every call site
            shares one rule.
            """
            entry = entries_by_id.get(entry_id) if entry_id else None
            enabled = default

            if entry:
                if callable(opt_reader):
                    try:
                        enabled = bool(opt_reader(entry, key, default))
                    except Exception:
                        enabled = bool(
                            entry.options.get(key, entry.data.get(key, default))
                        )
                else:
                    enabled = bool(entry.options.get(key, entry.data.get(key, default)))

            return bool(enabled)

        def _map_view_enabled(entry_id: str | None) -> bool:
            cache_key = entry_id or ""
            if cache_key not in map_view_enabled_cache:
                entry = entries_by_id.get(entry_id) if entry_id else None
                map_view_enabled_cache[cache_key] = _is_map_view_enabled_for_entry(
                    entry
                )
            return map_view_enabled_cache[cache_key]

        def _expiration_enabled(entry_id: str | None) -> bool:
            return _read_bool_option(entry_id, opt_key, default_expiration)

        def _token_for_entry(entry_id: str | None) -> str:
            cache_key = entry_id or ""
            if cache_key in token_cache:
                return token_cache[cache_key]

            entry_part = entry_id or ""
            if _expiration_enabled(entry_id):
                seed = map_token_secret_seed(ha_uuid, entry_part, True, now=now)
            else:
                seed = map_token_secret_seed(ha_uuid, entry_part, False)

            token_cache[cache_key] = map_token_hex_digest(seed)
            return token_cache[cache_key]

        def _device_is_service(device: Any) -> bool:
            identifiers: set[tuple[str, str]] = (
                getattr(device, "identifiers", set()) or set()
            )
            for domain, ident in identifiers:
                if domain != DOMAIN:
                    continue
                ident_str = str(ident)
                if ident_str == LEGACY_SERVICE_IDENTIFIER or ident_str.startswith(
                    SERVICE_DEVICE_IDENTIFIER_PREFIX
                ):
                    return True
            return False

        def _canonical_identifier(device: Any) -> str | None:
            serial = getattr(device, "serial_number", None)
            if isinstance(serial, str) and serial:
                return serial

            identifiers: set[tuple[str, str]] = (
                getattr(device, "identifiers", set()) or set()
            )
            for domain, ident in identifiers:
                if domain != DOMAIN:
                    continue

                ident_str = str(ident)
                if ident_str == LEGACY_SERVICE_IDENTIFIER or ident_str.startswith(
                    SERVICE_DEVICE_IDENTIFIER_PREFIX
                ):
                    continue

                # Strip the namespace of whichever of our entries the
                # identifier carries, not only the chosen token owner's: below
                # Core 2026.8 a shared device can keep the identifier of a
                # disabled owner while the link is signed for an enabled one,
                # and the map view looks the bare device id up.
                candidate, sep, remainder = ident_str.partition(":")
                if sep and candidate in entries_by_id:
                    ident_str = remainder

                return ident_str

            return None

        dev_reg = dr.async_get(hass)
        updated_count = 0
        resolved_count = 0
        # (device, owner_entry_id, canonical_id) for every resolved device whose
        # entry has Map View enabled. Populated by the first pass below and
        # consumed by the second, after the base-URL lookup.
        enabled_items: list[tuple[Any, str, str]] = []
        # Ask per entry instead of scanning the whole registry. Two things
        # change with that, both deliberate. The owning entry is now read from
        # the query rather than guessed from the device: the previous loop took
        # the first of ``config_entries`` that was ours and otherwise the first
        # one at all, so a device owned by a foreign integration could be given
        # one of our map URLs signed with a token seeded from that foreign entry
        # id. And a device that carries our identifier while belonging to no
        # GoogleFindMy entry is no longer touched at all; it is registry
        # residue, not one of our devices. ``async_entries_for_config_entry``
        # exists unchanged at tag ``2025.9.1`` and at ``2026.9.0``, so this
        # needs no capability switch.
        #
        # This pass also clears disabled entries' stale links before the
        # base-URL lookup (mirrors ``_async_refresh_device_urls``); enabled
        # entries' devices are only collected here, their URL built below
        # once ``base_url`` is known.
        # Below 2026.8 a device can hang on two of our entries at once. The
        # owners are collected per device first, in the order the entries are
        # asked; an owner with Map View enabled wins, so a disabled owner asked
        # first cannot clear the link of an owner that still serves the map.
        # Among equals the first entry asked wins (set iteration order decided
        # this before as well, so only determinism changes); it seeds the map
        # token, while ``_canonical_identifier`` strips any of our entries'
        # namespaces from the device id.
        owners_by_device: dict[str, tuple[Any, list[str]]] = {}
        for owner_entry_id in entries_by_id:
            for device in dr.async_entries_for_config_entry(dev_reg, owner_entry_id):
                slot = owners_by_device.setdefault(device.id, (device, []))
                slot[1].append(owner_entry_id)

        for device, owner_ids in owners_by_device.values():
            identifiers: set[tuple[str, str]] = (
                getattr(device, "identifiers", set()) or set()
            )
            if not any(domain == DOMAIN for domain, _ in identifiers):
                continue

            if _device_is_service(device):
                continue

            owner_entry_id = next(
                (oid for oid in owner_ids if _map_view_enabled(oid)), owner_ids[0]
            )
            canonical_id = _canonical_identifier(device)
            if not canonical_id:
                continue

            resolved_count += 1

            if not _map_view_enabled(owner_entry_id):
                # Disabled: clear any stale link rather than refresh one that
                # just refuses every request. Count only a real change; an
                # already-empty link is not written.
                if device.configuration_url is not None:
                    dev_reg.async_update_device(
                        device_id=device.id, configuration_url=None
                    )
                    updated_count += 1
                continue

            enabled_items.append((device, owner_entry_id, canonical_id))

        if resolved_count and not enabled_items:
            # Every resolved device is on a disabled entry, so skip the
            # base-URL lookup entirely.
            _LOGGER.info("Refreshed URLs for %d Google Find My devices", updated_count)
            return

        try:
            base_url = get_url(
                hass,
                prefer_external=True,
                allow_cloud=True,
                allow_internal=True,
            )
        except (HomeAssistantError, NoURLAvailableError) as err:
            _LOGGER.warning(
                "Skipping configuration URL refresh; no reachable URL available: %s",
                err,
            )
            return

        if not base_url:
            _LOGGER.warning(
                "Skipping configuration URL refresh; no reachable URL available",
            )
            return

        try:
            internal_url = get_url(
                hass,
                allow_external=False,
                allow_cloud=False,
                allow_internal=True,
            )
        except (HomeAssistantError, NoURLAvailableError):
            internal_url = None
        if base_url.rstrip("/") == (internal_url or "").rstrip("/"):
            _LOGGER.info(
                "Using internal URL for map view links; "
                "set an external URL in Home Assistant settings for remote access",
            )

        for device, owner_entry_id, canonical_id in enabled_items:
            auth_token = _token_for_entry(owner_entry_id)
            new_config_url = (
                f"{base_url}/api/googlefindmy/map/{canonical_id}?token={auth_token}"
            )
            dev_reg.async_update_device(
                device_id=device.id,
                configuration_url=new_config_url,
            )
            updated_count += 1
            if ctx.get("redact_url_token"):
                _LOGGER.debug(
                    "Updated URL for device %s: %s",
                    device.name_by_user or device.name,
                    ctx["redact_url_token"](new_config_url),
                )

        _LOGGER.info("Refreshed URLs for %d Google Find My devices", updated_count)

    async def async_rebuild_device_registry_service(call: ServiceCall) -> None:
        """Run the two-phase Device Registry rebuild + cleanup workflow."""

        await async_rebuild_device_registry(hass, call)

    async def async_rebuild_registry_service(call: ServiceCall) -> None:
        """Handle the service call to migrate or rebuild the integration."""

        mode = str(call.data.get("mode") or "rebuild").lower()
        if mode not in {"rebuild", "migrate"}:
            _LOGGER.warning(
                "Unsupported rebuild_registry mode '%s'; defaulting to rebuild", mode
            )
            mode = "rebuild"

        entry_ids_from_service = call.data.get(ATTR_ENTRY_ID)
        if isinstance(entry_ids_from_service, str):
            provided_entry_ids: list[str] = [entry_ids_from_service]
        elif isinstance(entry_ids_from_service, Iterable):
            provided_entry_ids = [str(item) for item in entry_ids_from_service]
        elif entry_ids_from_service is None:
            provided_entry_ids = []
        else:
            _LOGGER.warning(
                "Invalid %s payload type: %s",
                ATTR_ENTRY_ID,
                type(entry_ids_from_service),
            )
            return

        device_ids_raw = call.data.get("device_ids")
        if isinstance(device_ids_raw, str):
            requested_device_ids: list[str] = [device_ids_raw]
        elif isinstance(device_ids_raw, Iterable):
            requested_device_ids = [str(item) for item in device_ids_raw]
        elif device_ids_raw is None:
            requested_device_ids = []
        else:
            _LOGGER.warning("Invalid device_ids payload type: %s", type(device_ids_raw))
            return

        entries = hass.config_entries.async_entries(DOMAIN)
        entry_map: dict[str, Any] = {entry.entry_id: entry for entry in entries}
        target_entry_ids: list[str] = []

        def _extend_target_entries(entry_ids: Iterable[str]) -> None:
            for candidate in entry_ids:
                if candidate in entry_map and candidate not in target_entry_ids:
                    target_entry_ids.append(candidate)

        _extend_target_entries(provided_entry_ids)

        if provided_entry_ids and not target_entry_ids:
            _LOGGER.warning(
                "No valid config entries found for IDs: %s", provided_entry_ids
            )
            return

        if requested_device_ids:
            dev_reg = dr.async_get(hass)
            missing_devices: list[str] = []
            for device_id in requested_device_ids:
                getter = getattr(dev_reg, "async_get", None)
                device = getter(device_id) if callable(getter) else None
                if device is None:
                    missing_devices.append(device_id)
                    continue

                _extend_target_entries(device_owning_entry_ids(device))

            if missing_devices:
                _LOGGER.warning(
                    "No device registry entries found for device_ids: %s",
                    missing_devices,
                )

            if requested_device_ids and not target_entry_ids:
                _LOGGER.warning(
                    "No config entries available for the requested device_ids: %s",
                    requested_device_ids,
                )
                return

        if not target_entry_ids:
            if not entries:
                _LOGGER.warning("No config entries available to reload.")
                return

            target_entry_ids.append(entries[0].entry_id)

        if mode == "migrate":
            migration_log_context = (
                f"Running migration helpers for config entry: {target_entry_ids[0]}"
                if len(target_entry_ids) == 1
                else f"Running migration helpers for config entries: {target_entry_ids}"
            )
            _LOGGER.info(migration_log_context)
            migration_steps = (
                ctx.get("soft_migrate_entry"),
                ctx.get("migrate_unique_ids"),
                ctx.get("relink_button_devices"),
                ctx.get("relink_subentry_entities"),
            )

            for entry_id in target_entry_ids:
                entry = entry_map.get(entry_id)
                if entry is None:
                    _LOGGER.warning(
                        "Config entry %s no longer available; skipping", entry_id
                    )
                    continue

                for step in migration_steps:
                    if not callable(step):
                        continue
                    try:
                        await step(hass, entry)
                    except Exception as err:  # noqa: BLE001 - defensive logging
                        _LOGGER.error(
                            "Error running migration step %s for entry %s: %s",
                            getattr(step, "__name__", step),
                            entry_id,
                            err,
                        )
        reload_log_context = (
            f"Reloading config entry: {target_entry_ids[0]}"
            if len(target_entry_ids) == 1
            else f"Reloading config entries: {target_entry_ids}"
        )

        _LOGGER.info(reload_log_context)

        # The rebuild mode removes entities and devices before the setup
        # (services.yaml). Unloading keeps registry entries, so the purge runs
        # here, between unload and setup, as a reload would order them.
        purge_entry = ctx.get("purge_entry_registrations")
        if mode == "rebuild" and not callable(purge_entry):
            _LOGGER.warning(
                "Registry purge unavailable; reloading without removing entities"
            )

        async def _rebuild_entry(
            entry_id: str,
            purge: Callable[[HomeAssistant, Any], Awaitable[tuple[int, int]]],
        ) -> None:
            if not await hass.config_entries.async_unload(entry_id):
                _LOGGER.error(
                    "Config entry %s could not be unloaded; registry entries kept",
                    entry_id,
                )
                return
            entry = hass.config_entries.async_get_entry(entry_id)
            if entry is None:
                return
            try:
                removed_entities, removed_devices = await purge(hass, entry)
            except Exception as err:  # noqa: BLE001 - the entry is unloaded already
                # The unload has happened, so an active entry is set up again
                # whatever the purge did; otherwise it stays unloaded until a
                # manual reload or a restart. Class only (AGENTS.md section 5).
                _LOGGER.error(
                    "Removing the registry entries of config entry %s failed (%s); "
                    "setting it up again",
                    entry_id,
                    type(err).__name__,
                )
                _LOGGER.debug(
                    "Purge error detail for config entry %s: %s",
                    entry_id,
                    describe_exception(err),
                )
            else:
                _LOGGER.info(
                    "Removed %d entities and %d devices of config entry %s",
                    removed_entities,
                    removed_devices,
                    entry_id,
                )
            if getattr(entry, "disabled_by", None):
                return
            await hass.config_entries.async_setup(entry_id)

        for entry_id in target_entry_ids:
            try:
                if mode == "rebuild" and callable(purge_entry):
                    await _rebuild_entry(entry_id, purge_entry)
                else:
                    await hass.config_entries.async_reload(entry_id)
            except Exception as err:
                _LOGGER.error("Error reloading config entry %s: %s", entry_id, err)

    beacon_lock = asyncio.Lock()

    async def async_read_beacon_state_service(call: ServiceCall) -> dict[str, Any]:
        """Read a tracker's FMDN state over GATT and return it to the caller."""
        address: str | None = call.data.get("address")
        if address is not None:
            address = address.strip().upper()
            if not _BLE_ADDRESS_PATTERN.fullmatch(address):
                raise _service_validation_error(
                    "The address is not a valid Bluetooth address.",
                    translation_key="beacon_invalid_address",
                )
        if "bluetooth" not in hass.config.components:
            raise _service_validation_error(
                "The Bluetooth integration is not loaded.",
                translation_key="beacon_bluetooth_unavailable",
            )
        if beacon_lock.locked():
            raise _service_validation_error(
                "Another beacon diagnostic is running.",
                translation_key="beacon_diagnostic_busy",
            )
        async with beacon_lock:
            runtime, canonical_id = await _resolve_runtime_for_device_id(
                str(call.data["device_id"])
            )
            keys = await _async_beacon_keys(hass, runtime, canonical_id)
            return await async_probe_tracker(
                hass, keys, address=address, timeout=call.data["timeout"]
            )

    # ---- Actual service registrations (global; visible even without entries) ----
    # NOTE: Field validation lives in the handlers; services.yaml only
    # describes the fields for the UI. The maintenance actions pass
    # _ADMIN_PASSTHROUGH_SCHEMA only because the admin helper would otherwise
    # reject every field.
    hass.services.async_register(
        DOMAIN, SERVICE_LOCATE_DEVICE, async_locate_device_service
    )
    hass.services.async_register(
        DOMAIN, SERVICE_LOCATE_EXTERNAL, async_locate_external_service
    )
    hass.services.async_register(DOMAIN, SERVICE_PLAY_SOUND, async_play_sound_service)
    hass.services.async_register(DOMAIN, SERVICE_STOP_SOUND, async_stop_sound_service)
    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_REFRESH_DEVICE_URLS,
        async_refresh_device_urls_service,
        _ADMIN_PASSTHROUGH_SCHEMA,
    )
    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_REBUILD_DEVICE_REGISTRY,
        async_rebuild_device_registry_service,
        _ADMIN_PASSTHROUGH_SCHEMA,
    )
    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_REBUILD_REGISTRY,
        async_rebuild_registry_service,
        _ADMIN_PASSTHROUGH_SCHEMA,
    )
    async_register_admin_service(
        hass,
        DOMAIN,
        SERVICE_READ_BEACON_STATE,
        async_read_beacon_state_service,
        _READ_BEACON_STATE_SCHEMA,
        supports_response=SupportsResponse.ONLY,
    )
