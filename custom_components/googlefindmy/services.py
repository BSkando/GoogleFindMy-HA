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

import logging
import time
from collections.abc import Iterable, Mapping
from typing import Any

from homeassistant.components.device_tracker import DOMAIN as DEVICE_TRACKER_DOMAIN
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError, ServiceValidationError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.network import NoURLAvailableError, get_url

try:  # Home Assistant 2025.5+: attribute constant exposed
    from homeassistant.const import ATTR_ENTRY_ID
except ImportError:  # pragma: no cover - forward compatibility for HA < 2025.5
    ATTR_ENTRY_ID = "entry_id"

from .const import (
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
    map_token_hex_digest,
    map_token_secret_seed,
    service_device_identifier,
)
from .device_registry_helpers import async_get_device_by_identifier_compat

_LOGGER = logging.getLogger(__name__)

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

    def _detach_hub_link_from_device(
        device_id: str,
        *,
        context_label: str,
        error_log_template: str,
    ) -> bool:
        """Remove the hub link from ``device_id`` with legacy kwarg support."""

        try:
            dev_reg.async_update_device(
                device_id,
                remove_config_entry_id=entry_id,
                remove_config_subentry_id=None,
            )
        except TypeError as err:
            if "remove_config_subentry_id" not in str(err):
                _LOGGER.error(error_log_template, entry_id, device_id, err)
                return False
            _LOGGER.debug(
                "[%s] Hub Cleanup: Retrying device registry update for %s %s without remove_config_subentry_id after %s",
                entry_id,
                context_label,
                device_id,
                err,
            )
            try:
                dev_reg.async_update_device(
                    device_id,
                    remove_config_entry_id=entry_id,
                )
            except Exception as retry_err:  # pragma: no cover - defensive guard
                _LOGGER.error(error_log_template, entry_id, device_id, retry_err)
                return False
            return True
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
        """Return normalized subentry identifiers for ``target_entry_id``."""

        if not isinstance(target_entry_id, str) or not target_entry_id:
            return set()

        mapping_obj = getattr(device, "config_entries_subentries", None)
        normalized: set[str | None] = set()
        if isinstance(mapping_obj, Mapping):
            raw_links = mapping_obj.get(target_entry_id)
            if isinstance(raw_links, str):
                normalized.add(raw_links)
            elif isinstance(raw_links, Iterable) and not isinstance(
                raw_links, (str, bytes)
            ):
                for candidate in raw_links:
                    if isinstance(candidate, str):
                        normalized.add(candidate)
                    elif candidate is None:
                        normalized.add(None)
            elif raw_links is None and target_entry_id in mapping_obj:
                normalized.add(None)

        if not normalized:
            fallback = getattr(device, "config_subentry_id", None)
            if isinstance(fallback, str):
                normalized.add(fallback)
            elif fallback is None:
                linked_entries = getattr(device, "config_entries", None)
                if isinstance(linked_entries, Iterable):
                    for candidate_entry_id in linked_entries:
                        if (
                            isinstance(candidate_entry_id, str)
                            and candidate_entry_id == target_entry_id
                        ):
                            normalized.add(None)
                            break

        return normalized

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
        _LOGGER.info("[%s] Hub Cleanup: Processing entry '%s'", entry_id, entry.title)

        # 1. Find the correct Service Device ID
        service_device_ident = service_device_identifier(entry_id)
        service_device = async_get_device_by_identifier_compat(
            dev_reg,
            service_device_ident,
            config_entry_id=entry_id,
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
                service_device = async_get_device_by_identifier_compat(
                    dev_reg,
                    service_device_ident,
                    config_entry_id=entry_id,
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
        cleaned_devices_entry = 1 if service_cleanup_applied else 0

        for device in devices_for_entry:
            if device is None or not hasattr(device, "id"):
                continue

            # Skip the Service Device itself
            if device.id == service_device_id:
                continue

            # Check if the device is correctly linked to the tracker subentry
            tracker_linked_entry_ids: set[str] = set()
            device_subentry_mapping = getattr(device, "config_entries_subentries", None)
            if isinstance(device_subentry_mapping, Mapping):
                for (
                    mapped_entry_id,
                    mapped_subentries,
                ) in device_subentry_mapping.items():
                    normalized_entry_id = str(mapped_entry_id)
                    if not normalized_entry_id:
                        continue

                    normalized_subentries: set[str] = set()
                    if isinstance(mapped_subentries, str):
                        if mapped_subentries:
                            normalized_subentries = {mapped_subentries}
                    elif isinstance(mapped_subentries, Iterable):
                        normalized_subentries = {
                            candidate
                            for candidate in mapped_subentries
                            if isinstance(candidate, str) and candidate
                        }

                    if correct_tracker_subentry_id in normalized_subentries:
                        tracker_linked_entry_ids.add(normalized_entry_id)

            raw_links: set[str] = getattr(device, "config_entries", set()) or set()
            linked_entry_ids = {
                str(link_entry_id)
                for link_entry_id in raw_links
                if isinstance(link_entry_id, str) and link_entry_id
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
                context_label="device",
                error_log_template="[%s] Hub Cleanup: Failed to detach hub entry from device %s: %s",
            ):
                cleaned_devices_entry += 1

        if cleaned_devices_entry > 0:
            _LOGGER.info(
                "[%s] Hub Cleanup: Removed %d orphaned device links.",
                entry_id,
                cleaned_devices_entry,
            )
            cleaned_devices_total += cleaned_devices_entry

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
        - "coalesce_account_entries": Callable[[HomeAssistant, ConfigEntry], Awaitable[ConfigEntry]]
        - "extract_normalized_email": Callable[[ConfigEntry], str | None]
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
            for entry_id in dev.config_entries:
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
            ok = await runtime.coordinator.async_play_sound(canonical_id)
            if not ok:
                placeholders = {"device_id": str(raw_device_id)}
                raise _service_validation_error(
                    "Play sound suppressed for device '{device_id}'".format(
                        **placeholders
                    ),
                    translation_key="play_sound_failed",
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
                translation_key="stop_sound_failed",
                translation_placeholders=placeholders,
            )
        request_uuid: str | None
        if request_uuid_raw is None:
            request_uuid = None
        else:
            request_uuid = request_uuid_raw
        try:
            runtime, canonical_id = await _resolve_runtime_for_device_id(raw_device_id)
            ok = await runtime.coordinator.async_stop_sound(canonical_id, request_uuid)
            if not ok:
                placeholders = {"device_id": str(raw_device_id)}
                raise _service_validation_error(
                    "Stop sound suppressed for device '{device_id}'".format(
                        **placeholders
                    ),
                    translation_key="stop_sound_failed",
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
        """
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

        expiration_cache: dict[str, bool] = {}
        token_cache: dict[str, str] = {}
        ha_uuid = str(hass.data.get("core.uuid", "ha"))
        now = int(time.time())

        def _expiration_enabled(entry_id: str | None) -> bool:
            cache_key = entry_id or ""
            if cache_key in expiration_cache:
                return expiration_cache[cache_key]

            entry = entries_by_id.get(entry_id) if entry_id else None
            enabled = default_expiration

            if entry:
                if callable(opt_reader):
                    try:
                        enabled = bool(opt_reader(entry, opt_key, default_expiration))
                    except Exception:
                        enabled = bool(
                            entry.options.get(
                                opt_key,
                                entry.data.get(opt_key, default_expiration),
                            )
                        )
                else:
                    enabled = bool(
                        entry.options.get(
                            opt_key,
                            entry.data.get(opt_key, default_expiration),
                        )
                    )

            expiration_cache[cache_key] = bool(enabled)
            return expiration_cache[cache_key]

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

        def _canonical_identifier(device: Any, entry_id: str | None) -> str | None:
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

                if entry_id:
                    prefix = f"{entry_id}:"
                    if ident_str.startswith(prefix):
                        ident_str = ident_str[len(prefix) :]
                elif ":" in ident_str:
                    candidate, remainder = ident_str.split(":", 1)
                    if candidate in entries_by_id:
                        ident_str = remainder

                return ident_str

            return None

        dev_reg = dr.async_get(hass)
        updated_count = 0
        for device in getattr(dev_reg, "devices", {}).values():
            identifiers: set[tuple[str, str]] = (
                getattr(device, "identifiers", set()) or set()
            )
            if not any(domain == DOMAIN for domain, _ in identifiers):
                continue

            if _device_is_service(device):
                continue

            config_entry_ids = list(getattr(device, "config_entries", None) or [])
            owner_entry_id: str | None = None
            for candidate in config_entry_ids:
                candidate_str = str(candidate)
                if candidate_str in entries_by_id:
                    owner_entry_id = candidate_str
                    break
                if owner_entry_id is None:
                    owner_entry_id = candidate_str

            canonical_id = _canonical_identifier(device, owner_entry_id)
            if not canonical_id:
                continue

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

                config_entries = getattr(device, "config_entries", None) or []
                _extend_target_entries(str(entry_id) for entry_id in config_entries)

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
        for entry_id in target_entry_ids:
            try:
                await hass.config_entries.async_reload(entry_id)
            except Exception as err:
                _LOGGER.error("Error reloading config entry %s: %s", entry_id, err)

    # ---- Actual service registrations (global; visible even without entries) ----
    # NOTE: We intentionally do NOT pass voluptuous schemas here; services.yaml is our SSoT.
    hass.services.async_register(
        DOMAIN, SERVICE_LOCATE_DEVICE, async_locate_device_service
    )
    hass.services.async_register(
        DOMAIN, SERVICE_LOCATE_EXTERNAL, async_locate_external_service
    )
    hass.services.async_register(DOMAIN, SERVICE_PLAY_SOUND, async_play_sound_service)
    hass.services.async_register(DOMAIN, SERVICE_STOP_SOUND, async_stop_sound_service)
    hass.services.async_register(
        DOMAIN, SERVICE_REFRESH_DEVICE_URLS, async_refresh_device_urls_service
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_REBUILD_DEVICE_REGISTRY,
        async_rebuild_device_registry_service,
    )
    hass.services.async_register(
        DOMAIN, SERVICE_REBUILD_REGISTRY, async_rebuild_registry_service
    )
