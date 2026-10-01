"""Device registry operations for GoogleFindMyCoordinator.

This module contains registry-related methods extracted from main.py
during Phase 2 of the refactoring.

Methods moved here:
- _call_device_registry_api: Core registry call with compatibility handling
- _device_registry_kwargs_need_legacy_retry: Legacy kwarg detection
- _device_registry_build_legacy_kwargs: Legacy kwarg translation
- _device_registry_config_subentry_kwarg_name: Subentry kwarg detection
- _device_registry_allows_translation_update: Translation support check
- _reindex_poll_targets_from_device_registry: Rebuild poll target sets
- _extract_our_identifier: Extract device identifier from registry
- _sync_owner_index: Sync hass.data owner index for FCM fallback
- _ensure_device_name_cache: Lazy device-name cache initialization
- _apply_pending_via_updates: Deprecated no-op (backward compat)
- _device_display_name: Get device name without sensitive data
- _entry_id: Get bound ConfigEntry ID
- _config_entry_exists: Check if config entry is registered
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from types import SimpleNamespace
from typing import Any, cast

from homeassistant.components.device_tracker import DOMAIN as DEVICE_TRACKER_DOMAIN
from homeassistant.config_entries import (
    ConfigEntry,
    UnknownEntry,
    UnknownSubEntry,
)
from homeassistant.core import callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.entity_registry import RegistryEntry as EntityRegistryEntry

from ..const import (
    DOMAIN,
    INTEGRATION_VERSION,
    LEGACY_SERVICE_IDENTIFIER,
    SERVICE_DEVICE_IDENTIFIER_PREFIX,
    SERVICE_DEVICE_MANUFACTURER,
    SERVICE_DEVICE_MODEL,
    SERVICE_DEVICE_TRANSLATION_KEY,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_HUB,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_SUBENTRY_KEY,
    service_device_identifier,
)
from ..device_registry_helpers import (
    async_get_device_by_identifier_compat,
    async_get_device_by_identifiers_compat,
)
from ._mixin_typing import _MixinBase
from .helpers.registry import (
    build_canonical_unique_id as _build_canonical_unique_id_impl,
)
from .helpers.registry import (
    build_entity_unique_id_candidates as _build_entity_unique_id_candidates_impl,
)
from .helpers.registry import (
    build_legacy_device_registry_kwargs as _build_legacy_kwargs_impl,
)
from .helpers.registry import (
    extract_canonical_device_id as _extract_canonical_device_id_impl,
)
from .helpers.registry import (
    extract_device_display_name as _extract_display_name_impl,
)
from .helpers.registry import (
    extract_service_subentry_ids as _extract_service_subentry_ids_impl,
)
from .helpers.registry import (
    extract_subentry_links as _extract_subentry_links_impl,
)
from .helpers.registry import (
    has_hub_link as _has_hub_link_impl,
)
from .helpers.registry import (
    has_subentry_link as _has_subentry_link_impl,
)
from .helpers.registry import (
    is_hub_device_check as _is_hub_device_check_impl,
)
from .helpers.registry import (
    match_entity_by_device_id as _match_entity_by_device_id_impl,
)
from .helpers.registry import (
    needs_legacy_kwarg_retry as _needs_legacy_retry_impl,
)
from .helpers.registry import (
    normalize_device_name as _normalize_device_name_impl,
)
from .helpers.registry import (
    parse_device_identifier as _parse_identifier_impl,
)
from .helpers.registry import (
    resolve_tracker_subentry_candidate as _resolve_tracker_subentry_impl,
)
from .helpers.registry import (
    should_defer_service_subentry as _should_defer_service_subentry_impl,
)
from .helpers.subentry import (
    sanitize_subentry_identifier as _sanitize_subentry_id_impl,
)

_LOGGER = logging.getLogger(__name__)


class RegistryOperations(_MixinBase):
    """Device registry operations mixin for GoogleFindMyCoordinator.

    This class contains methods that manage device registry entries,
    including creation, updates, and synchronization of HA device registry
    with the Google Find My device list.
    """

    def _call_device_registry_api(
        self,
        call: Callable[..., Any],
        *,
        base_kwargs: Mapping[str, Any] | None = None,
    ) -> Any:
        """Call a device registry API, handling keyword compatibility."""

        kwargs = dict(base_kwargs or {})
        if "config_subentry_id" in kwargs:
            replacement = self._device_registry_config_subentry_kwarg_name(call)
            if replacement is None:
                kwargs.pop("config_subentry_id")
            elif replacement != "config_subentry_id":
                kwargs[replacement] = kwargs.pop("config_subentry_id")

        try:
            return call(**kwargs)
        except TypeError as err:
            if not self._device_registry_kwargs_need_legacy_retry(call, err, kwargs):
                raise

            legacy_kwargs = self._device_registry_build_legacy_kwargs(kwargs)
            _LOGGER.debug(
                "Retrying device registry call %s with legacy keyword arguments after %s",
                getattr(call, "__qualname__", repr(call)),
                err,
            )
            return call(**legacy_kwargs)
        except (UnknownEntry, UnknownSubEntry) as err:
            kwarg_name = self._device_registry_config_subentry_kwarg_name(call)
            if (
                kwarg_name == "add_config_subentry_id"
                or "config_subentry_id" not in kwargs
            ):
                raise
            _LOGGER.debug(
                "Device registry call %s rejected config_subentry_id (%s); retrying without it",
                getattr(call, "__qualname__", repr(call)),
                err,
            )
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("config_subentry_id", None)
            return call(**fallback_kwargs)

    def _device_registry_kwargs_need_legacy_retry(
        self,
        call: Callable[..., Any],
        err: TypeError,
        kwargs: Mapping[str, Any],
    ) -> bool:
        """Return True when ``kwargs`` must be rewritten for legacy cores."""
        kwarg_name = self._device_registry_config_subentry_kwarg_name(call)
        return _needs_legacy_retry_impl(kwarg_name, str(err), kwargs)

    @staticmethod
    def _device_registry_build_legacy_kwargs(
        kwargs: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Translate modern device-registry kwargs to their legacy names."""
        return _build_legacy_kwargs_impl(kwargs)

    def _device_registry_config_subentry_kwarg_name(
        self, call: Callable[..., Any]
    ) -> str | None:
        """Return the config-subentry kwarg name accepted by ``call``.

        Home Assistant 2025.11 renamed the ``async_update_device`` keyword from
        ``config_subentry_id`` to ``add_config_subentry_id``. Earlier versions still
        expect ``config_subentry_id``. This helper inspects the callable signature
        and returns the supported keyword, caching the result for reuse.
        """

        cache_attr = "_device_registry_config_subentry_kwarg_cache"
        cache_obj = getattr(self, cache_attr, None)
        cache: dict[Callable[..., Any], str | None]
        if isinstance(cache_obj, dict):
            cache = cast(dict[Callable[..., Any], str | None], cache_obj)
        else:
            cache = cast(dict[Callable[..., Any], str | None], {})
            setattr(self, cache_attr, cache)

        func = getattr(call, "__func__", call)
        if func in cache:
            return cache[func]

        try:
            signature = inspect.signature(call)
        except (TypeError, ValueError):  # pragma: no cover - defensive fallback
            kwarg_name: str | None = None
        else:
            parameters = signature.parameters
            if "config_subentry_id" in parameters:
                kwarg_name = "config_subentry_id"
            elif "add_config_subentry_id" in parameters:
                kwarg_name = "add_config_subentry_id"
            elif any(
                param.kind is inspect.Parameter.VAR_KEYWORD
                for param in parameters.values()
            ):
                kwarg_name = "config_subentry_id"
            else:
                kwarg_name = None

        cache[func] = kwarg_name
        return kwarg_name

    def _device_registry_allows_translation_update(self, dev_reg: Any) -> bool:
        """Return True if the registry accepts translation metadata during updates."""

        cached = getattr(self, "_device_registry_supports_translation_update", None)
        if isinstance(cached, bool):
            return cached

        update_helper = getattr(dev_reg, "async_update_device", None)
        supports_translation = False
        if callable(update_helper):
            try:
                signature = inspect.signature(update_helper)
            except (TypeError, ValueError):
                supports_translation = False
            else:
                params = signature.parameters
                supports_translation = (
                    "translation_key" in params and "translation_placeholders" in params
                )

        setattr(
            self, "_device_registry_supports_translation_update", supports_translation
        )
        return supports_translation

    @callback  # type: ignore[misc, untyped-decorator, unused-ignore]
    def _reindex_poll_targets_from_device_registry(
        self,
    ) -> None:
        """Rebuild internal poll target sets from registries (fast, robust, diagnostics-aware).

        Semantics:
        - Consider ONLY devices that belong to THIS config entry (no global scan).
        - A device is "present" if we can extract a valid (DOMAIN, identifier).
        - A device is "enabled for polling" if there is at least one ENABLED
          `device_tracker` entity for our domain on that device AND the device
          itself is not disabled. This preserves the entities-driven polling
          selection and reduces UI churning.

        Multi-account safety:
        - Uses entry-scoped identifiers in the Device Registry:
              (DOMAIN, f"{entry_id}:{device_id}")
          and gracefully accepts legacy identifiers `(DOMAIN, device_id)`.
        """
        dev_reg = dr.async_get(self.hass)
        ent_reg = er.async_get(self.hass)
        entry_id = self._entry_id()

        if not entry_id:
            self._devices_with_entry = set()
            self._enabled_poll_device_ids = set()
            _LOGGER.debug("Skipping DR reindex: no config_entry bound yet")
            return

        # Limit to our integration's devices/entities: avoids interference & improves performance.
        devices_for_entry = dr.async_entries_for_config_entry(dev_reg, entry_id)
        entities_for_entry = er.async_entries_for_config_entry(ent_reg, entry_id)

        present: set[str] = set()
        enabled: set[str] = set()

        # Map device_id -> has_enabled_tracker_entity
        has_enabled_tracker: dict[str, bool] = {}
        for ent in entities_for_entry:
            # We only care about our domain and enabled entities
            if ent.platform != DOMAIN or ent.disabled_by is not None:
                continue
            # Only trackers drive polling
            if ent.domain == "device_tracker" and ent.device_id:
                has_enabled_tracker[ent.device_id] = True

        for dev in devices_for_entry:
            ident = self._extract_our_identifier(dev)
            if not ident:
                continue
            present.add(ident)
            if dev.id in has_enabled_tracker and dev.disabled_by is None:
                enabled.add(ident)

        self._devices_with_entry = present
        self._enabled_poll_device_ids = enabled

        # Update subentry metadata since enabled/present sets may affect visibility
        self._refresh_subentry_index()

        _LOGGER.debug(
            "Reindexed targets for entry %s: %d present / %d enabled (entities-driven)",
            entry_id,
            len(present),
            len(enabled),
        )
        self._schedule_eid_resolver_refresh()

    def _extract_our_identifier(self, device: dr.DeviceEntry) -> str | None:
        """Return the first valid (DOMAIN, identifier) from a device, else None.

        Multi-account compatibility:
        - Since 2025.5+ we use **entry-scoped device identifiers** in the Device Registry
          to guarantee global uniqueness across multiple accounts:
              (DOMAIN, f\"{entry_id}:{device_id}\")
        - For backward compatibility we also recognize legacy identifiers:
              (DOMAIN, device_id)

        This helper:
        * Extracts our identifier
        * If it has the namespaced form, it returns the **raw device_id** part
          (the coordinator uses canonical device IDs internally).
        * If malformed tuples are encountered, it logs once and records a diagnostics warning.
        """
        entry_id = self._entry_id()
        for item in device.identifiers:
            result = _parse_identifier_impl(
                item,
                DOMAIN,
                entry_id,
                SERVICE_DEVICE_IDENTIFIER_PREFIX,
                LEGACY_SERVICE_IDENTIFIER,
            )
            if result is not None:
                return result
        return None

    def _sync_owner_index(self, devices: list[dict[str, Any]] | None) -> None:
        """Sync hass.data owner index for this entry (FCM fallback support)."""
        hass = getattr(self, "hass", None)
        entry_id = self._entry_id()
        if hass is None or not entry_id:
            return

        try:
            # hass.data[DOMAIN] is compatible with HassKey-based DATA_DOMAIN in __init__.
            bucket = hass.data.setdefault(DOMAIN, {})
            owner_index: dict[str, str] = bucket.setdefault("device_owner_index", {})
        except Exception as err:  # noqa: BLE001 - defensive guard
            _LOGGER.debug(
                "[entry=%s] Owner-index sync skipped: %s",
                entry_id,
                err,
            )
            return

        seen: set[str] = set()
        for device in devices or []:
            canonical = (
                device.get("canonicalId")
                or device.get("canonical_id")
                or device.get("id")
                or device.get("device_id")
            )
            if canonical is None:
                continue
            if not isinstance(canonical, str):
                canonical = str(canonical)
            canonical = canonical.strip()
            if not canonical:
                continue

            # Do not overwrite an existing routing entry from another account.
            # Shared devices may appear under multiple entries; keep the first
            # registration so FCM messages route to the primary owner instead
            # of the most recently loaded account.
            if canonical not in owner_index or owner_index[canonical] == entry_id:
                owner_index[canonical] = entry_id
                seen.add(canonical)

        if owner_index:
            stale = [
                cid
                for cid, eid in list(owner_index.items())
                if eid == entry_id and cid not in seen
            ]
            for cid in stale:
                owner_index.pop(cid, None)
            if stale:
                _LOGGER.debug(
                    "[entry=%s] Pruned %d stale owner-index entries",
                    entry_id,
                    len(stale),
                )

    def _ensure_device_name_cache(
        self,
    ) -> dict[str, str]:
        """Return the lazily initialized device-name cache."""
        cache = getattr(self, "_device_names", None)
        if cache is None:
            cache = {}
            setattr(self, "_device_names", cache)
        return cache

    def _apply_pending_via_updates(self) -> None:
        """Deprecated no-op retained for backward compatibility."""
        # Tracker devices no longer link to the service device via ``via_device``.
        # Keep the method defined to avoid AttributeError in case third-party
        # callers relied on the old behavior, but return immediately.
        return

    def _device_display_name(self, dev: dr.DeviceEntry, fallback: str) -> str:
        """Return the best human-friendly device name without sensitive data."""
        return _extract_display_name_impl(dev.name_by_user, dev.name, fallback)

    def _entry_id(self) -> str | None:
        """Small helper to read the bound ConfigEntry ID (None at very early startup)."""
        entry = getattr(self, "config_entry", None)
        return getattr(entry, "entry_id", None)

    def _config_entry_exists(self, entry_id: str | None = None) -> bool:
        """Return True when the coordinator's entry is still registered."""
        hass = getattr(self, "hass", None)
        config_entries = getattr(hass, "config_entries", None)
        if entry_id is None:
            entry_id = self._entry_id()

        if entry_id is None:
            return False

        getter = getattr(config_entries, "async_get_entry", None)
        if callable(getter):
            try:
                return getter(entry_id) is not None
            except Exception:  # pragma: no cover - defensive guard
                return True

        return True

    def _redact_text(self, value: str | None, max_len: int = 120) -> str:
        """Return a short, redacted string variant suitable for logs/diagnostics."""
        if not value:
            return ""
        s = str(value)
        return s if len(s) <= max_len else (s[:max_len] + "…")

    def _ensure_service_device_exists(self, entry: ConfigEntry | None = None) -> None:
        """Idempotently create/update the per-entry 'service device' in the device registry.

        This keeps diagnostic entities (e.g. polling/auth-status) grouped under a stable
        integration-level device. Safe to call multiple times.

        Subentry fallback rules and the rationale for recording the service
        ``config_subentry_id`` are documented in ``docs/CONFIG_SUBENTRIES_HANDBOOK.md``;
        consult the handbook before changing identifier selection to avoid regressing
        tracker/service separation.
        """
        # Resolve hass
        hass = getattr(self, "hass", None)
        if hass is None:
            return

        # Resolve ConfigEntry (works with either .entry or .config_entry on the coordinator)
        entry = entry or getattr(self, "entry", None) or self.config_entry
        if entry is None:
            _LOGGER.debug(
                "Service-device ensure skipped: ConfigEntry not available on coordinator."
            )
            return

        entry_id = getattr(entry, "entry_id", None)

        # Refresh subentry metadata to obtain the current service subentry context.
        try:
            self._refresh_subentry_index(skip_manager_update=True, skip_repair=True)
        except Exception:  # pragma: no cover - defensive guard
            pass

        service_meta = self._subentry_metadata.get(SERVICE_SUBENTRY_KEY)

        def _normalize_subentry_id(value: Any) -> str | None:
            return _sanitize_subentry_id_impl(value)

        entry_service_subentry_id = _normalize_subentry_id(
            getattr(entry, "service_subentry_id", None)
        )

        entry_subentries = getattr(entry, "subentries", None)
        service_subentry_ids = _extract_service_subentry_ids_impl(
            entry_subentries,
            entry_service_subentry_id,
            SUBENTRY_TYPE_SERVICE,
            SERVICE_SUBENTRY_KEY,
        )

        def _is_real_service_subentry(candidate: Any) -> str | None:
            """Return candidate when it matches a confirmed service subentry.

            When the config entry lacks recorded subentries (for example, after
            provisional creation), fall back to the coordinator's metadata so the
            service device still records a stable ``config_subentry_id``.
            """

            normalized_candidate = _normalize_subentry_id(candidate)
            if normalized_candidate is None:
                return None

            if entry_service_subentry_id is not None:
                if normalized_candidate != entry_service_subentry_id:
                    return None
                if (
                    service_subentry_ids
                    and normalized_candidate not in service_subentry_ids
                ):
                    return None
                return normalized_candidate

            if service_subentry_ids and normalized_candidate in service_subentry_ids:
                return normalized_candidate

            if not service_subentry_ids:
                return normalized_candidate

            return None

        service_config_subentry_id = None
        meta_identifier: Any | None = None
        if service_meta is not None:
            meta_identifier = getattr(service_meta, "config_subentry_id", None)
        for candidate in (meta_identifier, entry_service_subentry_id):
            resolved = _is_real_service_subentry(candidate)
            if resolved is not None:
                service_config_subentry_id = resolved
                break

        current_subentries = getattr(entry, "subentries", None)
        if _should_defer_service_subentry_impl(
            service_config_subentry_id,
            current_subentries,
            entry_id,
            SERVICE_SUBENTRY_KEY,
        ):
            _LOGGER.debug(
                "[%s] Deferring unknown service config_subentry_id %s until registry catches up",
                entry.entry_id,
                service_config_subentry_id,
            )
            service_config_subentry_id = None
        elif (
            service_config_subentry_id is not None
            and isinstance(current_subentries, Mapping)
            and service_config_subentry_id not in current_subentries
        ):
            # Log when using stable default (not deferred but also not in subentries)
            _LOGGER.debug(
                "[%s] Applying stable default service config_subentry_id %s (registry not ready)",
                entry.entry_id,
                service_config_subentry_id,
            )

        service_subentry_identifier: tuple[str, str] | None = None
        if service_config_subentry_id is not None:
            service_subentry_identifier = (
                DOMAIN,
                f"{entry.entry_id}:{service_config_subentry_id}:service",
            )

        setattr(
            self,
            "_service_device_identifier",
            service_device_identifier(entry.entry_id),
        )

        previous_service_identifier_sentinel = object()
        previous_service_identifier = getattr(
            self,
            "_service_device_last_subentry_identifier",
            previous_service_identifier_sentinel,
        )

        # Fast-path: already ensured in this runtime and the service subentry
        # context has not changed.
        if (
            getattr(self, "_service_device_ready", False)
            and getattr(self, "_service_device_id", None)
            and previous_service_identifier is not previous_service_identifier_sentinel
            and service_subentry_identifier is not None
            and previous_service_identifier == service_subentry_identifier
        ):
            self._apply_pending_via_updates()
            return

        dev_reg = dr.async_get(hass)
        if not hasattr(dev_reg, "async_get_or_create") or not hasattr(
            dev_reg, "async_update_device"
        ):
            _LOGGER.debug(
                "Service-device ensure skipped: registry stub missing create/update APIs."
            )
            return
        identifiers: set[tuple[str, str]] = {
            service_device_identifier(entry.entry_id)
        }  # {(DOMAIN, f"integration_<entry_id>")}
        if service_subentry_identifier is not None:
            identifiers.add(service_subentry_identifier)

        def _service_entry_links(device: Any) -> set[str | None]:
            """Return the set of subentry identifiers linked to ``entry``."""

            if not entry_id:
                return set()

            mapping_obj = getattr(device, "config_entries_subentries", None)
            normalized: set[str | None] = set()
            if isinstance(mapping_obj, Mapping):
                raw_links = mapping_obj.get(entry_id)
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
                elif raw_links is None and entry_id in mapping_obj:
                    normalized.add(None)

            if not normalized:
                fallback = getattr(device, "config_subentry_id", None)
                if isinstance(fallback, str):
                    normalized.add(fallback)
                elif fallback is None and isinstance(
                    getattr(device, "config_entries", None), Iterable
                ):
                    for candidate_entry_id in cast(
                        Iterable[Any], getattr(device, "config_entries", ())
                    ):
                        if (
                            isinstance(candidate_entry_id, str)
                            and candidate_entry_id == entry_id
                        ):
                            normalized.add(None)
                            break

            return normalized

        def _service_has_service_link(device: Any) -> bool:
            if service_config_subentry_id is None:
                return False
            links = _service_entry_links(device)
            return _has_subentry_link_impl(links, service_config_subentry_id)

        def _service_has_hub_link(device: Any) -> bool:
            links = _service_entry_links(device)
            return _has_hub_link_impl(links)

        def _detach_service_hub_link(device: Any) -> Any:
            update_call = getattr(dev_reg, "async_update_device", None)
            if not callable(update_call) or not entry_id:
                return device
            device_id = getattr(device, "id", None)
            if not isinstance(device_id, str) or not device_id:
                return device
            self._call_device_registry_api(
                update_call,
                base_kwargs={
                    "device_id": device_id,
                    "remove_config_entry_id": entry_id,
                    "remove_config_subentry_id": None,
                },
            )
            return _refresh_service_device_entry(device)

        device = async_get_device_by_identifiers_compat(
            dev_reg,
            tuple(identifiers),
            config_entry_id=entry.entry_id,
        )

        def _refresh_service_device_entry(candidate: Any) -> Any:
            """Return a fresh copy of the service device entry when possible."""

            if candidate is None:
                return None

            getter = getattr(dev_reg, "async_get", None)
            device_id = getattr(candidate, "id", None)
            if not callable(getter) or not isinstance(device_id, str) or not device_id:
                return candidate

            try:
                refreshed = getter(device_id)
            except TypeError:
                return candidate

            return candidate if refreshed is None else refreshed

        if (
            device is not None
            and service_config_subentry_id is not None
            and getattr(device, "config_subentry_id", None)
            != service_config_subentry_id
        ):
            device_id = getattr(device, "id", None)
            if isinstance(device_id, str) and device_id:
                _LOGGER.debug(
                    "[%s] Healing service device: correcting config_subentry_id from %s to %s",
                    entry.entry_id,
                    getattr(device, "config_subentry_id", None),
                    service_config_subentry_id,
                )
                healed = self._call_device_registry_api(
                    dev_reg.async_update_device,
                    base_kwargs={
                        "device_id": device_id,
                        "config_subentry_id": service_config_subentry_id,
                        "add_config_entry_id": entry.entry_id,
                    },
                )
                device = _refresh_service_device_entry(healed or device)
                if device is None:
                    _LOGGER.error("[%s] Failed to heal service device", entry.entry_id)
                    raise HomeAssistantError("Failed to heal service device")
            else:
                _LOGGER.debug(
                    "[%s] Service device missing identifier; unable to heal config_subentry_id",
                    entry.entry_id,
                )

        existing_name: str | None = None
        existing_user_name: str | None = None
        has_user_name = False
        if device is not None:
            existing_name = getattr(device, "name", None)
            existing_user_name = getattr(device, "name_by_user", None)
            if isinstance(existing_user_name, str) and existing_user_name.strip():
                has_user_name = True

        entry_title = getattr(entry, "title", None)
        sanitized_entry_title = (
            entry_title.strip()
            if isinstance(entry_title, str) and entry_title.strip()
            else None
        )
        service_device_name = existing_name or sanitized_entry_title

        _LOGGER.debug(
            "Service device registry pre-ensure (entry=%s): name=%s, name_by_user=%s",
            entry.entry_id,
            self._redact_text(existing_name),
            self._redact_text(existing_user_name),
        )

        if device is None:
            create_kwargs: dict[str, Any] = {
                "config_entry_id": entry.entry_id,
                "identifiers": identifiers,
                "manufacturer": SERVICE_DEVICE_MANUFACTURER,
                "model": SERVICE_DEVICE_MODEL,
                "sw_version": INTEGRATION_VERSION,
                "entry_type": dr.DeviceEntryType.SERVICE,
                "configuration_url": "https://github.com/BSkando/GoogleFindMy-HA",
            }
            if service_device_name:
                create_kwargs["name"] = service_device_name
            create_kwargs["translation_key"] = SERVICE_DEVICE_TRANSLATION_KEY
            create_kwargs["translation_placeholders"] = {}
            if service_config_subentry_id is not None:
                create_kwargs["config_subentry_id"] = service_config_subentry_id

            device = self._call_device_registry_api(
                dev_reg.async_get_or_create,
                base_kwargs=create_kwargs,
            )
            device = _refresh_service_device_entry(device)
            _LOGGER.debug(
                "Created Google Find My service device for entry %s (device_id=%s)",
                entry.entry_id,
                getattr(device, "id", None),
            )
        else:
            # Keep metadata fresh if it drifted (rare)
            raw_device_identifiers: set[tuple[str, str]] = (
                getattr(device, "identifiers", set()) or set()
            )
            device_identifiers = set(raw_device_identifiers)
            identifiers_to_apply = set(identifiers)
            extraneous_service_identifiers: set[tuple[Any, ...]] = set()
            for existing in list(device_identifiers):
                if (
                    isinstance(existing, tuple)
                    and len(existing) == 2
                    and existing[0] == DOMAIN
                    and isinstance(existing[1], str)
                    and existing[1].endswith(":service")
                    and existing not in identifiers_to_apply
                ):
                    extraneous_service_identifiers.add(existing)

            missing_identifiers = identifiers_to_apply - device_identifiers
            needs_identifier_sync = bool(
                missing_identifiers or extraneous_service_identifiers
            )
            current_service_links = {
                candidate
                for candidate in _service_entry_links(device)
                if isinstance(candidate, str)
            }

            dev_translation_key = getattr(device, "translation_key", None)
            dev_translation_placeholders = getattr(
                device, "translation_placeholders", None
            )
            dev_config_subentry_id = getattr(device, "config_subentry_id", None)
            should_remove_service_link = service_config_subentry_id is None and bool(
                current_service_links
            )
            should_add_hub_link = (
                service_config_subentry_id is None
                and not _service_has_hub_link(device)
                and bool(entry_id)
            )

            translation_refresh_required = (
                dev_translation_key != SERVICE_DEVICE_TRANSLATION_KEY
                or (dev_translation_placeholders or {}) != {}
            )
            translation_update_supported = (
                translation_refresh_required
                and self._device_registry_allows_translation_update(dev_reg)
            )

            needs_name_refresh = (
                service_device_name is not None
                and service_device_name != existing_name
                and not has_user_name
            )

            needs_update = (
                device.manufacturer != SERVICE_DEVICE_MANUFACTURER
                or device.model != SERVICE_DEVICE_MODEL
                or device.sw_version != INTEGRATION_VERSION
                or device.entry_type != dr.DeviceEntryType.SERVICE
                or dev_config_subentry_id != service_config_subentry_id
                or translation_refresh_required
                or needs_name_refresh
                or needs_identifier_sync
                or should_remove_service_link
                or should_add_hub_link
            )
            if needs_update:
                update_kwargs: dict[str, Any] = {
                    "device_id": device.id,
                    "manufacturer": SERVICE_DEVICE_MANUFACTURER,
                    "model": SERVICE_DEVICE_MODEL,
                    "sw_version": INTEGRATION_VERSION,
                    "entry_type": dr.DeviceEntryType.SERVICE,
                    "configuration_url": "https://github.com/BSkando/GoogleFindMy-HA",
                }
                if service_config_subentry_id is not None:
                    update_kwargs["config_subentry_id"] = service_config_subentry_id
                if needs_identifier_sync:
                    new_identifiers = (
                        device_identifiers - extraneous_service_identifiers
                    ) | identifiers_to_apply
                    update_kwargs["new_identifiers"] = new_identifiers
                if entry_id and (
                    service_config_subentry_id is not None
                    or should_remove_service_link
                    or should_add_hub_link
                ):
                    update_kwargs["add_config_entry_id"] = entry.entry_id
                    if service_config_subentry_id is not None:
                        update_kwargs["add_config_subentry_id"] = (
                            service_config_subentry_id
                        )
                if should_remove_service_link and entry_id:
                    update_kwargs["remove_config_entry_id"] = entry.entry_id
                    removal_id: str | None = None
                    if current_service_links:
                        removal_id = next(iter(current_service_links))
                    elif (
                        isinstance(dev_config_subentry_id, str)
                        and dev_config_subentry_id.strip()
                    ):
                        removal_id = dev_config_subentry_id.strip()
                    update_kwargs["remove_config_subentry_id"] = removal_id
                if needs_name_refresh and service_device_name:
                    update_kwargs["name"] = service_device_name

                call_kwargs = dict(update_kwargs)
                if translation_update_supported:
                    call_kwargs["translation_key"] = SERVICE_DEVICE_TRANSLATION_KEY
                    call_kwargs["translation_placeholders"] = {}

                try:
                    self._call_device_registry_api(
                        dev_reg.async_update_device, base_kwargs=call_kwargs
                    )
                except TypeError as err:
                    if translation_update_supported:
                        setattr(
                            self,
                            "_device_registry_supports_translation_update",
                            False,
                        )
                        translation_update_supported = False
                        self._call_device_registry_api(
                            dev_reg.async_update_device, base_kwargs=update_kwargs
                        )
                    else:  # pragma: no cover - propagate unexpected contract errors
                        raise err
                device = _refresh_service_device_entry(device)
                if translation_refresh_required and not translation_update_supported:
                    translation_kwargs: dict[str, Any] = {
                        "config_entry_id": entry.entry_id,
                        "identifiers": identifiers,
                        "manufacturer": SERVICE_DEVICE_MANUFACTURER,
                        "model": SERVICE_DEVICE_MODEL,
                        "sw_version": INTEGRATION_VERSION,
                        "entry_type": dr.DeviceEntryType.SERVICE,
                        "configuration_url": "https://github.com/BSkando/GoogleFindMy-HA",
                        "translation_key": SERVICE_DEVICE_TRANSLATION_KEY,
                        "translation_placeholders": {},
                    }
                    if service_config_subentry_id is not None:
                        translation_kwargs["config_subentry_id"] = (
                            service_config_subentry_id
                        )
                    if needs_name_refresh and service_device_name:
                        translation_kwargs["name"] = service_device_name
                    device = self._call_device_registry_api(
                        dev_reg.async_get_or_create,
                        base_kwargs=translation_kwargs,
                    )
                    device = _refresh_service_device_entry(device)
                    _LOGGER.debug(
                        "Backfilled service device translation metadata using get_or_create for entry %s",
                        entry.entry_id,
                    )
                _LOGGER.debug(
                    "Updated Google Find My service device metadata for entry %s",
                    entry.entry_id,
                )

        # Book-keeping for quick re-entrance
        self._service_device_ready = True
        self._service_device_id = getattr(device, "id", None)
        setattr(
            self,
            "_service_device_last_subentry_identifier",
            service_subentry_identifier,
        )
        setattr(
            self,
            "_service_device_last_config_subentry_id",
            service_config_subentry_id,
        )

        if device is not None:
            links = _service_entry_links(device)
            has_hub_link = None in links
            if has_hub_link and service_config_subentry_id is not None:
                _LOGGER.info(
                    "[%s] Removing redundant hub link from service device %s",
                    entry.entry_id,
                    getattr(device, "id", "<unknown>"),
                )
                device = _detach_service_hub_link(device)
                self._service_device_id = getattr(device, "id", None)

        if device is not None:
            _LOGGER.debug(
                "Service device registry post-ensure (entry=%s): name=%s, name_by_user=%s",
                entry.entry_id,
                self._redact_text(getattr(device, "name", None)),
                self._redact_text(getattr(device, "name_by_user", None)),
            )

        # Backfill any end devices that were created before the service device was known
        self._apply_pending_via_updates()

    # Optional back-compat alias (some callers may use the public-style name)
    ensure_service_device_exists = _ensure_service_device_exists

    def _find_tracker_entity_entry(self, device_id: str) -> EntityRegistryEntry | None:
        """Return the registry entry for a tracker and migrate legacy unique IDs.

        Uses Phase 12 helpers for identifier extraction and unique_id generation.
        """
        ent_reg = er.async_get(self.hass)
        device_reg = dr.async_get(self.hass)
        entry_id = self._entry_id()

        registry_device = (
            device_reg.async_get(device_id) if device_reg is not None else None
        )
        canonical_device_id = device_id
        registry_identifier: str | None = None

        # Use helper to extract canonical device ID from identifiers
        if registry_device is not None:
            identifiers = getattr(registry_device, "identifiers", None)
            registry_identifier = _extract_canonical_device_id_impl(
                identifiers,
                DOMAIN,
                entry_id=entry_id,
                service_prefix=SERVICE_DEVICE_IDENTIFIER_PREFIX,
                legacy_service_id=LEGACY_SERVICE_IDENTIFIER,
            )
            if registry_identifier:
                canonical_device_id = registry_identifier
                if canonical_device_id != device_id:
                    _LOGGER.debug(
                        "Tracker entity lookup remapped registry_id=%s to canonical_id=%s",
                        device_id,
                        canonical_device_id,
                    )
            else:
                _LOGGER.debug(
                    "Tracker entity lookup found device %s but no matching identifier in %s",
                    device_id,
                    identifiers,
                )
        else:
            _LOGGER.debug(
                "Tracker entity lookup could not find device registry entry for id=%s",
                device_id,
            )

        device_label = (
            self.get_device_display_name(canonical_device_id) or canonical_device_id
        )

        entities_container = getattr(ent_reg, "entities", None)
        ent_registry_values: Sequence[Any] = ()
        if entities_container is not None:
            try:
                ent_registry_values = list(entities_container.values())
            except Exception:  # noqa: BLE001 - best-effort compatibility
                ent_registry_values = ()

        canonical_unique_id: str | None = None
        tracker_subentry_identifier: str | None = None
        tracker_subentry_key: str = TRACKER_SUBENTRY_KEY
        if entry_id:
            tracker_meta: Any | None = None
            meta_getter = getattr(self, "get_subentry_metadata", None)
            if callable(meta_getter):
                try:
                    tracker_meta = meta_getter(feature="device_tracker")
                except TypeError:
                    tracker_meta = None
                except AttributeError:
                    tracker_meta = None
            if tracker_meta is not None:
                candidate_key = getattr(tracker_meta, "key", None)
                if isinstance(candidate_key, str) and candidate_key.strip():
                    tracker_subentry_key = candidate_key.strip()

            identifier_getter = getattr(self, "stable_subentry_identifier", None)
            if callable(identifier_getter):
                try:
                    tracker_subentry_identifier = identifier_getter(
                        key=tracker_subentry_key,
                        feature="device_tracker",
                    )
                except TypeError:
                    tracker_subentry_identifier = identifier_getter(
                        key=tracker_subentry_key
                    )
                except Exception:  # noqa: BLE001 - defensive for legacy coordinators
                    tracker_subentry_identifier = None
            if (
                not isinstance(tracker_subentry_identifier, str)
                or not tracker_subentry_identifier.strip()
            ):
                tracker_subentry_identifier = tracker_subentry_key

            # Use helper to build canonical unique_id
            canonical_unique_id = _build_canonical_unique_id_impl(
                entry_id, tracker_subentry_identifier, canonical_device_id
            )

        def _get_entry_for_unique_id(
            unique_id: str,
        ) -> EntityRegistryEntry | None:
            """Return the registry entry for a given unique_id if it exists."""

            if not unique_id:
                return None

            try:
                entity_id = ent_reg.async_get_entity_id(
                    DEVICE_TRACKER_DOMAIN,
                    DOMAIN,
                    unique_id,
                )
            except TypeError:
                entity_id = None

            if not entity_id:
                return None

            entry: EntityRegistryEntry | None = None
            getter = getattr(ent_reg, "async_get", None)
            if callable(getter):
                try:
                    entry = getter(entity_id)
                except TypeError:
                    entry = None

            if entry is None and ent_registry_values:
                for candidate in ent_registry_values:
                    if getattr(candidate, "entity_id", None) == entity_id:
                        entry = candidate
                        break

            if entry is None:
                entry = SimpleNamespace(
                    entity_id=entity_id,
                    unique_id=unique_id,
                    domain=DEVICE_TRACKER_DOMAIN,
                    platform=DOMAIN,
                    config_entry_id=entry_id,
                )

            return cast("EntityRegistryEntry", entry)

        if canonical_unique_id:
            entry = _get_entry_for_unique_id(canonical_unique_id)
            if entry is not None:
                _LOGGER.debug(
                    "Tracker registry matched canonical unique_id=%s for device '%s' (entity_id=%s)",
                    canonical_unique_id,
                    device_label,
                    entry.entity_id,
                )
                return entry

        # Use helper to build candidate unique_ids
        candidate_unique_ids = _build_entity_unique_id_candidates_impl(
            canonical_device_id,
            entry_id,
            tracker_subentry_identifier,
            DOMAIN,
            subentry_key=tracker_subentry_key
            if tracker_subentry_key != tracker_subentry_identifier
            else None,
        )

        for unique_id in candidate_unique_ids:
            entry = _get_entry_for_unique_id(unique_id)
            if entry is None:
                continue

            if canonical_unique_id and entry.unique_id != canonical_unique_id:
                _LOGGER.info(
                    "Migrating tracker entity %s for device '%s' from legacy unique_id=%s to canonical unique_id=%s",
                    entry.entity_id,
                    device_label,
                    entry.unique_id,
                    canonical_unique_id,
                )
                try:
                    update_entity = getattr(ent_reg, "async_update_entity", None)
                    if callable(update_entity):
                        update_entity(
                            entry.entity_id,
                            new_unique_id=canonical_unique_id,
                        )
                        migrated = _get_entry_for_unique_id(canonical_unique_id)
                        if migrated is not None:
                            return migrated
                    else:
                        _LOGGER.debug(
                            "Entity registry for entry %s lacks async_update_entity; skipping canonical migration",
                            entry_id,
                        )
                        return entry
                except ValueError as err:
                    _LOGGER.error(
                        "Failed to migrate tracker entity %s to canonical unique_id=%s: %s",
                        entry.entity_id,
                        canonical_unique_id,
                        err,
                    )
                    return entry

            return entry

        for entry in ent_registry_values:
            # Use helper for fallback entity matching
            if not _match_entity_by_device_id_impl(
                getattr(entry, "unique_id", ""),
                getattr(entry, "config_entry_id", None),
                canonical_device_id,
                entry_id,
                domain=DEVICE_TRACKER_DOMAIN,
                platform=DOMAIN,
                entity_domain=getattr(entry, "domain", ""),
                entity_platform=getattr(entry, "platform", ""),
            ):
                continue
            unique_id = getattr(entry, "unique_id", "")

            if canonical_unique_id and unique_id != canonical_unique_id:
                _LOGGER.info(
                    "Migrating tracker entity %s for device '%s' from heuristic unique_id=%s to canonical unique_id=%s",
                    entry.entity_id,
                    device_label,
                    unique_id,
                    canonical_unique_id,
                )
                try:
                    update_entity = getattr(ent_reg, "async_update_entity", None)
                    if callable(update_entity):
                        update_entity(
                            entry.entity_id,
                            new_unique_id=canonical_unique_id,
                        )
                        migrated = _get_entry_for_unique_id(canonical_unique_id)
                        if migrated is not None:
                            return migrated
                    else:
                        _LOGGER.debug(
                            "Entity registry for entry %s lacks async_update_entity; skipping canonical migration",
                            entry_id,
                        )
                        return cast("EntityRegistryEntry", entry)
                except ValueError as err:
                    _LOGGER.error(
                        "Failed to migrate heuristic tracker entity %s to canonical unique_id=%s: %s",
                        entry.entity_id,
                        canonical_unique_id,
                        err,
                    )
                    return cast("EntityRegistryEntry", entry)

            _LOGGER.debug(
                "Tracker registry fallback matched entity_id=%s (unique_id=%s) for device '%s'",
                entry.entity_id,
                unique_id,
                device_label,
            )
            return cast("EntityRegistryEntry", entry)

        _LOGGER.debug(
            "No entity registry entry for device '%s'; checked unique_id formats %s (canonical=%s registry_id=%s registry_identifier=%s)",
            device_label,
            candidate_unique_ids,
            canonical_unique_id,
            device_id,
            registry_identifier,
        )
        return None

    def find_tracker_entity_entry(self, device_id: str) -> EntityRegistryEntry | None:
        """Public wrapper to expose tracker entity lookup to platforms."""
        return self._find_tracker_entity_entry(device_id)

    def _ensure_registry_for_devices(
        self,
        devices: list[dict[str, Any]],
        ignored: set[str],
    ) -> int:
        """Ensure end-device DR entries exist and link via the per-entry service device.

        Multi-account/compatibility rules:
        - **Primary identifier (namespaced):** (DOMAIN, f"{entry_id}:{device_id}")
          guarantees global uniqueness across config entries.
        - **Legacy identifier (non-namespaced):** (DOMAIN, device_id) recognized for
          existing installs. If a legacy device belongs to *this* entry, we migrate it
          by adding the new identifier (union) via `async_update_device`.
        - If a legacy device is associated with a *different* entry, we **do not merge**.
          We create a fresh device with the namespaced identifier to avoid cross-account
          collisions.
        - Prefer linking devices to the service anchor via the identifier-based
          `via_device` kwarg when supported. Older cores fall back to `via_device_id`
          once the service device has been created.

        Returns:
            Count of devices that were created or updated.

        See ``docs/CONFIG_SUBENTRIES_HANDBOOK.md`` for the full subentry lifecycle
        and registry expectations that keep tracker devices out of the service bucket.
        """
        entry = self.config_entry or getattr(self, "entry", None)
        entry_id = getattr(entry, "entry_id", None) if entry is not None else None
        if not entry_id:
            return 0

        entry_type: str | None = None
        if entry is not None:
            for container in (
                getattr(entry, "data", None),
                getattr(entry, "options", None),
            ):
                if isinstance(container, Mapping):
                    marker = container.get("subentry_type")
                    if isinstance(marker, str):
                        entry_type = marker
                        break
            if entry_type is None and isinstance(getattr(entry, "data", None), Mapping):
                fallback_marker = cast(Mapping[str, Any], entry.data).get("type")
                if not isinstance(fallback_marker, str):
                    fallback_marker = cast(Mapping[str, Any], entry.data).get(
                        "entry_type"
                    )
                if isinstance(fallback_marker, str):
                    entry_type = fallback_marker

        if entry_type in {SUBENTRY_TYPE_HUB, "hub"}:
            _LOGGER.debug(
                "Skipping Device Registry ensure for hub entry %s; subentries manage device links.",
                entry_id,
            )
            return 0

        try:
            self._refresh_subentry_index(devices)
        except Exception:  # pragma: no cover - defensive guard
            pass

        tracker_meta = self._subentry_metadata.get(TRACKER_SUBENTRY_KEY)

        def _normalize_tracker_subentry_id(value: Any) -> str | None:
            return _sanitize_subentry_id_impl(value)

        entry_tracker_subentry_id = _normalize_tracker_subentry_id(
            getattr(entry, "tracker_subentry_id", None)
        )

        entry_subentries = getattr(entry, "subentries", None)
        # Reuse extract_service_subentry_ids with tracker type constants
        tracker_subentry_ids = _extract_service_subentry_ids_impl(
            entry_subentries,
            entry_tracker_subentry_id,
            SUBENTRY_TYPE_TRACKER,
            TRACKER_SUBENTRY_KEY,
        )

        tracker_config_subentry_id = None
        tracker_meta_identifier: Any | None = None
        if tracker_meta is not None:
            tracker_meta_identifier = getattr(tracker_meta, "config_subentry_id", None)
        for candidate in (tracker_meta_identifier, entry_tracker_subentry_id):
            normalized_candidate = _normalize_tracker_subentry_id(candidate)
            resolved_tracker = _resolve_tracker_subentry_impl(
                normalized_candidate,
                entry_tracker_subentry_id,
                tracker_subentry_ids,
            )
            if resolved_tracker is not None:
                tracker_config_subentry_id = resolved_tracker
                break

        if tracker_config_subentry_id is None:
            _LOGGER.debug(
                "[%s] Skipping tracker Device Registry ensure; config_subentry_id unresolved",
                entry_id,
            )
            return 0

        parent_identifier = service_device_identifier(entry_id)
        setattr(self, "_service_device_identifier", parent_identifier)

        dev_reg = dr.async_get(self.hass)
        async_get_or_create = getattr(dev_reg, "async_get_or_create", None)
        if not callable(async_get_or_create):
            _LOGGER.debug(
                "Skipping Device Registry ensure: registry stub missing async_get_or_create."
            )
            return 0
        update_device = getattr(dev_reg, "async_update_device", None)
        get_device_by_id = getattr(dev_reg, "async_get", None)
        created_or_updated = 0

        # Use imported helper for name normalization
        _normalized_name = _normalize_device_name_impl

        hub_device = None
        hub_device_id: str | None = None
        hub_device_names: set[str] = set()
        hub_devices_by_name: dict[str, Any] = {}
        hub_device = async_get_device_by_identifier_compat(
            dev_reg,
            parent_identifier,
            config_entry_id=entry_id,
        )
        if hub_device is not None:
            hub_device_id = getattr(hub_device, "id", None)
            _hub_base_name = getattr(hub_device, "name_by_user", None) or getattr(
                hub_device, "name", None
            )
            normalized_base = _normalized_name(_hub_base_name)
            if normalized_base:
                hub_device_names.add(normalized_base)
                hub_devices_by_name[normalized_base] = hub_device

        devices_map = getattr(dev_reg, "devices", None)
        if isinstance(devices_map, Mapping) and hub_device_id:
            for device_entry in devices_map.values():
                if getattr(device_entry, "via_device_id", None) != hub_device_id:
                    continue
                candidate_name = getattr(device_entry, "name_by_user", None) or getattr(
                    device_entry, "name", None
                )
                normalized_candidate = _normalized_name(candidate_name)
                if normalized_candidate:
                    hub_device_names.add(normalized_candidate)
                    hub_devices_by_name.setdefault(normalized_candidate, device_entry)

        def _track_hub_name(name: str | None, device: Any | None) -> None:
            normalized = _normalized_name(name)
            if not normalized:
                return
            hub_device_names.add(normalized)
            if device is not None:
                hub_devices_by_name.setdefault(normalized, device)

        def _is_hub_device(device: Any | None) -> bool:
            """Return True when ``device`` represents the hub/service anchor."""
            if device is None:
                return False
            return _is_hub_device_check_impl(
                getattr(device, "id", None),
                hub_device_id,
                getattr(device, "identifiers", None),
                parent_identifier,
            )

        def _resolve_hub_name(
            use_name: str | None, *, device_label: str, device_id: str | None
        ) -> tuple[str | None, Any | None]:
            if not use_name:
                return None, None

            normalized = _normalized_name(use_name)
            if normalized is None:
                return use_name, None

            existing = hub_devices_by_name.get(normalized)
            if normalized not in hub_device_names or (
                existing is not None and getattr(existing, "id", None) == device_id
            ):
                return use_name, None

            if (
                existing is not None
                and entry_id in getattr(existing, "config_entries", set())
                and not _is_hub_device(existing)
            ):
                _LOGGER.debug(
                    "[%s] Reusing hub device '%s' (id=%s) for label '%s' due to name collision",
                    entry_id,
                    use_name,
                    getattr(existing, "id", ""),
                    device_label,
                )
                return use_name, existing

            suffix_source = tracker_config_subentry_id or device_id or entry_id
            suffix = str(suffix_source)[-6:]
            disambiguated = f"{use_name} ({suffix})"
            _LOGGER.debug(
                "[%s] Disambiguated device name for '%s' from '%s' to '%s' (hub collision)",
                entry_id,
                device_label,
                use_name,
                disambiguated,
            )
            return disambiguated, None

        def _has_tracker_link(device: Any) -> bool:
            if tracker_config_subentry_id is None:
                return False
            return tracker_config_subentry_id in _extract_subentry_links_impl(
                device, entry_id
            )

        def _has_hub_link(device: Any) -> bool:
            return None in _extract_subentry_links_impl(device, entry_id)

        def _remove_hub_link(device: Any) -> Any:
            if (
                not callable(update_device)
                or not entry_id
                or tracker_config_subentry_id is None
            ):
                return device
            device_id = getattr(device, "id", "")
            if not device_id:
                return device
            self._call_device_registry_api(
                update_device,
                base_kwargs={
                    "device_id": device_id,
                    "remove_config_entry_id": entry_id,
                    "remove_config_subentry_id": None,
                    "add_config_entry_id": entry_id,
                    "add_config_subentry_id": tracker_config_subentry_id,
                },
            )
            return _refresh_device_entry(device_id, device)

        def _update_device_with_kwargs(kwargs: dict[str, Any]) -> None:
            if not callable(update_device):
                return
            if tracker_config_subentry_id is not None and entry_id:
                kwargs.setdefault("add_config_entry_id", entry_id)
                kwargs.setdefault("add_config_subentry_id", tracker_config_subentry_id)
                kwargs.setdefault("config_subentry_id", tracker_config_subentry_id)
            self._call_device_registry_api(
                update_device,
                base_kwargs=dict(kwargs),
            )

        def _refresh_device_entry(device_id: str, fallback: Any) -> Any:
            if not callable(get_device_by_id) or not device_id:
                return fallback
            try:
                refreshed = get_device_by_id(device_id)
            except TypeError:
                return fallback
            return fallback if refreshed is None else refreshed

        def _heal_tracker_device_subentry(
            device: Any, *, device_label: str, device_id_hint: str | None
        ) -> tuple[Any, bool]:
            if (
                device is None
                or tracker_config_subentry_id is None
                or not callable(update_device)
            ):
                return device, False
            device_id = getattr(device, "id", None)
            if not isinstance(device_id, str) or not device_id:
                device_id = device_id_hint or None
            if not isinstance(device_id, str) or not device_id:
                return device, False
            subentry_links = _extract_subentry_links_impl(device, entry_id)
            needs_tracker_link = tracker_config_subentry_id not in subentry_links
            has_hub_link = None in subentry_links
            extraneous_links = {
                link
                for link in subentry_links
                if link is not None and link != tracker_config_subentry_id
            }
            changed = False

            if extraneous_links:
                for link in sorted(extraneous_links):
                    self._call_device_registry_api(
                        update_device,
                        base_kwargs={
                            "device_id": device_id,
                            "remove_config_entry_id": entry_id,
                            "remove_config_subentry_id": link,
                            "add_config_entry_id": entry_id,
                            "add_config_subentry_id": tracker_config_subentry_id,
                            "config_subentry_id": tracker_config_subentry_id,
                        },
                    )
                device = _refresh_device_entry(device_id, device)
                _LOGGER.debug(
                    "[%s] Removed extraneous config_subentry_id links for device '%s': %s",
                    entry_id,
                    device_label,
                    ", ".join(sorted(str(link) for link in extraneous_links)),
                )
                subentry_links = _extract_subentry_links_impl(device, entry_id)
                needs_tracker_link = tracker_config_subentry_id not in subentry_links
                has_hub_link = None in subentry_links
                changed = True

            current_subentry_id = getattr(device, "config_subentry_id", None)
            if needs_tracker_link or has_hub_link:
                _LOGGER.debug(
                    "[%s] Healing device '%s': correcting config_subentry_id from %s to %s",
                    entry_id,
                    device_label,
                    current_subentry_id,
                    tracker_config_subentry_id,
                )
                base_kwargs = {
                    "device_id": device_id,
                    "config_subentry_id": tracker_config_subentry_id,
                    "add_config_entry_id": entry_id,
                }
                if has_hub_link:
                    base_kwargs["remove_config_entry_id"] = entry_id
                    base_kwargs["remove_config_subentry_id"] = None
                    base_kwargs["add_config_subentry_id"] = tracker_config_subentry_id
                updated = self._call_device_registry_api(
                    update_device,
                    base_kwargs=base_kwargs,
                )
                healed_device = _refresh_device_entry(device_id, updated or device)
                if healed_device is None:
                    _LOGGER.error(
                        "[%s] Failed to heal device %s", entry_id, device_label
                    )
                    return device, False
                return healed_device, True

            return device, changed

        for d in devices:
            dev_id = d.get("id")
            if not isinstance(dev_id, str) or dev_id in ignored:
                continue

            raw_label = (d.get("name") or "").strip()
            device_label = raw_label or dev_id or "<unknown>"
            raw_manufacturer = d.get("manufacturer")
            manufacturer = (
                raw_manufacturer.strip()
                if isinstance(raw_manufacturer, str) and raw_manufacturer.strip()
                else "Google"
            )
            raw_model = d.get("model")
            model = (
                raw_model.strip()
                if isinstance(raw_model, str) and raw_model.strip()
                else "Find My Device"
            )
            device_updated = False

            # Build identifiers
            ns_ident = (DOMAIN, f"{entry_id}:{dev_id}")
            legacy_ident = (DOMAIN, dev_id)

            # Preferred: device already known by namespaced identifier?
            dev = async_get_device_by_identifier_compat(
                dev_reg,
                ns_ident,
                config_entry_id=entry_id,
            )
            if dev is None:
                # Legacy present?
                legacy_dev = async_get_device_by_identifier_compat(
                    dev_reg,
                    legacy_ident,
                    config_entry_id=entry_id,
                )
                if legacy_dev is not None:
                    # If legacy device belongs to THIS entry, migrate by adding namespaced ident.
                    if entry_id in legacy_dev.config_entries:
                        new_idents = set(legacy_dev.identifiers)
                        new_idents.add(ns_ident)
                        needs_identifiers = new_idents != legacy_dev.identifiers
                        needs_config_subentry = (
                            tracker_config_subentry_id is not None
                            and not _has_tracker_link(legacy_dev)
                        )
                        needs_manufacturer = (
                            getattr(legacy_dev, "manufacturer", None) != manufacturer
                        )
                        needs_model = getattr(legacy_dev, "model", None) != model
                        raw_name = (d.get("name") or "").strip()
                        use_name = (
                            raw_name
                            if raw_name and raw_name != "Google Find My Device"
                            else None
                        )
                        use_name, _ = _resolve_hub_name(
                            use_name,
                            device_label=device_label,
                            device_id=getattr(legacy_dev, "id", None),
                        )
                        needs_name = (
                            bool(use_name)
                            and not getattr(legacy_dev, "name_by_user", None)
                            and getattr(legacy_dev, "name", None) != use_name
                        )
                        needs_parent_clear = (
                            getattr(legacy_dev, "via_device_id", None) is not None
                        )
                        if (
                            needs_identifiers
                            or needs_config_subentry
                            or needs_name
                            or needs_parent_clear
                        ):
                            update_kwargs: dict[str, Any] = {
                                "device_id": legacy_dev.id,
                            }
                            if needs_config_subentry:
                                update_kwargs["add_config_entry_id"] = entry_id
                                update_kwargs["add_config_subentry_id"] = (
                                    tracker_config_subentry_id
                                )
                            if needs_identifiers:
                                update_kwargs["new_identifiers"] = new_idents
                            if needs_name:
                                update_kwargs["name"] = use_name
                            if needs_manufacturer:
                                update_kwargs["manufacturer"] = manufacturer
                            if needs_model:
                                update_kwargs["model"] = model
                            if needs_parent_clear:
                                update_kwargs["via_device_id"] = None
                            if tracker_config_subentry_id is not None:
                                update_kwargs.setdefault(
                                    "add_config_entry_id", entry_id
                                )
                                update_kwargs.setdefault(
                                    "add_config_subentry_id", tracker_config_subentry_id
                                )
                            legacy_id = getattr(legacy_dev, "id", None)
                            _update_device_with_kwargs(update_kwargs)
                            legacy_dev = _refresh_device_entry(
                                legacy_id or "",
                                legacy_dev,
                            )
                            if (
                                tracker_config_subentry_id is not None
                                and _has_tracker_link(legacy_dev)
                                and _has_hub_link(legacy_dev)
                            ):
                                legacy_dev = _remove_hub_link(legacy_dev)
                        device_updated = True
                        dev = legacy_dev
                    else:
                        # Belongs to another entry → create a new device with namespaced ident (no merge).
                        dev = None

            # Create if still missing
            if dev is None:
                # Only set a real label; never write placeholders on cold boot
                use_name = (
                    raw_label
                    if raw_label and raw_label != "Google Find My Device"
                    else None
                )

                use_name, reuse_device = _resolve_hub_name(
                    use_name,
                    device_label=device_label,
                    device_id=dev_id,
                )
                if reuse_device is not None:
                    dev = reuse_device
                    reuse_device_id = getattr(dev, "id", None)
                    if reuse_device_id:
                        reuse_update_kwargs: dict[str, Any] = {
                            "device_id": reuse_device_id
                        }
                        if ns_ident not in getattr(dev, "identifiers", set()):
                            updated_identifiers = set(
                                getattr(dev, "identifiers", set())
                            )
                            updated_identifiers.add(ns_ident)
                            reuse_update_kwargs["new_identifiers"] = updated_identifiers
                        if getattr(dev, "manufacturer", None) != manufacturer:
                            reuse_update_kwargs["manufacturer"] = manufacturer
                        if getattr(dev, "model", None) != model:
                            reuse_update_kwargs["model"] = model
                        reuse_update_kwargs.setdefault("add_config_entry_id", entry_id)
                        reuse_update_kwargs.setdefault(
                            "add_config_subentry_id", tracker_config_subentry_id
                        )

                        if len(reuse_update_kwargs) > 1:
                            _update_device_with_kwargs(reuse_update_kwargs)
                            dev = _refresh_device_entry(reuse_device_id, dev)
                            device_updated = True

                create_kwargs: dict[str, Any] = {
                    "config_entry_id": entry_id,
                    "identifiers": {ns_ident},
                    "manufacturer": manufacturer,
                    "model": model,
                    "name": use_name,
                }

                if tracker_config_subentry_id is not None:
                    create_kwargs["config_subentry_id"] = tracker_config_subentry_id

                if dev is None:
                    dev = self._call_device_registry_api(
                        async_get_or_create,
                        base_kwargs=create_kwargs,
                    )
                    device_updated = True
                dev, healed = _heal_tracker_device_subentry(
                    dev,
                    device_label=device_label,
                    device_id_hint=dev_id,
                )
                device_updated = device_updated or healed
                if (
                    tracker_config_subentry_id is not None
                    and dev is not None
                    and _has_tracker_link(dev)
                    and _has_hub_link(dev)
                ):
                    dev = _remove_hub_link(dev)
                    device_updated = True
            else:
                dev, healed = _heal_tracker_device_subentry(
                    dev,
                    device_label=device_label,
                    device_id_hint=dev_id,
                )
                device_updated = device_updated or healed
                # Keep name fresh if not user-overridden and a new upstream label is available
                use_name = (
                    raw_label
                    if raw_label and raw_label != "Google Find My Device"
                    else None
                )

                use_name, _ = _resolve_hub_name(
                    use_name,
                    device_label=device_label,
                    device_id=getattr(dev, "id", None),
                )

                device_id = getattr(dev, "id", "")
                update_existing_kwargs: dict[str, Any] = {"device_id": device_id}
                name_needs_update = (
                    bool(use_name)
                    and not getattr(dev, "name_by_user", None)
                    and dev.name != use_name
                )
                manufacturer_needs_update = (
                    getattr(dev, "manufacturer", None) != manufacturer
                )
                model_needs_update = getattr(dev, "model", None) != model
                if manufacturer_needs_update:
                    update_existing_kwargs["manufacturer"] = manufacturer
                if model_needs_update:
                    update_existing_kwargs["model"] = model
                if name_needs_update:
                    update_existing_kwargs["name"] = use_name

                needs_config_subentry_update = (
                    tracker_config_subentry_id is not None
                    and not _has_tracker_link(dev)
                )

                needs_parent_clear = getattr(dev, "via_device_id", None) is not None

                if needs_parent_clear:
                    update_existing_kwargs["via_device_id"] = None

                needs_update = (
                    name_needs_update
                    or needs_config_subentry_update
                    or needs_parent_clear
                    or manufacturer_needs_update
                    or model_needs_update
                )

                if needs_update and callable(update_device) and device_id:
                    if needs_config_subentry_update:
                        update_existing_kwargs["add_config_entry_id"] = entry_id
                        update_existing_kwargs["add_config_subentry_id"] = (
                            tracker_config_subentry_id
                        )
                    elif (
                        tracker_config_subentry_id is not None
                        and _has_tracker_link(dev)
                        and _has_hub_link(dev)
                    ):
                        update_existing_kwargs["remove_config_entry_id"] = entry_id
                        update_existing_kwargs["remove_config_subentry_id"] = None
                    _update_device_with_kwargs(update_existing_kwargs)
                    dev = _refresh_device_entry(device_id or "", dev)
                    if (
                        tracker_config_subentry_id is not None
                        and _has_tracker_link(dev)
                        and _has_hub_link(dev)
                    ):
                        dev = _remove_hub_link(dev)
                    device_updated = True

                elif (
                    tracker_config_subentry_id is not None
                    and callable(update_device)
                    and device_id
                    and _has_tracker_link(dev)
                    and _has_hub_link(dev)
                ):
                    dev = _remove_hub_link(dev)
                    device_updated = True

            if dev is not None:
                _track_hub_name(
                    getattr(dev, "name_by_user", None) or getattr(dev, "name", None),
                    dev,
                )

            if device_updated:
                created_or_updated += 1

        self._apply_pending_via_updates()
        return created_or_updated
