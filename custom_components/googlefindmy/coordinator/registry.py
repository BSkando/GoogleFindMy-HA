"""Device registry operations for GoogleFindMyCoordinator.

This module contains registry-related methods extracted from main.py
during Phase 2 of the refactoring.

Methods moved here:
- _call_device_registry_api: Core registry call with compatibility handling
- _apply_device_ownership: The coordinator's execution point for ownership changes
- _device_registry_capabilities: Signature-derived capability profile
- _report_unmigrated_ownership_kwargs: Brake against pre-2026.8 keywords
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
from collections.abc import Callable, Mapping, Sequence
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

from ..Auth.log_safety import describe_exception
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
from ._mixin_typing import _MixinBase
from .helpers.registry import (
    NOT_GIVEN,
    STALE_REMOVAL_MAX_ATTEMPTS,
    STALE_REMOVAL_REQUIRED_MISSES,
    DeviceRegistryCapabilities,
    NotGivenType,
    OwnershipIntent,
    plan_stale_device_removals,
    read_device_ownership,
)
from .helpers.registry import (
    OWNERSHIP_ADD_KWARGS as _OWNERSHIP_ADD_KWARGS,
)
from .helpers.registry import (
    OWNERSHIP_REMOVE_KWARGS as _OWNERSHIP_REMOVE_KWARGS,
)
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
    detect_device_registry_capabilities as _detect_capabilities_impl,
)
from .helpers.registry import (
    device_belongs_to_entry as _device_belongs_to_entry_impl,
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
    iter_all_devices as _iter_all_devices_impl,
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
    plan_device_ownership as _plan_device_ownership_impl,
)
from .helpers.registry import (
    resolve_device_by_identifiers as _resolve_device_by_identifiers_impl,
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

# Two-stage emergency brake for ownership keywords that were not migrated to
# ``_apply_device_ownership``. It now stands on "drop": such a keyword is
# removed from the call and logged as an error. Dropping only became safe once
# every call site speaks intents; doing it earlier would have made each
# intermediate commit non-functional on Core 2026.8+, where the old keywords
# still parse and where ``add_``/``remove_`` together form a working move.
# It stays a switch rather than being hard-wired: the "report" stage keeps the
# warning/debug levels alive and under test. There is no runtime option behind
# it; reaching that stage means editing this line or patching it in a test. A
# test reads this constant without patching it, so an edit back to "report"
# turns that test red instead of passing unnoticed.
_OWNERSHIP_ENFORCEMENT = "drop"


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
        """Call a device registry API, handling keyword compatibility.

        Reach note: the ``UnknownEntry``/``UnknownSubEntry`` retry below fires
        only for calls that carried the bare ``config_subentry_id``. Since AP-12
        the coordinator's ownership calls come from ``plan_device_ownership``
        and name ``add_config_subentry_id`` or ``new_config_subentry_id``, so
        that retry now serves the compatibility shim and ``async_get_or_create``
        rather than the ownership paths.
        """

        kwargs = dict(base_kwargs or {})
        sent_legacy_subentry_kwarg = False
        if "config_subentry_id" in kwargs:
            replacement = self._device_registry_config_subentry_kwarg_name(call)
            if replacement is None:
                kwargs.pop("config_subentry_id")
            elif replacement != "config_subentry_id":
                kwargs[replacement] = kwargs.pop("config_subentry_id")
            else:
                sent_legacy_subentry_kwarg = True

        self._report_unmigrated_ownership_kwargs(call, kwargs)

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
            # Retry without subentry information only when the call really did
            # carry the bare ``config_subentry_id``. A callable that speaks the
            # renamed keyword (``add_`` or ``new_``) means what it says, so an
            # unknown subentry there is a genuine error and must surface.
            if not sent_legacy_subentry_kwarg:
                raise
            _LOGGER.debug(
                "Device registry call %s rejected config_subentry_id (%s); retrying without it",
                getattr(call, "__qualname__", repr(call)),
                err,
            )
            # Only the bare name is dropped. Filtering every subentry spelling
            # would strip ``remove_config_subentry_id`` while leaving
            # ``remove_config_entry_id`` in place, and an entry-wide removal is
            # exactly the call that deletes a device on Core 2026.8+.
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("config_subentry_id", None)
            return call(**fallback_kwargs)

    def _report_unmigrated_ownership_kwargs(
        self, call: Callable[..., Any], kwargs: dict[str, Any]
    ) -> None:
        """Drop pre-2026.8 ownership keywords, or on the report stage log them.

        From Core 2026.8 the ``new_config_*`` keywords must not be combined with
        the ``add_``/``remove_`` pair; core raises ``HomeAssistantError``. Note
        what the legacy ``add_`` keyword really does on that core: on its own it
        attaches nothing, but combined with its ``remove_`` counterpart it arms a
        pending move that the removal then completes. Dropping only one half of
        such a pair therefore turns a working move into a deletion, which is why
        the two families are only ever dropped together.
        """
        # No name check: measured at tags 2025.9.1, 2026.8.0 and 2026.9.0,
        # ``async_update_device`` is the only registry call that carries the
        # ``new_*`` keywords, so the profile already narrows this to it. A name
        # check would additionally miss every wrapper without a ``__name__``.
        caps = self._device_registry_capabilities(call)
        if not caps.single_owner_model:
            return
        stale = [key for key in _OWNERSHIP_REMOVE_KWARGS if key in kwargs] + [
            key for key in _OWNERSHIP_ADD_KWARGS if key in kwargs
        ]
        if not stale:
            return
        dropping = _OWNERSHIP_ENFORCEMENT == "drop"
        # A lone add_* is inert on this core. A remove_* on the owning entry is
        # not: it deletes the device. The two deserve different visibility.
        destructive = _OWNERSHIP_REMOVE_KWARGS[0] in stale
        # Level by consequence, not by sentiment. On the "report" stage nothing
        # is altered, so an error per device and refresh would drown the real
        # ones; there, a lone add_* is inert and only the remove_* form can
        # delete. Once the switch drops keywords the call really changes, and
        # that must be loud, which is why dropping overrides the split.
        if dropping:
            log = _LOGGER.error
        elif destructive:
            log = _LOGGER.warning
        else:
            log = _LOGGER.debug
        log(
            "Unmigrated ownership keywords %s for %s: ownership changes must go "
            "through _apply_device_ownership on Home Assistant 2026.8+ (%s)",
            ", ".join(stale),
            getattr(call, "__qualname__", repr(call)),
            "dropped" if dropping else "passed through",
        )
        if dropping:
            # All of them, or none: see the docstring above.
            for key in stale:
                kwargs.pop(key, None)

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

    def _device_registry_capabilities(
        self, call: Callable[..., Any]
    ) -> DeviceRegistryCapabilities:
        """Return the cached capability profile for ``call``.

        The profile is read from the signature, never from a version string.
        A fork, a backport or a patched core can carry any number; the signature
        is what the call actually has to satisfy. See
        ``docs/AI_DEPRECATIONS_GUIDE.md``, section VI.
        """
        cache_attr = "_device_registry_capability_cache"
        cache_obj = getattr(self, cache_attr, None)
        cache: dict[Any, DeviceRegistryCapabilities]
        if isinstance(cache_obj, dict):
            cache = cast(dict[Any, DeviceRegistryCapabilities], cache_obj)
        else:
            cache = cast(dict[Any, DeviceRegistryCapabilities], {})
            setattr(self, cache_attr, cache)

        func = getattr(call, "__func__", call)
        try:
            caps = cache.get(func)
        except TypeError:  # pragma: no cover - unhashable callable
            return _detect_capabilities_impl(call)
        if caps is None:
            caps = _detect_capabilities_impl(call)
            cache[func] = caps
        return caps

    def _device_registry_config_subentry_kwarg_name(
        self, call: Callable[..., Any]
    ) -> str | None:
        """Return the config-subentry kwarg name accepted by ``call``.

        Compatibility shim for ``async_get_or_create`` and for the callers that
        still speak the bare ``config_subentry_id``. Ownership *changes* do not
        come through here any more; they go through
        :meth:`_apply_device_ownership`, which asks
        :meth:`_device_registry_capabilities` for the right keyword.

        The preference order lives in
        :attr:`DeviceRegistryCapabilities.subentry_kwarg_for_shim` and is
        deliberately the historical one: a callable that still accepts the bare
        ``config_subentry_id`` gets it, and ``new_config_subentry_id`` is never
        returned. Handing out the newer spelling here would silently turn this
        shim back into an ownership-changing path, and on Core 2026.8+ that is
        the difference between a no-op and a move.

        Version note corrected: the rename predates our declared minimum.
        Tag ``2025.9.1`` already ships ``add_config_subentry_id`` on
        ``async_update_device`` (``homeassistant/helpers/device_registry.py``,
        line 1014 in that tag) and no longer accepts ``config_subentry_id``
        there at all (signature at line 1009). The legacy branch below
        therefore serves no supported core any more, only registry doubles in
        tests. It stays for now; removing it is a separate change.

        Note also that ``async_get_or_create`` keeps ``config_subentry_id`` in
        *every* release up to 2026.9 (verified at tags 2025.9.1, 2026.8.0 and
        2026.9.0), which is why that call needs no special case here: the
        preference order picks the bare name for it anyway. Ownership *changes*
        must not come through this helper at all; see
        :meth:`_apply_device_ownership` and ``docs/AI_DEPRECATIONS_GUIDE.md``,
        section VI.
        """

        return self._device_registry_capabilities(call).subentry_kwarg_for_shim

    def _apply_device_ownership(  # noqa: PLR0913 - one parameter per ownership axis
        self,
        dev_reg: Any,
        *,
        intent: OwnershipIntent,
        device_id: str,
        entry_id: str,
        target_subentry_id: str | None = None,
        detach_subentry_id: str | None | NotGivenType = NOT_GIVEN,
        device: Any | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> Any:
        """Execute an ownership intent against ``dev_reg``.

        This is where the coordinator changes which config entry or subentry
        owns a device. It is not the only executor in the integration: call
        sites without a coordinator instance (``services.py`` and
        ``config_flow.py``) run their plan through
        ``execute_ownership_plan`` in the helpers module. What must stay single
        is the *translation*, and that is ``plan_device_ownership``, which both
        executors use. This method adds what only coordinator call sites need:
        the unmigrated-keyword brake and the ``config_subentry_id``
        compatibility shim. Call sites state an intent; the keywords are chosen
        by ``plan_device_ownership`` from the signature of the installed core.
        See ``docs/AI_DEPRECATIONS_GUIDE.md``, section VI.

        ``device`` is the device entry the caller already holds. When it is
        absent, the intent needs the current ownership (DETACH, ENSURE) and the
        core uses the single-owner model, this method resolves it first: DETACH
        must never run on an unknown ownership state, because on that core
        dropping the owning entry deletes the device and every entity attached
        to it. MOVE does not read the current ownership and skips the lookup.

        Args:
            dev_reg: The device registry.
            intent: MOVE, ENSURE or DETACH.
            device_id: The device to act on.
            entry_id: Our config entry.
            target_subentry_id: Where the device shall sit (MOVE, ENSURE).
            detach_subentry_id: Which link to give up (DETACH, legacy MOVE).
                ``None`` names the hub link, that is the device sitting directly
                on the entry. Leaving the argument out is a different statement:
                it lets the planner read the link from the current ownership.
            device: The device entry, when the caller already holds it.
            extra: Additional keyword arguments for the update call, for
                example ``name``. Ownership and subentry keywords do not belong
                in here: a ``config_subentry_id`` would pass through the
                compatibility shim and arrive as ``add_config_subentry_id`` next
                to the ``new_*`` pair, which Core 2026.8+ rejects with
                ``HomeAssistantError``. Use ``target_subentry_id`` instead.

        Returns:
            The updated device entry, ``None`` after a removal, or the device
            that was passed in when nothing had to be done.
        """
        update_call = getattr(dev_reg, "async_update_device", None)
        if not callable(update_call) or not device_id or not entry_id:
            return device

        caps = self._device_registry_capabilities(update_call)
        needs_state = intent in (OwnershipIntent.DETACH, OwnershipIntent.ENSURE)
        if device is None and needs_state and caps.single_owner_model:
            # ``async_get(device_id)`` is *not* deprecated, unlike the
            # by-identifiers lookup it replaces; it is the cheap way to learn
            # the current ownership before deciding anything. MOVE does not read
            # it, so it does not pay for it.
            get_by_id = getattr(dev_reg, "async_get", None)
            if callable(get_by_id):
                device = get_by_id(device_id)
        # A device entry that carries neither attribute tells us nothing, and
        # "unknown" must not collapse into "owned by nobody": that difference is
        # the whole point of DeviceOwnership, and on a single-owner core it is
        # the difference between a refusal and a silent no-op.
        # ``read_device_ownership`` is the one place that draws that line.
        current = read_device_ownership(device)

        try:
            plan = _plan_device_ownership_impl(
                intent,
                caps=caps,
                device_id=device_id,
                entry_id=entry_id,
                target_subentry_id=target_subentry_id,
                detach_subentry_id=detach_subentry_id,
                current=current,
                extra=extra,
            )
        except ValueError:
            if intent not in (
                OwnershipIntent.DETACH,
                OwnershipIntent.ENSURE,
            ):  # pragma: no cover - MOVE has no refusal today; guard for later
                # Only the planner's refusals are expected here; anything else
                # is a real error and must not be swallowed under a wrong
                # message.
                raise
            # The planner refuses to guess: DETACH would delete a device it
            # cannot describe, ENSURE without a target would move it to the
            # entry root. Both are reported instead of attempted.
            _LOGGER.error(
                "Cannot apply %s to device %s on entry %s: device entry not resolvable",
                intent.value,
                device_id,
                entry_id,
            )
            return device

        result: Any = device
        for operation in plan:
            if operation.method == "async_remove_device":
                remove_call = getattr(dev_reg, "async_remove_device", None)
                if callable(remove_call):
                    remove_call(operation.kwargs["device_id"])
                    result = None
                else:
                    # The return value cannot express this failure: the device
                    # comes back unchanged, which the caller cannot tell apart
                    # from an empty plan. Hence the log.
                    _LOGGER.error(
                        "Cannot remove device %s: registry has no async_remove_device",
                        device_id,
                    )
                continue
            result = self._call_device_registry_api(
                update_call, base_kwargs=dict(operation.kwargs)
            )
        return result

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
            """Return the set of subentry identifiers linked to ``entry``.

            Delegates to the shared accessor, which reads the scalar ownership
            fields before the ``config_entries_subentries`` shim that Core
            2026.10 reports on every read; see ``extract_subentry_links``.
            """

            if not entry_id:
                return set()
            return _extract_subentry_links_impl(device, entry_id)

        def _service_has_service_link(device: Any) -> bool:
            if service_config_subentry_id is None:
                return False
            links = _service_entry_links(device)
            return _has_subentry_link_impl(links, service_config_subentry_id)

        def _service_has_hub_link(device: Any) -> bool:
            links = _service_entry_links(device)
            return _has_hub_link_impl(links)

        def _detach_service_hub_link(device: Any) -> Any:  # pragma: no cover
            """Put the service device where it belongs: the service subentry.

            The caller reaches this only when the device carries a hub link
            *and* a service subentry exists, so the intent is a MOVE and not a
            DETACH. That distinction is not cosmetic. On a single-owner core a
            hub link means the device really sits on the entry root, and a
            DETACH of that link is the call that deletes the device together
            with every entity attached to it. MOVE reaches the same end state
            on both core generations and deletes nothing.

            Unreachable on both core generations, and that is measured, not
            derived: the healing step above runs first and names the hub link
            as the one to detach, so no hub link is left when this is asked
            (with the legacy double, whose ``add_config_subentry_id`` is a set
            union like Core 2025.9, the healing branch is hit and this one is
            not). The MOVE here is therefore precaution rather than a fix for
            a reachable defect, and it is worth having for exactly that
            reason: the branch is one condition away from being reachable
            again. The ``service_config_subentry_id is None`` guard below is
            of the same kind; the only caller today excludes it, and it stays
            for the next one.
            """
            device_id = getattr(device, "id", None)
            if (
                not entry_id
                or service_config_subentry_id is None
                or not isinstance(device_id, str)
                or not device_id
            ):
                return device
            self._apply_device_ownership(
                dev_reg,
                intent=OwnershipIntent.MOVE,
                device_id=device_id,
                entry_id=entry_id,
                target_subentry_id=service_config_subentry_id,
                detach_subentry_id=None,
                device=device,
            )
            return _refresh_service_device_entry(device)

        # Order the candidates rather than handing over a set: the stable
        # per-entry identifier first, the subentry-scoped one second, because
        # the stable form survives a subentry change and the scoped one goes
        # stale with it. From Core 2026.8 the resolver walks that order and
        # scopes each lookup to the entry; below it there is no replacement API,
        # so it still passes the set on and the old iteration-order behaviour
        # remains. The order is therefore a guarantee on the newer core and an
        # expression of intent on the older one.
        lookup_candidates: tuple[tuple[str, str], ...] = (
            service_device_identifier(entry.entry_id),
        )
        if service_subentry_identifier is not None:
            lookup_candidates += (service_subentry_identifier,)
        device = _resolve_device_by_identifiers_impl(
            dev_reg, lookup_candidates, entry_id=entry.entry_id
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
                healed = self._apply_device_ownership(
                    dev_reg,
                    intent=OwnershipIntent.MOVE,
                    device_id=device_id,
                    entry_id=entry.entry_id,
                    target_subentry_id=service_config_subentry_id,
                    device=device,
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
                # Two axes, deliberately kept apart. The metadata axis says what
                # the device shall look like; the ownership axis says where it
                # shall sit. Only the second one has a core-version problem, and
                # mixing them is what produced the keyword soup this replaces.
                update_kwargs: dict[str, Any] = {
                    "manufacturer": SERVICE_DEVICE_MANUFACTURER,
                    "model": SERVICE_DEVICE_MODEL,
                    "sw_version": INTEGRATION_VERSION,
                    "entry_type": dr.DeviceEntryType.SERVICE,
                    "configuration_url": "https://github.com/BSkando/GoogleFindMy-HA",
                }
                if needs_identifier_sync:
                    new_identifiers = (
                        device_identifiers - extraneous_service_identifiers
                    ) | identifiers_to_apply
                    update_kwargs["new_identifiers"] = new_identifiers
                if needs_name_refresh and service_device_name:
                    update_kwargs["name"] = service_device_name

                # The intent is always a MOVE: the service device shall sit in
                # the service subentry, or on the entry root when there is none.
                # A bare DETACH would be wrong here even where the old code only
                # removed a link, because on Core 2026.8+ removing the owning
                # entry deletes the device instead of freeing it.
                ownership_target = service_config_subentry_id
                # Left as "not given" unless a link is really named: an explicit
                # ``None`` would mean "give up the hub link", which is a
                # different statement from "move the device off whatever
                # subentry it sits on now". On the declared minimum core the two
                # end up at the same payload, because a device entry there
                # describes no ownership and the planner then emits no removal
                # at all; the distinction is what keeps that behaviour honest
                # rather than accidental.
                ownership_detach: str | None | NotGivenType = NOT_GIVEN
                needs_ownership = bool(entry_id) and (
                    service_config_subentry_id is not None
                    or should_remove_service_link
                    or should_add_hub_link
                )
                # ``should_remove_service_link`` implies a non-empty link set
                # (see its definition above), so there is nothing to fall back
                # to; the old ``config_subentry_id`` branch that sat here was
                # unreachable for that reason and is gone.
                if should_remove_service_link and entry_id:
                    ownership_detach = next(iter(current_service_links))

                # Bound before the closure rather than read from it, so the
                # retry below cannot depend on a later rebinding of ``device``
                # (there is none between the two calls today, and this keeps it
                # that way).
                service_device_entry = device
                service_device_id: str = device.id

                def _write_service_device(with_translation: bool) -> None:
                    """Write metadata, and ownership too when it has to change."""

                    payload = dict(update_kwargs)
                    if with_translation:
                        payload["translation_key"] = SERVICE_DEVICE_TRANSLATION_KEY
                        payload["translation_placeholders"] = {}
                    if needs_ownership:
                        self._apply_device_ownership(
                            dev_reg,
                            intent=OwnershipIntent.MOVE,
                            device_id=service_device_id,
                            entry_id=entry.entry_id,
                            target_subentry_id=ownership_target,
                            detach_subentry_id=ownership_detach,
                            device=service_device_entry,
                            extra=payload,
                        )
                        return
                    self._call_device_registry_api(
                        dev_reg.async_update_device,
                        base_kwargs={"device_id": service_device_id, **payload},
                    )

                try:
                    _write_service_device(translation_update_supported)
                except TypeError as err:
                    if translation_update_supported:
                        setattr(
                            self,
                            "_device_registry_supports_translation_update",
                            False,
                        )
                        translation_update_supported = False
                        _write_service_device(False)
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
                # AGENTS.md section 5 (b): the migration runs on the polling
                # path and repeats until it succeeds, so INFO and ERROR carry
                # the unique_ids (class b) only; entity_id and label at DEBUG.
                _LOGGER.info(
                    "Migrating tracker entity from legacy unique_id=%s to canonical unique_id=%s",
                    entry.unique_id,
                    canonical_unique_id,
                )
                _LOGGER.debug(
                    "Migrating tracker entity %s for device '%s' (legacy unique_id=%s)",
                    entry.entity_id,
                    device_label,
                    entry.unique_id,
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
                    # The registry's own error text names the entity_id; the
                    # only ValueError here is the unique_id being rejected.
                    _LOGGER.error(
                        "Failed to migrate tracker entity from legacy unique_id=%s to canonical unique_id=%s: the entity registry rejected the new unique_id",
                        entry.unique_id,
                        canonical_unique_id,
                    )
                    _LOGGER.debug(
                        "Migration failure detail for %s: %s", entry.entity_id, err
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
                    "Migrating tracker entity from heuristic unique_id=%s to canonical unique_id=%s",
                    unique_id,
                    canonical_unique_id,
                )
                _LOGGER.debug(
                    "Migrating heuristic tracker entity %s for device '%s' (unique_id=%s)",
                    entry.entity_id,
                    device_label,
                    unique_id,
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
                    # The registry's own error text names the entity_id; the
                    # only ValueError here is the unique_id being rejected.
                    _LOGGER.error(
                        "Failed to migrate heuristic tracker entity from unique_id=%s to canonical unique_id=%s: the entity registry rejected the new unique_id",
                        unique_id,
                        canonical_unique_id,
                    )
                    _LOGGER.debug(
                        "Migration failure detail for %s: %s", entry.entity_id, err
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

    @callback  # type: ignore[misc, untyped-decorator, unused-ignore]
    def reindex_poll_targets(self) -> None:
        """Re-derive the poll target sets after entities were registered.

        ``_enabled_poll_device_ids`` is entity-driven but rebuilt only from
        *device* registry events, and this coordinator subscribes to no entity
        registry channel. A brand-new tracker gets its device registry entry
        from ``_ensure_registry_for_devices`` **before** its entity exists, so
        the event that rebuild triggers sees the device as present but not
        enabled. Nothing revisits that verdict: the polling predicate admits a
        device that is either enabled or not present at all, so from the next
        cycle on the new tracker is excluded -- present, never enabled -- until
        the entry is reloaded or Home Assistant restarts.

        The platform therefore calls this once the additions it scheduled have
        reached the entity registry. Re-deriving both sets from the registries
        is idempotent, and it refreshes the subentry index along with them, so
        the stored metadata stops calling the device disabled while its entity
        already publishes state.

        **Not during early setup.** The refresh underneath runs without
        ``skip_manager_update``, so calling this while the stubbed
        ``ConfigEntrySubEntryManager`` is still bound would mutate it, which the
        repository contract forbids for coordinator metadata probes. The only
        caller today is the tracker registry probe, which fires a full grace
        period after setup; a new caller inside ``async_setup_entry`` needs the
        flag plumbed through first.
        """

        self._reindex_poll_targets_from_device_registry()

    def _remove_stale_devices(
        self,
        fresh_list_ids: set[str],
        *,
        dropped_candidates: int,
        canonicless_warn: int,
        canonicless_names: Mapping[tuple[str, str], int] | None = None,
    ) -> int:
        """Remove tracker devices that left the Google account.

        Called by the update cycle only with a device list it fetched in this
        cycle and that is not empty. The decision is
        ``plan_stale_device_removals`` (protection rules S-R1 to S-R9); this
        method keeps the miss counter, logs, and executes the plan.

        Removal goes through ``_apply_device_ownership`` with DETACH on the
        subentry link the device sits on. Without ``detach_subentry_id`` DETACH
        names the hub link and leaves a tracker device untouched (M-10); with
        it, core removes the device and its entities once no link is left. The
        device is **not** added to ``ignored_devices``: a tracker added back to
        the account comes back, and core restores its device id, user name,
        area and entity ids from the registry's deleted entries.

        The platforms do not bring its entities back on their own: each keeps
        the ids it has set up in a set that lives as long as the setup, so a
        known id coming back adds nothing. The removed Google ids are therefore
        remembered, and the first fresh list that carries one again reloads the
        entry once (``_reload_if_removed_devices_returned``).

        A device counts as removed when it is gone from the registry or no
        longer has a link to this entry afterwards (on a core below 2026.8
        another entry can keep it; this entry's entities are gone either way).
        A refused DETACH, or one that raises, leaves the device, its Google
        ids and the remembered set untouched. The next fresh list tries again,
        once: after ``STALE_REMOVAL_MAX_ATTEMPTS`` failed attempts the device is
        given up until the next setup, because ``_apply_device_ownership`` logs
        a DETACH it refuses as an error and an unbounded retry would repeat
        that with every device list. An error on one device does not cost the
        others their removal or their report.

        Args:
            fresh_list_ids: Google ids of the list fetched in this cycle.
            dropped_candidates: List entries without a valid id in this fetch.
            canonicless_warn: The decoder's ``warn`` drop count for this fetch.
            canonicless_names: ``(raw name, kind)`` pairs the decoder dropped
                from this fetch for lack of a canonical id, with their counts
                (S-R3d).

        Returns:
            The number of devices removed.
        """
        entry = self.config_entry or getattr(self, "entry", None)
        entry_id = getattr(entry, "entry_id", None) if entry is not None else None
        if not entry_id:
            return 0

        self._reload_if_removed_devices_returned(entry, entry_id, fresh_list_ids)

        dev_reg = dr.async_get(self.hass)
        given_up = self._stale_removal_given_up
        plan = plan_stale_device_removals(
            [
                device
                for device in dr.async_entries_for_config_entry(dev_reg, entry_id)
                if device.id not in given_up
            ],
            entry_id=entry_id,
            domain=DOMAIN,
            service_identifiers=(
                service_device_identifier(entry_id),
                (DOMAIN, LEGACY_SERVICE_IDENTIFIER),
            ),
            fresh_list_ids=fresh_list_ids,
            dropped_candidates=dropped_candidates,
            canonicless_warn=canonicless_warn,
            canonicless_names=canonicless_names or {},
            previous_misses=getattr(self, "_stale_device_misses", {}),
            name_baseline=self._stale_name_baseline,
        )
        self._stale_device_misses = dict(plan.misses)
        self._stale_name_baseline = plan.name_baseline

        if plan.skipped_reason is not None:
            _LOGGER.debug(
                "[%s] stale-device removal skipped (reason=%s)",
                entry_id,
                plan.skipped_reason,
            )
            return 0

        # Polling record: the device name stays at DEBUG (AGENTS.md section 5,
        # class (b)); INFO carries the Home Assistant device id instead.
        for device, misses in plan.first_misses:
            _LOGGER.info(
                "[%s] Device %s is missing from the Google device list (%d/%d)",
                entry_id,
                device.id,
                misses,
                STALE_REMOVAL_REQUIRED_MISSES,
            )
            _LOGGER.debug(
                "[%s] Device %s missing from the Google device list is '%s'",
                entry_id,
                device.id,
                device.name_by_user or device.name,
            )

        if not plan.remove:
            return 0

        # Local import: the tracker module pulls in the decryption stack.
        from ..NovaApi.ExecuteAction.LocateTracker.foreign_reading_tracker import (
            FOREIGN_READING_TRACKER,
        )

        tracker_scope = getattr(entry, "parent_entry_id", None) or entry_id
        removed_ids: list[str] = []
        for device, google_ids in plan.remove:
            try:
                removed = self._detach_stale_device(dev_reg, device, entry_id)
            except Exception as err:  # noqa: BLE001 - one device must not cost the others
                _LOGGER.warning(
                    "[%s] Removing device %s that left the Google account failed (%s)",
                    entry_id,
                    device.id,
                    type(err).__name__,
                )
                _LOGGER.debug(
                    "[%s] Removal error detail for device %s: %s",
                    entry_id,
                    device.id,
                    describe_exception(err),
                )
                removed = False
            if not removed:
                self._count_failed_stale_removal(entry_id, device.id)
                continue
            self._stale_removal_attempts.pop(device.id, None)
            for google_id in sorted(google_ids):
                self.purge_device(google_id)
                FOREIGN_READING_TRACKER.forget_device(tracker_scope, google_id)
                self._stale_removed_google_ids.add(google_id)
            _LOGGER.debug(
                "[%s] Removed stale device %s '%s' (Google ids: %s)",
                entry_id,
                device.id,
                device.name_by_user or device.name,
                ", ".join(self._redact_text(gid, 8) for gid in sorted(google_ids)),
            )
            removed_ids.append(device.id)

        if removed_ids:
            _LOGGER.info(
                "[%s] Removed %d device(s) no longer in the Google account: %s",
                entry_id,
                len(removed_ids),
                ", ".join(removed_ids),
            )
        return len(removed_ids)

    def _detach_stale_device(self, dev_reg: Any, device: Any, entry_id: str) -> bool:
        """Detach every link of ``device`` to ``entry_id``; tell whether it left.

        Returns:
            ``True`` when the device is gone from the registry or has no link
            to ``entry_id`` any more; ``False`` when core kept it linked.
        """
        for link in sorted(
            _extract_subentry_links_impl(device, entry_id),
            key=lambda value: value or "",
        ):
            current = dev_reg.async_get(device.id)
            if current is None:
                break
            self._apply_device_ownership(
                dev_reg,
                intent=OwnershipIntent.DETACH,
                device_id=device.id,
                entry_id=entry_id,
                detach_subentry_id=link,
                device=current,
            )
        current = dev_reg.async_get(device.id)
        return current is None or not _extract_subentry_links_impl(current, entry_id)

    def _count_failed_stale_removal(self, entry_id: str, device_id: str) -> None:
        """Retry a refused or failed removal once, then give the device up.

        Nothing was removed, so nothing is purged or remembered.
        ``_apply_device_ownership`` logs a DETACH it refuses as an error, so
        the retry is bounded: after
        ``STALE_REMOVAL_MAX_ATTEMPTS`` attempts the device is left alone until
        the next setup, and that is said once at WARNING.
        """
        attempts = self._stale_removal_attempts.get(device_id, 0) + 1
        self._stale_removal_attempts[device_id] = attempts
        if attempts >= STALE_REMOVAL_MAX_ATTEMPTS:
            self._stale_removal_given_up.add(device_id)
            self._stale_device_misses.pop(device_id, None)
            _LOGGER.warning(
                "[%s] Giving up removing device %s after %d failed attempts; it "
                "stays until the integration is set up again",
                entry_id,
                device_id,
                attempts,
            )
            return
        _LOGGER.debug(
            "[%s] Device %s is still linked after detaching it; not counted as "
            "removed, the next fresh list tries again",
            entry_id,
            device_id,
        )
        # One more fresh list without it retries the removal.
        self._stale_device_misses[device_id] = STALE_REMOVAL_REQUIRED_MISSES - 1

    def _reload_if_removed_devices_returned(
        self, entry: Any, entry_id: str, fresh_list_ids: set[str]
    ) -> bool:
        """Reload the entry once when a removed device is back in the account.

        ``_remove_stale_devices`` remembers the Google ids it removed. When a
        fresh list carries one of them again, the update cycle has already
        put the device back into the registry, but no platform adds its
        entities: each keeps the ids it has set up for as long as the setup
        lives. A reload is the lever, the same one the tracker platform's
        self-heal uses, and it goes through the same shared latch so that it
        never doubles a reload that is already on its way.

        The remembered ids are forgotten once this call has scheduled the
        reload. They are kept in every other case: when the entry cannot reach
        a setup, when the schedule fails, and when another reload holds the
        latch (if that one lands it replaces this coordinator and the set with
        it; if it never lands, a later fresh list tries again, and the latch
        keeps that from doubling a reload).

        Args:
            entry: The config entry of this coordinator.
            entry_id: Its id.
            fresh_list_ids: Google ids of the list fetched in this cycle.

        Returns:
            ``True`` when this call scheduled a reload.
        """
        returned = self._stale_removed_google_ids & set(fresh_list_ids)
        if not returned:
            return False

        # Local imports: the package module imports this one.
        from .. import claim_pending_entry_reload, discard_pending_entry_reload
        from ..entry_reload_gate import entry_reload_is_hopeless

        schedule_reload = getattr(
            getattr(self.hass, "config_entries", None), "async_schedule_reload", None
        )
        if not callable(schedule_reload) or entry_reload_is_hopeless(
            self.hass, entry_id, entry
        ):
            _LOGGER.debug(
                "[%s] %d removed device(s) are back in the Google account, but the "
                "entry cannot be reloaded now",
                entry_id,
                len(returned),
            )
            return False

        if not claim_pending_entry_reload(self.hass, entry_id):
            _LOGGER.debug(
                "[%s] %d removed device(s) are back in the Google account; a reload "
                "is already on its way",
                entry_id,
                len(returned),
            )
            return False

        try:
            schedule_reload(entry_id)
        except Exception as err:  # noqa: BLE001 - a claim that comes to nothing must be given back
            discard_pending_entry_reload(self.hass, entry_id)
            _LOGGER.warning(
                "[%s] Failed to schedule the reload for devices back in the Google "
                "account (%s)",
                entry_id,
                type(err).__name__,
            )
            return False

        self._stale_removed_google_ids.clear()
        _LOGGER.info(
            "[%s] Reloading the entry once: %d removed device(s) are back in the "
            "Google account",
            entry_id,
            len(returned),
        )
        return True

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

        hub_device_id: str | None = None
        hub_device_names: set[str] = set()
        hub_devices_by_name: dict[str, Any] = {}
        hub_device = _resolve_device_by_identifiers_impl(
            dev_reg, (parent_identifier,), entry_id=entry_id
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

        # The collision check spans the whole registry on purpose: a sibling
        # hanging off our hub may belong to any config entry, and narrowing this
        # to our own entry would miss exactly the names we must not reuse. Going
        # through the helper is what keeps that true across cores -- ``devices``
        # is a plain mapping on the declared minimum ``2025.9.1`` and a view
        # from ``2026.9`` on, and an ``isinstance(..., Mapping)`` guard here
        # silently dropped the whole loop on the newer one.
        if hub_device_id:
            for device_entry in _iter_all_devices_impl(dev_reg):
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
                and _device_belongs_to_entry_impl(existing, entry_id)
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
            """Move a device off the entry root and into the tracker subentry.

            The old name says "remove", the intent is a MOVE: the device is not
            given up, it is put where it belongs. The distinction decides
            whether Core 2026.8+ relocates the device or deletes it.
            """
            if (
                not callable(update_device)
                or not entry_id
                or tracker_config_subentry_id is None
            ):
                return device
            device_id = getattr(device, "id", "")
            if not device_id:
                return device
            self._apply_device_ownership(
                dev_reg,
                intent=OwnershipIntent.MOVE,
                device_id=device_id,
                entry_id=entry_id,
                target_subentry_id=tracker_config_subentry_id,
                detach_subentry_id=None,
                device=device,
            )
            return _refresh_device_entry(device_id, device)

        def _update_device_with_kwargs(kwargs: dict[str, Any]) -> None:
            """Write metadata and make sure the device sits in our subentry.

            The ownership half is an ENSURE: the callers arrive with a device
            that may or may not already carry the tracker link, and ENSURE is
            the intent for "put it there unless it is already there". The
            metadata rides along as ``extra`` so both halves stay one registry
            write, and so that a device which already sits right still gets its
            metadata (the planner keeps ``extra`` even when the ownership plan
            itself is empty).
            """
            if not callable(update_device):
                return
            payload = dict(kwargs)
            device_id = payload.pop("device_id", None)
            if (
                not isinstance(device_id, str) or not device_id
            ):  # pragma: no cover - every caller sets it; without the guard a
                # ``device_id=None`` would reach the registry call
                return
            # No fallback for "no tracker subentry": the enclosing method
            # returns early both when ``entry_id`` is empty and when
            # ``tracker_config_subentry_id`` stays unresolved, so this condition
            # holds whenever this helper runs at all. Writing the metadata
            # without the ownership intent would be dead code that the next
            # reader has to keep alive.
            self._apply_device_ownership(
                dev_reg,
                intent=OwnershipIntent.ENSURE,
                device_id=device_id,
                entry_id=entry_id,
                target_subentry_id=tracker_config_subentry_id,
                extra=payload,
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
                    # The link to give up is named, never guessed: the planner
                    # must not pick one, and on a single-owner core an unnamed
                    # link is what turns a move into a deletion.
                    self._apply_device_ownership(
                        dev_reg,
                        intent=OwnershipIntent.MOVE,
                        device_id=device_id,
                        entry_id=entry_id,
                        target_subentry_id=tracker_config_subentry_id,
                        detach_subentry_id=link,
                        device=device,
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
                # A hub link means the device sits on the entry root and has to
                # be moved off it; without one the device only needs the tracker
                # link ensured. Both end in the tracker subentry.
                updated = self._apply_device_ownership(
                    dev_reg,
                    intent=(
                        OwnershipIntent.MOVE if has_hub_link else OwnershipIntent.ENSURE
                    ),
                    device_id=device_id,
                    entry_id=entry_id,
                    target_subentry_id=tracker_config_subentry_id,
                    # Read by MOVE only; ENSURE ignores it. Named rather than
                    # omitted because the hub link is exactly what MOVE gives up
                    # here, and in this module ``None`` and "not given" are two
                    # different statements.
                    detach_subentry_id=None,
                    device=device,
                )
                healed_device = _refresh_device_entry(device_id, updated or device)
                # Unreachable today: ``device`` is not None here and
                # ``_refresh_device_entry`` returns its fallback instead of None.
                # Kept as a guard should the helper ever start returning None.
                if healed_device is None:  # pragma: no cover - fallback never None
                    # The registry sync runs on every poll cycle, so the ERROR names
                    # the device by its registry id and the display name goes to a
                    # DEBUG companion (AGENTS.md section 5 (b)).
                    _LOGGER.error("[%s] Failed to heal device %s", entry_id, device_id)
                    _LOGGER.debug(
                        "[%s] Heal failure above concerns '%s' (device %s)",
                        entry_id,
                        device_label,
                        device_id,
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
            dev = _resolve_device_by_identifiers_impl(
                dev_reg, (ns_ident,), entry_id=entry_id
            )
            if dev is None:
                # Legacy present? Kept as a separate lookup on purpose: the two
                # results are treated differently below, so folding them into
                # one prioritised call would lose the distinction.
                legacy_dev = _resolve_device_by_identifiers_impl(
                    dev_reg, (legacy_ident,), entry_id=entry_id
                )
                if legacy_dev is not None:
                    # If legacy device belongs to THIS entry, migrate by adding namespaced ident.
                    if _device_belongs_to_entry_impl(legacy_dev, entry_id):
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
                            # Ownership is not stated here: the helper below
                            # ensures the tracker link for every device it
                            # touches, so ``needs_config_subentry`` only decides
                            # whether the write happens at all.
                            update_kwargs: dict[str, Any] = {
                                "device_id": legacy_dev.id,
                            }
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
                        # Always, not only when metadata changed: the two
                        # ``setdefault`` calls that used to stand here pushed the
                        # length past one every time, so a reused device was
                        # written even when nothing about it differed. That call
                        # is what claims the device for our subentry, and the
                        # helper now expresses the claim as an ENSURE.
                        #
                        # ``device_updated`` is set even when the planner finds
                        # the device already in place and writes nothing. The
                        # counter therefore reports "considered", not "written";
                        # it feeds a debug line and the return value, not a
                        # decision. Making it exact would mean returning the
                        # operation count from ``_apply_device_ownership``.
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
                # Keep the integration name in step with Google, also when the
                # user has named the device: core does the same, and the user
                # name stays in ``name_by_user`` and wins in the UI. The removal
                # of stale devices matches canonicless drops against this name.
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
                name_needs_update = bool(use_name) and dev.name != use_name
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
                    # Neither branch that used to stand here is needed any more.
                    # The tracker link is ensured by the helper, and the surplus
                    # hub link is taken off by the ``_remove_hub_link`` call a
                    # few lines below, which states the intent as a MOVE instead
                    # of a naked removal. That call covers the condition of the
                    # old ``elif`` (tracker link *and* hub link at once) whether
                    # or not the state can arise, so it must not be removed with
                    # it.
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
