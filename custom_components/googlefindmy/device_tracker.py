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

import inspect
import logging
import time
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

# ``automation`` and ``script`` are no dependencies (hassfest allows these two
# imports, ALLOWED_USED_COMPONENTS). Imported at module level because Home
# Assistant imports this platform in the executor; a lazy import inside the
# start callback would load the modules on the event loop.
from homeassistant.components.automation import automations_with_entity
from homeassistant.components.script import scripts_with_entity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    ATTR_LATITUDE,
    ATTR_LONGITUDE,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import DeviceInfo, EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.start import async_at_started

from . import (
    EntityRecoveryManager,
    _extract_email_from_entry,
    _opt,
    claim_pending_entry_reload,
    claim_registry_selfheal_reload,
    discard_pending_entry_reload,
    discard_registry_selfheal_reload,
)
from .const import (
    DEFAULT_SHOW_LOCATION_AGE,
    DOMAIN,
    OPT_SHOW_LOCATION_AGE,
    TRACKER_ATTRS_BREAKS_IN_HA_VERSION,
    TRACKER_SUBENTRY_KEY,
    TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN,
    TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN_NO_AUTOMATIONS,
    frozen_attributes_issue_id,
)
from .coordinator import (
    GoogleFindMyCoordinator,
    _as_ha_attributes,
    classify_location_status,
    encode_plus_code_for_row,
    is_reliable_fix,
    location_age_seconds,
    parse_last_seen_timestamp,
    recorded_accuracy_pair,
    resolve_seeded_accuracy,
    resolve_stale_threshold,
    select_display_row,
)
from .entity import (
    GoogleFindMyDeviceEntity,
    ensure_config_subentry_id,
    ensure_dispatcher_dependencies,
    known_config_subentry_ids,
    resolve_coordinator,
    schedule_add_entities,
)
from .entry_reload_gate import entry_reload_is_hopeless
from .ha_typing import RestoreEntity, TrackerEntity, callback
from .ProtoDecoders.decoder import is_allowed_image_url

_LOGGER = logging.getLogger(__name__)

# Sentinel distinguishing "caller passed no row" (fall back to the live
# current-or-cache lookup) from "caller explicitly passed a row" (which may
# legitimately be None -> no data). Without this, an explicit None argument
# would silently fall back to the internal lookup and reintroduce a second
# source of truth for the published age/status (Codex #202).
_ROW_UNSET: Any = object()

# Read-only mirror of coordinator state; no I/O performed per-entity.
PARALLEL_UPDATES = 0

# Attributes that age with the clock rather than with a new fix. The tracker
# publishes them as of the last observation (see
# GoogleFindMyDeviceTracker._hold_observation_attributes); the live age is
# on sensor.*_last_seen, the live status on sensor.*_location_status.
_HELD_ATTRIBUTES: tuple[str, ...] = ("location_age", "location_status")

# Side information about a coarse fix the accuracy gate discarded (#216). It can
# appear (the gate rejects a fix, the published position stays) and expire (the
# clock passes the stale threshold) without a new published observation, so the
# main tracker holds it with the observation too: it changes only together with
# a new observation. The live value is on the coarse-fix sensor. Unlike
# _HELD_ATTRIBUTES, an absent key does not drop the held value, because absence
# here means "expired", not "switched off by an option".
_COARSE_ATTRIBUTES: tuple[str, ...] = (
    "coarse_latitude",
    "coarse_longitude",
    "coarse_accuracy",
    "coarse_last_seen",
)

# Wall-clock grace before a fresh entity addition is judged against the entity
# registry.
#
# Nothing in the callback world announces "the entity is registered now": the
# core schedules the addition as its own task, ``update_before_add=True`` runs
# each entity's update first, and the registry entry is only written after that.
# A later coordinator listener run is no barrier either -- one poll cycle can
# publish a per-device push and its closing snapshot back to back without ever
# yielding, so the very next run may still see nothing (Codex, PR #1222).
# Deferring by wall clock is the only signal that outlives both.
#
# Generous on purpose. Reloading an entry that was fine is worse than healing a
# genuinely broken addition a minute late, and the value stays far below the
# device-list cadence (DEVICE_LIST_POLL_INTERVAL, 300 s).
_REGISTRY_PROBE_DELAY = 60.0


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


# Unique-id suffixes of the per-device sensors that carry the live values of the
# held tracker attributes. The issue lists their registered entity ids: Home
# Assistant derives the object id from the translated entity name for most
# languages, so a literal such as ``sensor.*_coarse_fix`` would not exist there.
_FROZEN_ATTRIBUTES_REPLACEMENT_SUFFIXES = (
    "_location_status",
    "_last_seen",
    "_coarse_fix",
)


@callback
def _async_track_frozen_attributes_issue(
    hass: HomeAssistant, config_entry: ConfigEntry
) -> None:
    """Show the held-attributes issue in Repairs while this entry is loaded.

    The list of automations and scripts is computed once Home Assistant has
    started: ``automation`` and ``script`` need not be loaded while this
    platform sets up (they would report no references), and neither is a
    dependency of the integration. The tracker and replacement sensor entity
    ids come from the entity registry, not from runtime entities. On a
    running Home Assistant (an entry added from the UI) the start callback
    runs at once, before any entity of the entry is registered; such an entry
    has no tracker of its own yet and gets no issue until the next start.
    Unloading the entry cancels a pending start callback but keeps the issue,
    so a reload does not undo the user's
    "ignore"; disabling the entry deletes it, ``async_remove_entry`` deletes
    it, and a non-persistent issue of an entry that is no longer set up turns
    inactive with the next start.
    """
    issue_id = frozen_attributes_issue_id(config_entry.entry_id)

    @callback
    def _create_issue(_hass: HomeAssistant) -> None:
        registered = er.async_entries_for_config_entry(
            er.async_get(hass), config_entry.entry_id
        )
        trackers = [
            entry.entity_id for entry in registered if entry.domain == "device_tracker"
        ]
        if not trackers:
            _LOGGER.debug(
                "Held-attributes repair issue skipped: no tracker registered yet"
            )
            return
        replacements = sorted(
            entry.entity_id
            for entry in registered
            if entry.domain == "sensor"
            and entry.unique_id.endswith(_FROZEN_ATTRIBUTES_REPLACEMENT_SUFFIXES)
        )
        users = sorted(
            {
                reference
                for tracker in trackers
                for reference in (
                    *automations_with_entity(hass, tracker),
                    *scripts_with_entity(hass, tracker),
                )
            }
        )
        placeholders = {
            "entry_title": config_entry.title or config_entry.entry_id,
            "replacement_sensors": "\n".join(f"- {ref}" for ref in replacements)
            # Not "-": under the sentence it would turn it into a heading.
            or "\u2026",
            "breaks_version": TRACKER_ATTRS_BREAKS_IN_HA_VERSION,
        }
        translation_key = TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN_NO_AUTOMATIONS
        if users:
            translation_key = TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN
            placeholders["automations"] = "\n".join(f"- {ref}" for ref in users)
        ir.async_create_issue(
            hass,
            DOMAIN,
            issue_id,
            breaks_in_ha_version=TRACKER_ATTRS_BREAKS_IN_HA_VERSION,
            is_fixable=False,
            is_persistent=False,
            severity=ir.IssueSeverity.WARNING,
            translation_key=translation_key,
            translation_placeholders=placeholders,
        )

    if not hasattr(hass, "state") or not hasattr(hass, "bus"):
        # A hass double without the start surface (fast test stubs): the issue
        # is advisory, so skip it rather than fail the platform setup. No
        # exception is caught here, so a failure while creating the issue on
        # a running Home Assistant still surfaces.
        _LOGGER.debug("Held-attributes repair issue not scheduled: no start state")
        return
    config_entry.async_on_unload(async_at_started(hass, _create_issue))

    @callback
    def _delete_issue_if_disabled() -> None:
        # Home Assistant sets ``disabled_by`` before it unloads the entry.
        if getattr(config_entry, "disabled_by", None) is not None:
            ir.async_delete_issue(hass, DOMAIN, issue_id)

    config_entry.async_on_unload(_delete_issue_if_disabled)


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
    _async_track_frozen_attributes_issue(hass, config_entry)

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

        # Device ids added but not yet confirmed in the entity registry. They are
        # judged once ``_REGISTRY_PROBE_DELAY`` has passed, never in the run that
        # adds them and never merely on "some later listener run": neither is a
        # barrier for the add task (see the constant's comment).
        pending_registry_ids: set[str] = set()
        registry_probe_unsub: Callable[[], None] | None = None

        @callback
        def _cancel_registry_probe() -> None:
            """Drop a pending grace timer so it cannot fire after unload."""

            nonlocal registry_probe_unsub

            if registry_probe_unsub is not None:
                registry_probe_unsub()
                registry_probe_unsub = None

        # Home Assistant runs these callbacks only in the success branch of
        # ``ConfigEntry.async_unload``; a failed unload leaves the entry in
        # ``FAILED_UNLOAD`` *and* leaves this timer armed. That is why
        # ``_schedule_selfheal_reload`` consults the state gate before it
        # promises anything: this probe is the claimant that outlives the very
        # dead end the latch cannot recover from (core 2026.2.3).
        config_entry.async_on_unload(_cancel_registry_probe)

        @callback
        def _schedule_selfheal_reload(missing_count: int) -> None:
            """Own the whole claim-then-schedule sequence of the self-heal reload.

            Two latches guard this one call and they answer different questions.
            ``claim_registry_selfheal_reload`` asks whether this entry has ever
            healed itself, which keeps a permanently empty registry from
            reloading in a loop. ``claim_pending_entry_reload`` asks whether some
            other owner already put a reload on its way, which matters because
            ``async_schedule_reload`` does not coalesce: a credential write and
            this probe would otherwise tear the entry down twice in a row.

            Order is not cosmetic, and it is four steps. The lever is resolved
            *before* any claim, so an old core without the method does not burn
            the one-shot attempt. The state gate comes next, for the same
            reason: the one-shot is handed back only on the two failure paths
            below, which have taken it themselves, and in ``async_remove_entry``.
            A hopeless verdict returns before any claim and hands nothing back,
            so an attempt spent there would be spent for the life of that entry.
            Behind the one-shot the gate would need a third release path of its
            own for no gain. And
            the shared latch is claimed *last*, immediately before scheduling,
            as its own contract demands.

            The gate matters here more than anywhere else: this is the one
            claimant whose timer outlives a failed unload, because
            ``async_on_unload`` callbacks only run once the unload succeeded.
            """

            schedule_reload = getattr(
                getattr(hass, "config_entries", None), "async_schedule_reload", None
            )
            if not callable(schedule_reload):
                _LOGGER.debug(
                    "Device tracker setup: async_schedule_reload unavailable; "
                    "leaving %d unregistered tracker(s) as they are",
                    missing_count,
                )
                return

            if entry_reload_is_hopeless(hass, config_entry.entry_id, config_entry):
                # Nothing has been claimed yet, so there is nothing to hand
                # back. The entry object we already hold is passed on purpose:
                # this platform's hass double carries no entry manager, and the
                # gate must not depend on one to reach its verdict.
                _LOGGER.debug(
                    "Device tracker setup: %d tracker(s) still missing from the "
                    "entity registry, but entry %s cannot reach a setup; not "
                    "promising a reload",
                    missing_count,
                    config_entry.entry_id,
                )
                return

            if not claim_registry_selfheal_reload(hass, config_entry.entry_id):
                _LOGGER.debug(
                    "Device tracker setup: %d tracker(s) still missing from the "
                    "entity registry, but this entry already used its one-shot "
                    "reload; not scheduling another",
                    missing_count,
                )
                return

            if not claim_pending_entry_reload(hass, config_entry.entry_id):
                # Someone else's reload rebuilds this platform exactly like ours
                # would, so hand the one-shot back instead of spending the
                # entry's only attempt on a reload we did not cause. A probe
                # after that reload can then still repair what it left missing.
                discard_registry_selfheal_reload(hass, config_entry.entry_id)
                _LOGGER.debug(
                    "Device tracker setup: %d tracker(s) still missing from the "
                    "entity registry, but a reload of this entry is already on "
                    "its way; standing down",
                    missing_count,
                )
                return

            try:
                schedule_reload(config_entry.entry_id)
            except Exception:  # noqa: BLE001 - both latches are promises to reload
                # A latch kept after a failed schedule is worse than the failure
                # itself: the shared one swallows every later reload of this
                # entry and the one-shot one would leave the trackers unhealed.
                _LOGGER.exception(
                    "Failed to schedule the self-heal reload of entry %s",
                    config_entry.entry_id,
                )
                discard_pending_entry_reload(hass, config_entry.entry_id)
                discard_registry_selfheal_reload(hass, config_entry.entry_id)
                return

            # Announced only once it is really on its way: a message that names
            # a reload which the line above failed to schedule reads like a
            # cause where there is none.
            _LOGGER.info(
                "Reloading the Find My entry once: %d newly added tracker(s) did "
                "not reach the entity registry",
                missing_count,
            )

        @callback
        def _promote_registered_trackers(registered_count: int) -> None:
            """Let trackers that reached the registry into the polling set.

            The coordinator derives ``_enabled_poll_device_ids`` from the
            *entity* registry but rebuilds it only on *device* registry events,
            and a brand-new tracker gets its device entry before its entity
            exists. The rebuild that device entry triggers therefore records the
            device as present and not enabled, and the polling predicate admits
            a device only while it is enabled or still absent from the registry:
            from the next cycle on the silently added tracker would be skipped
            for good, entity and all.

            This probe is the point where the addition is known to have landed,
            so it is also the point where that verdict has to be re-derived.
            """

            reindex = getattr(coordinator, "reindex_poll_targets", None)
            if not callable(reindex):
                # Not a detail: without the re-derivation those trackers own an
                # entity that is never polled again, so this is warned about
                # rather than whispered into a debug log.
                _LOGGER.warning(
                    "Device tracker setup: coordinator cannot reindex poll "
                    "targets; %d newly registered tracker(s) stay out of the "
                    "polling set until the next reload",
                    registered_count,
                )
                return

            try:
                reindex()
            except Exception:  # noqa: BLE001 - never let this eat the self-heal
                _LOGGER.warning(
                    "Device tracker setup: reindexing poll targets after %d "
                    "registered tracker(s) failed; they stay out of the polling "
                    "set until the next reload",
                    registered_count,
                    exc_info=True,
                )

        @callback
        def _verify_pending_registry_entries() -> None:
            """Confirm earlier additions, or reload the entry once to fix them.

            The reload is the only lever that helps here. Re-polling the device
            list does not: ``known_ids`` already holds those ids, so
            ``_build_entities`` returns nothing and no entity is created again.
            """

            if not pending_registry_ids:
                return

            probe_ids = sorted(pending_registry_ids)
            pending_registry_ids.clear()

            registry_lookup = getattr(coordinator, "find_tracker_entity_entry", None)
            if not callable(registry_lookup):
                _LOGGER.debug(
                    "Device tracker setup: registry helper unavailable; cannot "
                    "verify %d recently added tracker(s)",
                    len(probe_ids),
                )
                return

            missing: list[str] = []
            for dev_id in probe_ids:
                try:
                    if registry_lookup(dev_id) is None:
                        missing.append(dev_id)
                except Exception:  # pragma: no cover - best effort registry probe
                    _LOGGER.debug("Registry lookup failed for tracker %s", dev_id)
                    missing.append(dev_id)

            registered_count = len(probe_ids) - len(missing)
            if registered_count:
                # Independent of the missing-check below, not of the count: with
                # nothing registered there is nothing to promote. Running it even
                # when some trackers are still missing is the point -- a self-heal
                # reload can stand down (no lever, or another owner holds the
                # latch), and the trackers that did register must not wait for a
                # reload that may never come.
                _promote_registered_trackers(registered_count)

            if not missing:
                return

            _schedule_selfheal_reload(len(missing))

        @callback
        def _registry_probe_due(_now: Any) -> None:
            """Grace period is over: judge what was added before it started."""

            nonlocal registry_probe_unsub

            registry_probe_unsub = None
            _verify_pending_registry_entries()

        @callback
        def _arm_registry_probe() -> None:
            """(Re-)start the grace period so the newest addition gets it whole.

            Re-arming means a device appearing every few seconds would keep
            pushing the probe out. That is the harmless direction: the probe
            only ever schedules a repair, and a device stream that dense is a
            different problem than the one this path exists for.
            """

            nonlocal registry_probe_unsub

            _cancel_registry_probe()
            handle = async_call_later(
                coordinator.hass, _REGISTRY_PROBE_DELAY, _registry_probe_due
            )
            if inspect.isawaitable(handle):
                # Mirrors the awaitable guard the integration already uses for
                # async_call_later elsewhere; such a stub leaves no unsub to keep.
                coordinator.hass.async_create_task(
                    cast(Coroutine[Any, Any, Any], handle),
                    name=f"{DOMAIN}.device_tracker_registry_probe",
                )
                return
            if callable(handle):
                registry_probe_unsub = handle

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
                        "post_reconfigure=%s)"
                    ),
                    tracker_subentry_key,
                    getattr(meta, "config_subentry_id", None),
                    ", ".join(visible_ids) or "<empty>",
                    ", ".join(enabled_ids) or "<empty>",
                    ", ".join(snapshot_keys) or "<empty>",
                    len(snapshot_map) if isinstance(snapshot_map, Mapping) else 0,
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

                # No discovery flow, no card, no user click: a new tracker is an
                # entity, not an account. Only remember the ids and start the
                # grace period after which the addition is judged.
                armed = False
                for entity in to_add:
                    dev_id = getattr(entity, "device_id", None)
                    if isinstance(dev_id, str) and dev_id:
                        pending_registry_ids.add(dev_id)
                        armed = True
                if armed:
                    _arm_registry_probe()

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
    _attr_entity_category: EntityCategory | None = (
        None  # ensure tracker is not diagnostic
    )
    # Default to enabled in the registry for per-device trackers
    _attr_entity_registry_enabled_default = True
    _attr_translation_key = "device"
    _attr_attribution: str | None = None  # Set in __init__ with account email
    # location_age is held between observations (see
    # _hold_observation_attributes) but still takes a new value with every
    # observation; keep it out of recorder history (HA developer guidance,
    # 2023-09 / Entity docs). The show_location_age toggle and the 60 s rounding
    # stay, but even mobile devices should not bloat the state_attributes table
    # here.
    _unrecorded_attributes = frozenset({"location_age"})
    # The main tracker carries the held coarse-fix side information; the
    # _last_location tracker never goes stale and carries none.
    _publishes_coarse_fix = True
    # Google's product image of the device model (``get_device_image_url``). The
    # browser loads it directly from Google; see the README. Only the main
    # tracker shows it, so the two trackers of one device stay distinguishable.
    _publishes_entity_picture = True
    _attr_entity_picture: str | None = None

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
        # Observation signature of the last published fix and the values of
        # _HELD_ATTRIBUTES published with it; see _hold_observation_attributes().
        self._observation_sig: tuple[Any, ...] | None = None
        self._held_attributes: dict[str, Any] = {}

    async def async_added_to_hass(self) -> None:
        """Restore the last known location, then seed the coordinator cache.

        On cold boots where the coordinator hasn't polled yet, this restores the
        last coordinates to give a better initial UX and primes the coordinator's
        cache via its public priming API. It runs *before*
        ``super().async_added_to_hass()`` so the single state publish that call
        triggers already reflects the restored fix.
        """
        try:
            last_state = await self.async_get_last_state()
        except (RuntimeError, AttributeError) as err:
            _LOGGER.debug("Failed to get last state for %s: %s", self.entity_id, err)
            last_state = None

        if not last_state:
            await super().async_added_to_hass()
            return

        # Standard device_tracker attributes. Fresh states expose the position via
        # the plain latitude/longitude keys; stale/blank states withhold them (the
        # producer strips them so a withheld position is not republished as the
        # live GPS attribute) and keep the last known fix in the recorder-only
        # last_latitude/last_longitude keys. Fall back to those so a restart still
        # recovers the last position.
        lat = last_state.attributes.get(
            ATTR_LATITUDE, last_state.attributes.get("last_latitude")
        )
        lon = last_state.attributes.get(
            ATTR_LONGITUDE, last_state.attributes.get("last_longitude")
        )
        # Read the accuracy value AND its estimated-provenance flag from the
        # same authoritative source as map_view and the snapshot builders:
        # accuracy_m (the stable producer attribute) over the volatile core
        # gps_accuracy, which Home Assistant clears on stale states. Reading the
        # value from gps_accuracy alone dropped a real accuracy_m radius (and,
        # guarded by ``acc is not None`` below, the paired flag) when reseeding
        # the cache after a restart, the same decoupling closed elsewhere in
        # PR #1124. Carrying the flag keeps a restored fallback radius
        # distinguishable from a real measurement of the same value; legacy rows
        # recorded before the flag carry estimated=None and we deliberately do
        # not fabricate one (the downstream map_view legacy fallback handles
        # that documented case).
        acc, estimated = recorded_accuracy_pair(last_state.attributes)

        restored: dict[str, Any] = {}
        try:
            if lat is not None and lon is not None:
                restored["latitude"] = float(lat)
                restored["longitude"] = float(lon)
            if acc is not None:
                # Seed the coordinator cache, whose ``accuracy`` field is a
                # sanitized float everywhere else (api/locate/fusion writers).
                # resolve_seeded_accuracy couples sanitization with provenance
                # the way the canonical _is_significant_update writer does
                # (which this direct prime bypasses): it normalizes through the
                # producer policy (safe_accuracy) -- avoiding the int() truncation
                # that would drop a restored sub-meter real fix (e.g. 0.5m -> 0)
                # below MIN_VALID_ACCURACY -- AND marks accuracy_estimated when
                # that sanitization had to fall back, so a fabricated 200m radius
                # cannot enter flagless and be reclassified as a real fix. An
                # explicitly recorded flag still wins; a legacy *valid* value
                # without a flag stays unflagged (map_view legacy fallback).
                accuracy_value, estimated_flag = resolve_seeded_accuracy(acc, estimated)
                restored["accuracy"] = accuracy_value
                if estimated_flag is not None:
                    restored["accuracy_estimated"] = estimated_flag
        except (TypeError, ValueError) as ex:
            _LOGGER.debug("Invalid restored coordinates for %s: %s", self.entity_id, ex)
            restored = {}

        if not restored:
            await super().async_added_to_hass()
            return

        # Restore last_seen so the recovered fix carries its true age; without
        # it _is_location_stale() assumes "not stale" and a stale restored
        # position would look fresh until the next poll.
        last_seen = parse_last_seen_timestamp(last_state.attributes.get("last_seen"))
        if last_seen is None:
            last_seen = parse_last_seen_timestamp(
                last_state.attributes.get("last_seen_utc")
            )
        if last_seen is not None:
            restored["last_seen"] = last_seen

        # Same retention gate as the operational path and the coordinator
        # prime below (is_reliable_fix + bootstrap): a restored estimated
        # fix seeds the still-empty cache (``is None``) but must not overwrite
        # a reliable fix once one exists, so the tracker's private last-good
        # and the coordinator's stay in sync by construction.
        if is_reliable_fix(restored) or self._last_good_accuracy_data is None:
            self._last_good_accuracy_data = {**restored}
        # Prime coordinator cache using its public API (no private access).
        dev_id = self.device_id
        try:
            self.coordinator.prime_device_location_cache(dev_id, restored)
        except (AttributeError, TypeError) as err:
            _LOGGER.debug(
                "Failed to seed coordinator cache for %s: %s", self.entity_id, err
            )

        # Delegate to super() now that the cache is primed: its one state
        # publish reflects the restored fix.
        await super().async_added_to_hass()

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
        """Return the configured stale threshold in seconds.

        Delegates to the shared SSOT reader so the tracker and the Plus Code
        sensor resolve the threshold identically.
        """
        return resolve_stale_threshold(self.coordinator)

    def _select_display_row(self) -> dict[str, Any] | None:
        """Return the single row whose data is actually published.

        This is the one source of truth for every published value (coordinates,
        name, age, status, extra attributes): the current row if it carries a
        usable accuracy, otherwise the last accuracy-bearing fix. A fresh update
        can arrive with valid lat/lon yet no accuracy; publishing coordinates
        from the cached fix while deriving age/status/attributes from the
        accuracy-less current row would show cached coordinates with fresh
        metadata (Codex #202). Binding every consumer to this row keeps the
        published snapshot internally consistent.
        """
        return select_display_row(self._current_row(), self._last_good_accuracy_data)

    def _get_location_age(self, row: Any = _ROW_UNSET) -> float | None:
        """Return the age of the location data in seconds, or None if unknown.

        With no argument the age reflects the live current-or-cache lookup (used
        by the liveness gate). When an explicit ``row`` is passed it is used
        verbatim, so callers can tie the published age to the same row that
        produced the coordinates (``_ROW_UNSET`` distinguishes "omitted" from an
        explicit ``None``).
        """
        data = (
            (self._current_row() or self._last_good_accuracy_data)
            if row is _ROW_UNSET
            else row
        )
        return location_age_seconds(data, time.time())

    def _is_location_stale(self) -> bool:
        """Return True if the location data is considered stale."""
        age = self._get_location_age()
        if age is None:
            return False  # No age information, assume not stale
        threshold = self._get_stale_threshold()
        return age > threshold

    def _get_location_status(self, row: Any = _ROW_UNSET) -> str:
        """Return the location status string based on data age.

        Accepts an explicit ``row`` so the published status describes the age of
        the same row that produced the coordinates (Codex #202).
        """
        return classify_location_status(
            self._get_location_age(row), self._get_stale_threshold()
        )

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

    def _entity_picture_url(self) -> str | None:
        """Return the image address to publish as ``entity_picture``, or None.

        Checked again here so that only an ``https`` address on an allowed host
        ever reaches the frontend, whatever the coordinator holds.
        """
        if not self._publishes_entity_picture:
            return None
        getter = getattr(self.coordinator, "get_device_image_url", None)
        if not callable(getter):
            return None
        value = getter(self.device_id)
        return value if is_allowed_image_url(value) else None

    def _sync_location_attrs(self) -> None:
        """Recompute and publish every TrackerEntity attribute via _attr_*.

        Must be called before *every* ``async_write_ha_state()`` so that
        HA's ``CachedProperties`` mechanism picks up fresh values. The values
        that change with the clock rather than with a new fix
        (``_HELD_ATTRIBUTES`` and ``_COARSE_ATTRIBUTES``) are published as of
        the last observation; see ``_hold_observation_attributes``.
        """
        stale = self._is_location_stale()

        # Published through the entity_picture property, which Home Assistant
        # adds to the state attributes itself; never part of _as_ha_attributes.
        self._attr_entity_picture = self._entity_picture_url()

        # Single source of truth for every published value below (coordinates,
        # name, age, status, extra attributes). Binding them all to this one
        # row prevents publishing cached coordinates with fresh metadata from an
        # accuracy-less current row (Codex #202). The liveness gate above stays
        # separate on purpose: a live device sending accuracy-less rows must
        # keep showing its last good position instead of flapping to "unknown".
        display_data = self._select_display_row()

        # --- latitude / longitude / location_accuracy ---
        if stale:
            self._attr_latitude = None
            self._attr_longitude = None
            self._attr_location_accuracy = 0.0
            self._attr_location_name = None
        else:
            # ``display_data`` already prefers the current row and only falls
            # back to the cached accuracy-bearing fix when the current row lacks
            # a usable accuracy. A row without accuracy cannot be published:
            # HA's zone engine raises TypeError on comparison, so blank the
            # coordinates in that case.
            if not display_data or display_data.get("accuracy") is None:
                self._attr_latitude = None
                self._attr_longitude = None
                self._attr_location_accuracy = 0.0
            else:
                self._attr_latitude = display_data.get("latitude")
                self._attr_longitude = display_data.get("longitude")
                acc = display_data.get("accuracy")
                try:
                    self._attr_location_accuracy = (
                        float(acc) if acc is not None else 0.0
                    )
                except (TypeError, ValueError):
                    self._attr_location_accuracy = 0.0

            # --- location_name (same display row as the coordinates) ---
            if not display_data:
                self._attr_location_name = None
            else:
                lat = display_data.get("latitude")
                lon = display_data.get("longitude")
                sem = display_data.get("semantic_name")
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

        # --- extra_state_attributes (bound to the SAME display row) ---
        # Deriving from display_data keeps the attribute set from exposing a
        # position that differs from the published coordinates (Codex #202).
        attributes: dict[str, Any] = _as_ha_attributes(display_data) or {}
        # Decouple the live GPS keys from the recorder-only keys. When the live
        # coordinates are withheld (_attr_latitude is None: stale, or a display
        # row without usable accuracy), HA's TrackerEntity.state_attributes omits
        # latitude/longitude -- but extra_state_attributes is merged last
        # (entity.py: attr |= extra_state_attributes), so leaving the plain
        # latitude/longitude keys here would republish the withheld position as
        # the live GPS attribute. closest()/proximity/the built-in map would then
        # treat a stale fix as the current location (Codex #1177). Strip the plain
        # keys (via the HA constants, same house-style as the timestamp sensor)
        # and expose the last known position through the recorder-only
        # last_latitude/last_longitude keys below. accuracy_m/altitude_m are not
        # HA GPS attributes and stay untouched, so restore/history/snapshot keep
        # the accuracy radius (Codex #202 stays fixed: no divergent live position).
        if self._attr_latitude is None:
            attributes.pop(ATTR_LATITUDE, None)
            attributes.pop(ATTR_LONGITUDE, None)
        attributes["google_device_id"] = self.device_id

        location_age = self._get_location_age(display_data)
        # Use the _opt() helper so options-first lookups stay consistent with
        # the rest of the integration and mypy --strict no longer flags the
        # config_entry attribute access (Any | None has no attribute options).
        show_location_age: bool = bool(
            _opt(
                self.coordinator.config_entry,
                OPT_SHOW_LOCATION_AGE,
                DEFAULT_SHOW_LOCATION_AGE,
            )
        )
        if show_location_age and location_age is not None:
            # Round to the nearest 1 minute (60 s) to reduce unnecessary
            # database writes for stationary devices while staying close to
            # the real value.
            attributes["location_age"] = int(round(location_age / 60.0) * 60)
        # When show_location_age is False the attribute is omitted entirely.
        # Absolute timestamps remain available via the "last_seen" attribute
        # and the sensor.*_last_seen entity.
        attributes["location_status"] = self._get_location_status(display_data)

        if self._attr_latitude is None and display_data:
            # Live coordinates are withheld (stale, or a display row without
            # usable accuracy -- both blank _attr_latitude, not just stale);
            # surface the last known position via dedicated recorder-only keys
            # instead (same display row). These feed the restore/history/snapshot
            # readers, which fall back to them when the plain latitude/longitude
            # keys are absent (stripped above).
            last_lat = display_data.get("latitude")
            last_lon = display_data.get("longitude")
            if last_lat is not None:
                attributes["last_latitude"] = last_lat
            if last_lon is not None:
                attributes["last_longitude"] = last_lon

        # Plus Code of the last known position. Sourced from the coordinator
        # display accessor (not the tracker-private display_data) so it is
        # value-identical to the standalone Plus Code sensor by construction
        # (single source + shared encoder). String attribute -> no second map
        # marker; recorded like last_latitude/last_longitude (changes only with
        # the coordinate), so deliberately NOT in _unrecorded_attributes.
        plus_code = encode_plus_code_for_row(
            self.coordinator.get_display_location_data_for_subentry(
                self.subentry_key, self.device_id
            )
        )
        if plus_code is not None:
            attributes["plus_code"] = plus_code

        # Coarse fix the accuracy gate discarded (#216). Exposed as side
        # information only: deliberately NOT as latitude/longitude, so Home
        # Assistant's zone logic never sees it. That logic is the actual damage
        # the gate prevents - it treats the accuracy radius as the zone
        # tolerance (``zone_dist - zone_radius < radius``), so a 460 m fix 346 m
        # away from a 32 m home zone would be published as "home".
        #
        # It is kept rather than dropped because a coarse fix still names the
        # city, and without any fix we would not even know that. Read from the
        # coordinator (single writer, in the fusion), never written here.
        #
        # Read through ``get_fresh_coarse_fix``, which is the accessor the contract
        # reserves for publishers; ``get_coarse_fix`` is the raw window for producers
        # and tests. Calling the raw one and repeating the age rule here made this
        # entity a second owner of that policy, and two owners of one rule drift.
        coarse = None
        get_coarse = getattr(self.coordinator, "get_fresh_coarse_fix", None)
        if self._publishes_coarse_fix and callable(get_coarse):
            coarse = get_coarse(self.device_id)
        if coarse:
            # The SECOND LINE, not the rule. The accessor above already applies the
            # shared ``stale_threshold`` and already treats a negative age as corrupt;
            # this repeats it because reading a position is where the damage would show,
            # and because a coordinator double without the accessor would otherwise
            # publish an unchecked fix. It must stay identical to the rule it echoes.
            coarse_age = location_age_seconds(coarse, time.time())
            threshold = resolve_stale_threshold(self.coordinator)
            # A NEGATIVE age means the stamp lies in the future, i.e. it is
            # corrupt - not "extremely fresh". The producer already refuses to
            # retain such a fix; this is the second line, because reading a
            # position is where the damage would show.
            if coarse_age is None or coarse_age < 0 or coarse_age > threshold:
                coarse = None
        if coarse:
            coarse_lat = coarse.get("latitude")
            coarse_lon = coarse.get("longitude")
            if coarse_lat is not None and coarse_lon is not None:
                attributes["coarse_latitude"] = coarse_lat
                attributes["coarse_longitude"] = coarse_lon
            if coarse.get("accuracy") is not None:
                attributes["coarse_accuracy"] = coarse["accuracy"]
            # No guard on ``last_seen``: reaching this point already proves it is
            # present and float-coercible, because ``location_age_seconds``
            # returns ``None`` otherwise and the expiry check above then drops
            # the fix. A second check here would be an unreachable branch that
            # reads like a real one.
            attributes["coarse_last_seen"] = coarse["last_seen"]

        self._attr_extra_state_attributes = attributes
        self._hold_observation_attributes(attributes)

    @property
    def force_update(self) -> bool:
        """Report a write with unchanged state and attributes, don't change it.

        ``TrackerEntity`` returns ``not self.should_poll`` here, i.e. ``True``
        for this push entity, and Core then treats every write as a change: a
        new ``last_updated`` on every coordinator tick. HA's ``person``
        integration picks the tracker with the newest ``last_updated`` (not
        ``last_seen``), so that let a tracker without a new fix outrank a
        genuinely fresher one. With ``False`` an identical write only advances
        ``last_reported`` (``state_reported``); a changed attribute or a zone
        change of the unchanged position is still a ``state_changed``.
        """
        return False

    def _state_signature(self) -> tuple[Any, ...]:
        """Return the values that identify the *observation* being published.

        Derived from ``_attr_extra_state_attributes`` (the exact dict
        ``_sync_location_attrs()`` publishes) rather than a hand-maintained
        attribute list, so a future attribute is automatically covered.

        ``_HELD_ATTRIBUTES`` and ``_COARSE_ATTRIBUTES`` are excluded: they
        change with the clock or the coarse-fix window, not with a new
        published fix, so they must not make a tick look like a new
        observation.
        """
        # No class-level default for this attr in HA core, so a bare read can
        # raise AttributeError before the first _sync_location_attrs() write;
        # mirror core's own hasattr() guard.
        attrs = getattr(self, "_attr_extra_state_attributes", None) or {}
        extra_items = tuple(
            sorted(
                (key, value)
                for key, value in attrs.items()
                if key not in _HELD_ATTRIBUTES and key not in _COARSE_ATTRIBUTES
            )
        )
        return (
            self._attr_latitude,
            self._attr_longitude,
            self._attr_location_accuracy,
            self._attr_location_name,
            self.available,
            self._device.get("name"),
            extra_items,
        )

    def _hold_observation_attributes(self, attributes: dict[str, Any]) -> None:
        """Publish the clock-driven values of the last observation until a new one.

        While the observation signature is unchanged, ``location_age``,
        ``location_status`` and the ``coarse_*`` side information keep the
        values published with that observation, so a coordinator tick writes
        identical attributes and Core only reports it (see ``force_update``). A
        new fix, a stale transition (it withholds the coordinates) or a change
        of availability changes the signature and publishes fresh values. The
        live age belongs to ``sensor.*_last_seen``, the live status to
        ``sensor.*_location_status``, the live coarse fix to the coarse-fix
        sensor; ``attributes`` is updated in place.
        """
        signature = self._state_signature()
        if signature != self._observation_sig:
            self._observation_sig = signature
            self._held_attributes = {
                key: attributes[key]
                for key in (*_HELD_ATTRIBUTES, *_COARSE_ATTRIBUTES)
                if key in attributes
            }
            return
        # A coarse fix that appeared or expired since the observation waits for
        # the next one: drop the live keys and republish the held ones.
        for key in _COARSE_ATTRIBUTES:
            attributes.pop(key, None)
            if key in self._held_attributes:
                attributes[key] = self._held_attributes[key]
        for key in _HELD_ATTRIBUTES:
            if key not in attributes:
                # Omitted on purpose (``show_location_age`` switched off): never
                # republish a held value the current options withhold.
                self._held_attributes.pop(key, None)
            elif key in self._held_attributes:
                attributes[key] = self._held_attributes[key]
            else:
                # Newly published (option switched on): hold from now on.
                self._held_attributes[key] = attributes[key]

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

        if lat is not None and lon is not None and is_reliable_fix(device_data):
            # Only cache *reliable* (non-estimated) fixes; the read-path fallback
            # in _sync_location_attrs relies on this cache holding a usable fix
            # (its name is _last_good_ACCURACY_data). A sanitized accuracy-less
            # fix carries the 200 m fallback with accuracy_estimated=True, so a
            # plain ``acc is not None`` check would let it overwrite the last
            # reliable position and poison the fallback (kept in sync with the
            # coordinator's is_reliable_fix retention gate, #1179).
            self._last_good_accuracy_data = device_data.copy()
        elif self._last_good_accuracy_data is None:
            # Bootstrap only: preserve the first update (semantic-only,
            # accuracy-less or estimated) when nothing reliable has been seen
            # yet, so the entity still has *something* to report.
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
    # Never stale, so an expiring coarse fix would be a timestamp move on a
    # tracker that otherwise holds still; the coarse-fix sensor carries it.
    _publishes_coarse_fix = False
    # No product image: it would make this tracker look like the main one.
    _publishes_entity_picture = False

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

    def _get_location_status(self, row: Any = _ROW_UNSET) -> str:
        """Return location status - always shows 'last_known' when data is aged.

        Mirrors the parent's ``row`` parameter so the published status describes
        the same row that produced the coordinates (Codex #202).
        """
        status = super()._get_location_status(row)
        return "last_known" if status == "stale" else status
