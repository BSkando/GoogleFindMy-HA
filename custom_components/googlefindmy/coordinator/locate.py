"""Locate operations for GoogleFindMyCoordinator.

This module contains location-related methods extracted from main.py.

Methods moved here:
- _normalize_coords: Validate and normalize latitude/longitude
- can_play_sound: Check if Play Sound is enabled for device
- _get_device_lock: Get or create device-specific lock
- can_request_location: Check if manual locate is allowed
- async_locate_device: Locate a device using the native async API
- async_play_sound: Play sound on a device
- async_stop_sound: Stop sound on a device
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Mapping
from typing import Any

from aiohttp import ClientConnectionError, ClientError
from homeassistant.exceptions import ConfigEntryAuthFailed, HomeAssistantError

from .._reauth_reason import ReauthReasonCode
from ..const import (
    DEFAULT_MIN_POLL_INTERVAL,
    STOP_SOUND_MIN_DELAY_AFTER_PLAY_S,
    PlaySoundOutcome,
    SoundDispatchOutcome,
    StopSoundOutcome,
    StopSoundUncorrelatedReason,
)
from ..NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
    DecryptionError,
    OwnerKeyLookupTransientError,
    StaleOwnerKeyError,
)
from ..NovaApi.ExecuteAction.LocateTracker.location_request import (
    LocationRequestNotAcceptedError,
)
from ..NovaApi.nova_request import (
    NovaAuthError,
    NovaHTTPError,
    NovaLogicError,
    NovaProtobufDecodeError,
    NovaRateLimitError,
    is_credential_rejection,
)
from ..SpotApi.spot_request import SpotAuthPermanentError
from ._mixin_typing import _MixinBase
from .helpers.cache import (
    SOUND_UUID_MAX_AGE_S,
    carry_reused_accuracy,
    is_sound_uuid_expired,
    substitute_zone_accuracy,
)
from .helpers.geo import MIN_PHYSICAL_ACCURACY_M
from .helpers.locate_clock import ensure_locate_stamps, stamp_on_dispatch

_LOGGER = logging.getLogger(__name__)

# Cooldown guardrails for owner purge window
_COOLDOWN_OWNER_MIN_S = 60.0
_COOLDOWN_OWNER_MAX_S = 600.0


def _clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp a value between min and max."""
    return max(min_val, min(max_val, value))


def _coordinate_in_range(value: float, bound: float) -> bool:
    """Return True when ``value`` is finite and within ``[-bound, bound]``."""
    return math.isfinite(value) and -bound <= value <= bound


def _as_float(value: object) -> float | None:
    """Return ``float(value)``, or None when it cannot be converted.

    ``OverflowError`` (an integer beyond the float range) counts as not
    convertible, so such a fix is rejected like any other bad value.
    """
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return None


def _log_rejected_device(device_label: str | None) -> None:
    """Name the device of a rejected fix at DEBUG only.

    Polling repeats unattended, and the label can be a user-provided device
    name, which AGENTS.md keeps out of records above DEBUG.
    """
    if device_label:
        _LOGGER.debug("Rejected coordinates belong to %s", device_label)


def _coordinate_kind(value: object) -> str:
    """Describe a rejected coordinate without echoing it.

    AGENTS.md forbids precise coordinates in logs, and a rejected value can
    still carry one (for example ``"48.137,"``). Only a fixed label is
    returned, so the log line names the defect, never the value.
    """
    if isinstance(value, str):
        return "text"
    if isinstance(value, (bytes, bytearray)):
        return "bytes"
    if isinstance(value, Mapping):
        return "a mapping"
    if isinstance(value, (list, tuple)):
        return "a sequence"
    return "another type"


class LocateOperations(_MixinBase):
    """Locate operations mixin for GoogleFindMyCoordinator.

    This class contains methods that handle device location requests,
    including coordinate validation and location management.
    """

    # Attribute declaration for mypy (actual value set in GoogleFindMyCoordinator.__init__)
    _is_polling: bool

    def _normalize_coords(
        self,
        payload: dict[str, Any],
        *,
        device_label: str | None = None,
        warn_on_invalid: bool = True,
    ) -> bool:
        """Validate and normalize latitude/longitude (and optionally accuracy).

        - Accepts numeric-like strings and converts them to floats.
        - Rejects NaN/Inf and out-of-range values.
        - Writes normalized floats back into `payload` when valid.
        - Normalizes `accuracy` to a finite float if present (best-effort).

        Returns:
            True if latitude/longitude are present and valid after normalization.
            False if coordinates are missing or invalid.

        Side effects:
            - Increments `invalid_coords` on invalid input.
            - Logs warnings for invalid data (unless warn_on_invalid=False).
        """
        lat = payload.get("latitude")
        lon = payload.get("longitude")
        if lat is None or lon is None:
            # Missing coordinates is not an error per se (semantic-only is valid).
            return False

        lat_f = _as_float(lat)
        lon_f = _as_float(lon)
        if lat_f is None or lon_f is None:
            self.increment_stat("invalid_coords")
            if warn_on_invalid:
                lat_state = "ok" if lat_f is not None else f"is {_coordinate_kind(lat)}"
                lon_state = "ok" if lon_f is not None else f"is {_coordinate_kind(lon)}"
                _LOGGER.warning(
                    "Ignoring invalid (non-numeric) coordinates: lat %s, lon %s",
                    lat_state,
                    lon_state,
                )
                _log_rejected_device(device_label)
            return False

        if not (
            _coordinate_in_range(lat_f, 90.0) and _coordinate_in_range(lon_f, 180.0)
        ):
            self.increment_stat("invalid_coords")
            if warn_on_invalid:
                _LOGGER.warning(
                    "Ignoring out-of-range/invalid coordinates: lat %s, lon %s",
                    "ok" if _coordinate_in_range(lat_f, 90.0) else "invalid",
                    "ok" if _coordinate_in_range(lon_f, 180.0) else "invalid",
                )
                _log_rejected_device(device_label)
            return False

        # Write back normalized floats
        payload["latitude"] = lat_f
        payload["longitude"] = lon_f

        # Best-effort normalize accuracy (if present).
        # The Android Location API uses 0.0 as an error code ("no accuracy").
        # Modern dual-frequency GNSS can achieve sub-meter accuracy, so we only
        # filter the error code (< 0.001m) and negative values.
        acc = payload.get("accuracy")
        if acc is not None:
            try:
                acc_f = float(acc)
                if math.isfinite(acc_f) and acc_f >= MIN_PHYSICAL_ACCURACY_M:
                    payload["accuracy"] = acc_f
                else:
                    # Error code (0.0), negative, NaN, Inf - remove it
                    payload.pop("accuracy", None)
            except (TypeError, ValueError):
                # Accuracy malformed; remove it
                payload.pop("accuracy", None)

        return True

    def can_play_sound(self, device_id: str) -> bool:
        """Return True if 'Play Sound' should be enabled for the device.

        **No network in availability path.**
        Strategy:
        - If capability is known from the lightweight device list -> use it (fast, cached).
        - If push readiness is explicitly False -> disable.
        - Otherwise -> optimistic True (known devices) to keep the UI usable.
          The actual action enforces reality and will start a cooldown on failure.

        Args:
            device_id: The canonical ID of the device.

        Returns:
            True if playing a sound is likely possible.
        """
        # 1) Use cached capability when available (fast path, no network).
        caps = self._device_caps.get(device_id)
        if caps and isinstance(caps.get("can_ring"), bool):
            res = bool(caps["can_ring"])
            _LOGGER.debug(
                "can_play_sound(%s) -> %s (from capability can_ring)", device_id, res
            )
            return res

        # 2) Short-circuit if push transport is not ready.
        ready = self._api_push_ready()
        if ready is False:
            # Respect explicit cooldowns triggered after recent failures, but do not
            # hide the action solely because push transport appears disconnected.
            if time.monotonic() < self._push_cooldown_until:
                _LOGGER.debug(
                    "can_play_sound(%s) -> False (push cooldown active)", device_id
                )
                return False
            _LOGGER.debug(
                "can_play_sound(%s): push not ready, keeping entity available",
                device_id,
            )

        # 3) Optimistic final decision based on whether we know the device.
        name_cache = self._ensure_device_name_cache()
        is_known = device_id in name_cache or device_id in self._device_location_data
        if is_known:
            _LOGGER.debug(
                "can_play_sound(%s) -> True (optimistic; known device, push_ready=%s)",
                device_id,
                ready,
            )
            return True

        _LOGGER.debug(
            "can_play_sound(%s) -> True (optimistic final fallback)", device_id
        )
        return True

    # ---------------------------- Public control / Locate gating ------------
    def _get_device_lock(self, device_id: str) -> asyncio.Lock:
        """Get or create a lock for a specific device.

        This prevents race conditions when multiple concurrent locate requests
        target the same device (e.g., rapid UI clicks or parallel service calls).
        """
        if device_id not in self._device_action_locks:
            self._device_action_locks[device_id] = asyncio.Lock()
        return self._device_action_locks[device_id]

    def can_request_location(self, device_id: str) -> bool:
        """Return True if a manual 'Locate now' request is currently allowed.

        Gate conditions:
          - device not ignored,
          - no sequential polling in progress,
          - no in-flight locate for the device,
          - per-device cooldown (lower-bounded by DEFAULT_MIN_POLL_INTERVAL) not active.
        Push readiness is checked lazily when submitting the request so the UI
        can stay responsive while the transport recovers.
        """
        # Block manual locate for ignored devices.
        if self.is_ignored(device_id):
            return False
        if self._is_polling:
            return False
        if device_id in self._locate_inflight:
            return False
        # Respect both manual-locate and poll cooldowns for the device
        now_mono = time.monotonic()
        until_manual = self._locate_cooldown_until.get(device_id, 0.0)
        if until_manual and now_mono < until_manual:
            return False
        until_poll = self._device_poll_cooldown_until.get(device_id, 0.0)
        if until_poll and now_mono < until_poll:
            return False
        return True

    # ---------------------------- Passthrough API ---------------------------
    async def async_locate_device(self, device_id: str) -> dict[str, Any]:
        """Locate a device using the native async API (no executor).

        UX & gating:
          - Reject immediately if `can_request_location()` is False.
          - Mark request as in-flight and (optimistically) start a cooldown that
            equals `DEFAULT_MIN_POLL_INTERVAL`. This disables repeated clicks.
          - On success: reset the polling baseline and set a **per-device cooldown**
            (owner-report purge window) by clamping a dynamic guess.
          - Always notify listeners via `async_set_updated_data(self.data)`.

        POPETS'25-informed behaviour:
          - If the returned payload carries an internal `_report_hint` of
            "in_all_areas" (~10 min throttle) or "high_traffic" (~5 min throttle),
            we additionally apply a type-aware cooldown (at least server minimum
            and at least one user poll interval). This stacks with the owner cooldown.

        Args:
            device_id: The canonical ID of the device.

        Returns:
            A dictionary containing the location data (empty dict on gating).

        Corrections:
            - Persist the received location data into the coordinator cache.
            - Mirror the Google Home spam filter used by the polling path.
            - Preserve previous coordinates for semantic-only locations.
            - Validate coordinates/accuracy and apply significance gating.
            - Push a fresh snapshot via `push_updated([device_id])`.
        """
        # Import helpers lazily to avoid circular imports
        from .helpers.cache import sanitize_decoder_row as _sanitize_decoder_row
        from .helpers.subentry import (
            normalize_epoch_seconds as _normalize_epoch_seconds,
        )

        name = self.get_device_display_name(device_id) or device_id

        # Acquire per-device lock to prevent race conditions on concurrent requests
        lock = self._get_device_lock(device_id)
        async with lock:
            if not self.can_request_location(device_id):
                _LOGGER.warning(
                    "Manual locate for %s is currently disabled (in-flight, cooldown, or polling).",
                    name,
                )
                return {}

            if not self._api_push_ready():
                # Only hard-block during the post-failure cooldown window;
                # otherwise allow the attempt (mirrors can_play_sound logic).
                if time.monotonic() < self._push_cooldown_until:
                    _LOGGER.warning(
                        "Manual locate for %s blocked (push transport recovering).",
                        name,
                    )
                    return {}
                _LOGGER.debug(
                    "Push transport not confirmed ready for %s; attempting locate anyway.",
                    name,
                )

            # Enter in-flight and set a lower-bound cooldown window
            self._locate_inflight.add(device_id)
            self._locate_cooldown_until[device_id] = time.monotonic() + float(
                DEFAULT_MIN_POLL_INTERVAL
            )
            self.async_set_updated_data(self.data)

            google_home_filter = self._get_google_home_filter()

            try:
                # The transport stamps the device immediately before it POSTs
                # the request, not here: an attempt that stops in its push or
                # token phase leaves no stamp a reader could log in the meantime
                # (helpers/locate_clock.py). Created lazily for coordinators
                # built without __init__.
                self._locate_requested_mono = ensure_locate_stamps(
                    getattr(self, "_locate_requested_mono", None)
                )
                with stamp_on_dispatch(self._locate_requested_mono, device_id):
                    location_data = await self.api.async_get_device_location(
                        device_id, name
                    )

                if not location_data:
                    # The inner FCM-wait timeout logs at DEBUG only, so without
                    # this record a locate that got no answer leaves nothing at
                    # INFO (the error branches in api.py log their own WARNING or
                    # ERROR before returning empty). An automation can repeat the
                    # invocation, so the INFO record carries the canonical id and
                    # the display name stays at DEBUG (AGENTS.md section 5 (b)).
                    _LOGGER.info(
                        "Manual locate for device %s returned no location data",
                        device_id,
                    )
                    _LOGGER.debug("Manual locate without location data for %s", name)
                    return {}

                # A result WITH content, and only that, proves on this path that
                # the credentials worked -- the same rule the sound handlers
                # state at their own sites. An empty return proves only that
                # nothing raised, and it reaches this method from several
                # pre-accept failures as well as from a healthy idle tag, so the
                # guard above runs first. It used not to, and a manual locate on
                # an idle tag then wiped a pending credential finding raised by
                # another device.
                #
                # The reset sits here and NOT further down, behind the coordinate
                # check: a record carrying only `last_seen` is an authenticated
                # server answer. The method returns {} for it, but the account
                # was accepted, and refusing to count that would be the same
                # error in the opposite direction.
                self._set_auth_state(failed=False)

                # Manual locate is upward-only for the reauth budget and never
                # consumes the poll-only decrypt-proof hint; drop it here (after the
                # empty guard, so location_data is a non-empty dict) so it cannot
                # leak into the cached payload or the returned dict (mirrors the
                # _report_hint stripping discipline).
                location_data.pop("_decrypt_proven", None)

                self._record_semantic_label(location_data, device_id=device_id)

                cached_loc = self._device_location_data.get(device_id)
                is_replay = False
                if isinstance(cached_loc, Mapping):
                    new_ts = _normalize_epoch_seconds(location_data.get("last_seen"))
                    old_ts = _normalize_epoch_seconds(cached_loc.get("last_seen"))
                    if new_ts is not None and old_ts is not None and new_ts == old_ts:
                        is_replay = True

                location_data["is_replayed"] = is_replay

                # Tally the REPORTED accuracy class here (#216); see the identical
                # call in the poll loop for the full reasoning. Same position for
                # the same two reasons: before the fusion, which may reject and
                # return, and before any substitution of the value (semantic
                # mapping, Google-Home filter, semantic-only preserve). And the
                # same replay rule: repeated manual locates without a new report
                # must not enter the distribution more than once.
                if self.claim_report_for_tally(device_id, location_data):
                    self.count_accuracy_class(location_data)
                location_data["_accuracy_counted"] = True

                mapping_applied = self._apply_semantic_mapping(location_data)

                # --- Parity with polling path: Google Home semantic spam filter --------
                # Consume coordinate substitution from the filter when needed.
                semantic_name = location_data.get("semantic_name")
                if (
                    not mapping_applied
                    and not is_replay
                    and semantic_name
                    and google_home_filter is not None
                ):
                    try:
                        (should_filter, replacement_attrs) = (
                            google_home_filter.should_filter_detection(
                                device_id, semantic_name
                            )
                        )
                    except Exception as gf_err:
                        _LOGGER.debug(
                            "Google Home filter error for %s: %s", name, gf_err
                        )
                    else:
                        if should_filter:
                            _LOGGER.debug(
                                "Filtering out Google Home spam detection for %s (manual locate)",
                                name,
                            )
                            # Successful but filtered: reset baseline, clear cooldown, and refresh UI.
                            self._last_poll_mono = time.monotonic()
                            self._locate_cooldown_until.pop(device_id, None)
                            self.push_updated([device_id])
                            return {}
                        if replacement_attrs:
                            prev_location = self._device_location_data.get(device_id)
                            keep_previous_precise = (
                                self._should_preserve_precise_home_coordinates(
                                    prev_location, replacement_attrs
                                )
                            )

                            location_data = dict(location_data)
                            if keep_previous_precise and prev_location is not None:
                                _LOGGER.debug(
                                    "Google Home filter: %s detected at '%s' (manual locate), preserving previous precise coordinates",
                                    name,
                                    semantic_name,
                                )
                                location_data["latitude"] = prev_location["latitude"]
                                location_data["longitude"] = prev_location["longitude"]
                                # Carry the producer flag with the reused accuracy so
                                # a preserved estimated fallback is not reclassified
                                # as a real measurement downstream.
                                carry_reused_accuracy(location_data, prev_location)
                            else:
                                if (
                                    "latitude" in replacement_attrs
                                    and "longitude" in replacement_attrs
                                ):
                                    location_data["latitude"] = replacement_attrs.get(
                                        "latitude"
                                    )
                                    location_data["longitude"] = replacement_attrs.get(
                                        "longitude"
                                    )
                                if (
                                    "radius" in replacement_attrs
                                    and replacement_attrs.get("radius") is not None
                                ):
                                    substitute_zone_accuracy(
                                        location_data,
                                        replacement_attrs["radius"],
                                    )
                            # Clear semantic name so HA Core's zone engine determines the final state.
                            location_data["semantic_name"] = None
                location_data.pop("is_replayed", None)
                # ----------------------------------------------------------------------

                # Preserve previous coordinates if only semantic location is provided.
                if (
                    location_data.get("latitude") is None
                    or location_data.get("longitude") is None
                ) and location_data.get("semantic_name"):
                    prev = self._device_location_data.get(device_id, {})
                    if prev:
                        location_data.setdefault("latitude", prev.get("latitude"))
                        location_data.setdefault("longitude", prev.get("longitude"))
                        # Carry the producer flag with the reused accuracy under
                        # setdefault semantics so a preserved estimated fallback
                        # keeps its flag without clobbering a value already present.
                        carry_reused_accuracy(location_data, prev, setdefault=True)
                        location_data["status"] = (
                            "Semantic location; preserving previous coordinates"
                        )

                # Validate/normalize coordinates (and accuracy if present).
                if not self._normalize_coords(location_data, device_label=name):
                    if not location_data.get("semantic_name"):
                        _LOGGER.debug(
                            "No location data (coordinates or semantic name) available for %s in manual locate.",
                            name,
                        )
                    return {}

                # Prepare a copy for gating/cooldown application
                slot = dict(location_data)
                slot.setdefault("last_updated", time.time())

                # Report-type cooldown removed: location_poll_interval already
                # regulates polling frequency; the 600s cooldown caused stale updates.
                report_hint = slot.get("_report_hint")

                # Track crowd-sourced updates when hint is present
                if report_hint:
                    self.increment_stat("crowd_sourced_updates")

                slot.pop("_report_hint", None)

                # Sanitize invariants + derive labels before significance gating
                slot = _sanitize_decoder_row(slot)

                if not self._apply_weighted_location_fusion(device_id, slot):
                    return {}

                slot["_fusion_preapplied"] = True

                # Increment crowdsourced stats for manual locate as well (if applicable)
                if slot.get("source_label") == "crowdsourced":
                    self.increment_stat("crowd_sourced_updates")

                # Commit to cache (update_device_cache ensures last_updated and stats)
                self.update_device_cache(device_id, slot)

                # Successful manual locate:
                # - reset poll baseline,
                # - set a per-device poll cooldown (owner purge window) using a dynamic guess
                #   clamped into guardrails,
                # - set the same cooldown for manual locate button to avoid spamming.
                self._last_poll_mono = time.monotonic()
                dynamic_guess = max(
                    float(DEFAULT_MIN_POLL_INTERVAL), float(self.location_poll_interval)
                )
                owner_cooldown = _clamp(
                    dynamic_guess, _COOLDOWN_OWNER_MIN_S, _COOLDOWN_OWNER_MAX_S
                )
                now_mono = time.monotonic()
                # Extend (not overwrite) any type-aware cooldown applied above
                existing_deadline = self._device_poll_cooldown_until.get(device_id, 0.0)
                owner_deadline = now_mono + owner_cooldown
                self._device_poll_cooldown_until[device_id] = max(
                    existing_deadline, owner_deadline
                )
                self._locate_cooldown_until[device_id] = max(
                    self._locate_cooldown_until.get(device_id, 0.0), owner_deadline
                )

                # Touch presence for the device (a fresh interaction implies it exists)
                self._present_last_seen[device_id] = now_mono

                self.push_updated([device_id])
                return location_data or {}
            except SpotAuthPermanentError as auth_err:
                self._set_auth_state(
                    failed=True,
                    reason=f"Auth failed during manual locate: {auth_err}",
                )
                # FIX 3: direct reauth site (no exception bubbles to a coordinator
                # catch), so record the classified reason directly. The condition
                # is a generic Spot-transport auth failure (SpotAuthPermanentError
                # is raised only in spot_request.py, e.g. "AAS token invalid after
                # refresh"), so it maps to SPOT_AUTH_PERMANENT -- the same code the
                # poll-cycle equivalent uses (polling.py) -- not an owner-key code.
                self.record_reauth_reason(
                    ReauthReasonCode.SPOT_AUTH_PERMANENT,
                    origin="locate.py:async_locate_device:spot_auth",
                )
                entry = getattr(self, "config_entry", None)
                reauth_started = False
                if entry is not None:
                    try:
                        # ``ConfigEntry.async_start_reauth`` is a synchronous
                        # ``@callback`` returning ``None`` (it schedules the flow
                        # itself). Awaiting it would raise ``TypeError`` on the
                        # ``None`` result, which the broad ``except`` below would
                        # swallow, leaving ``reauth_started`` False and emitting
                        # the wrong user message. Call it without ``await``.
                        entry.async_start_reauth(self.hass)
                        reauth_started = True
                    except Exception as reauth_err:  # pragma: no cover - defensive
                        _LOGGER.debug(
                            "Failed to start reauth flow after manual locate auth error: %s",
                            reauth_err,
                        )
                message = (
                    "Authentication for Google Find My Device expired; "
                    "re-authentication has been started."
                    if reauth_started
                    else "Authentication for Google Find My Device expired; please re-authenticate."
                )
                raise HomeAssistantError(message) from auth_err
            except ConfigEntryAuthFailed as auth_exc:
                # Mark error and request a refresh; no need to re-raise here for manual action.
                self._set_auth_state(
                    failed=True, reason=f"Auth failed during manual locate: {auth_exc}"
                )
                try:
                    await self.async_request_refresh()
                except Exception:
                    pass
                return {}
            except NovaAuthError as auth_err:
                # Branch on the STATUS, never on the type. A device removed from
                # the account arrived here as an auth error and flipped the
                # integration-wide auth state: Repairs issue, EVENT_AUTH_ERROR,
                # diagnostic sensor on. The sign-in was fine the whole time.
                # api passes such a status through instead of returning {}, so a
                # manual locate on a deleted tracker reaches this branch and not
                # the success path above. That mattered more than it does now:
                # the success path used to call _set_auth_state(failed=False)
                # BEFORE the empty guard and would have CLEARED a pending auth
                # error for every 5xx and every empty result. It no longer does
                # -- the reset sits behind the empty guard now -- so this branch
                # is the second line of defence rather than the only one.
                if not is_credential_rejection(auth_err):
                    _LOGGER.warning(
                        "Manual locate for %s failed (client error): HTTP %s - %s",
                        name,
                        getattr(auth_err, "status", "?"),
                        auth_err,
                    )
                    self.note_error(auth_err, where="async_locate_device", device=name)
                    return {}

                # Expected: Authentication/permission issue from Nova API
                _LOGGER.warning(
                    "Manual locate for %s failed (authentication): HTTP %s - %s",
                    name,
                    getattr(auth_err, "status", "?"),
                    auth_err,
                )
                self._set_auth_state(failed=True, reason=f"Nova auth error: {auth_err}")
                self.note_error(auth_err, where="async_locate_device", device=name)
                return {}
            except NovaRateLimitError as rate_err:
                # Expected: Rate limiting from Nova API (429 Too Many Requests)
                _LOGGER.warning(
                    "Manual locate for %s rate-limited by Google: %s",
                    name,
                    rate_err,
                )
                self.note_error(rate_err, where="async_locate_device", device=name)
                return {}
            except NovaHTTPError as http_err:
                # Expected: Server errors from Nova API (5xx)
                _LOGGER.warning(
                    "Manual locate for %s failed (server error): HTTP %s - %s",
                    name,
                    getattr(http_err, "status", "?"),
                    http_err,
                )
                self.note_error(http_err, where="async_locate_device", device=name)
                return {}
            except NovaLogicError as logic_err:
                # Expected: Logic error from Protobuf response (e.g., invalid device ID)
                _LOGGER.warning(
                    "Manual locate for %s failed (API logic error): Code %s - %s",
                    name,
                    getattr(logic_err, "code", "?"),
                    getattr(logic_err, "message", str(logic_err)),
                )
                self.note_error(logic_err, where="async_locate_device", device=name)
                return {}
            except NovaProtobufDecodeError as decode_err:
                # Expected: Malformed Protobuf response
                _LOGGER.warning(
                    "Manual locate for %s failed (decode error): %s",
                    name,
                    decode_err,
                )
                self.note_error(decode_err, where="async_locate_device", device=name)
                return {}
            except StaleOwnerKeyError as stale_err:
                # Per-tracker outdated owner key: an account-wide reauth would not
                # fix a single outdated tracker. Record it through the shared entry
                # point (per-tracker log, no account counter) and return an empty
                # result rather than a generic HomeAssistantError. Must precede the
                # DecryptionError handler below -- it is a subclass.
                self.note_decrypt_failure(stale=True, error=stale_err, device=name)
                self.note_error(stale_err, where="async_locate_device", device=name)
                return {}
            except OwnerKeyLookupTransientError as transient_err:
                # Transient owner-key lookup miss (partial server response,
                # network/gRPC failure). NOT a credential defect: do NOT feed the
                # account-wide reauth counter and do NOT escalate. Treat it as a
                # plain skip (empty result), like a "no pending report" cycle, so a
                # transient single-device failure never drives a spurious reauth.
                # Must precede the DecryptionError handler; it is NOT a subclass, so
                # this explicit catch keeps it out of the escalation path.
                _LOGGER.debug(
                    "Manual locate for %s skipped (transient owner-key lookup): %s",
                    name,
                    transient_err,
                )
                self.note_error(transient_err, where="async_locate_device", device=name)
                return {}
            except DecryptionError as dec_err:
                # Account-wide stale/missing shared key. Feed the SAME escalation
                # counter as the poll and push paths (single source of truth) so a
                # user clicking "Locate" contributes to -- and can trigger -- the
                # reauth flow instead of silently swallowing into a generic error.
                # Below threshold: graceful empty result, like the poll path keeps
                # polling. At threshold: start reauth (HA cannot refresh the shared
                # key itself; the user must supply a fresh secrets.json).
                #
                # This path only advances the shared counter, never resets it: the
                # authoritative reset is the poll loop's whole-cycle decrypt-clean
                # gate. A manual locate cannot prove the key is healthy (an empty
                # result means "no pending report", which decrypts nothing), so
                # resetting here could mask a genuinely stale key. The next clean
                # poll cycle reconciles the counter. Locate is an additional upward
                # evidence source, not a reset surface.
                should_reauth = self.note_decrypt_failure(
                    stale=False, error=dec_err, device=name
                )
                self.note_error(dec_err, where="async_locate_device", device=name)
                if not should_reauth:
                    return {}
                self._set_auth_state(
                    failed=True,
                    reason=(
                        "Location decryption keeps failing during manual locate; "
                        "the shared key is stale and a fresh secrets.json "
                        "(re-authentication) is required"
                    ),
                )
                # FIX 3: direct reauth site (a stale shared key needs fresh
                # credentials); record the classified reason directly. This is the
                # account-wide stale-shared-key decrypt condition, so it maps to
                # DECRYPT_STALE_KEY -- the same code the poll-cycle equivalent uses
                # (polling.py, _finalize_cycle_decrypt_state) -- not an AAS/token code.
                self.record_reauth_reason(
                    ReauthReasonCode.DECRYPT_STALE_KEY,
                    origin="locate.py:async_locate_device:decrypt_stale_key",
                )
                entry = getattr(self, "config_entry", None)
                reauth_started = False
                if entry is not None:
                    try:
                        # async_start_reauth is a synchronous @callback returning
                        # None (it schedules the flow); awaiting it would raise on
                        # the None result. Call it without await, mirroring the
                        # SpotAuthPermanentError handler above.
                        entry.async_start_reauth(self.hass)
                        reauth_started = True
                    except Exception as reauth_err:  # pragma: no cover - defensive
                        _LOGGER.debug(
                            "Failed to start reauth flow after manual locate "
                            "decryption failure: %s",
                            reauth_err,
                        )
                message = (
                    "Google Find My Device can no longer decrypt locations "
                    "(the shared key is stale); re-authentication has been started."
                    if reauth_started
                    else "Google Find My Device can no longer decrypt locations "
                    "(the shared key is stale); please re-authenticate."
                )
                raise HomeAssistantError(message) from dec_err
            except LocationRequestNotAcceptedError as not_accepted_err:
                # The request never got past this integration's accept point. For the
                # manual path that is an ordinary empty result, exactly as it
                # looks to the user today -- a locate that found nothing. What
                # changes is what does NOT happen on the way there: the
                # `_set_auth_state(failed=False)` on the success path above is
                # skipped by construction, so a 5xx can no longer clear the
                # account's auth-failure state on its way through. That skip is
                # the whole point of catching this here.
                #
                # Returns rather than raises, unlike the broad handler below: a
                # failed request is not an "unexpected error" worth a red toast
                # in the UI, and re-wrapping it into a `HomeAssistantError` would
                # be a behaviour change this step does not intend. It must still
                # sit BEFORE that handler, though -- `Exception` is the base, so
                # the broad branch would otherwise claim it first.
                #
                # WARNING, not DEBUG. This path runs because someone asked for
                # it -- a button press, an automation, or the `locate_device` /
                # `locate_external` service -- so the failure has an audience and
                # a moment. The poll loop logs the SAME condition at DEBUG, and
                # that is the same rule rather than an inconsistency: it runs
                # unattended and repeatedly. Leaving it at DEBUG here would have
                # made a user ask for a locate, get nothing, and find nothing in
                # the log at the default level.
                #
                # Two neighbours could be read as precedent, and exactly one of
                # them is. NOT the `NovaRateLimitError` / `NovaHTTPError` handlers
                # a few branches up, however tempting the symmetry: `api.py`
                # answers both with `return {}` of its own (in `api.py`, the
                # `except NovaRateLimitError` branch for the 429 and the
                # `except NovaHTTPError` branch for the non-401/403 5xx; anchors
                # rather than line numbers, which this file has already outrun
                # twice), so neither type ever
                # reaches this method and those two branches are dead on this
                # path. Nor did this branch inherit their traffic -- before the
                # request layer started raising, a 5xx returned `[]` from
                # `location_request` and arrived here on the SUCCESS path, which
                # is precisely the defect this catch exists to end. The live
                # neighbour is `OwnerKeyLookupTransientError` a few branches up,
                # which api.py does re-raise and which logs at DEBUG; the line
                # between them is subject, not severity. That one is a miss on one
                # tracker's owner key while the account is otherwise fine. This
                # one is a request that never got past the accept point, which is
                # what the same operation already treats as WARNING when it
                # refuses to start (the in-flight/cooldown and push-recovery
                # guards near the top of this method).
                #
                # Two costs of the promotion, both weighed rather than discovered
                # later. First, this is not the first record for the incident:
                # `location_request` writes one before every raise of this type --
                # measured six, four WARNING and two ERROR, not just the three
                # transport rungs. Those records are PATH-AGNOSTIC: the identical
                # sentence appears for an unattended poll. What this line adds is
                # that the failed request was a USER-INITIATED one, plus the stage
                # that says where it stopped. That is the whole of its value, and
                # it is thin for `no_fcm_token`, whose transport record already
                # names the operation. It does NOT tell the user WHICH device --
                # that half is at DEBUG, see below -- so do not defend this line
                # with an identification it no longer performs. It is one line per
                # user action, and the poll path stays at DEBUG, so nothing
                # repeats unattended.
                # Second, `name` falls back to the raw canonical id when no
                # display name is cached (see where it is bound above). The rule
                # is AGENTS.md section 5: the canonical id is a class (b)
                # identifier (allowed at any level), the user-provided name is
                # derived information, and section 5 states the level split
                # itself, the "R6 / Count@WARNING, Name@DEBUG" pattern that
                # `test_location_request_r6_name_sweep.py` states and that the
                # signal class itself cites in its docstring. Because `name` may
                # fall back to the id, the split is applied to the whole value.
                # That is why the sentence is split the way the transport
                # layer splits its own:
                # the WARNING carries the operation and the reason, the identified
                # half stays at DEBUG. An earlier revision of this step logged
                # `name` in the WARNING and defended it as consistent with the
                # sibling branches of this method, which do the same. The defence
                # does not hold, and the reason is worth keeping: it was THIS step
                # that raised the line from DEBUG to WARNING, so it was this step
                # that put a possible device name into the default log. Section 5
                # settles the question the neighbours raised: a record that
                # answers one user action may name the device, a record that can
                # repeat unattended keeps the name out (a count or an index may
                # stand in its place). This branch is
                # one line per user action, so naming would be allowed; it keeps
                # the split anyway, because the DEBUG record beneath it already
                # carries the device and the test pins exactly that shape.
                _LOGGER.warning("Manual locate failed: %s", not_accepted_err)
                _LOGGER.debug(
                    "Manual locate for %s failed (request not accepted): %s",
                    name,
                    not_accepted_err,
                )
                self.note_error(
                    not_accepted_err, where="async_locate_device", device=name
                )
                return {}
            except Exception as err:
                short_err = self._short_error_message(err)
                _LOGGER.error("Manual locate for %s failed: %s", name, short_err)
                self.note_error(err, where="async_locate_device", device=name)
                raise HomeAssistantError(
                    f"Manual locate for '{name}' failed due to an unexpected error. "
                    "Check logs for details."
                ) from err
            finally:
                self._locate_inflight.discard(device_id)
                # Push an update so buttons/entities can refresh availability
                self.async_set_updated_data(self.data)

    def _cached_sound_uuid_is_stale(self, device_id: str) -> bool:
        """Return True if the device's cached Play Sound UUID has aged out.

        Mirrors the load-path expiry (#108) so the store-path overwrite guard
        and the Stop read-path agree with the reload filter on what "stale"
        means. A key with no tracked timestamp (e.g. tests that bypass
        ``__init__``, or pre-tracking entries) is treated as fresh here: only a
        present, genuinely aged timestamp marks the key stale, so a known-good
        cancel key is never dropped on incomplete state.
        """
        timestamps = getattr(self, "_sound_request_timestamps", None)
        if timestamps is None:
            return False
        existing_ts = timestamps.get(device_id)
        if existing_ts is None:
            return False
        return is_sound_uuid_expired(existing_ts, time.time(), SOUND_UUID_MAX_AGE_S)

    def _note_stop_transport_problem_without_extending(self) -> None:
        """Report a failed stop as a transport problem without LENGTHENING a live window.

        ``_note_push_transport_problem()`` does two things: it flags the
        transport ``DEGRADED``, and it sets ``_push_cooldown_until`` to
        ``monotonic() + cooldown_s`` ABSOLUTELY. The second part means calling
        it while a window is already running restarts that window instead of
        topping it up. Only the restart is unwanted here, so the call is made
        in full and the window is put back afterwards -- skipping the call
        outright would also drop the status flag, and a transport that just
        failed must not keep reporting itself healthy because a location push
        happened to arrive earlier in the same window.

        Reaching this code with a live window is close to new. Before the
        correlated-stop exception in the gate, ``async_stop_sound`` returned
        SUPPRESSED as its first statement whenever ``_api_push_ready()`` said
        no, and a running window is one of the reasons it says no; only the
        interleaving below could get past that.

        What the restart would cost: the stop button has no availability guard
        (there is no ``can_stop_sound`` on the coordinator, ``button.py`` only
        probes for one), so a user pressing Stop during an outage would push the
        end of the window forward with every press. The window would still be
        bounded -- it never outlives ``last press + cooldown_s`` -- but it also
        gates manual locate unconditionally (and Play Sound for every device
        whose ``can_ring`` capability is not cached, see ``can_play_sound()``),
        and those stay disabled for as long as the pressing goes on.

        The stop itself is still sent; only the window is left alone. The same
        rule covers a rare interleaving that predates this package: if a
        concurrent play armed a window while this stop was in flight, that
        window stands rather than being restarted from here.
        """
        now = time.monotonic()
        window_ends_at = self._push_cooldown_until
        self._note_push_transport_problem()
        if now < window_ends_at:
            self._push_cooldown_until = window_ends_at
            _LOGGER.debug(
                "Stop failed on the transport while a push cooldown was "
                "already running; keeping the existing window instead of "
                "restarting it"
            )

    def _stop_would_be_correlated(
        self, device_id: str, request_uuid: str | None
    ) -> bool:
        """Return True if a stop for ``device_id`` would carry a PROVEN cancel key.

        Read-only, no side effects. ``request_uuid`` must already be normalised
        (a blank string collapsed to ``None``) by ``_normalize_cancel_key``;
        ``async_stop_sound`` and ``classify_stop_correlation`` both apply it
        before they call this.

        Proven means exactly one thing: the key that would go on the wire is our
        own cached key and it is still fresh. An explicitly passed foreign
        string is a claim, not a handle, and an aged key of ours cannot vouch
        for the ring that is audible now. Both of those end as
        ``StopSoundOutcome.UNCORRELATED``.

        This exists as one predicate because three decisions depend on the
        same answer and must never drift apart: whether the stop may break a
        self-inflicted push cooldown (IRR-CA-STOP-BREAKS-SELF-INFLICTED-COOLDOWN),
        whether an accepted stop is reported as ``CANCELLED``, and whether the
        stop may be held back until STOP_SOUND_MIN_DELAY_AFTER_PLAY_S after the
        play it cancels. The key itself is never spent by a stop
        (IRR-CA-STOP-KEEPS-CANCEL-KEY); that used to be the second decision
        (IRR-CA-POP-ON-CORRELATED-CANCEL-ONLY, superseded).
        """
        cached_uuid = self._sound_request_uuids.get(device_id)
        if cached_uuid is None:
            return False
        if request_uuid is not None and request_uuid != cached_uuid:
            return False
        return not self._cached_sound_uuid_is_stale(device_id)

    def is_own_sound_request_uuid(self, request_uuid: str) -> bool:
        """Return True if ``request_uuid`` is a Play Sound key this instance holds.

        Read-only. Used by the push receiver to recognise the acknowledgement
        pushes Nova sends after a Play or Stop Sound: they carry the play's
        request UUID in ``fcmMetadata.requestUuid`` (measured 2026-10-17/18,
        ten rings, H5) and no location report. A set ``requestUuid`` alone is
        NOT the criterion, because locate requests set one too
        (``nbe_execute_action``); only a key from this cache is. Aged keys stay
        cached (IRR-CA-STOP-KEEPS-CANCEL-KEY) and count, so the acknowledgement
        of a stop sent with an aged key is recognised as well.
        """
        if not request_uuid:
            return False
        return request_uuid in self._sound_request_uuids.values()

    @staticmethod
    def _normalize_cancel_key(request_uuid: str | None) -> str | None:
        """Collapse a blank cancel key to ``None``; see ``async_stop_sound``."""
        return (request_uuid or "").strip() or None

    def classify_stop_correlation(
        self, device_id: str, request_uuid: str | None
    ) -> StopSoundUncorrelatedReason | None:
        """Return why a stop for ``device_id`` would be UNCORRELATED, or ``None``.

        Read-only, no side effects. ``None`` means the stop would carry a proven
        key (``_stop_would_be_correlated``). It exists so the service layer can
        tell the user WHICH case it is (K10), and it is built on the same
        predicate rather than beside it, so the message can never claim a case
        the outcome did not come from. Callers must invoke it immediately before
        ``async_stop_sound`` with nothing awaited in between: both then read the
        same cache state.
        """
        key = self._normalize_cancel_key(request_uuid)
        if self._stop_would_be_correlated(device_id, key):
            return None
        cached_uuid = self._sound_request_uuids.get(device_id)
        if key is not None and key != cached_uuid:
            return StopSoundUncorrelatedReason.FOREIGN_KEY
        if cached_uuid is None:
            return StopSoundUncorrelatedReason.NO_KEY
        return StopSoundUncorrelatedReason.AGED_KEY

    async def _async_hold_stop_until_ring_started(
        self, device_id: str, request_uuid: str
    ) -> None:
        """Hold a stop for our own play back until STOP_SOUND_MIN_DELAY_AFTER_PLAY_S.

        A stop that reaches the tracker around the moment its ring starts can
        leave the tracker silent until its button is pressed (measured twice,
        see the constant). Only a stop for the play whose acceptance we stamped
        is held: the stamp must carry the key that is about to be sent.
        Everything else -- no key, an aged key, a foreign key, an ambiguous
        play -- has no known start and goes out at once.

        Called after the readiness gate, so a stop that is going to be
        SUPPRESSED is declined at once instead of after the wait.
        """
        accepted_stamps = getattr(self, "_sound_play_accepted_at", None)
        if not accepted_stamps:
            return
        stamp = accepted_stamps.get(device_id)
        if stamp is None or stamp[0] != request_uuid:
            return
        remaining = STOP_SOUND_MIN_DELAY_AFTER_PLAY_S - (time.monotonic() - stamp[1])
        if remaining <= 0:
            return
        # INFO, because the user notices it: the ring goes on for this long
        # after the press. No device identifier, as in the can_ring warning.
        _LOGGER.info(
            "Stop Sound deferred by %.1f s: a stop that reaches the tracker "
            "while its ring is starting can leave the tracker unresponsive",
            remaining,
        )
        await asyncio.sleep(remaining)

    async def async_play_sound(self, device_id: str) -> PlaySoundOutcome:
        """Play sound on a device using the native async API (no executor).

        Guard with can_play_sound(); on failure, start a short cooldown to avoid repeated errors.

        **IMPORTANT**: This method tracks the request UUID so that Stop Sound can properly
        cancel the specific Play Sound request. Without UUID tracking, sounds may continue
        ringing indefinitely even after pressing Stop.

        Args:
            device_id: The canonical ID of the device.

        Returns:
            A :class:`PlaySoundOutcome`. Three-valued on purpose: ``ACCEPTED``
            (Nova took the command), ``SUPPRESSED`` (declined here by a local
            condition that clears itself, so waiting is the whole remedy) and
            ``FAILED`` (everything else, from a device that cannot ring to a
            server rejection). The dividing line between the latter two is the
            REMEDY, not where the decision was taken; see ``PlaySoundOutcome``
            and IRR-CA-PLAY-REMEDY-SPLIT. The bool this method used to return
            merged all three, so one message had to advise on all of them.
        """
        # The capability verdict is read BEFORE the readiness gate, because the
        # gate answers "may we send" with a single bool for two conditions whose
        # remedies are opposites. ``can_ring is False`` is permanent -- waiting
        # never makes a device ring that reports it cannot -- while the gate's
        # only other refusal, an active push cooldown, passes on its own within
        # ninety seconds. Those are its two ``False`` paths and there is no
        # third: an unknown device is waved through optimistically rather than
        # refused. Reading both as SUPPRESSED would tell the owner of a
        # non-ringing tracker to try again in a moment, forever. The stop path
        # has no equivalent branch: its gate checks push readiness only and
        # never asks about capability.
        # getattr, like the timestamp store below: some tests build the
        # coordinator via __new__ and never run __init__, and a gate that
        # raises AttributeError before the api call would fail them for a
        # reason that has nothing to do with what they pin.
        caps_by_device = getattr(self, "_device_caps", None) or {}
        caps = caps_by_device.get(device_id)
        if caps and caps.get("can_ring") is False:
            # Warning, not debug: the user-facing message for FAILED points at
            # the log as the carrier of the specific cause, and a message that
            # points at a log which says nothing is worse than no message. This
            # branch is reached once per explicit service call, never in a loop.
            #
            # No device identifier of any kind, neither the id (AGENTS.md
            # section 5, "never log ... device IDs") nor the display name, which
            # the same section names as derived identifying information to
            # redact. The coordinate warnings above do carry the display name;
            # this line does not follow them, because it is new and the
            # attribution it would buy is already in the user-facing error, which
            # names the device and appears at the same moment. What the log is
            # for here is the CAUSE, and the cause is device-independent.
            _LOGGER.warning(
                "Play Sound was refused: the device reports it cannot ring "
                "(can_ring is False). Waiting will not change this."
            )
            return PlaySoundOutcome.FAILED
        if not self.can_play_sound(device_id):
            _LOGGER.debug(
                "Suppressing play_sound call for %s: push cooldown active",
                device_id,
            )
            return PlaySoundOutcome.SUPPRESSED
        try:
            play = await self.api.async_play_sound(device_id)
            # Taken before any further await, so the stamp stays as close to
            # Nova's acceptance as this process can observe it.
            answered_at = time.monotonic()
            # api.async_play_sound carries two independent facts. ``accepted``
            # answers "did Nova take the command", ``cancel_key`` answers "may
            # the device be ringing", and ``outcome`` names WHO refused. The
            # cause is read from ``outcome`` below and never reconstructed from
            # the presence of a key -- that out-of-band inference is what
            # IRR-CA-SOUND-FAILURE-CLASS removed.
            ok, request_uuid = play.accepted, play.cancel_key
            # Decide whether to (over)write the cached Stop cancel key.
            # api.async_play_sound returns a non-None UUID in exactly the two
            # cases where a ring may be active and Stop needs the key: (1) the
            # server accepted the command (HTTP 200, ok=True) or (2) post-dispatch
            # ambiguity — a failure at/after the request reached the wire (server
            # disconnect, read timeout, or a transient 5xx that may have been
            # generated before Nova accepted the command; ok=False but the play
            # may have started). It returns None for every failure that provably
            # never rang (pre-dispatch/connection-setup failure OR an explicit
            # rejection such as 401/403). Storing rule: overwrite only on an
            # accepted command (ok=True) or when no key is cached yet. A merely
            # ambiguous result (ok=False with a fresh UUID) must NOT clobber a
            # known-good key for an earlier, possibly still-ringing play —
            # otherwise the default Stop would cancel the wrong request. With no
            # cached key — or one that has aged past SOUND_UUID_MAX_AGE_S, which
            # the reload filter would discard — the ambiguous UUID is still
            # stored, since it may be the only handle on a current ring. See
            # IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY.
            #
            # A stop no longer drops the key at all, not even a correlated one
            # (IRR-CA-STOP-KEEPS-CANCEL-KEY), so the only writers left are this
            # store, the reload filter and device removal.
            existing_uuid = self._sound_request_uuids.get(device_id)
            existing_is_stale = (
                existing_uuid is not None
                and self._cached_sound_uuid_is_stale(device_id)
            )
            if request_uuid is not None and (
                ok or existing_uuid is None or existing_is_stale
            ):
                self._sound_request_uuids[device_id] = request_uuid
                # Use getattr for test compatibility (tests may bypass __init__)
                timestamps = getattr(self, "_sound_request_timestamps", None)
                if timestamps is not None:
                    timestamps[device_id] = time.time()
                _LOGGER.debug(
                    "Stored Play Sound UUID for %s: %s", device_id, request_uuid
                )
                await self._async_save_sound_uuids()
            # Arm the stop delay (STOP_SOUND_MIN_DELAY_AFTER_PLAY_S) for THIS
            # play only, and only once Nova accepted it: an ambiguous play
            # proves no start time, so a stop for it is never held back. The
            # stamp carries the key it belongs to, so a stamp left behind by an
            # older play cannot delay the stop of a newer one.
            if ok and request_uuid is not None:
                accepted_stamps = getattr(self, "_sound_play_accepted_at", None)
                if accepted_stamps is not None:
                    accepted_stamps[device_id] = (request_uuid, answered_at)
            # Only a transport that gave us no usable answer is a push problem.
            # A server rejection (401/403/5xx), a rate limit, a missing local
            # action token and a bug of our own all reached this point as the
            # same False before SoundDispatchOutcome existed, so every one of
            # them armed the 90-second cooldown, flipped the integration to
            # FcmStatus.DEGRADED and made can_play_sound report the button as
            # unavailable -- an outage this integration inflicted on itself over
            # a network that was working, and one that then also suppressed the
            # user's follow-up Stop. See IRR-CA-SOUND-FAILURE-CLASS.
            if play.outcome is SoundDispatchOutcome.TRANSPORT_FAILED:
                self._note_push_transport_problem()
            elif ok:
                # Only an ACCEPTED submission proves the credentials worked.
                # An auth rejection arrives as REJECTED_AUTH, indistinguishable
                # from a read timeout before this contract existed, so clearing
                # the auth-failure state on a failed play erased the very signal
                # an expired sign-in produces. async_stop_sound has always
                # applied this rule and states the reason; the two paths agree.
                self._set_auth_state(failed=False)
            # Negative form, as on the stop path: the safe default of this
            # branch is failure. An ``if/elif`` cascade over the seven members
            # of SoundDispatchOutcome would let a value from outside the enum --
            # a bool from a stale test double, a member added later without
            # visiting this site -- fall through to success. Only ACCEPTED is
            # success, and only it is named here.
            if play.outcome is not SoundDispatchOutcome.ACCEPTED:
                return PlaySoundOutcome.FAILED
            return PlaySoundOutcome.ACCEPTED
        except ConfigEntryAuthFailed as auth_exc:
            self._set_auth_state(
                failed=True, reason=f"Auth failed during play_sound: {auth_exc}"
            )
            try:
                await self.async_request_refresh()
            except Exception:
                pass
            return PlaySoundOutcome.FAILED
        except (TimeoutError, ClientConnectionError, ClientError) as conn_err:
            _LOGGER.warning(
                "Connection failed during play_sound for %s: %s",
                device_id,
                conn_err,
            )
            self.note_error(conn_err, where="async_play_sound", device=device_id)
            self._note_push_transport_problem()
            return PlaySoundOutcome.FAILED
        except Exception as err:
            _LOGGER.error(
                "Unexpected error during play_sound for %s: %s",
                device_id,
                err,
                exc_info=True,
            )
            self.note_error(err, where="async_play_sound", device=device_id)
            # No cooldown here. api.async_play_sound classifies every Exception
            # in band and returns INTERNAL_ERROR instead of raising, so nothing
            # that reaches this handler came from the push transport: what is
            # left is this method's own body around the call, or an api
            # implementation that breaks the Protocol. Blaming the transport for
            # either is the misclassification IRR-CA-SOUND-FAILURE-CLASS stops.
            return PlaySoundOutcome.FAILED

    async def async_stop_sound(
        self,
        device_id: str,
        request_uuid: str | None = None,
    ) -> StopSoundOutcome:
        """Stop sound on a device using the native async API (no executor).

        **IMPORTANT**: This method retrieves the UUID from the previous Play Sound request
        and uses it to cancel that specific request. Without it the field is absent from
        the proto3 payload, so the server cannot correlate the stop with a running ring
        and the device may keep ringing.

        Args:
            device_id: The canonical ID of the device.
            request_uuid: Optional request UUID that identifies the prior play request.

        Returns:
            A :class:`StopSoundOutcome`. The state space is four-valued on
            purpose: ``CANCELLED`` (submitted with a correlated cancel key),
            ``UNCORRELATED`` (submitted without one, so nothing proves an
            effect), ``SUPPRESSED`` (declined here because the push transport is
            not up yet, so waiting is the remedy) and ``FAILED`` (attempted and
            unsuccessful, which includes the missing action token that never
            reaches the wire). A bool cannot carry the middle state, and
            collapsing it into success is what reported a stop for a ring that
            kept playing (BSkando#195). The two failure states split by REMEDY,
            not by distance travelled; see ``StopSoundOutcome``.
        """
        # Blank means "no opinion" -- an optional field left empty, a template
        # that rendered to nothing -- so it must fall through to the cached key
        # below, never pose as one. In-process the absence of a key already has
        # exactly one name, None, and only that name routes into the lookup.
        # Every stop (service handler, button via the service, direct
        # coordinator callers) passes here, and anything below the cache is too
        # late to restore the fallback. The rule itself lives only in
        # _normalize_cancel_key, which classify_stop_correlation applies too.
        # Post-condition: request_uuid_to_use is None or non-blank, which is
        # what every `is not None` check below relies on.
        request_uuid_to_use = self._normalize_cancel_key(request_uuid)
        # True only when the key about to be sent came from our own cache AND
        # was still fresh. An explicitly passed foreign UUID does NOT qualify:
        # it proves nothing about a ring we started, so it neither counts as
        # correlated nor arms the stop delay. Decided here, ABOVE the
        # readiness gate, because that gate needs the same answer; the single
        # definition lives in _stop_would_be_correlated().
        used_own_fresh_key = self._stop_would_be_correlated(
            device_id, request_uuid_to_use
        )

        # Less strict than can_play_sound(): stopping is harmless but still
        # requires push readiness.
        if not self._api_push_ready():
            # ... with exactly one exception, and it runs OPPOSITE to
            # can_play_sound() and the manual-locate guard above, which treat an
            # active cooldown as the hard block and a merely unconfirmed
            # transport as passable.
            #
            # What the exception tests is narrower than "the transport is
            # fine", and it has to be: while a cooldown runs, _api_push_ready()
            # short-circuits on that cooldown and never asks the transport at
            # all, so the transport state is simply unknown here. The two cases
            # this guard can tell apart are "not ready BECAUSE a window is
            # running" and "not ready for some other reason", and only the
            # first is passable. Claiming it also excludes a genuinely dead
            # transport would be untrue: a transport failure is the ONLY thing
            # that arms this window, so the two overlap by construction.
            #
            # Why the first case is passable at all: a play that reached the
            # wire and lost the answer stores a cancel key and arms the 90 s
            # window in the same breath, so the stop that key exists for was
            # locked out for the first 90 seconds after that play -- which is
            # exactly when a user reaches for the Stop button
            # (IRR-CA-STOP-BREAKS-SELF-INFLICTED-COOLDOWN). Nothing here claims
            # to know how long the ring itself lasts; that is a different timer
            # on a different layer, see the note on the aged cached key below.
            # The window is global while keys are per device, so "the play that
            # armed it" is the common case, not a proven one. What IS proven is
            # that this stop can be correlated, and that is the entire benefit
            # bought here.
            #
            # The exception is bound to a PROVEN key, so every case that could
            # at best report UNCORRELATED stays suppressed. It is not free,
            # though, and the price is a changed outcome CLASS: a stop that
            # used to end as SUPPRESSED ("not sent, try again in a moment") now
            # reaches the transport, so it can also end as FAILED, which
            # services.py reports with a different message. That is the honest
            # trade -- an attempt that can actually silence the device, at the
            # cost of a report that names the transport instead of the gate --
            # and it is pinned by a test rather than left implicit.
            #
            # It also cannot feed itself: a broken-through stop that fails on
            # the transport leaves the window exactly where it was, see
            # _note_stop_transport_problem_without_extending().
            if used_own_fresh_key and time.monotonic() < self._push_cooldown_until:
                _LOGGER.debug(
                    "Push cooldown active for %s, but this stop carries our own "
                    "fresh cancel key: sending it anyway",
                    device_id,
                )
            else:
                _LOGGER.debug(
                    "Suppressing stop_sound call for %s: push not ready", device_id
                )
                # A suppressed stop is a stop that was never sent, so it is a
                # failure and must reach the service layer as one -- but as its
                # own kind: nothing left this machine, and the condition clears
                # itself.
                return StopSoundOutcome.SUPPRESSED

        cached_uuid_was_expired = False
        if request_uuid_to_use is None:
            cached_uuid = self._sound_request_uuids.get(device_id)
            # An aged-out key is still strictly better than no key, so it is
            # SENT but not TRUSTED.
            #
            # Why sent: Nova queues an action command until the tracker becomes
            # reachable, which can take hours to days (BSkando#108). A key that
            # aged past SOUND_UUID_MAX_AGE_S may therefore still be the handle
            # on the ring that is audible right now. It cannot hit a different
            # ring -- it belongs to this device, and a fresher key would have
            # replaced it. Dropping it traded a possible correlation for a
            # guaranteed absence of one.
            #
            # Why not trusted: nothing proves the aged key references the
            # running ring, so the outcome stays UNCORRELATED and reaches the
            # user as such. It stays cached afterwards, as every key does
            # (IRR-CA-STOP-KEEPS-CANCEL-KEY): it is still the best handle a
            # second attempt has.
            #
            # The previous justification ("the ring it referenced auto-stopped
            # long ago") conflated two timers on two protocol layers. The FMDN
            # BLE ring timeout (Data ID 0x05, at most 10 minutes) bounds how
            # long a ring LASTS from the moment it starts; the Nova queue
            # bounds how long DELIVERY takes. This timestamp records when we
            # sent the play, not when the ring began, so nothing about the age
            # of the key implies the ring is over.
            cached_uuid_was_expired = cached_uuid is not None and (
                self._cached_sound_uuid_is_stale(device_id)
            )
            request_uuid_to_use = cached_uuid
            if request_uuid_to_use is None:
                _LOGGER.warning(
                    "No cancel key for %s; submitting an uncorrelated stop "
                    "(the ring may keep playing)",
                    device_id,
                )
            elif cached_uuid_was_expired:
                _LOGGER.debug(
                    "Using aged cached Play Sound UUID for %s (sent, but the "
                    "outcome is reported as uncorrelated)",
                    device_id,
                )
            else:
                _LOGGER.debug(
                    "Using cached Play Sound UUID for %s: %s",
                    device_id,
                    request_uuid_to_use,
                )
        else:
            # An explicitly passed key is a CLAIM of correlation, not a proof.
            # It proves correlation in exactly one case: it IS our own fresh
            # cached key. Any other string -- a typo, a stale template, the key
            # of a different ring -- is unverifiable, and reporting CANCELLED
            # for it would be BSkando#195 one layer up: success without effect.
            # Which of the two it is was already settled by
            # _stop_would_be_correlated() above; re-deriving the predicate here
            # is exactly the drift this package removed. What is left for this
            # branch is to say, per case, what it means.
            cached_uuid = self._sound_request_uuids.get(device_id)
            if used_own_fresh_key:
                # It is our own live key, so the stop is correlated and may be
                # held back like an implicit one.
                _LOGGER.debug(
                    "Cancel key supplied for %s is our own live key: %s",
                    device_id,
                    request_uuid_to_use,
                )
            elif cached_uuid is not None and cached_uuid == request_uuid_to_use:
                # Ours, but aged: it matches, it is simply too old to vouch
                # for. Saying "does not match" here would be untrue, and the
                # implicit path already distinguishes the two cases.
                _LOGGER.debug(
                    "Cancel key supplied for %s matches our cached key but it "
                    "has aged out; sending it, reporting the stop as "
                    "uncorrelated",
                    device_id,
                )
            else:
                _LOGGER.warning(
                    "Cancel key for %s was supplied by the caller and does not "
                    "match a live Play Sound request of this integration; the "
                    "stop cannot be correlated (the ring may keep playing)",
                    device_id,
                )

        if used_own_fresh_key and request_uuid_to_use is not None:
            await self._async_hold_stop_until_ring_started(
                device_id, request_uuid_to_use
            )

        try:
            stop_outcome = await self.api.async_stop_sound(
                device_id, request_uuid_to_use
            )
            # Same rule as on the play path above. The outer test stays in its
            # negative form on purpose and is the single exception to the
            # positive-list discipline: the safe default of THIS branch is
            # FAILED, so an outcome nobody anticipated must fall through to it
            # rather than be waved past. The cooldown inside it keeps the
            # positive form, because there the safe default is to do nothing.
            if stop_outcome is not SoundDispatchOutcome.ACCEPTED:
                if stop_outcome is SoundDispatchOutcome.TRANSPORT_FAILED:
                    self._note_stop_transport_problem_without_extending()
                # No credential proof on any non-accepted path: an auth
                # rejection arrives as REJECTED_AUTH, and clearing the
                # auth-failure state here would erase the very signal an expired
                # sign-in produces.
                return StopSoundOutcome.FAILED
            # An accepted submission, and only that, proves credentials worked.
            self._set_auth_state(failed=False)
            # CANCELLED is bound to PROVEN correlation, never to "some string
            # was sent". A key we cannot vouch for falls through to
            # UNCORRELATED below, which is what reaches the user as an error.
            if used_own_fresh_key:
                # Correlated stop accepted with OUR key. The key is KEPT
                # (IRR-CA-STOP-KEEPS-CANCEL-KEY): HTTP 200 means Nova took the
                # stop, not that the tracker executed it, so the key may still
                # be the only handle on a ring that is audible right now. Until
                # it was kept, a second press sent no key at all and was
                # reported as "could not be matched" for a ring this
                # integration had started itself. It leaves the cache the same
                # ways every key does: a newer play replaces it, it ages past
                # SOUND_UUID_MAX_AGE_S and the reload filter drops it, or the
                # device is removed.
                return StopSoundOutcome.CANCELLED
            # An aged key was sent unproven, so it is not trusted -- and, like
            # every key, not spent: it remains the best handle a retry has.
            return StopSoundOutcome.UNCORRELATED
        except ConfigEntryAuthFailed as auth_exc:
            self._set_auth_state(
                failed=True, reason=f"Auth failed during stop_sound: {auth_exc}"
            )
            try:
                await self.async_request_refresh()
            except Exception:
                pass
            return StopSoundOutcome.FAILED
        except (TimeoutError, ClientConnectionError, ClientError) as conn_err:
            _LOGGER.warning(
                "Connection failed during stop_sound for %s: %s",
                device_id,
                conn_err,
            )
            self.note_error(conn_err, where="async_stop_sound", device=device_id)
            self._note_stop_transport_problem_without_extending()
            return StopSoundOutcome.FAILED
        except Exception as err:
            _LOGGER.error(
                "Unexpected error during stop_sound for %s: %s",
                device_id,
                err,
                exc_info=True,
            )
            self.note_error(err, where="async_stop_sound", device=device_id)
            # No cooldown, for the reason spelled out on the play path:
            # api.async_stop_sound returns INTERNAL_ERROR for every unexpected
            # Exception instead of raising, so this handler only ever sees a
            # failure of our own bookkeeping around the call.
            return StopSoundOutcome.FAILED
