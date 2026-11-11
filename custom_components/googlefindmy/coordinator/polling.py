# custom_components/googlefindmy/coordinator/polling.py
"""Polling operations for GoogleFindMyCoordinator.

This module contains polling-related methods extracted from main.py.

Methods moved here:
- _set_api_status: Update API polling status
- _set_fcm_status: Update FCM push transport status
- api_status: StatusSnapshot property for API health
- fcm_status: StatusSnapshot property for push transport health
- is_fcm_connected: Convenience boolean for push availability
- consecutive_timeouts: Number of consecutive poll timeouts
- last_poll_result: Last recorded poll result
- _is_on_hass_loop: Check if on HA event loop
- _run_on_hass_loop: Schedule callable on HA loop
- _dispatch_async_request_refresh: Safe refresh dispatch
- _schedule_short_retry: Coalesced short retry scheduling
- _handle_dr_event: Handle device registry changes
- _compute_type_cooldown_seconds: Server-aware cooldown duration
- _apply_report_type_cooldown: Apply per-device poll cooldown
- is_polling: Property for current polling state
- get_fcm_acquire_duration_seconds: Duration to acquire FCM
- get_last_poll_duration_seconds: Duration of last poll cycle
- _is_fcm_ready_soft: Check if push transport appears ready
- _note_fcm_deferral: Escalation timeline for FCM not ready
- _clear_fcm_deferral: Clear escalation on FCM ready
- _get_predicted_poll_time: Predict next update time
- _note_push_transport_problem: Enter cooldown after push failure
- force_poll_due: Force next poll immediately
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import time
from collections.abc import Callable, Mapping
from datetime import datetime
from statistics import mean, stdev
from typing import Any

from homeassistant.config_entries import ConfigEntryAuthFailed
from homeassistant.core import Event
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import UpdateFailed

from .._reauth_reason import ReauthReasonCode
from ..Auth.fcm_receiver_ha import CRASH_LOOP_FATAL_PREFIX
from ..Auth.log_safety import describe_exception
from ..const import (
    DEVICE_LIST_POLL_INTERVAL,
    DOMAIN,
    POLL_DEVICE_OUTER_TIMEOUT_S,
)
from ..NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
    DecryptionError,
    OwnerKeyLookupTransientError,
    OwnReportIdentityMismatchError,
    SharedKeyMissingError,
    StaleOwnerKeyError,
    is_real_location_record,
)
from ..NovaApi.ExecuteAction.LocateTracker.location_request import (
    LocationRequestNotAcceptedError,
)
from ..NovaApi.nova_request import (
    NovaAuthError,
    NovaAuthPermanentError,
    is_credential_rejection,
)
from ..ProtoDecoders import decoder
from ..SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
    SpotApiEmptyResponseError,
)
from ..SpotApi.spot_request import SpotAuthPermanentError
from ._mixin_typing import _MixinBase
from .helpers.cache import (
    carry_reused_accuracy,
    strip_transient_keys,
    substitute_zone_accuracy,
)
from .helpers.cache import sanitize_decoder_row as _sanitize_decoder_row
from .helpers.locate_clock import (
    ensure_locate_counts,
    ensure_locate_stamps,
    stamp_on_dispatch,
)
from .helpers.stats import ApiStatus, CryptoStatus, FcmStatus, StatusSnapshot
from .helpers.subentry import normalize_epoch_seconds as _normalize_epoch_seconds
from .helpers.update import (
    calculate_presence_ttl as _calculate_presence_ttl_impl,
)
from .helpers.update import (
    filter_and_dedupe_devices as _filter_and_dedupe_impl,
)
from .helpers.update import (
    is_fatal_fcm_auth_error as _is_fatal_fcm_auth_error_impl,
)
from .helpers.update import (
    is_poll_cycle_due as _is_poll_cycle_due_impl,
)
from .helpers.update import (
    normalize_device_list_payload as _normalize_device_list_impl,
)
from .helpers.update import (
    should_defer_empty_list as _should_defer_empty_list_impl,
)

_LOGGER = logging.getLogger(__name__)


# Iter-14 (Codex follow-up): re-auth-escalation classification SSOT.
# The FCM receiver's ``_fatal_errors`` map is a shared channel: it carries
# both classic auth fatals (401/404 with credentials issues, raise
# ``ConfigEntryAuthFailed`` immediately or after _FCM_ERROR_RETRY_THRESHOLD
# cycles) and supervisor-level "short-run crash loop" fatals (poison
# message defense; user must reload / regenerate credentials per the
# repair issue, NOT re-authenticate). Without this classification the
# crash-loop fatal would be reclassified as an auth fatal after 3 cycles
# and push users into HA's re-auth flow. Keep this as a free function so
# branch-coverage tests can pin the classification contract without
# spinning up the full update coroutine.
_FCM_FATAL_CLASS_CRASH_LOOP_EXCLUDED = "crash_loop_excluded"
_FCM_FATAL_CLASS_AUTH_IMMEDIATE = "auth_immediate"
_FCM_FATAL_CLASS_AUTH_AFTER_THRESHOLD = "auth_after_threshold"


def _classify_fcm_fatal(fatal_error: str) -> str:
    """Classify a non-empty FCM fatal error for the re-auth escalation.

    Returns one of:
      - ``_FCM_FATAL_CLASS_CRASH_LOOP_EXCLUDED``: supervisor-level
        short-run cap; excluded from re-auth (repair issue carries the
        user-visible guidance).
      - ``_FCM_FATAL_CLASS_AUTH_IMMEDIATE``: classic auth fatal that
        should raise ``ConfigEntryAuthFailed`` immediately.
      - ``_FCM_FATAL_CLASS_AUTH_AFTER_THRESHOLD``: unclassified non-cap
        fatal; count and escalate after _FCM_ERROR_RETRY_THRESHOLD
        consecutive update cycles.
    """
    if fatal_error.startswith(CRASH_LOOP_FATAL_PREFIX):
        return _FCM_FATAL_CLASS_CRASH_LOOP_EXCLUDED
    if _is_fatal_fcm_auth_error_impl(fatal_error):
        return _FCM_FATAL_CLASS_AUTH_IMMEDIATE
    return _FCM_FATAL_CLASS_AUTH_AFTER_THRESHOLD


# Cooldown constants for crowdsourced reports
_COOLDOWN_MIN_IN_ALL_AREAS_S = 10 * 60  # 10 minutes
_COOLDOWN_MIN_HIGH_TRAFFIC_S = 5 * 60  # 5 minutes

# Accept an empty device list only on the 2nd consecutive result (defers once)
_EMPTY_LIST_QUORUM = 2

# A device counts as silent once this many consecutive polls returned no
# location data; only then is it reported at INFO, once, together with a single
# INFO record when it answers again (Home Assistant quality-scale rule
# ``log-when-unavailable``). A lower value floods the log for BLE tags at the
# edge of reporter coverage: measured on a live instance over three days, one
# tag alternated between empty results and locations 688 times, and only two of
# those empty runs reached a length of three (568 had length one, 118 length
# two). With a quorum of one, that tag alone would have produced about 1,375
# INFO records instead of four.
_EMPTY_LOCATION_QUORUM = 3

# Maximum number of transient auth failures before triggering re-auth
_MAX_TRANSIENT_AUTH_FAILURES = 3

# Maximum consecutive stale/missing shared-key decryption failures before
# escalating to a reauth flow. Gives the in-decrypt self-heal (force_refresh /
# blind refresh) a chance first; only a persistently un-decryptable shared key
# escalates to ConfigEntryAuthFailed.
_MAX_DECRYPT_FAILURES = 3

# Cooldown between decrypt-triggered reauth escalations (seconds). Once the reauth
# flow is open, the un-fixable condition persists every cycle; this prevents
# re-firing the escalation on each poll until the user supplies a fresh bundle.
_DECRYPT_REAUTH_COOLDOWN_S = 6 * 3600

# FCM error retry threshold before triggering re-auth
_FCM_ERROR_RETRY_THRESHOLD = 3

# Timeout for FCM not ready before forcing poll (seconds)
_PUSH_NOT_READY_TIMEOUT_S = 15

# Predictive polling buffer to avoid requesting data before it is available server-side
_PREDICTION_BUFFER_S = 45


class PollingOperations(_MixinBase):
    """Polling operations mixin for GoogleFindMyCoordinator.

    This class contains methods that manage the polling lifecycle,
    including status tracking and event loop helpers.
    """

    # Attribute declarations for mypy (actual values set in GoogleFindMyCoordinator.__init__)
    _push_ready_memo: bool | None
    _last_poll_result: str | None
    _api_status_state: str
    _api_status_reason: str | None
    _api_status_changed_at: float | None
    _fcm_status_state: str
    _fcm_status_reason: str | None
    _fcm_status_changed_at: float | None
    _crypto_status_state: str
    _crypto_status_reason: str | None
    _crypto_status_changed_at: float | None
    _force_device_list_reason: str | None
    _short_retry_cancel: Callable[[], None] | None
    _poll_cycle_task: asyncio.Task[None] | None
    _poll_cycle_teardown: bool
    _fcm_error_count: int
    _fcm_last_error: str | None
    _last_transient_auth_error: str | None
    _is_polling: bool
    _startup_complete: bool

    def _set_api_status(self, status: str, *, reason: str | None = None) -> None:
        """Update the API polling status and notify listeners if it changed."""
        if status == self._api_status_state and reason == self._api_status_reason:
            return

        self._api_status_state = status
        self._api_status_reason = reason
        self._api_status_changed_at = time.time()

        try:
            self.async_set_updated_data(self.data)
        except Exception:
            # Fallback for very early startup when listeners are not ready yet.
            pass

    def _set_fcm_status(self, status: str, *, reason: str | None = None) -> None:
        """Update the push transport status while avoiding noisy churn."""
        if status == self._fcm_status_state and reason == self._fcm_status_reason:
            return

        self._fcm_status_state = status
        self._fcm_status_reason = reason
        self._fcm_status_changed_at = time.time()

        try:
            self.async_set_updated_data(self.data)
        except Exception:
            pass

    def _set_crypto_status(self, status: str, *, reason: str | None = None) -> None:
        """Update the location-decryption status while avoiding noisy churn.

        Mirrors ``_set_fcm_status`` exactly. This is the single writer for the
        diagnostic encryption-key sensor: failure states come from
        ``note_decrypt_failure`` (one mapping for poll, push and manual locate),
        the ``OK`` state from a decrypt-clean poll cycle or a proven
        background-push decrypt (``note_background_decrypt_success``, account-wide
        states only). Keeping one writer
        guarantees the sensor never diverges from the reauth escalation.
        """
        if status == self._crypto_status_state and reason == self._crypto_status_reason:
            return

        self._crypto_status_state = status
        self._crypto_status_reason = reason
        self._crypto_status_changed_at = time.time()

        try:
            self.async_set_updated_data(self.data)
        except Exception:
            pass

    @property
    def api_status(self) -> StatusSnapshot:
        """Return a snapshot describing the current API polling health."""
        return StatusSnapshot(
            state=self._api_status_state,
            reason=self._api_status_reason,
            changed_at=self._api_status_changed_at,
        )

    @property
    def fcm_status(self) -> StatusSnapshot:
        """Return a snapshot describing the current push transport health."""
        return StatusSnapshot(
            state=self._fcm_status_state,
            reason=self._fcm_status_reason,
            changed_at=self._fcm_status_changed_at,
        )

    @property
    def crypto_status(self) -> StatusSnapshot:
        """Return a snapshot describing end-to-end location decryption health."""
        return StatusSnapshot(
            state=self._crypto_status_state,
            reason=self._crypto_status_reason,
            changed_at=self._crypto_status_changed_at,
        )

    @property
    def is_fcm_connected(self) -> bool:
        """Convenience boolean for entities relying on push transport availability."""
        return self._fcm_status_state == FcmStatus.CONNECTED

    def _note_empty_location_result(self, dev_id: str, dev_name: str) -> None:
        """Log an empty location result and report a silent device once.

        Every empty result logs at DEBUG with the device name. The poll that
        completes ``_EMPTY_LOCATION_QUORUM`` consecutive empty results adds one
        INFO record that names the device by its id only: the poll loop repeats
        unattended, so the display name stays at DEBUG (AGENTS.md section 5 (b)).
        Only logging state lives here; the auth and cycle bookkeeping of the
        caller is untouched.
        """
        _LOGGER.debug("No location data returned for %s", dev_name)
        streaks = getattr(self, "_empty_location_streak", None)
        if streaks is None:
            # Coordinators built via __new__ in tests skip __init__.
            streaks = self._empty_location_streak = {}
        streak = streaks.get(dev_id, 0) + 1
        streaks[dev_id] = streak
        if streak == _EMPTY_LOCATION_QUORUM:
            _LOGGER.info(
                "Device %s returned no location data in %d consecutive polls",
                dev_id,
                streak,
            )

    def _note_location_content(self, dev_id: str, dev_name: str) -> None:
        """Clear the silence streak and report the recovery of a silent device.

        Logs one INFO record (id only) when the device had been reported silent
        by ``_note_empty_location_result``; a streak below the quorum is cleared
        without any record at INFO.
        """
        streaks = getattr(self, "_empty_location_streak", None)
        if not streaks:
            return
        streak = streaks.pop(dev_id, 0)
        if streak >= _EMPTY_LOCATION_QUORUM:
            _LOGGER.info(
                "Device %s is returning location data again after %d empty polls",
                dev_id,
                streak,
            )
            _LOGGER.debug("Location data is back for %s", dev_name)

    def _log_idle_poll_diagnostics(
        self, dev_id: str, dev_name: str, *, source: str
    ) -> None:
        """Emit a single DEBUG line per device per cycle to tell an expected
        idle poll (no reporter in range to relay a BLE tag's location) apart
        from a possible FCM transport fault.

        Only rendered at DEBUG level, so this stays silent during normal
        operation and adds no WARNING/INFO noise. ``source`` distinguishes the
        inner empty result (``empty-result``) from the outer poll-guard timeout
        (``outer-timeout``) and from a request that was never accepted
        (``not-accepted``).

        The rendered line still opens with "Idle poll", which for the third
        source is the very conflation this diagnostic is meant to help take
        apart. Left as it is on purpose: four assertions in
        ``tests/test_poll_timeout_hardening.py`` match on that exact prefix, so
        renaming it is a change with its own blast radius and does not belong to
        the step that merely adds the source. ``source`` disambiguates it in the
        meantime.
        """
        if not _LOGGER.isEnabledFor(logging.DEBUG):
            return
        now = time.time()
        cached = self._device_location_data.get(dev_id) or {}
        last_seen = cached.get("last_seen")
        try:
            report_age = f"{now - float(last_seen):.0f}s" if last_seen else "never"
        except (TypeError, ValueError):
            report_age = "unknown"
        changed_at = self._fcm_status_changed_at
        fcm_age = f"{now - changed_at:.0f}s" if changed_at else "unknown"
        _LOGGER.debug(
            "Idle poll for %s (%s): last report %s ago, FCM status %s for %s",
            dev_name,
            source,
            report_age,
            self._fcm_status_state,
            fcm_age,
        )

    @property
    def consecutive_timeouts(self) -> int:
        """Return the number of consecutive poll timeouts."""
        return self._consecutive_timeouts

    @property
    def last_poll_result(self) -> str | None:
        """Return the last recorded poll result ("success"/"failed")."""
        return self._last_poll_result

    def note_decrypt_failure(
        self, *, stale: bool, error: Exception, device: str | None = None
    ) -> bool:
        """Record a location-decryption failure and decide whether to escalate.

        Single entry point shared by the poll path and the FCM push path so a
        push-only setup escalates identically (no second, divergent code path).

        A stale/incompatible or missing shared key is an auth-fatal condition: the
        bundle can no longer decrypt the server-side owner key, so every location
        report fails. Home Assistant cannot refresh the shared key itself (it is
        derived from the user's lock-screen knowledge factor via the interactive
        browser flow), so the only correct automatic action is to open the reauth
        flow and let the user supply a fresh secrets.json.

        Args:
            stale: True for a per-tracker ``StaleOwnerKeyError`` (owner-key version
                mismatch). Such failures are NOT account-wide and must not drive
                the account-wide reauth counter -- an account reauth would not fix a
                single outdated tracker.
            error: The decryption error, kept for diagnostics/logging.
            device: Optional device name for log context.

        Returns:
            True if the caller should escalate to ``ConfigEntryAuthFailed`` now
            (account-wide shared-key failure persisted past the threshold and the
            cooldown allows a fresh escalation); False otherwise.
        """
        self._last_decrypt_error = f"{type(error).__name__}: {error}"

        if stale:
            # Per-tracker outdated owner key: log per occurrence, do not touch the
            # account-wide counter, never escalate to reauth here. The full
            # user-facing repair issue is added with the diagnostic sensor work.
            self._set_crypto_status(CryptoStatus.TRACKER_KEY_OUTDATED)
            _LOGGER.warning(
                "Tracker %s is encrypted with an outdated owner key version (%s); "
                "remove and re-pair this tracker. Other devices are unaffected.",
                device or "?",
                type(error).__name__,
            )
            return False

        # Account-wide shared-key failure: map the concrete error to the
        # diagnostic state from the SAME signal that drives the escalation below
        # (D6, single source -- no second, divergent state for the sensor).
        self._set_crypto_status(
            CryptoStatus.SHARED_KEY_MISSING
            if isinstance(error, SharedKeyMissingError)
            else CryptoStatus.SHARED_KEY_INVALID
        )

        self._consecutive_decrypt_failures += 1
        if self._consecutive_decrypt_failures < _MAX_DECRYPT_FAILURES:
            # Below threshold: give the in-decrypt self-heal (force_refresh / blind
            # refresh) another cycle before escalating to reauth.
            _LOGGER.warning(
                "Location decryption failed for %s (%d/%d): %s. Will retry on the "
                "next cycle before any re-authentication.",
                device or "?",
                self._consecutive_decrypt_failures,
                _MAX_DECRYPT_FAILURES,
                error,
            )
            return False

        now = self._monotonic()
        last = self._last_decrypt_reauth_monotonic
        if last is not None and (now - last) < _DECRYPT_REAUTH_COOLDOWN_S:
            # A previous escalation exists and is still cooling down: keep logging
            # but do not re-fire the reauth flow on every poll cycle. The first
            # escalation (last is None) never enters this branch, so it can never
            # be suppressed by the host's process uptime.
            _LOGGER.debug(
                "Decryption still failing for %s but reauth escalation is in "
                "cooldown (%.0fs since last).",
                device or "?",
                now - last,
            )
            return False

        self._last_decrypt_reauth_monotonic = now
        self._consecutive_decrypt_failures = 0
        self._set_auth_state(
            failed=True,
            reason=(
                "Location decryption keeps failing; the shared key is stale and a "
                "fresh secrets.json (re-authentication) is required"
            ),
        )
        _LOGGER.error(
            "Location decryption failed %d times in a row (%s); escalating to "
            "re-authentication. A fresh secrets.json is required.",
            _MAX_DECRYPT_FAILURES,
            error,
        )
        return True

    @staticmethod
    def _is_device_local_decrypt_failure(error: DecryptionError | None) -> bool:
        """True ONLY for the one provably device-local class: own-report staleness.

        Fail-SAFE discriminator: a sibling decrypting proves the account keys are
        healthy, so a device whose OWN server reports all fail authentication
        (``OwnReportIdentityMismatchError`` -- e.g. a phone powered off for days, or
        a re-paired device) is device-local and a fresh secrets.json cannot fix it.

        EVERY other ``DecryptionError`` defaults to the account-wide reauth path:
        the explicit shared-key subclasses (``SharedKeyMissingError`` /
        ``SharedKeyMismatchError``) AND a plain ``DecryptionError`` from the
        identity-key derivation path ("Identity key decryption failed.") -- the
        owner/shared key feeding that derivation is account-wide, and a sibling
        decrypting from cached owner/EIK material cannot refute it. Defaulting an
        unrecognised failure to "account-wide / escalate" rather than "device-local
        / suppress" means a NEW raise-site fails safe instead of stranding a broken
        device without a reauth prompt (the Codex finding class). Single
        discriminator for both the cycle-error capture priority and the escalation
        gate so the two cannot drift apart.
        """
        return isinstance(error, OwnReportIdentityMismatchError)

    @classmethod
    def _prefer_account_wide_decrypt_error(
        cls,
        current: DecryptionError | None,
        candidate: DecryptionError,
    ) -> DecryptionError:
        """Pick the cycle's representative decrypt error, account-wide first.

        A single cycle can mix a device-local own-report failure (a phone powered
        off for days) with an account-wide failure on another device (a shared-key
        subclass, or a plain identity-key derivation failure). The verdict
        downgrades only the device-local case on sibling success, so the
        representative error must be the account-wide one whenever present.
        Otherwise an earlier-polled offline device would mask a genuine account-wide
        failure and suppress the reauth the user actually needs. Keeps the first
        error of equal severity (stable, order-independent for same-class errors).
        """
        if current is None:
            return candidate
        if cls._is_device_local_decrypt_failure(
            current
        ) and not cls._is_device_local_decrypt_failure(candidate):
            return candidate
        return current

    def _decrypt_failure_is_account_wide(
        self,
        cycle_decrypt_error: DecryptionError | None,
        cycle_had_successful_decrypt: bool,
    ) -> bool:
        """Does a cycle's decrypt failure implicate the account-wide shared key?

        Single source of truth shared by the escalation verdict
        (:meth:`_resolve_cycle_decrypt_outcome`) and the cycle-failure
        reconciliation (:meth:`_finalize_cycle_decrypt_state`) so the "escalate to
        reauth" decision and the "mark the coordinator update failed" decision
        cannot drift apart.

        - No decrypt error at all -> not account-wide.
        - Not a single device decrypted this cycle -> account-wide by the absence of
          any positive proof the shared key still works.
        - A sibling decrypted, so positive proof dominates, but it refutes ONLY the
          device-local own-report class (:meth:`_is_device_local_decrypt_failure`):
          ``OwnReportIdentityMismatchError`` (e.g. a phone powered off for days). The
          shared-key subclasses AND a plain identity-key derivation failure remain
          account-wide -- a sibling decrypting from cached owner/EIK material cannot
          refute them, so they keep their reauth prompt (the Codex finding class).
        """
        if cycle_decrypt_error is None:
            return False
        if not isinstance(cycle_decrypt_error, DecryptionError):
            # A transient owner-key lookup miss (OwnerKeyLookupTransientError, base
            # Exception, NOT a DecryptionError) is never an account-wide credential
            # failure. Option B already keeps it out of the per-device
            # ``except DecryptionError`` blocks; this is the defense-in-depth guard
            # so the shared discriminator never drives a transient miss into reauth.
            return False
        if not cycle_had_successful_decrypt:
            return True
        return not self._is_device_local_decrypt_failure(cycle_decrypt_error)

    def _resolve_cycle_decrypt_outcome(
        self,
        *,
        cycle_decrypt_error: DecryptionError | None,
        cycle_had_successful_decrypt: bool,
        cycle_had_stale_key: bool,
    ) -> bool:
        """Resolve a poll cycle's decrypt verdict and return whether to escalate.

        Performs the post-loop crypto-sensor and reauth-budget side effects and
        returns ``True`` iff the caller must escalate to ``ConfigEntryAuthFailed``.

        Positive proof dominates (D7), but ONLY for the device-local own-report
        class: if ANY device produced an authenticated coordinate this cycle
        (``cycle_had_successful_decrypt``), a sibling's
        ``OwnReportIdentityMismatchError`` -- e.g. a phone powered off for days whose
        own server reports no longer decrypt -- is a per-device condition that must
        NOT advance the account-wide reauth budget and must NOT trigger reauth. This
        mirrors the per-tracker ``StaleOwnerKeyError`` (``stale=True``) treatment
        ("other devices are unaffected").

        Every OTHER ``DecryptionError`` is the exception (see
        ``_is_device_local_decrypt_failure``): the shared-key subclasses
        (``SharedKeyMissingError`` / ``SharedKeyMismatchError``) AND a plain
        identity-key derivation failure ("Identity key decryption failed.") all rest
        on the account-wide owner/shared key, so a sibling decrypting from cached
        owner/EIK material does NOT prove the key is healthy. Such failures stay on
        the account-wide reauth path even with sibling success -- otherwise a mixed
        cycle would strand the broken device without ever prompting for fresh
        secrets (the Codex finding class).

        Before the positive-proof gate the account-wide counter was advanced
        whenever a decrypt error was seen, regardless of sibling success, so a
        single offline device dragged a healthy multi-device account across
        ``_MAX_DECRYPT_FAILURES`` and tripped a spurious reauth (removing the
        offline device silenced it -- the observed bug).
        """
        # The explicit ``is not None`` narrows the type for the typed
        # note_decrypt_failure(error=...) call; it is logically redundant with the
        # predicate (which returns False for None) but keeps the predicate the
        # single source of truth for the account-wide DECISION.
        if cycle_decrypt_error is not None and self._decrypt_failure_is_account_wide(
            cycle_decrypt_error, cycle_had_successful_decrypt
        ):
            # Account-wide failure: either NOT ONE device decrypted this cycle, OR
            # the failure is anything other than the device-local own-report class (a
            # shared-key subclass or a plain identity-key derivation failure) that a
            # sibling's cached-material success cannot refute. Advance the
            # consecutive-cycle counter exactly once; escalate (cooldown-gated) on
            # threshold.
            return self.note_decrypt_failure(
                stale=False, error=cycle_decrypt_error, device=None
            )

        # Either a clean cycle, or a per-device decrypt failure that a sibling
        # success refutes as account-wide. Never escalate to reauth from here.
        if cycle_decrypt_error is not None:
            # cycle_had_successful_decrypt is True here (guard above): a sibling
            # proved the shared key works, so this failure is device-local. Log it
            # as a warning -- the account-wide escalation ERROR is reserved for the
            # genuinely account-wide path -- and leave the reauth budget untouched.
            _LOGGER.warning(
                "Location decryption failed for a device this cycle while other "
                "devices decrypted successfully; the account shared key is "
                "healthy, so no re-authentication is required. The affected "
                "device may be powered off or its own server reports may be "
                "stale: %s",
                cycle_decrypt_error,
            )
        if cycle_had_successful_decrypt and not cycle_had_stale_key:
            # Positive proof and no per-tracker stale key: heal the diagnostic
            # sensor (D6 OK source, single writer) and clear the error class with
            # it. Gating on ``not cycle_had_stale_key`` keeps a per-tracker
            # StaleOwnerKeyError's error class intact while its tracker_key_outdated
            # status persists.
            self._set_crypto_status(CryptoStatus.OK)
            self._last_decrypt_error = None
        if cycle_had_successful_decrypt:
            # Clear the consecutive-decrypt counter only on POSITIVE proof the
            # account-wide shared key works, via the shared success entry point so
            # the poll path and the FCM background-push decode cannot drift apart.
            self.note_decrypt_success()
        return False

    def _finalize_cycle_decrypt_state(
        self,
        *,
        cycle_decrypt_error: DecryptionError | None,
        cycle_had_successful_decrypt: bool,
        cycle_had_stale_key: bool,
        cycle_failed: bool,
        last_exception: Exception | None,
    ) -> tuple[bool, Exception | None, ConfigEntryAuthFailed | None]:
        """Apply the cycle's decrypt verdict and reconcile the cycle-failure state.

        Runs :meth:`_resolve_cycle_decrypt_outcome` (the counter/sensor side
        effects) and then reconciles ``cycle_failed`` / ``last_exception`` with the
        verdict in ONE place, returning ``(cycle_failed, last_exception,
        reauth_exc)``. ``reauth_exc`` is non-``None`` iff the caller must escalate
        (request reauth and abort the cycle).

        The decrypt error must NOT contribute to the cycle-failure state at the
        point it is caught: whether it fails the whole coordinator update depends on
        the cross-device outcome resolved only after the loop. A per-device
        own-report failure that a sibling success refutes is benign and leaves
        ``cycle_failed`` / ``last_exception`` untouched, so a single offline/stale
        device no longer marks every poll failed (the Codex finding). Only a
        genuinely account-wide failure (see :meth:`_decrypt_failure_is_account_wide`)
        surfaces on the coordinator update; at the consecutive-cycle threshold it
        escalates to reauth. Failures from other device errors (timeouts, transient
        auth) set ``cycle_failed`` / ``last_exception`` independently and are
        preserved here untouched.
        """
        if self._resolve_cycle_decrypt_outcome(
            cycle_decrypt_error=cycle_decrypt_error,
            cycle_had_successful_decrypt=cycle_had_successful_decrypt,
            cycle_had_stale_key=cycle_had_stale_key,
        ):
            reauth_exc = ConfigEntryAuthFailed(
                "Location decryption keeps failing: the shared key "
                "is stale; a fresh secrets.json (re-authentication) "
                "is required"
            )
            reauth_exc.reauth_code = ReauthReasonCode.DECRYPT_STALE_KEY
            return True, reauth_exc, reauth_exc
        if self._decrypt_failure_is_account_wide(
            cycle_decrypt_error, cycle_had_successful_decrypt
        ):
            # Genuine account-wide decrypt failure below the reauth threshold: the
            # cycle failed to decrypt account data, so surface it on the coordinator
            # update even though we do not yet force reauth.
            cycle_failed = True
            if last_exception is None:
                last_exception = cycle_decrypt_error
        # Otherwise: a clean cycle, or a per-device failure a sibling success
        # refuted. The decrypt error (if any) is benign and must NOT mark the
        # coordinator update failed.
        return cycle_failed, last_exception, None

    def note_decrypt_success(self) -> None:
        """Record a proven location decryption and clear the reauth budget.

        Shared success entry point that mirrors :meth:`note_decrypt_failure`. The
        account-wide ``_consecutive_decrypt_failures`` counter is *fed* from more
        than one source -- the poll cycle and the FCM background-push decode both
        increment it through ``note_decrypt_failure`` -- so the symmetric clear
        must be reachable from every automatic source that can observe positive
        proof the shared key still decrypts. A successful own/crowd location
        decrypt is exactly that proof, so it resets the counter that drives
        ``ConfigEntryAuthFailed`` escalation; otherwise non-consecutive failures
        from one source accumulate to the threshold and trip a spurious reauth.
        Idempotent and cheap when the counter is already zero.

        Intentionally touches ONLY the account-wide counter, not the diagnostic
        sensor surface (:class:`CryptoStatus` and ``_last_decrypt_error``): those
        are owned by the poll cycle, whose OK transition is additionally gated on
        the absence of a per-tracker stale key so it never flickers
        ``tracker_key_outdated`` -- nor the error class behind it -- away. A
        per-tracker ``StaleOwnerKeyError`` records ``_last_decrypt_error`` for the
        sensor while deliberately leaving this counter at zero, so clearing that
        field here would erase a still-relevant diagnostic on the very cycle a
        sibling tracker decrypts. A single background decode likewise has no
        cross-tracker cycle view, so this shared entry point only clears the
        account-wide reauth budget and leaves the per-tracker sensor surface to
        the poll loop. The push path heals the *account-wide* failure states via
        :meth:`note_background_decrypt_success`, which wraps this method.
        """
        if self._consecutive_decrypt_failures > 0:
            _LOGGER.info(
                "Location decryption succeeded; clearing %d decrypt failure(s).",
                self._consecutive_decrypt_failures,
            )
            self._consecutive_decrypt_failures = 0

    def note_background_decrypt_success(self) -> None:
        """Clear the reauth budget and heal the account-wide sensor from a push.

        Called only from the FCM background-push decode on positive proof of an
        authenticated coordinate (see ``_note_decrypt_success_for_entry``). It
        first clears the shared reauth budget via :meth:`note_decrypt_success`,
        then lifts the diagnostic encryption-key sensor out of the *account-wide*
        failure states (``SHARED_KEY_INVALID`` / ``SHARED_KEY_MISSING``) that the
        proof refutes: a successful decrypt with the account owner key proves the
        shared key works regardless of any single cycle's cross-tracker view, so
        push-only setups whose scheduled polls stay idle no longer leave the
        sensor stuck on a stale-key status the account has already recovered from.

        It deliberately does NOT touch ``TRACKER_KEY_OUTDATED``: a single
        background decode cannot prove a per-tracker outdated owner key has been
        re-paired (no cross-tracker view), so that per-tracker state -- and its
        error class -- stays owned by the poll cycle, which alone observes the
        absence of a stale key across all trackers. ``UNKNOWN`` is left untouched
        too: there is no failure to heal and the OK semantics stay anchored to
        observed account-wide proof, not fabricated before the first real signal.
        Routes through the single ``_set_crypto_status`` writer, so the sensor
        never diverges from the reauth escalation it visualizes.
        """
        self.note_decrypt_success()
        if self._crypto_status_state in (
            CryptoStatus.SHARED_KEY_INVALID,
            CryptoStatus.SHARED_KEY_MISSING,
        ):
            # Move status and error class together: they are one sensor surface.
            self._set_crypto_status(CryptoStatus.OK)
            self._last_decrypt_error = None

    def _is_on_hass_loop(self) -> bool:
        """Return True if currently executing on the HA event loop thread."""
        loop = self.hass.loop
        try:
            return asyncio.get_running_loop() is loop
        except RuntimeError:
            return False

    def _run_on_hass_loop(
        self,
        func: Callable[..., None],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Schedule a plain callable to run on the HA loop thread ASAP.

        Note:
        - This is intentionally **fire-and-forget**; `call_soon_threadsafe` does not
          return the callable's result to the caller. Only use with functions that
          **return None** and are safe to run on the HA loop.
        """
        if kwargs:
            self.hass.loop.call_soon_threadsafe(
                functools.partial(func, *args, **kwargs)
            )
        else:
            self.hass.loop.call_soon_threadsafe(func, *args)

    def _dispatch_async_request_refresh(
        self, *, task_name: str, log_context: str
    ) -> None:
        """Invoke ``async_request_refresh`` safely regardless of its implementation."""
        fn = getattr(self, "async_request_refresh", None)
        if not callable(fn):
            return

        def _invoke() -> None:
            try:
                result = fn()
                if inspect.isawaitable(result):
                    self.hass.async_create_task(result, name=task_name)
            except Exception as err:
                _LOGGER.debug(
                    "async_request_refresh dispatch failed (%s): %s", log_context, err
                )

        if self._is_on_hass_loop():
            _invoke()
        else:
            self._run_on_hass_loop(_invoke)

    def _schedule_short_retry(self, delay_s: float = 5.0) -> None:
        """Schedule a short, coalesced refresh instead of shifting the poll baseline.

        Rationale:
        - When FCM/push is not ready, we *do not* advance `_last_poll_mono`.
          Advancing the baseline hides readiness transitions and can put the
          scheduler to "sleep". Instead, we request a short follow-up refresh.

        Behavior:
        - Coalesces multiple calls by cancelling a pending callback first.
        - Always runs on the HA event loop.

        Args:
            delay_s: Delay in seconds before requesting a coordinator refresh.
        """

        def _do_schedule() -> None:
            # Cancel a pending short retry (coalesce)
            if self._short_retry_cancel is not None:
                try:
                    self._short_retry_cancel()
                except Exception:  # defensive
                    pass
                finally:
                    self._short_retry_cancel = None

            def _cb(_now: datetime) -> None:
                # Clear handle and request a refresh (non-blocking)
                self._short_retry_cancel = None
                self._dispatch_async_request_refresh(
                    task_name=f"{DOMAIN}.short_retry_refresh",
                    log_context="short retry",
                )

            self._short_retry_cancel = async_call_later(
                self.hass, max(0.0, float(delay_s)), _cb
            )

        if self._is_on_hass_loop():
            _do_schedule()
        else:
            self._run_on_hass_loop(_do_schedule)

    async def _handle_dr_event(self, _event: Event) -> None:
        """Handle Device Registry changes by rebuilding poll targets (rare)."""
        self._reindex_poll_targets_from_device_registry()
        # After changes, request a refresh so the next tick uses the new target sets.
        self._dispatch_async_request_refresh(
            task_name=f"{DOMAIN}.dr_event_refresh",
            log_context="device registry event",
        )

    def _compute_type_cooldown_seconds(self, report_hint: str | None) -> int:
        """Return a server-aware cooldown duration in seconds for a crowdsourced report type.

        Derived from POPETS'25 observations:
        - "in_all_areas": ~10 min throttle window (minimum).
        - "high_traffic": ~5 min throttle window (minimum).

        IMPORTANT:
        - To guarantee effect, the applied cooldown is **never shorter than** the
          configured `location_poll_interval`. This ensures at least one scheduled
          poll cycle is skipped in practice.
        """
        if not report_hint:
            return 0

        # Guarantee the cooldown always spans at least one poll interval
        effective_poll = max(1, int(self.location_poll_interval))
        if report_hint == "in_all_areas":
            base_cooldown = _COOLDOWN_MIN_IN_ALL_AREAS_S
        elif report_hint == "high_traffic":
            base_cooldown = _COOLDOWN_MIN_HIGH_TRAFFIC_S
        else:
            return 0

        return max(base_cooldown, effective_poll)

    def _apply_report_type_cooldown(
        self, device_id: str, report_hint: str | None
    ) -> None:
        """Apply a per-device **poll** cooldown based on the crowdsourced report type.

        - Does nothing for None/unknown hints.
        - Uses monotonic time, and **extends** any existing cooldown (takes the max).
        - Internal only; does not touch public APIs or entity attributes.
        """
        try:
            seconds = int(self._compute_type_cooldown_seconds(report_hint))
        except Exception:  # defensive
            seconds = 0
        if seconds <= 0:
            return

        now_mono = time.monotonic()
        new_deadline = now_mono + float(seconds)
        prev_deadline = self._device_poll_cooldown_until.get(device_id, 0.0)
        if new_deadline > prev_deadline:
            self._device_poll_cooldown_until[device_id] = new_deadline
            _LOGGER.debug(
                "Applied %ss poll cooldown for %s (hint='%s', poll_interval=%ss)",
                seconds,
                device_id,
                report_hint,
                self.location_poll_interval,
            )

    # -------------------- Public read-only state for diagnostics/UI --------------------
    @property
    def is_polling(self) -> bool:
        """Expose current polling state (public read-only API).

        Returns:
            True if a polling cycle is currently in progress.
        """
        return self._is_polling

    def get_fcm_acquire_duration_seconds(
        self,
    ) -> float | None:
        """Duration between 'setup_start_monotonic' and 'fcm_acquired_monotonic'."""
        from .helpers.stats import get_duration as _get_duration_impl

        return _get_duration_impl(
            self.get_metric("setup_start_monotonic"),
            self.get_metric("fcm_acquired_monotonic"),
        )

    def get_last_poll_duration_seconds(
        self,
    ) -> float | None:
        """Duration of the most recent sequential polling cycle (if recorded)."""
        return self._get_duration("last_poll_start_mono", "last_poll_end_mono")

    # -------------------- FCM readiness checks --------------------
    def _is_fcm_ready_soft(self) -> bool:
        """Return True if push transport appears ready (no awaits, no I/O).

        Priority order:
          1) Ask API (single source of truth if available).
          2) Receiver-level booleans.
          3) Push-client heuristic (run_state + do_listen).
        """
        try:
            # 1) API knowledge (preferred)
            try:
                fn = getattr(self.api, "is_push_ready", None)
                if callable(fn):
                    return bool(fn())
            except Exception:
                pass

            # 2) Receiver-level flags
            fcm = self.hass.data.get(DOMAIN, {}).get("fcm_receiver")
            if not fcm:
                return False
            fatal_error: str | None = getattr(fcm, "_fatal_error", None)
            entry_id = self._entry_id()
            fatal_by_entry = getattr(fcm, "_fatal_errors", None)
            if isinstance(fatal_by_entry, Mapping) and entry_id:
                fatal_error = fatal_by_entry.get(entry_id) or fatal_error
            if isinstance(fatal_error, str) and fatal_error:
                return False
            for attr in ("is_ready", "ready"):
                val = getattr(fcm, attr, None)
                if isinstance(val, bool):
                    return val

            # 3) Heuristic: push client state (no enum import)
            pc = getattr(fcm, "pc", None)
            if pc is not None:
                state = getattr(pc, "run_state", None)
                state_name = getattr(state, "name", state)
                if state_name == "STARTED" and bool(getattr(pc, "do_listen", False)):
                    return True

            return False
        except Exception:
            return False

    def _note_fcm_deferral(self, now_mono: float) -> None:
        """Advance a quiet escalation timeline while FCM is not ready.

        FIX: Use less aggressive log levels to reduce log spam (#124).
        Emits at most:
            - one INFO after ~2 minutes (was WARNING after 60s)
            - one WARNING after ~5 minutes (was ERROR after 300s)
        Resets when readiness returns.
        """
        if self._fcm_defer_started_mono == 0.0:
            self._fcm_defer_started_mono = now_mono
            self._fcm_last_stage = 0
            self._set_fcm_status(
                FcmStatus.DEGRADED,
                reason="Push transport not ready; awaiting connection",
            )
            return
        elapsed = now_mono - self._fcm_defer_started_mono
        # Stage 1: After 2 minutes, log at INFO level (not WARNING)
        if elapsed >= 120 and self._fcm_last_stage < 1:
            self._fcm_last_stage = 1
            _LOGGER.info(
                "Push transport connection taking longer than expected (2 min). "
                "Push updates may be delayed, but polling continues."
            )
            self._set_fcm_status(
                FcmStatus.DEGRADED,
                reason="Push transport waiting for connection (2 min elapsed)",
            )
        # Stage 2: After 5 minutes, log at WARNING level (not ERROR)
        if elapsed >= 300 and self._fcm_last_stage < 2:
            self._fcm_last_stage = 2
            _LOGGER.warning(
                "Push transport not connected after 5 minutes. "
                "Check network connectivity and credentials if this persists."
            )
            self._set_fcm_status(
                FcmStatus.DISCONNECTED,
                reason="Push transport not connected after prolonged wait",
            )

    def _clear_fcm_deferral(self) -> None:
        """Clear the escalation timeline once FCM becomes ready (log once)."""
        if self._fcm_defer_started_mono:
            _LOGGER.info("FCM/push is ready; resuming scheduled polling.")
        self._fcm_defer_started_mono = 0.0
        self._fcm_last_stage = 0
        self._degraded_mode_warned = False
        self._set_fcm_status(FcmStatus.CONNECTED)

    def _nudge_fcm_supervisor(self) -> None:
        """Ask the FCM supervisor to wake up and retry connection immediately."""
        try:
            fcm = self.hass.data.get(DOMAIN, {}).get("fcm_receiver")
            if fcm is None:
                return
            nudge_fn = getattr(fcm, "nudge_retry", None)
            if callable(nudge_fn):
                entry_id = self._entry_id()
                nudge_fn(entry_id)
        except Exception:  # noqa: BLE001
            pass

    # -------------------- Poll timing prediction --------------------
    def _get_predicted_poll_time(self) -> float | None:
        """Predict the earliest next update time based on device histories.

        The histories of ignored trackers and of trackers whose automatic
        location requests are switched off (#242) are skipped. Both sets are read
        here because this runs before the refresh reads the ignore list. Other
        trackers the cycle does not poll, such as one disabled in Home
        Assistant's device registry, still count with their stored history.
        """

        history_store = getattr(self, "_device_update_history", None)
        if not history_store:
            return None

        excluded = self._get_ignored_set() | self._get_automatic_polling_disabled_set()
        predictions: list[float] = []

        for dev_id, history in history_store.items():
            if dev_id in excluded:
                continue
            if len(history) < 2:
                continue

            intervals = [
                history[idx + 1] - history[idx] for idx in range(len(history) - 1)
            ]
            avg_interval = mean(intervals)

            if len(intervals) >= 2 and stdev(intervals) > 300:
                continue

            predictions.append(history[-1] + avg_interval)

        if not predictions:
            return None

        return min(predictions)

    # -------------------- Push transport error handling --------------------
    def _note_push_transport_problem(self, cooldown_s: int = 90) -> None:
        """Enter a temporary cooldown after a push transport failure to avoid spamming.

        Args:
            cooldown_s: The duration of the cooldown in seconds.
        """
        self._push_cooldown_until = time.monotonic() + cooldown_s
        self._push_ready_memo = False
        _LOGGER.debug(
            "Entering push cooldown for %ss after transport failure", cooldown_s
        )
        self._set_fcm_status(
            FcmStatus.DEGRADED,
            reason=f"Push transport recovering from error (cooldown {cooldown_s}s)",
        )

    def force_poll_due(self) -> None:
        """Force the next poll to be due immediately (no private access required externally)."""
        effective_interval = max(self.location_poll_interval, self.min_poll_interval)
        # Move the baseline back so that (now - _last_poll_mono) >= effective_interval
        self._last_poll_mono = time.monotonic() - float(effective_interval)

    # ---------------------------- HA Coordinator ----------------------------
    async def _async_update_data(self) -> list[dict[str, Any]]:
        """Provide cached device data; trigger background poll if due.

        Discovery semantics:
        - Refresh the **full** lightweight device list (no executor) on a paced interval
          and reuse the cached list between refreshes.
        - Update presence and metadata caches for **all** devices.
        - The published snapshot (`self.data`) contains **all** devices (for dynamic entity creation).
        - The sequential **polling cycle** polls devices that are enabled in HA's Device Registry
          **for this config entry** and not explicitly ignored in integration options.

        Returns:
            A list of dictionaries, where each dictionary represents a device's state.

        Raises:
            ConfigEntryAuthFailed: If authentication fails during device list fetching.
            UpdateFailed: For other transient or unexpected errors.
        """
        try:
            # Check for fatal FCM errors (for example, 404/401 during registration) to trigger re-auth
            # FIX: Only trigger re-auth after multiple consecutive failures (#114)
            entry = self.config_entry
            runtime = getattr(entry, "runtime_data", None)
            fcm_receiver = getattr(runtime, "fcm_receiver", None)

            fatal_error: str | None = None
            if fcm_receiver is not None:
                fatal_by_entry = getattr(fcm_receiver, "_fatal_errors", None)
                entry_id = self._entry_id()

                if isinstance(fatal_by_entry, Mapping) and entry_id:
                    fatal_error = fatal_by_entry.get(entry_id)

            if isinstance(fatal_error, str) and fatal_error:
                # Iter-14 (Codex follow-up, channel-confusion): the FCM
                # receiver's supervisor-level "short-run crash loop" cap
                # publishes its terminal state into the SAME ``_fatal_errors``
                # map this re-auth escalation consumes. That defense is a
                # poison-message bulkhead (the user has to reload or
                # regenerate credentials per the repair issue, NOT
                # re-authenticate). Classify the fatal first so a cap-fired
                # listener-lifecycle fatal does not get reclassified as an
                # auth fatal after _FCM_ERROR_RETRY_THRESHOLD cycles and
                # push users into Home Assistant's re-auth flow.
                fatal_class = _classify_fcm_fatal(fatal_error)

                if fatal_class == _FCM_FATAL_CLASS_CRASH_LOOP_EXCLUDED:
                    # Reset any partial auth-error count so a later,
                    # unrelated auth fatal starts from zero rather than
                    # inheriting a stale count from the crash-loop window.
                    if self._fcm_error_count > 0:
                        _LOGGER.debug(
                            "FCM crash-loop fatal observed; resetting auth-"
                            "error counter (was %d). Repair issue surfaces "
                            "guidance, no re-auth.",
                            self._fcm_error_count,
                        )
                        self._fcm_error_count = 0
                        self._fcm_last_error = None
                else:
                    if fatal_class == _FCM_FATAL_CLASS_AUTH_IMMEDIATE:
                        # Fatal auth errors - fail immediately, no retry
                        _LOGGER.error(
                            "Fatal FCM authentication error: %s. Triggering re-authentication.",
                            fatal_error,
                        )
                        self._set_auth_state(failed=True, reason=fatal_error)
                        # F-4: this raise is caught by the ConfigEntryAuthFailed
                        # handler in _async_update_data (single recording choke
                        # point), so only tag the exception here; do NOT record
                        # directly to avoid a double record.
                        fcm_exc = ConfigEntryAuthFailed(fatal_error)
                        fcm_exc.reauth_code = ReauthReasonCode.FCM_AUTH_FATAL
                        raise fcm_exc

                    # Track consecutive FCM errors for transient issues
                    if fatal_error != self._fcm_last_error:
                        # New error type, reset counter
                        self._fcm_error_count = 1
                        self._fcm_last_error = fatal_error
                    else:
                        self._fcm_error_count += 1

                    # Only trigger re-auth after _FCM_ERROR_RETRY_THRESHOLD consecutive failures
                    if self._fcm_error_count >= _FCM_ERROR_RETRY_THRESHOLD:
                        _LOGGER.error(
                            "FCM error persisted after %d attempts: %s. Triggering re-authentication.",
                            self._fcm_error_count,
                            fatal_error,
                        )
                        self._set_auth_state(failed=True, reason=fatal_error)
                        # F-4: caught by the _async_update_data
                        # ConfigEntryAuthFailed handler (single recording choke
                        # point); tag only, do not record here.
                        fcm_exc = ConfigEntryAuthFailed(fatal_error)
                        fcm_exc.reauth_code = ReauthReasonCode.FCM_AUTH_FATAL
                        raise fcm_exc
                    else:
                        _LOGGER.warning(
                            "FCM error detected (%d/%d): %s. Will retry before triggering re-auth.",
                            self._fcm_error_count,
                            _FCM_ERROR_RETRY_THRESHOLD,
                            fatal_error,
                        )
            # No error - reset counter on successful check
            elif self._fcm_error_count > 0:
                _LOGGER.debug(
                    "FCM error cleared after %d attempts", self._fcm_error_count
                )
                self._fcm_error_count = 0
                self._fcm_last_error = None

            # One-time wait for FCM on first run.
            # FIX: Better user feedback during FCM initialization (#116)
            if not self._startup_complete:
                fcm_evt = getattr(self, "fcm_ready_event", None)
                if isinstance(fcm_evt, asyncio.Event) and not fcm_evt.is_set():
                    _LOGGER.info(
                        "Waiting for push notification service (FCM)... "
                        "This may take up to 15 seconds on first start."
                    )
                    try:
                        # Wait in 5-second increments with progress logging
                        for attempt in range(3):
                            try:
                                await asyncio.wait_for(fcm_evt.wait(), timeout=5.0)
                                _LOGGER.info("Push notification service is ready.")
                                break
                            except TimeoutError:
                                if attempt < 2:
                                    _LOGGER.debug(
                                        "FCM not ready yet, waiting... (%d/3)",
                                        attempt + 1,
                                    )
                        else:
                            # All 3 attempts exhausted
                            _LOGGER.warning(
                                "Push notification service not ready after 15s; "
                                "continuing in degraded mode. Push updates may be delayed."
                            )
                    except Exception as fcm_wait_err:
                        _LOGGER.debug("FCM wait error: %s", fcm_wait_err)
                self._startup_complete = True

            fcm_now_ready = self._is_fcm_ready_soft()
            if fcm_now_ready:
                self._set_fcm_status(FcmStatus.CONNECTED)
                # FIX: Clear deferral state as soon as FCM comes back, regardless
                # of whether a poll is due.  Without this, a forced poll during
                # an outage would update _last_poll_mono and the deferral would
                # linger until the next due window (up to effective_interval).
                if self._fcm_defer_started_mono:
                    self._clear_fcm_deferral()
                    # Trigger an immediate poll so we don't wait the full
                    # interval after a prolonged outage.
                    self.force_poll_due()
            elif self._fcm_last_stage < 2:
                self._set_fcm_status(
                    FcmStatus.DEGRADED,
                    reason="Push transport not ready; continuing with cached data",
                )

            list_check_mono = time.monotonic()
            list_due = (
                list_check_mono - self._last_list_poll_mono
            ) >= DEVICE_LIST_POLL_INTERVAL
            force_list_refresh = self._force_device_list_refresh
            if force_list_refresh:
                self._force_device_list_refresh = False
                force_reason = self._force_device_list_reason
                self._force_device_list_reason = None
                use_cached_list = False
                self._last_list_poll_mono = 0.0
                _LOGGER.debug(
                    "[%s] Device list refresh forced%s",
                    self._entry_id() or "unknown",
                    f" ({force_reason})" if force_reason else "",
                )
            else:
                use_cached_list = not list_due and bool(self._last_device_list)

            filtered_devices: list[dict[str, Any]]
            # Set only below, for a list fetched in this cycle that is not empty.
            # Cached, deferred and accepted-empty lists prove nothing about which
            # devices left the account (stale-device rules S-R1, S-R2).
            fresh_list_ids: set[str] | None = None
            dropped_candidates = 0
            if use_cached_list:
                filtered_devices = list(self._last_device_list)
                self._set_api_status(ApiStatus.OK)
                _LOGGER.debug(
                    "Skipping device list refresh (next in %.0fs); using cached list (%d devices)",
                    max(
                        0.0,
                        DEVICE_LIST_POLL_INTERVAL
                        - (list_check_mono - self._last_list_poll_mono),
                    ),
                    len(filtered_devices),
                )
            else:
                # 1) Fetch the lightweight FULL device list using the native async API
                payload = await self.api.async_get_basic_device_list()

                # Mirror the decoder's per-poll canonicless drop tally (written by
                # the main poll inside the fetch above) into the persisted stats.
                self._refresh_canonicless_drop_stats(self._entry_id())

                # Success path: if we were in an auth error state, clear it now.
                #
                # This is the strongest proof source in the integration FOR THE
                # ACCOUNT TOKEN, and that is all it is used for here. It was
                # briefly the transient counter's everyday reset source too; that
                # is history, and the paragraph below says why it had to stop.
                # `async_get_basic_device_list` has no non-throwing error exit.
                # Its OUTER `try` has nine except branches, all of which re-raise:
                # eight as `ConfigEntryAuthFailed` or `UpdateFailed`, the ninth a
                # bare `raise` that lets `asyncio.CancelledError` through. Four
                # further branches sit on inner `try`s and do swallow (a missing
                # username, two legacy-signature fallbacks, a missing email), but
                # none of them leaves the method. The method holds exactly one
                # `return`, and it is the success path. A return that got this far therefore
                # means Nova accepted the account token, which is exactly what
                # the AUTH STATE reports -- and an expired login raises before it
                # can reach this line, so it cannot mask itself here the way it
                # could through an empty location result. What the counter counts
                # is a different claim, and it is not settled here.
                #
                # Exactly ONE reset belongs in this branch, and it is the auth
                # state. Even that one belongs here and not above it: the cached
                # branch skips the call entirely and so proves nothing.
                #
                # The transient counter is deliberately NOT reset here, and that
                # is the correction of an earlier revision which did reset it.
                # This branch proves the ACCOUNT token; the counter counts the
                # action RPC accepting that same token, and the two are not the
                # same claim. Worse, they run on different clocks:
                # `DEVICE_LIST_POLL_INTERVAL` is a fixed 300 s while the poll
                # interval is an option between 60 s and 3600 s. Zeroing a
                # per-cycle counter from this cadence produced one of two errors
                # at every ratio, both found in review and both pinned as tests
                # now: at a long poll interval the refresh cleared the count
                # before every single attempt and the escalation starved, at a
                # short one rejections that were never consecutive reached the
                # threshold. A streak is broken where it is counted -- by a poll
                # cycle that books no rejection (see the end of the device loop)
                # or by a pass that finds nothing to poll at all.
                self._set_auth_state(failed=False)
                self._set_api_status(ApiStatus.OK)

                # Normalize payloads and filter/dedupe devices using pure helpers
                device_candidates = _normalize_device_list_impl(payload)
                filtered_devices, seen_ids = _filter_and_dedupe_impl(device_candidates)

                # Log skipped devices for debugging (pure helper doesn't log)
                _logged_ids: set[str] = set()
                for candidate in device_candidates:
                    if not isinstance(candidate, Mapping):
                        continue
                    dev_id_raw = candidate.get("id")
                    if not isinstance(dev_id_raw, str) or not dev_id_raw.strip():
                        _LOGGER.debug("Skipping device without valid id: %r", candidate)
                        dropped_candidates += 1
                        continue
                    dev_id = dev_id_raw.strip()
                    if dev_id in _logged_ids:
                        _LOGGER.debug(
                            "Skipping duplicate device entry for id=%s", dev_id
                        )
                    _logged_ids.add(dev_id)

                # Minimal hardening against false empties (keep prior behaviour)
                if not filtered_devices:
                    self._empty_list_streak += 1
                    # Use helper to decide if we should defer the empty list
                    if _should_defer_empty_list_impl(
                        self._empty_list_streak,
                        _EMPTY_LIST_QUORUM,
                        bool(self._last_device_list),
                    ):
                        # Defer clearing once; keep previous view stable.
                        _LOGGER.debug(
                            "Successful empty device list received (%d/%d). Deferring clear until quorum is met.",
                            self._empty_list_streak,
                            _EMPTY_LIST_QUORUM,
                        )
                        filtered_devices = list(self._last_device_list)
                    else:
                        _LOGGER.debug(
                            "Accepting empty device list after %d consecutive empties.",
                            self._empty_list_streak,
                        )
                        # Once accepted, forget any prior list so snapshot becomes empty below.
                        self._last_device_list = []
                        self._last_list_poll_mono = list_check_mono
                else:
                    # Non-empty result: reset streak and remember latest good list.
                    self._empty_list_streak = 0
                    self._last_device_list = list(filtered_devices)
                    self._last_list_poll_mono = list_check_mono
                    fresh_list_ids = {dev["id"] for dev in filtered_devices}

            # Presence TTL derives from the effective poll cadence
            effective_interval = max(
                self.location_poll_interval, self.min_poll_interval
            )
            self._presence_ttl_s = _calculate_presence_ttl_impl(effective_interval, 120)
            now_mono = time.monotonic()

            predicted_target = self._get_predicted_poll_time()
            predictive_due = False
            predictive_block = False
            if predicted_target is not None:
                wall_now = time.time()
                time_until = predicted_target - wall_now

                if 0 < time_until <= effective_interval:
                    delay = time_until + _PREDICTION_BUFFER_S
                    _LOGGER.debug(
                        "Predictive polling: update expected in %.1fs; scheduling retry in %.1fs (buffer=%ss)",
                        time_until,
                        delay,
                        _PREDICTION_BUFFER_S,
                    )
                    self._schedule_short_retry(delay)
                    predictive_block = True
                elif time_until <= 0:
                    _LOGGER.debug(
                        "Predictive polling: update overdue by %.1fs; polling now if limits allow.",
                        abs(time_until),
                    )
                    predictive_due = True

            # Cold-start guard: if the very first seen list is empty, treat it as transient
            if not filtered_devices and self._last_nonempty_wall == 0.0:
                raise UpdateFailed(
                    "Cold start: empty device list; treating as transient.",
                    translation_domain=DOMAIN,
                    translation_key="cold_start_empty_device_list",
                )

            # Maintain owner index for FCM fallback routing (entry-scoped).
            self._sync_owner_index(filtered_devices)

            ignored = self._get_ignored_set()

            # Record presence timestamps from the full list (unfiltered by ignore)
            if filtered_devices:
                for dev in filtered_devices:
                    dev_id = dev["id"]
                    self._present_last_seen[dev_id] = now_mono
                # Keep a diagnostics-only set mirroring the latest non-empty list
                self._present_device_ids = {dev["id"] for dev in filtered_devices}
                self._last_nonempty_wall = now_mono
            # If the list is empty, leave _present_last_seen untouched; TTL will decide availability.

            # 2) Update internal name/capability caches for ALL devices
            name_cache = self._ensure_device_name_cache()
            for dev in filtered_devices:
                dev_id = dev["id"]
                raw_name = dev.get("name")
                if isinstance(raw_name, str) and raw_name.strip():
                    name_cache[dev_id] = raw_name
                elif dev_id not in name_cache:
                    name_cache[dev_id] = dev_id

                # Normalize and cache the "can ring" capability
                if "can_ring" in dev:
                    can_ring = bool(dev.get("can_ring"))
                    slot = self._device_caps.setdefault(dev_id, {})
                    slot["can_ring"] = can_ring

            # 2.2) Seed the location cache with list-provided coordinates when available
            for dev in filtered_devices:
                dev_id = dev["id"]
                if dev_id in ignored:
                    continue

                lat = dev.get("latitude")
                lon = dev.get("longitude")
                if lat is None or lon is None:
                    continue

                if not self._normalize_coords(dev, device_label=dev_id):
                    continue

                # FIX #155: Skip seeding if cache already has fresher data
                # (e.g., from FCM push delivered between device list refreshes).
                # This prevents stale device-list coordinates from entering the
                # cache pipeline and causing unnecessary fusion/churn.
                incoming_ts = _normalize_epoch_seconds(dev.get("last_seen"))
                existing_cache = self._device_location_data.get(dev_id)
                if (
                    isinstance(existing_cache, Mapping)
                    and existing_cache.get("latitude") is not None
                ):
                    cached_ts = _normalize_epoch_seconds(
                        existing_cache.get("last_seen")
                    )
                    if cached_ts is not None and (
                        incoming_ts is None or incoming_ts <= cached_ts
                    ):
                        continue  # Cache has fresher or equal data, skip seed

                cache_seed = dict(dev)

                # Avoid poisoning the cache with explicit None accuracy values so
                # stationary clamps do not overwrite fresher accuracy readings.
                if cache_seed.get("accuracy") is None:
                    cache_seed.pop("accuracy", None)

                self.update_device_cache(dev_id, cache_seed, source="seed")

            # 2.5) Ensure Device Registry entries exist (service device + end-devices, namespaced)
            self._ensure_service_device_exists()
            created = self._ensure_registry_for_devices(filtered_devices, ignored)
            if created:
                _LOGGER.debug(
                    "Device Registry ensured/updated for %d device(s).", created
                )

            # 2.6) Remove tracker devices that left the Google account
            # (Integration Quality Scale rule ``stale-devices``). After the
            # ensure step, so creation and removal see the same registry. A
            # failure here must never fail the update cycle (S-R7).
            if fresh_list_ids is not None:
                try:
                    self._remove_stale_devices(
                        fresh_list_ids,
                        dropped_candidates=dropped_candidates,
                        canonicless_warn=int(
                            self.stats.get("canonicless_drop_warn", 0)
                        ),
                        canonicless_names=decoder.get_canonicless_name_counts(
                            self._entry_id() or ""
                        ),
                    )
                except Exception as err:  # noqa: BLE001 - S-R7, logged once
                    # Polling record (AGENTS.md section 5): the exception text
                    # can carry account data, which no level may log, so INFO
                    # and above name its class and DEBUG the log-safe summary.
                    _LOGGER.warning(
                        "[%s] Removing devices that left the Google account failed (%s)",
                        self._entry_id() or "unknown",
                        type(err).__name__,
                    )
                    _LOGGER.debug(
                        "[%s] Stale-device removal error detail: %s",
                        self._entry_id() or "unknown",
                        describe_exception(err),
                    )

            # 3) Decide whether to trigger a poll cycle (monotonic clock)
            # Build list of devices to POLL:
            # Poll devices that have at least one enabled DR entry for this config entry;
            # if a device has no DR entry yet, include it to allow initial discovery.
            # Trackers whose automatic location requests are switched off
            # (#242) stay out of every automatic path; read once per refresh.
            automatic_off = self._get_automatic_polling_disabled_set()
            devices_to_poll: list[dict[str, Any]] = []
            for dev in filtered_devices:
                dev_id = dev["id"]
                if dev_id in ignored:
                    continue
                if dev_id in automatic_off:
                    continue
                if (
                    dev_id in self._enabled_poll_device_ids
                    or dev_id not in self._devices_with_entry
                ):
                    devices_to_poll.append(dev)

            if (
                filtered_devices
                and not devices_to_poll
                and not self._is_polling
                and self._consecutive_transient_auth_failures > 0
            ):
                # The account HAS devices and not one of them is pollable: every
                # tracker is disabled or ignored. No cycle will run, so the
                # release path the cycle owns is unreachable and a booked streak
                # would stand for as long as that lasts -- until a tracker is
                # re-enabled weeks later and its first rejection inherits the old
                # count into a premature reauth.
                #
                # `filtered_devices` is part of the condition and not decoration.
                # Without it this also fires when the LIST came back empty, which
                # happens on a backend hiccup once the two-pass empty-list quorum
                # accepts it -- and clearing a rejection budget on the strength of
                # an outage is precisely what the second half of the end-of-cycle
                # condition below refuses to do.
                #
                # Measured on the PRE-cooldown list on purpose: a device held
                # back only by its poll cooldown has proved nothing, and clearing
                # on that would be a starvation path of its own.
                #
                # `not self._is_polling` for a related reason. The poll cycle is a
                # fire-and-forget task, so a refresh can arrive while an earlier
                # cycle is still asking its devices. A cycle in flight owns the
                # streak it is spending; clearing it here would turn a standing 2
                # into a 1 the moment that cycle books, and with no further cycle
                # able to run the escalation would be lost for good.
                _LOGGER.info(
                    "No pollable device remains; clearing %d transient auth failure(s).",
                    self._consecutive_transient_auth_failures,
                )
                self._consecutive_transient_auth_failures = 0
                self._last_transient_auth_error = None

            # Apply per-device poll cooldowns
            if self._device_poll_cooldown_until and devices_to_poll:
                devices_to_poll = [
                    d
                    for d in devices_to_poll
                    if now_mono >= self._device_poll_cooldown_until.get(d["id"], 0.0)
                ]

            # Cold start detection: force immediate poll on first install when devices have no location data
            is_cold_start = bool(
                self._last_poll_mono == 0.0
                and filtered_devices
                and not any(
                    self._device_location_data.get(dev["id"])
                    for dev in filtered_devices
                )
            )

            elapsed_since_poll = now_mono - self._last_poll_mono
            hard_limit_passed = elapsed_since_poll >= self.min_poll_interval

            # Use helper to determine if poll cycle is due
            due = _is_poll_cycle_due_impl(
                elapsed=elapsed_since_poll,
                effective_interval=effective_interval,
                predictive_block=predictive_block,
                predictive_due=predictive_due,
                hard_limit_passed=hard_limit_passed,
                is_cold_start=is_cold_start,
            )
            if self._poll_cycle_teardown:
                # Unload or stop is under way. The cancel helper can only cancel a
                # task that exists; a refresh that was already past the cancel
                # when it ran would otherwise create a fresh cycle here, against
                # a cache about to close or a core about to stop.
                _LOGGER.debug("Teardown in progress; not scheduling a poll cycle")
            elif due and not self._is_polling and devices_to_poll:
                force_poll = False
                fcm_ready = self._is_fcm_ready_soft()
                if not fcm_ready:
                    # No baseline jump; schedule a short retry and escalate politely.
                    self._note_fcm_deferral(now_mono)
                    defer_started = self._fcm_defer_started_mono or 0.0
                    if defer_started:
                        elapsed = now_mono - defer_started
                        if elapsed >= _PUSH_NOT_READY_TIMEOUT_S:
                            force_poll = True
                        else:
                            self._schedule_short_retry(
                                min(5.0, effective_interval / 2.0)
                            )
                    else:
                        self._schedule_short_retry(min(5.0, effective_interval / 2.0))
                elif self._fcm_defer_started_mono:
                    self._clear_fcm_deferral()

                if fcm_ready or force_poll:
                    if force_poll:
                        if not self._degraded_mode_warned:
                            _LOGGER.warning(
                                "Push transport unavailable for %ds; forcing poll cycle.",
                                _PUSH_NOT_READY_TIMEOUT_S,
                            )
                        else:
                            _LOGGER.debug(
                                "Push transport still unavailable; forcing poll cycle.",
                            )
                        # Nudge FCM supervisor to retry connection sooner
                        self._nudge_fcm_supervisor()
                    _LOGGER.debug(
                        "Scheduling background polling cycle (devices=%d, interval=%ds)",
                        len(devices_to_poll),
                        effective_interval,
                    )
                    # Sequential requests and inter-device delays must not hold
                    # up HA startup. Keep the handle for our teardown cleanup;
                    # entry-managed background tasks also cancel on unload.
                    poll_cycle = self._async_start_poll_cycle(
                        devices_to_poll, force=force_poll
                    )
                    if self.config_entry is not None:
                        self._poll_cycle_task = (
                            self.config_entry.async_create_background_task(
                                self.hass, poll_cycle, name=f"{DOMAIN}.poll_cycle"
                            )
                        )
                    else:
                        self._poll_cycle_task = self.hass.async_create_background_task(
                            poll_cycle, name=f"{DOMAIN}.poll_cycle"
                        )
            else:
                _LOGGER.debug(
                    "Poll not due (elapsed=%.1fs/%ss) or already running=%s",
                    now_mono - self._last_poll_mono,
                    effective_interval,
                    self._is_polling,
                )

            # 4) Build data snapshot for devices visible to the user (ignore-filter applied)
            visible_devices = [
                dev for dev in filtered_devices if dev["id"] not in ignored
            ]
            for dev in visible_devices:
                dev_id = dev["id"]
                cached_name = name_cache.get(dev_id)
                name = dev.get("name")
                if cached_name and (not isinstance(name, str) or not name.strip()):
                    dev["name"] = cached_name
            self._refresh_subentry_index(visible_devices)
            snapshot = await self._async_build_device_snapshot_with_fallbacks(
                visible_devices
            )
            self._store_subentry_snapshots(snapshot)

            _LOGGER.debug(
                "Returning %d device entries; next poll in ~%ds",
                len(snapshot),
                int(
                    max(
                        0,
                        effective_interval - (time.monotonic() - self._last_poll_mono),
                    )
                ),
            )
            return snapshot

        except asyncio.CancelledError:
            raise
        except ConfigEntryAuthFailed as auth_exc:
            # Surface up to HA to trigger re-auth flow; create Repairs issue & flag before bubbling up.
            reason = self._short_error_message(auth_exc)
            self._set_api_status(ApiStatus.REAUTH, reason=reason)
            self._set_auth_state(
                failed=True,
                reason=f"Auth failed while fetching device list: {reason}",
            )
            # FIX 3: single recording choke point for the awaited-refresh path.
            # The reason code rides on the exception attribute (set at the raise
            # site, e.g. api.py / polling FCM-fatal); default to UNKNOWN when the
            # exception carries none.
            self.record_reauth_reason(
                getattr(auth_exc, "reauth_code", ReauthReasonCode.UNKNOWN),
                origin="polling.py:_async_update_data",
            )
            raise auth_exc
        except UpdateFailed as update_err:
            # Let pre-wrapped UpdateFailed bubble as-is after updating status
            self._set_api_status(
                ApiStatus.ERROR,
                reason=self._short_error_message(update_err),
            )
            raise
        except Exception as err:
            # Record and raise as UpdateFailed per coordinator contract
            self.note_error(err, where="_async_update_data")
            message = self._short_error_message(err)
            self._set_api_status(ApiStatus.ERROR, reason=message)
            _LOGGER.exception("Unexpected error during coordinator update")
            raise UpdateFailed(
                f"{type(err).__name__}: {err}",
                translation_domain=DOMAIN,
                translation_key="update_unexpected_error",
                translation_placeholders={"error_type": type(err).__name__},
            ) from err

    def _request_poll_reauth(self, reauth_exc: ConfigEntryAuthFailed) -> None:
        """Start the config-entry reauth flow from the background poll cycle.

        ``_async_start_poll_cycle`` runs as a background task (not
        awaited by the refresh; the task is held in ``_poll_cycle_task`` only so
        it can be cancelled on unload and on Home Assistant stop), so a raised
        ``ConfigEntryAuthFailed`` never reaches the awaited
        coordinator refresh and Home Assistant's automatic reauth
        (``_async_refresh`` -> ``ConfigEntry.async_start_reauth``) is never
        triggered. Start the entry-scoped reauth flow directly instead, mirroring
        the manual-locate (``locate.py``) and FCM-receiver paths. The caller still
        records ``reauth_exc`` as ``last_exception`` so the ``finally`` block marks
        the update failed via ``async_set_update_error``.
        """
        # FIX 3: single recording choke point for the fire-and-forget poll path.
        # All six poll-cycle reauth sites (including the 2074 pre-converted
        # ConfigEntryAuthFailed catch) funnel through here, so record exactly
        # once. The reason code rides on the exception attribute when present.
        self.record_reauth_reason(
            getattr(reauth_exc, "reauth_code", ReauthReasonCode.UNKNOWN),
            origin="polling.py:_request_poll_reauth",
        )
        entry = getattr(self, "config_entry", None)
        if entry is None:
            _LOGGER.debug("Cannot start reauth from poll cycle: no config entry bound.")
            return
        try:
            # ``ConfigEntry.async_start_reauth`` is a synchronous @callback that
            # returns None and schedules the flow itself; awaiting it would raise
            # TypeError. Call it without await (mirrors locate.py and
            # fcm_receiver_ha.py).
            entry.async_start_reauth(self.hass)
            _LOGGER.warning("Poll cycle triggered re-authentication: %s", reauth_exc)
        except Exception as reauth_err:  # pragma: no cover - defensive
            _LOGGER.debug("Failed to start reauth flow from poll cycle: %s", reauth_err)

    # ---------------------------- Polling Cycle -----------------------------
    async def _async_start_poll_cycle(
        self,
        devices: list[dict[str, Any]],
        *,
        force: bool = False,
    ) -> None:
        """Run a full sequential polling cycle in a background task.

        This runs with a lock to avoid overlapping cycles, updates the
        internal cache, and pushes snapshots at start and end.

        Throttling awareness:
        - If a device returns a crowdsourced location with `_report_hint` equal to
          "in_all_areas" (~10 min throttle) or "high_traffic" (~5 min throttle),
          we apply a per-device cooldown so subsequent polls avoid the throttled window.
          (See POPETS'25 for measured behaviour.)
        - The cooldown is at least the server minimum and at least one user poll interval.

        Args:
            devices: A list of device dictionaries to poll.
        """
        if not devices:
            return

        async with self._poll_lock:
            if self._is_polling:
                return

            # Double-check FCM readiness inside the lock to avoid a narrow race:
            # if readiness regressed between scheduling and execution, skip cleanly
            # unless we are explicitly forcing the cycle after a prolonged outage.
            if not self._is_fcm_ready_soft():
                if not force:
                    # No baseline jump; schedule a short retry and keep escalation ticking.
                    self._note_fcm_deferral(time.monotonic())
                    self._schedule_short_retry(5.0)
                    return
                if not self._degraded_mode_warned:
                    _LOGGER.warning(
                        "Starting poll cycle without push transport; continuing in degraded mode."
                    )
                    self._degraded_mode_warned = True
                else:
                    _LOGGER.debug(
                        "Poll cycle still without push transport (degraded mode)."
                    )
            elif self._fcm_defer_started_mono:
                # If we were deferring previously, clear the escalation timeline.
                self._clear_fcm_deferral()

            self._is_polling = True
            self.safe_update_metric("last_poll_start_mono", time.monotonic())
            _LOGGER.debug("Starting sequential poll of %d devices", len(devices))

            google_home_filter = self._get_google_home_filter()

            last_exception: Exception | None = None
            _any_device_got_data = False
            try:
                cycle_failed = False
                # First account-wide decryption error seen this cycle (None == the
                # cycle was decrypt-clean). The account-wide failure counter is a
                # per-cycle quantity: it is advanced exactly once after the device
                # loop, never per device. In a multi-device account a single healthy
                # tracker must not mask a persistently un-decryptable shared key on
                # the others, so both the escalation and the reset gate are
                # cycle-scoped. Consequence (deliberate): in the escalation cycle the
                # whole device loop runs to completion before the single escalation
                # fires, so the remaining trackers still get one (failing) Nova call.
                # That is the rare end-state cost of per-cycle counting.
                cycle_decrypt_error: DecryptionError | None = None
                # True if any device hit a per-tracker StaleOwnerKeyError this
                # cycle. Such cycles must NOT be reported as crypto OK even when
                # the account-wide key is healthy, so the diagnostic sensor does
                # not flicker away the tracker_key_outdated state.
                cycle_had_stale_key = False
                # True once a device returns usable location data without a
                # DecryptionError: positive proof the shared key works. Crypto OK
                # requires this proof, not merely the absence of a decrypt error
                # (a cycle of pure timeouts must not flicker a stale key to OK).
                cycle_had_successful_decrypt = False
                # Devices this cycle whose location request the server refused
                # with a non-credential 4xx. A single rejection stays a
                # per-device skip; only a cycle in which EVERY device was
                # refused is account-wide on the rejections' own terms.
                # Deliberately NOT the same shape as the decrypt verdict above:
                # that one gates on `cycle_had_successful_decrypt`, a positive
                # proof. The request path has no such proof to gate on. It used
                # to have none at all, because `api.async_get_device_location`
                # returned the same empty dict for a healthy idle BLE tag as for
                # a 5xx, a 429, a protobuf decode failure or a Nova logic error.
                # PLAN_GFMY_EMPTY_RESULT_DISTINGUISHABLE narrowed that, and
                # measured, it narrowed LESS than the old list suggests: what now
                # arrives as `LocationRequestNotAcceptedError` on the branch below
                # is what `async_nova_request` raises -- the 5xx and the 429 on
                # their own rungs, anything else through the generic guard. The
                # protobuf decode failure does NOT: on this path it is raised
                # inside the FCM callback, i.e. AFTER the accept line, and the
                # callback catches it itself and hands back an empty result. The
                # Nova logic error is not raised anywhere in the tree at all,
                # so it never told us anything either way. Measured with a
                # pattern this comment does not itself match, because the naive
                # grep for the raise statement now finds this very line:
                # `grep -rn 'raise NovaLogic''Error' custom_components/`.
                # So an empty result is narrower evidence than it was, and still
                # not proof that anything worked. It remains the outcome of the
                # idle tag, of every failure the FCM callback absorbs, and of the
                # post-accept branches of the locate flow -- not a closed list,
                # which is the point: this counter must not be read as evidence.
                cycle_rejected_devices = 0
                # Counted separately from `cycle_rejected_devices` rather than
                # folded into it, because the two say different things and only
                # their SUM answers the cycle-level question.
                #
                # The difference is NOT flow position, and saying so would be
                # measurably wrong: `location_request.py` re-raises the
                # non-credential 4xx from the same `try` that produces the 5xx
                # and the 429, all of them BEFORE the `Location request accepted`
                # line. Neither kind reached the accept point.
                #
                # The difference is what the outcome says. A REJECTION is the
                # server's answer ABOUT THIS DEVICE -- "not this one", typically
                # a tracker deleted from the account. A NON-ACCEPTED request is
                # the absence of such an answer: the request did not leave this
                # integration as accepted, usually for a reason on the way to the
                # server (a 5xx, a 429, a network error, a failed FCM
                # registration).
                #
                # Resist sharpening that into "permanent versus transient". It
                # holds for the common cases and breaks on `no_fcm_token`, whose
                # source returns None for a canonic id that is not present in the
                # receiver's device set -- device-specific and not transient at
                # all. Both counts mean only "this device contributed no
                # evidence", which is what the post-loop guard needs; they are
                # kept apart so whoever reads the log can tell a configuration
                # change from an outage.
                cycle_unaccepted_devices = 0
                # Trackers skipped at their turn because their automatic
                # switch went off after the cycle was planned (#242). They were
                # never asked, so the post-loop guards judge the cycle on the
                # trackers that were: ``len(devices)`` minus these.
                cycle_skipped_devices = 0
                # True once ANY device in this cycle booked a transient auth
                # failure. A cycle that books none zeroes
                # `_consecutive_transient_auth_failures` after the loop: that is
                # the counter's everyday reset source, and it runs on the same
                # clock that increments it.
                cycle_booked_transient_auth = False
                # How many devices got a request this cycle that was not
                # refused: a location with content, or an empty result, which is
                # the FCM wait returning without raising.
                #
                # The second case is WEAK and knowingly so. Four pre-accept
                # failures still arrive as an empty dict (an unregistered FCM
                # provider, a provider returning None, a missing token cache, a
                # late ImportError), so an empty result does not prove Nova took
                # the request -- it proves only that nothing raised. That is
                # enough to break a streak of rejections and deliberately not
                # enough to clear the auth state, which is why the two verdicts
                # sit at different sites.
                #
                # What is NOT counted here: a timeout, a 5xx, a non-credential
                # rejection, a stale owner key, a decryption failure and a
                # LocationRequestNotAcceptedError. Each of those says nothing in
                # either direction, and a cycle made of nothing but those must
                # not clear a rejection budget on the strength of an outage.
                # The price of that choice is stated at the end of the loop.
                cycle_accepted_requests = 0
                # How many devices returned a location WITH content: the one
                # strong proof on this path. Kept as a cycle-wide tally because
                # the reauth verdict is a cycle-wide verdict, and a proof held by
                # any device outranks a non-permanent rejection by another.
                cycle_content_proofs = 0
                # First device that booked a rejection this cycle, with its
                # error. The end-of-cycle verdict names them, so the message a
                # user sees still points at a concrete tracker.
                first_rejecting_device: str | None = None
                first_rejection_error: Exception | None = None
                # Second slot, for a failure that had to queue behind a rejection.
                # See the timeout branch below for why a rejection cannot simply
                # be overwritten.
                independent_error: Exception | None = None
                for idx, dev in enumerate(devices):
                    dev_id = dev["id"]
                    dev_name = dev.get("name", dev_id)
                    # The switch is read live: a tracker switched off after
                    # the cycle was planned is skipped at its turn (#242).
                    if not self.is_automatic_polling_enabled(dev_id):
                        _LOGGER.debug(
                            "Sequential poll: skipping %s (%d/%d), automatic "
                            "location requests are switched off",
                            dev_name,
                            idx + 1,
                            len(devices),
                        )
                        cycle_skipped_devices += 1
                        continue
                    _LOGGER.debug(
                        "Sequential poll: requesting location for %s (%d/%d)",
                        dev_name,
                        idx + 1,
                        len(devices),
                    )

                    try:
                        # Protect API awaitable with an outer guard that is
                        # strictly larger than the inner FCM wait
                        # (LOCATION_REQUEST_TIMEOUT_S, see
                        # NovaApi/.../location_request.py). Equal nested budgets
                        # make this outer guard win the race and raise a spurious
                        # TimeoutError before the inner request can return its
                        # clean empty result (Nygard: stagger nested timeouts).
                        # The transport stamps the device immediately before it
                        # POSTs the request (helpers/locate_clock.py): an attempt
                        # cut off or failing in its push or token phase leaves
                        # no stamp. The observer is a context variable, and
                        # ``wait_for`` awaits the call in this task (Python 3.12
                        # and later), so it reaches the request. Created lazily
                        # for coordinators built without __init__.
                        self._locate_requested_mono = ensure_locate_stamps(
                            getattr(self, "_locate_requested_mono", None)
                        )
                        # The same POST counts the request as automatic (#242).
                        self._locate_request_counts = ensure_locate_counts(
                            getattr(self, "_locate_request_counts", None)
                        )
                        with stamp_on_dispatch(
                            self._locate_requested_mono,
                            dev_id,
                            counts=self._locate_request_counts,
                            kind="auto",
                        ):
                            location = await asyncio.wait_for(
                                self.api.async_get_device_location(dev_id, dev_name),
                                timeout=POLL_DEVICE_OUTER_TIMEOUT_S,
                            )

                        if not location:
                            # Expected for BLE tags with no reporter nearby: the
                            # inner FCM wait returns an empty result rather than
                            # raising. This is a healthy idle outcome, not a fault:
                            # DEBUG per poll, INFO once for a device that stays
                            # silent (see _note_empty_location_result).
                            self._note_empty_location_result(dev_id, dev_name)
                            self._log_idle_poll_diagnostics(
                                dev_id, dev_name, source="empty-result"
                            )
                            # Weak evidence, and deliberately only counted as
                            # such: the request was accepted, which is not the
                            # same as the credentials being proven. It breaks a
                            # streak of rejections; it does not clear the auth
                            # state, and it does not zero the counter here.
                            cycle_accepted_requests += 1
                            continue

                        self._note_location_content(dev_id, dev_name)

                        # A location WITH content, and only that, proves on this
                        # path that the credentials worked -- the same rule the
                        # sound handlers state at their own sites ("only an
                        # ACCEPTED submission proves the credentials worked").
                        # An empty return proves only that nothing raised.
                        #
                        # The guard above therefore runs FIRST. It used not to,
                        # and that was the defect: an empty dict is WEAK evidence
                        # that the request was accepted, not proof of it. The 5xx,
                        # the 429, the network error and the failed FCM
                        # registration raise LocationRequestNotAcceptedError today
                        # instead of flattening into {}, but four pre-accept
                        # failures still arrive here as an empty dict (an
                        # unregistered FCM receiver provider, a provider returning
                        # None, a missing token cache, and a failure binding the
                        # lazily imported decrypt / eid-info modules), because they
                        # are raised before the handler that would convert them.
                        #
                        # The cost of getting this wrong is not cosmetic. The
                        # counter exists to escalate a genuinely expired login
                        # after _MAX_TRANSIENT_AUTH_FAILURES cycles; a fleet with
                        # one idle BLE tag cleared it on every single pass, so the
                        # threshold was never reached and the re-auth prompt this
                        # mechanism was built for never appeared.
                        self._set_auth_state(failed=False)
                        cycle_content_proofs += 1
                        # Behaviour-neutral today and kept as an invariant, not
                        # as a live effect: a mutation that drops this line kills
                        # no test, because the reset two lines down already zeroes
                        # the counter at this very site. It is here so the tally
                        # means what its name says -- every accepted request of
                        # this cycle -- for the day that site-local reset moves or
                        # goes away.
                        cycle_accepted_requests += 1
                        if self._consecutive_transient_auth_failures > 0:
                            _LOGGER.info(
                                "Location request succeeded; clearing %d transient auth failure(s).",
                                self._consecutive_transient_auth_failures,
                            )
                            self._consecutive_transient_auth_failures = 0
                            self._last_transient_auth_error = None

                        # A device returned an authenticated coordinate report
                        # without raising a DecryptionError: positive proof the
                        # account-wide shared key still decrypts. Gate the crypto OK
                        # state on this. Truthy-but-unauthenticated rows -- a
                        # metadata_only sentinel (secrets-bundle key material) or a
                        # SEMANTIC row (no decrypted coordinates) -- prove nothing, so
                        # one report-less device must not mask another device's stale
                        # key for the whole cycle.
                        #
                        # The proof is a property of the WHOLE response, but
                        # api.async_get_device_location collapses it to a single
                        # display record (newest last_seen) that can be a report-less
                        # SEMANTIC row even when a sibling coordinate report decrypted.
                        # It therefore carries the full-list verdict in the internal
                        # _decrypt_proven hint; pop it here (leak-safe, like
                        # _report_hint) and prefer it. Fall back to the collapsed
                        # record only when the hint is absent (legacy/mocked stubs).
                        decrypt_proven = location.pop("_decrypt_proven", None)
                        if decrypt_proven is None:
                            decrypt_proven = is_real_location_record(location)
                        if decrypt_proven:
                            cycle_had_successful_decrypt = True

                        self._record_semantic_label(location, device_id=dev_id)
                        raw_semantic_name = (
                            location.get("semantic_name")
                            if isinstance(location.get("semantic_name"), str)
                            else None
                        )
                        semantic_replaced = False

                        cached_loc = self._device_location_data.get(dev_id)
                        is_replay = False
                        if isinstance(cached_loc, Mapping):
                            new_ts = _normalize_epoch_seconds(location.get("last_seen"))
                            old_ts = _normalize_epoch_seconds(
                                cached_loc.get("last_seen")
                            )
                            if (
                                new_ts is not None
                                and old_ts is not None
                                and new_ts == old_ts
                            ):
                                is_replay = True

                        location["is_replayed"] = is_replay

                        # Tally the REPORTED accuracy class here (#216), which is
                        # before the fusion AND before anything can substitute the
                        # value. Before the fusion because the gate may reject and
                        # this loop then skips to the next device, so a counter
                        # after it would miss exactly the coarse fixes the
                        # distribution is for. Before the substitutions because
                        # ``_apply_semantic_mapping`` writes an anchor radius and
                        # the two preserve-sites below reuse the CACHED accuracy
                        # via ``carry_reused_accuracy``: counted after either, a
                        # semantic-only response would enter the distribution as a
                        # freshly reported measurement and pull it towards whatever
                        # happened to be cached. This distribution answers "what do
                        # incoming fixes report", so a response that reports no
                        # accuracy of its own must not appear in it at all.
                        #
                        # A REPLAY is not an incoming fix. The poll returned a
                        # report timestamp we already hold, so counting it would
                        # let the distribution follow the poll interval instead
                        # of the reports - a device that reports once an hour and
                        # is polled every five minutes would enter its class
                        # twelve times. The marker is set either way, so
                        # ``update_device_cache`` does not tally it later: the
                        # decision "not counted" belongs here.
                        #
                        # ``is_replayed_report`` rather than the ``is_replay``
                        # flag above: that flag compares against the PUBLISHED
                        # row only, and a fix the accuracy gate rejected leaves
                        # that row untouched by design. Its report is stored
                        # aside, so every later poll of it would look new - for
                        # exactly the coarse fixes this distribution is for.
                        if self.claim_report_for_tally(dev_id, location):
                            self.count_accuracy_class(location)
                        location["_accuracy_counted"] = True

                        mapping_applied = self._apply_semantic_mapping(location)

                        # --- Apply Google Home filter (keep parity with FCM push path) ---
                        # Consume coordinate substitution from the filter when needed.
                        semantic_name = location.get("semantic_name")
                        if (
                            not mapping_applied
                            and not is_replay
                            and semantic_name
                            and google_home_filter is not None
                        ):
                            try:
                                (
                                    should_filter,
                                    replacement_attrs,
                                ) = google_home_filter.should_filter_detection(
                                    dev_id, semantic_name
                                )
                            except Exception as gf_err:
                                _LOGGER.debug(
                                    "Google Home filter error for %s: %s",
                                    dev_name,
                                    gf_err,
                                )
                            else:
                                if should_filter:
                                    _LOGGER.warning(
                                        "Filtering out Google Home spam detection for %s",
                                        dev_id,
                                    )
                                    _LOGGER.debug(
                                        "Device %s is named %s", dev_id, dev_name
                                    )
                                    continue
                                if replacement_attrs:
                                    prev_location = self._device_location_data.get(
                                        dev_id
                                    )
                                    keep_previous_precise = (
                                        self._should_preserve_precise_home_coordinates(
                                            prev_location, replacement_attrs
                                        )
                                    )

                                    location = dict(location)
                                    if (
                                        keep_previous_precise
                                        and prev_location is not None
                                    ):
                                        _LOGGER.debug(
                                            "Google Home filter: %s detected at '%s', preserving previous precise coordinates",
                                            dev_name,
                                            semantic_name,
                                        )
                                        location["latitude"] = prev_location["latitude"]
                                        location["longitude"] = prev_location[
                                            "longitude"
                                        ]
                                        # Carry the producer flag with the reused
                                        # accuracy so a preserved estimated fallback
                                        # is not reclassified as a real measurement.
                                        carry_reused_accuracy(location, prev_location)
                                    else:
                                        # Unattended poll path: this record names
                                        # the device by its id; the display name
                                        # and the Google Home label go to the DEBUG
                                        # record below (AGENTS.md section 5 (b)).
                                        _LOGGER.info(
                                            "Google Home filter: device %s detected at a Google Home location, substituting with Home coordinates",
                                            dev_id,
                                        )
                                        _LOGGER.debug(
                                            "Google Home filter: %s (device %s) detected at '%s', substituting with Home coordinates",
                                            dev_name,
                                            dev_id,
                                            semantic_name,
                                        )
                                        if (
                                            "latitude" in replacement_attrs
                                            and "longitude" in replacement_attrs
                                        ):
                                            location["latitude"] = (
                                                replacement_attrs.get("latitude")
                                            )
                                            location["longitude"] = (
                                                replacement_attrs.get("longitude")
                                            )
                                        if (
                                            "radius" in replacement_attrs
                                            and replacement_attrs.get("radius")
                                            is not None
                                        ):
                                            substitute_zone_accuracy(
                                                location,
                                                replacement_attrs["radius"],
                                            )
                                    # Clear semantic name so HA Core's zone engine determines the final state.
                                    location["semantic_name"] = None
                                    semantic_replaced = True
                        location.pop("is_replayed", None)
                        # ------------------------------------------------------------------

                        if raw_semantic_name and not semantic_replaced:
                            location["semantic_name"] = raw_semantic_name

                        # If we only got a semantic location, preserve previous coordinates.
                        # Use Phase 11 helper for the check
                        from .helpers.polling import (
                            calculate_location_age_hours,
                            get_age_log_level,
                            should_preserve_previous_coordinates,
                        )

                        if should_preserve_previous_coordinates(location):
                            prev = self._device_location_data.get(dev_id, {})
                            if prev:
                                location["latitude"] = prev.get("latitude")
                                location["longitude"] = prev.get("longitude")
                                # Carry the producer flag with the reused accuracy
                                # so a preserved estimated fallback keeps its dashed
                                # circle downstream instead of being treated as real.
                                carry_reused_accuracy(location, prev)
                                location["status"] = (
                                    "Semantic location; preserving previous coordinates"
                                )

                        # Validate/normalize coordinates (and accuracy if present).
                        clear_metadata_only = (
                            location.get("latitude") is not None
                            or location.get("longitude") is not None
                        ) and location.get("metadata_only") is not True
                        self._persist_anchor_metadata(
                            dev_id, location, clear_metadata_only=clear_metadata_only
                        )
                        if not self._normalize_coords(location, device_label=dev_name):
                            if not location.get("semantic_name"):
                                _LOGGER.warning(
                                    "No location data (coordinates or semantic name) available for %s in this update.",
                                    dev_id,
                                )
                                _LOGGER.debug("Device %s is named %s", dev_id, dev_name)
                            # Nothing to commit/update in cache
                            # Strip any internal hint before dropping to avoid accidental exposure
                            location.pop("_report_hint", None)
                            continue

                        # Sanitize invariants + enrich fields (label, utc-string)
                        location = _sanitize_decoder_row(location)

                        if not self._apply_weighted_location_fusion(dev_id, location):
                            continue

                        # Skip redundant fusion inside update_device_cache when reusing this payload.
                        location["_fusion_preapplied"] = True

                        # Age diagnostics - use Phase 11 helpers. Both age
                        # categories log at DEBUG: a stale location repeats on
                        # every poll and the record carries the device name, so
                        # it may not appear at INFO (AGENTS.md section 5 (b)).
                        wall_now = time.time()
                        last_seen = location.get("last_seen", 0)
                        age_hours = calculate_location_age_hours(last_seen, wall_now)
                        if age_hours is not None:
                            log_level, should_log = get_age_log_level(age_hours)
                            if should_log and log_level == "info":
                                _LOGGER.debug(
                                    "Using old location data for %s (age=%.1fh)",
                                    dev_name,
                                    age_hours,
                                )
                            elif should_log and log_level == "debug":
                                _LOGGER.debug(
                                    "Using location data for %s (age=%.1fh)",
                                    dev_name,
                                    age_hours,
                                )

                        # Commit via the shared cache helper when available;
                        # test doubles may stub this method, so fall back to a
                        # minimal cache write in that case.
                        from .main import GoogleFindMyCoordinator as _CoordinatorClass

                        helper = getattr(self.update_device_cache, "__func__", None)
                        if helper is _CoordinatorClass.update_device_cache:
                            self.update_device_cache(dev_id, location, source="poll")
                        else:
                            # Strip EVERY transient marker, not a list of the
                            # ones that exist today: F-1 (supersede), Fund A
                            # (the deferred anchor seed/consume) and the two
                            # accuracy markers all arrived one at a time, and
                            # each time the list here was correct until the next
                            # one was added. Semantic divergence vs. the real
                            # update_device_cache path stays as documented:
                            # production pops AND applies the anchor intents
                            # post-commit, this test-double fallback only strips
                            # them and intentionally performs no anchor mutation.
                            strip_transient_keys(location)
                            location.setdefault("last_updated", wall_now)
                            merged_location = self._merge_with_existing_cache_row(
                                dev_id, location
                            )
                            self._device_location_data[dev_id] = merged_location
                            # Third commit site, same rule: a newer position
                            # makes a retained coarse fix obsolete.
                            self._expire_coarse_fix(dev_id, merged_location)

                        self.increment_stat("polled_updates")
                        self._consecutive_timeouts = 0
                        _any_device_got_data = True

                        # Immediate per-device update for more responsive UI during long poll cycles.
                        self.push_updated([dev_id])

                    except TimeoutError as terr:
                        if self.is_fcm_connected:
                            # The outer guard repeats unattended, so the INFO
                            # record names the device by its id; the idle
                            # diagnostics below carry the name at DEBUG
                            # (AGENTS.md section 5 (b)).
                            _LOGGER.info(
                                "Poll timed out for device %s (FCM connected); ignoring error to keep status healthy",
                                dev_id,
                            )
                            self._log_idle_poll_diagnostics(
                                dev_id, dev_name, source="outer-timeout"
                            )
                            self.increment_stat("timeouts")
                        else:
                            _LOGGER.info(
                                "Location request timed out for device %s after %s seconds",
                                dev_id,
                                POLL_DEVICE_OUTER_TIMEOUT_S,
                            )
                            _LOGGER.debug(
                                "Location request timeout for %s (device %s)",
                                dev_name,
                                dev_id,
                            )
                            self.increment_stat("timeouts")
                            self._consecutive_timeouts += 1
                            cycle_failed = True
                            self.note_error(terr, where="poll_timeout", device=dev_name)
                            # `last_exception` is first-come, but a credential
                            # rejection holds it only PROVISIONALLY: the
                            # mixed-cycle branch after the loop withdraws that
                            # report once a sibling answers WITH CONTENT. Dropping
                            # this report because the slot is taken would then
                            # leave NOTHING: the coordinator update reports
                            # success and every tracker stays available, although
                            # `last_poll_result` says failed and this device never
                            # answered at all.
                            #
                            # Overwriting the rejection instead would be wrong the
                            # other way round, because whether the withdrawal
                            # happens is only known AFTER the loop
                            # (`cycle_content_proofs`). In a cycle without content
                            # the rejection is the more specific diagnosis and the
                            # cause of the running escalation, and it must stay.
                            # So it is kept in a SECOND slot and handed over only
                            # if the withdrawal really fires. Identity, not
                            # truthiness: only the rejection THIS cycle booked
                            # yields its place. The stale-owner-key and generic
                            # branches below carry the same rule; the rejection's
                            # own slot deliberately does not, because a rejection
                            # must not displace anything.
                            if last_exception is None:
                                last_exception = UpdateFailed(
                                    f"Location request timed out for device {dev_id}",
                                    translation_domain=DOMAIN,
                                    translation_key="locate_timeout",
                                    translation_placeholders={"device_id": str(dev_id)},
                                )
                                last_exception.__cause__ = terr
                            # INVARIANT, not a live condition: mutating
                            # `last_exception is first_rejection_error` to
                            # `first_rejection_error is not None` survives every
                            # test, and measurably so -- `independent_error` is
                            # only ever handed over by the withdrawal, and the
                            # withdrawal itself demands that identity. The check
                            # is kept because it states at the point of writing
                            # what the second slot is FOR, and a later change to
                            # the handover would otherwise silently widen it.
                            elif (
                                last_exception is first_rejection_error
                                and independent_error is None
                            ):
                                independent_error = UpdateFailed(
                                    f"Location request timed out for device {dev_id}",
                                    translation_domain=DOMAIN,
                                    translation_key="locate_timeout",
                                    translation_placeholders={"device_id": str(dev_id)},
                                )
                                independent_error.__cause__ = terr
                    except SpotAuthPermanentError:
                        _LOGGER.warning(
                            "Authentication failed for %s; triggering reauth flow.",
                            dev_id,
                        )
                        _LOGGER.debug("Device %s is named %s", dev_id, dev_name)
                        self._set_auth_state(
                            failed=True,
                            reason=f"Auth failed during poll for {dev_name}: session invalid",
                        )
                        cycle_failed = True
                        self._last_poll_result = "failed"
                        self._consecutive_timeouts = 0
                        reauth_exc = ConfigEntryAuthFailed(
                            "Google session invalid; re-authentication required"
                        )
                        reauth_exc.reauth_code = ReauthReasonCode.SPOT_AUTH_PERMANENT
                        last_exception = reauth_exc
                        self._request_poll_reauth(reauth_exc)
                        return
                    except SpotApiEmptyResponseError:
                        _LOGGER.warning(
                            "Authentication failed for %s; triggering reauth flow.",
                            dev_id,
                        )
                        _LOGGER.debug("Device %s is named %s", dev_id, dev_name)
                        self._set_auth_state(
                            failed=True,
                            reason=f"Auth failed during poll for {dev_name}: session invalid",
                        )
                        cycle_failed = True
                        self._last_poll_result = "failed"
                        self._consecutive_timeouts = 0
                        reauth_exc = ConfigEntryAuthFailed(
                            "Google session invalid; re-authentication required"
                        )
                        reauth_exc.reauth_code = (
                            ReauthReasonCode.OWNER_KEY_EMPTY_RESPONSE
                        )
                        last_exception = reauth_exc
                        self._request_poll_reauth(reauth_exc)
                        return
                    except NovaAuthPermanentError as perm_err:
                        # Permanent auth failure (AAS token invalid) - immediate reauth
                        _LOGGER.error(
                            "Permanent authentication failure for %s: %s. Re-authentication required.",
                            dev_id,
                            perm_err,
                        )
                        _LOGGER.debug("Device %s is named %s", dev_id, dev_name)
                        self._set_auth_state(
                            failed=True,
                            reason=f"Permanent auth failure for {dev_name}: credentials invalid",
                        )
                        cycle_failed = True
                        self._last_poll_result = "failed"
                        self._consecutive_timeouts = 0
                        self._consecutive_transient_auth_failures = 0
                        reauth_exc = ConfigEntryAuthFailed(
                            "Google credentials invalid; re-authentication required"
                        )
                        reauth_exc.reauth_code = ReauthReasonCode.NOVA_AUTH_PERMANENT
                        last_exception = reauth_exc
                        self._request_poll_reauth(reauth_exc)
                        return
                    except NovaAuthError as transient_err:
                        # Branch on the STATUS, never on the type. The transport
                        # raises this class for every non-retryable 4xx, so a
                        # device removed from the account fed the transient-auth
                        # counter and produced a re-auth prompt after exactly
                        # three cycles, with the sign-in intact the whole time.
                        # api.async_get_device_location passes such a status
                        # through rather than collapsing it to {}, precisely so
                        # this branch is the one that runs. A non-raising return
                        # would take the success path above, which used to clear
                        # the auth state and reset the counter before it looked
                        # at whether the result is empty; since that reset moved
                        # behind the empty guard it would clear nothing, which
                        # makes this branch the narrower of two guarantees rather
                        # than the only one.
                        if not is_credential_rejection(transient_err):
                            _LOGGER.warning(
                                "Client error (HTTP %s) for %s: %s. "
                                "Skipping this device; the sign-in is not in question.",
                                getattr(transient_err, "status", "unknown"),
                                dev_id,
                                transient_err,
                            )
                            _LOGGER.debug("Device %s is named %s", dev_id, dev_name)
                            self.note_error(
                                transient_err,
                                where="poll_client_error",
                                device=dev_name,
                            )
                            # Recorded, but deliberately NOT account-wide.
                            # ``cycle_failed`` and ``last_exception`` drive two
                            # different things in the ``finally`` block:
                            # ``cycle_failed`` only writes the
                            # ``last_poll_result`` diagnostic attribute, while
                            # ``last_exception`` drives
                            # ``async_set_update_error`` -- and
                            # ``GoogleFindMyEntity.available`` follows the
                            # coordinator's ``last_update_success``. Setting it
                            # here made ONE rejected tracker mark EVERY tracker
                            # entity unavailable, and unlike a 5xx a device
                            # deleted from the account never recovers, so the
                            # outage repeated on every poll for as long as the
                            # tracker stayed in the cached list. Trading a
                            # spurious re-auth prompt for a permanent
                            # availability outage is not a fix. So: keep
                            # ``cycle_failed`` (the cycle really did not poll
                            # every device) and leave ``last_exception`` to the
                            # failures that are about the account. This is the
                            # only branch here that sets one without the other,
                            # deliberately: every other one reports a condition
                            # that says something about the account, this one
                            # does not.
                            cycle_failed = True
                            cycle_rejected_devices += 1
                            # Deliberately NOT touched HERE: the transient-auth
                            # counter is neither raised nor reset. A client
                            # rejection says nothing about the credentials in
                            # either direction.
                            # The neighbouring success path no longer treats a
                            # non-raising return as proof that the credentials
                            # work: it resets this counter and clears the auth
                            # state only for a location WITH content, behind the
                            # empty guard. An idle BLE tag therefore clears
                            # nothing, and neither does a rejection that gets
                            # here -- the raise in api.py keeps it off that path
                            # entirely, which is one guarantee on top of the
                            # other, not the only one left.
                            # The all-rejected case is caught after the loop,
                            # not here: whether a rejection is per-device or
                            # account-wide is only knowable once every device
                            # has been tried. Same reasoning as
                            # _finalize_cycle_decrypt_state, which defers the
                            # decrypt verdict for exactly that reason.
                            continue

                        # Transient auth failure - may self-heal in subsequent poll cycles.
                        # Only trigger reauth after multiple consecutive failures.
                        #
                        # NOTHING IS DECIDED HERE. The device records that it was
                        # rejected; the cycle raises the streak and judges the
                        # threshold once, after every sibling has been asked. Two
                        # measured reasons, both from review:
                        #
                        # * Incrementing per device made two broken trackers worth
                        #   two cycles, so the threshold of three was crossed in
                        #   the middle of the second cycle and the number in the
                        #   user-facing message was not a cycle count.
                        # * Judging here returned from the cycle at once, so a
                        #   sibling that would have answered WITH CONTENT was
                        #   never asked. The same responses then produced opposite
                        #   verdicts depending on the order the devices happened
                        #   to sit in.
                        #
                        # The cause is recorded for every rejecting device, because
                        # the newest one is the useful one to show.
                        # Guarded on the ERROR, not on the name: `dev_name` comes
                        # from `dev.get("name", dev_id)` and is `None` for a device
                        # dict that carries an explicit `"name": None`. Guarding on
                        # the name would let a second rejection overwrite
                        # `first_rejection_error` while `last_exception` still holds
                        # the first, and both the second slot above and the
                        # withdrawal below compare by identity.
                        if first_rejection_error is None:
                            first_rejecting_device = dev_name
                            first_rejection_error = transient_err
                        self._last_transient_auth_error = str(transient_err)
                        # Marks the cycle, not the device: the post-loop reset
                        # only fires when NO device rejected, so an idle sibling
                        # cannot clear what this device just booked.
                        cycle_booked_transient_auth = True

                        # No count in this message on purpose: the streak is
                        # raised after the loop, so any number printed here would
                        # be the previous cycle's.
                        _LOGGER.warning(
                            "Transient auth failure for %s: %s. "
                            "Will retry in next poll cycle.",
                            dev_id,
                            transient_err,
                        )
                        _LOGGER.debug("Device %s is named %s", dev_id, dev_name)
                        cycle_failed = True
                        if last_exception is None:
                            last_exception = transient_err
                        continue  # Try next device instead of aborting entire cycle
                    except ConfigEntryAuthFailed as auth_exc:
                        # A pre-converted ConfigEntryAuthFailed (e.g. raised directly
                        # by api.async_get_device_location on HTTP 401/403 or a
                        # permanent Nova auth failure) reaches here without going
                        # through the typed SpotAuth/NovaAuth handlers above. Mark
                        # auth failed and abort remaining devices. Start reauth
                        # directly instead of re-raising: this poll cycle runs as a
                        # fire-and-forget task, so a re-raise would never reach the
                        # awaited coordinator refresh and HA's reauth would not fire.
                        self._set_auth_state(
                            failed=True,
                            reason=f"Auth failed during poll for {dev_name}: {auth_exc}",
                        )
                        cycle_failed = True
                        self._last_poll_result = "failed"
                        self._consecutive_timeouts = 0
                        last_exception = auth_exc
                        self._request_poll_reauth(auth_exc)
                        return
                    except StaleOwnerKeyError as stale_err:
                        # Per-tracker owner-key version mismatch (D2): an account-wide
                        # reauth would not help -- only this tracker is outdated. Log
                        # and keep polling other devices; do NOT touch the
                        # account-wide decrypt counter (no reauth storm).
                        self.note_decrypt_failure(
                            stale=True, error=stale_err, device=dev_name
                        )
                        cycle_failed = True
                        cycle_had_stale_key = True
                        self._consecutive_timeouts = 0
                        # Queues behind a provisional rejection -- see the
                        # timeout branch above for the second slot.
                        if last_exception is None:
                            last_exception = stale_err
                        elif (
                            last_exception is first_rejection_error
                            and independent_error is None
                        ):
                            independent_error = stale_err
                        continue
                    except OwnerKeyLookupTransientError as owner_transient_err:
                        # Transient owner-key lookup miss (partial server response,
                        # network/gRPC failure). NOT a credential defect and NOT a
                        # DecryptionError, so it never reaches the account-wide
                        # reauth verdict (_finalize_cycle_decrypt_state). Treat it as
                        # an ordinary per-device skip: keep polling the other
                        # devices, do NOT advance the decrypt-failure counter and do
                        # NOT set cycle_decrypt_error. Must precede the
                        # DecryptionError handler; the explicit catch keeps a
                        # transient miss from aborting the whole cycle.
                        _LOGGER.debug(
                            "Owner-key lookup for %s skipped (transient): %s",
                            dev_name,
                            owner_transient_err,
                        )
                        self._consecutive_timeouts = 0
                        continue
                    except DecryptionError as dec_err:
                        # Defer the cycle-failure bookkeeping (cycle_failed /
                        # last_exception) to the post-loop verdict in
                        # _finalize_cycle_decrypt_state. Whether this decrypt error
                        # fails the whole coordinator update depends on the
                        # cross-device outcome: a device-local own-report failure a
                        # sibling success refutes is benign and must NOT mark the
                        # cycle failed (otherwise a single offline device fails every
                        # poll), while a genuinely account-wide failure does. Only
                        # capture the representative error here -- a non-device-local
                        # failure (shared-key subclass or plain identity-key
                        # derivation failure) is kept over a co-occurring device-local
                        # own-report failure so the resolver's sibling-success
                        # downgrade cannot mask a genuine account-wide problem. The
                        # account-wide failure counter is still advanced exactly once
                        # after the loop, never per device, preserving the documented
                        # consecutive-cycle budget that gives the in-decrypt self-heal
                        # time to recover.
                        self._consecutive_timeouts = 0
                        cycle_decrypt_error = self._prefer_account_wide_decrypt_error(
                            cycle_decrypt_error, dec_err
                        )
                        continue
                    except LocationRequestNotAcceptedError as not_accepted_err:
                        # The request never got past this integration's accept point, so
                        # this device produced no evidence in either direction --
                        # neither that the account is healthy nor that it is
                        # broken. It stays a PER-DEVICE skip: keep polling the
                        # siblings, record the miss, and leave the account-wide
                        # verdict to the post-loop guard. Recording
                        # `last_exception` here instead would make a single 5xx on
                        # a single tracker mark every entity of the account
                        # unavailable.
                        #
                        # The shape below is taken from the client-rejection
                        # branch above, deliberately and for the same reason:
                        # `cycle_failed` and `last_exception` drive two different
                        # things. `cycle_failed` only writes the
                        # `last_poll_result` diagnostic attribute, so the cycle
                        # stays honest about not having polled every device.
                        # `last_exception` drives `async_set_update_error` and
                        # with it entity availability, which is an account-wide
                        # claim this branch has no evidence for.
                        #
                        # Deliberately absent, each for its own measured reason:
                        #
                        # * NO `last_exception`. See above. The count feeds the
                        #   post-loop guard instead, which is the only place that
                        #   can see whether ANY device got through.
                        # * NO `_set_auth_state(failed=False)` and NO reset of
                        #   `_consecutive_transient_auth_failures`. Those sit on
                        #   the success path above and are skipped by construction
                        #   now that the raise sites exist. That skip IS the fix for
                        #   the counter being wiped by a 5xx on its way through;
                        #   re-doing the reset here would hand it straight back.
                        # * NO reset of `_consecutive_timeouts`. Measured before
                        #   the raise sites were armed: a failed request reached
                        #   the `if not location:` branch, which does NOT reset
                        #   that counter -- so leaving it alone here is what
                        #   preserves the behaviour, not what changes it. Resetting
                        #   it here would therefore not preserve current
                        #   behaviour but invent a health signal the request
                        #   never earned -- the same false-success reasoning this
                        #   change exists to remove. Nor is the counter a general
                        #   liveness mark to be refreshed by anything that ran:
                        #   it is incremented in exactly one place, the outer
                        #   timeout branch, and a refused request is not a
                        #   timeout. Its only consumer is the connectivity binary
                        #   sensor's attribute dict, so it feeds no verdict.
                        #   The sibling handlers here are NOT unanimous, which is
                        #   why their shape is a weak argument either way: eight
                        #   of the ten clear it unconditionally, `TimeoutError`
                        #   never does (it increments), and `NovaAuthError`
                        #   clears it on only one of its three exits.
                        _LOGGER.debug(
                            "Location request for %s was not accepted (%s); "
                            "skipping this device for this cycle",
                            dev_name,
                            not_accepted_err,
                        )
                        self._log_idle_poll_diagnostics(
                            dev_id, dev_name, source="not-accepted"
                        )
                        cycle_failed = True
                        cycle_unaccepted_devices += 1
                        continue
                    except Exception as err:
                        _LOGGER.error("Failed to get location for %s: %s", dev_id, err)
                        _LOGGER.debug("Device %s is named %s", dev_id, dev_name)
                        cycle_failed = True
                        self._consecutive_timeouts = 0
                        self.note_error(err, where="poll_exception", device=dev_name)
                        # Queues behind a provisional rejection -- see the
                        # timeout branch above for the second slot.
                        if last_exception is None:
                            last_exception = err
                        elif (
                            last_exception is first_rejection_error
                            and independent_error is None
                        ):
                            independent_error = err

                    # Inter-device delay (except after the last one)
                    if idx < len(devices) - 1 and self.device_poll_delay > 0:
                        await asyncio.sleep(self.device_poll_delay)

                cycle_polled_devices = len(devices) - cycle_skipped_devices
                _LOGGER.debug(
                    "Completed polling cycle for %d devices (%d skipped)",
                    cycle_polled_devices,
                    cycle_skipped_devices,
                )
                if cycle_booked_transient_auth and not cycle_content_proofs:
                    # The cycle rejected and held no proof to the contrary, so the
                    # streak grows by exactly one and the threshold is judged once,
                    # with every sibling already asked.
                    #
                    # `not cycle_content_proofs` is the part that makes the verdict
                    # independent of device order. A location WITH content clears
                    # the counter at its own site; if that happened anywhere in
                    # this cycle, raising the streak here would undo a proof the
                    # cycle actually holds, and whether it did would depend on
                    # which device answered first.
                    self._consecutive_transient_auth_failures += 1
                    if (
                        self._consecutive_transient_auth_failures
                        >= _MAX_TRANSIENT_AUTH_FAILURES
                    ):
                        _LOGGER.error(
                            "Transient auth failure for %s persisted across %d poll cycles: %s. "
                            "Triggering re-authentication.",
                            first_rejecting_device,
                            self._consecutive_transient_auth_failures,
                            first_rejection_error,
                        )
                        self._set_auth_state(
                            failed=True,
                            reason=f"Auth failed for {first_rejecting_device} after {self._consecutive_transient_auth_failures} attempts",
                        )
                        cycle_failed = True
                        self._last_poll_result = "failed"
                        self._consecutive_timeouts = 0
                        reauth_exc = ConfigEntryAuthFailed(
                            f"Authentication failed after {self._consecutive_transient_auth_failures} attempts; re-authentication required"
                        )
                        reauth_exc.reauth_code = (
                            ReauthReasonCode.NOVA_AUTH_TRANSIENT_EXHAUSTED
                        )
                        last_exception = reauth_exc
                        self._request_poll_reauth(reauth_exc)
                        return
                    _LOGGER.debug(
                        "Poll cycle booked a credential rejection (%d/%d).",
                        self._consecutive_transient_auth_failures,
                        _MAX_TRANSIENT_AUTH_FAILURES,
                    )
                elif cycle_accepted_requests:
                    # The cycle is not counted as a failing one -- either nothing
                    # rejected, or something rejected and a sibling answered WITH
                    # content, which the branch above lets win -- and at least one
                    # request was accepted. The streak is broken and the count
                    # goes with it, together with the stored cause.
                    #
                    # `cycle_accepted_requests` rather than
                    # `not cycle_booked_transient_auth` is what carries the mixed
                    # case here. A content-bearing device zeroes both fields at
                    # its own site, but a rejection processed AFTER it writes the
                    # cause back; without this branch the pair would end as
                    # (count 0, cause set) in one device order and (count 0, cause
                    # None) in the other, from identical answers. The diagnostic
                    # snapshot exports both fields, and a cause standing next to a
                    # zero count names a failure that is over. Both halves are load-bearing:
                    # without the second, a cycle in which every device timed
                    # out or hit a 5xx would count as a clean cycle and clear a
                    # budget on the strength of an outage. This is the
                    # counter's everyday reset source, and it deliberately sits
                    # on the same clock that increments it -- the device-list
                    # refresh runs on an independent cadence, and resetting from
                    # there starved the escalation at one ratio and escalated
                    # non-consecutive rejections at another.
                    #
                    # CYCLE-WIDE is the load-bearing part of THIS reset: the
                    # original defect was an idle BLE tag clearing the rejection
                    # its sibling had just booked in the same pass, and a
                    # per-device version here would hand that straight back. One
                    # per-device reset does survive, at the location-WITH-content
                    # site above, and it is kept on purpose because content is
                    # the one strong proof on this path. It used to make the end
                    # state depend on device order -- [rejection, content] ended
                    # at 0, [content, rejection] at 1 -- which is why the streak
                    # is now raised and judged once after the loop, gated on the
                    # cycle-wide content tally. Both orders now end at 0.
                    #
                    # The measured limit of the streak rule, stated instead of
                    # discovered later: "consecutive" is counted over cycles that
                    # carried information. A cycle in which every device timed
                    # out or hit a 5xx carries none, so it neither books nor
                    # breaks. Measured consequence on a single-device account:
                    # 403, timeout, 403, timeout, 403 yields 1, 1, 2, 2, 3 and
                    # does escalate, although the rejections were not adjacent.
                    # The alternative -- letting an outage break the streak --
                    # was rejected because it reintroduces the fault this whole
                    # change removes, absence of evidence used as evidence, and
                    # because three pinned tests state the opposite rule for the
                    # non-credential branches.
                    if self._consecutive_transient_auth_failures > 0:
                        _LOGGER.info(
                            "Poll cycle booked no credential rejection; clearing "
                            "%d transient auth failure(s).",
                            self._consecutive_transient_auth_failures,
                        )
                        self._consecutive_transient_auth_failures = 0
                    # Outside the guard above on purpose. In a mixed cycle the
                    # content site has already zeroed the count, and a rejection
                    # processed after it wrote the cause back -- so the count is
                    # 0 here while a cause still stands. The two fields are
                    # exported together and have to end together.
                    self._last_transient_auth_error = None
                    # The same reconciliation for the ACCOUNT-WIDE verdict, and
                    # it is a third field, not a repetition of the two above.
                    # The rejection branch sets `last_exception`, which is the
                    # sole driver of `async_set_update_error` in the `finally`
                    # below, and `GoogleFindMyEntity.available` follows the
                    # coordinator's `last_update_success`. In a mixed cycle that
                    # marked EVERY tracker unavailable although a sibling had
                    # answered WITH CONTENT in the same pass -- the one strong
                    # proof on this path, which this very branch has just
                    # accepted as proof for the counter. Accepting it for the
                    # counter and rejecting it for availability would be two
                    # verdicts from one cycle. The non-credential 4xx branch
                    # above already states this rule for its own case: keep
                    # `cycle_failed`, because the cycle really did not poll every
                    # device and the diagnostic attribute has to say so, and
                    # withdraw only the account-wide report.
                    #
                    # Identity rather than truthiness, so only the rejection THIS
                    # cycle booked is withdrawn. A timeout, a stale owner key or
                    # an account-wide decrypt failure may have got there first
                    # and stays: none of them is refuted by one device answering.
                    if (
                        first_rejection_error is not None
                        and last_exception is first_rejection_error
                    ):
                        # Hand the place over rather than empty it: a failure that
                        # had to queue behind this rejection is refuted by none of
                        # this, and dropping it here would report the cycle as a
                        # successful coordinator update.
                        last_exception = independent_error
                # No device's request was accepted: the cycle must surface.
                # Two counters, one verdict -- see their declaration above for
                # why they are not the same thing. Either way the device produced
                # no evidence, and when no device produced any, the coordinator
                # has nothing to report success about.
                #
                # Do NOT lean on the device list to catch this one layer up -- it
                # is a different RPC (`nbe_list_devices`) on its own clock:
                # DEVICE_LIST_POLL_INTERVAL is a fixed 300 s while the poll
                # interval is configurable between 60 and 3600 s. Below 300 s most
                # cycles reuse the cached list without calling it at all; at the
                # default of 300 s it is refreshed in nearly every cycle. The
                # first reason stands on its own either way.
                #
                # Read the check for exactly what it tests. The equality is over
                # the SUM, so a cycle in which one tracker was rejected and every
                # other one was refused by the server surfaces, while a cycle with
                # a single surviving sibling stays silent.
                #
                # What an empty sibling proves is NARROWER than it was, and
                # narrower than it is tempting to write. The transport failures
                # that used to flatten into `{}` now raise:
                # `location_request.py` turns the 5xx, the 429, the
                # `aiohttp.ClientError` and the generic Nova failure into
                # `LocationRequestNotAcceptedError` itself, so the `api.py`
                # handlers that return `{}` for those four no longer stand
                # between the failure and this loop, and the empty dict comes
                # from the no-location fallthrough (`api.py`, the bare `return {}` after
                # the "No location data for %s" debug), which sits
                # after the request.
                #
                # It does NOT prove that the request was accepted, and the guard
                # must not be read as if it did. Measured, four failures still
                # reach this loop as `{}`, because they are raised BEFORE the
                # outer handler that would convert them: an unregistered FCM
                # receiver provider (`RuntimeError`), a provider that returns
                # None (`RuntimeError`), a missing token cache
                # (`MissingTokenCacheError`), and a failure while binding the
                # lazily imported decrypt / eid-info modules (`ImportError` /
                # `AttributeError`) -- that binding sits above the `try`, unlike
                # the same import inside the FCM callback, which IS guarded. Four
                # modes in four clauses on purpose: grouping the two provider
                # guards into one is how a reader of this list, and then a
                # docstring quoting it, arrived at "three". All of them are flattened by `api.py`'s
                # `except RuntimeError` / `except Exception` arms. The first is
                # deliberate and documented there as a cold-boot race that retries
                # on the next cycle; turning it into an outage would report every
                # restart as one. The others are programming or packaging faults
                # rather than operating states. They are named here so the next
                # reader does not mistake this guard for exhaustive.
                #
                # That is also why the guard does NOT count empty siblings, and
                # why it does not need to. It is deliberately conservative: it
                # demands that EVERY device missed, so one surviving tracker is
                # enough to keep the account available -- and an unrecognised
                # failure makes it stay silent, never fire wrongly.
                #
                # Named because it is a real behaviour change and not a neutral
                # gap: BEFORE the raise sites existed, a cycle in which every
                # single request failed server-side reported `success` and left
                # every entity available with its cached position. The reference
                # case that must NOT flip with it is the healthy one --
                # `test_a_cycle_of_only_empty_results_still_reports_success` holds
                # idle BLE tags at `success`. The price of closing the gap is
                # accepted in the plan: on a single-device account one failed
                # request now takes that account's tracker unavailable for the
                # cycle. It is not only single-device accounts, and that is worth
                # stating rather than discovering: a forced cycle runs even
                # without a ready push transport, so an account whose FCM
                # registration fails for every device meets this guard at any
                # device count. No consecutive-cycle threshold is applied, because a
                # threshold without a field measurement would be a guessed
                # number; `_finalize_cycle_decrypt_state` is the pattern to copy
                # if flapping is ever observed. See
                # `PLAN_GFMY_EMPTY_RESULT_DISTINGUISHABLE`.
                #
                # `last_exception is None` is defence in depth here, and is
                # marked as such rather than left to look load-bearing: measured,
                # it cannot currently be False when the equality holds. Every
                # branch that sets `last_exception` (timeout, transient auth,
                # stale key, the broad handler) increments NEITHER counter, so
                # one of those devices already breaks the sum. A mutation that
                # deletes the term therefore turns no test red -- deliberately,
                # because writing a test for an unreachable state would only pin
                # the reasoning, not the behaviour. The term stays because the
                # obvious next edit is a branch that both counts and reports, and
                # then it is the difference between reporting the expired
                # credential and reporting the outage that hid it.
                if (
                    cycle_unaccepted_devices
                    and (cycle_unaccepted_devices + cycle_rejected_devices)
                    == cycle_polled_devices
                    and last_exception is None
                ):
                    last_exception = UpdateFailed(
                        f"No device's locate request was accepted this cycle "
                        f"({cycle_unaccepted_devices} not accepted, "
                        f"{cycle_rejected_devices} rejected)",
                        translation_domain=DOMAIN,
                        translation_key="locate_not_accepted",
                        translation_placeholders={
                            "not_accepted": str(cycle_unaccepted_devices),
                            "rejected": str(cycle_rejected_devices),
                        },
                    )
                # Every device was refused by name: on the rejections' own
                # terms that is account-wide, so the cycle must surface. Kept as
                # its own guard rather than folded into the sum above so the
                # reported message names the condition; the two are mutually
                # exclusive by construction, because this one requires the
                # unaccepted count to be zero for the equality to hold.
                if (
                    cycle_rejected_devices
                    and cycle_rejected_devices == cycle_polled_devices
                    and last_exception is None
                ):
                    last_exception = UpdateFailed(
                        f"Every device ({cycle_rejected_devices}) was rejected by "
                        "the server this cycle",
                        translation_domain=DOMAIN,
                        translation_key="locate_all_rejected",
                        translation_placeholders={
                            "rejected": str(cycle_rejected_devices)
                        },
                    )
                # Resolve the cycle's decrypt verdict AND reconcile the cycle's
                # failure state with it in one place (positive proof dominates: a
                # sibling success refutes a single device's failure as account-wide).
                # A benign per-device downgrade neither escalates to reauth NOR marks
                # the coordinator update failed; a genuinely account-wide failure
                # surfaces and, at the consecutive-cycle threshold, escalates.
                cycle_failed, last_exception, reauth_exc = (
                    self._finalize_cycle_decrypt_state(
                        cycle_decrypt_error=cycle_decrypt_error,
                        cycle_had_successful_decrypt=cycle_had_successful_decrypt,
                        cycle_had_stale_key=cycle_had_stale_key,
                        cycle_failed=cycle_failed,
                        last_exception=last_exception,
                    )
                )
                if reauth_exc is not None:
                    self._last_poll_result = "failed"
                    self._request_poll_reauth(reauth_exc)
                    return
            finally:
                # Update scheduling baseline and clear flag, then push end snapshot.
                # Always advance the poll baseline to prevent high-frequency
                # forced-poll loops that bypass min_poll_interval.  When push
                # transport reconnects, push_updated() resets the baseline so
                # there is no "dead zone" risk.
                if force and not _any_device_got_data:
                    _LOGGER.debug(
                        "Forced poll returned no data; scheduling retry "
                        "after min_poll_interval (%ds).",
                        self.min_poll_interval,
                    )
                    self._schedule_short_retry(self.min_poll_interval)
                self._last_poll_mono = time.monotonic()
                self._is_polling = False
                self.safe_update_metric("last_poll_end_mono", time.monotonic())
                if cycle_failed:
                    self._last_poll_result = "failed"
                else:
                    self._last_poll_result = "success"
                # Always publish the full snapshot so cached devices remain visible
                ignored = self._get_ignored_set()
                # Use the latest remembered full list; filter ignored
                visible_devices = [
                    d
                    for d in (self._last_device_list or [])
                    if d.get("id") not in ignored
                ]
                end_snapshot = self._build_snapshot_from_cache(
                    visible_devices, wall_now=time.time()
                )
                self.async_set_updated_data(end_snapshot)
                self._schedule_eid_resolver_refresh()
                if last_exception:
                    self.async_set_update_error(last_exception)
