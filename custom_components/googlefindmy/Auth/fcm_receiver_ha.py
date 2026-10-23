# custom_components/googlefindmy/Auth/fcm_receiver_ha.py
"""Home Assistant compatible FCM receiver for Google Find My Device.

This module provides an HA-integrated Firebase Cloud Messaging (FCM) receiver that:
- Runs fully async with supervised background loops (one supervisor per config entry).
- Persists credentials via each entry's TokenCache (HA Store-backed).
- Notifies registered request callbacks or pushes background updates to the *right* coordinator(s).
- Avoids any synchronous cache access in the event loop.

Design notes
------------
* Lifecycle: A single shared receiver instance is managed in `hass.data[DOMAIN]`.
  Internally, this receiver manages **one FCM client per entry_id**.
* No global singletons outside this module; Home Assistant orchestrates creation/cleanup.
* The receiver never triggers UI/ChromeDriver flows; it only consumes credentials
  from caches and updates them when the server requests re-registration.
* All potentially blocking work (protobuf decoding, user callbacks) runs in executors.

Multi-account support (entry-scoped clients)
--------------------------------------------
* One client per entry: `self.pcs[entry_id]` and in-memory creds `self.creds[entry_id]`.
* One supervisor loop per entry: `self.supervisors[entry_id]` with the same
  backoff/heartbeat logic as before.
* Per-entry persistence: credentials are written to the entry's TokenCache
  (key: `fcm_credentials`); a routing set of tokens can be stored (key:
  `fcm_routing_tokens`) for resume-after-restart.
* Token → entry routing: incoming pushes are routed using the message `to` token
  (or registration endpoint token). Fallback: if no token mapping exists, all
  coordinators are considered. Optionally, an owner-index fallback can be used
  when a Home Assistant instance is attached (see `attach_hass`).

Precise fan-out (debounce with routing context)
-----------------------------------------------
* We debounce per **(entry_id, device_id)**:
    - `_pending[(entry_id, device_id)]` holds the latest decoded payload **plus** the
      routed target entry set.
    - `_schedule_flush(entry_id, device_id)` (re)starts a short timer (default 250 ms).
    - `_flush(entry_id, device_id)` fans the coalesced payload out only to coordinators
      for the routed entries (no broadcast).

Runtime telemetry (for diagnostics)
-----------------------------------
* Per-receiver metrics retained for compatibility:
  `last_start_monotonic`, `last_stop_monotonic`, `start_count` (aggregate view).
* DEBUG logs include routing details (one record per received push):
  `push_received(entry=<id>|unknown, device=..., fanout_targets=n, route=token|owner_index|client|fallback)`.

Retry/404 mitigation (unchanged behavior)
-----------------------------------------
* Registration keeps the existing fixes: numeric `messaging_sender_id`, `Android-GCM/1.5`
  UA in the underlying client, 404 toggle `/register ↔ /register3` (handled in client),
  bounded retries on `PHONE_REGISTRATION_ERROR`, and **no** retries on `BadAuthentication`.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextvars
import functools
import json
import logging
import math
import random
import time
from collections.abc import (
    Awaitable,
    Callable,
    Coroutine,
    Iterator,
    Mapping,
    MutableMapping,
)
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, cast

from aiohttp import ClientError
from cryptography.exceptions import InvalidTag
from homeassistant.helpers import issue_registry as ir
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from custom_components.googlefindmy._reauth_reason import ReauthReasonCode
from custom_components.googlefindmy.Auth.firebase_messaging.fcmregister import (
    FcmRegisterHTTPError,
)
from custom_components.googlefindmy.Auth.log_safety import (
    describe_exception,
    exception_origin,
)
from custom_components.googlefindmy.exceptions import FatalRegistrationError
from custom_components.googlefindmy.location_row_markers import (
    strip_transient_keys,
    substitute_zone_accuracy,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
    DecryptionError,
    OwnerKeyLookupTransientError,
    StaleOwnerKeyError,
    any_real_location_record,
    async_decrypt_location_response_locations,
)
from custom_components.googlefindmy.NovaApi.nova_request import (
    _CACHE_PROVIDER,
)
from custom_components.googlefindmy.ProtoDecoders import decoder as decoder_module

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from custom_components.googlefindmy.Auth.firebase_messaging._typing import (
        CredentialsUpdatedCallable,
    )
    from custom_components.googlefindmy.Auth.token_cache import TokenCache
    from custom_components.googlefindmy.google_home_filter import (
        GoogleHomeFilter as GoogleHomeFilterProtocol,
    )
else:
    GoogleHomeFilterProtocol = Any
    TokenCache = Any

# Integration-level tunables (safe fallbacks if missing)
try:
    from custom_components.googlefindmy.const import (
        DATA_SECRET_BUNDLE,
        DOMAIN,
        FCM_ABORT_ON_SEQ_ERROR_COUNT,
        FCM_CLIENT_HEARTBEAT_INTERVAL_S,
        FCM_CONNECTION_RETRY_COUNT,
        FCM_DATA_STARVATION_S,
        FCM_IDLE_RESET_AFTER_S,
        FCM_MONITOR_INTERVAL_S,
        FCM_SERVER_HEARTBEAT_INTERVAL_S,
        FCM_ZOMBIE_CHECK_INTERVAL_S,
        FCM_ZOMBIE_MAX_RECONNECTS,
        FCM_ZOMBIE_RECONNECT_BACKOFF_BASE_S,
        FCM_ZOMBIE_RECONNECT_BACKOFF_CAP_S,
        OPT_IGNORED_DEVICES,  # for ignore fallback via options
    )
except ImportError:  # pragma: no cover
    FCM_CLIENT_HEARTBEAT_INTERVAL_S = 20
    FCM_SERVER_HEARTBEAT_INTERVAL_S = 10
    FCM_IDLE_RESET_AFTER_S = 90.0
    FCM_CONNECTION_RETRY_COUNT = 5
    FCM_MONITOR_INTERVAL_S = 1
    FCM_ABORT_ON_SEQ_ERROR_COUNT = 3
    FCM_DATA_STARVATION_S = 900
    FCM_ZOMBIE_CHECK_INTERVAL_S = 30
    FCM_ZOMBIE_RECONNECT_BACKOFF_BASE_S = 60
    FCM_ZOMBIE_RECONNECT_BACKOFF_CAP_S = 900
    FCM_ZOMBIE_MAX_RECONNECTS = 5
    DATA_SECRET_BUNDLE = "secrets_data"
    DOMAIN = "googlefindmy"
    OPT_IGNORED_DEVICES = "ignored_devices"


# Readiness budget (seconds) for a fresh FCM identity to reach the STARTED
# state after a (re)connect.  Derived from the library's connection-retry
# count rather than a fixed value: a brand-new identity's first MCS connect
# can take the full retry budget (FCM_CONNECTION_RETRY_COUNT attempts at
# ~_FCM_READY_WAIT_PER_ATTEMPT_S each) plus a small margin.  SINGLE SOURCE OF
# TRUTH shared by the manual-/first-locate path and the zombie-starvation
# watchdog so the two readiness budgets can never drift.  The watchdog in
# particular must NOT wait the full FCM_DATA_STARVATION_S (900s) here: its
# reconnect wrapper holds the shared per-entry first-locate lock while
# waiting, so a 900s budget would block a concurrent manual locate on the
# same entry for up to 15 minutes (Codex F1).  The 900s starvation threshold
# still governs the *decision* to reconnect (``_is_data_starved``); only this
# wait-for-STARTED budget is bounded by it.
_FCM_READY_WAIT_PER_ATTEMPT_S: float = 4.0
_FCM_READY_WAIT_MARGIN_S: float = 2.0


def _max_ready_wait_s() -> float:
    """Return the STARTED-readiness budget in seconds (see note above)."""
    return (
        int(FCM_CONNECTION_RETRY_COUNT) * _FCM_READY_WAIT_PER_ATTEMPT_S
        + _FCM_READY_WAIT_MARGIN_S
    )


# Optional import of worker run-state enum (for robust state checks)
if TYPE_CHECKING:
    from custom_components.googlefindmy.Auth.firebase_messaging import (
        FcmPushClient,
        FcmPushClientConfig,
        FcmPushClientRunState,
        FcmRegisterConfig,
    )

    HAVE_FCM_PUSH_CLIENT = True
else:
    try:
        from custom_components.googlefindmy.Auth.firebase_messaging import (
            FcmPushClient,
            FcmPushClientConfig,
            FcmPushClientRunState,
            FcmRegisterConfig,
        )

        HAVE_FCM_PUSH_CLIENT = True
    except ImportError:  # pragma: no cover
        HAVE_FCM_PUSH_CLIENT = False
        FcmPushClientRunState = None
        FcmPushClient = cast("type[Any]", object)
        FcmRegisterConfig = cast("type[Any]", object)
        FcmPushClientConfig = cast("type[Any]", object)

_LOGGER = logging.getLogger(__name__)


# AP4 (finding 4c): the driving supervisor generation for the in-flight FCM
# handshake. The vendored ``firebase_messaging`` client invokes
# ``credentials_updated_callback`` SYNCHRONOUSLY from inside
# ``checkin_or_register`` / ``reregister_keeping_identity`` (same task). The
# credentials callback closure is bound at client-build time and is shared
# across a fast reload (``unregister_coordinator`` bumps the generation but does
# NOT pop the reused client), so the closure alone cannot tell which supervisor
# drives the current handshake. ``_register_for_fcm_entry`` publishes its
# captured generation here for the duration of the await; the callback reads it
# and gates its writes on the *triggering* generation. A ContextVar (not a plain
# attribute) is required: many awaits inside firebase separate the publish from
# the callback, so a concurrent supervisor task must not clobber the value --
# task-local isolation does exactly that. Outside any handshake (a live-
# connection cred refresh) the default ``None`` preserves the historical
# tombstone-only behaviour.
_FCM_DRIVING_GENERATION: contextvars.ContextVar[tuple[str, int | None] | None] = (
    contextvars.ContextVar("fcm_driving_generation", default=None)
)


# Iter-12 (Codex follow-up): the short-run crash cap needs to know
# whether the FCM client ever reached ``FcmPushClientRunState.STARTED``
# during a run, even when the entire CREATED -> STARTED -> CRASH
# lifecycle happens between two monitor polls (``FCM_MONITOR_INTERVAL_S``
# >= 0.5s). Sampling alone is too coarse for the very poison-message
# loop the cap is meant to stop. A subclass with a ``__setattr__`` hook
# latches ``_has_been_started`` the moment the vendored library assigns
# ``run_state = STARTED`` -- independent of any polling cadence and
# without modifying the vendored library itself. See
# CA-DEFENSE-OBSERVABILITY-001.
if HAVE_FCM_PUSH_CLIENT and FcmPushClientRunState is not None:
    # Snapshot of the original class for runtime identity comparison.
    # Tests substitute the module-level ``FcmPushClient`` symbol via
    # monkeypatch (the documented seam) to inject test doubles; the
    # factory below honours that substitution by class identity, not by
    # subclass relation. See CA-SUBCLASS-IDENTITY-001.
    _OriginalFcmPushClient: type[Any] | None = FcmPushClient

    class _ObservableFcmPushClient(FcmPushClient[Any]):
        """FcmPushClient subclass with a run_state observer latch.

        See CA-DEFENSE-OBSERVABILITY-001.
        """

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            # Seed the latch before super().__init__ so any attribute
            # assignment performed by the parent's constructor still
            # routes through our ``__setattr__`` without raising.
            object.__setattr__(self, "_has_been_started", False)
            super().__init__(*args, **kwargs)

        def __setattr__(self, name: str, value: Any) -> None:
            super().__setattr__(name, value)
            if (
                name == "run_state"
                and FcmPushClientRunState is not None
                and value == FcmPushClientRunState.STARTED
            ):
                # ``object.__setattr__`` to avoid recursion through this hook.
                object.__setattr__(self, "_has_been_started", True)

    _ObservableFcmPushClientCls: type[Any] | None = _ObservableFcmPushClient
else:  # pragma: no cover - exercised only when the vendored library is absent
    _OriginalFcmPushClient = None
    _ObservableFcmPushClientCls = None


def _FcmPushClientFactory(*args: Any, **kwargs: Any) -> FcmPushClient[Any]:
    """Construct an FCM client at call time, honouring test substitutions.

    Production: prefers the ``_ObservableFcmPushClient`` subclass so the
    short-run crash cap can observe ``run_state -> STARTED`` transitions
    that happen between monitor polls (CA-DEFENSE-OBSERVABILITY-001).

    Tests: when the module-level ``FcmPushClient`` symbol has been
    monkeypatched away from the original class (the documented test
    seam), the factory instantiates the patched class DIRECTLY rather
    than the production subclass. This preserves ``isinstance`` identity
    for test doubles (CA-SUBCLASS-IDENTITY-001) instead of returning a
    subclass that diverges from the patched symbol.
    """

    current = globals().get("FcmPushClient")
    if (
        _ObservableFcmPushClientCls is not None
        and _OriginalFcmPushClient is not None
        and current is _OriginalFcmPushClient
    ):
        return cast(
            "FcmPushClient[Any]",
            _ObservableFcmPushClientCls(*args, **kwargs),
        )
    if current is None:  # pragma: no cover - vendored library missing
        raise RuntimeError("FcmPushClient is not available")
    return cast("FcmPushClient[Any]", current(*args, **kwargs))


type JSONDict = dict[str, Any]
type MutableJSONMapping = MutableMapping[str, Any]

_MAX_SUPERVISOR_BACKOFF_S = 120.0  # cap retry delay at 2 minutes (was 4096s)
_BACKOFF_WARNING_THRESHOLD_S = 64.0  # warn after ~6 failed attempts
_MAX_FATAL_AUTH_RETRIES = 3  # 401: max retries (60s, 120s, then give up)
_MAX_FATAL_ENDPOINT_RETRIES = 7  # 404: max retries (30s-300s, Google rotates endpoints)
# Defense 2: supervisor-level crash cap as a bulkhead around the inner-loop
# poison-message filter (``fcmpushclient._listen`` Defense 1+3). When the FCM
# client connects but loses the run-loop within ``_SHORT_RUN_THRESHOLD_S``
# repeatedly, a persistent poison condition is suspected and the supervisor
# stops looping to prevent unbounded retry pressure on Google + log spam.
_MAX_CONSECUTIVE_SHORT_RUNS = 10
_SHORT_RUN_THRESHOLD_S = (
    30.0  # strictly < 30s between pc.start() and STOPPED = short run
)
# First-locate delivery reconnect (AP1): a brand-new MCS session can reach
# ``STARTED`` yet never receive the very first locate push (``STARTED`` is
# necessary but not sufficient for delivery; third of the #1148/#1149 family).
# Only a full reconnect — a fresh session that subscribes *after* registration
# completes — heals it.  This window bounds "how young a session still counts as
# freshly registered / churning" for that one-shot decision.  It is deliberately
# NOT the activity-health freshness window (``_activity_stale_after_s`` /
# ``FCM_IDLE_RESET_AFTER_S`` = 90s, idle-reset semantics): it feeds no health
# snapshot and no coordinator state, so it does not violate the Auth/AGENTS.md
# "reuse the health helper, do not add parallel health windows" rule (whose
# rationale is diagnostics/coordinator-state consistency, untouched here).
_CHURN_WINDOW_S = 15.0

_HTTP_UNAUTHORIZED = 401
_HTTP_NOT_FOUND = 404

# Defense 2 iter-14 (Codex follow-up): the supervisor-level crash cap writes
# its terminal state into ``_fatal_errors[entry_id]`` so the binary_sensor
# diagnostic attribute and the "is FCM ready" check in
# ``coordinator/polling.py`` see the listener as unavailable. However, the
# coordinator's re-auth escalation in ``coordinator/polling.py`` also consumes
# that map and, after _FCM_ERROR_RETRY_THRESHOLD consecutive update cycles,
# raises ``ConfigEntryAuthFailed`` for ANY non-auth fatal it does not
# explicitly classify. The crash-loop fatal is NOT an auth problem
# (the user must reload / regenerate credentials per the repair issue, not
# re-authenticate). To prevent users from being pushed into Home Assistant's
# re-auth flow for what is really a poison-message defense, the cap-fire
# branch tags its message with this prefix and the polling-side consumer
# excludes it from the re-auth escalation. This module is the SSOT for the
# prefix; the polling-side consumer imports it from here.
CRASH_LOOP_FATAL_PREFIX = "FCM short-run crash loop:"


async def _call_in_executor[**P, T](
    func: Callable[P, T], /, *args: P.args, **kwargs: P.kwargs
) -> T:
    """Run ``func`` in a background thread with wide Python compatibility."""

    to_thread_obj = getattr(asyncio, "to_thread", None)
    if to_thread_obj is not None:
        to_thread = cast(Callable[..., Awaitable[T]], to_thread_obj)
        return await to_thread(func, *args, **kwargs)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args, **kwargs)
            return future.result()

    return await loop.run_in_executor(None, functools.partial(func, *args, **kwargs))


def _coordinator_google_home_filter(
    coordinator: Any,
) -> GoogleHomeFilterProtocol | None:
    """Return the Google Home filter for a coordinator if present."""

    entry = getattr(coordinator, "config_entry", None) or getattr(
        coordinator, "entry", None
    )
    runtime_data = getattr(entry, "runtime_data", None)
    google_home_filter = getattr(runtime_data, "google_home_filter", None)
    return cast("GoogleHomeFilterProtocol | None", google_home_filter)


class FcmReceiverHA:
    """FCM receiver integrated with Home Assistant's async lifecycle (multi-client).

    Responsibilities:
        * Initialize and supervise a dedicated FCM client per config entry.
        * Handle per-request callbacks for specific devices.
        * Route background updates only to the owning entry's coordinator(s).
        * Persist credential updates to each entry's TokenCache.

    Contract:
        * Call `async_initialize()` once (idempotent).
        * `register_coordinator()` / `unregister_coordinator()` are synchronous
          to match HA's `async_on_unload` contract.
        * `async_stop()` gracefully shuts down **all** supervisors/clients.
        * `request_stop()` signals a stop without awaiting.
    """

    # -------------------- Construction & shared constants --------------------

    def __init__(self) -> None:
        # Optional handle to Home Assistant (for owner-index fallback). Set via attach_hass().
        self._hass: HomeAssistant | None = None

        # Per-entry in-memory credentials and clients
        self.creds: dict[
            str, MutableJSONMapping | None
        ] = {}  # entry_id -> credentials dict
        self.pcs: dict[str, FcmPushClient[Any]] = {}  # entry_id -> FcmPushClient
        self.supervisors: dict[
            str, asyncio.Task[None]
        ] = {}  # entry_id -> supervisor task
        self._stop_evts: dict[str, asyncio.Event] = {}  # entry_id -> stop event

        # Per-request callbacks awaiting device responses (entry-agnostic)
        self.location_update_callbacks: dict[str, Callable[[str, str], None]] = {}

        # Coordinators eligible to receive background updates
        self.coordinators: list[Any] = []

        # Routing tables
        self._token_to_entries: dict[str, set[str]] = {}  # token -> set(entry_id)
        self._entry_to_tokens: dict[str, set[str]] = {}  # entry_id -> set(token)

        # Entry-scoped TokenCache instances (for background decrypt path)
        self._entry_caches: dict[str, TokenCache] = {}
        self._pending_creds: dict[str, MutableJSONMapping | None] = {}
        self._pending_routing_tokens: dict[str, set[str]] = {}
        self._default_entry_id: str | None = None

        # Debounce state (push path): keyed by (entry_id, device_id)
        self._pending: dict[tuple[str, str], JSONDict] = {}
        self._pending_targets: dict[tuple[str, str], set[str] | None] = {}
        self._flush_tasks: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._debounce_ms: int = 250

        self._fatal_error: str | None = None
        self._fatal_errors: dict[str, str] = {}
        self._fatal_retry_counts: dict[
            str, int
        ] = {}  # "entry:auth"|"entry:endpoint" -> count

        # Aggregate telemetry
        self.last_start_monotonic: float = 0.0
        self.last_stop_monotonic: float = 0.0
        self.start_count: int = 0

        # Firebase project configuration for Google Find My Device
        self.project_id = "google.com:api-project-289722593072"
        self.app_id = "1:289722593072:android:3cfcf5bc359f0308"
        self.api_key = "AIzaSyD_gko3P392v6how2H7UpdeXQ0v2HLettc"
        self.message_sender_id = "289722593072"  # numeric Sender ID (project number)

        # Config used by all clients
        self._client_cfg: FcmPushClientConfig | None = None  # initialized lazily

        # Guard against concurrent start/stop/register races
        self._lock = asyncio.Lock()

        # Per-entry runtime health
        self._entry_last_activity_monotonic: dict[str, float] = {}
        self._activity_stale_after_s: float = float(FCM_IDLE_RESET_AFTER_S)
        self._entry_health: dict[str, bool] = {}
        self._entry_last_connected_wall: dict[str, float] = {}

        # Data-starvation liveness watchdog (re-arming zombie self-heal).
        #
        # ``_entry_last_data_delivery_monotonic`` is a *separate* clock from the
        # activity clock above: it advances ONLY on a real locate data delivery
        # (the cb-hit branch of ``_handle_notification_async``), never on a bare
        # heartbeat ack. This separation is the crux of the fix -- the activity
        # clock is heartbeat-poisoned and therefore masks starvation, so the
        # watchdog must not consult it for "data flowing". A missing entry means
        # "no data delivered yet" (only starved once a locate has been sent).
        self._entry_last_data_delivery_monotonic: dict[str, float] = {}
        # ``_entry_last_locate_sent_monotonic`` records the last time a locate was
        # dispatched for the entry (stamped in ``async_register_for_location_
        # updates``). The predicate requires "at least one locate since the last
        # delivery" so an empty account (no locates -> no expected delivery) is
        # never mis-classified as a zombie.
        self._entry_last_locate_sent_monotonic: dict[str, float] = {}
        # Per-entry watchdog backoff state and throttle clock.
        self._zombie_reconnect_attempts: dict[str, int] = {}
        self._zombie_next_allowed_reconnect_monotonic: dict[str, float] = {}
        self._zombie_next_eval_monotonic: dict[str, float] = {}
        # Tracked in-flight watchdog reconnect tasks (one per entry). The reconnect
        # runs in a SEPARATE task (never inline in the supervisor monitor loop,
        # which would deadlock: ``_force_first_locate_reconnect`` awaits a fresh
        # STARTED pc that only the supervisor's outer loop can build). Cancelled
        # on teardown in ``_purge_entry_tokens``.
        self._zombie_reconnect_tasks: dict[str, asyncio.Task[None]] = {}

        # One-shot first-locate reconnect (#1150) serialization. Concurrent
        # manual locates for different trackers on the *same* entry both enter
        # the fresh/no-delivery reconnect branch; without coordination the
        # second caller tears down the replacement client the first caller just
        # waited for, so the first request hands its token to a listener that is
        # already gone and times out (Codex #1150 follow-up). A per-entry lock
        # serialises the decision and ``_first_locate_reconnect_done`` records
        # the replacement we already reconnected to (by identity), so a
        # concurrent or sequential second caller reuses it instead of forcing
        # another reconnect. A genuinely new session generation (a different
        # client object) is still allowed exactly one fresh reconnect. Not the
        # global ``self._lock``: the reconnect awaits the supervisor rebuild,
        # which itself takes ``self._lock``, so reusing it would deadlock.
        self._first_locate_locks: dict[str, asyncio.Lock] = {}
        self._first_locate_reconnect_done: dict[str, FcmPushClient[Any]] = {}

        # Active background tasks for exception tracking (P0 fix)
        self._active_tasks: set[asyncio.Task[Any]] = set()

        # Per-entry nudge events: allows coordinator to wake up a sleeping supervisor
        self._retry_nudge_evts: dict[str, asyncio.Event] = {}

        # Defense 2 iter-8: persistent short-run cap latch.
        #
        # When the cap fires the entry is in a *poison-message crash loop*
        # that only a real healthy supervisor run (>= _SHORT_RUN_THRESHOLD_S)
        # can prove gone. A successful FCM re-registration alone is NOT a
        # proof: registration only contacts the GCM endpoint, it does not
        # show that the subsequent listener stays up long enough to drain
        # the poisoned notification.
        #
        # The latch is therefore set together with the user-visible repair
        # issue at cap-fire time and is the *only* thing that gates whether
        # ``_clear_fatal_error_for_entry`` (which gets called from every
        # registration-success path) is allowed to drop the fatal-error map
        # entry and the Repairs UI artefact. Without this latch, any reload
        # or credential update would clear the cap-related fatal state, the
        # supervisor would start a fresh 10-run burst, and the user would
        # see no visible fatal state in between (Codex defense-2 iter-8
        # second finding).
        self._short_run_cap_latched: set[str] = set()
        # Defense 2 iter-16 (Codex follow-up): persistent snapshot of the
        # cap-fire message per entry. Populated when the cap fires and used
        # by ``_clear_fatal_error_for_entry`` to restore the cap fatal after
        # a non-cap fatal (e.g. a terminal 401/404 from a subsequent
        # re-registration) was cleared. Cleared in the same two places that
        # release the latch: the healthy-run cleanup branch and any
        # ``force=True`` teardown. This keeps the per-entry fatal visible to
        # the coordinator and the diagnostic binary sensor for the entire
        # latch lifetime, even across non-cap fatal overwrites.
        self._short_run_cap_messages: dict[str, str] = {}

        # AP4 (finding 4a): tombstone set of entry_ids whose synchronous
        # teardown (``unregister_coordinator``) has already purged their
        # per-entry state. ``unregister_coordinator`` does not await the
        # still-running supervisor task, which writes per-entry state (fatal
        # counts, cap latch, creds) largely outside ``self._lock`` and could
        # otherwise re-create exactly what the purge removed. While an entry is
        # tombstoned those supervisor writes are skipped; ``register_coordinator``
        # clears the tombstone on the next setup so a reload restores normal
        # writes while a real removal stays clean.
        self._unregistered: set[str] = set()

        # AP4 (finding 4b): per-entry supervisor generation. The boolean
        # tombstone above is cleared by ``register_coordinator`` on the next
        # setup, but every unload also calls ``request_stop()`` which only
        # *cancels* the prior supervisor without awaiting it. A fast reload can
        # therefore clear the tombstone while the cancelled supervisor is still
        # unwinding, re-opening the teardown race for that stale task. Each
        # teardown bumps this counter; a supervisor captures the value current
        # when it starts, so a superseded supervisor's writes stay suppressed by
        # generation even after the tombstone is cleared for the new
        # incarnation. The credentials-updated callback cannot capture this at
        # build time (its FCM client -- and the bound closure -- is reused across
        # a fast reload, so the build-time snapshot would suppress the LIVE
        # incarnation's legitimate writes). Instead it reads the *driving*
        # generation that ``_register_for_fcm_entry`` publishes in
        # ``_FCM_DRIVING_GENERATION`` around each handshake (finding 4c): a
        # cancelled-but-unwinding supervisor that fires the callback mid-checkin
        # is then recognised as superseded, while a live handshake (matching
        # generation) or an out-of-band refresh (no driving generation -> the
        # boolean tombstone window only) still writes.
        self._entry_generation: dict[str, int] = {}

    def _entry_writes_suppressed(
        self, entry_id: str, generation: int | None = None
    ) -> bool:
        """Return True when a per-entry write must be dropped (AP4).

        Two independent conditions suppress a write:

        * The entry is tombstoned by a synchronous teardown (boolean
          ``_unregistered``); cleared by ``register_coordinator`` on the next
          setup. This covers the teardown window and the reused FCM client's
          credentials callback.
        * A ``generation`` is supplied and no longer matches the entry's current
          generation, meaning the caller belongs to a superseded supervisor that
          was cancelled but is still unwinding. This keeps a stale supervisor's
          writes suppressed even after the tombstone is cleared for a new
          incarnation (finding 4b).

        Callers without a captured generation (e.g. the credentials callback)
        pass ``None`` and get the pure tombstone-window behaviour.

        Use this to gate *write* side effects that RE-CREATE per-entry state
        (token routing, fatal-error publication, cap-fire). For *removal* side
        effects that are teardown-safe in the tombstone window but harmful only
        when a stale generation would clobber the live incarnation, gate on
        ``_entry_generation_superseded`` instead.
        """
        if entry_id in self._unregistered:
            return True
        return self._entry_generation_superseded(entry_id, generation)

    def _entry_generation_superseded(
        self, entry_id: str, generation: int | None
    ) -> bool:
        """Return True when ``generation`` belongs to a superseded supervisor.

        This checks ONLY the generation-mismatch condition (a stale supervisor
        that captured an old generation and lost ownership to a live incarnation
        after a fast reload), NOT the boolean tombstone window. A caller without
        a captured generation (the public re-register / credentials path) is a
        live operation and is never "superseded", so it returns False.

        It is the narrow guard for *removal* side effects (clearing the fatal
        latch, deleting the reauth repair, resetting shared retry counters):
        those are teardown-safe while an entry is merely tombstoned (it is going
        away and ``unregister_coordinator`` already reset it), but must be
        skipped when an old generation would wipe the LIVE generation's
        diagnostics.
        """
        return generation is not None and generation != self._entry_generation.get(
            entry_id, 0
        )

    def _clear_fatal_error_for_entry(
        self,
        entry_id: str,
        *,
        reason: str | None = None,
        force: bool = False,
    ) -> None:
        """Clear any latched fatal registration error for a config entry.

        Also removes the Defense 2 short-run-crash-loop repair issue from the
        Home Assistant Issue Registry. The fatal-error map and the Issue
        Registry entry are a CREATE/DELETE pair: any recovery path that clears
        the in-memory latch must also delete the user-visible UI artefact, or
        the Repairs panel keeps a stale non-fixable error after the underlying
        condition is gone (e.g. user re-registers credentials and FCM starts
        successfully again).

        A hard-reset teardown (``force=True``) additionally releases the
        short-run cap latch (see below). It deliberately does NOT delete the
        entry-scoped *fixable* reauth repairs: ``force=True`` is reached from
        ``unregister_coordinator``, which fires on EVERY unload, including the
        reload Home Assistant performs right after a successful reauth flow.
        Deleting them here would dismiss a still-active prompt before the new
        supervisor has proven recovery. Those repairs are retired by the
        supervisor success path on reload and by
        ``async_delete_entry_repair_issues`` on actual entry removal (Codex
        PR #1104 follow-up).

        Defense 2 iter-8 (Codex second finding): a successful FCM
        re-registration is NOT a recovery proof for the short-run crash cap.
        While ``_short_run_cap_latched`` contains *entry_id* this method must
        protect the cap state (latched fatal message + Repairs UI artefact)
        from being silently cleared, so users keep seeing the fatal state
        until a real healthy supervisor run (>= _SHORT_RUN_THRESHOLD_S) drops
        the latch from the monitor loop. Hard-reset callers (entry unregister,
        test cleanup, integration teardown) must opt in with ``force=True``.

        Defense 2 iter-10 (Codex follow-up): the latch guard is intentionally
        narrow. It is a no-op ONLY when the currently stored fatal IS the
        cap-related message (``FCM short-run crash loop: ...``). Non-cap fatal
        errors (e.g. a terminal 401/404 from a subsequent re-registration that
        overwrote ``_fatal_errors[entry_id]``) must clear normally so a
        credentials recovery is not blocked by an unrelated latch. The cap
        latch, any cap-related fatal it still protects, and the Repairs UI
        artefact remain in place in that case; only the unrelated fatal is
        dropped.
        """

        if not force and entry_id in self._short_run_cap_latched:
            current_fatal = self._fatal_errors.get(entry_id)
            # No fatal stored, or the stored fatal IS the cap message:
            # preserve everything (latch, message, Repairs UI artefact).
            if current_fatal is None or current_fatal.startswith(
                CRASH_LOOP_FATAL_PREFIX
            ):
                _LOGGER.debug(
                    "[entry=%s] Skipping fatal-error clear; short-run cap latch "
                    "active (reason=%s). Latch will be released by a healthy "
                    "supervisor run.",
                    entry_id,
                    reason or "unspecified",
                )
                return

            # Non-cap fatal stored while the cap latch is active. Clear the
            # unrelated fatal but KEEP the cap latch and Repairs UI artefact
            # intact, so the user-visible cap warning is not silently
            # invalidated by an unrelated recovery path.
            #
            # Defense 2 iter-16 (Codex follow-up): the per-entry cap-fire
            # message snapshot (``_short_run_cap_messages``) must be
            # re-written into ``_fatal_errors`` so the coordinator and the
            # diagnostic binary sensor continue to see the cap-fatal state
            # during the recovery/retry window. Otherwise the latch would
            # silently invalidate the per-entry fatal even though the
            # in-memory latch and the Repairs UI artefact remain set.
            _LOGGER.debug(
                "[entry=%s] Cap latch active; clearing non-cap fatal "
                "(reason=%s) while preserving cap latch and repair issue",
                entry_id,
                reason or "unspecified",
            )
            self._fatal_errors.pop(entry_id, None)
            cap_message = self._short_run_cap_messages.get(entry_id)
            if cap_message is not None:
                self._fatal_errors[entry_id] = cap_message
            self._fatal_error = next(iter(self._fatal_errors.values()), None)
            return

        if force:
            self._short_run_cap_latched.discard(entry_id)
            # Iter-16: drop the cap-fire snapshot in lockstep with the latch
            # so a forced teardown also retires the message that the
            # pass-through branch would otherwise restore.
            self._short_run_cap_messages.pop(entry_id, None)

        removed = False
        if entry_id in self._fatal_errors:
            if reason:
                _LOGGER.debug(
                    "[entry=%s] Clearing latched FCM fatal error (%s)", entry_id, reason
                )
            else:
                _LOGGER.debug("[entry=%s] Clearing latched FCM fatal error", entry_id)

            self._fatal_errors.pop(entry_id, None)
            removed = True

        if removed:
            self._fatal_error = next(iter(self._fatal_errors.values()), None)

        self._delete_repair_issues_for_entry(entry_id)

    def _delete_repair_issues_for_entry(self, entry_id: str) -> None:
        """Retire the transient short-run-crash-loop repair for a cleared entry.

        This is the DELETE half of the ``fcm_short_run_crash_loop_{entry_id}``
        CREATE/DELETE pair with the fatal-error map: any path that clears the
        in-memory latch must also delete the user-visible UI artefact, or the
        Repairs panel keeps a stale non-fixable error after the underlying
        condition is gone.

        It deliberately does NOT touch the *fixable* reauth repairs
        (``fcm_reauth_required_{entry_id}`` / legacy ``fcm_stuck_{entry_id}``).
        Those outlive a reload on purpose and are retired only by the supervisor
        success path (reload recovery) or
        ``async_delete_entry_repair_issues`` (actual entry removal). See those
        for the full lifecycle rationale (Codex PR #1104 follow-up).

        The deletion is best-effort UI cleanup; failures are logged and
        swallowed so a registry hiccup never blocks the in-memory state reset.
        """
        if self._hass is None:
            return

        try:
            ir.async_delete_issue(
                self._hass, DOMAIN, f"fcm_short_run_crash_loop_{entry_id}"
            )
        except Exception as cleanup_err:  # noqa: BLE001 - best-effort UI cleanup
            _LOGGER.debug(
                "[entry=%s] Failed to delete short-run crash-loop repair issue (%s at %s)",
                entry_id,
                describe_exception(cleanup_err),
                exception_origin(cleanup_err),
            )

    @staticmethod
    def async_delete_entry_repair_issues(hass: HomeAssistant, entry_id: str) -> None:
        """Delete every entry-scoped FCM Repairs issue for a REMOVED config entry.

        Call this ONLY from ``async_remove_entry`` (actual removal). On removal
        no supervisor will run again, so the entry-scoped repairs must be retired
        explicitly or they stay pinned in the Repairs panel for a config entry
        that no longer exists. This covers the *fixable* reauth prompt
        (``fcm_reauth_required_{entry_id}``), its legacy non-fixable predecessor
        (``fcm_stuck_{entry_id}``) and the transient short-run crash-loop
        diagnostic (``fcm_short_run_crash_loop_{entry_id}``).

        A reload must NOT use this: there ``unregister_coordinator`` fires via
        ``async_on_unload`` while the entry lives on, and the supervisor success
        path clears the reauth repair once FCM re-registers. Deleting it on
        reload would dismiss a still-active prompt before the new supervisor has
        proven recovery (Codex PR #1104 follow-up).

        All deletions are best-effort UI cleanup; failures are swallowed so a
        registry hiccup never blocks entry removal.
        """
        for issue_id in (
            f"fcm_short_run_crash_loop_{entry_id}",
            f"fcm_reauth_required_{entry_id}",
            f"fcm_stuck_{entry_id}",
        ):
            try:
                ir.async_delete_issue(hass, DOMAIN, issue_id)
            except Exception as cleanup_err:  # noqa: BLE001 - best-effort UI cleanup
                _LOGGER.debug(
                    "[entry=%s] Failed to delete repair issue %s on removal (%s at %s)",
                    entry_id,
                    issue_id,
                    describe_exception(cleanup_err),
                    exception_origin(cleanup_err),
                )

    @staticmethod
    def _ensure_cache_entry_id(cache: Any, entry_id: str) -> None:
        """Attach the entry_id to a cache instance when possible."""

        try:
            current = getattr(cache, "entry_id", None)
        except Exception:  # noqa: BLE001 - attribute access guard
            current = None

        if isinstance(current, str):
            normalized = current.strip()
            if normalized and normalized != entry_id:
                _LOGGER.warning(
                    "[entry=%s] Cache instance provided to FCM receiver has mismatched entry_id '%s'; overriding.",
                    entry_id,
                    normalized,
                )
                try:
                    setattr(cache, "entry_id", entry_id)
                except Exception as err:  # noqa: BLE001 - best-effort
                    _LOGGER.debug(
                        "[entry=%s] Failed to override cache entry_id: %s",
                        entry_id,
                        describe_exception(err),
                    )
            elif not normalized:
                try:
                    setattr(cache, "entry_id", entry_id)
                except Exception as err:  # noqa: BLE001 - best-effort
                    _LOGGER.debug(
                        "[entry=%s] Failed to attach entry_id to cache: %s",
                        entry_id,
                        describe_exception(err),
                    )
        else:
            try:
                setattr(cache, "entry_id", entry_id)
            except Exception as err:  # noqa: BLE001 - best-effort
                _LOGGER.debug(
                    "[entry=%s] Failed to tag cache with entry_id: %s",
                    entry_id,
                    describe_exception(err),
                )

    # -------------------- Optional HA attach --------------------

    def attach_hass(self, hass: HomeAssistant) -> None:
        """Optionally attach Home Assistant for owner-index fallback routing."""
        self._hass = hass

    # -------------------- Loop-safe dispatch (P0 fix) --------------------

    def _dispatch_to_hass_loop(
        self, coro: Coroutine[Any, Any, Any], *, label: str
    ) -> None:
        """Dispatch a coroutine into the HA event loop from any thread safely.

        This fixes P0 thread-safety issue: _on_notification may be called from
        a non-event-loop thread by the FCM client. Using asyncio.create_task
        directly would fail with 'no running event loop'.
        """
        try:
            loop = asyncio.get_running_loop()
            # Already in an event loop - schedule directly
            new_task: asyncio.Task[Any] = loop.create_task(
                coro,
                name=f"{DOMAIN}.{label}",
            )
            self._track_task(new_task, label=label)
            return
        except RuntimeError:
            # No running loop in this thread; schedule onto HA loop
            pass

        hass_loop = getattr(self._hass, "loop", None) if self._hass else None
        if hass_loop is None:
            _LOGGER.error("FCM notification dropped: Home Assistant loop not available")
            return

        def _schedule() -> None:
            scheduled_task: asyncio.Task[Any] = hass_loop.create_task(
                coro, name=f"{DOMAIN}.{label}"
            )
            self._track_task(scheduled_task, label=label)

        hass_loop.call_soon_threadsafe(_schedule)

    def _track_task(self, task: asyncio.Task[Any], *, label: str) -> None:
        """Ensure task exceptions are retrieved and logged.

        Prevents 'Task exception was never retrieved' warnings by attaching
        a done callback that logs any exceptions.
        """
        self._active_tasks.add(task)

        def _done(t: asyncio.Task[Any]) -> None:
            self._active_tasks.discard(t)
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                return
            except Exception as result_err:
                _LOGGER.error(
                    "Unhandled exception retrieving task result (%s): %s at %s",
                    label,
                    describe_exception(result_err),
                    exception_origin(result_err),
                )
                return
            if exc:
                _LOGGER.error(
                    "Background task failed (%s): %s at %s",
                    label,
                    describe_exception(exc),
                    exception_origin(exc),
                )

        task.add_done_callback(_done)

    @contextmanager
    def _scoped_cache_provider(
        self, provider: Callable[[], Any]
    ) -> Iterator[contextvars.Token[Callable[[], Any] | None] | None]:
        """Context manager for cache provider with guaranteed cleanup (P1-2 fix).

        Uses only contextvars (not global state) to prevent cross-contamination
        between concurrent async operations from different config entries.
        """
        token: contextvars.Token[Callable[[], Any] | None] | None = None
        try:
            token = _CACHE_PROVIDER.set(provider)
            yield token
        finally:
            if token is not None:
                try:
                    _CACHE_PROVIDER.reset(token)
                except Exception as err:  # noqa: BLE001
                    _LOGGER.debug(
                        "Cache provider reset failed: %s", describe_exception(err)
                    )

    @contextmanager
    def _driving_generation_scope(
        self, entry_id: str, generation: int | None
    ) -> Iterator[None]:
        """Publish the driving supervisor generation for one FCM handshake.

        ``firebase_messaging`` fires ``credentials_updated_callback``
        synchronously from inside the awaited ``checkin_or_register`` /
        ``reregister_keeping_identity`` (same task). The callback closure is
        shared across a fast reload, so it gates on this published generation
        instead of a stale build-time snapshot. See ``_FCM_DRIVING_GENERATION``
        for the full rationale (finding 4c). Reset in ``finally`` so a nested or
        sibling handshake never inherits a leaked value.
        """
        token = _FCM_DRIVING_GENERATION.set((entry_id, generation))
        try:
            yield
        finally:
            _FCM_DRIVING_GENERATION.reset(token)

    def _monotonic_from_wall_time(
        self, wall_time: float, monotonic_now: float
    ) -> float | None:
        """Convert a wall clock timestamp to the monotonic clock domain."""

        if wall_time <= 0:
            return None

        try:
            wall_now = time.time()
            offset = monotonic_now - wall_now
            return wall_time + offset
        except Exception:  # pragma: no cover - defensive conversion
            return None

    def _update_last_activity_for_entry(
        self, entry_id: str, pc: Any, monotonic_now: float
    ) -> float | None:
        """Refresh and return the last activity timestamp for an entry."""

        last_message_time = getattr(pc, "last_message_time", None)
        last_activity = None
        if isinstance(last_message_time, (int, float)):
            last_activity = self._monotonic_from_wall_time(
                float(last_message_time), monotonic_now
            )

        if last_activity is None:
            last_activity = self._entry_last_activity_monotonic.get(entry_id)

        if last_activity is not None:
            self._entry_last_activity_monotonic[entry_id] = last_activity

        return last_activity

    def get_health_snapshots(self) -> dict[str, dict[str, Any]]:
        """Return per-entry health snapshots for diagnostics/state reporting."""

        now = time.monotonic()
        stale_after = max(self._activity_stale_after_s, 0.0)

        snapshots: dict[str, dict[str, Any]] = {}
        for entry_id, pc in self.pcs.items():
            supervisor = self.supervisors.get(entry_id)
            supervisor_running = supervisor is not None and not supervisor.done()
            run_state = getattr(pc, "run_state", None)
            do_listen = bool(getattr(pc, "do_listen", False))

            last_activity = self._entry_last_activity_monotonic.get(entry_id)
            activity_age = None
            if last_activity is not None:
                activity_age = max(now - last_activity, 0.0)

            if FcmPushClientRunState is not None:
                client_ready = run_state == FcmPushClientRunState.STARTED and do_listen
            else:
                client_ready = do_listen

            fresh_activity = last_activity is not None and (
                stale_after == 0.0
                or (activity_age is not None and activity_age <= stale_after)
            )

            snapshots[entry_id] = {
                "supervisor_running": supervisor_running,
                "client_ready": client_ready,
                "run_state": getattr(run_state, "name", run_state),
                "do_listen": do_listen,
                "last_activity_monotonic": last_activity,
                "seconds_since_last_activity": activity_age,
                "activity_stale": not fresh_activity,
                "healthy": supervisor_running and client_ready and fresh_activity,
            }

        return snapshots

    def _update_entry_health(self, entry_id: str, healthy: bool) -> None:
        """Track entry health transitions and notify coordinators."""

        previous = self._entry_health.get(entry_id)
        if previous is healthy:
            return

        self._entry_health[entry_id] = healthy
        if healthy:
            self._entry_last_connected_wall[entry_id] = time.time()

        for coordinator in list(self.coordinators):
            entry = getattr(coordinator, "config_entry", None)
            if getattr(entry, "entry_id", None) != entry_id:
                continue

            try:
                update_listeners = getattr(coordinator, "async_update_listeners", None)
                if callable(update_listeners):
                    update_listeners()
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "[entry=%s] Coordinator listener notification failed: %s",
                    entry_id,
                    describe_exception(err),
                )

    # -------------------- Basic readiness (aggregate) --------------------

    @property
    def is_ready(self) -> bool:
        """True if at least one client is started and listening."""
        snapshots = self.get_health_snapshots()
        return any(snap.get("healthy") for snap in snapshots.values())

    ready = is_ready  # alias used by callers

    def nudge_retry(self, entry_id: str | None = None) -> bool:
        """Wake a sleeping supervisor so it retries FCM connection immediately.

        Called by the polling coordinator when it enters degraded mode and wants
        the supervisor to attempt reconnection sooner than the backoff timer.
        Returns True if a nudge event was set.
        """
        key = entry_id or self._default_entry_id
        if not key:
            # Nudge all entries
            nudged = False
            for evt in self._retry_nudge_evts.values():
                evt.set()
                nudged = True
            return nudged
        nudge_evt = self._retry_nudge_evts.get(key)
        if nudge_evt is not None:
            nudge_evt.set()
            return True
        return False

    def get_last_connected_wall_time(self, entry_id: str | None) -> float | None:
        """Return the last wall-clock timestamp when the entry marked healthy."""

        entry_key = entry_id if entry_id is not None else self._default_entry_id
        if entry_key is None:
            return None

        ts = self._entry_last_connected_wall.get(entry_key)
        return float(ts) if ts is not None else None

    # -------------------- Lifecycle --------------------

    async def _prime_cache_state(self, entry_id: str, cache: TokenCache) -> None:
        """Load entry-scoped credentials and routing tokens before startup."""

        try:
            creds_val = await cache.get("fcm_credentials")
            if isinstance(creds_val, str):
                creds_val = json.loads(creds_val)
            if isinstance(creds_val, MutableMapping):
                self.creds[entry_id] = creds_val
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "[entry=%s] Failed to load the cached FCM registration: %s",
                entry_id,
                describe_exception(err),
            )

        try:
            tokens_val = await cache.get("fcm_routing_tokens")
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "[entry=%s] Failed to load cached routing entries: %s",
                entry_id,
                describe_exception(err),
            )
            return

        if isinstance(tokens_val, (list, tuple, set)):
            tokens = {t for t in tokens_val if isinstance(t, str) and t}
            if tokens:
                entry_tokens = self._entry_to_tokens.setdefault(entry_id, set())
                entry_tokens.update(tokens)
                for token in tokens:
                    mapped_entries = self._token_to_entries.setdefault(token, set())
                    mapped_entries.add(entry_id)

    async def async_initialize(
        self, *, entry_id: str | None = None, cache: TokenCache | None = None
    ) -> bool:
        """Initialize receiver (idempotent). Defers client creation to coordinator registration."""
        # Prepare shared client config once
        if self._client_cfg is None and HAVE_FCM_PUSH_CLIENT:
            self._client_cfg = FcmPushClientConfig(
                client_heartbeat_interval=int(FCM_CLIENT_HEARTBEAT_INTERVAL_S),
                server_heartbeat_interval=int(FCM_SERVER_HEARTBEAT_INTERVAL_S),
                idle_reset_after=float(FCM_IDLE_RESET_AFTER_S),
                connection_retry_count=int(FCM_CONNECTION_RETRY_COUNT),
                monitor_interval=float(FCM_MONITOR_INTERVAL_S),
                abort_on_sequential_error_count=int(FCM_ABORT_ON_SEQ_ERROR_COUNT),
            )

        if entry_id and cache is None:
            try:
                from . import token_cache as token_cache_module  # noqa: PLC0415

                cache = token_cache_module.get_cache_for_entry(entry_id)
            except Exception:  # pragma: no cover - best-effort fallback
                cache = None

        if entry_id and cache is not None:
            self._ensure_cache_entry_id(cache, entry_id)
            self._entry_caches[entry_id] = cache
            await self._prime_cache_state(entry_id, cache)

        if entry_id is not None and self._default_entry_id is None:
            self._default_entry_id = entry_id

        _LOGGER.info("FCM receiver initialized (multi-client ready)")
        return True

    async def _load_entry_creds_for_build(
        self, entry_id: str, cache: TokenCache | None
    ) -> tuple[MutableJSONMapping | None, bool]:
        """Resolve entry creds for a client build (the sole suspension point).

        Returns ``(creds, loaded_from_cache)``. ``creds`` falls back to the
        in-memory map, then the pending-creds buffer, then is overridden by a
        successful awaited cache read. ``loaded_from_cache`` is True only when
        that read produced a fresh credentials dict, so the caller knows it may
        pop the pending-creds entry as part of its guarded commit.

        The ``await`` here is the only suspension point in
        ``_ensure_client_for_entry``'s critical section; the caller MUST
        re-validate ``_entry_writes_suppressed`` after this returns, because a
        synchronous teardown can land while this coroutine is parked on it.
        """
        creds = self.creds.get(entry_id)
        if creds is None:
            pending = self._pending_creds.get(entry_id)
            if isinstance(pending, dict):
                creds = pending
        loaded_from_cache = False
        try:
            if cache is not None:
                val = await cache.get("fcm_credentials")
                if isinstance(val, str):
                    val = json.loads(val)
                if isinstance(val, dict):
                    creds = val
                    loaded_from_cache = True
        except Exception as err:  # noqa: BLE001 - best-effort creds load
            _LOGGER.debug(
                "Failed to load entry-scoped FCM creds for %s: %s",
                entry_id,
                describe_exception(err),
            )
        return creds, loaded_from_cache

    async def _ensure_client_for_entry(
        self, entry_id: str, cache: TokenCache | None, generation: int | None = None
    ) -> FcmPushClient[Any] | None:
        """Create or return the FCM client for the given entry (idempotent).

        ``generation`` is the supervisor's captured generation snapshot (or
        ``None`` for live callers such as manual-locate registration). It gates
        the *build* path through ``_entry_writes_suppressed`` (finding 5).
        """
        if cache is not None:
            self._ensure_cache_entry_id(cache, entry_id)
        async with self._lock:
            # AP6 (finding 5 + finding 6): gate BEFORE the existing-client fast
            # path. A tombstoned entry or a superseded supervisor generation
            # must never receive a client handle -- not even an already-built
            # one. Two races collapse into this single chokepoint:
            #
            #   * finding 5 (build path): ``register_coordinator`` dispatches the
            #     supervisor asynchronously; if ``unregister_coordinator``
            #     tombstones the entry before that queued coroutine runs
            #     (unload-before-dispatch), the build below would revive a
            #     torn-down entry's ``self.pcs``/``self.creds``.
            #   * finding 6 (read path): after a fast reload the new incarnation
            #     installs a fresh client under ``entry_id``; a stale supervisor
            #     (old generation) then calls in here. If the existing-client
            #     early return ran first it would hand that stale supervisor the
            #     LIVE client, which it later stops + discards on its own
            #     suppression branch. ``_discard_own_client`` compares by
            #     identity, but the handle IS the live client, so the identity
            #     guard cannot save it -- the new incarnation loses its client.
            #
            # The generation guard in ``_register_for_fcm_entry`` fires one step
            # too late for both. Gating first closes the build path AND the read
            # path here. The supervisor loop treats this None as terminal (it
            # built no own client, so there is nothing to clean up).
            if self._entry_writes_suppressed(entry_id, generation):
                _LOGGER.debug(
                    "[entry=%s] Skipping FCM client handover for suppressed entry "
                    "(tombstoned or superseded generation)",
                    entry_id,
                )
                return None

            # Idempotent read: an already-built client for a live, current
            # generation is returned as-is (a read, not a re-creation).
            if entry_id in self.pcs:
                return self.pcs[entry_id]

            # Resolve entry creds at the sole suspension point (the awaited cache
            # read). Extracted into a named seam so this build chokepoint stays
            # within its branch budget and the await is isolated; the store into
            # ``self.creds[entry_id]`` is deferred to the single guarded commit
            # below, because writing entry-state mid-method would let a teardown
            # landing during the await re-create exactly what it just purged.
            creds, loaded_from_cache = await self._load_entry_creds_for_build(
                entry_id, cache
            )

            # AP7 (finding 7): re-validate suppression AFTER the await, BEFORE any
            # per-entry store. ``self._lock`` is an asyncio lock -- it serialises
            # other coroutines but does NOT exclude the SYNCHRONOUS
            # ``unregister_coordinator`` (``async_on_unload``), which takes no
            # lock. The ``await cache.get(...)`` above is the sole suspension
            # point in this critical section; while parked there a teardown can
            # tombstone the entry and ``_purge_entry_tokens`` it, so the first
            # gate's verdict is stale on resume. Committing the stores below now
            # would resurrect the purged ``self.pcs``/``self.creds`` -- the
            # TOCTOU-over-await sibling of the fast-path race the first gate
            # closes. Drop the handover; live callers treat None gracefully and a
            # superseded supervisor terminates on it.
            if self._entry_writes_suppressed(entry_id, generation):
                _LOGGER.debug(
                    "[entry=%s] Dropping FCM client handover after creds load; "
                    "entry was suppressed (tombstoned or superseded) during the "
                    "cache await",
                    entry_id,
                )
                return None

            # Build register config (shared across entries)
            if not HAVE_FCM_PUSH_CLIENT:
                _LOGGER.error("FCM client support not available; cannot create client")
                return None

            fcm_config = FcmRegisterConfig(
                project_id=self.project_id,
                app_id=self.app_id,
                api_key=self.api_key,
                messaging_sender_id=self.message_sender_id,
                bundle_id="com.google.android.apps.adm",
                android_cert_sha1="38918a453d07199354f8b19af05ec6562ced5788",
            )

            # Per-entry credentials update callback
            def _on_creds_updated_entry(updated: MutableJSONMapping) -> None:
                self._on_credentials_updated_for_entry(entry_id, updated)

            http_client_session = None
            if self._hass is not None:
                http_client_session = async_get_clientsession(self._hass)

            try:
                credential_callback = cast(
                    "CredentialsUpdatedCallable[MutableJSONMapping]",
                    _on_creds_updated_entry,
                )
                pc = _FcmPushClientFactory(
                    lambda payload, persistent_id, context, eid=entry_id: (
                        self._on_notification(eid, payload, persistent_id, context)
                    ),
                    fcm_config,
                    creds,
                    credential_callback,
                    http_client_session=http_client_session,
                    config=self._client_cfg,
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.error(
                    "Failed to construct FCM client for %s: %s",
                    entry_id,
                    describe_exception(err),
                )
                return None

            # Single guarded commit: every per-entry store happens here, after
            # the post-await re-validation -- never mid-method. The pending-creds
            # pop is part of the commit because it only applies when the cache
            # load above produced fresh credentials.
            self.pcs[entry_id] = pc
            self.creds[entry_id] = creds if isinstance(creds, dict) else None
            if loaded_from_cache:
                self._pending_creds.pop(entry_id, None)
            return pc

    def _discard_own_client(self, entry_id: str, pc: FcmPushClient[Any]) -> None:
        """Remove *pc* from ``self.pcs`` only if it is still the live client.

        A supervisor cleans up the client it built by popping ``self.pcs``
        under ``self._lock``. A key-only ``pop(entry_id)`` is identity-blind:
        when a fast reload bumps the generation and the new incarnation has
        already installed a fresh client under the same ``entry_id`` (via
        ``_ensure_client_for_entry``), a stale supervisor reaching its cleanup
        would evict the LIVE client, leaving the new generation without an FCM
        client until its next restart. Comparing by identity ensures each
        supervisor only discards the client it owns; a superseded supervisor
        leaves the live client untouched.

        Caller must hold ``self._lock``.
        """
        if self.pcs.get(entry_id) is pc:
            self.pcs.pop(entry_id, None)

    def _async_raise_reauth_issue(self, entry_id: str, *, reason: str) -> None:
        """Surface a fixable repair that drives the user into the reauth flow.

        All three terminal give-up points collapse into a single issue id
        (``fcm_reauth_required_{entry_id}``) because the corrective action is
        identical: re-authenticate / re-register. The fix flow in
        ``repairs.py`` calls ``entry.async_start_reauth`` on confirmation.

        The escalation thresholds are NOT re-invented here. They are the
        existing retry caps consumed at the call sites:

        * ``_MAX_FATAL_ENDPOINT_RETRIES`` (7) -- with the ``min(300, 30*n)``
          backoff this is ~630 s of sustained 404 endpoint exhaustion before
          give-up; a transient Google endpoint rotation clears well before it.
        * ``_MAX_FATAL_AUTH_RETRIES`` (3) -- 401 token-renewal exhaustion.
        * ``_BACKOFF_WARNING_THRESHOLD_S`` (64 s, ~6 failed registrations) --
          the registration backoff warning point.

        Because the repair hangs off those existing give-up/warning points, a
        transient failure (count still below the cap) never raises it. The
        recovery delete on a successful (re-)registration clears it again.

        ``reason`` is a non-localised diagnostic hint forwarded via ``data``
        so the fix flow / diagnostics can tell the three origins apart; the
        fix flow itself only consumes ``entry_id``.
        """
        if not self._hass:
            return
        ir.async_create_issue(
            self._hass,
            DOMAIN,
            f"fcm_reauth_required_{entry_id}",
            is_fixable=True,
            severity=ir.IssueSeverity.ERROR,
            translation_key="fcm_reauth_required",
            data={"entry_id": entry_id, "reason": reason},
        )

    async def _start_supervisor_for_entry(  # noqa: PLR0915
        self, entry_id: str, cache: TokenCache | None
    ) -> None:
        """Start the supervisor loop for the given entry if not running."""
        if entry_id in self.supervisors and not self.supervisors[entry_id].done():
            return

        # AP4 (finding 4b): capture the generation current at start. Every
        # per-entry write below is gated on this snapshot, so once a teardown
        # bumps the generation this (now superseded) supervisor stops writing
        # even after ``register_coordinator`` clears the boolean tombstone for
        # the next incarnation.
        generation = self._entry_generation.get(entry_id, 0)

        # AP5 (finding 4b): a prior stop (request_stop on unload, or the cap
        # teardown) leaves the entry's event *set* without installing a fresh
        # one. The old ``setdefault`` then handed that set event to the new
        # supervisor, whose ``while not stop_evt.is_set()`` exited immediately
        # (wedged restart after a reload). Replace the event only when it is
        # missing or already set; a still-unset event is kept so a deliberately
        # pre-installed one (and the still-running old supervisor, which holds
        # its own reference) keeps working.
        stop_evt = self._stop_evts.get(entry_id)
        if stop_evt is None or stop_evt.is_set():
            stop_evt = asyncio.Event()
            self._stop_evts[entry_id] = stop_evt

        async def _supervisor() -> None:  # noqa: PLR0912, PLR0915
            nudge_evt = self._retry_nudge_evts.setdefault(entry_id, asyncio.Event())

            async def _nudge_sleep(seconds: float) -> bool:
                """Sleep up to *seconds*, returning True if nudged early."""
                nudge_evt.clear()
                try:
                    await asyncio.wait_for(nudge_evt.wait(), timeout=seconds)
                    return True  # nudged
                except TimeoutError:
                    return False

            backoff = 1.0
            # Defense 2: per-supervisor closure state for the short-run
            # crash cap. Kept closure-local (not on ``self``) so a
            # re-registration that cancels this supervisor and spawns a
            # new one cannot inherit a stale counter (CA-Audit B1).
            short_run_counter: int = 0
            entry_start: float = 0.0
            try:
                while not stop_evt.is_set():
                    pc = await self._ensure_client_for_entry(
                        entry_id, cache, generation
                    )
                    if not pc:
                        # finding 6: the build chokepoint now returns None for a
                        # tombstoned entry or a superseded generation BEFORE
                        # handing out any client. That is terminal for this
                        # supervisor -- it must stop, not spin: no own client was
                        # built (nothing to clean up) and the outer finally is
                        # suppression-aware. A None from a transient build
                        # failure (push-client support absent, factory error) is
                        # NOT suppression and keeps retrying with backoff.
                        if self._entry_writes_suppressed(entry_id, generation):
                            break
                        nudged = await _nudge_sleep(backoff)
                        if nudged:
                            backoff = 1.0
                        else:
                            backoff = min(backoff * 2, _MAX_SUPERVISOR_BACKOFF_S)
                        continue

                    try:
                        ok_reg = await self._register_for_fcm_entry(
                            entry_id, generation
                        )
                    except FatalRegistrationError as err:
                        message = str(err) or "FCM registration failed"
                        # Publish to ``_fatal_errors`` only once the retry
                        # budget is exhausted (see ``break`` paths below);
                        # otherwise the coordinator's 401-substring match
                        # in ``coordinator/helpers/update.py`` escalates
                        # to ``ConfigEntryAuthFailed`` before this loop
                        # gets a chance to renew tokens.
                        _LOGGER.error(
                            "[entry=%s] FCM registration failed: %s",
                            entry_id,
                            message,
                        )
                        try:
                            await pc.stop()
                        except Exception:
                            pass
                        finally:
                            async with self._lock:
                                self._discard_own_client(entry_id, pc)

                        is_auth = getattr(err, "is_auth_error", False)
                        counter_key = (
                            f"{entry_id}:auth" if is_auth else f"{entry_id}:endpoint"
                        )
                        fatal_count = self._fatal_retry_counts.get(counter_key, 0) + 1
                        # AP4 race guard: a teardown or a supersede may have
                        # purged/re-owned this entry mid-flight. A suppressed
                        # supervisor is stale for the ENTIRE retry-budget
                        # handling, not just the counter write below: the give-up
                        # branches publish ``_fatal_error`` / ``_fatal_errors``
                        # and raise the reauth repair, and ``fatal_count`` is read
                        # from the shared ``_fatal_retry_counts`` map the LIVE
                        # generation may already own after a fast reload. Letting
                        # a stale generation reach the give-up branches would
                        # surface a reauth/fatal error against the live entry.
                        # Bail out entirely; the supervisor terminates via the
                        # surrounding loop's finally cleanup (the failed client
                        # was already stopped and popped above).
                        if self._entry_writes_suppressed(entry_id, generation):
                            break
                        self._fatal_retry_counts[counter_key] = fatal_count

                        if is_auth:
                            # 401: Token renewal while preserving android_id
                            max_retries = _MAX_FATAL_AUTH_RETRIES
                            if fatal_count >= max_retries:
                                self._fatal_error = message
                                self._fatal_errors[entry_id] = message
                                _LOGGER.error(
                                    "[entry=%s] FCM auth failed %d "
                                    "consecutive times; giving up. "
                                    "Re-authentication may be required.",
                                    entry_id,
                                    fatal_count,
                                )
                                # Auth retry budget exhausted
                                # (``_MAX_FATAL_AUTH_RETRIES``): offer the
                                # fixable reauth repair instead of giving up
                                # silently.
                                self._async_raise_reauth_issue(
                                    entry_id, reason="auth_401"
                                )
                                break

                            await self._invalidate_fcm_tokens(entry_id, generation)

                            delay = 60.0 * fatal_count
                            _LOGGER.warning(
                                "[entry=%s] FCM auth error 401 (%d/%d); "
                                "renewing tokens (android_id preserved), "
                                "retry in %.0fs",
                                entry_id,
                                fatal_count,
                                max_retries,
                                delay,
                            )
                            await asyncio.sleep(delay)
                            continue
                        else:
                            # 404: Endpoint rotation — just retry
                            max_retries = _MAX_FATAL_ENDPOINT_RETRIES
                            if fatal_count >= max_retries:
                                self._fatal_error = message
                                self._fatal_errors[entry_id] = message
                                _LOGGER.error(
                                    "[entry=%s] FCM endpoint 404 "
                                    "persisted for %d attempts; "
                                    "giving up.",
                                    entry_id,
                                    fatal_count,
                                )
                                # Endpoint retry budget exhausted
                                # (``_MAX_FATAL_ENDPOINT_RETRIES`` == 7 ⇒
                                # ~630 s of sustained 404 with the
                                # ``min(300, 30*n)`` backoff): surface the
                                # fixable reauth repair. This give-up was
                                # previously a silent break with no issue.
                                self._async_raise_reauth_issue(
                                    entry_id, reason="endpoint_404"
                                )
                                break

                            delay = min(300.0, 30.0 * fatal_count)
                            _LOGGER.warning(
                                "[entry=%s] FCM endpoint 404 (%d/%d); "
                                "retrying in %.0fs (likely Google "
                                "endpoint rotation)",
                                entry_id,
                                fatal_count,
                                max_retries,
                                delay,
                            )
                            await asyncio.sleep(delay)
                            continue

                    # AP4 (finding: stale registration success at the caller).
                    # ``_register_for_fcm_entry`` guards its own per-entry writes,
                    # but its boolean return cannot tell this caller that the
                    # generation was superseded (or the entry tombstoned)
                    # mid-flight. Without this guard a stale supervisor would run
                    # the success path below -- deleting the LIVE generation's
                    # reauth repair issue, bumping start metrics and
                    # (re)starting + monitoring the stale client -- or, on a
                    # transient ``ok_reg`` False, burn the shared retry budget for
                    # an entry it no longer owns. Returning False instead would be
                    # wrong: it routes into the backoff/retry path, but a
                    # superseded generation must terminate, not retry. Bail out
                    # like the fatal-retry break above; the surrounding finally
                    # skips the stale health write under the same suppression
                    # guard.
                    if self._entry_writes_suppressed(entry_id, generation):
                        try:
                            await pc.stop()
                        except Exception:
                            pass
                        finally:
                            async with self._lock:
                                self._discard_own_client(entry_id, pc)
                        break

                    if not ok_reg:
                        try:
                            await pc.stop()
                        except Exception:
                            pass
                        finally:
                            async with self._lock:
                                self._discard_own_client(entry_id, pc)
                        delay = backoff + random.uniform(0.1, 0.3) * backoff  # nosec B311
                        if backoff >= _BACKOFF_WARNING_THRESHOLD_S:
                            _LOGGER.warning(
                                "[entry=%s] FCM registration still failing after multiple attempts (next retry in %.1fs). "
                                "Check Firewall (Port 5228) or Credentials.",
                                entry_id,
                                delay,
                            )
                            # Registration backoff warning point
                            # (``_BACKOFF_WARNING_THRESHOLD_S``): escalate to
                            # the same fixable reauth repair as the 404/401
                            # give-up paths (was a non-fixable ``fcm_stuck``
                            # warning offering no action path).
                            self._async_raise_reauth_issue(
                                entry_id, reason="registration_backoff"
                            )
                        else:
                            _LOGGER.info(
                                "[entry=%s] Re-trying FCM registration in %.1fs",
                                entry_id,
                                delay,
                            )
                        nudged = await _nudge_sleep(delay)
                        if nudged:
                            backoff = 1.0
                        else:
                            backoff = min(backoff * 2, _MAX_SUPERVISOR_BACKOFF_S)
                        continue

                    # Telemetry (aggregate counters)
                    if self._hass:
                        # A successful (re-)registration resolves the fixable
                        # reauth repair. Also delete the legacy non-fixable
                        # ``fcm_stuck`` issue so it does not linger after an
                        # upgrade from the previous key.
                        ir.async_delete_issue(
                            self._hass,
                            DOMAIN,
                            f"fcm_reauth_required_{entry_id}",
                        )
                        ir.async_delete_issue(
                            self._hass, DOMAIN, f"fcm_stuck_{entry_id}"
                        )
                    self.last_start_monotonic = time.monotonic()
                    self.start_count += 1

                    try:
                        await pc.start()
                        _LOGGER.debug(
                            "[entry=%s] FCM client started; entering monitor loop",
                            entry_id,
                        )
                        self._update_last_activity_for_entry(
                            entry_id, pc, time.monotonic()
                        )
                        backoff = 1.0  # reset only after a successful start
                    except Exception as err:
                        _LOGGER.info(
                            "[entry=%s] FCM client failed to start: %s",
                            entry_id,
                            describe_exception(err),
                        )

                    # Defense 2 snapshot: timestamp the entry into the
                    # monitor loop.
                    #
                    # Iter-11 (Codex follow-up): per-run latch tracking
                    # whether the client ever reached a connected/listening
                    # state. The cap is meant to detect a poison-message
                    # crash loop (client connects, decodes, dies, repeat),
                    # NOT ordinary connectivity outages where ``_listen()``
                    # exhausts its 5 connection retries (~15-20s) without
                    # ever reaching ``STARTED``. Without this gate, a normal
                    # MCS-unreachable / Google outage would cap the
                    # supervisor after 10 brief connection-failure runs and
                    # permanently disable FCM push until reload.
                    entry_start = time.monotonic()
                    became_started = False

                    while not stop_evt.is_set():
                        await asyncio.sleep(max(FCM_MONITOR_INTERVAL_S, 0.5))
                        state = getattr(pc, "run_state", None)
                        do_listen = getattr(pc, "do_listen", False)
                        monotonic_now = time.monotonic()
                        # AP4 (finding 4b, related sweep): a superseded or
                        # tombstoned supervisor must not refresh per-entry
                        # activity/health here. Those writes resurrect exactly
                        # the snapshots ``_purge_entry_tokens`` dropped -- a stale
                        # ``_entry_health`` makes the first healthy transition of
                        # the next incarnation look like a no-change and
                        # suppresses its listener push (the AP2 class named in
                        # finding 2b). Skip the writes while suppressed; the
                        # restart decision below still uses ``state`` /
                        # ``do_listen``.
                        writes_suppressed = self._entry_writes_suppressed(
                            entry_id, generation
                        )
                        last_activity = (
                            None
                            if writes_suppressed
                            else self._update_last_activity_for_entry(
                                entry_id, pc, monotonic_now
                            )
                        )
                        stale_after = max(self._activity_stale_after_s, 0.0)
                        # Iter-11: track ``became_started`` symmetrically to
                        # the health probe (same fallback for libraries that
                        # do not expose a ``run_state`` enum).
                        if FcmPushClientRunState is None:
                            if do_listen:
                                became_started = True
                        elif state == FcmPushClientRunState.STARTED:
                            became_started = True
                        # Iter-12 (Codex follow-up): sampling-gap defense.
                        # When the full lifecycle ``CREATED -> STARTED ->
                        # CRASH`` happens between two monitor polls (>=0.5s),
                        # this loop reads STOPPED/!do_listen and ``became_started``
                        # would remain false. The vendored client's
                        # ``__setattr__`` hook latches ``_has_been_started``
                        # the moment the library assigns ``run_state =
                        # STARTED``, independent of polling cadence; mirror
                        # that observation here so the cap branch correctly
                        # classifies micro-window poison-message crashes.
                        became_started = became_started or bool(
                            getattr(pc, "_has_been_started", False)
                        )
                        healthy = (
                            (
                                FcmPushClientRunState is None
                                or state == FcmPushClientRunState.STARTED
                            )
                            and do_listen
                            and last_activity is not None
                            and (
                                stale_after == 0.0
                                or (monotonic_now - last_activity <= stale_after)
                            )
                        )
                        if not writes_suppressed:
                            self._update_entry_health(entry_id, healthy)
                            # Data-starvation liveness watchdog (AP4). Evaluated
                            # only when writes are not suppressed (never against a
                            # superseded/tombstoned generation), and throttled to
                            # FCM_ZOMBIE_CHECK_INTERVAL_S rather than every 1s
                            # tick. Detection + scheduling only -- the reconnect
                            # runs as its own task, so the loop is never blocked
                            # (no ``break``, no inline ``await`` on the reconnect).
                            next_eval = self._zombie_next_eval_monotonic.get(entry_id)
                            if next_eval is None or monotonic_now >= next_eval:
                                self._zombie_next_eval_monotonic[entry_id] = (
                                    monotonic_now + FCM_ZOMBIE_CHECK_INTERVAL_S
                                )
                                self._maybe_reconnect_starved(entry_id, monotonic_now)
                        if state is None:
                            _LOGGER.info(
                                "[entry=%s] FCM client state unknown; scheduling restart",
                                entry_id,
                            )
                            break
                        if (
                            FcmPushClientRunState is not None
                            and state
                            in (
                                FcmPushClientRunState.STOPPING,
                                FcmPushClientRunState.STOPPED,
                            )
                        ) or not do_listen:
                            _LOGGER.info(
                                "[entry=%s] FCM client stopped; scheduling restart",
                                entry_id,
                            )
                            break

                        if last_activity is None:
                            _LOGGER.info(
                                "[entry=%s] FCM client has no activity timestamp; scheduling restart",
                                entry_id,
                            )
                            break

                        if (
                            stale_after > 0.0
                            and monotonic_now - last_activity > stale_after
                        ):
                            _LOGGER.info(
                                "[entry=%s] FCM client activity stale (age=%.1fs); scheduling restart",
                                entry_id,
                                monotonic_now - last_activity,
                            )
                            break

                    # Defense 2: short-run crash cap. Counts consecutive
                    # monitor exits with ``run_duration <
                    # _SHORT_RUN_THRESHOLD_S`` THAT ALSO reached
                    # ``STARTED`` at least once during the run. After
                    # ``_MAX_CONSECUTIVE_SHORT_RUNS`` such runs, surface a
                    # non-fixable repair issue and stop the loop. The
                    # comparison is strictly ``<`` (a 30.0s run counts as
                    # healthy and resets the counter).
                    #
                    # Iter-11 (Codex follow-up): the ``became_started`` gate
                    # ensures the cap is specific to the poison-message
                    # scenario (client connects, decodes, dies, repeat) and
                    # excludes pre-start connection/login failures. A burst
                    # of MCS-unreachable failures (5 retries x ~3-4s each)
                    # would otherwise be counted as short-run crashes and
                    # permanently disable FCM push after one ordinary
                    # network outage. Pre-start failures leave the counter
                    # AND the latch UNTOUCHED so that (a) a poison-message
                    # cascade interleaved with outages still accumulates
                    # correctly, and (b) a long pre-start hang does not
                    # falsely clear a real cap latch (no healthy proof).
                    run_duration = time.monotonic() - entry_start
                    credential_error = getattr(pc, "credential_error", None)
                    if credential_error is not None:
                        # Finding 1 (Codex): the listener surfaced corrupt FCM
                        # key material via ``credential_error``. Restarting
                        # against the same credentials would loop until the
                        # short-run crash cap fires; instead invalidate the key
                        # material so the next registration regenerates it
                        # (a valid GCM identity is preserved, a corrupt one
                        # dropped) and do NOT
                        # charge this run to the crash cap -- corrective action
                        # is being taken, this is not a poison-message loop.
                        _LOGGER.error(
                            "[entry=%s] FCM registration data corrupt (%s); "
                            "invalidating FCM tokens to force re-registration",
                            entry_id,
                            credential_error,
                        )
                        await self._invalidate_fcm_tokens(entry_id, generation)
                        short_run_counter = 0
                    elif not became_started:
                        # Pre-start failure (no connected/listening state
                        # observed during this run). Do NOT touch the
                        # short-run counter or the cap latch.
                        pass
                    elif run_duration < _SHORT_RUN_THRESHOLD_S:
                        short_run_counter += 1
                        # Test hook: expose the closure counter to stubs
                        # without leaking it onto ``self``.
                        observe = getattr(pc, "_observe_counter", None)
                        if callable(observe):
                            try:
                                observe(short_run_counter)
                            except Exception:  # noqa: BLE001
                                pass
                        if short_run_counter >= _MAX_CONSECUTIVE_SHORT_RUNS:
                            message = (
                                f"{CRASH_LOOP_FATAL_PREFIX} "
                                f"{_MAX_CONSECUTIVE_SHORT_RUNS} consecutive "
                                f"runs ended within "
                                f"{_SHORT_RUN_THRESHOLD_S:.0f}s "
                                f"(persistent poison message suspected)"
                            )
                            _LOGGER.error("[entry=%s] %s", entry_id, message)
                            # AP4 race guard: a teardown may have purged this
                            # entry while the monitor loop was running. Skip the
                            # cap latch / fatal map / Repairs issue so they are
                            # not resurrected for a dead entry; the terminal
                            # break below still stops this supervisor.
                            if not self._entry_writes_suppressed(entry_id, generation):
                                self._fatal_errors[entry_id] = message
                                # Defense 2 iter-8: latch the cap state. While
                                # this latch is set,
                                # ``_clear_fatal_error_for_entry`` is a no-op
                                # (registration / credential-update paths cannot
                                # clear the fatal map or the Repairs issue before
                                # a real healthy supervisor run proves the poison
                                # message is gone).
                                self._short_run_cap_latched.add(entry_id)
                                # Iter-16 (Codex follow-up): snapshot the
                                # cap-fire message so the pass-through clear
                                # branch in ``_clear_fatal_error_for_entry`` can
                                # restore it after a non-cap fatal overwrote
                                # ``_fatal_errors[entry_id]``. Released in the
                                # same two places that release the latch.
                                self._short_run_cap_messages[entry_id] = message
                                if self._hass:
                                    ir.async_create_issue(
                                        self._hass,
                                        DOMAIN,
                                        f"fcm_short_run_crash_loop_{entry_id}",
                                        is_fixable=False,
                                        severity=ir.IssueSeverity.ERROR,
                                        translation_key="fcm_short_run_crash_loop",
                                    )
                            # Cleanup the client before breaking out so
                            # the loop exit doesn't leave a half-stopped pc.
                            try:
                                await pc.stop()
                            except Exception:  # noqa: BLE001
                                pass
                            finally:
                                async with self._lock:
                                    self._discard_own_client(entry_id, pc)
                                # AP4 (finding 4b, related sweep): suppress the
                                # stale-health write for a superseded/tombstoned
                                # supervisor (resurrection class, finding 2b).
                                if not self._entry_writes_suppressed(
                                    entry_id, generation
                                ):
                                    self._update_entry_health(entry_id, False)
                            # Defense 2 iter-9 (Codex follow-up): the cap is
                            # a TERMINAL state for this supervisor. ``break``
                            # alone only exits the inner monitor loop and
                            # falls through to the per-iteration restart
                            # block (``if not stop_evt.is_set(): ...
                            # _nudge_sleep(delay)``), which would spawn a
                            # fresh FCM client and continue the retry
                            # pressure / log spam the cap is meant to stop.
                            # Set the stop event so the outer ``while not
                            # stop_evt.is_set()`` exits, and pop the entry
                            # from ``_stop_evts`` so a legitimate
                            # re-registration / reload (which constructs a
                            # fresh event via ``setdefault``) is not
                            # short-circuited by the now-consumed event.
                            stop_evt.set()
                            self._stop_evts.pop(entry_id, None)
                            break
                    else:
                        short_run_counter = 0  # healthy run resets the counter
                        observe = getattr(pc, "_observe_counter", None)
                        if callable(observe):
                            try:
                                observe(short_run_counter)
                            except Exception:  # noqa: BLE001
                                pass
                        # Mirror the counter reset in the Issue Registry: if
                        # the cap had previously fired on a prior supervisor
                        # incarnation, a healthy run is the user-visible
                        # signal that the condition is gone.
                        #
                        # Defense 2 iter-8: a healthy run is the ONLY proof
                        # the system has that the poison-message condition is
                        # gone, so this is also the only callsite that may
                        # drop the cap latch. After dropping the latch, the
                        # cap-related ``_fatal_errors`` entry is removed and
                        # the aggregate ``_fatal_error`` re-derived (so the
                        # System Health surface stops showing the stale
                        # message). The Repairs delete below stays inside
                        # ``self._hass is not None`` and intentionally also
                        # runs when no latch was set (cheap idempotent UI
                        # cleanup).
                        # AP4 (finding 4b, related sweep): a superseded or
                        # tombstoned supervisor must not release the LIVE
                        # generation's cap latch / fatal map / Repairs issue.
                        # This mirrors the cap-FIRE guard above: a stale healthy
                        # run is not proof the live incarnation's poison-message
                        # condition is gone, and an old generation finishing a
                        # healthy run after a fast reload would otherwise wipe the
                        # new generation's diagnostics. The closure-local
                        # ``short_run_counter`` reset above stays unguarded (it
                        # cannot leak across incarnations).
                        if not self._entry_writes_suppressed(entry_id, generation):
                            cap_was_latched = entry_id in self._short_run_cap_latched
                            if cap_was_latched:
                                self._short_run_cap_latched.discard(entry_id)
                                # Iter-16: a healthy run is also the moment to
                                # retire the cap-fire snapshot so the
                                # pass-through clear branch cannot re-introduce
                                # it after the latch is gone.
                                self._short_run_cap_messages.pop(entry_id, None)
                                cap_message = self._fatal_errors.get(entry_id)
                                if cap_message and cap_message.startswith(
                                    CRASH_LOOP_FATAL_PREFIX
                                ):
                                    self._fatal_errors.pop(entry_id, None)
                                    self._fatal_error = next(
                                        iter(self._fatal_errors.values()), None
                                    )
                                    _LOGGER.info(
                                        "[entry=%s] FCM short-run cap latch "
                                        "released by healthy supervisor run",
                                        entry_id,
                                    )
                            if self._hass is not None:
                                try:
                                    ir.async_delete_issue(
                                        self._hass,
                                        DOMAIN,
                                        f"fcm_short_run_crash_loop_{entry_id}",
                                    )
                                except Exception:  # noqa: BLE001
                                    pass

                    # Cleanup before restart
                    try:
                        await pc.stop()
                    except Exception:
                        pass
                    finally:
                        async with self._lock:
                            self._discard_own_client(entry_id, pc)
                        # AP4 (finding 4b, related sweep): suppress the
                        # stale-health write for a superseded/tombstoned
                        # supervisor (resurrection class, finding 2b).
                        if not self._entry_writes_suppressed(entry_id, generation):
                            self._update_entry_health(entry_id, False)

                    if not stop_evt.is_set():
                        delay = backoff + random.uniform(0.1, 0.3) * backoff  # nosec B311
                        _LOGGER.info(
                            "[entry=%s] Restarting FCM client in %.1fs", entry_id, delay
                        )
                        nudged = await _nudge_sleep(delay)
                        if nudged:
                            backoff = 1.0
                        else:
                            backoff = min(backoff * 2, _MAX_SUPERVISOR_BACKOFF_S)
            except asyncio.CancelledError:
                _LOGGER.debug("[entry=%s] FCM supervisor cancelled", entry_id)
                raise
            except Exception as err:  # noqa: BLE001
                _LOGGER.error(
                    "[entry=%s] FCM supervisor crashed: %s",
                    entry_id,
                    describe_exception(err),
                )
            finally:
                _LOGGER.info("[entry=%s] FCM supervisor stopped", entry_id)
                # AP4 (finding 4b, related sweep): a supervisor cancelled by a
                # teardown/reload must not write a stale "unhealthy" snapshot
                # here -- ``_purge_entry_tokens`` just dropped ``_entry_health``
                # for this entry, and re-creating it as False would suppress the
                # first healthy transition of the next incarnation (the AP2
                # resurrection class). Skip the write once this supervisor is
                # superseded or the entry is tombstoned; a genuine shutdown
                # (no generation bump, no tombstone) still records it.
                if not self._entry_writes_suppressed(entry_id, generation):
                    self._update_entry_health(entry_id, False)
                self._retry_nudge_evts.pop(entry_id, None)

        task = asyncio.create_task(
            _supervisor(), name=f"{DOMAIN}.fcm_supervisor[{entry_id}]"
        )
        self.supervisors[entry_id] = task
        _LOGGER.info("Started FCM supervisor for entry %s", entry_id)

    @staticmethod
    def _is_reregisterable_id(value: Any) -> bool:
        """Return True when ``value`` survives the reregister path's ``int()``.

        ``FcmRegister._get_checkin_payload`` re-uses a cached identity only when
        both fields are truthy *and* ``int()``-convertible (it runs
        ``int(android_id)`` / ``int(security_token)``). A truthy-but-malformed
        value — e.g. a non-numeric string ``"sec-tok"`` or a list — passes the
        truthiness gate but raises ``ValueError``/``TypeError`` on ``int()``,
        trapping the supervisor in the same retry loop. A corrupted payload may
        also carry a float infinity (``1e309`` / ``Infinity`` in JSON), which is
        truthy and not a bool but raises ``OverflowError`` on ``int()`` — the
        same trap, so it is rejected here too. ``bool`` is rejected explicitly:
        it is an ``int`` subclass but never a real device identity.
        """
        if not value or isinstance(value, bool):
            return False
        try:
            int(value)
        except (ValueError, TypeError, OverflowError):
            return False
        return True

    @classmethod
    def _gcm_identity_is_complete(cls, gcm: dict[str, Any]) -> bool:
        """Return True when a GCM block carries a re-registerable identity.

        ``FcmRegister.reregister_keeping_identity()`` subscripts
        ``credentials["gcm"]["android_id"]`` and ``["security_token"]`` *without*
        guarding (and only falls back to a full register when the ``gcm`` key is
        entirely absent). A partial block — e.g. an ``android_id`` with no
        ``security_token`` — raises ``KeyError`` on every retry, and a block
        whose values are present but not ``int()``-convertible raises
        ``ValueError``/``TypeError`` instead (see ``_is_reregisterable_id``).
        Only a block carrying both ``int()``-convertible values can be preserved
        across an FCM-token invalidation; anything else must be dropped so the
        next attempt performs a full fresh handshake instead of looping on the
        same broken identity.
        """
        return cls._is_reregisterable_id(
            gcm.get("android_id")
        ) and cls._is_reregisterable_id(gcm.get("security_token"))

    async def _invalidate_fcm_tokens(
        self, entry_id: str, generation: int | None = None
    ) -> None:
        """Clear renewable FCM tokens, preserving a *usable* GCM identity.

        When the GCM block still carries a complete device identity
        (android_id + security_token), only the renewable FCM parts are stripped
        so the next attempt can take the fast ``reregister_keeping_identity()``
        path. When the identity itself is incomplete/corrupt, the whole ``gcm``
        block is dropped instead: keeping it would make
        ``reregister_keeping_identity()`` raise ``KeyError`` forever (Codex
        follow-up), so dropping it forces ``_register_for_fcm_entry`` onto the
        full ``checkin_or_register()`` handshake.

        ``generation`` is the supervisor's captured generation (AP4 finding 4b).
        The public ``async_reregister_fcm`` path is a live operation and passes
        ``None`` (tombstone-window check only); a supervisor passes its snapshot
        so a superseded run cannot write back the creds the teardown dropped.
        """
        # AP4 race guard: never persist creds for an entry the teardown already
        # purged (a still-running or superseded supervisor must not write back
        # the credentials _purge_entry_tokens dropped).
        if self._entry_writes_suppressed(entry_id, generation):
            return
        creds = self.creds.get(entry_id)
        if creds and isinstance(creds, dict):
            # Renewable FCM parts always go.
            creds.pop("fcm", None)
            creds.pop("keys", None)

            gcm = creds.get("gcm")
            if isinstance(gcm, dict) and self._gcm_identity_is_complete(gcm):
                # Identity is usable: strip only the renewable GCM token parts
                # and keep android_id/security_token for a fast re-register.
                gcm.pop("token", None)
                # Rescue the superseded subscription id across the invalidation
                # so the subsequent reregister_keeping_identity() can unregister
                # the now-orphaned webpush subscription. Popping it outright (the
                # pre-fix #1169 behaviour) left ``old_app_id = None`` at capture
                # time, so the unregister guard never fired and orphaned
                # subscriptions kept accumulating at Google (the "does not match"
                # warnings). Keeping it in a dedicated slot also lets a
                # restart-triggered re-register clean up the orphan.
                orphan_app_id = gcm.pop("app_id", None)
                if orphan_app_id:
                    gcm["orphan_app_id"] = orphan_app_id
                _LOGGER.info(
                    "[entry=%s] Invalidated FCM tokens; "
                    "android_id/security_token preserved for re-registration",
                    entry_id,
                )
            else:
                # Identity is missing/partial/corrupt: drop the whole block so
                # the next attempt does a full registration rather than replaying
                # the same KeyError-raising creds.
                creds.pop("gcm", None)
                _LOGGER.warning(
                    "[entry=%s] Corrupt or incomplete GCM identity; dropped device "
                    "identity to force a full FCM re-registration",
                    entry_id,
                )

            # Persist partial credentials so restarts also trigger re-register
            entry_cache = self._entry_caches.get(entry_id)
            if entry_cache is not None:
                try:
                    await entry_cache.set("fcm_credentials", creds)
                except Exception:
                    pass

    @staticmethod
    def _raise_if_fatal_client_error(entry_id: str, err: ClientError) -> None:
        """Raise ``FatalRegistrationError`` for 401/404 client errors."""
        status_raw = getattr(err, "status", None)
        if status_raw is None:
            status_raw = getattr(err, "status_code", None)
        try:
            status_int = int(cast(int | str, status_raw))
        except (TypeError, ValueError):
            status_int = None

        if status_int == _HTTP_UNAUTHORIZED:
            raise FatalRegistrationError(
                f"FCM registration auth failed ({status_int}): token renewal needed",
                is_auth_error=True,
            ) from err

        if status_int == _HTTP_NOT_FOUND:
            raise FatalRegistrationError(
                f"FCM registration endpoint not found ({status_int}): may be transient",
                is_auth_error=False,
            ) from err

        _LOGGER.error(
            "[entry=%s] FCM registration client error (status=%s): %s",
            entry_id,
            status_raw,
            describe_exception(err),
        )

    @staticmethod
    def _raise_if_fatal_http_error(entry_id: str, err: FcmRegisterHTTPError) -> None:
        """Map ``FcmRegisterHTTPError`` to ``FatalRegistrationError``.

        ``FcmRegisterHTTPError`` is raised inside the firebase_messaging helper
        functions (``fcm_install``, ``fcm_register``, ``fcm_refresh_install_token``,
        ``gcm_check_in``, ``gcm_register``) when an FCM/GCM endpoint returns 401
        or 404. Because those helpers do not propagate aiohttp ``ClientError``,
        the existing ``_raise_if_fatal_client_error`` path cannot see those
        statuses; this companion classifier closes that gap so the
        ``_register_for_fcm_entry`` retry budget (auth vs. endpoint) runs.
        """
        status_int = int(err.status)

        if status_int == _HTTP_UNAUTHORIZED:
            raise FatalRegistrationError(
                f"FCM registration auth failed ({status_int}): token renewal needed",
                is_auth_error=True,
            ) from err

        if status_int == _HTTP_NOT_FOUND:
            raise FatalRegistrationError(
                f"FCM registration endpoint not found ({status_int}): may be transient",
                is_auth_error=False,
            ) from err

        _LOGGER.error(
            "[entry=%s] FCM register fatal HTTP error (status=%s): %s",
            entry_id,
            status_int,
            err,
        )

    async def _register_for_fcm_entry(
        self, entry_id: str, generation: int | None = None
    ) -> bool:
        """Single registration attempt for a specific entry.

        ``generation`` is the supervisor's captured generation (AP4 finding 4b);
        it gates the success-path routing writes so a superseded supervisor
        cannot resurrect routing state after the boolean tombstone is cleared.
        Callers outside a supervisor pass ``None`` (tombstone-window check only).
        """
        pc = self.pcs.get(entry_id)
        if not pc:
            return False
        try:
            creds = getattr(pc, "credentials", None)
            # If FCM tokens were invalidated (partial creds with only GCM
            # identity), re-register while preserving android_id.
            needs_reregister = (
                isinstance(creds, dict) and "gcm" in creds and "fcm" not in creds
            )
            # Publish this supervisor's captured generation for the handshake so
            # the synchronously-fired credentials callback gates on the driving
            # generation, not the reused client's stale build-time one (4c).
            with self._driving_generation_scope(entry_id, generation):
                if needs_reregister:
                    _LOGGER.info(
                        "[entry=%s] Partial credentials detected; "
                        "re-registering with existing device identity",
                        entry_id,
                    )
                    token_or_creds = await pc.reregister_keeping_identity()
                else:
                    token_or_creds = await pc.checkin_or_register()

            if token_or_creds:
                _LOGGER.info("[entry=%s] FCM registered successfully", entry_id)
                # AP4 race guard (two tiers): a teardown or a supersede may have
                # purged or re-owned this entry while this attempt was in flight.
                #
                # * Routing writes RE-CREATE the per-entry state
                #   ``_purge_entry_tokens`` removed, so they are skipped whenever
                #   writes are suppressed (tombstone window OR superseded gen).
                # * The clears / counter resets only REMOVE state, so they stay
                #   teardown-safe in the pure-tombstone window (the entry is going
                #   away and ``unregister_coordinator`` already reset it). But a
                #   SUPERSEDED generation finishing after a fast reload must NOT
                #   run them: that would wipe the LIVE generation's fatal latch,
                #   reauth repair and retry counters (finding: guard all stale
                #   success side effects). Gate them on the narrow generation
                #   check so the documented teardown-safe-clear behaviour is kept.
                if not self._entry_writes_suppressed(entry_id, generation):
                    token = self.get_fcm_token(entry_id)
                    if token:
                        self._update_token_routing(token, {entry_id})
                        await self._persist_routing_token(entry_id, token)
                if not self._entry_generation_superseded(entry_id, generation):
                    self._clear_fatal_error_for_entry(
                        entry_id, reason="Registration succeeded"
                    )
                    # Reset fatal retry counters on success
                    self._fatal_retry_counts.pop(f"{entry_id}:auth", None)
                    self._fatal_retry_counts.pop(f"{entry_id}:endpoint", None)
                return True
            _LOGGER.warning("[entry=%s] FCM registration returned no token", entry_id)
            return False
        except FatalRegistrationError:
            raise
        except ClientError as err:
            self._raise_if_fatal_client_error(entry_id, err)
            return False
        except FcmRegisterHTTPError as err:
            # MUST precede the generic RuntimeError catch below, because
            # FcmRegisterHTTPError is a RuntimeError subclass. This branch
            # turns persistent 401/404 from the firebase_messaging helpers
            # into a FatalRegistrationError so the supervisor's auth/endpoint
            # retry budget (with _invalidate_fcm_tokens) runs.
            self._raise_if_fatal_http_error(entry_id, err)
            return False
        except Exception as err:  # noqa: BLE001
            # AP6 (finding 3a): the previous combined
            # ``(TimeoutError, RuntimeError, Exception)`` made the specific types
            # redundant and treated every non-fatal error the same. Delegate the
            # classification to a dedicated helper (extract-method keeps this
            # function under PLR0911 and makes the rule unit-testable) and return
            # a single non-fatal result. Fatal paths
            # (FatalRegistrationError / ClientError / FcmRegisterHTTPError) are
            # handled above and never reach here.
            await self._classify_registration_exception(entry_id, err, generation)
            return False

    async def _classify_registration_exception(
        self, entry_id: str, err: BaseException, generation: int | None = None
    ) -> None:
        """Classify a non-fatal registration exception (AP6, finding 3a).

        Splits the formerly conflated catch-all into three honest classes:

        * ``TimeoutError`` / ``RuntimeError`` — genuinely transient (network
          timeout, or firebase_messaging "...token missing..."); just retry.
        * ``KeyError`` / ``ValueError`` / ``TypeError`` — structural credential
          corruption (``reregister_keeping_identity()`` /
          ``checkin_or_register()`` raise ``KeyError`` on corrupt
          ``credentials["gcm"][...]`` and ``ValueError``/``TypeError`` on
          malformed/JSON-broken creds; ``json.JSONDecodeError`` is a
          ``ValueError``). NOT transient: retrying replays the same broken creds
          forever with no visible fatal state. Escalate by invalidating the FCM
          tokens, which preserves a *complete* GCM identity but drops a
          partial/corrupt one, so the next attempt performs a fresh handshake
          instead of looping. Conservative: only these well-defined types.
        * anything else — unexpected; logged at error (not silently swallowed as
          benign) but left non-fatal so a single surprise cannot crash the
          supervisor.

        ``generation`` is the calling supervisor's captured generation (AP4
        finding 4b); it is forwarded to ``_invalidate_fcm_tokens`` so a
        superseded supervisor cannot write back the creds a teardown dropped.
        Callers outside a supervisor pass ``None`` (tombstone-window check only).
        """
        if isinstance(err, (TimeoutError, RuntimeError)):
            _LOGGER.info(
                "[entry=%s] FCM registration failed (transient): %s - will retry",
                entry_id,
                describe_exception(err),
            )
        elif isinstance(err, (KeyError, ValueError, TypeError)):
            _LOGGER.error(
                "[entry=%s] FCM registration data is corrupt: %s "
                "- invalidating FCM tokens to force re-registration",
                entry_id,
                describe_exception(err),
            )
            await self._invalidate_fcm_tokens(entry_id, generation)
        else:
            _LOGGER.error(
                "[entry=%s] FCM registration unexpected error: %s",
                entry_id,
                describe_exception(err),
            )

    # Public entrypoint kept for back-compat (starts supervisors lazily if needed)
    async def _start_listening(self) -> None:
        """Ensure supervisors are running for all known coordinators' entries."""
        # Start a supervisor per entry present among registered coordinators
        for coordinator in self.coordinators.copy():
            entry = getattr(coordinator, "config_entry", None)
            cache = getattr(coordinator, "cache", None)
            if entry is None:
                continue
            await self._start_supervisor_for_entry(entry.entry_id, cache)

    # -------------------- Coordinator wiring --------------------

    def register_coordinator(self, coordinator: Any) -> None:
        """Register a coordinator for background updates and ensure its entry client runs.

        Side effects:
            * Adds coordinator to the fan-out list.
            * Mirrors current credentials into this entry's TokenCache (if available).
            * Starts (or ensures) a supervisor for the coordinator's entry.
            * Updates token→entry routing for any available token.
            * Loads previously persisted routing tokens (`fcm_routing_tokens`) and maps them to this entry.
        """
        if coordinator not in self.coordinators:
            self.coordinators.append(coordinator)
            _LOGGER.debug("Coordinator registered (total=%d)", len(self.coordinators))

        entry = getattr(coordinator, "config_entry", None)
        cache = getattr(coordinator, "cache", None)
        if entry is None:
            return

        # AP4 (finding 4a): a setup (initial or reload) un-tombstones the entry
        # so the *new* incarnation's writes take effect again. This is the
        # single reset point: it runs synchronously before the supervisor is
        # (re)dispatched below. Clearing the boolean tombstone alone would
        # re-open the race for a prior supervisor that request_stop only
        # cancelled (not awaited) and which is still unwinding; finding 4b
        # closes that gap via the generation captured per supervisor, so a
        # superseded run stays suppressed by generation even though the
        # tombstone is cleared here. The generation is intentionally NOT reset:
        # the next supervisor reads the post-teardown value and is allowed,
        # while the stale one keeps its old (now mismatched) snapshot.
        self._unregistered.discard(entry.entry_id)

        self._entry_to_tokens.setdefault(entry.entry_id, set())

        if cache is not None:
            self._ensure_cache_entry_id(cache, entry.entry_id)
            self._entry_caches[entry.entry_id] = cache

            pending_creds = self._pending_creds.pop(entry.entry_id, None)
            if pending_creds is not None:
                self._dispatch_to_hass_loop(
                    cache.set("fcm_credentials", pending_creds),
                    label=f"set_pending_creds_{entry.entry_id}",
                )

            pending_tokens = self._pending_routing_tokens.pop(entry.entry_id, set())

            if pending_tokens:
                self._entry_to_tokens.setdefault(entry.entry_id, set()).update(
                    pending_tokens
                )

                async def _flush_tokens() -> None:
                    try:
                        existing = await cache.get("fcm_routing_tokens")
                        tokens = set(existing or [])
                        tokens.update(pending_tokens)
                        await cache.set("fcm_routing_tokens", sorted(tokens))
                    except Exception as err:
                        _LOGGER.debug(
                            "[entry=%s] Failed to flush pending routing entries: %s",
                            entry.entry_id,
                            describe_exception(err),
                        )

                self._dispatch_to_hass_loop(
                    _flush_tokens(),
                    label=f"flush_pending_tokens_{entry.entry_id}",
                )

        # Mirror any known credentials to this entry cache
        try:
            creds = self.creds.get(entry.entry_id)
            if creds and cache is not None:
                self._dispatch_to_hass_loop(
                    cache.set("fcm_credentials", creds),
                    label=f"mirror_creds_{entry.entry_id}",
                )
        except Exception as err:
            _LOGGER.debug(
                "Entry-scoped persistence of the FCM registration skipped: %s",
                describe_exception(err),
            )

        # Update routing with any token we already have
        token = self.get_fcm_token(entry.entry_id)
        if token:
            self._update_token_routing(token, {entry.entry_id})
            self._dispatch_to_hass_loop(
                self._persist_routing_token(entry.entry_id, token),
                label=f"persist_routing_token_{entry.entry_id}",
            )

        # Load persisted routing tokens for this entry and map them as well
        if cache is not None:

            async def _load_tokens() -> None:
                try:
                    existing = await cache.get("fcm_routing_tokens")
                    if isinstance(existing, (list, tuple, set)):
                        for t in existing:
                            if isinstance(t, str) and t:
                                self._update_token_routing(t, {entry.entry_id})
                except Exception as err:
                    _LOGGER.debug(
                        "[entry=%s] Failed to load persisted routing entries: %s",
                        entry.entry_id,
                        describe_exception(err),
                    )

            self._dispatch_to_hass_loop(
                _load_tokens(),
                label=f"load_persisted_tokens_{entry.entry_id}",
            )

        # Start supervisor for this entry
        self._dispatch_to_hass_loop(
            self._start_supervisor_for_entry(entry.entry_id, cache),
            label=f"start_supervisor_{entry.entry_id}",
        )

    def unregister_coordinator(self, coordinator: Any) -> None:
        """Unregister a coordinator (sync; safe for async_on_unload)."""
        entry = getattr(coordinator, "config_entry", None)
        entry_id: str | None = None
        if entry is not None:
            entry_id = getattr(entry, "entry_id", None)

        try:
            self.coordinators.remove(coordinator)
            _LOGGER.debug("Coordinator unregistered (total=%d)", len(self.coordinators))
        except ValueError:
            pass  # already removed

        if entry_id is not None:
            replacement = None
            for other in self.coordinators:
                other_entry = getattr(other, "config_entry", None)
                other_cache = getattr(other, "cache", None)
                if (
                    other_entry is not None
                    and getattr(other_entry, "entry_id", None) == entry_id
                    and other_cache is not None
                ):
                    replacement = other_cache
                    break

            if replacement is not None:
                self._entry_caches[entry_id] = replacement
            else:
                self._entry_caches.pop(entry_id, None)
                # AP4 (finding 4a): tombstone the entry BEFORE purging so any
                # write from the still-running supervisor task (this method is
                # synchronous and does not await it) is dropped instead of
                # re-creating the state purged below. Cleared by
                # register_coordinator on the next setup (reload-safe).
                self._unregistered.add(entry_id)
                # AP4 (finding 4b): bump the generation BEFORE the tombstone is
                # later cleared by ``register_coordinator``. ``request_stop()``
                # only cancels the prior supervisor without awaiting it, so this
                # bump is what keeps that cancelled-but-unwinding supervisor's
                # writes suppressed once the tombstone is gone -- it captured the
                # old generation, which no longer matches.
                self._entry_generation[entry_id] = (
                    self._entry_generation.get(entry_id, 0) + 1
                )
                self._purge_entry_tokens(entry_id)
                # Hard-reset on unregister: this fires via ``async_on_unload``
                # on EVERY unload (reload AND removal), so it only resets the
                # in-memory state -- the short-run cap latch, the fatal-error
                # map and the transient short-run crash-loop Repairs issue
                # (force=True). It must NOT delete the fixable reauth repairs:
                # on a reauth-success reload the new supervisor still has to
                # prove recovery first. Those are retired by the supervisor
                # success path (reload) or ``async_delete_entry_repair_issues``
                # (actual removal). See ``_clear_fatal_error_for_entry``.
                self._clear_fatal_error_for_entry(
                    entry_id, reason="Entry unregistered", force=True
                )

    # -------------------- Incoming notifications --------------------

    def _on_notification(
        self,
        entry_id: str,
        payload: Mapping[str, Any],
        persistent_id: str | None,
        context: Any | None,
    ) -> None:
        """Handle incoming FCM notification (sync callback from per-entry client).

        This callback may be invoked from any thread by the FCM client.
        We use _dispatch_to_hass_loop for thread-safe async dispatch (P0 fix).
        """
        _ = persistent_id  # maintained for signature compatibility
        _ = context

        # Thread-safe dispatch to HA event loop
        label = f"fcm-notification[{entry_id[:8] if entry_id else 'unknown'}]"
        self._dispatch_to_hass_loop(
            self._handle_notification_async(entry_id, payload),
            label=label,
        )

    async def _handle_notification_async(
        self, entry_id: str, payload: Mapping[str, Any]
    ) -> None:
        """Async handler executed in HA loop: parse, route, and schedule downstream work."""
        try:
            hex_string = self._extract_hex_payload(payload)
            if hex_string is None:
                return

            # P1 fix: offload protobuf parsing to executor
            canonic_id = await self._extract_canonic_id_async(hex_string)
            if not canonic_id:
                _LOGGER.debug("FCM response has no canonical id")
                return

            token = self._extract_push_token(dict(payload))
            target_entries, route_src = self._route_target_entries(
                entry_id, canonic_id, token
            )
            target_coordinators = self._coordinators_for_entries(target_entries)

            cb = self.location_update_callbacks.get(canonic_id)
            if cb:
                # Real locate data delivery: stamp the data-delivery clock for
                # every routed target entry and clear any watchdog backoff state,
                # since data is flowing again (re-arm on the next starvation).
                self._stamp_data_delivery(target_entries)
                self._log_push_received(canonic_id, target_entries, route_src, 1)
                await self._run_callback_async(cb, canonic_id, hex_string)
                return

            # Log FCM pushes that have no registered callback (e.g. sound
            # confirmations, device status updates).  This fires only in
            # response to a user-initiated action (Play Sound button etc.)
            # so it does not create log spam during normal operation. Length
            # only: raw API payloads are never logged (AGENTS.md section 5).
            _LOGGER.debug(
                "FCM push for %s has no registered callback "
                "(may be action confirmation): payload_len=%d",
                canonic_id[:8],
                len(hex_string),
            )

            tracked = [
                c for c in target_coordinators if self._is_tracked(c, canonic_id)
            ]
            for coordinator in target_coordinators:
                if coordinator in tracked:
                    continue
                _LOGGER.debug(
                    "Skipping FCM update for ignored device %s", canonic_id[:8]
                )

            self._log_push_received(canonic_id, target_entries, route_src, len(tracked))

            if not tracked:
                _LOGGER.debug(
                    "No registered coordinator will process %s; dropping FCM update",
                    canonic_id[:8],
                )
                return

            await self._process_background_update(
                entry_id, canonic_id, hex_string, target_entries
            )

        except Exception as handle_err:
            _LOGGER.error(
                "Failed to handle FCM notification safely (%s at %s)",
                describe_exception(handle_err),
                exception_origin(handle_err),
            )

    # -------------------- Routing helpers --------------------

    def _stamp_data_delivery(self, target_entries: set[str] | None) -> None:
        """Record a real locate data delivery for every routed target entry.

        Called ONLY from the cb-hit branch of ``_handle_notification_async`` (a
        genuine locate data message), never on heartbeat acks and never from
        ``_log_push_received``. Token routing can fan out to several receiver
        entries under multi-account setups, so all entries in ``target_entries``
        are stamped; a ``None`` routing set (broadcast / unknown owner) is
        conservatively left unstamped so it can never mask starvation on a
        specific entry. Delivery also re-arms the watchdog by clearing any
        backoff state (data is flowing again).
        """
        if not target_entries:
            return
        now = time.monotonic()
        for eid in target_entries:
            self._entry_last_data_delivery_monotonic[eid] = now
            self._reset_zombie_backoff(eid)

    def _reset_zombie_backoff(self, entry_id: str) -> None:
        """Clear the watchdog backoff/count state for *entry_id* (re-arm)."""
        self._zombie_reconnect_attempts.pop(entry_id, None)
        self._zombie_next_allowed_reconnect_monotonic.pop(entry_id, None)

    def _extract_hex_payload(self, payload: Mapping[str, Any]) -> str | None:
        """Return the decoded hex payload or None when absent."""
        payload_dict = (payload.get("data") or {}).get(
            "com.google.android.apps.adm.FCM_PAYLOAD"
        )
        if not payload_dict:
            _LOGGER.debug("FCM notification without FMD payload")
            return None

        pad = len(payload_dict) % 4
        if pad:
            payload_dict += "=" * (4 - pad)

        try:
            decoded = base64.b64decode(payload_dict)
        except (binascii.Error, ValueError) as err:
            _LOGGER.error("FCM Base64 decode failed: %s", describe_exception(err))
            return None

        return binascii.hexlify(decoded).decode("utf-8")

    def _route_target_entries(
        self, entry_id: str, canonic_id: str, token: str | None
    ) -> tuple[set[str] | None, str]:
        """Determine routing targets and the selected source."""
        if token and token in self._token_to_entries:
            return set(self._token_to_entries[token]), "token"

        if entry_id is not None:
            return {entry_id}, "client"

        owner_entry = self._lookup_owner_entry(canonic_id)
        if owner_entry:
            return {owner_entry}, "owner_index"

        return None, "fallback"

    def _lookup_owner_entry(self, canonic_id: str) -> str | None:
        """Best-effort lookup of owner entry id from hass data."""
        if self._hass is None:
            return None
        owner_index = self._hass.data.get(DOMAIN, {}).get("device_owner_index", {})
        entry_id = owner_index.get(canonic_id)
        return entry_id if isinstance(entry_id, str) else None

    def _log_push_received(
        self,
        canonic_id: str,
        target_entries: set[str] | None,
        route_src: str,
        fanout_targets: int,
    ) -> None:
        """Log the push receipt with routing context (DEBUG: one per locate)."""
        _LOGGER.debug(
            "push_received(entry=%s, device=%s, fanout_targets=%d, route=%s)",
            ",".join(sorted(target_entries)) if target_entries else "unknown",
            canonic_id[:8],
            fanout_targets,
            route_src,
        )

    @staticmethod
    def _extract_push_token(envelope: dict[str, Any]) -> str | None:
        """Extract the push target token from the envelope (best-effort)."""
        try:
            token = envelope.get("to") or envelope.get("token")
            if not token and isinstance(envelope.get("message"), dict):
                token = envelope["message"].get("token")
            if isinstance(token, str) and token:
                return token
        except Exception:
            pass
        return None

    def _coordinators_for_entries(self, entries: set[str] | None) -> list[Any]:
        """Return coordinators for the given entry set (or all if None)."""
        if entries is None:
            return self.coordinators.copy()
        if not entries:
            return []
        res: list[Any] = []
        for c in self.coordinators:
            try:
                entry = getattr(c, "config_entry", None)
                if entry is not None and entry.entry_id in entries:
                    res.append(c)
            except Exception:
                pass
        return res

    def _prepare_coordinator_payload(
        self, coordinator: Any, key: tuple[str, str], payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Apply coordinator-specific filtering and return payload or None if filtered."""
        coordinator_payload = dict(payload)

        # Tally the REPORTED accuracy class here (#216), before the Google Home
        # filter below can replace it with a configured radius. The coordinator
        # cannot do it: by the time ``update_device_cache`` sees this payload the
        # substitution has already happened, so a class counted there would
        # describe the filter's geometry instead of what the push reported. The
        # marker tells the coordinator not to count it a second time; it is
        # popped there, so it never reaches the cached row.
        # Same replay rule as the poll and locate paths: a push carrying a
        # report timestamp we already hold (a duplicate delivery, or the same
        # report after a poll) must not enter the distribution twice. Asked of
        # the coordinator because only it holds both references, and duck-typed
        # because this receiver runs against partial coordinators too.
        tally = getattr(coordinator, "count_accuracy_class", None)
        if callable(tally):
            try:
                claim = getattr(coordinator, "claim_report_for_tally", None)
                # No claim method on a partial coordinator: count, as before.
                # Losing a duplicate suppression is a smaller defect than losing
                # the measurement itself.
                if not callable(claim) or claim(key[1], coordinator_payload):
                    tally(coordinator_payload)
                coordinator_payload["_accuracy_counted"] = True
            except Exception as tally_err:  # pragma: no cover - diagnostics only
                _LOGGER.debug(
                    "Accuracy tally failed for %s: %s",
                    key[1][:8],
                    describe_exception(tally_err),
                )

        semantic_name = coordinator_payload.get("semantic_name")
        ghf = _coordinator_google_home_filter(coordinator)
        if semantic_name and ghf is not None:
            try:
                should_filter, replacement_attrs = ghf.should_filter_detection(
                    key[1], semantic_name
                )
            except Exception as gf_err:
                _LOGGER.debug(
                    "Google Home filter error for %s: %s",
                    key[1][:8],
                    describe_exception(gf_err),
                )
                should_filter, replacement_attrs = False, None

            if should_filter:
                _LOGGER.debug(
                    "Filtered Google Home detection for %s (push path)", key[1][:8]
                )
                return None

            if replacement_attrs:
                if "latitude" in replacement_attrs and "longitude" in replacement_attrs:
                    coordinator_payload["latitude"] = replacement_attrs.get("latitude")
                    coordinator_payload["longitude"] = replacement_attrs.get(
                        "longitude"
                    )
                radius = replacement_attrs.get("radius")
                if radius is not None:
                    substitute_zone_accuracy(coordinator_payload, radius)
                coordinator_payload["semantic_name"] = None

        return coordinator_payload

    def _write_coordinator_payload(
        self, coordinator: Any, device_id: str, payload: dict[str, Any]
    ) -> bool:
        """Persist the payload into coordinator caches."""
        update_cache = getattr(coordinator, "update_device_cache", None)
        if callable(update_cache):
            update_cache(device_id, payload)
            return True

        try:
            # Shared with the poll cycle's direct-write fallback, which had the
            # same leak: rationale in ``strip_transient_keys``.
            strip_transient_keys(payload)
            coordinator._device_location_data[device_id] = payload  # noqa: SLF001
            # Fourth commit site, same rule as the others: a newer position
            # makes a retained coarse fix obsolete. Duck-typed because this
            # fallback exists precisely for coordinators that do not carry the
            # full surface.
            expire = getattr(coordinator, "_expire_coarse_fix", None)
            if callable(expire):
                expire(device_id, payload)
            _LOGGER.debug(
                "Fallback: wrote to coordinator._device_location_data directly"
            )
            coordinator.increment_stat("background_updates")
        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "Coordinator cache update failed for %s: %s",
                device_id,
                describe_exception(err),
            )
            return False
        return True

    def _record_crowd_source_stat(self, coordinator: Any) -> None:
        """Increment crowd-source stat when available."""
        try:
            coordinator.increment_stat("crowd_sourced_updates")
        except Exception:
            pass

    async def _notify_coordinator(self, coordinator: Any, device_id: str) -> None:
        """Notify coordinator about updated payload."""
        push = getattr(coordinator, "push_updated", None)
        if callable(push):
            push([device_id])
            return

        await coordinator.async_request_refresh()

    def _update_token_routing(self, token: str, entry_ids: set[str]) -> None:
        """Update the token→entry mapping."""
        try:
            if not isinstance(token, str) or not token:
                return
            prev = set(self._token_to_entries.get(token, set()))
            new_entries = {eid for eid in entry_ids if isinstance(eid, str) and eid}

            if new_entries:
                self._token_to_entries[token] = new_entries
            else:
                self._token_to_entries.pop(token, None)

            removed_entries = prev - new_entries
            for entry_id in removed_entries:
                tokens = self._entry_to_tokens.get(entry_id)
                if tokens is not None:
                    tokens.discard(token)
                    if not tokens:
                        self._entry_to_tokens.pop(entry_id, None)

            for entry_id in new_entries:
                self._entry_to_tokens.setdefault(entry_id, set()).add(token)

            if prev != new_entries:
                # Entry ids and a route count only: no prefix of the push
                # token is logged (AGENTS.md section 5: tokens).
                _LOGGER.debug(
                    "Updated FCM push routing: %d route(s) known, entries %s -> %s",
                    len(self._token_to_entries),
                    ",".join(sorted(prev)) or "<none>",
                    ",".join(sorted(new_entries)) or "<none>",
                )
        except Exception as err:
            _LOGGER.debug("Routing update skipped: %s", describe_exception(err))

    async def _persist_routing_token(self, entry_id: str, token: str) -> None:
        """Persist routing tokens per entry (best-effort, entry-scoped if cache available)."""
        if not isinstance(token, str) or not token:
            return

        self._entry_to_tokens.setdefault(entry_id, set()).add(token)

        cache = self._entry_caches.get(entry_id)
        if cache is not None:
            try:
                existing = await cache.get("fcm_routing_tokens")
                tokens = set(existing or [])
                tokens.add(token)
                await cache.set("fcm_routing_tokens", sorted(tokens))
            except Exception as err:
                _LOGGER.debug(
                    "Persisting the routing entry failed for %s: %s",
                    entry_id,
                    describe_exception(err),
                )
            return

        # Resolve cache lazily via registered coordinators
        for coordinator in self.coordinators.copy():
            entry = getattr(coordinator, "config_entry", None)
            cache = getattr(coordinator, "cache", None)
            if entry is None or entry.entry_id != entry_id or cache is None:
                continue
            self._entry_caches[entry_id] = cache
            try:
                existing = await cache.get("fcm_routing_tokens")
                tokens = set(existing or [])
                tokens.add(token)
                await cache.set("fcm_routing_tokens", sorted(tokens))
            except Exception as err:
                _LOGGER.debug(
                    "Persisting the routing entry failed for %s: %s",
                    entry_id,
                    describe_exception(err),
                )
            return

        pending = self._pending_routing_tokens.setdefault(entry_id, set())
        pending.add(token)

    # -------------------- Ignore / target helpers --------------------

    @staticmethod
    def _norm(dev_id: str) -> str:
        """Normalize a device id for equality checks."""
        return (dev_id or "").replace("-", "").lower()

    def _is_tracked(self, coordinator: Any, canonic_id: str) -> bool:
        """Return True if device is eligible for push processing."""
        try:
            is_ignored_fn = getattr(coordinator, "is_ignored", None)
            if callable(is_ignored_fn) and is_ignored_fn(canonic_id):
                return False
        except Exception:
            pass
        try:
            entry = getattr(coordinator, "config_entry", None)
            if entry is not None:
                ignored = entry.options.get(OPT_IGNORED_DEVICES, [])
                if isinstance(ignored, list) and canonic_id in ignored:
                    return False
        except Exception:
            pass
        return True

    def _extract_canonic_id_from_response(self, hex_response: str) -> str | None:
        """Extract canonical id via the decoder.

        Handles the schema difference between Android devices (nested in phoneInformation)
        and Spot/Tracker devices (direct canonicIds), as defined in DeviceUpdate.proto.
        """
        try:
            device_update = decoder_module.parse_device_update_protobuf(hex_response)
            if device_update.HasField("deviceMetadata"):
                info = device_update.deviceMetadata.identifierInformation

                # Aligns with ProtoDecoders.decoder.get_canonic_ids logic:
                # type == 1 → Android devices store IDs under phoneInformation.canonicIds.
                if info.type == 1:
                    ids = info.phoneInformation.canonicIds.canonicId
                else:
                    ids = info.canonicIds.canonicId

                if ids:
                    return ids[0].id
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug(
                "Failed to extract canonical id from FCM response: %s",
                describe_exception(err),
            )
        return None

    async def _extract_canonic_id_async(self, hex_response: str) -> str | None:
        """Extract canonical ID via the decoder in an executor (P1-1 fix).

        Offloads CPU-bound protobuf parsing to avoid blocking the event loop.
        """
        return await _call_in_executor(
            self._extract_canonic_id_from_response, hex_response
        )

    async def _run_callback_async(
        self, callback: Callable[[str, str], None], canonic_id: str, hex_string: str
    ) -> None:
        """Run a user callback safely without unhandled task exceptions (P0-2 fix).

        Catches and logs exceptions to prevent 'Task exception was never retrieved'.
        """
        try:
            await _call_in_executor(callback, canonic_id, hex_string)
        except Exception as callback_err:
            # Do NOT log the full payload - use length only for safety
            _LOGGER.error(
                "FCM locate callback failed (canonic_id=%s, payload_len=%d, %s at %s)",
                canonic_id[:8] if canonic_id else "unknown",
                len(hex_string) if hex_string else 0,
                describe_exception(callback_err),
                exception_origin(callback_err),
            )

    # -------------------- Push-path decode → debounce → flush --------------------

    async def _process_background_update(
        self,
        entry_id: str,
        canonic_id: str,
        hex_string: str,
        target_entries: set[str] | None,
    ) -> None:
        """Decode location, enqueue for debounce, and schedule a flush (with routing context).

        The routing context (target entry set) is stored alongside the pending payload to
        enable precise fan-out in `_flush(...)`.
        """
        try:
            location_data = await self._decode_background_location_async(
                entry_id, hex_string
            )
            if not location_data:
                _LOGGER.debug(
                    "No location data in background update for %s", canonic_id
                )
                return

            payload: JSONDict = dict(location_data)
            payload.setdefault("last_updated", time.time())

            key = (
                next(iter(target_entries))
                if (target_entries and len(target_entries) == 1)
                else entry_id,
                canonic_id,
            )
            # Store the payload and the full routing target set (may be None for broadcast fallback)
            self._pending[key] = payload
            self._pending_targets[key] = set(target_entries) if target_entries else None

            self._schedule_flush(key)

        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "Error processing background update for %s: %s",
                canonic_id,
                describe_exception(err),
            )

    def _schedule_flush(self, key: tuple[str, str]) -> None:
        """(Re)schedule a short debounce before fanning out updates for (entry, device)."""
        existing = self._flush_tasks.pop(key, None)
        if existing and not existing.done():
            existing.cancel()

        async def _delayed() -> None:
            try:
                await asyncio.sleep(self._debounce_ms / 1000.0)
                await self._flush(key)
            except asyncio.CancelledError:
                return
            except Exception as err:
                _LOGGER.error(
                    "Flush task for %s/%s failed: %s",
                    key[0],
                    key[1],
                    describe_exception(err),
                )

        task = asyncio.create_task(
            _delayed(), name=f"{DOMAIN}.fcm_flush[{key[0]}:{key[1][:8]}]"
        )
        self._flush_tasks[key] = task

    async def _flush(self, key: tuple[str, str]) -> None:
        """Flush the latest pending payload to target coordinators only.

        Args:
            key: Tuple of (entry_id_hint, device_id). The exact target entries are taken
                 from `_pending_targets[key]`, which may be a set or None (broadcast fallback).
        """
        payload = self._pending.pop(key, None)
        entries = self._pending_targets.pop(key, None)
        self._flush_tasks.pop(key, None)
        if not payload:
            return

        target_coordinators = self._coordinators_for_entries(entries)

        for coordinator in target_coordinators:
            try:
                if not self._is_tracked(coordinator, key[1]):
                    continue

                coordinator_payload = self._prepare_coordinator_payload(
                    coordinator, key, payload
                )
                if coordinator_payload is None:
                    continue

                if not self._write_coordinator_payload(
                    coordinator, key[1], coordinator_payload
                ):
                    continue

                if coordinator_payload.get("is_own_report") is False:
                    self._record_crowd_source_stat(coordinator)

                await self._notify_coordinator(coordinator, key[1])

            except Exception as err:
                _LOGGER.debug(
                    "Failed to fan-out push update for %s to one coordinator: %s",
                    key[1][:8],
                    describe_exception(err),
                )

    # -------------------- Decode helper --------------------

    def _note_decrypt_failure_for_entry(
        self, entry_id: str, *, stale: bool, error: Exception
    ) -> None:
        """Feed a background-path decryption failure into the matching coordinator(s).

        Mirrors the poll path so a push-only setup escalates to re-authentication
        identically (one shared counter, no second divergent code path). Because the
        background decode runs outside a coordinator update cycle, an escalation
        cannot be surfaced by raising ``ConfigEntryAuthFailed`` here; instead the
        entry's reauth flow is started directly -- the same synchronous ``@callback``
        mechanism repairs.py and coordinator/locate.py already use. It is idempotent:
        Home Assistant does not duplicate an already-open reauth flow.

        This must never break the push path, so all failures are swallowed to debug.
        """
        hass = self._hass
        for coordinator in self._coordinators_for_entries({entry_id}):
            try:
                escalate = coordinator.note_decrypt_failure(stale=stale, error=error)
            except Exception as err:  # noqa: BLE001 - never break the push path
                _LOGGER.debug(
                    "note_decrypt_failure failed for entry %s: %s",
                    entry_id,
                    describe_exception(err),
                )
                continue
            if escalate and hass is not None:
                entry = getattr(coordinator, "config_entry", None)
                if entry is not None:
                    # FIX 3: direct reauth site (background decrypt escalation for
                    # a stale shared key); record the classified reason on the
                    # matching coordinator before starting the flow. This escalation
                    # is only reached on the account-wide stale-shared-key decrypt
                    # path (note_decrypt_failure returns False for stale=True), so it
                    # maps to DECRYPT_STALE_KEY -- the same code the poll and locate
                    # equivalents use -- not an AAS/token code.
                    recorder = getattr(coordinator, "record_reauth_reason", None)
                    if callable(recorder):
                        recorder(
                            ReauthReasonCode.DECRYPT_STALE_KEY,
                            origin="fcm_receiver_ha.py:_note_decrypt_failure_for_entry",
                        )
                    entry.async_start_reauth(hass)

    def _note_decrypt_success_for_entry(self, entry_id: str) -> None:
        """Feed a background-path decryption success into the matching coordinator(s).

        Symmetric counterpart of :meth:`_note_decrypt_failure_for_entry`. The
        background decode shares the coordinator's account-wide decrypt-failure
        counter, so a successful background decrypt must clear it just like a
        clean poll cycle does. Without this, push-only accounts whose scheduled
        polls stay idle could accumulate a couple of *non-consecutive* background
        decrypt failures up to the reauth threshold even though real locations
        keep arriving and decrypting -- a spurious reauth prompt. The same idle
        push-only condition would also strand the diagnostic encryption-key
        sensor on a stale ``shared_key_invalid`` / ``shared_key_missing`` status,
        so this proven decrypt heals the account-wide sensor state too via
        :meth:`note_background_decrypt_success`. Swallowed to debug so it never
        breaks the push path.
        """
        for coordinator in self._coordinators_for_entries({entry_id}):
            try:
                coordinator.note_background_decrypt_success()
            except Exception as err:  # noqa: BLE001 - never break the push path
                _LOGGER.debug(
                    "note_background_decrypt_success failed for entry %s: %s",
                    entry_id,
                    describe_exception(err),
                )

    async def _decode_background_location_async(  # noqa: PLR0911, PLR0912
        self, entry_id: str, hex_string: str
    ) -> JSONDict:
        """Decode background location using protobuf decoders.

        P1 fixes applied:
        - Protobuf parsing offloaded to executor (non-blocking)
        - Scoped cache provider prevents cross-contamination between entries
        """
        try:
            # P1-1: Offload CPU-bound protobuf parsing to executor
            device_update = await _call_in_executor(
                decoder_module.parse_device_update_protobuf, hex_string
            )
            cache = self._entry_caches.get(entry_id)
            if cache is None:
                _LOGGER.error(
                    "No cache instance available for entry %s during background decrypt",
                    entry_id,
                )
                return {}

            # P1-2: Use scoped context manager for proper cleanup
            with self._scoped_cache_provider(lambda: cache):
                try:
                    raw_locations = await async_decrypt_location_response_locations(
                        device_update, cache=cache
                    )
                except StaleOwnerKeyError as stale_err:
                    _LOGGER.info(
                        "Background location update skipped (stale tracker key) for "
                        "entry %s",
                        entry_id,
                    )
                    self._note_decrypt_failure_for_entry(
                        entry_id, stale=True, error=stale_err
                    )
                    return {}
                except DecryptionError as dec_err:
                    _LOGGER.warning(
                        "Background location decryption failed for entry %s: %s. "
                        "Escalates to re-authentication if it persists.",
                        entry_id,
                        dec_err,
                    )
                    self._note_decrypt_failure_for_entry(
                        entry_id, stale=False, error=dec_err
                    )
                    return {}
                except OwnerKeyLookupTransientError as transient_err:
                    # A transient owner-key lookup miss (partial/trailers-only
                    # server response, transport failure, otherwise unclassified
                    # miss). The credentials are presumed valid, so this is NOT a
                    # decrypt-failure budget event: do NOT call
                    # _note_decrypt_failure_for_entry and do NOT escalate to reauth.
                    # Pure severity downgrade to DEBUG; the next push/poll retries.
                    # Catches the _OwnerKeyRederiveRequired subclass too (fail-safe).
                    _LOGGER.debug(
                        "Background location update skipped (transient owner-key "
                        "miss, %s) for entry %s: %s; presumed valid keys, will retry",
                        type(transient_err).__name__,
                        entry_id,
                        transient_err,
                    )
                    return {}
                except InvalidTag:
                    _LOGGER.warning(
                        "Background location decryption auth failed (InvalidTag) "
                        "for entry %s. Cached identity key may be stale; "
                        "will retry on next update.",
                        entry_id,
                    )
                    return {}

            locations: list[JSONDict] = (
                raw_locations if raw_locations is not None else []
            )
            if not locations:
                return {}

            # Clear the shared reauth budget only on positive proof: at least one
            # *authenticated* coordinate report. Truthy-but-unauthenticated rows --
            # a metadata_only sentinel (only secrets-bundle key material) or a
            # SEMANTIC row (no decrypted coordinates) -- are non-empty but carry no
            # successful decrypt, so they must NOT clear the budget -- otherwise
            # report-less pushes interleaved with failures could keep a stale key
            # below the reauth threshold. This mirrors the poll cycle's
            # cycle_had_successful_decrypt gate, which excludes the same shapes,
            # and lets non-consecutive background failures interleaved with genuine
            # pushes avoid stranding a healthy account at the reauth threshold.
            # Keyed on the source entry_id -- the cache that actually performed
            # this decrypt -- and symmetric with the failure paths above. Routed
            # fan-out targets share the source's FCM registration token, hence the
            # same account-wide owner_key, so the source success already proves
            # their key; clearing the routed target set instead would break the
            # source entry's own reset invariant.
            if any_real_location_record(locations):
                self._note_decrypt_success_for_entry(entry_id)

            best_record: Mapping[str, Any] | None = None
            best_key: tuple[float, int, int] | None = None

            for record in locations:
                raw_last_seen = record.get("last_seen")
                if raw_last_seen is None:
                    continue
                try:
                    last_seen = float(raw_last_seen)
                except (TypeError, ValueError):
                    continue
                if math.isnan(last_seen):
                    continue

                has_coordinates = int(
                    record.get("latitude") is not None
                    and record.get("longitude") is not None
                )
                has_altitude = int(record.get("altitude") is not None)

                key = (last_seen, has_coordinates, has_altitude)
                if best_key is None or key > best_key:
                    best_record = record
                    best_key = key

            return dict(best_record) if best_record is not None else dict(locations[0])
        except Exception as err:  # noqa: BLE001
            _LOGGER.error(
                "Failed to decode background location data: %s",
                describe_exception(err),
            )
            return {}

    # -------------------- Credentials & stop --------------------

    def _on_credentials_updated_for_entry(self, entry_id: str, creds: Any) -> None:
        """Update in-memory creds for the entry and persist asynchronously."""
        # AP4 race guard (two tiers): the old FCM client is not awaited during
        # the synchronous teardown in ``unregister_coordinator``, so it can still
        # fire this credentials-updated callback *after* ``_purge_entry_tokens``
        # cleared the entry. Without this guard the callback would re-write
        # ``self.creds[entry_id]``, restore token routing and queue persistence /
        # pending creds, making a removed entry look known again to
        # ``async_reregister_fcm``.
        #
        # The tombstone alone is insufficient on a fast reload (finding 4c):
        # ``unregister_coordinator`` bumps the generation but does NOT pop the
        # reused client, and ``register_coordinator`` clears the tombstone for
        # the new incarnation while a cancelled-but-unwinding supervisor is still
        # inside ``checkin_or_register`` on that same client. Its synchronously-
        # fired callback would then pass a tombstone-only gate and clobber the
        # LIVE generation's creds, routing and fatal latch -- exactly the writes
        # ``_register_for_fcm_entry`` guards post-await. Resolve the *driving*
        # generation published by the supervisor for this handshake and gate on
        # it: a superseded supervisor is suppressed, a live handshake (matching
        # generation) and an out-of-band refresh (no driving generation -> pure
        # tombstone window) still write. ``register_coordinator`` clears the
        # tombstone on the next setup so a genuine reload still accepts creds.
        driving = _FCM_DRIVING_GENERATION.get()
        generation = (
            driving[1] if driving is not None and driving[0] == entry_id else None
        )
        if self._entry_writes_suppressed(entry_id, generation):
            _LOGGER.debug(
                "[entry=%s] Ignoring credentials-updated callback for suppressed "
                "entry (tombstoned or superseded generation)",
                entry_id,
            )
            return
        normalized: Any = creds
        if isinstance(normalized, str):
            try:
                normalized = json.loads(normalized)
            except json.JSONDecodeError:
                _LOGGER.debug(
                    "[entry=%s] FCM credentials arrived as non-JSON string", entry_id
                )
        self.creds[entry_id] = normalized if isinstance(normalized, dict) else None

        # Update token routing from fresh creds if possible.  Read strictly
        # entry-scoped: the creds for this entry were just written above, so
        # ``get_fcm_token``'s cross-entry fallback could only route a *foreign*
        # account's token under this entry when the update is tokenless -- the
        # same leak class as the forced first-locate reconnect refresh (Codex
        # #1150 follow-ups).  A tokenless update must route nothing here.
        token = self._entry_scoped_fcm_token(entry_id)
        if token:
            self._update_token_routing(token, {entry_id})
            self._dispatch_to_hass_loop(
                self._persist_routing_token(entry_id, token),
                label=f"persist_routing_token_{entry_id}",
            )
        self._clear_fatal_error_for_entry(
            entry_id, reason="Credentials updated for entry"
        )

        self._dispatch_to_hass_loop(
            self._async_save_credentials_for_entry(entry_id),
            label=f"save_credentials_{entry_id}",
        )
        _LOGGER.info("[entry=%s] FCM credentials updated", entry_id)

    def _persist_credentials_to_entry(
        self, entry_id: str, creds: Mapping[str, Any]
    ) -> None:
        """Write a refreshed FCM registration back into the config entry.

        The config entry, not the token cache, is the durable source of truth:
        ``async_setup_entry`` re-seeds the entry's secrets bundle into the cache
        on every start, so a credential that only lives in the cache is
        overwritten by the bundle's older copy on the next reload or restart.
        A registration obtained by a later (re-)registration is exactly such a
        value, and it is worthless if the next start restores the dead
        registration it replaced.

        Only the ``fcm_credentials`` member of an existing bundle is rewritten,
        together with the top-level copy the config flow may have left beside
        it. An entry without a bundle (individual-token sign-in) has nothing the
        seed would clobber, and a missing bundle is not invented here. The
        payload is the entry data read at write time, so a concurrent change to
        another key is not rolled back.

        ``async_update_entry`` mutates the in-memory entry and schedules the
        debounced store write; a reload in between reads the updated entry, so
        no await is needed here.
        """
        # The caller-side gate lives in ``_on_credentials_updated_for_entry``;
        # re-check it here because that check ran before the persistence task
        # was dispatched, and an entry torn down in between must not be written
        # again (TOCTOU).
        if entry_id in self._unregistered:
            return
        hass = self._hass
        entries = getattr(hass, "config_entries", None) if hass is not None else None
        lookup = getattr(entries, "async_get_entry", None)
        update = getattr(entries, "async_update_entry", None)
        if not callable(lookup) or not callable(update):
            return
        entry = lookup(entry_id)
        if entry is None:
            return
        data = getattr(entry, "data", None)
        if not isinstance(data, Mapping):
            return
        bundle = data.get(DATA_SECRET_BUNDLE)
        if not isinstance(bundle, Mapping):
            return
        updated_bundle = dict(bundle)
        updated_bundle["fcm_credentials"] = dict(creds)
        updated_data = dict(data)
        updated_data[DATA_SECRET_BUNDLE] = updated_bundle
        # The config flow also stores the same value at the top level of the
        # entry data. Refresh that copy where it exists so the entry does not
        # carry two contradictory registrations; a missing copy is not created
        # here, because the seed reads the bundle and nothing reads this key.
        if "fcm_credentials" in updated_data:
            updated_data["fcm_credentials"] = dict(creds)
        try:
            update(entry, data=updated_data)
        except Exception as err:  # noqa: BLE001 - durability is best-effort
            _LOGGER.debug(
                "[entry=%s] Failed to persist the refreshed FCM registration to "
                "the config entry: %s",
                entry_id,
                describe_exception(err),
            )
        else:
            _LOGGER.info(
                "[entry=%s] Persisted the refreshed FCM registration to the "
                "config entry",
                entry_id,
            )

    async def _async_save_credentials_for_entry(self, entry_id: str) -> None:
        """Persist current credentials to the entry and its TokenCache."""
        creds = self.creds.get(entry_id)
        if isinstance(creds, Mapping):
            self._persist_credentials_to_entry(entry_id, creds)
        cache = self._entry_caches.get(entry_id)
        if cache is not None:
            try:
                await cache.set("fcm_credentials", creds)
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "[entry=%s] Failed to save the FCM registration to entry cache: %s",
                    entry_id,
                    describe_exception(err),
                )
            else:
                self._pending_creds.pop(entry_id, None)
            return

        for coordinator in self.coordinators.copy():
            entry = getattr(coordinator, "config_entry", None)
            cache = getattr(coordinator, "cache", None)
            if entry is None or entry.entry_id != entry_id or cache is None:
                continue
            self._entry_caches[entry_id] = cache
            try:
                await cache.set("fcm_credentials", creds)
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "[entry=%s] Failed to save the FCM registration to entry cache: %s",
                    entry_id,
                    describe_exception(err),
                )
            else:
                self._pending_creds.pop(entry_id, None)
            return

        # Defer until cache available
        if (
            entry_id not in self._pending_creds
            or creds is not self._pending_creds[entry_id]
        ):
            self._pending_creds[entry_id] = creds if isinstance(creds, dict) else None

    def request_stop(self) -> None:
        """Signal a cooperative stop for all supervisors without awaiting."""
        for eid, evt in self._stop_evts.items():
            evt.set()
            task = self.supervisors.get(eid)
            if task:
                task.cancel()

    async def async_reregister_fcm(self, entry_id: str) -> bool:
        """Public API: invalidate FCM tokens and re-register for a specific entry.

        Preserves a *complete* GCM device identity (android_id/security_token)
        for a fast ``reregister_keeping_identity()``; a partial/corrupt identity
        is dropped so the next attempt does a full ``checkin_or_register()``
        handshake instead of looping on broken creds.

        Returns True if re-registration was initiated (supervisor restarted),
        False if the entry is not known to this receiver.
        """
        # A torn-down entry must never be revived from here. unregister_coordinator
        # (sync, via async_on_unload) tombstones the entry and purges its creds, but
        # it cannot await pc.stop(), so the live client lingers in self.pcs until the
        # cancelled supervisor (or async_stop) drains it. The creds/pcs membership
        # guard below would still pass on that residual client and restart a
        # supervisor for an entry mid-teardown (zombie). _unregistered is the single
        # source of truth for "torn down" (see _entry_writes_suppressed); consult it
        # first and bail before doing any teardown work. Cleared by
        # register_coordinator on the next setup.
        if entry_id in self._unregistered:
            _LOGGER.debug(
                "[entry=%s] Cannot re-register FCM: entry torn down (tombstoned)",
                entry_id,
            )
            return False

        if entry_id not in self.pcs and entry_id not in self.creds:
            _LOGGER.warning(
                "[entry=%s] Cannot re-register FCM: entry not known to receiver",
                entry_id,
            )
            return False

        # 1. Invalidate FCM tokens (keeps GCM identity)
        await self._invalidate_fcm_tokens(entry_id)

        # 2. Stop the supervisor for this entry
        stop_evt = self._stop_evts.get(entry_id)
        if stop_evt is not None:
            stop_evt.set()
        task = self.supervisors.pop(entry_id, None)
        if task is not None:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                pass

        # 3. Reset the stop event BEFORE stopping the client to prevent
        #    a race where concurrent code sees the old (set) event.
        self._stop_evts[entry_id] = asyncio.Event()

        # 4. Stop the client so it can be re-created
        pc = self.pcs.pop(entry_id, None)
        if pc is not None:
            try:
                await asyncio.wait_for(pc.stop(), timeout=5.0)
            except (TimeoutError, ConnectionError, asyncio.CancelledError):
                pass
            except Exception:  # noqa: BLE001
                pass

        # 4b. Cancel any in-flight data-starvation watchdog reconnect task and
        #     drop its backoff/throttle state for this entry, mirroring the
        #     teardown path ``_purge_entry_tokens``. Step 2 cancels the
        #     supervisor and step 4 stops the client, but a ``zombie`` reconnect
        #     (``_reconnect_for_starvation``, which awaits
        #     ``_force_first_locate_reconnect``) runs in a SEPARATE task; left
        #     alive it would race step 5 and build a *second* live
        #     ``FcmPushClient`` for the same entry -- the two-instance symptom
        #     behind the cross-registration subtype-mismatch / InvalidTag
        #     reports. Fire-and-forget ``cancel()`` (no await), exactly like
        #     ``_purge_entry_tokens``: the zombie task awaits a fresh STARTED pc
        #     that only the supervisor's outer loop (restarted just below in
        #     step 5) can build, so awaiting it here would deadlock (see the
        #     ``_zombie_reconnect_tasks`` note in ``__init__``).
        self._zombie_reconnect_attempts.pop(entry_id, None)
        self._zombie_next_allowed_reconnect_monotonic.pop(entry_id, None)
        self._zombie_next_eval_monotonic.pop(entry_id, None)
        zombie_task = self._zombie_reconnect_tasks.pop(entry_id, None)
        if zombie_task is not None and not zombie_task.done():
            zombie_task.cancel()

        # 5. Restart the supervisor (will re-register with fresh tokens)
        cache = self._entry_caches.get(entry_id)
        await self._start_supervisor_for_entry(entry_id, cache)

        _LOGGER.info(
            "[entry=%s] FCM re-registration initiated; "
            "supervisor restarted with invalidated tokens",
            entry_id,
        )
        return True

    async def async_stop(self, timeout: float = 5.0) -> None:
        """Stop all supervisors and clients (graceful, bounded)."""
        for eid, evt in self._stop_evts.items():
            evt.set()
        for eid, task in list(self.supervisors.items()):
            if task:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=timeout)
                except TimeoutError:
                    _LOGGER.warning(
                        "[entry=%s] FCM supervisor did not stop within %.1fs; detaching",
                        eid,
                        timeout,
                    )
                except asyncio.CancelledError:
                    pass
        self.supervisors.clear()

        # Stop all clients
        for eid, pc in list(self.pcs.items()):
            try:
                await asyncio.wait_for(pc.stop(), timeout=timeout)
            except TimeoutError:
                _LOGGER.warning(
                    "[entry=%s] FCM client did not stop within %.1fs; detaching",
                    eid,
                    timeout,
                )
            except ConnectionError as err:
                _LOGGER.debug(
                    "[entry=%s] FCM client stop network error: %s",
                    eid,
                    describe_exception(err),
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "[entry=%s] FCM client stop unexpected error: %s",
                    eid,
                    describe_exception(err),
                )
            finally:
                self.pcs.pop(eid, None)
                self._purge_entry_tokens(eid)

        self.last_stop_monotonic = time.monotonic()
        _LOGGER.info("FCM receiver stopped")

    def _purge_entry_debounce(self, entry_id: str) -> None:
        """Cancel and drop the debounce trias for *entry_id* (AP7).

        The push-path debounce maps (``_pending`` / ``_pending_targets`` /
        ``_flush_tasks``) are keyed by ``(source_entry, device_id)`` and are only
        ever cleaned per key inside ``_flush``. No teardown path iterates them
        by entry, so a flush task still pending at unregister keeps running and
        may write to an already-removed coordinator, while ``_pending`` /
        ``_pending_targets`` leak until the next flush. ``task.cancel()`` is safe
        from sync code and idempotent.

        Two membership relations matter (Codex follow-up):

        * **Keyed by this entry** (``key[0] == entry_id``): drop the whole trias.
        * **A recipient of another entry's flush** (``entry_id`` is a member of
          ``_pending_targets[key]`` for a key owned by a *different* source
          entry): the debounce key is the source entry, not every recipient, so
          a fan-out keyed elsewhere can still target this entry. Remove it from
          those target sets; if that empties the set the flush has no remaining
          recipients, so cancel and drop it (an empty set fans out to no one,
          while ``None`` means broadcast). Broadcast fallbacks (target set is
          ``None``) are device-scoped, not entry-scoped, and are left untouched.

        Without the second relation a reload within the debounce window could
        deliver a pre-teardown payload to the new coordinator of this entry.
        """
        for key in [key for key in self._flush_tasks if key[0] == entry_id]:
            task = self._flush_tasks.pop(key, None)
            if task is not None:
                task.cancel()
        pending_keys = {
            key
            for key in (*self._pending, *self._pending_targets)
            if key[0] == entry_id
        }
        for key in pending_keys:
            self._pending.pop(key, None)
            self._pending_targets.pop(key, None)

        # Drop this entry from the target sets of flushes keyed by other
        # entries. Keys owned by this entry were already removed above, so any
        # key remaining here is keyed by a different source entry.
        for key in list(self._pending_targets):
            targets = self._pending_targets.get(key)
            if not targets or entry_id not in targets:
                continue  # broadcast (None) or not a recipient
            targets.discard(entry_id)
            if targets:
                continue  # other recipients remain; keep the flush
            # No recipients left: cancel and drop the orphaned flush.
            self._pending_targets.pop(key, None)
            self._pending.pop(key, None)
            task = self._flush_tasks.pop(key, None)
            if task is not None:
                task.cancel()

    def _purge_entry_tokens(self, entry_id: str) -> None:
        """Remove all routing references and per-entry runtime state for an entry."""
        self._entry_last_activity_monotonic.pop(entry_id, None)
        # Data-starvation watchdog state (LC-13): drop the data/locate clocks and
        # the backoff/count/throttle records, and cancel any in-flight watchdog
        # reconnect task so it cannot keep working against the torn-down ``pc``
        # after the entry is purged.
        self._entry_last_data_delivery_monotonic.pop(entry_id, None)
        self._entry_last_locate_sent_monotonic.pop(entry_id, None)
        self._zombie_reconnect_attempts.pop(entry_id, None)
        self._zombie_next_allowed_reconnect_monotonic.pop(entry_id, None)
        self._zombie_next_eval_monotonic.pop(entry_id, None)
        zombie_task = self._zombie_reconnect_tasks.pop(entry_id, None)
        if zombie_task is not None and not zombie_task.done():
            zombie_task.cancel()
        # First-locate reconnect serialization state (#1150 follow-up): drop the
        # per-entry lock and the consumed-replacement record so a later reload
        # under the same entry_id starts from a clean slate.
        self._first_locate_locks.pop(entry_id, None)
        self._first_locate_reconnect_done.pop(entry_id, None)
        # AP1 (finding 2a): fatal retry counters are only cleared on the success
        # path (registration ok), so they leak per teardown without this.
        self._fatal_retry_counts.pop(f"{entry_id}:auth", None)
        self._fatal_retry_counts.pop(f"{entry_id}:endpoint", None)
        # AP2 (finding 2b): per-entry health snapshots are written by
        # _update_entry_health but never purged; a stale snapshot makes the
        # first healthy transition after a same-id reload look like a no-change
        # and suppresses its listener push.
        self._entry_health.pop(entry_id, None)
        self._entry_last_connected_wall.pop(entry_id, None)
        # AP3 (finding 2c): in-memory creds keep async_reregister_fcm's guard
        # (entry_id in self.creds) True after teardown, which can revive a dead
        # entry (zombie supervisor). The on-disk cache is untouched, so a reload
        # reloads creds via _ensure_client_for_entry.
        self.creds.pop(entry_id, None)
        self._pending_creds.pop(entry_id, None)
        # AP7 (finding 2d): cancel pending debounce flush tasks and drop their
        # payloads so a flush cannot fire at an already-removed coordinator.
        self._purge_entry_debounce(entry_id)
        tokens = self._entry_to_tokens.pop(entry_id, set())
        self._pending_routing_tokens.pop(entry_id, None)
        if not tokens:
            return
        for token in tokens:
            entries = self._token_to_entries.get(token)
            if entries is None:
                continue
            entries = set(entries)
            entries.discard(entry_id)
            if entries:
                self._token_to_entries[token] = entries
            else:
                self._token_to_entries.pop(token, None)

    # -------------------- Public token accessor --------------------

    def _entry_scoped_fcm_token(self, entry_id: str) -> str | None:
        """Return the FCM token that belongs strictly to ``entry_id``.

        Reads the entry's persisted creds first, then the live client's
        credentials.  Unlike :meth:`get_fcm_token`, this never falls back to
        another entry's token, so a caller that requires per-entry correctness
        (the forced first-locate reconnect in a multi-account setup) can fail
        closed instead of routing a foreign account's token to the wrong entry.
        """
        creds = self.creds.get(entry_id)
        if isinstance(creds, dict):
            tok = (creds.get("fcm") or {}).get("registration", {}).get("token")
            if isinstance(tok, str) and tok:
                return tok
        # Also try the current client's live creds if present
        pc = self.pcs.get(entry_id)
        if pc:
            try:
                c = getattr(pc, "credentials", None)
                if isinstance(c, dict):
                    tok = (c.get("fcm") or {}).get("registration", {}).get("token")
                    if isinstance(tok, str) and tok:
                        return tok
            except Exception:
                pass
        return None

    def get_fcm_token(self, entry_id: str | None = None) -> str | None:
        """Return current FCM token (best-effort).

        If `entry_id` is provided, returns the token for that entry's client when available.
        Otherwise returns the token for the receiver's default entry when set,
        falling back to the first available token across clients.
        """
        target_entry = entry_id if entry_id is not None else self._default_entry_id

        if target_entry:
            scoped = self._entry_scoped_fcm_token(target_entry)
            if scoped:
                return scoped
        # Fallback: first available token across entries
        for c in self.creds.values():
            if isinstance(c, dict):
                tok = (c.get("fcm") or {}).get("registration", {}).get("token")
                if isinstance(tok, str) and tok:
                    return tok
        return None

    def set_default_entry_id(self, entry_id: str | None) -> None:
        """Record the preferred entry_id for legacy token lookups."""

        if isinstance(entry_id, str):
            self._default_entry_id = entry_id
            return
        self._default_entry_id = None

    # -------------------- Manual locate registration --------------------

    def _select_manual_locate_entry(  # noqa: PLR0912
        self, canonic_id: str
    ) -> tuple[str | None, TokenCache | None]:
        """Choose the best entry/cache for manual locate registration."""
        fallback_entry: str | None = None
        fallback_cache: TokenCache | None = None
        display_entry: str | None = None
        display_cache: TokenCache | None = None

        for coordinator in self.coordinators.copy():
            entry = getattr(coordinator, "config_entry", None)
            candidate_entry_id = (
                getattr(entry, "entry_id", None) if entry is not None else None
            )
            if candidate_entry_id is None:
                continue

            candidate_cache = self._entry_caches.get(candidate_entry_id)
            if candidate_cache is None:
                candidate_cache = getattr(coordinator, "cache", None) or getattr(
                    coordinator, "_cache", None
                )
                if candidate_cache is not None:
                    self._entry_caches[candidate_entry_id] = candidate_cache

            present = False
            present_fn = getattr(coordinator, "is_device_present", None)
            if callable(present_fn):
                try:
                    present = bool(present_fn(canonic_id))
                except Exception as err:  # noqa: BLE001
                    _LOGGER.debug(
                        "[entry=%s] Manual locate presence check failed for %s: %s",
                        candidate_entry_id,
                        canonic_id[:8],
                        describe_exception(err),
                    )

            has_display = False
            if not present:
                name_fn = getattr(coordinator, "get_device_display_name", None)
                if callable(name_fn):
                    try:
                        has_display = bool(name_fn(canonic_id))
                    except Exception:
                        has_display = False

            if present:
                return candidate_entry_id, candidate_cache

            if has_display:
                display_entry = candidate_entry_id
                display_cache = candidate_cache

            if fallback_entry is None:
                fallback_entry = candidate_entry_id
                fallback_cache = candidate_cache

        if display_entry is not None:
            return display_entry, display_cache

        return fallback_entry, fallback_cache

    async def _ensure_token_for_entry(self, entry_id: str) -> str | None:
        """Request and return a token for the given entry when missing."""
        token = self.get_fcm_token(entry_id)
        if token:
            return token

        ok_reg = await self._register_for_fcm_entry(entry_id)
        if not ok_reg:
            return None

        return self.get_fcm_token(entry_id)

    async def _await_entry_started(
        self,
        entry_id: str,
        fallback_pc: FcmPushClient[Any],
        max_wait_s: float,
        *,
        exclude_pc: FcmPushClient[Any] | None = None,
    ) -> FcmPushClient[Any] | None:
        """Poll the live entry client until it reaches ``STARTED``.

        Returns the ``STARTED`` client, or ``None`` if the budget expires.

        Re-reads ``self.pcs[entry_id]`` on every poll (falling back to
        ``fallback_pc`` only while the slot is momentarily empty) so a concurrent
        supervisor restart that stops the captured client and swaps a replacement
        into the slot is honored — the stale object may never transition while
        the replacement reaches ``STARTED``.

        ``exclude_pc`` (used after a forced reconnect) requires a *different*
        instance to count, so a stale client that still lingers in ``STARTED`` is
        not mistaken for the fresh replacement.
        """
        _POLL_INTERVAL_S = 0.3
        waited = 0.0
        while waited < max_wait_s:
            current_pc = self.pcs.get(entry_id, fallback_pc)
            if self._is_started(current_pc) and (
                exclude_pc is None or current_pc is not exclude_pc
            ):
                return current_pc
            await asyncio.sleep(_POLL_INTERVAL_S)
            waited += _POLL_INTERVAL_S
        return None

    def _is_started(self, pc: FcmPushClient[Any]) -> bool:
        """True if *pc* is currently in the ``STARTED`` run state.

        Single definition of "listening" shared by the readiness poll and the
        first-locate reconnect short-circuits, so both fail closed on a client
        that is not actually STARTED (e.g. a fresh, not-yet-started replacement
        a concurrent supervisor swapped in).
        """
        if FcmPushClientRunState is None:
            return False
        return bool(getattr(pc, "run_state", None) == FcmPushClientRunState.STARTED)

    def _session_age_s(self, pc: FcmPushClient[Any]) -> float | None:
        """Age (seconds) since this client's STARTED transition, or ``None``.

        Anchored on the client's own ``_started_monotonic`` — stamped where the
        library assigns ``run_state = STARTED`` — not the supervisor's
        ``last_start_monotonic``, which is recorded *before* ``pc.start()`` and
        before the listener reaches STARTED.  The readiness gate deliberately
        allows a fresh MCS connection ~15-20s to reach STARTED; measuring the
        churn window from the supervisor start would let a slow-but-fresh session
        age past ``_CHURN_WINDOW_S`` before its first STARTED observation and skip
        the very reconnect this heals (Codex #1150 follow-up).  Being per-client,
        it is also exact under Home Assistant multi-entry — no aggregate blur.

        Returns ``None`` until the client has reached STARTED at least once; the
        manual-locate caller only consults this for an already-STARTED client, so
        ``None`` there means "library without a STARTED stamp" and fails safe (no
        reconnect).
        """
        started: float | None = getattr(pc, "_started_monotonic", None)
        if started is None:
            return None
        return max(time.monotonic() - started, 0.0)

    def _needs_first_locate_reconnect(self, pc: FcmPushClient[Any]) -> bool:
        """True if a fresh, not-yet-delivering session should reconnect once.

        Trigger (T-A + per-entry sharpening): the session is *young*
        (STARTED age < ``_CHURN_WINDOW_S``, measured from this client's own
        STARTED transition) **and** this client has not delivered any data
        message since login.  Delivery is read from the library's dedicated
        ``_first_data_message_delivered`` flag, which is reset to ``False`` on
        each successful login and set ``True`` only when a ``DataMessageStanza``
        is *actually delivered* (decrypted and dispatched to the callback).  It
        deliberately does *not* read ``persistent_ids`` for this: that list
        records *every* selective-acked message (including dropped
        foreign-subtype/control pushes) because it is the RMQ2 login dedup, so a
        dropped orphan push arriving first would falsely mark the session as
        "has delivered" — the two contracts (all acked vs. only delivered)
        conflict on one field, so delivery proof is a separate signal.  It also
        deliberately does *not* use
        the activity clock (``last_message_time`` / ``_entry_last_activity_
        monotonic``), which a bare heartbeat ack also advances and which would
        therefore never separate a broken fresh session (heartbeats, zero data)
        from a healthy one.  A healthy, already-delivering session (e.g. a
        long-running HA entry) is thus never torn down, independent of the
        session age.
        """
        age = self._session_age_s(pc)
        if age is None or age >= _CHURN_WINDOW_S:
            return False
        delivered = bool(getattr(pc, "_first_data_message_delivered", False))
        return not delivered

    async def _force_first_locate_reconnect(
        self, entry_id: str, stale_pc: FcmPushClient[Any], max_wait_s: float
    ) -> FcmPushClient[Any] | None:
        """Force one cooperative reconnect for a fresh, not-yet-delivering session.

        Stops ``stale_pc``, nudges the supervisor to rebuild immediately, and
        waits (``max_wait_s`` budget) for the *replacement* instance to reach
        STARTED.  Returns the fresh client, or ``None`` if it never starts (the
        caller then fails closed, exactly like the readiness gate).  The
        supervisor owns the rebuild, so no new race surface is introduced against
        it.  Part of the first-locate-delivery fix (#1148/#1149 family): STARTED
        is necessary but not sufficient for the first push to arrive.
        """
        session_age = self._session_age_s(stale_pc)
        _LOGGER.debug(
            "[entry=%s] Fresh FCM session (age=%.1fs) reached STARTED but has not "
            "delivered a data message yet; forcing one reconnect before the first "
            "locate",
            entry_id,
            session_age if session_age is not None else -1.0,
        )
        try:
            await stale_pc.stop()
        except Exception as err:  # noqa: BLE001
            # A stop() failure on an already-dying client is benign; the
            # supervisor rebuild is driven by the nudge below.
            _LOGGER.debug(
                "[entry=%s] Forced-reconnect stop() raised (ignored): %s",
                entry_id,
                describe_exception(err),
            )
        self.nudge_retry(entry_id)
        return await self._await_entry_started(
            entry_id, stale_pc, max_wait_s, exclude_pc=stale_pc
        )

    def _get_first_locate_lock(self, entry_id: str) -> asyncio.Lock:
        """Return the per-entry first-locate reconnect lock, creating it lazily.

        Runs in the event loop with no ``await`` between the read and the
        store, so the lazy creation is atomic against other callers.
        """
        lock = self._first_locate_locks.get(entry_id)
        if lock is None:
            lock = asyncio.Lock()
            self._first_locate_locks[entry_id] = lock
        return lock

    async def _reconnect_for_first_locate(
        self, entry_id: str, max_wait_s: float
    ) -> FcmPushClient[Any] | None:
        """Serialise the one-shot first-locate reconnect for *entry_id*.

        Concurrent manual locates for different trackers on the same entry must
        not each tear down the fresh session (Codex #1150 follow-up): only the
        first caller forces the single cooperative reconnect, and concurrent or
        sequential callers await it and reuse the same replacement. The live
        client is re-read under the per-entry lock because a concurrent caller
        may already have replaced it. Returns the client to hand the locate to,
        or ``None`` when no started client is available (the caller then fails
        closed exactly like the readiness gate).
        """
        async with self._get_first_locate_lock(entry_id):
            current = self.pcs.get(entry_id)
            if current is None:
                # The supervisor tore the client down; fail closed.
                return None
            reuse_current = (
                # This exact replacement already had its one forced reconnect
                # (a concurrent caller won the race), ...
                self._first_locate_reconnect_done.get(entry_id) is current
                # ... or another caller's reconnect already delivered / the
                # session is no longer a fresh, undelivered one.
                or not self._needs_first_locate_reconnect(current)
            )
            if reuse_current:
                # Reuse the live client instead of forcing another reconnect,
                # but only if it is actually STARTED. A concurrent supervisor
                # swap may have installed a fresh, not-yet-started client here
                # between the branch pre-check and this lock; handing the locate
                # token to a non-listening client would reproduce the very
                # timeout this heals, so fail closed like the readiness gate.
                return current if self._is_started(current) else None
            fresh_pc = await self._force_first_locate_reconnect(
                entry_id, current, max_wait_s
            )
            if fresh_pc is not None:
                self._first_locate_reconnect_done[entry_id] = fresh_pc
            return fresh_pc

    # -------------------- Data-starvation liveness watchdog --------------------

    def _is_data_starved(self, entry_id: str, now: float) -> bool:  # noqa: PLR0911
        """True if *entry_id* is a data-starvation zombie at monotonic time *now*.

        Pure and side-effect-free (no IO): safe to call every evaluation tick.
        Returns True only when ALL hold:

        * a live client exists and is ``STARTED`` (``_is_started``);
        * the session is *established* (``age >= _CHURN_WINDOW_S``) -- a young
          session is the one-shot first-locate reconnect's job, not the watchdog;
        * the activity clock (K4) is *fresh* -- the heartbeat is alive, so this
          is a "STARTED, heartbeating, but silent" zombie, not a dead socket
          (that is the idle-reset's job);
        * at least ``FCM_DATA_STARVATION_S`` elapsed since the last real data
          delivery (a missing data clock counts as "never delivered", starved
          only in combination with the locate-sent gate below); and
        * a locate was sent for this entry AND it is at or after the last data
          delivery (>= 1 locate since the last delivery) -- this is what rules
          out an empty account with no pending locate.
        """
        pc = self.pcs.get(entry_id)
        if pc is None or not self._is_started(pc):
            return False
        age = self._session_age_s(pc)
        if age is None or age < _CHURN_WINDOW_S:
            return False
        last_activity = self._entry_last_activity_monotonic.get(entry_id)
        stale_after = max(self._activity_stale_after_s, 0.0)
        if last_activity is None:
            return False
        if stale_after > 0.0 and now - last_activity > stale_after:
            # Heartbeat is not fresh: a dead session, not a data-starved zombie.
            return False
        last_locate = self._entry_last_locate_sent_monotonic.get(entry_id)
        if last_locate is None:
            # No locate ever dispatched for this entry: nothing to expect.
            return False
        last_delivery = self._entry_last_data_delivery_monotonic.get(entry_id)
        if last_delivery is None:
            # Never delivered, but a locate was sent: starved once the locate is
            # old enough that a healthy session would have answered.
            return now - last_locate >= FCM_DATA_STARVATION_S
        if now - last_delivery < FCM_DATA_STARVATION_S:
            return False
        # Require >= 1 locate since the last delivery, else a quiet-but-healthy
        # account (delivered, then no new locate) would look starved.
        return last_locate >= last_delivery

    async def _reconnect_for_starvation(self, entry_id: str, max_wait_s: float) -> None:
        """Force one cooperative reconnect for a data-starved zombie session.

        Runs in its OWN task (scheduled by ``_maybe_reconnect_starved``), never
        inline in the supervisor monitor loop -- awaiting the reconnect there
        would deadlock, because ``_force_first_locate_reconnect`` waits for a
        fresh STARTED client that only the supervisor's outer loop can build.

        Acquires the shared ``_get_first_locate_lock(entry_id)`` (serialises
        against the first-/manual-locate reconnect so the same ``pc`` is never
        torn down twice), re-reads the live client under the lock, and -- if it
        is still a live STARTED session -- calls ``_force_first_locate_reconnect``
        UNCONDITIONALLY. It deliberately does NOT go through
        ``_reconnect_for_first_locate``: that method's reuse gate
        (``not _needs_first_locate_reconnect(current)``) is always True for an
        aged zombie (``_needs_first_locate_reconnect`` returns False once
        ``age >= _CHURN_WINDOW_S``) and would return ``current`` WITHOUT
        reconnecting. The backoff / count / one-in-flight guard has already made
        the reconnect decision before this runs. ``_first_locate_reconnect_done``
        is left untouched (that latch belongs to the first-locate path).
        """
        async with self._get_first_locate_lock(entry_id):
            current = self.pcs.get(entry_id)
            if current is None or not self._is_started(current):
                # The supervisor tore the client down (or it is no longer
                # STARTED) between scheduling and lock acquisition; nothing to do.
                return
            await self._force_first_locate_reconnect(entry_id, current, max_wait_s)

    def _maybe_reconnect_starved(self, entry_id: str, now: float) -> None:
        """Schedule a watchdog reconnect if *entry_id* is a starved zombie.

        Called from the supervisor monitor loop (throttled to
        ``FCM_ZOMBIE_CHECK_INTERVAL_S``). Detection + scheduling only: it never
        awaits the reconnect (that runs as a separate task). Guards:

        * predicate ``_is_data_starved`` must hold;
        * the backoff window must be open
          (``now >= _zombie_next_allowed_reconnect_monotonic``);
        * the hard reconnect ceiling ``FCM_ZOMBIE_MAX_RECONNECTS`` must not be
          reached; and
        * no watchdog reconnect task may be in flight for this entry.

        When all pass it schedules ``_reconnect_for_starvation`` via
        ``hass.async_create_task`` and, only once that task is created, commits
        the next (exponentially larger, capped) backoff window and the
        incremented attempt count. If scheduling raises ``RuntimeError`` (event
        loop closing during shutdown) it leaves all scheduler state untouched so
        a later tick can retry, rather than spending an attempt with no task.
        """
        if self._hass is None:
            return
        existing = self._zombie_reconnect_tasks.get(entry_id)
        if existing is not None and not existing.done():
            return  # one-in-flight guard (b): a reconnect is already running.
        if not self._is_data_starved(entry_id, now):
            return
        next_allowed = self._zombie_next_allowed_reconnect_monotonic.get(entry_id)
        if next_allowed is not None and now < next_allowed:
            return  # still inside the current backoff window.
        attempts = self._zombie_reconnect_attempts.get(entry_id, 0)
        if attempts >= FCM_ZOMBIE_MAX_RECONNECTS:
            return  # hard ceiling reached for this starvation episode.
        # Plan the next backoff window (exponential from BASE, doubled per
        # attempt, capped) and record the attempt before scheduling.
        backoff = min(
            FCM_ZOMBIE_RECONNECT_BACKOFF_BASE_S * (2**attempts),
            FCM_ZOMBIE_RECONNECT_BACKOFF_CAP_S,
        )
        # F1: bound the reconnect's wait-for-STARTED to the conservative
        # readiness budget (~22s), NOT FCM_DATA_STARVATION_S (900s).
        # ``_reconnect_for_starvation`` holds the shared per-entry first-locate
        # lock while awaiting STARTED; a 900s budget would starve a concurrent
        # manual locate on the same entry for up to 15 minutes.  The 900s
        # starvation threshold still gates the reconnect *decision* above; only
        # the STARTED-wait is capped here.  Shared SSOT with the native locate
        # path via ``_max_ready_wait_s`` so the two budgets cannot drift.
        max_wait_s = _max_ready_wait_s()
        # F3: schedule the task FIRST and commit the backoff-window / attempt
        # counter only once it is guaranteed created.  During HA shutdown the
        # event loop is closing and ``async_create_task`` raises ``RuntimeError``
        # (same failure mode guarded at the direct-schedule paths above); if the
        # counter were bumped before that raise, this tick would "spend" a
        # reconnect attempt with no task ever running, drifting the entry toward
        # ``FCM_ZOMBIE_MAX_RECONNECTS`` while never actually reconnecting.  On
        # failure we leave all scheduler state untouched so a post-restart tick
        # can retry.  There is no ``await`` between task creation and return, so
        # the task body cannot run before the state below is committed.
        try:
            task = self._hass.async_create_task(
                self._reconnect_for_starvation(entry_id, max_wait_s),
                name=f"fcm_zombie_reconnect_{entry_id}",
            )
        except RuntimeError:
            _LOGGER.debug(
                "[entry=%s] Deferred watchdog reconnect: event loop unavailable "
                "(shutdown in progress); scheduler state left intact for a later "
                "tick",
                entry_id,
            )
            return
        self._zombie_next_allowed_reconnect_monotonic[entry_id] = now + backoff
        self._zombie_reconnect_attempts[entry_id] = attempts + 1
        self._zombie_reconnect_tasks[entry_id] = task

        def _clear_task(_done: asyncio.Task[None], _eid: str = entry_id) -> None:
            if self._zombie_reconnect_tasks.get(_eid) is _done:
                self._zombie_reconnect_tasks.pop(_eid, None)

        task.add_done_callback(_clear_task)
        _LOGGER.info(
            "[entry=%s] FCM data starvation detected (attempt %d/%d); forcing a "
            "watchdog reconnect (next backoff %ds)",
            entry_id,
            attempts + 1,
            FCM_ZOMBIE_MAX_RECONNECTS,
            backoff,
        )

    async def _reconnect_and_refresh_token(
        self, entry_id: str, previous_token: str, max_wait_s: float
    ) -> str | None:
        """Force the first-locate reconnect, then return the *live* entry token.

        Fails closed (returns ``None``) when the replacement never reaches
        STARTED, or when the entry has no FCM token after the reconnect.

        The reconnect nudges a supervisor rebuild that may run a full
        re-registration (``checkin_or_register`` / ``reregister_keeping_identity``)
        and rotate the entry's FCM token after the caller captured
        ``previous_token``.  Returning the stale token would send the locate to
        the old registration while the live listener is subscribed with the new
        one, reproducing the very timeout this fix heals (Codex #1150 follow-up).
        Routing is re-pointed at the refreshed token only when it actually
        rotated; the stale mapping for a dead registration is benign (it receives
        no pushes) and is not eagerly purged here.

        The refreshed token is read strictly entry-scoped
        (:meth:`_entry_scoped_fcm_token`, not :meth:`get_fcm_token`): in a
        multi-account setup ``get_fcm_token``'s cross-entry fallback could return
        another account's token when this entry is momentarily tokenless after the
        rebuild, which would route a foreign token for this entry.  Failing closed
        is correct there (Codex #1150 follow-up).
        """
        fresh_pc = await self._reconnect_for_first_locate(entry_id, max_wait_s)
        if fresh_pc is None:
            _LOGGER.warning(
                "[entry=%s] Forced reconnect did not reach STARTED after %.1fs; "
                "aborting manual locate registration (fail-closed)",
                entry_id,
                max_wait_s,
            )
            return None

        refreshed = self._entry_scoped_fcm_token(entry_id)
        if not refreshed:
            _LOGGER.warning(
                "[entry=%s] FCM token unavailable after forced reconnect; "
                "aborting manual locate registration (fail-closed)",
                entry_id,
            )
            return None

        if refreshed != previous_token:
            _LOGGER.debug(
                "[entry=%s] FCM token rotated during forced first-locate "
                "reconnect; using the refreshed token for the locate",
                entry_id,
            )
            self._update_token_routing(refreshed, {entry_id})
            await self._persist_routing_token(entry_id, refreshed)
        return refreshed

    async def async_register_for_location_updates(
        self, canonic_id: str, callback: Callable[[str, str], None]
    ) -> str | None:
        """Register a manual locate callback and ensure an entry token is available."""

        if not isinstance(canonic_id, str) or not canonic_id:
            _LOGGER.warning("Manual locate registration skipped: missing canonical id")
            return None
        if not callable(callback):
            _LOGGER.error(
                "Manual locate registration for %s rejected: callback is not callable",
                canonic_id[:8],
            )
            return None

        entry_id, cache = self._select_manual_locate_entry(canonic_id)
        if entry_id is None:
            _LOGGER.warning(
                "Manual locate registration skipped for %s: no coordinator available",
                canonic_id[:8],
            )
            return None

        self.location_update_callbacks[canonic_id] = callback
        # Watchdog signal (AP2.5): a locate has now been dispatched for this
        # entry. The starvation predicate requires >= 1 locate since the last
        # delivery, so an empty account (no locates) never looks like a zombie.
        self._entry_last_locate_sent_monotonic[entry_id] = time.monotonic()

        token: str | None = None
        try:
            client = await self._ensure_client_for_entry(entry_id, cache)
            if client is None:
                _LOGGER.warning(
                    "[entry=%s] Manual locate registration failed: client unavailable",
                    entry_id,
                )
                return None

            await self._start_supervisor_for_entry(entry_id, cache)

            token = await self._ensure_token_for_entry(entry_id)

            if not token:
                _LOGGER.warning(
                    "[entry=%s] Manual locate registration failed: token unavailable",
                    entry_id,
                )
                return None

            self._update_token_routing(token, {entry_id})
            await self._persist_routing_token(entry_id, token)

            # Wait for the FCM client to be actively listening before sending
            # the location request.  Without this, the push notification from
            # Google may arrive before the MCS connection is established.
            pc = self.pcs.get(entry_id)
            if pc is not None:
                # Derive the readiness budget from the library's connection-retry
                # count instead of a fixed 15.0s.  A brand-new FCM identity's
                # first MCS connect can take the full retry budget
                # (FCM_CONNECTION_RETRY_COUNT attempts, ~15-20s per the note near
                # _listen()); a hard-coded 15.0s under-covered that window and let
                # the gate expire before STARTED on the very first run.  Adding a
                # small margin keeps a fresh registration from being abandoned
                # prematurely, which is the root of the deterministic first-run
                # locate timeout.
                _MAX_READY_WAIT_S = _max_ready_wait_s()
                started_pc = await self._await_entry_started(
                    entry_id, pc, _MAX_READY_WAIT_S
                )
                if started_pc is None:
                    # Fail closed: the client never started listening, so sending
                    # the location request would hand the token to a dead listener
                    # and the caller would block until an opaque 30s timeout.
                    # Clearing the local token means the function returns None (the
                    # sole caller, location_request.py, then reports a clear error
                    # and returns early) and the finally block pops the manual-
                    # locate callback so no stale registration leaks.
                    _LOGGER.warning(
                        "[entry=%s] FCM client not in STARTED state after %.1fs; "
                        "aborting manual locate registration (fail-closed)",
                        entry_id,
                        _MAX_READY_WAIT_S,
                    )
                    token = None
                elif self._needs_first_locate_reconnect(started_pc):
                    # AP1 (first-locate-delivery fix): a fresh, not-yet-delivering
                    # session reached STARTED but empirically will not receive the
                    # very first locate push until it reconnects.  Force exactly
                    # one cooperative reconnect (serialised per entry so concurrent
                    # locates on the same entry share one replacement instead of
                    # tearing down each other's fresh session), then re-read the
                    # token: the reconnect's supervisor rebuild may re-register and
                    # rotate the entry's FCM token after it was captured above.
                    # Fail closed if the replacement never starts or the token
                    # vanished (Codex #1150 follow-ups).
                    token = await self._reconnect_and_refresh_token(
                        entry_id, token, _MAX_READY_WAIT_S
                    )

            if token is not None:
                _LOGGER.debug(
                    "[entry=%s] Manual locate registration ready for %s",
                    entry_id,
                    canonic_id[:8],
                )
            return token
        finally:
            if not token:
                self.location_update_callbacks.pop(canonic_id, None)

    async def async_unregister_for_location_updates(self, canonic_id: str) -> None:
        """Remove a manual locate callback if registered."""

        if self.location_update_callbacks.pop(canonic_id, None) is not None:
            _LOGGER.debug(
                "Manual locate callback removed for %s",
                canonic_id[:8],
            )
