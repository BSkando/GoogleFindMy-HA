# custom_components/googlefindmy/coordinator.py
"""Data coordinator for Google Find My Device (async-first, HA-friendly).

Discovery vs. polling semantics:
- Every coordinator tick fetches the lightweight **full** Google device list.
- Presence and name/capability caches are updated for all devices.
- The published snapshot (`self.data`) contains **all** devices (for dynamic entity creation).
- The sequential **polling cycle** polls **only devices that are enabled** in Home Assistant's
  Device Registry (devices with `disabled_by is None`) for **this** config entry. Devices explicitly
  ignored via options are filtered out as well. Devices without a Device Registry entry yet are
  included to allow initial discovery.

Google Home semantic locations (note):
- When the Google Home filter identifies a "Google Home-like" semantic location,
  we substitute **Home zone coordinates** (lat/lon[/radius]) instead of forcing
  a zone label. This lets HA Core's zone engine set the state to `home`, which
  aligns with best practices.

Thread-safety and quality goals:
- All state mutations and task creations occur on HA's event loop thread.
- Public methods that may be invoked from background threads marshal execution
  onto the loop using a single hop (no chained hops).
- Owner-driven locates introduce a server-side purge/cooldown window; we respect
  this via **per-device poll cooldowns** with **dynamic guardrails** (min/max bounds),
  without changing any external API or entity fields.

Implementation notes (server behaviour, POPETS'25):
- The network applies *type-specific throttling* to crowdsourced reports:
  "In All Areas" reports are effectively throttled for ~10 minutes,
  "High Traffic" reports for ~5 minutes. We respect this by applying
  per-device cooldowns derived from an internal `_report_hint` (set by the
  decrypt/parse layer) without changing public APIs or entity attributes.
- The well-known "~9h" rate limit discussed in the paper applies to *finder*
  devices contributing reports, not to the owner pulling locations. We **document**
  this here for maintainers but do **not** enforce it client-side.

Authentication handling and HA best practices (Platinum standard):
- The **DataUpdateCoordinator** is the central place to detect auth failures.
- When the API raises `ConfigEntryAuthFailed`, we:
  1) Create (idempotently) a **Repairs issue** using Home Assistant's issue registry
     so the integration is marked with **"Reconfigure"** in the UI (system repairs).
  2) Fire a domain-scoped **event** so users can automate on top of the condition.
  3) Set an internal **flag** (`auth_error_active`) that the diagnostic binary_sensor
     can expose as `on` (see step 5.1-C, binary_sensor.py).
  4) Re-raise `ConfigEntryAuthFailed` from the coordinator so HA triggers the
     **Re-auth flow** defined in `config_flow.py` (platinum-standard behavior).
- As soon as any subsequent API call succeeds, we:
  1) Dismiss the Repairs issue,
  2) Fire a matching **OK event**,
  3) Clear the internal flag and push an update so the sensor flips back to `off`.

This module must not log secrets and must keep user-facing strings out of code; use translations instead.
# custom_components/googlefindmy/coordinator.py
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from types import ModuleType
from typing import TYPE_CHECKING, Any, Protocol, cast

# Operations classes - currently empty mixins for future method extraction
from .cache import CacheOperations
from .helpers.cache import (
    build_base_snapshot_entry as _build_base_snapshot_entry_impl,
)
from .helpers.cache import (
    determine_location_status,
    epoch_to_datetime_utc,
    is_presence_expired,
)
from .helpers.cache import (
    sanitize_decoder_row as _sanitize_decoder_row,
)
from .helpers.geo import (
    coerce_float as _coerce_float_impl,
)
from .helpers.stats import (
    ApiStatus,
    DiagnosticsBuffer,
    FcmStatus,
    format_recent_errors,
)
from .helpers.stats import (
    get_duration as _get_duration_impl,
)
from .helpers.stats import (
    short_error_message as _short_error_message_impl,
)
from .helpers.subentry import (
    format_epoch_utc as _format_epoch_utc_impl,
)
from .helpers.subentry import (
    normalize_epoch_seconds as _normalize_epoch_impl,
)
from .helpers.subentry import (
    parse_last_seen_timestamp as _parse_last_seen_impl,
)
from .identity import IdentityOperations
from .locate import LocateOperations
from .polling import PollingOperations
from .registry import RegistryOperations
from .subentry import SubentryMetadata, SubentryOperations

if TYPE_CHECKING:
    from homeassistant.core import Event

    from .. import ConfigEntrySubEntryManager
    from ..google_home_filter import GoogleHomeFilter as GoogleHomeFilterProtocol
else:  # pragma: no cover - typing fallback for runtime imports
    Event = Any
    GoogleHomeFilterProtocol = Any

from homeassistant.config_entries import (
    ConfigEntry,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import EVENT_DEVICE_REGISTRY_UPDATED

# IMPORTANT: make Common_pb2 import **mandatory** (integration packaging must include it).
# This avoids silent type/name drift and keeps source labels stable.
# NOTE: Import GoogleFindMyAPI directly for type hints; use _get_api_class() at
# call-time to respect package-level monkeypatches from tests.
from ..api import GoogleFindMyAPI
from ..const import (
    CACHE_KEY_CONTRIBUTOR_MODE,
    CACHE_KEY_LAST_MODE_SWITCH,
    # Credential meta for Repairs placeholders
    CONTRIBUTOR_MODE_HIGH_TRAFFIC,
    CONTRIBUTOR_MODE_IN_ALL_AREAS,
    DEFAULT_CONTRIBUTOR_MODE,
    # Core / options
    DEFAULT_MIN_POLL_INTERVAL,
    DEFAULT_OPTIONS,
    DEFAULT_SEMANTIC_DETECTION_RADIUS,
    DOMAIN,
    # Required symbols provided by const.py (5.1-A)
    EVENT_AUTH_ERROR,
    EVENT_AUTH_OK,
    OPT_IGNORED_DEVICES,
    OPT_SEMANTIC_LOCATIONS,
    UPDATE_INTERVAL,
    coerce_ignored_mapping,
)
from ..ha_typing import DataUpdateCoordinator

_LOGGER = logging.getLogger(__name__)

# Store original class at import time for monkeypatch detection in _get_api_class()
_OriginalGoogleFindMyAPI: type[GoogleFindMyAPI] = GoogleFindMyAPI


def _get_api_class() -> type[GoogleFindMyAPI]:
    """Return the GoogleFindMyAPI class, respecting monkeypatches.

    Tests and add-ons may monkeypatch `coordinator.main.GoogleFindMyAPI` (module level)
    or `coordinator.GoogleFindMyAPI` (package level) to inject mocks. This function
    checks both namespaces at call-time so those patches propagate correctly.

    Priority order:
    1. Module-level patch (coordinator.main.GoogleFindMyAPI) - used by existing tests
    2. Package-level patch (coordinator.GoogleFindMyAPI) - for downstream compatibility
    3. Direct import fallback
    """
    import sys

    # Check module-level patch first (existing tests use this)
    main_module = sys.modules.get("custom_components.googlefindmy.coordinator.main")
    if main_module is not None:
        patched = getattr(main_module, "GoogleFindMyAPI", None)
        # Only use if it differs from the original import (i.e., was actually patched)
        if patched is not None and patched is not _OriginalGoogleFindMyAPI:
            return patched  # type: ignore[no-any-return]

    # Check package-level patch (for downstream add-ons)
    coordinator_pkg = sys.modules.get("custom_components.googlefindmy.coordinator")
    if coordinator_pkg is not None:
        patched = getattr(coordinator_pkg, "GoogleFindMyAPI", None)
        if patched is not None and patched is not _OriginalGoogleFindMyAPI:
            return patched  # type: ignore[no-any-return]

    return _OriginalGoogleFindMyAPI


# --- Lightweight cache protocol for entry-scoped persistence -----------------
class CacheProtocol(Protocol):  # pylint: disable=unnecessary-ellipsis
    """Minimal cache protocol used by the coordinator.

    Implementations are provided by the integration's token/cache layer.
    """

    async def async_get_cached_value(self, key: str) -> Any:
        """Return a cached value for a given key (or None)."""
        ...

    async def async_set_cached_value(self, key: str, value: Any) -> None:
        """Persist a value under a given key (overwriting the previous one)."""
        ...


# --- Module constants (cooldowns & quorum) ---------------------------------
# Accept an empty device list only on the 2nd consecutive result (defers once)
_EMPTY_LIST_QUORUM = 2

# POPETS'25-informed throttling windows for crowdsourced reports
_COOLDOWN_MIN_IN_ALL_AREAS_S = 10 * 60  # 10 minutes
_COOLDOWN_MIN_HIGH_TRAFFIC_S = 5 * 60  # 5 minutes

# Maximum consecutive transient auth failures before triggering reauth.
# Transient failures (401 after token refresh) may self-heal as Google's
# backend propagates the refreshed token. We give it 3 poll cycles before
# asking the user to re-authenticate.
_MAX_TRANSIENT_AUTH_FAILURES = 3

# Guardrails for owner-driven locate cooldown
_COOLDOWN_OWNER_MIN_S = 60  # at least 1 minute
_COOLDOWN_OWNER_MAX_S = 15 * 60  # at most 15 minutes

# Sound UUID expiry after restart (prevents phantom re-triggers)
_SOUND_UUID_MAX_AGE_S = 30 * 60  # 30 minutes

# Minimum position change (meters) to accept location update without timestamp
_LOCATION_SIGNIFICANT_CHANGE_M = 50.0

# FCM error retry threshold before triggering re-auth
_FCM_ERROR_RETRY_THRESHOLD = 3

# Maximum delay before falling back to polling even when push is unavailable.
# [FIX: Reduce 300s -> 15s to allow degraded-mode polling quickly]
_PUSH_NOT_READY_TIMEOUT_S = 15
# Backward-compatible alias for legacy status tests; keep in sync with the
# primary timeout constant above.
_FCM_FALLBACK_POLL_AFTER_S = _PUSH_NOT_READY_TIMEOUT_S

# Altitude adjustments smaller than 1 m are considered noise for significance checks.
_ALTITUDE_SIGNIFICANT_DELTA_M = 1.0

# Predictive polling buffer to avoid requesting data before it is available server-side.
_PREDICTION_BUFFER_S = 45

# Fields consumed by get_active_device_identities from cached payloads/anchors.
_PERSISTED_METADATA_KEYS: tuple[str, ...] = (
    "battery_level",
    "device_type",
    "pair_date",
    "encrypted_account_key",
    "secrets_creation_date",
    "device_registration",
    "public_key_address",
    "device_type_information",
    "encrypted_user_secrets",
    "identity_key",
    "identity_key_candidates",
    "encrypted_identity_key",
    "encrypted_identity_key_candidates",
    "owner_key_version",
    "time_anchors_debug",
    # FIX: Add device metadata fields for phones
    "manufacturer",
    "model",
    "fast_pair_model_id",
)


def _update_preserve_metadata(
    target: dict[str, Any], source: Mapping[str, Any]
) -> None:
    """Merge source into target without overwriting persisted metadata keys with None.

    This prevents a payload with None values (e.g., from a phone device listing
    that lacks key material) from clobbering valid data already in the target
    (e.g., from a prior FCM locate response).

    For keys in _PERSISTED_METADATA_KEYS:
      - Only update if source value is not None
    For all other keys:
      - Normal dict.update() semantics apply
    """
    for key, value in source.items():
        if key in _PERSISTED_METADATA_KEYS:
            # Preserve existing non-None values: only overwrite if new value is not None
            if value is not None:
                target[key] = value
            # If value is None and key already exists with a value, keep the existing
        else:
            # Normal update for non-metadata keys
            target[key] = value


# _clamp is imported from coordinator_geo (module extraction Phase 1)


# DiagnosticsBuffer is imported from coordinator_stats (module extraction Phase 2)


# --- Epoch normalization (ms→s tolerant) -----------------------------------
def normalize_epoch_seconds(value: Any) -> int | None:
    """Return epoch seconds as an ``int`` with millisecond tolerance."""
    return _normalize_epoch_impl(value)


def _normalize_epoch_seconds(value: Any) -> int | None:
    """Backward-compatible alias for :func:`normalize_epoch_seconds`."""
    return _normalize_epoch_impl(value)


# NOTE: keep helper public for reuse in entities/system health snapshots.
def format_epoch_utc(value: Any) -> str | None:
    """Return an ISO 8601 UTC timestamp for epoch values (seconds or ms)."""
    return _format_epoch_utc_impl(value)


# --- Decoder-row Normalization & Attribute Helpers -------------------------
# _row_source_label, _sanitize_decoder_row, _get_common_pb2 moved to helpers/cache.py


def _parse_last_seen_timestamp(value: Any) -> float | None:
    """Parse a last_seen candidate into epoch seconds."""
    return _parse_last_seen_impl(value)


def _resolve_last_seen_from_attributes(
    attributes: Mapping[str, Any] | None, fallback: float | None
) -> float | None:
    """Prefer attribute-derived timestamps and fall back to provided default."""

    if not attributes:
        return fallback

    candidate: Any = attributes.get("last_seen")
    if candidate is None:
        candidate = attributes.get("last_seen_utc")

    ts = _parse_last_seen_timestamp(candidate)
    if ts is not None:
        return ts
    return fallback


def _as_ha_attributes(row: dict[str, Any] | None) -> dict[str, Any] | None:
    """Create a curated, stable attribute set for HA entities (recorder-friendly)."""
    if not row:
        return None
    r = _sanitize_decoder_row(row)

    def _cf(value: Any) -> float | None:
        try:
            candidate = float(value)
        except (TypeError, ValueError):
            return None
        return candidate if math.isfinite(candidate) else None

    lat = _cf(r.get("latitude"))
    lon = _cf(r.get("longitude"))
    acc = _cf(r.get("accuracy"))
    alt = _cf(r.get("altitude"))

    last_seen_iso = format_epoch_utc(r.get("last_seen"))
    last_seen_utc = r.get("last_seen_utc") or last_seen_iso

    out: dict[str, Any] = {
        "device_name": r.get("name"),
        "device_id": r.get("device_id") or r.get("id"),
        "status": r.get("status"),
        "semantic_name": r.get("semantic_name"),
        "battery_level": r.get("battery_level"),
        "last_seen": last_seen_iso,
        "last_seen_utc": last_seen_utc,
        "source_label": r.get("source_label"),
        "source_rank": r.get("source_rank"),
        "is_own_report": r.get("is_own_report"),
    }
    if lat is not None and lon is not None:
        out["latitude"] = lat
        out["longitude"] = lon
    if acc is not None:
        out["accuracy_m"] = acc
    if alt is not None:
        out["altitude_m"] = alt
    return {k: v for k, v in out.items() if v is not None}


class _RecorderHistoryProxy:
    """Lazy loader for Recorder history helpers."""

    _module: ModuleType | None = None

    def _load(self) -> ModuleType:
        if self._module is None:
            from homeassistant.components.recorder import history as history_module

            self._module = history_module
        return self._module

    def __getattr__(self, name: str) -> Any:  # pragma: no cover - proxy passthrough
        return getattr(self._load(), name)

    def __setattr__(
        self, name: str, value: Any
    ) -> None:  # pragma: no cover - proxy passthrough
        if name == "_module":
            super().__setattr__(name, value)
            return
        setattr(self._load(), name, value)

    def __delattr__(self, name: str) -> None:  # pragma: no cover - proxy passthrough
        if name == "_module":
            super().__delattr__(name)
            return
        try:
            delattr(self._load(), name)
        except AttributeError:
            # Mirror monkeypatch's raising=False semantics by ignoring missing attrs.
            return


recorder_history = _RecorderHistoryProxy()


def get_recorder(hass: HomeAssistant) -> Any:
    """Lazily import and return the Recorder instance for a hass object."""

    from homeassistant.components.recorder import get_instance as get_recorder_instance

    return get_recorder_instance(hass)


# -------------------------------------------------------------------------
# Synchronous history helper (runs in Recorder executor)
# -------------------------------------------------------------------------
def _sync_get_last_gps_from_history(
    hass: HomeAssistant, entity_id: str
) -> dict[str, Any] | None:
    """Fetch the last state with GPS coordinates from Recorder History.

    IMPORTANT:
        This function is synchronous and performs database I/O. It MUST be
        run in a worker thread (e.g., the Recorder's executor) to avoid
        blocking the Home Assistant event loop.

    Args:
        hass: The Home Assistant instance.
        entity_id: The entity ID of the device_tracker to query.

    Returns:
        A dictionary containing the last known location data, or None if not found.
    """
    try:
        # Minimal query: the last single change for this entity_id
        # API expects an iterable of entity_ids.
        changes = recorder_history.get_last_state_changes(hass, 1, [entity_id])
        samples = changes.get(entity_id, [])
        if not samples:
            return None

        last_state = samples[-1]
        attrs = getattr(last_state, "attributes", {}) or {}
        lat = attrs.get("latitude")
        lon = attrs.get("longitude")
        if lat is None or lon is None:
            return None

        last_seen_ts = _resolve_last_seen_from_attributes(
            attrs, last_state.last_updated.timestamp()
        )
        return {
            "latitude": lat,
            "longitude": lon,
            "accuracy": attrs.get("gps_accuracy"),
            "last_seen": last_seen_ts,
            "status": "Using historical data",
        }
    except Exception as err:
        _LOGGER.debug("History lookup failed for %s: %s", entity_id, err)
        return None


# StatusSnapshot, ApiStatus, FcmStatus are imported from coordinator_stats (Phase 2)


@dataclass
class SemanticLabelRecord:
    """Observed semantic label metadata cached for diagnostics."""

    label: str
    first_seen: float
    last_seen: float
    devices: set[str] = field(default_factory=set)

    def copy(self) -> SemanticLabelRecord:
        """Return a shallow copy with a cloned device set."""

        return replace(self, devices=set(self.devices))


@dataclass(slots=True)
class DeviceIdentity:
    """Stable identifier and key material for a tracker.

    Attributes:
        registry_id: Home Assistant device registry identifier for the tracker.
        canonical_id: Namespaced device identifier used by the integration's API.
        identity_key: Ephemeral identity key used to derive rotating EIDs.
        encrypted_identity_key: Encrypted EIK blob (hex-decoded) for on-demand decryption.
        owner_key_version: Version of the owner key used to encrypt the EIK.
        device_type: SpotDeviceType enum value reported by the tracker, when known.
        config_entry_id: Parent config entry ID that owns the tracker.
        fast_pair_model_id: Fast Pair model identifier advertised by the tracker.
        manufacturer: Tracker manufacturer string extracted from registration metadata.
        model: Tracker model string extracted from registration metadata.
        pair_date: Hypothesized tracker pairing epoch (seconds) derived from registrations
            or cached secrets; treated as advisory when reconciling EID timelines.
        secrets_creation_date: Creation time for the encrypted user secrets bundle, if
            present; surfaced for debugging because the exact server-side semantics are
            not yet confirmed.
        encrypted_account_key: Encrypted account key blob (hex-decoded) used for future
            account-key recovery flows.
        public_key_address: Encrypted SHA256 public address associated with the account
            key; surfaced for debugging and potential resolver extensions.
        time_anchors_debug: Optional debug payload with server-provided anchor hints used
            to reason about EID drift; shape may vary and is forwarded best-effort.
    """

    registry_id: str
    canonical_id: str
    identity_key: bytes | None
    encrypted_identity_key: bytes | None = None
    owner_key_version: int | None = None
    device_type: int | None = None
    config_entry_id: str | None = None
    fast_pair_model_id: str | None = None
    manufacturer: str | None = None
    model: str | None = None
    pair_date: int | None = None
    secrets_creation_date: int | None = None
    encrypted_account_key: bytes | None = None
    public_key_address: bytes | None = None
    time_anchors_debug: Any | None = None


class GoogleFindMyCoordinator(
    RegistryOperations,
    SubentryOperations,
    PollingOperations,
    IdentityOperations,
    LocateOperations,
    CacheOperations,
    DataUpdateCoordinator[list[dict[str, Any]]],
):
    """Coordinator that manages polling, cache, and push updates for Google Find My Device.

    Thread-safety & event loop rules (IMPORTANT):
    - All interactions that create HA tasks or publish state must occur on the HA event loop thread.
    - This class provides small helpers to "hop" from any background thread into the loop
      using `loop.call_soon_threadsafe(...)` before touching HA APIs.

    Pitfalls & mitigations (general guidance for future reviewers):
    - Pitfall 1 – return values with `call_soon_threadsafe`:
      `call_soon_threadsafe` does not propagate return values to the calling thread.
      **Mitigation:** All methods we marshal to the loop in this module (e.g. `increment_stat`,
      `update_device_cache`, `push_updated`, `purge_device`) are consciously `None`-returning.
    - Pitfall 2 – excessive thread hops:
      Unnecessary hops add overhead if used for micro-operations.
      **Mitigation:** We hop **once at the public method boundary**, then execute the
      complete logic on the HA loop (single-threaded, deterministic).
    - Pitfall 3 – complex external locks:
      Using extra `threading.Lock`s for shared state increases complexity and risk.
      **Mitigation:** We **serialize** state changes by marshalling to the **single-threaded**
      HA event loop – the loop itself is the synchronization primitive.
    """

    # ---------------------------- Lifecycle ---------------------------------
    def __init__(
        self,
        hass: HomeAssistant,
        cache: CacheProtocol,
        *,
        location_poll_interval: int = 300,
        device_poll_delay: int = 5,
        min_poll_interval: int = DEFAULT_MIN_POLL_INTERVAL,
        allow_history_fallback: bool = False,
        contributor_mode: str = DEFAULT_CONTRIBUTOR_MODE,
        contributor_mode_switch_epoch: int | None = None,
    ) -> None:
        """Initialize the coordinator.

        This sets up the central data management for the integration, including
        API communication, state caching, and polling logic.

        Notes:
            - Credentials and related metadata are provided via the entry-scoped TokenCache.
            - The HA-managed aiohttp ClientSession is reused to avoid per-call pools.

        Args:
            hass: The Home Assistant instance.
            cache: An object implementing the CacheProtocol for persistent storage.
            location_poll_interval: The interval in seconds between polling cycles.
            device_poll_delay: The delay in seconds between polling individual devices.
            min_poll_interval: The minimum allowed interval between polling cycles.
            allow_history_fallback: Whether to fall back to Recorder history for location.
            contributor_mode: Preferred Nova contributor mode ("high_traffic" or "in_all_areas").
            contributor_mode_switch_epoch: Epoch timestamp when the contributor mode last changed.
        """
        self.hass = hass
        self._cache = cache
        self.config_entry: ConfigEntry | None = getattr(self, "config_entry", None)

        # Try to ensure entry-scoped namespace on the cache early when possible.
        # This will be finalized in async_setup() once the ConfigEntry is bound.
        try:
            if (
                not getattr(self._cache, "entry_id", None)
                and self.config_entry is not None
            ):
                setattr(self._cache, "entry_id", self.config_entry.entry_id)
        except Exception:
            pass

        # Get the singleton aiohttp.ClientSession from Home Assistant and reuse it.
        self._session = async_get_clientsession(hass)

        normalized_mode = self._sanitize_contributor_mode(contributor_mode)
        if contributor_mode_switch_epoch is None or contributor_mode_switch_epoch <= 0:
            contributor_mode_switch_epoch = int(time.time())
        self._contributor_mode = normalized_mode
        self._contributor_mode_switch_epoch = int(contributor_mode_switch_epoch)

        # Use _get_api_class() to respect package-level monkeypatches from tests
        APIClass = _get_api_class()
        self.api = APIClass(
            cache=self._cache,
            session=self._session,
            contributor_mode=self._contributor_mode,
            contributor_mode_switch_epoch=self._contributor_mode_switch_epoch,
        )

        # Configuration (user options; updated via update_settings())
        self.location_poll_interval = int(location_poll_interval)
        self.device_poll_delay = int(device_poll_delay)
        self.min_poll_interval = int(
            min_poll_interval
        )  # hard lower bound between cycles
        self.allow_history_fallback = bool(allow_history_fallback)

        # Initialize diagnostics buffer and one-shot warning guard for malformed IDs.
        self._diag = DiagnosticsBuffer(max_items=200)
        self._warned_bad_identifier_devices: set[str] = set()

        # Internal caches & bookkeeping
        self._device_location_data: dict[
            str, dict[str, Any]
        ] = {}  # device_id -> location dict
        self._device_names: dict[str, str] = {}  # device_id -> human name
        self._device_caps: dict[
            str, dict[str, Any]
        ] = {}  # device_id -> caps (e.g., {"can_ring": True})
        self._device_update_history: dict[str, deque[float]] = {}
        self._present_device_ids: set[str] = (
            set()
        )  # diagnostics-only set from latest non-empty list
        self._semantic_label_cache: dict[str, SemanticLabelRecord] = {}

        # Flag to separate initial discovery from later runtime additions.
        # After the first successful non-empty device list is processed, this becomes True.
        self._initial_discovery_done: bool = False

        # Presence smoothing (TTL):
        # - Per-device "last seen in full list" timestamp (monotonic)
        # - Cold-start marker: timestamp of last non-empty list (monotonic)
        # - Presence TTL in seconds (derived from poll interval, min 120s)
        self._present_last_seen: dict[str, float] = {}
        self._last_nonempty_wall: float = 0.0
        self._presence_ttl_s: int = 120

        # Minimal hardening state (empty-list quorum)
        self._last_device_list: list[dict[str, Any]] = []
        self._empty_list_streak: int = 0

        # Polling state
        self._poll_lock = asyncio.Lock()
        self._is_polling = False
        self._startup_complete = False
        self._last_poll_mono: float = 0.0  # monotonic timestamp for scheduling
        self._last_list_poll_mono: float = (
            0.0  # monotonic timestamp for discovery pacing
        )

        # Push readiness deferral/escalation bookkeeping
        self._fcm_defer_started_mono: float = 0.0
        self._fcm_last_stage: int = 0  # 0=none, 1=warned, 2=errored

        # Push readiness memoization and cooldown after transport errors
        self._push_ready_memo: bool | None = None
        self._push_cooldown_until: float = 0.0

        # Manual locate gating (UX + server protection)
        self._locate_inflight: set[str] = set()  # device_id -> in-flight flag
        self._locate_cooldown_until: dict[str, float] = {}  # device_id -> mono deadline

        # Per-device action locks to prevent race conditions on concurrent requests
        self._device_action_locks: dict[str, asyncio.Lock] = {}

        # Play Sound UUID tracking (needed to properly cancel sound requests)
        self._sound_request_uuids: dict[str, str] = {}  # device_id -> request_uuid
        self._sound_request_timestamps: dict[
            str, float
        ] = {}  # device_id -> creation timestamp

        # Per-device poll cooldowns after owner/crowdsourced reports.
        self._device_poll_cooldown_until: dict[str, float] = {}
        # Per-device interval history for predictive polling
        self._device_interval_history: dict[str, list[float]] = {}

        # DR-driven poll targeting
        self._enabled_poll_device_ids: set[str] = set()
        self._devices_with_entry: set[str] = set()
        self._dr_unsub: Callable[[], None] | None = None
        self._unsub_scheduler: Callable[[], None] | None = None
        self.eid_resolver: Any | None = None

        # Service device tracking (registry operations)
        self._service_device_ready: bool = False
        self._service_device_id: str | None = None
        self._service_device_identifier: tuple[str, str] | None = None

        # User-configured ignored devices (updated via update_settings)
        self.ignored_devices: list[str] = []

        # Subentry awareness (feature groups / platform scoping)
        self._subentry_metadata: dict[str, SubentryMetadata] = {}
        self._subentry_snapshots: dict[str, tuple[dict[str, Any], ...]] = {}
        self._feature_to_subentry: dict[str, str] = {}
        self._default_subentry_key_value: str = "core_tracking"
        self._subentry_manager: ConfigEntrySubEntryManager | None = None
        self._pending_subentry_repair: asyncio.Task[None] | None = None

        # Statistics (extend as needed)
        self.stats: dict[str, int] = {
            "background_updates": 0,  # FCM/push-driven updates + manual commits
            "polled_updates": 0,  # sequential poll-driven updates
            "crowd_sourced_updates": 0,  # number of crowdsourced updates observed
            "history_fallback_used": 0,  # times we had to fall back to Recorder history
            "timeouts": 0,  # request timeouts
            "invalid_coords": 0,  # coordinate validation failures
            "invalid_ts_drop_count": 0,  # invalid or stale (< existing) timestamps
            "future_ts_drop_count": 0,  # timestamps too far in the future
            "drop_reason_invalid_ts": 0,  # invalid/stale timestamps (detail bucket)
            "fused_updates": 0,  # overlapping fixes fused to stabilize coordinates
            "accuracy_sanitized_count": 0,  # accuracy values clamped to valid range
        }
        _LOGGER.debug("Initialized stats: %s", self.stats)

        self._consecutive_timeouts: int = 0
        self._last_poll_result: str | None = None

        # Granular status tracking (API polling vs. push transport)
        self._api_status_state: str = ApiStatus.UNKNOWN
        self._api_status_reason: str | None = None
        self._api_status_changed_at: float | None = None
        self._fcm_status_state: str = FcmStatus.UNKNOWN
        self._fcm_status_reason: str | None = None
        self._fcm_status_changed_at: float | None = None

        # Performance metrics (timestamps, durations) & recent errors (bounded)
        self.performance_metrics: dict[str, float] = {}
        self.recent_errors: deque[tuple[float, str, str]] = deque(maxlen=5)

        # Debounced stats persistence (avoid flushing on every increment)
        self._stats_save_task: asyncio.Task[None] | None = None
        self._stats_debounce_seconds: float = 5.0

        # Load persistent statistics asynchronously (name the task for better debugging)
        self.hass.async_create_task(
            self._async_load_stats(), name=f"{DOMAIN}.load_stats"
        )

        # One-shot device list refresh trigger (used after reconfigure)
        self._force_device_list_refresh: bool = False
        self._force_device_list_reason: str | None = None

        # Marker for diagnostics after reconfigure-triggered reloads
        self._recent_reconfigure_at: float | None = None

        # Short-retry scheduling handle (coalesced)
        self._short_retry_cancel: Callable[[], None] | None = None

        # NEW: Authentication/repairs state
        self._auth_error_active: bool = False
        self._auth_error_since: float = 0.0
        self._auth_error_message: str | None = None

        # FIX: FCM error counter to avoid triggering re-auth on transient errors (#114)
        self._fcm_error_count: int = 0
        self._fcm_last_error: str | None = None

        # Transient auth error tracking: only trigger reauth after consecutive failures
        # across multiple poll cycles. This prevents premature reauth prompts when
        # Google's backend is temporarily slow to propagate refreshed tokens.
        self._consecutive_transient_auth_failures: int = 0
        self._last_transient_auth_error: str | None = None

        # Reload guard: defer core subentry repairs once after reload-driven attach
        self._skip_repair_during_reload_refresh: bool = False
        self._reload_repair_skip_pending_release: bool = False

        # Shared tracker support: identity_key → set of device_ids mapping
        # Enables Location propagation across devices sharing the same tracker
        self._identity_key_to_devices: dict[bytes, set[str]] = {}
        # Guard against infinite propagation loops
        self._propagating_location: bool = False

        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )

    async def async_config_entry_first_refresh(self) -> None:
        """Run the first refresh, tolerating coordinators without the helper."""

        try:
            parent_first_refresh = super().async_config_entry_first_refresh
        except AttributeError:  # pragma: no cover - compatibility with older cores
            parent_first_refresh = None

        if parent_first_refresh is not None:
            await parent_first_refresh()
            return

        _LOGGER.debug(
            "[%s] Falling back to async_refresh for initial coordinator sync",
            self._entry_id() or "unknown",
        )
        await self.async_refresh()

    def mark_recent_reconfigure(self, when: float | None = None) -> None:
        """Record that a reconfigure just completed for diagnostics."""

        when_ts = when if when is not None else time.time()
        try:
            self._recent_reconfigure_at = float(when_ts)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            self._recent_reconfigure_at = None

    @property
    def recent_reconfigure_at(self) -> float | None:
        """Return the most recent reconfigure marker if present."""

        return self._recent_reconfigure_at

    def request_device_list_refresh(
        self, *, reason: str | None = None, schedule_refresh: bool = True
    ) -> None:
        """Force the next update cycle to refetch the device list immediately."""

        if not self._is_on_hass_loop():
            self._run_on_hass_loop(
                self.request_device_list_refresh,
                reason=reason,
                schedule_refresh=schedule_refresh,
            )
            return

        if not self._force_device_list_refresh:
            self._force_device_list_refresh = True
            self._force_device_list_reason = reason

            _LOGGER.debug(
                "[%s] Forcing device list refresh%s",
                self._entry_id() or "unknown",
                f" ({reason})" if reason else "",
            )

        if schedule_refresh:
            self._dispatch_async_request_refresh(
                task_name=f"{DOMAIN}.force_device_list_refresh",
                log_context="request_device_list_refresh",
            )

    @property
    def cache(self) -> CacheProtocol:
        """Return the entry-scoped token cache backing this coordinator."""

        return self._cache

    @staticmethod
    def _sanitize_contributor_mode(mode: str | None) -> str:
        """Normalize the contributor mode to a supported value."""

        if isinstance(mode, str):
            normalized = mode.strip().lower()
            if normalized in (
                CONTRIBUTOR_MODE_HIGH_TRAFFIC,
                CONTRIBUTOR_MODE_IN_ALL_AREAS,
            ):
                return normalized
        return DEFAULT_CONTRIBUTOR_MODE

    def _async_persist_contributor_mode(self) -> None:
        """Persist the contributor mode preferences asynchronously."""

        async def _persist() -> None:
            try:
                await self._cache.async_set_cached_value(
                    CACHE_KEY_CONTRIBUTOR_MODE, self._contributor_mode
                )
                await self._cache.async_set_cached_value(
                    CACHE_KEY_LAST_MODE_SWITCH,
                    self._contributor_mode_switch_epoch,
                )
            except Exception as err:  # pragma: no cover - defensive persistence
                _LOGGER.debug("Failed to persist contributor mode state: %s", err)

        self.hass.async_create_task(
            _persist(), name=f"{DOMAIN}.persist_contributor_mode"
        )

    async def async_setup(self) -> None:
        """One-time async setup called from __init__.py (entry setup).

        - Loads stats (already scheduled in __init__, so this is idempotent).
        - Indexes poll targets from the Device Registry.
        - Subscribes to DR updates (unsubscribed in `async_shutdown()`).
        - Ensures the per-entry "service device" exists in the Device Registry.
        - Enforces entry-scoped namespace by attaching `entry_id` to the cache object.
        """
        # Ensure the cache carries our entry_id namespace for downstream Nova/API helpers.
        try:
            entry = self.config_entry or getattr(self, "entry", None)
            if entry and not getattr(self._cache, "entry_id", None):
                setattr(self._cache, "entry_id", entry.entry_id)
        except Exception:
            pass

        # Make sure the service device exists early so end devices can link to it promptly.
        self._ensure_service_device_exists()

        # Initial index (works even if config_entry is not yet bound; will re-run on DR event)
        self._reindex_poll_targets_from_device_registry()
        if self._dr_unsub is None:
            self._dr_unsub = self.hass.bus.async_listen(
                EVENT_DEVICE_REGISTRY_UPDATED, self._handle_dr_event
            )

    def _get_google_home_filter(self) -> GoogleHomeFilterProtocol | None:
        """Return the Google Home filter associated with this coordinator."""

        entry = self.config_entry or getattr(self, "entry", None)
        runtime_data = getattr(entry, "runtime_data", None)
        google_home_filter = getattr(runtime_data, "google_home_filter", None)
        return cast("GoogleHomeFilterProtocol | None", google_home_filter)

    def _should_preserve_precise_home_coordinates(
        self,
        prev_location: Mapping[str, Any] | None,
        replacement_attrs: Mapping[str, Any],
    ) -> bool:
        """Return True when cached coordinates are precise and still inside Home.

        The Google Home filter proposes substituting a semantic detection with the
        Home zone's coordinates and radius. Preserve the previously cached
        coordinates only when they are **both** more precise than the proposed
        radius **and** still fall within that radius, so we do not pin a device to
        an outdated off-site fix after a semantic Home report arrives.
        """

        if not prev_location:
            return False

        try:
            prev_lat = float(prev_location["latitude"])
            prev_lon = float(prev_location["longitude"])
            prev_acc = float(prev_location["accuracy"])
            proposed_lat = float(replacement_attrs["latitude"])
            proposed_lon = float(replacement_attrs["longitude"])
            proposed_radius = float(replacement_attrs["radius"])
        except (KeyError, TypeError, ValueError):
            return False

        if not (
            math.isfinite(prev_lat)
            and math.isfinite(prev_lon)
            and math.isfinite(prev_acc)
            and math.isfinite(proposed_lat)
            and math.isfinite(proposed_lon)
            and math.isfinite(proposed_radius)
        ):
            return False

        if proposed_radius <= 0 or prev_acc >= proposed_radius:
            return False

        return (
            self._haversine_distance(prev_lat, prev_lon, proposed_lat, proposed_lon)
            <= proposed_radius
        )

    async def async_shutdown(self) -> None:
        """Clean up listeners and timers on entry unload to avoid leaks."""
        self._cancel_pending_subentry_repair()
        # Unsubscribe DR listener
        dr_unsub = getattr(self, "_dr_unsub", None)
        if dr_unsub is not None:
            try:
                dr_unsub()
            except Exception:
                pass
            self._dr_unsub = None
        # Cancel short-retry callback if scheduled
        short_retry_cancel = getattr(self, "_short_retry_cancel", None)
        if short_retry_cancel is not None:
            try:
                short_retry_cancel()
            except Exception:
                pass
            finally:
                self._short_retry_cancel = None
        # Cancel pending debounced stats write
        stats_save_task = getattr(self, "_stats_save_task", None)
        if stats_save_task and not stats_save_task.done():
            stats_save_task.cancel()

        await self._async_unload()

    async def _async_unload(self) -> None:
        """Cleanup resources on unload."""
        unsub_scheduler = getattr(self, "_unsub_scheduler", None)
        if unsub_scheduler:
            unsub_scheduler()
            self._unsub_scheduler = None

        eid_resolver = getattr(self, "eid_resolver", None)
        if eid_resolver is not None:
            eid_resolver.stop()

        api = getattr(self, "api", None)
        if api is not None:
            # Safe to call close() now that it strictly un-refs the session
            # without killing the connection.
            await api.close()

    # --- NEW: Repairs + Auth state helpers ---------------------------------
    def _set_auth_state(self, *, failed: bool, reason: str | None = None) -> None:
        """State machine for authentication error transitions.

        - When entering the "failed" state:
            * create a Repairs issue (idempotent)
            * fire a domain-scoped HA event (EVENT_AUTH_ERROR)
            * set `auth_error_active = True` and store a short message
            * push updated data so diagnostic sensors can flip to `on`
        - When entering the "ok" state from "failed":
            * dismiss the Repairs issue
            * fire an OK event (EVENT_AUTH_OK)
            * set `auth_error_active = False` and clear the message
            * push updated data so diagnostic sensors can flip back to `off`
        """
        if failed and not self._auth_error_active:
            self._auth_error_active = True
            self._auth_error_since = time.time()
            self._auth_error_message = (reason or "Authentication failed").strip()
            # Repairs + event
            self._create_auth_issue()
            entry_id = self.config_entry.entry_id if self.config_entry else ""
            self.hass.bus.async_fire(
                EVENT_AUTH_ERROR,
                {
                    "entry_id": entry_id,
                    "email": self._get_account_email(),
                    "message": self._auth_error_message,
                },
            )
            # Notify listeners (binary_sensor etc.)
            try:
                self.async_set_updated_data(self.data)
            except Exception:
                pass
        elif not failed:
            issue_dismissed = self._dismiss_auth_issue()
            state_changed = False

            if self._auth_error_active:
                self._auth_error_active = False
                self._auth_error_message = None
                state_changed = True

            if issue_dismissed or state_changed:
                entry_id = self.config_entry.entry_id if self.config_entry else ""
                self.hass.bus.async_fire(
                    EVENT_AUTH_OK,
                    {
                        "entry_id": entry_id,
                        "email": self._get_account_email(),
                    },
                )
                try:
                    self.async_set_updated_data(self.data)
                except Exception:
                    pass

    @property
    def auth_error_active(self) -> bool:
        """Expose the current "auth failed" condition for diagnostic entities (binary_sensor)."""
        return self._auth_error_active

    # Former `_async_start_reauth_flow` helper removed: rely on HA's automatic
    # reauth trigger when `ConfigEntryAuthFailed` bubbles up.

    # --- END: Add/Replace inside Coordinator class --------------------------------

    # ---------------------------- Semantic mappings ------------------------
    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        """Return a float representation or ``None`` when conversion fails."""
        return _coerce_float_impl(value)

    def _find_semantic_match(
        self, raw_name: str, mapping: Mapping[str, dict[str, float]]
    ) -> dict[str, float] | None:
        """Return a semantic mapping entry using normalized matching."""

        normalized_name = raw_name.casefold().strip()
        if normalized_name.startswith("near "):
            normalized_name = normalized_name[len("near ") :].strip()

        normalized_mapping = {key.casefold(): value for key, value in mapping.items()}
        return normalized_mapping.get(normalized_name)

    def _apply_semantic_mapping(self, payload: dict[str, Any]) -> bool:
        """Substitute coordinates using user-defined semantic mappings when available."""

        semantic_name = payload.get("semantic_name")
        if not isinstance(semantic_name, str) or not semantic_name.strip():
            return False

        # Skip replayed semantic hints to preserve debounce behaviour when mappings are absent.
        if payload.get("is_replayed") is True or payload.get("replayed") is True:
            return False

        entry = getattr(self, "config_entry", None)
        if entry is None:
            return False

        raw_mappings = entry.options.get(OPT_SEMANTIC_LOCATIONS)
        if not raw_mappings:
            raw_mappings = entry.data.get(OPT_SEMANTIC_LOCATIONS)
        if not isinstance(raw_mappings, Mapping):
            return False

        normalized: dict[str, dict[str, float]] = {}
        for name, coords in raw_mappings.items():
            if not isinstance(name, str) or not isinstance(coords, Mapping):
                continue

            latitude = self._coerce_float(coords.get("latitude"))
            longitude = self._coerce_float(coords.get("longitude"))
            if latitude is None or longitude is None:
                continue

            accuracy = self._coerce_float(coords.get("accuracy"))
            if accuracy is None or accuracy <= 0:
                # Use default radius for semantic zones without explicit accuracy.
                # Never use 0.0 as it's physically impossible for GPS.
                accuracy = DEFAULT_SEMANTIC_DETECTION_RADIUS

            normalized[name.casefold()] = {
                "latitude": latitude,
                "longitude": longitude,
                "accuracy": accuracy,
            }

        mapped = self._find_semantic_match(semantic_name, normalized)
        if mapped is None:
            return False

        payload["latitude"] = mapped["latitude"]
        payload["longitude"] = mapped["longitude"]
        payload["accuracy"] = mapped["accuracy"]
        payload["location_type"] = "trusted"

        return True

    def _record_semantic_label(
        self,
        payload: Mapping[str, Any],
        *,
        device_id: str | None = None,
    ) -> None:
        """Track observed semantic labels for diagnostics and UX helpers."""

        raw_name = payload.get("semantic_name")
        if not isinstance(raw_name, str):
            return

        semantic_name = raw_name.strip()
        if not semantic_name:
            return

        cache = getattr(self, "_semantic_label_cache", None)
        if cache is None:
            cache = {}
            self._semantic_label_cache = cache

        normalized = semantic_name.casefold()
        now = time.time()
        record = cache.get(normalized)
        if record is None:
            record = SemanticLabelRecord(
                label=semantic_name, first_seen=now, last_seen=now
            )
            cache[normalized] = record
        else:
            record.last_seen = now

        if device_id:
            record.devices.add(device_id)

    def get_observed_semantic_labels(self) -> list[SemanticLabelRecord]:
        """Return a stable snapshot of observed semantic labels."""

        cache = getattr(self, "_semantic_label_cache", None)
        if not cache:
            return []

        snapshot = [record.copy() for record in cache.values()]
        snapshot.sort(key=lambda item: item.label.casefold())
        return snapshot

    # ---------------------------- Ignore helpers ----------------------------
    @staticmethod
    def _normalize_identity_key(raw: object) -> bytes | None:
        """Normalize candidate identity key values into bytes."""

        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw)
        if isinstance(raw, str):
            try:
                return bytes.fromhex(raw)
            except ValueError:
                # Silent failure: registry/API payloads are not always trustworthy.
                return None
        return None

    @staticmethod
    def _normalize_identity_key_candidates(raw: object) -> list[bytes]:
        """Normalize collections of candidate identity keys."""

        if raw is None:
            return []
        if isinstance(raw, (bytes, bytearray, str)):
            one = GoogleFindMyCoordinator._normalize_identity_key(raw)
            return [one] if one is not None else []
        if isinstance(raw, Iterable) and not isinstance(raw, (str, bytes, bytearray)):
            out: list[bytes] = []
            for item in raw:
                key = GoogleFindMyCoordinator._normalize_identity_key(item)
                if key is not None and key not in out:
                    out.append(key)
            return out
        return []

    @staticmethod
    def _normalize_optional_string(raw: object) -> str | None:
        """Return a stripped string when available, otherwise ``None``."""

        if isinstance(raw, str):
            value = raw.strip()
            return value or None
        return None

    @staticmethod
    def _normalize_encrypted_blob(raw: object) -> bytes | None:
        """Normalize encrypted payloads encoded as bytes or hex strings."""

        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw)
        if isinstance(raw, str):
            try:
                return bytes.fromhex(raw)
            except ValueError:
                return None
        return None

    # get_active_device_identities moved to identity.py (IdentityOperations mixin)

    def _get_ignored_set(self) -> set[str]:
        """Return the set of device IDs the user chose to ignore (options-first).

        Notes:
            - Uses config_entry.options if available; falls back to an attribute
              'ignored_devices' when set through update_settings().
            - Intentionally simple equality (no normalization) to avoid surprises.
        """
        try:
            entry = self.config_entry
            if entry is not None:
                raw = entry.options.get(
                    OPT_IGNORED_DEVICES, DEFAULT_OPTIONS.get(OPT_IGNORED_DEVICES, {})
                )
                # Accept list[str] (legacy) or mapping (current)
                mapping, _migrated = coerce_ignored_mapping(raw)
                if mapping:
                    return set(mapping.keys())
        except Exception:  # defensive
            pass
        raw_attr = getattr(self, "ignored_devices", None)
        if isinstance(raw_attr, list):
            return {x for x in raw_attr if isinstance(x, str)}
        return set()

    def is_ignored(self, device_id: str) -> bool:
        """Return True if the device is currently ignored by user choice."""
        return device_id in self._get_ignored_set()

    # ---------------------------- Metrics & errors helpers ------------------
    def safe_update_metric(self, key: str, value: float) -> None:
        """Safely set a numeric performance metric (float-coerced)."""
        try:
            self.performance_metrics[str(key)] = float(value)
        except Exception:
            # Never raise from diagnostics helpers
            pass

    def _short_error_message(self, exc: Exception | str) -> str:
        """Return a compact, single-line error string."""
        return _short_error_message_impl(exc)

    def _append_recent_error(self, err_type: str, message: str) -> None:
        """Append a (timestamp, type, message) triple to the bounded deque."""
        try:
            self.recent_errors.append(
                (time.time(), err_type, self._short_error_message(message))
            )
        except Exception:
            pass

    def note_error(
        self, exc: Exception, *, where: str = "", device: str | None = None
    ) -> None:
        """Public helper to record non-fatal errors with minimal context."""
        prefix = where or "coordinator"
        if device:
            prefix += f"({device})"
        err_type = type(exc).__name__
        self._append_recent_error(err_type, f"{prefix}: {exc}")

    # Safe getters for durations based on keys that __init__.py may set.
    def get_metric(self, key: str) -> float | None:
        val = self.performance_metrics.get(key)
        return float(val) if isinstance(val, (int, float)) else None

    def _get_duration(self, start_key: str, end_key: str) -> float | None:
        """Calculate duration between two metric keys."""
        return _get_duration_impl(self.get_metric(start_key), self.get_metric(end_key))

    def get_setup_duration_seconds(self) -> float | None:
        """Duration between 'setup_start_monotonic' and 'setup_end_monotonic'."""
        return self._get_duration("setup_start_monotonic", "setup_end_monotonic")

    def get_recent_errors(self) -> list[dict[str, Any]]:
        """Return a JSON-friendly copy of recent error triples."""
        return format_recent_errors(self.recent_errors)

    # _async_update_data and _async_start_poll_cycle moved to polling.py (PollingOperations mixin)

    # ---------------------------- Snapshot helpers --------------------------
    def _build_base_snapshot_entry(self, device_dict: dict[str, Any]) -> dict[str, Any]:
        """Create the base snapshot entry for a device."""
        return _build_base_snapshot_entry_impl(device_dict)

    def _update_entry_from_cache(self, entry: dict[str, Any], wall_now: float) -> bool:
        """Update the given snapshot entry in place from the in-memory cache.

        Args:
            entry: The device snapshot entry to update.
            wall_now: The current wall-clock time as a float timestamp.

        Returns:
            True if the cache contained data for this device and the entry was updated, else False.
        """
        dev_id = entry["device_id"]
        cached = self._device_location_data.get(dev_id)
        if not cached:
            return False

        if isinstance(cached, Mapping):
            entry.update(cached)
        else:
            entry.update({"status": "Anchor metadata cached"})

        if entry.get("metadata_only"):
            entry.setdefault("status", "Anchor metadata cached")
            return True

        last_updated_ts = (
            cached.get("last_updated", 0) if isinstance(cached, Mapping) else 0
        )
        age = max(0.0, wall_now - float(last_updated_ts))
        entry["status"] = determine_location_status(age, self.location_poll_interval)
        return True

    def _build_snapshot_from_cache(
        self, devices: list[dict[str, Any]], wall_now: float
    ) -> list[dict[str, Any]]:
        """Build a lightweight snapshot using only the in-memory cache.

        This never touches HA state or the database; it is safe in background tasks.

        Args:
            devices: A list of device dictionaries to include in the snapshot.
            wall_now: The current wall-clock time as a float timestamp.

        Returns:
            A list of device state dictionaries built from the cache.
        """
        snapshot: list[dict[str, Any]] = []
        for dev in devices:
            entry = self._build_base_snapshot_entry(dev)
            # If cache has info, update status accordingly; otherwise keep default status.
            self._update_entry_from_cache(entry, wall_now)
            snapshot.append(entry)
        return snapshot

    async def _async_build_device_snapshot_with_fallbacks(
        self, devices: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Build a snapshot using cache, HA state and (optionally) history fallback.

        Args:
            devices: A list of device dictionaries to build the snapshot for.

        Returns:
            A complete list of device state dictionaries with fallbacks applied.
        """
        snapshot: list[dict[str, Any]] = []
        wall_now = time.time()

        for dev in devices:
            entry = self._build_base_snapshot_entry(dev)

            # Prefer cached result
            if self._update_entry_from_cache(entry, wall_now):
                entry = _sanitize_decoder_row(entry)
                snapshot.append(entry)
                continue

            # No cache -> Registry + State (cheap, non-blocking)
            dev_id = entry["device_id"]
            registry_entry = self._find_tracker_entity_entry(dev_id)
            if registry_entry is None:
                _LOGGER.debug(
                    "Skipping state/history fallback for '%s' because tracker cache entry is unavailable.",
                    entry["name"],
                )
                snapshot.append(entry)
                continue

            entity_id = registry_entry.entity_id
            if not entity_id:
                _LOGGER.debug(
                    "Entity registry entry missing entity_id for tracker '%s' (unique_id=%s)",
                    entry["name"],
                    registry_entry.unique_id,
                )
                snapshot.append(entry)
                continue

            state = self.hass.states.get(entity_id)
            if state:
                lat = state.attributes.get("latitude")
                lon = state.attributes.get("longitude")
                acc = state.attributes.get("gps_accuracy")
                last_seen_ts = _resolve_last_seen_from_attributes(
                    state.attributes, state.last_updated.timestamp()
                )
                if lat is not None and lon is not None:
                    entry.update(
                        {
                            "latitude": lat,
                            "longitude": lon,
                            "accuracy": acc,
                            "last_seen": last_seen_ts,
                            "status": "Using current state",
                        }
                    )
                    entry = _sanitize_decoder_row(entry)
                    snapshot.append(entry)
                    continue

            # Optional history fallback
            if self.allow_history_fallback:
                _LOGGER.warning(
                    "No live state for %s (entity_id=%s); attempting history fallback via Recorder.",
                    entry["name"],
                    entity_id,
                )
                rec = get_recorder(self.hass)
                result = await rec.async_add_executor_job(
                    _sync_get_last_gps_from_history, self.hass, entity_id
                )
                if result:
                    entry.update(result)
                    self.increment_stat("history_fallback_used")
                else:
                    _LOGGER.warning(
                        "No historical GPS data found for %s (entity_id=%s). "
                        "Entity may be excluded from Recorder.",
                        entry["name"],
                        entity_id,
                    )

            entry = _sanitize_decoder_row(entry)
            snapshot.append(entry)

        return snapshot

    # ---------------------------- Stats persistence -------------------------
    async def _async_load_stats(self) -> None:
        """Load statistics from entry-scoped cache."""
        try:
            cached = await self._cache.async_get_cached_value("integration_stats")
            if cached and isinstance(cached, dict):
                for key in self.stats.keys():
                    if key in cached:
                        self.stats[key] = cached[key]
                _LOGGER.debug("Loaded statistics from cache: %s", self.stats)
        except Exception as err:
            _LOGGER.debug("Failed to load statistics from cache: %s", err)

        try:
            sound_request_uuids = await self._cache.async_get_cached_value(
                "sound_request_uuids"
            )
            if not isinstance(sound_request_uuids, dict):
                _LOGGER.debug("No cached Play Sound UUIDs found; keeping current map")
                return

            # FIX: Filter out stale UUIDs to prevent Play Sound re-trigger after restart (#108)
            # Sound requests older than _SOUND_UUID_MAX_AGE_S are considered expired
            max_age_seconds = _SOUND_UUID_MAX_AGE_S
            now = time.time()
            loaded_sound_request_uuids: dict[str, str] = {}
            loaded_sound_request_timestamps: dict[str, float] = {}

            for device_id, data in sound_request_uuids.items():
                if not isinstance(device_id, str):
                    continue

                # Support new format: {"uuid": str, "ts": float}
                if isinstance(data, dict):
                    uuid_val = data.get("uuid")
                    ts_val = data.get("ts", 0)
                    if not isinstance(uuid_val, str) or not uuid_val:
                        continue
                    # Discard if older than max_age_seconds
                    if now - float(ts_val) > max_age_seconds:
                        _LOGGER.debug(
                            "Discarding expired Play Sound UUID for %s (age: %.0fs)",
                            device_id,
                            now - float(ts_val),
                        )
                        continue
                    loaded_sound_request_uuids[device_id] = uuid_val
                    loaded_sound_request_timestamps[device_id] = float(ts_val)
                # Support legacy format: str (UUID only, no timestamp)
                elif isinstance(data, str) and data:
                    # Legacy entries without timestamp are discarded on restart
                    # to prevent potential re-triggers
                    _LOGGER.debug(
                        "Discarding legacy Play Sound UUID for %s (no timestamp)",
                        device_id,
                    )
                    continue

            if self._sound_request_uuids and not loaded_sound_request_uuids:
                _LOGGER.debug(
                    "Skipping cached empty Play Sound UUID map; runtime map already populated"
                )
                return

            merged_sound_request_uuids = {
                **loaded_sound_request_uuids,
                **self._sound_request_uuids,
            }
            merged_sound_request_timestamps = {
                **loaded_sound_request_timestamps,
                **self._sound_request_timestamps,
            }
            if merged_sound_request_uuids != self._sound_request_uuids:
                self._sound_request_uuids = merged_sound_request_uuids
                self._sound_request_timestamps = merged_sound_request_timestamps
                _LOGGER.debug(
                    "Loaded Play Sound UUIDs from cache: %s", self._sound_request_uuids
                )
        except Exception as err:
            _LOGGER.debug(
                "Failed to load Play Sound UUIDs from cache; keeping current map: %s",
                err,
            )

    async def _async_save_stats(self) -> None:
        """Persist statistics to entry-scoped cache."""
        try:
            await self._cache.async_set_cached_value(
                "integration_stats", self.stats.copy()
            )
        except Exception as err:
            _LOGGER.debug("Failed to save statistics to cache: %s", err)

    async def _async_save_sound_uuids(self) -> None:
        """Persist Play Sound request UUIDs to entry-scoped cache.

        FIX: Store with timestamps to allow expiry filtering on reload (#108).
        Format: {device_id: {"uuid": str, "ts": float}}
        The timestamp is set when the UUID is first created, not on every save.
        """
        try:
            # Convert internal format to timestamped format for persistence
            # Use the original creation timestamp, not current time
            now = time.time()
            timestamped_uuids = {
                device_id: {
                    "uuid": uuid_val,
                    "ts": self._sound_request_timestamps.get(device_id, now),
                }
                for device_id, uuid_val in self._sound_request_uuids.items()
            }
            await self._cache.async_set_cached_value(
                "sound_request_uuids", timestamped_uuids
            )
        except Exception as err:
            _LOGGER.debug("Failed to save Play Sound UUIDs to cache: %s", err)

    async def _debounced_save_stats(self) -> None:
        """Debounce wrapper to coalesce frequent stat updates into a single write.

        This coroutine MUST run on the HA event loop. It is scheduled safely via
        `_schedule_stats_persist()` which ensures loop-thread execution.
        """
        try:
            await asyncio.sleep(self._stats_debounce_seconds)
            await self._async_save_stats()
        except asyncio.CancelledError:
            # Expected if a new increment arrives before the delay elapses; do nothing.
            return
        except Exception as err:
            _LOGGER.debug("Debounced stats save failed: %s", err)

    def _schedule_stats_persist(self) -> None:
        """(Re)schedule a debounced persistence task for statistics.

        Thread-safe: may be called from any thread. Ensures cancellation and creation
        of the debounced task happen on the HA loop.
        """

        def _do_schedule() -> None:
            # Cancel a pending writer, if any, and schedule a fresh one (loop-local).
            if self._stats_save_task and not self._stats_save_task.done():
                self._stats_save_task.cancel()
            create_task = getattr(
                self.hass, "async_create_background_task", self.hass.async_create_task
            )
            self._stats_save_task = create_task(
                self._debounced_save_stats(),
                name=f"{DOMAIN}.save_stats_debounced",
            )

        if self._is_on_hass_loop():
            _do_schedule()
        else:
            self._run_on_hass_loop(_do_schedule)

    def _increment_stat_on_loop(self, stat_name: str) -> None:
        """Increment a statistic on the HA loop and schedule persistence."""
        if stat_name in self.stats:
            before = self.stats[stat_name]
            self.stats[stat_name] = before + 1
            _LOGGER.debug(
                "Incremented %s from %s to %s",
                stat_name,
                before,
                self.stats[stat_name],
            )
            self._schedule_stats_persist()
            try:
                self.async_update_listeners()
            except Exception as err:
                _LOGGER.debug("Stats listener notification failed: %s", err)
        else:
            _LOGGER.warning(
                "Tried to increment unknown stat '%s'; available=%s",
                stat_name,
                list(self.stats.keys()),
            )

    def increment_stat(self, stat_name: str) -> None:
        """Increment a statistic counter (thread-safe).

        May be called from any thread. The actual mutation and scheduling are
        marshalled onto the HA event loop.

        Note on performance:
        - The "hop" to the loop occurs exactly once here (constant per call).
          We avoid repeated hops for inner micro-operations.
        """
        if self._is_on_hass_loop():
            self._increment_stat_on_loop(stat_name)
        else:
            self._run_on_hass_loop(self._increment_stat_on_loop, stat_name)

    # Cache methods moved to cache.py (CacheOperations mixin):
    # get_device_location_data, prime_device_location_cache, seed_device_last_seen,
    # _track_device_interval, _persist_anchor_metadata, update_device_cache,
    # _propagate_location_to_shared_devices, _is_significant_update,
    # _merge_with_existing_cache_row, _haversine_distance, _apply_weighted_location_fusion

    def get_device_last_seen(self, device_id: str) -> datetime | None:
        """Return last_seen as timezone-aware datetime (UTC) if cached."""
        ts = self._device_location_data.get(device_id, {}).get("last_seen")
        return epoch_to_datetime_utc(ts)

    def get_device_display_name(self, device_id: str) -> str | None:
        """Return the human-readable device name if known.

        Args:
            device_id: The canonical ID of the device.

        Returns:
            The display name as a string, or None.
        """
        return self._ensure_device_name_cache().get(device_id)

    def get_device_name_map(self) -> dict[str, str]:
        """Return a shallow copy of the internal device-id -> name mapping.

        Returns:
            A dictionary mapping device IDs to their names.
        """
        return dict(self._ensure_device_name_cache())

    # ---------------------------- Presence & Purge API ----------------------------
    def is_device_present(self, device_id: str) -> bool:
        """Return True if the given device_id is present (TTL-smoothed)."""
        ts = self._present_last_seen.get(device_id, 0.0)
        return not is_presence_expired(ts, time.monotonic(), self._presence_ttl_s)

    def get_absent_device_ids(self) -> list[str]:
        """Return ids known by name/cache that are expired under the presence TTL."""
        now_mono = time.monotonic()
        ttl = self._presence_ttl_s
        name_cache = self._ensure_device_name_cache()
        known = set(name_cache) | set(self._device_location_data)
        return sorted(
            [
                d
                for d in known
                if is_presence_expired(
                    self._present_last_seen.get(d, 0.0), now_mono, ttl
                )
            ]
        )

    def purge_device(self, device_id: str) -> None:
        """Remove all cached data and cooldown state for a device (thread-safe publish).

        Called from the config-entry device deletion flow. This does not trigger a poll,
        but it immediately publishes an updated snapshot so UI can refresh.
        """
        if not self._is_on_hass_loop():
            self._run_on_hass_loop(self.purge_device, device_id)
            return

        self._device_location_data.pop(device_id, None)
        self._ensure_device_name_cache().pop(device_id, None)
        self._device_caps.pop(device_id, None)
        self._locate_inflight.discard(device_id)
        self._locate_cooldown_until.pop(device_id, None)
        removed_uuid = self._sound_request_uuids.pop(device_id, None)
        self._sound_request_timestamps.pop(device_id, None)
        self._device_poll_cooldown_until.pop(device_id, None)
        self._present_device_ids.discard(device_id)

        if removed_uuid is not None:
            self.hass.async_create_task(self._async_save_sound_uuids())
        self._present_last_seen.pop(device_id, None)
        # Rebuild the cached snapshot without the purged device
        current_snapshot: list[dict[str, Any]] = []
        for row in list(self.data or []):
            if not isinstance(row, dict):
                continue
            if row.get("device_id") == device_id or row.get("id") == device_id:
                continue
            current_snapshot.append(dict(row))

        devices_stub = [
            {"id": entry.get("device_id") or entry.get("id"), "name": entry.get("name")}
            for entry in current_snapshot
            if isinstance(entry.get("device_id") or entry.get("id"), str)
        ]
        self._refresh_subentry_index(devices_stub)
        self._store_subentry_snapshots(current_snapshot)
        self.async_set_updated_data(current_snapshot)

    # ---------------------------- Push updates ------------------------------
    def push_updated(
        self,
        device_ids: list[str] | None = None,
        *,
        reset_baseline: bool = True,
    ) -> None:
        """Publish a fresh snapshot to listeners after push (FCM) cache updates.

        Thread-safe: may be called from any thread. This method ensures all state
        publishing happens on the HA event loop.

        This **does not** trigger a poll. It:
        - Immediately pushes cache state to entities via `async_set_updated_data()`.
        - Optionally resets the internal poll baseline to 'now' to prevent an immediate
          re-poll when push-driven updates arrive (`reset_baseline=True` by default).
        - Optionally limits the snapshot to `device_ids`; otherwise includes all known devices.

        Args:
            device_ids: An optional list of device IDs to include in the update.
            reset_baseline: If True (default), reset the scheduler baseline to now.
        """
        if not self._is_on_hass_loop():
            self._run_on_hass_loop(
                self.push_updated, device_ids, reset_baseline=reset_baseline
            )
            return

        wall_now = time.time()
        self._set_fcm_status(FcmStatus.CONNECTED)
        if reset_baseline:
            self._last_poll_mono = time.monotonic()  # optional: reset poll timer

        # Choose device ids for the snapshot
        name_cache = self._ensure_device_name_cache()

        if device_ids:
            ids = device_ids
        else:
            # union of all known names and cached locations
            ids = list({*name_cache.keys(), *self._device_location_data.keys()})

        # Apply ignore filter first to avoid touching presence for ignored devices.
        ignored = self._get_ignored_set()
        ids = [d for d in ids if d not in ignored]

        # Touch presence timestamps for pushed devices (keeps presence stable)
        now_mono = time.monotonic()
        for dev_id in ids:
            self._present_last_seen[dev_id] = now_mono

        # Build "devices" stubs from id->name mapping
        devices_stub: list[dict[str, Any]] = []
        for dev_id in ids:
            cached_name = name_cache.get(dev_id)
            if isinstance(cached_name, str) and cached_name.strip():
                name = cached_name if cached_name != dev_id else "Google Find My Device"
            else:
                name = "Google Find My Device"
            devices_stub.append({"id": dev_id, "name": name})

        snapshot = self._build_snapshot_from_cache(devices_stub, wall_now=wall_now)

        # Merge this push snapshot with the existing self.data (all devices)
        old_devices = {d["device_id"]: d for d in (self.data or []) if isinstance(d, dict) and "device_id" in d}
        new_devices = {d["device_id"]: d for d in snapshot if isinstance(d, dict) and "device_id" in d}
        merged_snapshot = list((old_devices | new_devices).values())

        # Refresh index and store snapshots using the complete merged list of all devices
        self._refresh_subentry_index(None)
        self._store_subentry_snapshots(merged_snapshot)
        self.async_set_updated_data(merged_snapshot)
        _LOGGER.debug(
            "Pushed snapshot for %d device(s) via push_updated()", len(snapshot)
        )

    # ---------------------------- Play sound helpers ------------------------
    def _api_push_ready(self) -> bool:
        """Best-effort check whether push/FCM is initialized (backward compatible).

        Optimistic default: if we cannot determine readiness explicitly,
        return True so the UI stays usable; the API call will enforce reality.

        Returns:
            True if the push mechanism is believed to be ready.
        """
        # Short-circuit via cooldown window after a transport failure.
        now = time.monotonic()
        if now < self._push_cooldown_until:
            if self._push_ready_memo is not False:
                _LOGGER.debug(
                    "Push readiness: cooldown active -> treating as not ready"
                )
            self._push_ready_memo = False
            return False

        ready: bool | None = None
        try:
            fn = getattr(self.api, "is_push_ready", None)
            if callable(fn):
                ready = bool(fn())
            else:
                for attr in ("push_ready", "fcm_ready", "receiver_ready"):
                    val = getattr(self.api, attr, None)
                    if isinstance(val, bool):
                        ready = val
                        break
                if ready is None:
                    fcm = getattr(self.api, "fcm", None)
                    if fcm is not None:
                        for attr in ("is_ready", "ready"):
                            val = getattr(fcm, attr, None)
                            if isinstance(val, bool):
                                ready = val
                                break
        except Exception as err:
            _LOGGER.debug(
                "Push readiness check exception: %s (defaulting optimistic True)", err
            )
            ready = True

        if ready is None:
            ready = True  # optimistic default

        if ready != self._push_ready_memo:
            _LOGGER.debug("Push readiness changed: %s", ready)
            self._push_ready_memo = ready

        return ready

    def update_settings(
        self,
        *,
        ignored_devices: list[str] | None = None,
        location_poll_interval: int | None = None,
        device_poll_delay: int | None = None,
        min_poll_interval: int | None = None,
        allow_history_fallback: bool | None = None,
        contributor_mode: str | None = None,
        contributor_mode_switch_epoch: int | None = None,
    ) -> None:
        """Apply updated user settings provided by the config entry (options-first).

        This method deliberately enforces basic typing/limits to keep the coordinator sane
        regardless of where the values came from.

        Args:
            ignored_devices: A list of device IDs to hide from snapshots/polling.
            location_poll_interval: The interval in seconds for location polling.
            device_poll_delay: The delay in seconds between polling devices.
            min_poll_interval: The minimum polling interval in seconds.
            allow_history_fallback: Whether to allow falling back to Recorder history.
            contributor_mode: Updated contributor mode ("high_traffic" or "in_all_areas").
            contributor_mode_switch_epoch: Epoch timestamp when the mode last changed.
        """
        if ignored_devices is not None:
            # This attribute is only used as a fallback when config_entry is not available.
            self.ignored_devices = list(ignored_devices)

        if location_poll_interval is not None:
            try:
                self.location_poll_interval = max(1, int(location_poll_interval))
            except (TypeError, ValueError):
                _LOGGER.warning(
                    "Ignoring invalid location_poll_interval=%r", location_poll_interval
                )

        if device_poll_delay is not None:
            try:
                self.device_poll_delay = max(0, int(device_poll_delay))
            except (TypeError, ValueError):
                _LOGGER.warning(
                    "Ignoring invalid device_poll_delay=%r", device_poll_delay
                )

        if min_poll_interval is not None:
            try:
                self.min_poll_interval = max(1, int(min_poll_interval))
            except (TypeError, ValueError):
                _LOGGER.warning(
                    "Ignoring invalid min_poll_interval=%r", min_poll_interval
                )

        if allow_history_fallback is not None:
            self.allow_history_fallback = bool(allow_history_fallback)

        if contributor_mode is not None:
            normalized_mode = self._sanitize_contributor_mode(contributor_mode)
            if (
                contributor_mode_switch_epoch is None
                or contributor_mode_switch_epoch <= 0
            ):
                contributor_mode_switch_epoch = int(time.time())
            epoch = int(contributor_mode_switch_epoch)
            if (
                normalized_mode != self._contributor_mode
                or epoch != self._contributor_mode_switch_epoch
            ):
                self._contributor_mode = normalized_mode
                self._contributor_mode_switch_epoch = epoch
                self.api.set_contributor_mode(
                    normalized_mode, switch_epoch=self._contributor_mode_switch_epoch
                )
                self._async_persist_contributor_mode()

        # Settings adjustments may change per-subentry views
        self._refresh_subentry_index()
        self._schedule_eid_resolver_refresh()
