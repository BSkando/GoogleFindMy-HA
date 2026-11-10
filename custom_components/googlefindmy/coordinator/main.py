# custom_components/googlefindmy/coordinator/main.py
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

from .._reauth_reason import ReauthReason, ReauthReasonCode

# Operations classes - currently empty mixins for future method extraction
from .cache import CacheOperations
from .helpers.cache import (
    SOUND_UUID_MAX_AGE_S,
    determine_location_status,
    epoch_to_datetime_utc,
    is_presence_expired,
    is_sound_uuid_expired,
)
from .helpers.cache import (
    build_base_snapshot_entry as _build_base_snapshot_entry_impl,
)
from .helpers.cache import (
    sanitize_decoder_row as _sanitize_decoder_row,
)
from .helpers.geo import (
    ACCURACY_BUCKET_STATS,
    accuracy_bucket,
    recorded_accuracy_pair,
)
from .helpers.geo import (
    coerce_float as _coerce_float_impl,
)
from .helpers.stats import (
    ApiStatus,
    CryptoStatus,
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
from homeassistant.const import EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.device_registry import EVENT_DEVICE_REGISTRY_UPDATED

# IMPORTANT: make Common_pb2 import **mandatory** (integration packaging must include it).
# This avoids silent type/name drift and keeps source labels stable.
# NOTE: Import GoogleFindMyAPI directly for type hints; use _get_api_class() at
# call-time to respect package-level monkeypatches from tests.
from ..api import GoogleFindMyAPI
from ..const import (
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
from ..ProtoDecoders import decoder

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

# How long an unload waits for the startup stats load before flushing anyway. The load
# reads two keys from an in-memory cache, so the ordinary case is sub-millisecond; the
# bound exists so a cache that cannot answer delays the unload instead of hanging it.
_STATS_LOAD_SHUTDOWN_WAIT_S = 5.0

# Fields consumed by get_active_device_identities from cached payloads/anchors.
_PERSISTED_METADATA_KEYS: tuple[str, ...] = (
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
    "trackable_components",
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


# Privacy-hardened mapping helpers for the anonymized per-device diagnostics list
# (P1-3). Kept as pure module functions so the boundary tables are testable in
# isolation and contain no device identifiers.

# device_type ints that map to the "phone" class (SpotDeviceType.DEVICE_TYPE_PHONE).
_DEVICE_TYPE_PHONE = 20


# Classification moved to coordinator.helpers.geo so the cache write path can
# reach it too (main.py imports cache.py, so the reverse would be a cycle).
# Re-exported under the historical private name: this module stays the caller
# the diagnostics tests address, while geo owns the single boundary table.
_accuracy_bucket = accuracy_bucket


# Sub-key under which the per-device tally claims travel inside the ``integration_stats``
# record. Reserved rather than free-form: the loader copies only known counters into
# ``self.stats``, so this entry cannot become a phantom counter, and the writer asserts
# that the name does not collide with a real one.
TALLY_CLAIMS_KEY = "_tally_claims"

# Mirror of the cache layer's ring length, used when merging a restored ring into a live
# one. Imported rather than restated would couple the modules the wrong way round; the
# value is asserted equal by a test, so a change to one of them cannot pass silently.
TALLY_RING_LIMIT = 8


def _round_age(age_seconds: float) -> int:
    """Round an age in seconds to the nearest 10 s (sub-poll timing is hidden)."""
    return int(round(age_seconds / 10) * 10)


def _wall_clock_age(now_epoch: float, stamp: Any) -> int | None:
    """Return the rounded age of a wall-clock stamp, or ``None`` if it is unusable.

    A NEGATIVE age means the stamp lies in the future, which is corruption rather than
    extreme freshness. The intake accepts a drift of
    ``MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S``, so such a stamp really does reach the
    snapshot and the coarse store, and the device tracker already suppresses a fix it
    finds there. Clamping the age to zero instead would make this telemetry claim the
    hidden fix happened "just now" and would contaminate the very numbers the accuracy
    gate is evaluated with, so an unusable stamp reports no age at all.

    Monotonic ages do not come through here: their ``max(0.0, ...)`` guards a clock that
    ran backwards, and no reader draws a visibility decision from their sign.
    """
    if not isinstance(stamp, (int, float)) or isinstance(stamp, bool):
        return None
    age = now_epoch - float(stamp)
    if age < 0:
        return None
    return _round_age(age)


def _device_class(device_type: Any) -> str:
    """Map a ``device_type`` slot int to a coarse three-class label.

    ``20`` -> ``phone``; ``None`` or ``0`` (unknown) -> ``other``; any other
    known int -> ``bt_tracker``. Deliberately abrupt (limitations note 3): the
    maintainer needs phone vs. physical tracker vs. unknown, not a fine class.
    """
    if device_type == _DEVICE_TYPE_PHONE:
        return "phone"
    if device_type is None or device_type == 0:
        return "other"
    return "bt_tracker"


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
    # Producer flag: True when ``accuracy_m`` is the conservative fallback radius
    # rather than a real measurement. Forwarded as a bool so the recorder
    # persists it and map_view can read it from history. Only emitted when the
    # producer actually set it; legacy rows without the key stay unflagged. The
    # trailing None filter keeps a False value (False is not None).
    estimated = r.get("accuracy_estimated")
    if estimated is not None:
        out["accuracy_estimated"] = bool(estimated)
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
        # Stale/blank recorded states withhold the plain latitude/longitude keys;
        # the last known fix lives in the recorder-only last_latitude/last_longitude
        # keys. Fall back to those so history reconstruction still finds a position.
        lat = attrs.get("latitude", attrs.get("last_latitude"))
        lon = attrs.get("longitude", attrs.get("last_longitude"))
        if lat is None or lon is None:
            return None

        last_seen_ts = _resolve_last_seen_from_attributes(
            attrs, last_state.last_updated.timestamp()
        )
        # Read accuracy and its estimated-provenance flag from the same
        # authoritative source (accuracy_m over the volatile gps_accuracy) and
        # carry the flag into the reconstructed row, so a recorded 200m fallback
        # stays distinguishable from a real measurement once it re-enters the
        # cache. Reading only gps_accuracy here dropped the flag and let the
        # producer reclassify the fallback as real (PR #1124 defect class).
        raw_accuracy, estimated = recorded_accuracy_pair(attrs)
        row: dict[str, Any] = {
            "latitude": lat,
            "longitude": lon,
            "accuracy": raw_accuracy,
            "last_seen": last_seen_ts,
            "status": "Using historical data",
        }
        if estimated is not None:
            row["accuracy_estimated"] = estimated
        return row
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
        trackable_components: ``DeviceCapabilities.trackableComponents`` when known. A
            value above 1 marks a device with per-component EID series (measured on
            Pixel Buds Pro 2, see ``docs/FMDN.md`` S3.4.1).
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
    trackable_components: int | None = None


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

        # Configuration (user options; applied at setup/reload from the config entry)
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
        # Last *reliable* fix per device, the coordinator-level "last good"
        # feeding the Plus Code display accessors (sensor + tracker attribute).
        # Only advanced for rows passing is_reliable_fix (a sanitized
        # accuracy-less fix carries the 200 m fallback with
        # accuracy_estimated=True and must never poison it), mirroring the
        # tracker's private last-good policy. Dropped in purge_device.
        self._device_last_good_location: dict[
            str, dict[str, Any]
        ] = {}  # device_id -> last reliable (non-estimated) location dict
        # Round-trip return anchors (Q2-A). RAM-only, one-shot: when the speed
        # gate accepts a wide jump A->B, A is remembered here so a later fix
        # returning near A within the TTL recovers instead of being gated.
        # Not persisted (restart in the TTL window falls back cleanly to the
        # device_tracker restore); dropped in purge_device.
        self._round_trip_anchors: dict[
            str, dict[str, Any]
        ] = {}  # device_id -> {"lat": float, "lon": float, "ts": float}
        self._device_names: dict[str, str] = {}  # device_id -> human name
        self._device_caps: dict[
            str, dict[str, Any]
        ] = {}  # device_id -> caps (e.g., {"can_ring": True})
        self._device_update_history: dict[str, deque[float]] = {}
        self._present_device_ids: set[str] = (
            set()
        )  # diagnostics-only set from latest non-empty list
        self._semantic_label_cache: dict[str, SemanticLabelRecord] = {}

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
        # Stale-device removal: consecutive fresh lists each tracker device was
        # missing from, keyed by device registry id. Volatile on purpose: a
        # restart starts the count again, which only delays a removal.
        self._stale_device_misses: dict[str, int] = {}
        # Google ids removed as stale; one of them in a fresh list reloads the
        # entry once so the platforms set up its entities again (S-R6).
        self._stale_removed_google_ids: set[str] = set()
        # Canonicless drop counts per name and kind of the last counting list
        # that carried each Google id, so a shared name protects only a new
        # drop (S-R3d, R2-6, R3-5).
        self._stale_name_baseline: dict[str, Mapping[tuple[str, str], int]] = {}
        # Refused or failed removals per device registry id, and the devices
        # given up after STALE_REMOVAL_MAX_ATTEMPTS until the next setup.
        self._stale_removal_attempts: dict[str, int] = {}
        self._stale_removal_given_up: set[str] = set()

        # Polling state
        self._poll_lock = asyncio.Lock()
        self._is_polling = False
        # The in-flight poll cycle, held so it can be cancelled on entry unload
        # and on Home Assistant stop (see ``_async_cancel_poll_cycle``).
        self._poll_cycle_task: asyncio.Task[None] | None = None
        # Latched by ``_async_cancel_poll_cycle``; the refresh schedules no new
        # cycle once it is set (a refresh may be mid-flight when teardown starts).
        self._poll_cycle_teardown = False
        # Remover of the EVENT_HOMEASSISTANT_STOP listener armed in ``async_setup``.
        self._hass_stop_unsub: Callable[[], None] | None = None
        self._startup_complete = False
        self._last_poll_mono: float = 0.0  # monotonic timestamp for scheduling
        self._last_list_poll_mono: float = (
            0.0  # monotonic timestamp for discovery pacing
        )

        # Push readiness deferral/escalation bookkeeping
        self._fcm_defer_started_mono: float = 0.0
        self._fcm_last_stage: int = 0  # 0=none, 1=warned, 2=errored
        self._degraded_mode_warned: bool = False

        # Push readiness memoization and cooldown after transport errors
        self._push_ready_memo: bool | None = None
        self._push_cooldown_until: float = 0.0

        # Manual locate gating (UX + server protection)
        self._locate_inflight: set[str] = set()  # device_id -> in-flight flag
        self._locate_cooldown_until: dict[str, float] = {}  # device_id -> mono deadline
        # When the integration last sent a location request for a device, on
        # the ``time.monotonic`` clock. Written by the transport immediately
        # before the POST, for both send sites (poll cycle and manual locate),
        # so an attempt that stops in its push or token phase leaves no value;
        # a request that fails after the POST still counts. See
        # ``coordinator/helpers/locate_clock.py``. ``_last_poll_mono`` cannot
        # serve here: it is a scheduling base, back-dated by the poll interval.
        # Read by the EID resolver for the ``poll_age_s`` field of
        # ``FMDN_UWT_TRANSITION`` through ``locate_requested_mono``.
        self._locate_requested_mono: dict[str, float] = {}

        # Per-device action locks to prevent race conditions on concurrent requests
        self._device_action_locks: dict[str, asyncio.Lock] = {}

        # Play Sound UUID tracking (needed to properly cancel sound requests)
        self._sound_request_uuids: dict[str, str] = {}  # device_id -> request_uuid
        self._sound_request_timestamps: dict[
            str, float
        ] = {}  # device_id -> creation timestamp
        # device_id -> (cancel key, time.monotonic() at which Nova accepted that
        # play). In memory only: it gates how soon a stop may follow the play
        # (STOP_SOUND_MIN_DELAY_AFTER_PLAY_S). A restart or a config-entry
        # reload starts it empty, so a stop within that delay after a reload is
        # sent at once. Known gap: the stored key does not record whether Nova
        # accepted the play, so the stamp cannot be rebuilt from it.
        self._sound_play_accepted_at: dict[str, tuple[str, float]] = {}

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

        # User-configured ignored devices (options-first; empty attribute fallback)
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
            "invalid_ts_drop_warn": 0,  # corrupt (pre-Y2K) timestamps (alarming sub-bucket of invalid_ts_drop_count)
            "invalid_ts_drop_benign": 0,  # regressed/out-of-order timestamps (expected sub-bucket of invalid_ts_drop_count)
            "fused_updates": 0,  # overlapping fixes fused to stabilize coordinates
            "accuracy_sanitized_count": 0,  # accuracy values clamped to valid range
            "speed_gate_rejects": 0,  # fixes rejected as physically implausible (#177)
            "round_trip_recoveries": 0,  # gated fixes recovered as legitimate returns (Q2-A)
            # Coarse fixes rejected because a better, reliable and still-fresh
            # position was already cached (#216, core of #211). Never counts a
            # cold start, an unreliable cached fix or a stale one - those are
            # accepted, because a coarse position still beats none at all.
            "accuracy_gate_rejects": 0,
            # Canonicless drops on the last main poll (transition-independent
            # diagnostics aggregate). Absolute per-poll values, not increments;
            # listed here so the restore path (iterates self.stats.keys()) and the
            # diagnostics stats block both see them.
            "canonicless_drop_total": 0,
            "canonicless_drop_benign": 0,
            "canonicless_drop_warn": 0,
        }
        # Distribution of the REPORTED accuracy of incoming fixes (#216). The
        # reject counter above answers "how often did the gate fire"; it does
        # not answer "are 200 m and factor 4 in the right place". Only the
        # distribution answers that, and it has to be collected BEFORE the gate,
        # or it would only ever see the fixes that got through.
        #
        # Keys are DERIVED from the single classification table in
        # coordinator.helpers.geo, never listed a second time here: a class
        # added there without a counter would otherwise fail silently
        # (increment_stat only warns on an unknown key). Flat, not nested -
        # increment_stat looks up ``stat_name in self.stats`` and the cache
        # restore path iterates ``self.stats.keys()``; a nested dict would miss
        # both.
        self.stats.update(dict.fromkeys(ACCURACY_BUCKET_STATS.values(), 0))
        _LOGGER.debug("Initialized stats: %s", self.stats)

        self._consecutive_timeouts: int = 0
        self._last_poll_result: str | None = None
        # Consecutive empty location results per device id; drives the
        # once-only INFO record for a silent device (log-when-unavailable).
        self._empty_location_streak: dict[str, int] = {}

        # Granular status tracking (API polling vs. push transport)
        self._api_status_state: str = ApiStatus.UNKNOWN
        self._api_status_reason: str | None = None
        self._api_status_changed_at: float | None = None
        self._fcm_status_state: str = FcmStatus.UNKNOWN
        self._fcm_status_reason: str | None = None
        self._fcm_status_changed_at: float | None = None
        # End-to-end location decryption health (drives the diagnostic enum
        # sensor and mirrors the reauth escalation; initial UNKNOWN -> HA renders
        # "unknown" until the first decryption is attempted).
        self._crypto_status_state: str = CryptoStatus.UNKNOWN
        self._crypto_status_reason: str | None = None
        self._crypto_status_changed_at: float | None = None

        # Performance metrics (timestamps, durations) & recent errors (bounded)
        self.performance_metrics: dict[str, float] = {}
        self.recent_errors: deque[tuple[float, str, str]] = deque(maxlen=5)

        # Debounced stats persistence (avoid flushing on every increment)
        self._stats_save_task: asyncio.Task[None] | None = None
        self._stats_debounce_seconds: float = 5.0
        # Which generation of the histogram the numbers below belong to. Bumped by a
        # reset, read by the load below across its await, so a load that started in the
        # previous generation cannot put it back. See ``begin_new_stats_epoch``.
        self._stats_epoch: int = 0
        # Devices purged while the load below is still in flight. A reset discards the
        # whole record, but a purge is per device and must not, so the epoch cannot
        # carry this case. Collected only until the load finishes, then dropped.
        self._purged_before_stats_load: set[str] = set()
        self._stats_loaded: bool = False
        # Set when a purge inside the load window needs the record rewritten. Deferred
        # rather than written on the spot, because a write before the merge would
        # persist live state that is still missing everything the load carries.
        self._save_after_stats_load: bool = False

        # Load persistent statistics asynchronously (name the task for better debugging).
        # The handle is kept because `async_shutdown` waits for it: its flush writes the
        # whole record from live state, which inside this window is missing everything
        # the load has not merged yet.
        self._stats_load_task: asyncio.Task[None] | None = self.hass.async_create_task(
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

        # FIX 3: structured, redaction-safe record of the most recent reauth
        # trigger, mirrored into diagnostics so the reason is visible without a
        # debug log capture. ``None`` until the first reauth is recorded.
        self._reauth_reason: ReauthReason | None = None
        # De-dup set for the once-per (code, origin) WARNING emitted by
        # ``record_reauth_reason`` so a repeating trigger does not spam the log.
        self._reauth_reason_logged: set[tuple[str, str]] = set()

        # Stale/missing shared-key decryption failures escalate to a reauth flow
        # only after several consecutive cycles, with a cooldown so the flow is not
        # re-fired on every poll once it is already open. Mirrors the transient-auth
        # counter above; the in-decrypt self-heal (force_refresh / blind refresh)
        # gets the first chance to recover an owner-key version bump without reauth.
        self._consecutive_decrypt_failures: int = 0
        # None means "never escalated yet". A real monotonic timestamp is only
        # recorded after the first escalation, so the cooldown gate is skipped
        # entirely on the first one. Using 0.0 as the sentinel was a bug: it is a
        # valid monotonic value, so on a freshly booted host (process uptime below
        # the cooldown window) ``monotonic() - 0.0 < cooldown`` would wrongly
        # suppress the very first reauth escalation for up to the cooldown period.
        self._last_decrypt_reauth_monotonic: float | None = None
        self._last_decrypt_error: str | None = None
        # Injectable monotonic-clock seam. Defaults to ``time.monotonic`` in
        # production; tests override it to drive the decrypt cooldown gate
        # deterministically (no dependency on the host's process uptime).
        self._monotonic: Callable[[], float] = time.monotonic

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

    async def async_setup(self) -> None:
        """One-time async setup called from __init__.py (entry setup).

        - Loads stats (already scheduled in __init__, so this is idempotent).
        - Indexes poll targets from the Device Registry.
        - Subscribes to DR updates (unsubscribed in `async_shutdown()`).
        - Ensures the per-entry "service device" exists in the Device Registry.
        - Enforces entry-scoped namespace by attaching `entry_id` to the cache object.
        """
        # Armed first: it depends on nothing below, and the steps below can
        # raise (the caller logs and moves on without calling this again).
        if self._hass_stop_unsub is None:
            self._hass_stop_unsub = self.hass.bus.async_listen_once(
                EVENT_HOMEASSISTANT_STOP, self._async_on_hass_stop
            )

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

    async def _async_on_hass_stop(self, _event: Event) -> None:
        """Cancel the in-flight poll cycle when Home Assistant stops.

        This cannot be left to ``async_shutdown``: for an entry-bound coordinator
        the core runs that only on entry unload (``config_entry.async_on_unload``
        in ``DataUpdateCoordinator.__init__``), and a stopping Home Assistant does
        not unload entries (``ConfigEntries._async_shutdown`` merely cancels retry
        setups). Before the poll became a background task, without this listener
        it outlived the stop event and the core logged ``Task ... was still
        running after final writes shutdown stage`` before cancelling it itself.
        Core now also cancels the background task on stop; this listener still
        settles it and disarms retries and resolver timers created by its cleanup.

        The one-time listener has already removed itself from the bus before it
        invokes this handler, so the remover it handed out is spent; drop it
        first so a later ``async_shutdown`` does not call it again.
        """
        self._hass_stop_unsub = None
        await self._async_cancel_poll_cycle()

    async def _async_cancel_poll_cycle(self) -> None:
        """Cancel and settle the in-flight poll cycle, if any.

        The cycle runs as a background task (entry-managed when available) in
        ``polling.py``, so startup and ``hass.async_block_till_done`` do not wait
        for its requests or inter-device delays. Core also cancels background
        tasks on stop and entry-managed tasks on unload. Shared by the
        unload path (``async_shutdown``) and the stop path (``_async_on_hass_stop``).
        Awaiting the cancelled task lets the cycle's ``finally`` settle first;
        what that ``finally`` arms (a short retry, the EID refresh debounce) is
        then disarmed here, so neither path leaves a raw ``call_later`` behind
        that would fire on a stopping core. ``async_shutdown`` repeats the EID
        cancels after its own awaits; both are idempotent.

        The latch comes first: a refresh that is suspended before its scheduling
        step when this runs would otherwise find nothing to cancel here and then
        create a fresh cycle that nobody cancels any more.
        """
        self._poll_cycle_teardown = True
        poll_task = getattr(self, "_poll_cycle_task", None)
        self._poll_cycle_task = None
        if poll_task is not None and not poll_task.done():
            poll_task.cancel()
            try:
                await poll_task
            except (asyncio.CancelledError, Exception):
                pass
        for attr in (
            "_short_retry_cancel",
            "_eid_refresh_debounce_handle",
            "_eid_inline_refresh_debounce_handle",
        ):
            handle = getattr(self, attr, None)
            if handle is None:
                continue
            try:
                # ``_short_retry_cancel`` is a remover, the others are timer handles.
                if attr == "_short_retry_cancel":
                    handle()
                else:
                    handle.cancel()
            except Exception:
                pass
            finally:
                setattr(self, attr, None)

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
        # The core's shutdown goes first. ``DataUpdateCoordinator.async_shutdown``
        # raises ``_shutdown_requested`` (a refresh that arrives after unload, say
        # a push callback calling ``async_request_refresh``, returns before it
        # touches the closed API), cancels the interval refresh that
        # ``_schedule_refresh`` may still have armed, and shuts the request
        # debouncer, which also drops the debouncer's hard reference to this
        # coordinator. None of it suspends, so those doors are closed before the
        # awaits below open a window; and it is idempotent, which matters because
        # unload runs this method twice (``async_unload_entry`` calls it, then the
        # ``async_on_unload`` hook the core registered in its ``__init__`` does).
        await super().async_shutdown()
        # Then cancel an in-flight poll cycle, so its ``finally`` (which may arm
        # a short retry) runs before the short-retry cancel below. This is the
        # unload path; the Home Assistant stop path is ``_async_on_hass_stop``.
        await self._async_cancel_poll_cycle()
        # Disarm the stop listener when the entry unloads before a stop. On the
        # stop path the handler has already cleared this (spent remover).
        hass_stop_unsub = getattr(self, "_hass_stop_unsub", None)
        if hass_stop_unsub is not None:
            try:
                hass_stop_unsub()
            except Exception:
                pass
            self._hass_stop_unsub = None
        self._cancel_pending_subentry_repair()
        # Unsubscribe DR listener
        dr_unsub = getattr(self, "_dr_unsub", None)
        if dr_unsub is not None:
            try:
                dr_unsub()
            except Exception:
                pass
            self._dr_unsub = None
        # The short-retry callback was cancelled by ``_async_cancel_poll_cycle``
        # above; nothing between there and here suspends, so nothing can arm it
        # again before this point.
        # End the pending debounced stats write by WRITING it, not by dropping it.
        # Cancelling alone loses whatever the window was still coalescing, which is every
        # increment of the last few seconds together with the claims recorded for them.
        #
        # It is also the backstop for the purge. `purge_device` writes immediately rather
        # than on the debounce, but it does so by creating a task it does not await, and
        # an unload can outrun that task. Its removal would then exist only in memory
        # that is about to go away, the deleted device's claim would survive in the
        # persisted record, and a device re-added under the same id would have its first
        # matching measurement suppressed by a claim from its previous life.
        #
        # The write is therefore unconditional IN THAT RESPECT: it does not ask whether a
        # debounced task is pending. Gating it on one was the first attempt and it missed
        # exactly the case that matters most: the purge writes through a task this
        # attribute never holds, so when only that one is in flight there is nothing here
        # to find and the whole block was skipped. A condition that cannot see the more
        # important of the two writers is not a condition, and the cost of dropping it is
        # one write per unload.
        #
        # One condition does apply, and it lives in the writer rather than here: while the
        # stats load is still in flight, `_async_save_stats` defers instead of writing,
        # because inside that window live state is missing everything the load has not
        # merged yet and a write would replace the stored record with it. That is a
        # different axis from the one this comment is about.
        #
        # The pending task is still cancelled first, so its coroutine stops waiting out
        # its sleep. `_debounced_save_stats` swallows the cancellation and returns, so
        # the await settles it; if it was already past the sleep and inside the write,
        # the repeated write is the same record and costs nothing. Failures are swallowed
        # like the other shutdown steps: an unload must not raise.
        # Let the startup load finish first, so the flush below is outside its window and
        # writes the merged record rather than deferring. Waiting is what makes the write
        # possible at all here: the entry-scoped cache is closed immediately after this
        # method returns, so a write deferred to the load's `finally` would reach a closed
        # store and be swallowed. Waited WITHOUT cancelling and with a bound, so a load
        # that cannot finish delays the unload by seconds rather than hanging it; if the
        # bound is hit the window is still open, the flush defers, and the record stays as
        # it was on disk.
        load_task = getattr(self, "_stats_load_task", None)
        if load_task is not None:
            # The whole probe is inside the try, not just the wait: `done()` is the first
            # thing touched on an attribute read with `getattr`, and a value that is not a
            # task fails there rather than in `asyncio.wait`. An unload must not raise, and
            # this wait is an optimisation of WHEN the flush writes - never a reason for
            # the unload to fail. Falling through leaves the window open, so the flush
            # below defers and the stored record stays as it was.
            try:
                if not load_task.done():
                    await asyncio.wait({load_task}, timeout=_STATS_LOAD_SHUTDOWN_WAIT_S)
            except Exception as err:
                _LOGGER.debug("Could not wait for the stats load on unload: %s", err)

        stats_save_task = getattr(self, "_stats_save_task", None)
        if stats_save_task and not stats_save_task.done():
            stats_save_task.cancel()
            try:
                await stats_save_task
            except (asyncio.CancelledError, Exception):
                pass
        self._stats_save_task = None
        try:
            await self._async_save_stats()
        except Exception:
            pass

        # Cancel pending EID-resolver refresh debounce timers so no
        # ``call_later`` callback fires (and creates a task) after unload.
        for attr in (
            "_eid_refresh_debounce_handle",
            "_eid_inline_refresh_debounce_handle",
        ):
            handle = getattr(self, attr, None)
            if handle is not None:
                try:
                    handle.cancel()
                except Exception:
                    pass
                finally:
                    setattr(self, attr, None)

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

        # Presence, not truthiness. The options flow writes this key on every
        # add, edit and delete, so an empty mapping is the user having removed
        # their last semantic location -- a deliberate "no mappings", not a
        # missing setting. Falling through to `entry.data` on a falsy value
        # revived the pre-migration copy, which `_async_soft_migrate_data_to_
        # options` only ever copies and never clears, so the deleted mapping
        # kept relabelling coordinates as `trusted` and survived restarts.
        options = getattr(entry, "options", None) or {}
        if OPT_SEMANTIC_LOCATIONS in options:
            raw_mappings = options[OPT_SEMANTIC_LOCATIONS]
        else:
            data = getattr(entry, "data", None) or {}
            raw_mappings = data.get(OPT_SEMANTIC_LOCATIONS)
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
            - Uses config_entry.options if available; falls back to the
              'ignored_devices' attribute (an empty default; options are the source of truth).
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

    def record_reauth_reason(
        self,
        code: ReauthReasonCode,
        origin: str,
        *,
        counters: Mapping[str, int] | None = None,
    ) -> None:
        """Persist a structured, redaction-safe reason for a reauth trigger.

        This is the single choke point that turns a classified reauth trigger
        into diagnosable state. It stores a :class:`ReauthReason` on the
        coordinator (mirrored into diagnostics) and emits a **once-per**
        ``(code, origin)`` WARNING with structured ``extra=`` fields so a
        repeating trigger does not spam the log.

        The inputs are literal-only by contract: ``code`` is a
        :class:`ReauthReasonCode`, ``origin`` is a constant literal string
        supplied at the call site, and ``counters`` (defaulting to a snapshot of
        the transient-auth-failure counter and its threshold) are plain ints.
        No token, e-mail, or ``str(err)`` content is ever stored or logged here.

        Args:
            code: The classified reauth reason code.
            origin: A constant literal string identifying the call site.
            counters: Optional counter snapshot; when omitted the current
                transient-auth-failure count and its threshold are captured.
        """
        if counters is None:
            snapshot: dict[str, int] = {
                "consecutive_transient_auth_failures": int(
                    self._consecutive_transient_auth_failures
                ),
                "max_transient_auth_failures": int(_MAX_TRANSIENT_AUTH_FAILURES),
            }
        else:
            snapshot = {key: int(value) for key, value in counters.items()}

        self._reauth_reason = ReauthReason(
            code=code,
            origin=origin,
            counters=snapshot,
            recorded_at=time.time(),
        )

        dedup_key = (str(code), origin)
        if dedup_key not in self._reauth_reason_logged:
            self._reauth_reason_logged.add(dedup_key)
            _LOGGER.warning(
                "Re-authentication triggered (%s at %s)",
                code,
                origin,
                extra={
                    "reauth_code": str(code),
                    "reauth_origin": origin,
                    "reauth_counters": snapshot,
                },
            )

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
        device_total = len(devices)

        for device_index, dev in enumerate(devices, start=1):
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
                # Stale/blank states withhold the plain latitude/longitude keys;
                # fall back to the recorder-only last_latitude/last_longitude so
                # the snapshot keeps the last known position.
                lat = state.attributes.get(
                    "latitude", state.attributes.get("last_latitude")
                )
                lon = state.attributes.get(
                    "longitude", state.attributes.get("last_longitude")
                )
                # Same authoritative read as the history path: take accuracy
                # from accuracy_m (stable producer attribute) over the volatile
                # gps_accuracy and carry accuracy_estimated, so a fallback radius
                # reused from the published state keeps its provenance instead of
                # being reclassified as real (PR #1124 defect class).
                acc, estimated = recorded_accuracy_pair(state.attributes)
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
                    if estimated is not None:
                        entry["accuracy_estimated"] = estimated
                    entry = _sanitize_decoder_row(entry)
                    snapshot.append(entry)
                    continue

            # Optional history fallback
            if self.allow_history_fallback:
                # AGENTS.md section 5 (b): this record repeats on every poll,
                # so WARNING carries the index and the name/entity_id go to DEBUG.
                _LOGGER.warning(
                    "No live state for device %d of %d; attempting history fallback via Recorder.",
                    device_index,
                    device_total,
                )
                _LOGGER.debug(
                    "History fallback for device %d of %d: %s (entity_id=%s)",
                    device_index,
                    device_total,
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
                        "No historical GPS data found for device %d of %d. "
                        "Entity may be excluded from Recorder.",
                        device_index,
                        device_total,
                    )
                    _LOGGER.debug(
                        "No historical GPS data for device %d of %d: %s (entity_id=%s)",
                        device_index,
                        device_total,
                        entry["name"],
                        entity_id,
                    )

            entry = _sanitize_decoder_row(entry)
            snapshot.append(entry)

        return snapshot

    # ---------------------------- Stats persistence -------------------------
    def begin_new_stats_epoch(self) -> bool:
        """Start a fresh histogram generation: zero, clear, invalidate.

        All three in ONE call, because the counters and the claims are two
        halves of one fact and must not come to hang on two call sites that a
        later edit can separate. An earlier version left the zeroing to the
        caller and claimed to be one call anyway; that was only safe as long as
        no ``await`` appeared between the two sites, an invariant nobody had
        written down. Now there is nothing to keep in step.

        The epoch is what makes the reset survive a load that is still in
        flight. ``_async_load_stats`` is created as a task during setup and can
        resume long after the user pressed the button; it adds the stored
        counters onto the live ones and merges the stored claim rings back in.
        Without a generation to compare against, that undoes the reset silently
        and the debounced write then makes the old generation durable again.

        Returns whether the counters were zeroed, so the caller can log a
        coordinator without a usable ``stats`` mapping the way it did before.
        The epoch and the claims are reset either way: a broken counter mapping
        is no reason to let a stale generation come back.
        """
        self._stats_epoch = getattr(self, "_stats_epoch", 0) + 1
        self.clear_tally_claims()
        stats = getattr(self, "stats", None)
        if not isinstance(stats, dict):
            return False
        for key in list(stats):
            stats[key] = 0
        return True

    async def _async_load_stats(self) -> None:
        """Load statistics from entry-scoped cache."""
        epoch = getattr(self, "_stats_epoch", 0)
        try:
            cached = await self._cache.async_get_cached_value("integration_stats")
            if getattr(self, "_stats_epoch", 0) != epoch:
                # A reset happened while this read was in flight. Both halves of the
                # record below belong to the generation the user just discarded, so
                # restoring either would undo the reset - the counters by addition, the
                # claims by suppressing the very reports that would refill the fresh
                # histogram. Dropping the read is the only outcome that keeps the pair
                # consistent; there is nothing to merge a discarded generation into.
                _LOGGER.debug("Discarded a stats load overtaken by a reset")
            elif cached and isinstance(cached, dict):
                # This load is scheduled, not awaited, so a push or the first poll can
                # already have counted something by the time it resumes. The live value
                # is not a newer total, it is the increments that happened since this
                # read began - so the two are ADDED. Keeping only one of them loses the
                # other: the persisted total if the live value wins, and an increment
                # whose claim survives if the persisted total does. Either way the pair
                # would disagree, and a claim without its increment permanently
                # suppresses a measurement.
                for key, value in self.stats.items():
                    stored = cached.get(key)
                    if isinstance(stored, int) and isinstance(value, int):
                        self.stats[key] = stored + value
                _LOGGER.debug("Loaded statistics from cache: %s", self.stats)
                self._restore_tally_claims(cached.get(TALLY_CLAIMS_KEY))
        except Exception as err:
            _LOGGER.debug("Failed to load statistics from cache: %s", err)
        finally:
            # In a finally, so a failed read closes the window too: from here on a
            # purge is the only writer of the claim store and needs no bookkeeping.
            self._stats_loaded = True
            purged = getattr(self, "_purged_before_stats_load", None)
            if purged is not None:
                purged.clear()
            if getattr(self, "_save_after_stats_load", False):
                # A purge landed inside the window and deferred its write to here,
                # where live state finally carries the merged record. Immediate rather
                # than debounced, for the reason the purge path states: a reload or
                # shutdown inside the debounce window cancels a pending write without
                # flushing it.
                # Cleared only once the task exists, so the flag is never cleared for
                # a write that did not get scheduled. Nothing reads it again after this
                # line - this is its only reader - so a stuck True has no effect; the
                # point is that the exception does not escape this ``finally`` and abort
                # the sound-UUID load below.
                try:
                    self.hass.async_create_task(self._async_save_stats())
                except Exception as err:
                    _LOGGER.debug(
                        "Deferred stats write could not be scheduled: %s", err
                    )
                else:
                    self._save_after_stats_load = False

        try:
            sound_request_uuids = await self._cache.async_get_cached_value(
                "sound_request_uuids"
            )
            if not isinstance(sound_request_uuids, dict):
                _LOGGER.debug("No cached Play Sound UUIDs found; keeping current map")
                return

            # FIX: Filter out stale UUIDs to prevent Play Sound re-trigger after restart (#108)
            # Sound requests older than SOUND_UUID_MAX_AGE_S are considered expired
            max_age_seconds = SOUND_UUID_MAX_AGE_S
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
                    if is_sound_uuid_expired(ts_val, now, max_age_seconds):
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

    def _restore_tally_claims(self, stored: Any) -> None:
        """Restore the per-device tally claim from the stats record.

        JSON has no tuples, so each stored identity comes back as a list and is turned
        back into the tuple the comparison expects; a list never equals a tuple, and that
        failure would be silent. The value per device is a ring of identities, so the
        conversion runs per element rather than once. A missing sub-key is a record written before this
        existed: the map stays empty, which is the old behaviour and not an error. An
        entry the loader cannot read is dropped rather than half-trusted, because a
        half-read claim would silence a report that was never counted.
        """
        if not isinstance(stored, dict):
            return
        purged = getattr(self, "_purged_before_stats_load", None) or set()
        restored: dict[str, list[tuple[Any, ...]]] = {}
        for device_id, ring in stored.items():
            if not isinstance(device_id, str) or not isinstance(ring, list):
                continue
            if device_id in purged:
                # Deleted while this read was in flight. Restoring it would let a
                # device re-added under the same id have its first matching
                # measurement suppressed by a claim from its previous life.
                continue
            identities = [
                tuple(identity)
                for identity in ring
                if isinstance(identity, (list, tuple))
            ]
            if identities:
                restored[device_id] = identities
        if not restored:
            return
        # Merge rather than replace, for the same race: a claim made while this load was
        # in flight belongs to an increment that already happened, and dropping it would
        # let the next delivery count that report again. Stored identities are appended
        # behind the live ones and the ring is trimmed, so the bound still holds.
        live = getattr(self, "_last_tallied_report_id", None)
        if not live:
            self._last_tallied_report_id = restored
        else:
            for device_id, identities in restored.items():
                current = list(live.get(device_id) or ())
                # Restored first, live last. The ring keeps its TAIL, so the order
                # decides who is dropped when both halves are full: with the live
                # entries in front, a claim made during this very load would be trimmed
                # away and the report it belongs to counted a second time. The oldest
                # stored identity is the right thing to lose.
                ring = [i for i in identities if i not in current] + current
                live[device_id] = ring[-TALLY_RING_LIMIT:]
        _LOGGER.debug("Restored %d tally claim(s) from cache", len(restored))

    async def _async_save_stats(self) -> None:
        """Persist statistics and their tally claims as ONE record.

        The histogram and the claim that keeps a report out of it twice are two halves
        of one fact. Written separately, a crash or a failed write between them leaves
        a claim without its increment - which silences a measurement that was never
        recorded - or an increment without its claim, which counts one twice on the next
        delivery. One key, one write, one failure mode: either both are yesterday's
        state or both are today's.

        The claims travel under a reserved sub-key. The loader only copies keys it
        already knows into ``self.stats``, so the extra entry cannot become a phantom
        counter, and the name is asserted not to collide with a real one.

        Nothing is written while the stats load is still in flight. This method
        serialises the WHOLE record from live state, and inside that window live state
        is missing everything the load has not merged yet - so a write there does not
        add to the stored record, it REPLACES the persisted totals and every claim ring
        with pre-load values, and the load then reads back what the write left. The
        guard belongs here rather than at the call sites: of the four callers, one is
        the load's own ``finally`` (by construction outside the window), one is
        ``purge_device``, which had to learn the rule for itself, and two - the debounced
        writer and the flush in ``async_shutdown`` - used to reach the window with
        nothing to stop them. A rule that only one of its callers knows is not a rule.

        The write is deferred, not dropped: the load's ``finally`` performs it once live
        state carries the merged record, and that ``finally`` runs on the success path,
        on the read-failure path and on the epoch-discard path alike.

        Deferral only works while there is still somewhere to write. On an unload there
        is not: the entry-scoped cache is closed right after ``async_shutdown`` returns,
        and a write reaching it afterwards raises and is swallowed below. That is why
        ``async_shutdown`` WAITS for the load task before it flushes, rather than relying
        on the deferral - the wait closes the window, and the flush then writes the merged
        record for real. Deferring is the answer for writers that have a future; for the
        one that does not, the window is closed instead.

        ``_stats_loaded`` defaults to True when absent, so a coordinator built without it
        writes as before; defaulting to False would silence every such writer instead,
        which is the mistake this direction is easy to make.
        """
        assert TALLY_CLAIMS_KEY not in self.stats, (
            f"{TALLY_CLAIMS_KEY} collides with a real counter"
        )
        if not getattr(self, "_stats_loaded", True):
            self._save_after_stats_load = True
            _LOGGER.debug("Deferred a stats write issued inside the load window")
            return
        claims = getattr(self, "_last_tallied_report_id", None) or {}
        record: dict[str, Any] = self.stats.copy()
        record[TALLY_CLAIMS_KEY] = {
            device_id: [
                list(identity) for identity in ring if isinstance(identity, tuple)
            ]
            for device_id, ring in claims.items()
            if isinstance(device_id, str) and isinstance(ring, list)
        }
        try:
            await self._cache.async_set_cached_value("integration_stats", record)
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
        """Increment a statistic on the HA loop and schedule persistence.

        NOTE (Issue #151): This method intentionally does NOT call
        ``async_update_listeners()``. Statistics counters live in
        ``self.stats`` and do not change ``self.data`` (the device snapshot
        list that entities react to). Notifying listeners on every stat
        increment caused a multiplicative entity-refresh storm: each FCM
        push triggered up to six ``async_update_listeners()`` calls
        (one per stat increment in ``update_device_cache`` plus one
        per crowd-source record), and each notification re-wrote the
        state of every ``device_tracker`` and sensor entity. With
        ~15 trackers and active FCM crowd-sourced reporting this saturated
        the Home Assistant WebSocket buffer (``4096 pending messages``)
        within seconds. Stats sensors are still refreshed by the regular
        coordinator tick (``UPDATE_INTERVAL``) and any real data-changing
        path (``push_updated`` / ``async_set_updated_data``).
        """
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

    def _refresh_canonicless_drop_stats(self, entry_id: str | None) -> None:
        """Pull the decoder's per-poll canonicless tally into ``self.stats``.

        The decoder publishes the transition-independent drop tally for the entry
        scope (written by the main poll); the coordinator mirrors it into the
        three persisted stat counters. Values are set ABSOLUTELY (the accessor
        already returns the per-poll aggregate, so a recovering count goes down)
        rather than incremented, then persistence is scheduled so the values
        survive a restart. A missing entry id is a no-op.
        """
        if not entry_id:
            return
        counts = decoder.get_canonicless_counts(entry_id)
        self.stats["canonicless_drop_total"] = counts["total"]
        self.stats["canonicless_drop_benign"] = counts["benign"]
        self.stats["canonicless_drop_warn"] = counts["warn"]
        self._schedule_stats_persist()

    def build_per_device_diagnostics(self) -> list[dict[str, Any]]:
        """Build the anonymized per-device telemetry list for diagnostics (P1-3).

        Each entry carries an opaque position ``index`` over ``sorted(device_ids)``
        plus ``device_class``, ``last_poll_age_s``, ``last_fix_age_s``,
        ``last_accuracy_bucket``, ``is_own_report``, ``has_key`` and the two
        accuracy-gate fields ``coarse_fix_accuracy_bucket`` /
        ``coarse_fix_age_s``. No names, canonical IDs, coordinates, or
        clear-text keys are emitted. Empty/None ``self.data`` yields ``[]``; any
        failure degrades to ``[]`` rather than leaking a partial record.

        Both wall-clock ages report ``None`` rather than ``0`` when their stamp lies in
        the future, so a corrupt stamp is never dressed up as a fresh one; see
        ``_wall_clock_age``.

        The two coarse-fix fields report what the accuracy gate discarded (#216)
        WITHOUT its coordinates: a diagnostics dump is routinely pasted into a
        public issue, and a raw radius or position would be re-identifying
        (P1-3). The coordinates of the discarded fix stay on the entity, where
        the user - and only the user - can see roughly which city it named.
        """
        try:
            rows = self.data or []
            by_id: dict[str, dict[str, Any]] = {}
            for row in rows:
                if not isinstance(row, dict):
                    continue
                dev_id = row.get("device_id")
                if isinstance(dev_id, str) and dev_id:
                    by_id[dev_id] = row

            now_epoch = time.time()
            now_mono = time.monotonic()

            entries: list[dict[str, Any]] = []
            for index, dev_id in enumerate(sorted(by_id)):
                row = by_id[dev_id]
                slot = self._device_location_data.get(dev_id) or {}

                last_fix_age_s = _wall_clock_age(now_epoch, row.get("last_seen"))

                poll_mono = self._present_last_seen.get(dev_id)
                if isinstance(poll_mono, (int, float)) and not isinstance(
                    poll_mono, bool
                ):
                    last_poll_age_s: int | None = _round_age(
                        max(0.0, now_mono - float(poll_mono))
                    )
                else:
                    last_poll_age_s = None

                # Runtime ``self.data`` snapshot rows carry the radius under
                # ``accuracy``; ``accuracy_m`` is only produced later by
                # ``_as_ha_attributes()`` for HA entity attributes. Prefer the
                # HA-shaped key when present (defensive), else read the snapshot
                # field, so the bucket is populated on the normal runtime path.
                accuracy_raw = row.get("accuracy_m")
                if accuracy_raw is None:
                    accuracy_raw = row.get("accuracy")

                coarse = self.get_fresh_coarse_fix(dev_id)
                coarse_bucket = None
                coarse_age_s: int | None = None
                if coarse:
                    coarse_bucket = _accuracy_bucket(coarse.get("accuracy"))
                    coarse_age_s = _wall_clock_age(now_epoch, coarse.get("last_seen"))

                entries.append(
                    {
                        "index": index,
                        "device_class": _device_class(slot.get("device_type")),
                        "last_poll_age_s": last_poll_age_s,
                        "last_fix_age_s": last_fix_age_s,
                        "last_accuracy_bucket": _accuracy_bucket(accuracy_raw),
                        "is_own_report": row.get("is_own_report", None),
                        "has_key": bool(slot.get("encrypted_identity_key")),
                        "coarse_fix_accuracy_bucket": coarse_bucket,
                        "coarse_fix_age_s": coarse_age_s,
                    }
                )
            return entries
        except Exception:  # pragma: no cover - defensive: never break diagnostics
            return []

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

    def locate_requested_mono(self, device_id: str) -> float | None:
        """Return when a location request for ``device_id`` was last sent.

        The value is on the ``time.monotonic`` clock and is ``None`` when this
        process has not sent a request for the device yet. It marks the moment
        before the POST, after push registration and token fetch; an attempt
        that stopped earlier left no value, and a request that was sent may
        still have failed afterwards (see ``helpers/locate_clock.py``). Read
        defensively so a coordinator built without ``__init__`` answers
        ``None`` instead of raising.
        """
        stamps: dict[str, float] = getattr(self, "_locate_requested_mono", {})
        return stamps.get(device_id)

    def purge_device(self, device_id: str) -> None:
        """Remove all cached data and cooldown state for a device (thread-safe publish).

        Called from the config-entry device deletion flow. This does not trigger a poll,
        but it immediately publishes an updated snapshot so UI can refresh.
        """
        if not self._is_on_hass_loop():
            self._run_on_hass_loop(self.purge_device, device_id)
            return

        self._device_location_data.pop(device_id, None)
        # Drop the last-good fallback too: otherwise get_display_location_data
        # keeps serving the purged device's last Plus Code/coordinates whenever
        # the current row is gone, leaking a deleted device's position until
        # restart or re-add (the current-row cache above is cleared, so the
        # display accessors fall straight through to this map).
        self._device_last_good_location.pop(device_id, None)
        # Round-trip anchor shares the same lifecycle as the location caches above.
        self._round_trip_anchors.pop(device_id, None)
        # The identities of fused reports belong to the cached row they describe: a
        # device re-added under the same id must fuse its first report, not refuse it
        # as a repeat. Read defensively because the store is created lazily.
        fused_reports = getattr(self, "_fused_report_ids", None)
        if fused_reports is not None:
            fused_reports.pop(device_id, None)
        # The accuracy gate's coarse fix (#216) is a position too, so it follows the
        # same lifecycle: without this a deleted device would keep leaking its last
        # coarse position through ``coarse_latitude``/``coarse_longitude``, and a
        # device re-added under the same id would inherit it. Read defensively
        # (getattr) because the store is created lazily on the first rejection.
        coarse_store = getattr(self, "_device_coarse_fix", None)
        if coarse_store is not None:
            coarse_store.pop(device_id, None)
        # A device re-added under the same id must not inherit a silence streak.
        silence_streaks = getattr(self, "_empty_location_streak", None)
        if silence_streaks is not None:
            silence_streaks.pop(device_id, None)
        # The tally claim is keyed by device id and is PERSISTED, so it outlives the
        # purge unless it is dropped here: a deleted device would keep an entry in the
        # store indefinitely, and one re-added under the same id would have its first
        # matching measurement suppressed by a claim from its previous life.
        # A load that is still in flight will merge the STORED rings back in, and it
        # cannot know about a device deleted after its read began. The epoch does not
        # help here: it discards a whole generation, while a purge removes one device
        # from one that stays valid. So the id is remembered until the load is done,
        # and remembered unconditionally - the claim may not even be in memory yet,
        # which is exactly the case the pop below cannot see.
        noted = False
        if not getattr(self, "_stats_loaded", True):
            purged = getattr(self, "_purged_before_stats_load", None)
            if purged is not None:
                purged.add(device_id)
                noted = True
        claims = getattr(self, "_last_tallied_report_id", None)
        popped = claims is not None and claims.pop(device_id, None) is not None
        if noted:
            # Inside the load window the durable half must be corrected too - the claim
            # is not in memory yet, so the pop finds nothing while the STORED record
            # still lists the device. But NOT by writing now: ``_async_save_stats``
            # serialises the whole record from live state, and live state is currently
            # missing everything the load has not merged yet, so a write here would
            # replace the persisted totals and every sibling ring with the pre-load
            # values. The load's ``finally`` performs it instead, after the merge.
            self._save_after_stats_load = True
        elif popped:
            # Written now, not on the debounce: a reload or shutdown inside the debounce
            # window cancels the pending write without flushing it, and the purged
            # device's claim would then outlive the device it was deleted with.
            self.hass.async_create_task(self._async_save_stats())
        # Device-id-keyed timing caches follow the same lifecycle; drop them so a
        # re-added device with the same id starts with clean poll-interval state.
        self._device_update_history.pop(device_id, None)
        self._device_interval_history.pop(device_id, None)
        self._ensure_device_name_cache().pop(device_id, None)
        self._device_caps.pop(device_id, None)
        self._locate_inflight.discard(device_id)
        self._locate_cooldown_until.pop(device_id, None)
        request_stamps = getattr(self, "_locate_requested_mono", None)
        if request_stamps is not None:
            request_stamps.pop(device_id, None)
        removed_uuid = self._sound_request_uuids.pop(device_id, None)
        self._sound_request_timestamps.pop(device_id, None)
        accepted_stamps = getattr(self, "_sound_play_accepted_at", None)
        if accepted_stamps is not None:
            accepted_stamps.pop(device_id, None)
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

        # Merge this push snapshot with the existing self.data so non-pushed
        # devices retain their last known state (fixes bouncing state bug).
        # Dict-union (Py3.9+) preserves insertion order of existing devices and
        # appends new device_ids at the end; on key collision the pushed value
        # wins (new_devices second operand).
        # NOTE: filter ignored devices from the old snapshot too (mirrors the
        # `ids` filter above), otherwise a device added to ignored_devices
        # after it already appeared in self.data would be re-published via
        # the merge until a full poll rebuilds the snapshot.
        # getattr() guards against tests/lifecycle paths where `data` is not
        # yet set by the DataUpdateCoordinator base class.
        old_devices = {
            d["device_id"]: d
            for d in (getattr(self, "data", None) or [])
            if isinstance(d, dict)
            and "device_id" in d
            and d["device_id"] not in ignored
        }
        new_devices = {
            d["device_id"]: d
            for d in snapshot
            if isinstance(d, dict) and "device_id" in d
        }
        merged_snapshot = list((old_devices | new_devices).values())

        # Refresh the subentry index and persist snapshots with the COMPLETE
        # merged device list so the index, storage, and the data published to
        # entities are consistent (F-9: pass merged_snapshot instead of None
        # to avoid the index reading stale self.data).
        self._refresh_subentry_index(merged_snapshot)
        self._store_subentry_snapshots(merged_snapshot)
        self.async_set_updated_data(merged_snapshot)
        _LOGGER.debug(
            "Pushed snapshot for %d device(s) via push_updated() (merged total: %d)",
            len(snapshot),
            len(merged_snapshot),
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
