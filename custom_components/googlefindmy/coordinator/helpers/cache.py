"""Cache utilities for the coordinator.

This module contains pure functions for cache operations extracted from
coordinator.py for improved testability and maintainability (Phase 3 + 15).

Contents:
- build_base_snapshot_entry(): Create base snapshot from device dict
- determine_location_status(): Determine status string from age
- epoch_to_datetime_utc(): Convert epoch timestamp to UTC datetime
- is_presence_expired(): Check if presence TTL has expired
- should_allow_location_update(): Determine if location update is allowed
- normalize_location_fields(): Normalize location field types
- preserve_metadata_fields(): Preserve metadata from previous cache
- should_clear_metadata_only_flag(): Determine if flag should be cleared

Phase 15 additions:
- compare_location_freshness(): Compare two timestamps for freshness
- select_best_location_source(): Select best source based on priority
- preserve_monotonic_timestamp(): Preserve monotonic timestamps
- fill_missing_coordinates(): Fill missing coordinate fields
- merge_cache_row(): Orchestrating function for cache row merging

Refactoring additions:
- get_common_pb2(): Lazy protobuf loader
- row_source_label(): Determine source rank and label
- sanitize_decoder_row(): Normalize decoder row for HA attributes
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

# Re-exported for the coordinator-side callers that have always imported them
# from here; the definitions live outside this package so ``Auth`` can use them
# without dragging the coordinator package into its import graph.
from ...location_row_markers import strip_transient_keys, substitute_zone_accuracy
from .geo import haversine_distance, safe_accuracy
from .subentry import format_epoch_utc, normalize_epoch_seconds

__all__ = [
    "DEFAULT_SNAPSHOT_FIELDS",
    "LOCATION_FIELDS",
    "SOUND_UUID_MAX_AGE_S",
    "SOURCE_PRIORITY",
    "STATUS_AGING",
    "STATUS_CURRENT",
    "STATUS_STALE",
    "STATUS_WAITING",
    "build_base_snapshot_entry",
    "carry_reused_accuracy",
    "compare_location_freshness",
    "determine_location_status",
    "epoch_to_datetime_utc",
    "fill_missing_coordinates",
    "get_common_pb2",
    "is_presence_expired",
    "is_sound_uuid_expired",
    "merge_cache_row",
    "normalize_location_fields",
    "preserve_metadata_fields",
    "preserve_monotonic_timestamp",
    "row_source_label",
    "sanitize_decoder_row",
    "select_best_location_source",
    "should_allow_location_update",
    "should_clear_metadata_only_flag",
    "strip_transient_keys",
    "substitute_zone_accuracy",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default fields for a device snapshot entry
DEFAULT_SNAPSHOT_FIELDS = (
    "name",
    "id",
    "device_id",
    "latitude",
    "longitude",
    "altitude",
    "accuracy",
    "last_seen",
    "status",
    "is_own_report",
    "semantic_name",
)

# Status strings for location data freshness
STATUS_CURRENT = "Location data current"
STATUS_AGING = "Location data aging"
STATUS_STALE = "Location data stale"
STATUS_WAITING = "Waiting for location poll"

# Location fields that should be blocked during location update rejection
LOCATION_FIELDS = frozenset(
    {
        "latitude",
        "longitude",
        "accuracy",
        # Travels atomically with ``accuracy``: when an insignificant update is
        # rejected the existing accuracy is kept, so its estimated flag must be
        # kept too. Otherwise a real fix would be paired with a rejected row's
        # flag (and vice versa), reintroducing the 200m-fallback ambiguity.
        "accuracy_estimated",
        "altitude",
        "status",
        "source_label",
        "source_rank",
        "is_own_report",
        "last_seen",
        "last_seen_utc",
    }
)

# Source priority for location data (higher = more authoritative)
SOURCE_PRIORITY: dict[str, int] = {
    "api": 10,
    "poll": 8,
    "fcm": 5,
    "cache": 2,
    "unknown": 0,
}

# Epsilon for timestamp comparison (floating point tolerance)
_TIMESTAMP_EPSILON = 0.001


# ---------------------------------------------------------------------------
# Snapshot Building
# ---------------------------------------------------------------------------


def build_base_snapshot_entry(device_dict: dict[str, Any]) -> dict[str, Any]:
    """Create the base snapshot entry for a device.

    This centralizes the common fields to keep snapshot builders DRY.
    No cache lookups or coordinator state access happens here.

    Args:
        device_dict: A dictionary containing basic device info (id, name).

    Returns:
        A dictionary with default fields for a device snapshot.
    """
    dev_id = device_dict["id"]
    dev_name = device_dict.get("name") or dev_id
    return {
        "name": dev_name,
        "id": dev_id,
        "device_id": dev_id,
        "latitude": None,
        "longitude": None,
        "altitude": None,
        "accuracy": None,
        "last_seen": None,
        "status": STATUS_WAITING,
        "is_own_report": None,
        "semantic_name": None,
    }


# ---------------------------------------------------------------------------
# Location Status
# ---------------------------------------------------------------------------


def determine_location_status(age: float, poll_interval: float) -> str:
    """Determine the location status string based on data age.

    Args:
        age: Age of the location data in seconds.
        poll_interval: The configured poll interval in seconds.

    Returns:
        One of: "Location data current", "Location data aging",
        or "Location data stale".

    This labels the coordinator snapshot row against the poll interval and is
    not the entity-facing location status; that one is
    ``classify_location_status`` in ``helpers/geo.py`` (stale-threshold based,
    shared by the device_tracker and the ``location_status`` sensor).
    """
    if age < poll_interval:
        return STATUS_CURRENT
    elif age < poll_interval * 2:
        return STATUS_AGING
    else:
        return STATUS_STALE


# ---------------------------------------------------------------------------
# DateTime Conversion
# ---------------------------------------------------------------------------


def epoch_to_datetime_utc(ts: float | int | str | None) -> datetime | None:
    """Convert an epoch timestamp to a timezone-aware UTC datetime.

    Args:
        ts: Epoch timestamp as float, int, string, or None.

    Returns:
        A timezone-aware datetime object in UTC, or None if conversion fails.
    """
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=UTC)
    except (TypeError, ValueError, OSError):
        return None


# ---------------------------------------------------------------------------
# Presence Checking
# ---------------------------------------------------------------------------


def is_presence_expired(
    last_seen_mono: float | None,
    now_mono: float,
    ttl_seconds: float,
) -> bool:
    """Check if a device's presence has expired based on TTL.

    Args:
        last_seen_mono: Monotonic timestamp when device was last seen,
                        or None/0 if never seen.
        now_mono: Current monotonic time.
        ttl_seconds: Time-to-live in seconds before presence expires.

    Returns:
        True if presence has expired, False if still valid.
    """
    if not last_seen_mono:
        return True
    return (now_mono - float(last_seen_mono)) > ttl_seconds


# ---------------------------------------------------------------------------
# Play Sound UUID Expiry
# ---------------------------------------------------------------------------

# Age at which a stored Play Sound cancel key stops being TRUSTED.
#
# This is not a claim about when a ring ends. It bounds two things only:
#   * store hygiene -- the reload filter drops keys older than this, so the
#     persisted map cannot grow without limit over deleted devices;
#   * the overwrite guard -- an ambiguous play (ok=False) may replace a key
#     older than this, but never a fresher one.
# An aged key is still SENT on a Stop; it is simply reported as an uncorrelated
# attempt rather than a confirmed cancel (coordinator/locate.py, async_stop_sound).
#
# The value stays at 30 minutes deliberately. No measurement of the Nova queue
# latency exists in this project, so every alternative number would be invented,
# and correctness does not rest on it: it rests on
# IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY plus the uncorrelated-outcome rule above.
SOUND_UUID_MAX_AGE_S = 30 * 60  # 30 minutes


def is_sound_uuid_expired(
    ts: float,
    now: float,
    max_age_seconds: float,
) -> bool:
    """Check if a stored Play Sound UUID has aged out.

    Single source of truth for the sound-UUID expiry so the load path, the
    store-path overwrite guard, and the Stop read-path all agree on what
    "stale" means (a divergent inline check caused PR #1106's follow-up bug).

    Args:
        ts: Epoch timestamp when the UUID was stored (wall-clock seconds).
            A missing/zero timestamp compares as expired against any positive
            ``now``, matching the load path's treatment of untimestamped keys.
        now: Current epoch time (``time.time()``).
        max_age_seconds: Maximum age before the UUID is considered stale.

    Returns:
        True if the UUID is older than ``max_age_seconds``, False otherwise.
    """
    return (now - float(ts)) > max_age_seconds


# ---------------------------------------------------------------------------
# Location Update Decision
# ---------------------------------------------------------------------------


def should_allow_location_update(
    existing_seen: int | float | None,
    incoming_seen: int | float | None,
    existing_rank: int | None,
    incoming_rank: int | None,
) -> bool | None:
    """Determine if a location update should be allowed based on timestamps and ranks.

    This implements the core logic for deciding whether to accept an incoming
    location update over an existing cached location.

    Args:
        existing_seen: Epoch timestamp of existing cached location (or None).
        incoming_seen: Epoch timestamp of incoming location (or None).
        existing_rank: Source rank of existing location (higher = more authoritative).
        incoming_rank: Source rank of incoming location.

    Returns:
        True if update should be allowed.
        False if update should be blocked.
        None if the decision requires additional context (e.g., distance check).
    """
    # Case 1: Both have timestamps - compare them
    if existing_seen is not None and incoming_seen is not None:
        if incoming_seen > existing_seen:
            return True
        if incoming_seen < existing_seen:
            return False
        # Same timestamp - compare ranks
        if existing_rank is not None and (
            incoming_rank is None or existing_rank > incoming_rank
        ):
            return False
        return True

    # Case 2: No existing timestamp - allow update
    if existing_seen is None and incoming_seen is not None:
        return True

    # Case 3: Existing has timestamp, incoming doesn't - needs distance check
    if existing_seen is not None and incoming_seen is None:
        return None  # Caller must do distance-based decision

    # Case 4: Neither has timestamp - compare ranks
    if existing_rank is not None and (
        incoming_rank is None or existing_rank > incoming_rank
    ):
        return False

    return True


def normalize_location_fields(
    data: dict[str, Any],
    fields: Sequence[str] = ("latitude", "longitude", "accuracy", "altitude"),
) -> dict[str, Any]:
    """Normalize location field types to float.

    Converts string representations of numbers to floats.
    Invalid values become None.

    Args:
        data: Location data dictionary.
        fields: Field names to normalize.

    Returns:
        New dictionary with normalized fields.
    """
    result = dict(data)

    for field in fields:
        value = result.get(field)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            result[field] = float(value)
        elif isinstance(value, str):
            try:
                result[field] = float(value)
            except ValueError:
                result[field] = None

    return result


def carry_reused_accuracy(
    target: dict[str, Any],
    source: Mapping[str, Any],
    *,
    setdefault: bool = False,
) -> None:
    """Copy a reused ``accuracy`` together with its ``accuracy_estimated`` flag.

    ``accuracy_estimated`` is a producer-side flag: ``True`` means the numeric
    ``accuracy`` is the conservative 200m fallback radius, not a real measurement.
    Once the value is the 200m fallback it is byte-for-byte indistinguishable from
    a genuine 200m fix downstream (fusion, the significance gate, the recorder and
    map_view), so only this flag tells the two apart.

    The upstream preserve-sites (Google-Home filter and semantic-only fallbacks in
    ``polling`` and ``locate``) reuse a previously cached ``accuracy``. Copying the
    value alone strips the flag, letting a downstream chokepoint reclassify a
    preserved fallback as a real measurement. This helper keeps the flag travelling
    with the value so the reclassification cannot happen, centralizing the rule in
    one place instead of repeating it at every preserve-site.

    Two modes mirror the two call shapes at the sites:

    - Assignment (default): unconditionally set ``target["accuracy"]`` from
      ``source`` and mirror the flag. When ``source`` has no flag the target's flag
      is removed, so a stale ``True`` cannot linger next to an unflagged value.
    - ``setdefault=True``: only fill ``accuracy``/``accuracy_estimated`` when the
      target lacks ``accuracy`` (matching the ``dict.setdefault`` semantics at the
      locate semantic-only site). A ``None`` flag is never injected.

    Args:
        target: Location payload being assembled (mutated in place).
        source: Previously cached location row to reuse the accuracy from.
        setdefault: When True, preserve existing values instead of overwriting.
    """
    if setdefault:
        if "accuracy" in target:
            # Target already declares an accuracy (the ``dict.setdefault`` no-op
            # case): leave both the value and any existing flag untouched.
            return
        target["accuracy"] = source.get("accuracy")
        if "accuracy_estimated" in source:
            # Only fill the flag when the source actually carries it; never
            # inject a None flag for an unflagged source row.
            target.setdefault("accuracy_estimated", source["accuracy_estimated"])
        return

    target["accuracy"] = source.get("accuracy")
    if "accuracy_estimated" in source:
        target["accuracy_estimated"] = source["accuracy_estimated"]
    else:
        # Source describes an unflagged fix; do not keep a stale estimated flag.
        target.pop("accuracy_estimated", None)


def preserve_metadata_fields(
    previous: dict[str, Any] | Mapping[str, Any] | None,
    current: dict[str, Any],
    metadata_keys: Sequence[str],
) -> dict[str, Any]:
    """Preserve metadata fields from previous cache when missing in current.

    Deep copies dict and list values to avoid shared references.

    Args:
        previous: Previous cache entry (or None).
        current: Current/incoming data.
        metadata_keys: Field names to preserve from previous.

    Returns:
        New dictionary with preserved metadata fields.
    """
    result = dict(current)

    if previous is None or not isinstance(previous, Mapping):
        return result

    for key in metadata_keys:
        # Only preserve if current doesn't have the value
        if key in result and result[key] is not None:
            continue

        cached_value = previous.get(key)
        if cached_value is None:
            continue

        # Deep copy mutable types
        if isinstance(cached_value, dict):
            result[key] = dict(cached_value)
        elif isinstance(cached_value, list):
            result[key] = list(cached_value)
        else:
            result[key] = cached_value

    return result


def should_clear_metadata_only_flag(
    data: dict[str, Any],
    incoming_metadata_only: bool | None,
) -> bool:
    """Determine if the metadata_only flag should be cleared from data.

    The flag should be cleared when:
    - incoming_metadata_only is explicitly False, OR
    - data has location payload AND incoming_metadata_only is not explicitly True

    Args:
        data: Location data dictionary.
        incoming_metadata_only: The incoming metadata_only value (True/False/None).

    Returns:
        True if the metadata_only flag should be cleared.
    """
    # Not set - nothing to clear
    if not data.get("metadata_only"):
        return False

    # Explicit False clears the flag
    if incoming_metadata_only is False:
        return True

    # Explicit True preserves the flag
    if incoming_metadata_only is True:
        return False

    # Check if there's location data
    has_location = data.get("latitude") is not None or data.get("longitude") is not None

    return has_location


# ---------------------------------------------------------------------------
# Phase 15: Cache Row Merge Helper Functions
# ---------------------------------------------------------------------------


def _normalize_epoch_seconds(value: Any) -> float | None:
    """Normalize a timestamp value to epoch seconds as float.

    Args:
        value: Timestamp value (float, int, str, or None).

    Returns:
        Normalized float timestamp or None.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    """Coerce a value to float.

    Args:
        value: Value to coerce.

    Returns:
        Float value or None if coercion fails.
    """
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compare_location_freshness(
    existing_ts: float | int | None,
    new_ts: float | int | None,
) -> int:
    """Compare two timestamps to determine relative freshness.

    Args:
        existing_ts: Existing location timestamp (epoch seconds).
        new_ts: New location timestamp (epoch seconds).

    Returns:
        1 if new is fresher (newer), -1 if existing is fresher, 0 if equal.
    """
    existing_norm = _normalize_epoch_seconds(existing_ts)
    new_norm = _normalize_epoch_seconds(new_ts)

    if existing_norm is None and new_norm is None:
        return 0
    if existing_norm is None:
        return 1  # New has timestamp, existing doesn't
    if new_norm is None:
        return -1  # Existing has timestamp, new doesn't

    # Compare with epsilon tolerance
    if abs(new_norm - existing_norm) < _TIMESTAMP_EPSILON:
        return 0
    if new_norm > existing_norm:
        return 1
    return -1


def select_best_location_source(
    source_a: str | None,
    source_b: str | None,
) -> str:
    """Select the best location source based on priority.

    Higher priority sources are more authoritative.

    Args:
        source_a: First source identifier.
        source_b: Second source identifier.

    Returns:
        The source with higher priority, or 'unknown' if both are None.
    """
    prio_a = SOURCE_PRIORITY.get(source_a or "unknown", 0)
    prio_b = SOURCE_PRIORITY.get(source_b or "unknown", 0)

    if prio_a >= prio_b:
        return source_a or "unknown"
    return source_b or "unknown"


def preserve_monotonic_timestamp(
    existing: dict[str, Any],
    merged: dict[str, Any],
) -> dict[str, Any]:
    """Ensure timestamps are monotonically preserved.

    When incoming data has an older or missing timestamp, preserve the
    existing timestamp to maintain monotonicity.

    Args:
        existing: Existing cache entry.
        merged: Merged data that may have older timestamps.

    Returns:
        New dictionary with monotonically preserved timestamps.
    """
    result = dict(merged)

    existing_seen = _normalize_epoch_seconds(existing.get("last_seen"))
    merged_seen = _normalize_epoch_seconds(merged.get("last_seen"))

    # Preserve existing timestamp if it's newer
    if existing_seen is not None and (
        merged_seen is None or merged_seen < existing_seen
    ):
        result["last_seen"] = existing.get("last_seen")
        if existing.get("last_seen_utc") is not None:
            result["last_seen_utc"] = existing.get("last_seen_utc")
    elif (
        merged_seen is not None
        and existing_seen is not None
        and abs(merged_seen - existing_seen) < _TIMESTAMP_EPSILON
        and result.get("last_seen_utc") is None
        and existing.get("last_seen_utc") is not None
    ):
        # Same timestamp but merged lacks UTC - preserve existing UTC
        result["last_seen_utc"] = existing.get("last_seen_utc")

    return result


def fill_missing_coordinates(
    existing: dict[str, Any],
    merged: dict[str, Any],
) -> dict[str, Any]:
    """Fill missing coordinate fields from existing data.

    Args:
        existing: Existing cache entry with coordinates.
        merged: Merged data that may have missing coordinates.

    Returns:
        New dictionary with missing coordinates filled.
    """
    result = dict(merged)

    for coord_field in ("latitude", "longitude", "accuracy", "altitude"):
        if result.get(coord_field) is None and existing.get(coord_field) is not None:
            result[coord_field] = existing.get(coord_field)

    return result


def merge_cache_row(
    existing: dict[str, Any] | None,
    incoming: dict[str, Any],
) -> dict[str, Any]:
    """Merge incoming location data with existing cache row.

    This is the main orchestrating function that combines all the merge logic:
    1. Determine if location update should be allowed based on timestamps/ranks
    2. If allowed, merge all fields; if not, only merge non-location fields
    3. Preserve monotonic timestamps
    4. Fill missing coordinates from existing

    When timestamp-based ordering is inconclusive (``should_allow_location_update``
    returns ``None``), an **accuracy-adaptive significance threshold** decides
    whether the positional change is real or measurement noise.  The threshold
    is ``0.5 * sqrt(acc_existing² + acc_incoming²)`` -- the combined standard
    deviation of two independent Gaussian position errors, scaled by 0.5 so
    that genuine movement is accepted quickly while jitter is suppressed.

    Practical examples:
    - GPS 10 m + GPS 10 m  →  threshold ≈  7 m  (fine-grained updates)
    - BLE 200 m + BLE 200 m →  threshold ≈ 141 m  (only real jumps)
    - GNSS 2 m + GNSS 2 m  →  threshold ≈  1.4 m (near-realtime)

    Args:
        existing: Existing cache entry (or None).
        incoming: Incoming location data.

    Returns:
        Merged cache row dictionary.
    """
    if not existing:
        return dict(incoming)

    # Get normalized timestamps and ranks
    existing_seen = _normalize_epoch_seconds(existing.get("last_seen"))
    incoming_seen = _normalize_epoch_seconds(incoming.get("last_seen"))
    existing_rank = existing.get("source_rank")
    incoming_rank = incoming.get("source_rank")

    # Determine if location update should be allowed
    allow_update = should_allow_location_update(
        existing_seen, incoming_seen, existing_rank, incoming_rank
    )

    # Handle case where distance check is needed (allow_update is None)
    if allow_update is None:
        # Need to check distance for significant change
        incoming_lat = _coerce_float(incoming.get("latitude"))
        incoming_lon = _coerce_float(incoming.get("longitude"))
        existing_lat = _coerce_float(existing.get("latitude"))
        existing_lon = _coerce_float(existing.get("longitude"))

        if (
            incoming_lat is not None
            and incoming_lon is not None
            and existing_lat is not None
            and existing_lon is not None
        ):
            try:
                dist = haversine_distance(
                    existing_lat, existing_lon, incoming_lat, incoming_lon
                )
                # Accuracy-adaptive significance: movement must exceed
                # the combined measurement uncertainty to be real.
                # sqrt(a1² + a2²) is the joint std-dev of two independent
                # Gaussian-distributed position errors.  Factor 0.5 keeps
                # us permissive enough for genuine movement while still
                # suppressing jitter.
                existing_acc = safe_accuracy(_coerce_float(existing.get("accuracy")))
                incoming_acc = safe_accuracy(_coerce_float(incoming.get("accuracy")))
                adaptive_threshold = math.sqrt(existing_acc**2 + incoming_acc**2) * 0.5
                allow_update = dist > adaptive_threshold
            except Exception:
                allow_update = False
        else:
            allow_update = False

    # Build merged result
    merged = dict(existing)

    if allow_update:
        # Update all fields from incoming
        merged.update(incoming)
    else:
        # Only update non-location fields
        for key, value in incoming.items():
            if key not in LOCATION_FIELDS:
                merged[key] = value

    # Preserve monotonic timestamps
    merged = preserve_monotonic_timestamp(existing, merged)

    # Fill missing coordinates
    merged = fill_missing_coordinates(existing, merged)

    return merged


# ---------------------------------------------------------------------------
# Row Sanitization Helpers (moved from main.py during refactoring)
# ---------------------------------------------------------------------------

# Sentinel for missing protobuf attributes
_MISSING = object()


def get_common_pb2() -> Any:
    """Import Common_pb2 lazily to defer protobuf initialization.

    Returns:
        The Common_pb2 module for protobuf status constants.
    """
    from ... import get_proto_decoder

    return get_proto_decoder("Common_pb2")


def row_source_label(row: dict[str, Any]) -> tuple[int, str]:
    """Determine (rank, label) for the source of a report.

    Rank: 3=owner, 2=crowdsourced, 1=aggregated, 0=semantic/unknown
    Label: 'owner' | 'crowdsourced' | 'aggregated' | 'semantic/unknown'

    Internal label mapping aligns with the protobuf Status enum and Google
    Contribution Settings:
    - 'crowdsourced' comes from Status.CROWDSOURCED (finder opted into
      "with network in all areas").
    - 'aggregated' comes from Status.AGGREGATED (finder opted into "with
      network in high-traffic areas only").

    Args:
        row: Dictionary containing report data.

    Returns:
        Tuple of (rank, label) indicating source type.
    """
    is_own = bool(row.get("is_own_report"))
    status_code = row.get("status_code")
    try:
        status_code_int = int(status_code) if status_code is not None else None
    except (TypeError, ValueError):
        status_code_int = None

    raw_status = row.get("status")
    if isinstance(raw_status, str):
        status_name = raw_status.strip().lower()
    elif isinstance(raw_status, (int, float)):
        try:
            status_name = get_common_pb2().Status.Name(int(raw_status)).lower()
        except Exception:
            status_name = str(int(raw_status))
    else:
        status_name = ""

    hint = str(row.get("_report_hint") or "").strip().lower()
    common_pb2 = get_common_pb2()
    cs = getattr(common_pb2, "CROWDSOURCED", _MISSING)
    ag = getattr(common_pb2, "AGGREGATED", _MISSING)

    if is_own:
        return 3, "owner"
    if (
        (cs is not _MISSING and status_code_int == cs)
        or "crowdsourced" in status_name
        or "in_all_areas" in status_name
        or hint == "in_all_areas"
    ):
        return 2, "crowdsourced"
    if (
        (ag is not _MISSING and status_code_int == ag)
        or "aggregated" in status_name
        or "high_traffic" in status_name
        or hint == "high_traffic"
    ):
        return 1, "aggregated"
    return 0, "semantic/unknown"


def _is_invalid_accuracy(accuracy: Any) -> bool:
    """Check if accuracy value is invalid (missing, non-positive, or non-finite).

    GPS accuracy of <= 0 is physically impossible (real GPS: 3-50m typically).
    Such values indicate missing/corrupted data from the API.
    """
    if accuracy is None:
        return True
    try:
        f = float(accuracy)
        # Non-finite (NaN/Inf) or non-positive values are invalid
        return not (f > 0 and math.isfinite(f))
    except (TypeError, ValueError):
        return True


def sanitize_decoder_row(row: dict[str, Any]) -> dict[str, Any]:
    """Enforce protocol invariants and prepare HA attributes.

    Notes:
        - Ensures timestamps are normalized and provides an ISO UTC mirror.
        - Derives a stable `source_label`/`source_rank` used for stats and UX.
        - Detects and corrects spurious `is_own_report=True` flags from the API.
        - Invalid accuracy (<=0, NaN, Inf) is coerced to None.

    Args:
        row: Raw decoder row dictionary.

    Returns:
        Sanitized dictionary with normalized fields.
    """
    r = dict(row)

    # --- Phase 1: Correct spurious is_own_report flags BEFORE label calculation ---
    # Google's API sometimes returns is_own_report=True for reports that are clearly
    # not from the owner's device. Detect and correct these cases:
    #
    # 1. accuracy <= 0 is physically impossible for real GPS (typical: 3-50m).
    #    This indicates missing/corrupted data, not a precise fix.
    # 2. No coordinates (lat/lon both None) means it's a semantic-only report
    #    which cannot be an "own device" GPS report.
    # 3. SEMANTIC status (status_code=0) is never an owner report by definition.
    if r.get("is_own_report"):
        has_invalid_accuracy = _is_invalid_accuracy(r.get("accuracy"))
        has_no_coordinates = r.get("latitude") is None and r.get("longitude") is None
        is_semantic_status = r.get("status_code") == 0

        if has_invalid_accuracy or has_no_coordinates or is_semantic_status:
            r["is_own_report"] = False

    # --- Phase 2: Calculate source label/rank based on corrected data ---
    rank, label = row_source_label(r)

    # Coerce invalid accuracy to None for cleaner UX
    if _is_invalid_accuracy(r.get("accuracy")):
        r["accuracy"] = None

    ts = normalize_epoch_seconds(r.get("last_seen"))
    r["last_seen"] = ts  # Store normalized float
    r["last_seen_utc"] = format_epoch_utc(ts)

    r["source_label"] = label
    r["source_rank"] = rank
    return r
