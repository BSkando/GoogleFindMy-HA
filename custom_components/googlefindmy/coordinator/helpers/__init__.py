"""Pure helper functions for GoogleFindMyCoordinator.

This package contains stateless utility functions extracted from coordinator.py
for improved testability and maintainability.

All public symbols are re-exported here for convenience.
"""

from __future__ import annotations

# cache.py - 13 functions + 7 constants
from .cache import (
    DEFAULT_SNAPSHOT_FIELDS,
    LOCATION_FIELDS,
    SOUND_UUID_MAX_AGE_S,
    SOURCE_PRIORITY,
    STATUS_AGING,
    STATUS_CURRENT,
    STATUS_STALE,
    STATUS_WAITING,
    build_base_snapshot_entry,
    compare_location_freshness,
    determine_location_status,
    epoch_to_datetime_utc,
    fill_missing_coordinates,
    is_presence_expired,
    is_sound_uuid_expired,
    merge_cache_row,
    normalize_location_fields,
    preserve_metadata_fields,
    preserve_monotonic_timestamp,
    select_best_location_source,
    should_allow_location_update,
    should_clear_metadata_only_flag,
)

# geo.py - 4 functions
from .geo import (
    clamp,
    coerce_float,
    haversine_distance,
    safe_accuracy,
)

# identity.py - 10 functions
from .identity import (
    extract_pair_date,
    extract_secrets_creation_date,
    extract_time_anchors_debug,
    extract_timestamp_from_keys,
    lookup_prio,
    lookup_prio_with_source,
    normalize_component_count,
    normalize_device_type,
    normalize_fast_pair_model_id,
    normalize_identity_timestamp,
    store_if_value,
)

# locate_clock.py - 2 functions
from .locate_clock import ensure_locate_stamps, stamp_on_dispatch

# polling.py - 3 functions
from .polling import (
    calculate_location_age_hours,
    get_age_log_level,
    should_preserve_previous_coordinates,
)

# registry.py - 16 functions + 2 constants
from .registry import (
    LEGACY_SERVICE_IDENTIFIER,
    SERVICE_DEVICE_IDENTIFIER_PREFIX,
    build_canonical_unique_id,
    build_entity_unique_id_candidates,
    build_legacy_device_registry_kwargs,
    extract_canonical_device_id,
    extract_device_display_name,
    extract_service_subentry_ids,
    extract_subentry_links,
    has_hub_link,
    has_subentry_link,
    is_hub_device_check,
    match_entity_by_device_id,
    needs_legacy_kwarg_retry,
    normalize_device_name,
    parse_device_identifier,
    resolve_tracker_subentry_candidate,
    should_defer_service_subentry,
)

# stats.py - 4 classes + 3 functions
from .stats import (
    ApiStatus,
    DiagnosticsBuffer,
    FcmStatus,
    StatusSnapshot,
    format_recent_errors,
    get_duration,
    short_error_message,
)

# subentry.py - 8 functions
from .subentry import (
    detect_missing_core_subentry_keys,
    extract_subentry_group_key,
    filter_provisional_identifier,
    format_epoch_utc,
    group_devices_by_subentry,
    normalize_epoch_seconds,
    parse_last_seen_timestamp,
    sanitize_subentry_identifier,
)

# update.py - 6 functions
from .update import (
    calculate_presence_ttl,
    filter_and_dedupe_devices,
    is_fatal_fcm_auth_error,
    is_poll_cycle_due,
    normalize_device_list_payload,
    should_defer_empty_list,
)

__all__ = [
    # registry.py
    "LEGACY_SERVICE_IDENTIFIER",
    "SERVICE_DEVICE_IDENTIFIER_PREFIX",
    "build_canonical_unique_id",
    "build_entity_unique_id_candidates",
    "build_legacy_device_registry_kwargs",
    "extract_canonical_device_id",
    "extract_device_display_name",
    "extract_service_subentry_ids",
    "extract_subentry_links",
    "has_hub_link",
    "has_subentry_link",
    "is_hub_device_check",
    "match_entity_by_device_id",
    "needs_legacy_kwarg_retry",
    "normalize_device_name",
    "parse_device_identifier",
    "resolve_tracker_subentry_candidate",
    "should_defer_service_subentry",
    # cache.py
    "DEFAULT_SNAPSHOT_FIELDS",
    "LOCATION_FIELDS",
    "SOUND_UUID_MAX_AGE_S",
    "SOURCE_PRIORITY",
    "STATUS_AGING",
    "STATUS_CURRENT",
    "STATUS_STALE",
    "STATUS_WAITING",
    "build_base_snapshot_entry",
    "compare_location_freshness",
    "determine_location_status",
    "epoch_to_datetime_utc",
    "fill_missing_coordinates",
    "is_presence_expired",
    "is_sound_uuid_expired",
    "merge_cache_row",
    "normalize_location_fields",
    "preserve_metadata_fields",
    "preserve_monotonic_timestamp",
    "select_best_location_source",
    "should_allow_location_update",
    "should_clear_metadata_only_flag",
    # identity.py
    "extract_pair_date",
    "extract_secrets_creation_date",
    "extract_time_anchors_debug",
    "extract_timestamp_from_keys",
    "lookup_prio",
    "lookup_prio_with_source",
    "normalize_component_count",
    "normalize_device_type",
    "normalize_fast_pair_model_id",
    "normalize_identity_timestamp",
    "store_if_value",
    # subentry.py
    "detect_missing_core_subentry_keys",
    "extract_subentry_group_key",
    "filter_provisional_identifier",
    "format_epoch_utc",
    "group_devices_by_subentry",
    "normalize_epoch_seconds",
    "parse_last_seen_timestamp",
    "sanitize_subentry_identifier",
    # update.py
    "calculate_presence_ttl",
    "filter_and_dedupe_devices",
    "is_fatal_fcm_auth_error",
    "is_poll_cycle_due",
    "normalize_device_list_payload",
    "should_defer_empty_list",
    # stats.py
    "ApiStatus",
    "DiagnosticsBuffer",
    "FcmStatus",
    "StatusSnapshot",
    "format_recent_errors",
    "get_duration",
    "short_error_message",
    # geo.py
    "clamp",
    "coerce_float",
    "haversine_distance",
    "safe_accuracy",
    # polling.py
    "calculate_location_age_hours",
    "get_age_log_level",
    "should_preserve_previous_coordinates",
    # locate_clock.py
    "ensure_locate_stamps",
    "stamp_on_dispatch",
]
