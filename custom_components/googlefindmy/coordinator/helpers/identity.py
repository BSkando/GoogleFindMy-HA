"""Identity utilities for the coordinator.

This module contains pure functions for device identity operations extracted from
coordinator.py for improved testability and maintainability (Phase 7).

Contents:
- normalize_device_type(): Normalize device type to int
- normalize_component_count(): Normalize a component count to a positive int
- normalize_fast_pair_model_id(): Normalize Fast Pair model ID
- lookup_prio(): Priority lookup across multiple sources
- lookup_prio_with_source(): Priority lookup with source label
- store_if_value(): Store non-null values to dict
- normalize_identity_timestamp(): Normalize timestamp with positive validation
- extract_timestamp_from_keys(): Extract timestamp from first matching key
- extract_pair_date(): Extract pair date from various payload shapes
- extract_secrets_creation_date(): Extract secrets creation date from payloads
- extract_time_anchors_debug(): Extract time anchor hints for diagnostics
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from .subentry import normalize_epoch_seconds

__all__ = [
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
]


# ---------------------------------------------------------------------------
# Type Variables
# ---------------------------------------------------------------------------

T = TypeVar("T")
U = TypeVar("U")


# ---------------------------------------------------------------------------
# Device Type Normalization
# ---------------------------------------------------------------------------


def normalize_device_type(value: Any) -> int | None:
    """Normalize device type to int.

    Handles bool (returns None), int (passthrough), and digit-only strings.
    Bools are explicitly rejected to prevent True/False being coerced to 1/0.

    Args:
        value: Device type as int, bool, string, or other type.

    Returns:
        Device type as int, or None if invalid/non-convertible.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def normalize_component_count(value: Any) -> int | None:
    """Return ``value`` if it is a positive ``int``, otherwise ``None``.

    Bools and strings are rejected: the count comes from a decoded protobuf
    field, so anything else in a row means the row was not built from one.
    """
    if type(value) is not int or value <= 0:
        return None
    return value


# ---------------------------------------------------------------------------
# Fast Pair Model ID Normalization
# ---------------------------------------------------------------------------


def normalize_fast_pair_model_id(value: Any) -> str | None:
    """Normalize Fast Pair model ID from string or bytes.

    For strings: strips whitespace, returns None if empty.
    For bytes/bytearray: attempts UTF-8 decode, falls back to hex encoding.

    Args:
        value: Model ID as string, bytes, bytearray, or other type.

    Returns:
        Normalized model ID string, or None if invalid/empty.
    """
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None

    if isinstance(value, (bytes, bytearray)):
        try:
            decoded = value.decode().strip()
        except UnicodeDecodeError:
            decoded = value.hex()
        return decoded or None

    return None


# ---------------------------------------------------------------------------
# Priority Lookup Functions
# ---------------------------------------------------------------------------


def lookup_prio(
    lookup_keys: tuple[str | None, ...],
    *sources: Mapping[str, T],
) -> T | None:
    """Return the first matching value across ordered sources.

    Iterates through sources in order, checking each lookup key within each
    source. Returns the first value found.

    Args:
        lookup_keys: Tuple of potential keys to look up. None values are filtered.
        *sources: Mapping objects to search in priority order.

    Returns:
        First matching value, or None if not found in any source.
    """
    key_candidates: tuple[str, ...] = tuple(
        key for key in lookup_keys if isinstance(key, str)
    )

    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in key_candidates:
            if key in source:
                return source[key]
    return None


def lookup_prio_with_source(
    lookup_keys: tuple[str | None, ...],
    *sources: tuple[Mapping[str, T], str],
) -> tuple[T | None, str | None]:
    """Return the first matching non-None value and its source label.

    Like lookup_prio, but each source is a (mapping, label) tuple.
    Only returns values that are not None.

    Args:
        lookup_keys: Tuple of potential keys to look up. None values are filtered.
        *sources: Tuples of (mapping, label) to search in priority order.

    Returns:
        Tuple of (value, source_label), or (None, None) if not found.
    """
    key_candidates: tuple[str, ...] = tuple(
        key for key in lookup_keys if isinstance(key, str)
    )

    for source, label in sources:
        if not isinstance(source, Mapping):
            continue
        for key in key_candidates:
            if key in source:
                value = source[key]
                if value is not None:
                    return value, label
    return None, None


# ---------------------------------------------------------------------------
# Value Storage Helper
# ---------------------------------------------------------------------------


def store_if_value(target: dict[str, U], key: str, value: U | None) -> None:
    """Store non-null lookup values to avoid shadowing fallbacks.

    Only stores the value if it is not None. This prevents None values
    from overwriting valid existing values in priority-based lookups.

    Args:
        target: Dictionary to store value in.
        key: Key under which to store the value.
        value: Value to store (only stored if not None).
    """
    if value is not None:
        target[key] = value


# ---------------------------------------------------------------------------
# Timestamp Normalization
# ---------------------------------------------------------------------------


def normalize_identity_timestamp(value: Any) -> int | None:
    """Return epoch seconds from ints/strings or Timestamp-like mappings.

    Values <= 0 are treated as invalid to prevent zero timestamps from
    shadowing valid values in priority lookups.

    Args:
        value: Timestamp as int, float, string, or Mapping with 'seconds' field.

    Returns:
        Epoch seconds as int, or None if invalid.
    """
    ts = normalize_epoch_seconds(value)
    if ts is not None and ts > 0:
        return ts

    if isinstance(value, Mapping):
        seconds = normalize_epoch_seconds(value.get("seconds"))
        if seconds is not None and seconds > 0:
            return seconds
        # If mapping has nanos/nsec but no valid seconds, return None
        if "nanos" in value or "nsec" in value:
            return None

    return None


# ---------------------------------------------------------------------------
# Timestamp Extraction from Keys
# ---------------------------------------------------------------------------


def extract_timestamp_from_keys(
    payload: Mapping[str, Any] | None, *keys: str
) -> int | None:
    """Extract timestamp from first matching key in payload.

    Iterates through keys in order, returning the first valid timestamp found.

    Args:
        payload: Mapping containing potential timestamp fields.
        *keys: Key names to check in order.

    Returns:
        Epoch seconds as int, or None if no valid timestamp found.
    """
    if not isinstance(payload, Mapping):
        return None

    for key in keys:
        if key in payload:
            ts = normalize_identity_timestamp(payload.get(key))
            if ts is not None:
                return ts
    return None


# ---------------------------------------------------------------------------
# Pair Date Extraction
# ---------------------------------------------------------------------------


def extract_pair_date(payload: Mapping[str, Any] | None) -> int | None:
    """Return best-effort pairDate from various payload shapes.

    Checks direct keys first, then nested deviceRegistration and information
    structures.

    Direct keys checked (in order):
    - pair_date
    - pairDate
    - pair_date_unix
    - pairingDate
    - pairedAt
    - paired_at

    Args:
        payload: Device payload mapping.

    Returns:
        Pair date as epoch seconds, or None if not found.
    """
    if not isinstance(payload, Mapping):
        return None

    # Check direct keys first
    direct = extract_timestamp_from_keys(
        payload,
        "pair_date",
        "pairDate",
        "pair_date_unix",
        "pairingDate",
        "pairedAt",
        "paired_at",
    )
    if direct is not None:
        return direct

    # Check nested deviceRegistration
    registration = payload.get("deviceRegistration") or payload.get(
        "device_registration"
    )
    if isinstance(registration, Mapping):
        nested = extract_pair_date(registration)
        if nested is not None:
            return nested

    # Check nested information
    information = payload.get("information")
    if isinstance(information, Mapping):
        nested = extract_pair_date(information)
        if nested is not None:
            return nested

    return None


# ---------------------------------------------------------------------------
# Secrets Creation Date Extraction
# ---------------------------------------------------------------------------


def extract_secrets_creation_date(
    payload: Mapping[str, Any] | None,
) -> int | None:
    """Return best-effort creation time for encrypted secrets bundles.

    Checks direct keys first, then nested encrypted_user_secrets and
    deviceRegistration structures.

    Direct keys checked (in order):
    - secrets_creation_date
    - secretsCreationDate
    - creation_date
    - creationDate

    Args:
        payload: Device payload mapping.

    Returns:
        Secrets creation date as epoch seconds, or None if not found.
    """
    if not isinstance(payload, Mapping):
        return None

    # Check direct keys first
    direct = extract_timestamp_from_keys(
        payload,
        "secrets_creation_date",
        "secretsCreationDate",
        "creation_date",
        "creationDate",
    )
    if direct is not None:
        return direct

    # Check nested encrypted_user_secrets
    encrypted = payload.get("encrypted_user_secrets") or payload.get(
        "encryptedUserSecrets"
    )
    if isinstance(encrypted, Mapping):
        encrypted_ts = extract_timestamp_from_keys(
            encrypted, "creation_date", "creationDate"
        )
        if encrypted_ts is not None:
            return encrypted_ts

    # Check nested deviceRegistration
    registration = payload.get("deviceRegistration") or payload.get(
        "device_registration"
    )
    if isinstance(registration, Mapping):
        nested = extract_secrets_creation_date(registration)
        if nested is not None:
            return nested

    return None


# ---------------------------------------------------------------------------
# Time Anchors Debug Extraction
# ---------------------------------------------------------------------------


def extract_time_anchors_debug(
    payload: Mapping[str, Any] | None,
) -> Any | None:
    """Return optional time anchor hints for diagnostics.

    Checks multiple key variants and returns the first found value.

    Keys checked (in order):
    - time_anchors_debug
    - timeAnchorsDebug
    - time_anchors
    - timeAnchors

    Args:
        payload: Device payload mapping.

    Returns:
        Time anchors debug data (any type), or None if not found.
    """
    if not isinstance(payload, Mapping):
        return None

    for key in (
        "time_anchors_debug",
        "timeAnchorsDebug",
        "time_anchors",
        "timeAnchors",
    ):
        if key in payload:
            return payload.get(key)

    return None
