# custom_components/googlefindmy/diagnostics.py
"""Diagnostics helpers for the Google Find My Device integration.

Design goals (HA quality scale / Platinum-ready):
- Never leak secrets or personal data (tokens, emails, device IDs, coordinates, names).
- Provide enough structured, anonymized context to debug typical issues (polling, counts, timings).
- Prefer runtime_data (modern pattern) but gracefully fall back to hass.data for older setups.
- Keep redaction centralized and defensive (include common token/email keys even if we don't expose them now).

Privacy note:
- POPETS’25 (Böttger et al., 2025) highlights that EID-related artifacts and UT bits can be used
  for correlation/identification. We therefore **over-redact** such fields, even if we never place
  them into diagnostics directly. This is a defense-in-depth safeguard to keep future changes safe.

Additional privacy hardening (message bodies):
- Coordinator "recent errors" may contain a human-readable "where(...)" prefix that can embed device
  names. We therefore strip any parenthesized content from the prefix and avoid returning the free-form
  message body entirely. Only a coarse "where" tag, error type, and timestamp are exposed.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.loader import async_get_integration

from .const import (
    CONF_GOOGLE_EMAIL,
    # secrets in entry.data (must never be exposed)
    CONF_OAUTH_TOKEN,
    DATA_EID_RESOLVER,
    DATA_SECRET_BUNDLE,
    # defaults for options (used to avoid hard-coded literals)
    DEFAULT_DEVICE_POLL_DELAY,
    DEFAULT_ENABLE_STATS_ENTITIES,
    DEFAULT_GOOGLE_HOME_FILTER_ENABLED,
    DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS,
    DEFAULT_LOCATION_POLL_INTERVAL,
    DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
    DEFAULT_MIN_POLL_INTERVAL,
    DOMAIN,
    OPT_DEVICE_POLL_DELAY,
    OPT_ENABLE_STATS_ENTITIES,
    OPT_GOOGLE_HOME_FILTER_ENABLED,
    OPT_GOOGLE_HOME_FILTER_KEYWORDS,
    OPT_IGNORED_DEVICES,
    # user-facing options (non-secret)
    OPT_LOCATION_POLL_INTERVAL,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    OPT_MIN_POLL_INTERVAL,
)
from .redaction import REDACTED, async_redact_data
from .shared_helpers import normalize_fcm_entry_snapshot, safe_fcm_health_snapshots

_LOGGER = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .coordinator import GoogleFindMyCoordinator  # noqa: F401

# ---------------------------------------------------------------------------
# Redaction policy
# ---------------------------------------------------------------------------
# Keys to redact anywhere they appear in the diagnostics payload.
# Keep this list generous; it is safe to over-redact (defense-in-depth).
TO_REDACT: list[str] = [
    # Known integration secrets (entry.data)
    CONF_OAUTH_TOKEN,
    CONF_GOOGLE_EMAIL,
    # The whole credential bundle, and the two keys that decrypt locations.
    # entry.data carries the bundle between the config flow and the migration in
    # async_setup_entry; if setup fails in between, it is still there when a user
    # downloads diagnostics for that very failure.
    DATA_SECRET_BUNDLE,
    "scanned_data",
    "shared_key",
    "owner_key",
    # FCM credential material carried inside the bundle. These are fixed key
    # names, so exact matching is enough; the surrounding fcm_* diagnostics
    # (status, receiver state, counters) are deliberately NOT redacted.
    "fcm_credentials",
    "fcm_creds",
    "fcm_installation",
    "fcm_registration",
    # Common token/email/credential shapes
    "aas_token",
    "access_token",
    "refresh_token",
    "token",
    "security_token",
    "authorization",
    "cookie",
    "set-cookie",
    "app_id",
    "android_id",
    "fid",
    "email",
    "username",
    "user",
    "Auth",
    "secret",
    "private",
    "public",
    "p256dh",
    "auth",
    "endpoint",
    # Identity resolving / E2EE related (never expose!)
    "irk",
    "irk_hex",
    "identity_resolving_key",
    "identity_resolving_keys",
    "encrypted_identity_resolving_key",
    "encrypted_identity_resolving_keys",
    "identityResolvingKey",
    "identityResolvingKeys",
    "encryptedIdentityResolvingKey",
    "encryptedIdentityResolvingKeys",
    "eik",
    "eik_hex",
    "identity_key",
    "identity_keys",
    "encrypted_identity_key",
    "encrypted_identity_keys",
    "identityKey",
    "identityKeys",
    "encryptedIdentityKey",
    "encryptedIdentityKeys",
    "ownerKey",
    "ownerKeyVersion",
    # EID / UT artifacts (see POPETS’25; redact to avoid correlation)
    "eid",
    "eid_prefix",
    "eidPrefix",
    "truncated_eid",
    "truncatedEid",
    "ut",
    "ut_bits",
    "utBits",
    # Device identifiers (avoid leaking stable IDs)
    "device_id",
    "deviceId",
    "canonical_id",
    "canonicalId",
    "canonic_id",
    "canonicId",
    "canonicIds",
    # Location-related fields (we do not include them, but redact defensively)
    "latitude",
    "longitude",
    "altitude",
    # The accuracy gate's coarse fix (#216) lives on the entity, never in this
    # dump. Listed for the same defensive reason as the three above: no path
    # writes it here today, and the net is spanned for tomorrow.
    "coarse_latitude",
    "coarse_longitude",
]

# Key names the token cache builds at run time, so they can never appear in a
# fixed list: `adm_token_<e-mail>`, `android_id_<e-mail>` and friends. Exact
# matching alone would pass every one of them through.
TO_REDACT_PREFIXES: tuple[str, ...] = (
    "aas_token",
    "adm_token",
    "spot_token",
    "android_id",
    "owner_key",
    "shared_key",
    "oauth_token",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _monotonic_to_wall_seconds(last_mono: float | None) -> float | None:
    """Convert a stored monotonic timestamp to wall-clock seconds since epoch (UTC).

    We infer the wall time using the current monotonic delta; this is best-effort
    and intentionally avoids reading any precise location timestamps from entities.
    """
    if not isinstance(last_mono, (int, float)) or last_mono <= 0:
        return None
    now_wall = time.time()
    now_mono = time.monotonic()
    # Clamp at 0 to avoid negative values when clocks drift
    return max(0.0, now_wall - (now_mono - float(last_mono)))


def _ha_core_version() -> str | None:
    """Return the Home Assistant core version string, or None if unavailable.

    Read lazily from ``homeassistant.const.__version__`` (the stable public
    API; ``hass.config.version`` is not). The import is guarded so stripped or
    stubbed environments that omit ``__version__`` degrade to ``None`` instead
    of breaking diagnostics collection.
    """
    try:
        from homeassistant.const import __version__ as ha_version

        return str(ha_version)
    except Exception:
        return None


def _count_keywords(value: Any) -> int:
    """Count comma-separated keywords without exposing their content."""
    if not value:
        return 0
    try:
        parts = [p.strip() for p in str(value).split(",")]
        return sum(1 for p in parts if p)
    except Exception:
        return 0


def _coerce_pos_int(value: Any, default: int) -> int:
    """Best-effort positive-int coercion for options (defensive)."""
    try:
        v = int(value)
        return v if v >= 0 else default
    except Exception:
        return default


def _iso_utc(ts: float | None) -> str | None:
    """Render epoch seconds as ISO 8601 UTC string, or None."""
    if not isinstance(ts, (int, float)) or ts <= 0:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=UTC).isoformat()
    except Exception:
        return None


def _safe_truncate(text: Any, limit: int = 160) -> str:
    """Return a short, non-sensitive representation of a value."""
    try:
        s = str(text)
    except Exception:
        return ""
    if len(s) <= limit:
        return s
    return s[: max(0, limit - 1)] + "…"


def _status_snapshot_to_dict(snapshot: Any) -> dict[str, Any] | None:
    """Serialize a StatusSnapshot dataclass (state/reason/changed_at)."""

    if snapshot is None:
        return None

    try:
        if is_dataclass(snapshot) and not isinstance(snapshot, type):
            data = asdict(snapshot)
        else:
            data = {
                "state": getattr(snapshot, "state", None),
                "reason": getattr(snapshot, "reason", None),
                "changed_at": getattr(snapshot, "changed_at", None),
            }
    except Exception:
        return None

    changed_at = data.get("changed_at")
    if not isinstance(changed_at, (int, float)):
        data["changed_at"] = None

    state = data.get("state")
    if state is not None and not isinstance(state, (str, int, float, bool)):
        data["state"] = str(state)

    reason = data.get("reason")
    if reason is not None and not isinstance(reason, str):
        data["reason"] = str(reason)

    return {
        "state": data.get("state"),
        "reason": data.get("reason"),
        "changed_at": data.get("changed_at"),
    }


def _reauth_reason_block(coordinator: Any) -> dict[str, Any] | None:
    """Serialize the coordinator's structured reauth reason for diagnostics.

    Mirrors :func:`_status_snapshot_to_dict`: returns ``None`` when no reauth has
    been recorded yet so the coordinator block stays structurally unchanged.

    Redaction is guaranteed by the *literal-only input* invariant of
    :class:`~._reauth_reason.ReauthReason` (see AE-7): ``code`` is a
    ``StrEnum`` value, ``origin`` is a constant literal string, and ``counters``
    are plain ints. No token, e-mail, or free-form ``str(err)`` content is ever
    stored on the reason, so nothing sensitive can reach this payload.
    """
    reason = getattr(coordinator, "_reauth_reason", None)
    if reason is None:
        return None

    try:
        code = getattr(reason, "code", None)
        origin = getattr(reason, "origin", None)
        raw_counters = getattr(reason, "counters", None)
        recorded_at = getattr(reason, "recorded_at", None)
    except Exception:
        return None

    counters: dict[str, int] = {}
    if isinstance(raw_counters, Mapping):
        for key, value in raw_counters.items():
            if (
                isinstance(key, str)
                and isinstance(value, int)
                and not isinstance(value, bool)
            ):
                counters[key] = value

    return {
        "code": str(code) if code is not None else None,
        "origin": _safe_truncate(origin) if origin is not None else None,
        "counters": counters,
        "recorded_at": _iso_utc(
            recorded_at if isinstance(recorded_at, (int, float)) else None
        ),
    }


def _sanitize_diag_entry(payload: Any) -> dict[str, Any]:
    """Return a diagnostics-friendly snapshot of a buffer entry."""
    if not isinstance(payload, dict):
        return {}

    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if not isinstance(key, str):
            # Skip unknown key types to avoid leaking repr() content.
            continue

        lowered_key = key.casefold()
        # Drop any name/title/label-like keys to avoid leaking human readable names.
        if "name" in lowered_key or "title" in lowered_key or "label" in lowered_key:
            continue

        if isinstance(value, (int, float, bool)) or value is None:
            sanitized[key] = value
        else:
            sanitized[key] = _safe_truncate(value)
    return sanitized


def _diagnostics_buffer_summary(raw: Any) -> dict[str, Any]:
    """Sanitize a diagnostics buffer payload for coordinator diagnostics."""
    if not isinstance(raw, dict):
        return {}

    summary: dict[str, Any] = {}

    raw_summary = raw.get("summary")
    if isinstance(raw_summary, dict):
        sanitized_summary: dict[str, Any] = {}
        for key, value in raw_summary.items():
            if isinstance(value, (int, float)):
                sanitized_summary[key] = value
            else:
                sanitized_summary[key] = _safe_truncate(value, 48)
        if sanitized_summary:
            summary["summary"] = sanitized_summary

    raw_warnings = raw.get("warnings")
    if isinstance(raw_warnings, list) and raw_warnings:
        summary["warnings_preview"] = [
            _sanitize_diag_entry(item) for item in raw_warnings[:5]
        ]

    raw_errors = raw.get("errors")
    if isinstance(raw_errors, list) and raw_errors:
        summary["errors_preview"] = [
            _sanitize_diag_entry(item) for item in raw_errors[:5]
        ]

    return summary


def _perf_durations(perf: dict[str, Any]) -> dict[str, Any]:
    """Compute stable setup durations (seconds) from monotonic stamps if present."""
    try:
        start = float(perf.get("setup_start_monotonic", 0) or 0)
        end = float(perf.get("setup_end_monotonic", 0) or 0)
        fcm = float(perf.get("fcm_acquired_monotonic", 0) or 0)
    except Exception:
        return {}

    out: dict[str, Any] = {}
    if start > 0 and end > 0 and end >= start:
        out["total_setup_duration_seconds"] = round(end - start, 3)
    if start > 0 and fcm >= start:
        out["fcm_acquisition_duration_seconds"] = round(fcm - start, 3)
    return out


def _concurrency_block(hass: HomeAssistant) -> dict[str, int]:
    """Return contention counters collected during setup/runtime."""
    bucket = hass.data.get(DOMAIN, {}) or {}
    return {
        "fcm_lock_contention_count": int(
            bucket.get("fcm_lock_contention_count", 0) or 0
        ),
        "services_lock_contention_count": int(
            bucket.get("services_lock_contention_count", 0) or 0
        ),
    }


def _crypto_block(info: dict[str, str | None]) -> dict[str, str | None]:
    """Return a shallow copy of the ECDSA acceleration info for diagnostics.

    Takes the already-materialized info dict (see
    ``get_ecdsa_acceleration_info``) and copies it so the cached SSOT dict is
    never mutated. No I/O and no defensive try/except here: the SSOT already
    handles ``PackageNotFoundError`` internally, so there is no reachable
    error branch to swallow (matches ``_concurrency_block`` style).
    """
    return dict(info)


def _fcm_receiver_state(hass: HomeAssistant) -> dict[str, Any] | None:
    """Summarize FCM receiver runtime health without leaking internals."""
    bucket = hass.data.get(DOMAIN, {}) or {}
    rcvr = bucket.get("fcm_receiver")
    if not rcvr:
        return None

    snapshots = safe_fcm_health_snapshots(rcvr)

    entries = []
    connected_entries: list[str] = []
    for entry_id, snap in snapshots.items():
        if snap.get("healthy"):
            connected_entries.append(entry_id)

        # Start with the shared base fields and extend with diagnostics-specific ones
        entry_data = normalize_fcm_entry_snapshot(entry_id, snap)
        entry_data.update(
            {
                "supervisor_running": bool(snap.get("supervisor_running")),
                "client_ready": bool(snap.get("client_ready")),
                "do_listen": bool(snap.get("do_listen")),
                "last_activity_monotonic": snap.get("last_activity_monotonic"),
            }
        )
        entries.append(entry_data)

    def _get(attr: str, default: Any = None) -> Any:
        try:
            return getattr(rcvr, attr, default)
        except Exception:
            return default

    last_start = _get("last_start_monotonic", 0.0)
    seconds_since_last_start = None
    try:
        if isinstance(last_start, (int, float)) and last_start > 0:
            seconds_since_last_start = round(time.monotonic() - float(last_start), 2)
    except Exception:
        seconds_since_last_start = None

    return {
        "connected_entries": sorted(connected_entries),
        "entries": entries,
        "ref_count": int(bucket.get("fcm_refcount", 0) or 0),
        "start_count": int(_get("start_count", 0) or 0),
        "seconds_since_last_start": seconds_since_last_start,
        "activity_stale_after_seconds": _get("_activity_stale_after_s"),
    }


def _recent_errors_block(coordinator: Any) -> list[dict[str, Any]] | None:
    """Convert coordinator.recent_errors (deque) to a redacted list.

    Original intent:
        Return a bounded list of recent non-fatal errors for diagnostics.

    Correction / privacy hardening:
        Messages may include a 'where(...)' prefix that could embed device display names.
        We now extract only the coarse 'where' label (text before the first ':') and
        replace any parenthesized content with a generic placeholder '(*)'. The free-form
        message body is **not** included to avoid PII leakage.
    """
    try:
        recent = getattr(coordinator, "recent_errors", None)
    except Exception:
        recent = None
    if not recent:
        return None

    items: list[dict[str, Any]] = []
    # recent is expected to be a deque of (ts, type, msg)
    for row in list(recent):
        try:
            ts, etype, msg = row
        except Exception:
            # Be defensive with unknown tuple shapes
            ts, etype, msg = (None, None, None)

        # Extract a safe "where" tag (prefix up to ':') and scrub parentheses content.
        where = None
        try:
            prefix = str(msg or "").split(":", 1)[0].strip()
            where = re.sub(r"\([^)]*\)", "(*)", prefix)
        except Exception:
            where = None

        items.append(
            {
                "timestamp": _iso_utc(ts),
                "error_type": _safe_truncate(etype, 64),
                "where": _safe_truncate(where, 64),
            }
        )
    return items or None


# ---------------------------------------------------------------------------
# Diagnostics entrypoint
# ---------------------------------------------------------------------------


_UWT_EVENT_FIELDS = (
    "kind",
    "old",
    "new",
    "source",
    "flags",
    "frame",
    "payload_len",
    "poll_age_s",
    "prev_seen_age_s",
    "observed",
    "seq",
)


def _component_flags_block(hass: HomeAssistant, entry_id: str) -> list[dict[str, Any]]:
    """Return the last hashed flags per FMDN component series of an entry.

    Raw values without interpretation: what a series stands for and what its
    flags mean is not known (``docs/FMDN.md`` S3.4.1 and S11). Devices appear by
    position in the sorted set of this entry, without any id. Without a resolver,
    or if reading it fails, the block is empty.
    """
    try:
        bucket = hass.data.get(DOMAIN) or {}
        resolver = bucket.get(DATA_EID_RESOLVER) if isinstance(bucket, dict) else None
        reader = getattr(resolver, "component_flags_snapshot", None)
        if not callable(reader):
            return []
        snapshot = reader(entry_id)
        return [
            {
                "device_index": index,
                "component": component,
                "decoded": reading["decoded"],
                "frame": reading["frame"],
                "observed_at": reading["observed_at_wall"],
                "observations": reading["observations"],
            }
            for index, canonical_id in enumerate(sorted(snapshot))
            for component, reading in sorted(snapshot[canonical_id].items())
        ]
    except Exception as err:  # noqa: BLE001 - diagnostics must not fail on this block
        # Only the type, as in _uwt_transitions_block.
        _LOGGER.debug("component_flags_block_failed error=%s", type(err).__name__)
        return []


def _uwt_transitions_block(hass: HomeAssistant, entry_id: str) -> dict[str, Any]:
    """Return the protection-mode changes the EID resolver recorded for an entry.

    Each event names its device by ``device_index``, the position of the
    device's canonical id in the sorted set of devices in this entry's
    events; the canonical id itself is left out. Only events of ``entry_id``
    are included. Without a resolver, or if reading it fails, the block is
    empty.
    """
    empty: dict[str, Any] = {
        "capacity": None,
        "dropped": 0,
        "info_lines_per_device_hour": None,
        "events": [],
    }
    try:
        bucket = hass.data.get(DOMAIN) or {}
        resolver = bucket.get(DATA_EID_RESOLVER) if isinstance(bucket, dict) else None
        reader = getattr(resolver, "uwt_transition_snapshot", None)
        if not callable(reader):
            return empty
        snapshot = reader(entry_id)
        order = {
            canonical_id: index
            for index, canonical_id in enumerate(
                sorted({event.canonical_id for event in snapshot.events})
            )
        }
        return {
            "capacity": snapshot.capacity,
            "dropped": snapshot.dropped,
            "info_lines_per_device_hour": snapshot.info_lines_per_device_hour,
            "events": [
                {
                    "device_index": order[event.canonical_id],
                    **{name: getattr(event, name) for name in _UWT_EVENT_FIELDS},
                }
                for event in snapshot.events
            ],
        }
    except Exception as err:  # noqa: BLE001 - diagnostics must not fail on this block
        # Only the type: the message of an error raised while reading the
        # resolver could carry an identifier (AGENTS.md section 5).
        _LOGGER.debug("uwt_transitions_block_failed error=%s", type(err).__name__)
        return empty


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return anonymized diagnostics for a config entry.

    Best practices:
    - Do NOT include: coordinates, device IDs, device names, emails, tokens,
      unique_id, or any raw content from external services.
    - DO include: anonymized counters, booleans, timings, and versions.

    POPETS’25 context (documentation only):
    - Server-side throttling and purging behaviors inform our coordinator logic,
      but diagnostics remain strictly anonymized and redacted to avoid leakage of
      EID/UT artifacts or stable identifiers.
    """
    # --- Integration metadata (manifest) ---
    integration_meta: dict[str, Any] = {}
    try:
        integ = await async_get_integration(hass, DOMAIN)
        # Name and version from manifest; both are safe to expose
        integration_meta = {
            "name": integ.name,
            "version": str(integ.version),
            # Home Assistant core version: lets a maintainer correlate
            # core-specific regressions.
            "ha_core_version": _ha_core_version(),
        }
    except Exception:
        # Stay resilient if loader fails in custom environments. Still expose
        # the core version (best-effort, may be None).
        integration_meta = {"ha_core_version": _ha_core_version()}

    # --- Coordinator / runtime_data (typed container) ---
    coordinator: Any | None = None
    runtime = getattr(entry, "runtime_data", None)
    if runtime is not None:
        candidate = getattr(runtime, "coordinator", runtime)
        if candidate is not None:
            coordinator = candidate

    # --- Build a compact, anonymized options snapshot (no raw strings that could contain PII) ---
    try:
        effective_config: dict[str, Any] = dict(entry.data)
    except Exception:
        effective_config = {}

    if isinstance(entry.options, Mapping):
        effective_config.update(entry.options)
    else:  # pragma: no cover - defensive fallback
        try:
            effective_config.update(dict(entry.options))
        except Exception:
            effective_config = dict(effective_config)

    keywords_count = _count_keywords(
        effective_config.get(
            OPT_GOOGLE_HOME_FILTER_KEYWORDS, DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS
        )
    )

    ignored_raw = effective_config.get(OPT_IGNORED_DEVICES) or {}

    effective_config_for_diag = dict(effective_config)
    effective_config_for_diag.pop("min_accuracy_threshold", None)

    # Coerce to handle legacy list[str] format gracefully
    if isinstance(ignored_raw, list):
        ignored_count = len(ignored_raw)
    elif isinstance(ignored_raw, dict):
        ignored_count = len(ignored_raw)
    else:
        ignored_count = 0

    if ignored_count:
        effective_config_for_diag[OPT_IGNORED_DEVICES] = [REDACTED] * ignored_count
    elif OPT_IGNORED_DEVICES in effective_config_for_diag:
        effective_config_for_diag[OPT_IGNORED_DEVICES] = []

    if keywords_count:
        effective_config_for_diag[OPT_GOOGLE_HOME_FILTER_KEYWORDS] = [
            REDACTED
        ] * keywords_count
    elif OPT_GOOGLE_HOME_FILTER_KEYWORDS in effective_config_for_diag:
        effective_config_for_diag[OPT_GOOGLE_HOME_FILTER_KEYWORDS] = []

    # One numbering for both redaction passes below, so that `<account-1>` in
    # `effective_config` and in the rest of the payload is the same account.
    accounts: dict[str, str] = {}
    redacted_effective_config = async_redact_data(
        effective_config_for_diag, TO_REDACT, TO_REDACT_PREFIXES, accounts=accounts
    )

    config_summary = {
        # Durations and numeric thresholds
        "location_poll_interval": _coerce_pos_int(
            effective_config.get(
                OPT_LOCATION_POLL_INTERVAL, DEFAULT_LOCATION_POLL_INTERVAL
            ),
            DEFAULT_LOCATION_POLL_INTERVAL,
        ),
        "min_poll_interval": _coerce_pos_int(
            effective_config.get(OPT_MIN_POLL_INTERVAL, DEFAULT_MIN_POLL_INTERVAL),
            DEFAULT_MIN_POLL_INTERVAL,
        ),
        "device_poll_delay": _coerce_pos_int(
            effective_config.get(OPT_DEVICE_POLL_DELAY, DEFAULT_DEVICE_POLL_DELAY),
            DEFAULT_DEVICE_POLL_DELAY,
        ),
        # Feature toggles
        "google_home_filter_enabled": bool(
            effective_config.get(
                OPT_GOOGLE_HOME_FILTER_ENABLED, DEFAULT_GOOGLE_HOME_FILTER_ENABLED
            )
        ),
        "enable_stats_entities": bool(
            effective_config.get(
                OPT_ENABLE_STATS_ENTITIES, DEFAULT_ENABLE_STATS_ENTITIES
            )
        ),
        # Token lifetime: store boolean value
        "map_view_token_expiration": bool(
            effective_config.get(
                OPT_MAP_VIEW_TOKEN_EXPIRATION, DEFAULT_MAP_VIEW_TOKEN_EXPIRATION
            )
        ),
        # Counts only (never expose strings/IDs)
        "google_home_filter_keywords_count": keywords_count,
        "ignored_devices_count": ignored_count,
    }

    # --- Device & entity registry counts (anonymized) ---
    device_registry_counts: dict[str, Any] = {}
    try:
        dev_reg = dr.async_get(hass)
        # Ask the registry for this entry's devices instead of walking the
        # registry-wide mapping and filtering by ownership.  Two deprecated
        # reads go at once, and only one of them is reported by Core.  What
        # reports is *using* the registry-wide container as a mapping -- from
        # 2026.9 the ``devices`` attribute hands out a view whose container
        # methods each raise a ``report_usage``; touching the attribute alone
        # does not, and neither does iterating it.  On 2026.9 the set-shaped
        # ``DeviceEntry.config_entries`` is a plain property and reports
        # nothing at all; from 2026.10 it reports too (``breaks_in_ha_version
        # ="2027.10.0"``).  Measured on 2026.9.1, reports per expression: the
        # attribute 0, a container method on it 1, the removed expression 1,
        # the line below 0, ``DeviceEntry.config_entries`` 0.  The ownership
        # read goes with it because from 2026.8 a device belongs to exactly one
        # config entry, which makes the set a compatibility shim whose single
        # element is derived from ``config_entry_id``.
        #
        # The module-level helper is the supported way to ask.  Its signature
        # and result are the same on every supported core (2025.9.1, 2026.8.2,
        # 2026.9.1); its body is not, and that is the point: from 2026.9 it
        # reaches the item store through the private name, which is why asking
        # this way does not itself trip the reporter.
        devices_for_entry = dr.async_entries_for_config_entry(dev_reg, entry.entry_id)
        device_registry_counts["devices_count"] = len(devices_for_entry)
    except Exception:
        device_registry_counts["devices_count"] = None

    entity_registry_counts: dict[str, Any] = {}
    try:
        ent_reg = er.async_get(hass)
        entities_for_entry = [
            e for e in ent_reg.entities.values() if e.config_entry_id == entry.entry_id
        ]
        entity_registry_counts["entities_count"] = len(entities_for_entry)
    except Exception:
        entity_registry_counts["entities_count"] = None

    # --- Coordinator-derived info (all anonymized/counted) ---
    coordinator_block: dict[str, Any] = {}
    if coordinator is not None:
        # Boolean flags and counters only; never expose maps with device IDs/names
        try:
            known_devices_count = len(getattr(coordinator, "_device_names", {}) or {})
        except (AttributeError, TypeError):
            known_devices_count = None

        try:
            cache_items_count = len(
                getattr(coordinator, "_device_location_data", {}) or {}
            )
        except (AttributeError, TypeError):
            cache_items_count = None

        try:
            last_poll_wall = _monotonic_to_wall_seconds(
                getattr(coordinator, "_last_poll_mono", None)
            )
        except (AttributeError, TypeError):
            last_poll_wall = None

        try:
            stats = dict(getattr(coordinator, "stats", {}) or {})
            # Stats should already be anonymized counters; still ensure only numbers
            for k, v in list(stats.items()):
                if not isinstance(v, (int, float)):
                    stats[k] = None
        except (AttributeError, TypeError):
            stats = {}

        # Performance metrics (optional; only durations)
        perf_metrics = getattr(coordinator, "performance_metrics", {}) or {}
        setup_perf = _perf_durations(perf_metrics)

        # Recent, strictly redacted non-fatal errors (bounded)
        recent_errors = _recent_errors_block(coordinator)

        # Optional anonymous counters: enabled poll targets & present devices as seen last
        try:
            enabled_poll_targets_count = len(
                getattr(coordinator, "_enabled_poll_device_ids", set()) or set()
            )
        except (AttributeError, TypeError):
            enabled_poll_targets_count = None
        try:
            present_devices_seen_count = len(
                getattr(coordinator, "_present_device_ids", set()) or set()
            )
        except (AttributeError, TypeError):
            present_devices_seen_count = None

        coordinator_block = {
            "known_devices_count": known_devices_count,
            "cache_items_count": cache_items_count,
            "last_poll_wall_ts": last_poll_wall,  # seconds since epoch (UTC)
            "stats": stats,
            "enabled_poll_targets_count": enabled_poll_targets_count,
            "present_devices_seen_count": present_devices_seen_count,
            "api_status": _status_snapshot_to_dict(
                getattr(coordinator, "api_status", None)
            ),
            "fcm_status": _status_snapshot_to_dict(
                getattr(coordinator, "fcm_status", None)
            ),
        }
        if setup_perf:
            coordinator_block["setup_performance"] = setup_perf
        if recent_errors:
            coordinator_block["recent_errors"] = recent_errors

        # FIX 3: structured, redaction-safe reauth reason (only when recorded).
        reauth_reason_block = _reauth_reason_block(coordinator)
        if reauth_reason_block is not None:
            coordinator_block["reauth_reason"] = reauth_reason_block

        # Anonymized per-device telemetry (P1-3): an opaque index plus coarse
        # fields only, no names/IDs/coordinates/keys. The exact field set is
        # owned by ``build_per_device_diagnostics`` and pinned by
        # ``tests/test_diagnostics_p1_per_device.py``; deliberately not
        # restated as a number here, because that number rots.
        # Resilient: [] on any failure.
        try:
            builder = getattr(coordinator, "build_per_device_diagnostics", None)
            coordinator_block["devices"] = builder() if callable(builder) else []
        except Exception:
            coordinator_block["devices"] = []

        diag_buffer = getattr(coordinator, "_diag", None)
        if diag_buffer is not None and hasattr(diag_buffer, "to_dict"):
            try:
                raw_diag = diag_buffer.to_dict()
            except Exception:
                raw_diag = None
            sanitized_diag = _diagnostics_buffer_summary(raw_diag)
            if sanitized_diag:
                coordinator_block["diagnostics_buffer"] = sanitized_diag

    # Concurrency & FCM receiver (global, not per-entry)
    concurrency = _concurrency_block(hass)
    fcm_state = _fcm_receiver_state(hass)

    # ECDSA big-int backend + installed accelerator versions (global), cached by
    # async_setup in hass.data. Read synchronously here (no I/O), consistent with
    # every other diagnostics block; present once async_setup has materialized it.
    crypto_info = (hass.data.get(DOMAIN, {}) or {}).get("ecdsa_acceleration_info")

    # EIK cache statistics (performance optimization metrics)
    # Import lazily to avoid circular import (diagnostics <- decrypt_locations <- __init__ <- diagnostics)
    from .NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
        get_eik_cache_stats,
    )

    eik_cache_stats = get_eik_cache_stats()

    # Provisional P-256 readings of crowdsourced reports, per device of this
    # entry. The block carries no ID value (positional index only) and is meant
    # to be posted to the feedback issue on its own.
    # Imported locally like the EIK block above, for symmetry only: unlike
    # decrypt_locations, the tracker module has no import cycle with this one.
    from .NovaApi.ExecuteAction.LocateTracker.foreign_reading_tracker import (
        get_foreign_reading_diagnostics,
    )

    foreign_report_readings = get_foreign_reading_diagnostics(entry.entry_id)

    # Recorded protection-mode changes of this entry (FMDN_UWT_TRANSITION).
    # Like the block above it carries no ID value, only a positional index,
    # and it can be posted to an issue on its own.
    uwt_transitions = _uwt_transitions_block(hass, entry.entry_id)

    # --- Assemble payload (without secrets) ---
    payload: dict[str, Any] = {
        "integration": integration_meta,
        "entry": {
            # Safe metadata only; DO NOT include entry.unique_id, entry.title, or entry.data (contains secrets)
            "entry_id": entry.entry_id,
            "version": entry.version,  # config-entry schema version (safe)
            "domain": entry.domain,
        },
        "config": config_summary,
        "effective_config": redacted_effective_config,
        "registries": {
            "device": device_registry_counts,
            "entity": entity_registry_counts,
        },
        "concurrency": concurrency,
        "eik_cache": eik_cache_stats,
        "foreign_report_readings": foreign_report_readings,
        "uwt_transitions": uwt_transitions,
        "fmdn_component_flags": _component_flags_block(hass, entry.entry_id),
    }
    if crypto_info:
        payload["crypto"] = _crypto_block(crypto_info)
    if coordinator_block:
        payload["coordinator"] = coordinator_block
    if fcm_state:
        payload["fcm_receiver_state"] = fcm_state

    # --- Final safety net: redact known secret-like keys anywhere in the payload ---
    # (We already avoided including secrets, but this keeps us safe against future extensions.)
    return async_redact_data(payload, TO_REDACT, TO_REDACT_PREFIXES, accounts=accounts)
