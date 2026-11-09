"""Cache operations mixin for GoogleFindMyCoordinator.

Methods moved here:
- get_device_location_data: Get cached location data for a device
- prime_device_location_cache: Prime cache with initial data
- seed_device_last_seen: Seed last_seen timestamp for a device
- _track_device_interval: Track device polling intervals
- _persist_anchor_metadata: Persist anchor metadata for EID resolution
- update_device_cache: Update device location cache
- _propagate_location_to_shared_devices: Propagate location to shared devices
- _is_significant_update: Check if update is significant
- _merge_with_existing_cache_row: Merge new data with existing cache
- _haversine_distance: Calculate distance between two coordinates
- _apply_weighted_location_fusion: Apply weighted location fusion
"""

from __future__ import annotations

import hashlib
import logging
import math
import time
from collections import deque
from collections.abc import Mapping
from typing import Any, NamedTuple

from ..const import (
    _EID_REFRESH_DEBOUNCE_S,
    DATA_EID_RESOLVER,
    DEFAULT_ACCURACY_GATE_ENABLED,
    DEFAULT_MAX_PLAUSIBLE_SPEED_MPS,
    DEFAULT_ROUNDTRIP_CONFIRM,
    DEFAULT_SPEED_GATE_ENABLED,
    DOMAIN,
    OPT_ACCURACY_GATE_ENABLED,
    OPT_ROUNDTRIP_CONFIRM,
    OPT_SPEED_GATE_ENABLED,
    ROUND_TRIP_ANCHOR_RADIUS_M,
    ROUND_TRIP_TTL_S,
)
from ._mixin_typing import _MixinBase
from .helpers.cache import (
    merge_cache_row as _merge_cache_row_impl,
)
from .helpers.cache import (
    normalize_location_fields as _normalize_location_fields_impl,
)
from .helpers.cache import (
    sanitize_decoder_row as _sanitize_decoder_row,
)
from .helpers.cache import (
    should_clear_metadata_only_flag as _should_clear_metadata_only_flag_impl,
)
from .helpers.geo import (
    ACCURACY_BUCKET_STATS,
    ACCURACY_GATE_MIN_M,
    ACCURACY_GATE_RATIO,
    DEFAULT_ACCURACY_FALLBACK_M,
    MIN_PHYSICAL_ACCURACY_M,
    is_reliable_fix,
    location_age_seconds,
    resolve_stale_threshold,
    select_display_row,
)
from .helpers.geo import (
    accuracy_bucket as _accuracy_bucket_impl,
)
from .helpers.geo import (
    coerce_float as _coerce_float_impl,
)
from .helpers.geo import (
    haversine_distance as _haversine_distance_impl,
)
from .helpers.geo import (
    is_valid_accuracy as _is_valid_accuracy,
)
from .helpers.subentry import normalize_epoch_seconds as _normalize_epoch_seconds

_LOGGER = logging.getLogger(__name__)

# Maximum accepted future drift of a report timestamp, in seconds (2 hours).
# Module level rather than local to ``_is_significant_update`` because the
# accuracy gate needs the same bound: it retains a rejected fix BEFORE that
# method runs, so without a shared constant there would be two answers to the
# question "is this timestamp plausible".
MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S = 7200

# Metadata keys to preserve across cache updates
_METADATA_KEYS = (
    "pair_date",
    "pairDate",
    "deviceRegistration",
    "secrets_creation_date",
    "secretsCreationDate",
    "encrypted_user_secrets_creation_date",
    "encryptedUserSecretsCreationDate",
    "time_anchors_debug",
    "identity_key",
    "identityKey",
    "eik",
    "encrypted_identity_key",
    "encryptedIdentityKey",
    "identity_key_candidates",
    "identityKeyCandidates",
    "owner_key_version",
    "device_type",
    "fast_pair_model_id",
    "fastPairModelId",
    "manufacturer",
    "model",
    "trackable_components",
    "encrypted_account_key",
    "encryptedAccountKey",
    "public_key_address",
    "encryptedSha256AccountKeyPublicAddress",
)

# CamelCase to snake_case key normalization for metadata fields
_CAMEL_TO_SNAKE: dict[str, str] = {
    "identityKey": "identity_key",
    "pairDate": "pair_date",
    "secretsCreationDate": "secrets_creation_date",
    "encryptedUserSecretsCreationDate": "encrypted_user_secrets_creation_date",
    "encryptedIdentityKey": "encrypted_identity_key",
    "identityKeyCandidates": "identity_key_candidates",
    "fastPairModelId": "fast_pair_model_id",
    "encryptedAccountKey": "encrypted_account_key",
    "encryptedSha256AccountKeyPublicAddress": "public_key_address",
}

# Epoch timestamp for year 2000 (2000-01-01 00:00:00 UTC)
# Used to reject timestamps that are clearly invalid (before Y2K)
_Y2K_EPOCH_SECONDS = 946684800.0


def _normalize_metadata_keys(data: dict[str, Any]) -> dict[str, Any]:
    """Normalize camelCase metadata keys to snake_case.

    This ensures consistent key naming in the cache regardless of
    whether the source payload uses camelCase or snake_case.

    Args:
        data: Dictionary potentially containing camelCase keys.

    Returns:
        New dictionary with normalized key names.
    """
    result: dict[str, Any] = {}
    for key, value in data.items():
        normalized_key = _CAMEL_TO_SNAKE.get(key, key)
        # Don't overwrite if snake_case version already set
        if normalized_key not in result:
            result[normalized_key] = value
    return result


class _GateMetrics(NamedTuple):
    """The four numbers the accuracy gate compares, bundled for one signature.

    They are computed once in the fusion and handed to the gate from two call
    sites; passing them individually pushed the signature past the project's
    argument limit, and recomputing them inside the gate would mean a second
    copy of ``_safe_accuracy``'s sentinel handling.
    """

    new_acc: float
    new_acc_raw: float | None
    existing_acc: float
    dist: float


class _StampVerdict(NamedTuple):
    """Verdict on a payload's ``last_seen``, for the two callers that need it.

    ``reason`` is None when the stamp is usable. ``rejected_downstream`` says
    whether ``_is_significant_update`` would reject this class on its own - the
    distinction the accuracy gate needs, and the one a plain boolean lost.
    ``seen`` is the normalized stamp, handed back so the retention path does not
    parse ``last_seen`` twice and so the type checker sees the narrowing.
    """

    reason: str | None
    rejected_downstream: bool
    seen: float | None


def _stamp_verdict(
    coord: Any,
    device_id: str,
    row: Mapping[str, Any],
) -> _StampVerdict:
    """Judge a payload's ``last_seen`` for retention and for gate ownership.

    TWO CALLERS, TWO QUESTIONS, ONE RULE. ``_record_coarse_fix`` must not RETAIN
    a fix with any unusable stamp (one hours ahead yields a negative age, which
    its freshness test reads as "very fresh"). ``_accuracy_gate_rejects`` asks
    something narrower: may it CLAIM this drop? It may not, for the three
    classes ``_is_significant_update`` rejects by itself (pre-Y2K, too far in
    the future, older than the published row) - those are dropped one step later
    anyway, and only there are they sorted into ``invalid_ts_drop_count`` /
    ``future_ts_drop_count``. Booking them as ``accuracy_gate_rejects`` would
    silence those counters and fill the one counter that exists to judge this
    gate's thresholds with drops that say nothing about accuracy.

    A MISSING OR UNPARSEABLE STAMP IS THE EXCEPTION, and it is the reason this
    returns a verdict rather than a bool. ``_is_significant_update`` normalizes
    to None there and then skips all three timestamp checks, so it does NOT
    reject the payload. Handing such a payload on would let the coarse fix
    through the very gate it was measured against. It therefore stays this
    gate's case: rejected and counted here, retention still refused.
    """
    seen = _normalize_epoch_seconds(row.get("last_seen"))
    if seen is None:
        return _StampVerdict(
            f"no usable timestamp {row.get('last_seen')!r}", False, None
        )
    if (
        seen < _Y2K_EPOCH_SECONDS
        or seen > time.time() + MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S
    ):
        return _StampVerdict(f"implausible timestamp {seen}", True, None)
    # The forward-order rule against the PUBLISHED row. A stamp in the future is
    # skipped as authority for the same reason as above: it is corrupt, not
    # newer.
    published = (getattr(coord, "_device_location_data", None) or {}).get(device_id)
    if published:
        published_seen = _normalize_epoch_seconds(published.get("last_seen"))
        if (
            published_seen is not None
            and published_seen <= time.time()
            and seen < published_seen
        ):
            return _StampVerdict(
                f"timestamp {seen} predates the published row at {published_seen}",
                True,
                None,
            )
    return _StampVerdict(None, False, seen)


def _accuracy_gate_rejects(
    coord: Any,
    device_id: str,
    new_data: dict[str, Any],
    existing: Mapping[str, Any],
    metrics: _GateMetrics,
) -> bool:
    """Return ``True`` when a coarse fix must not displace the cached one (#216).

    The comparative half of the fusion's rejection logic, extracted so the two
    overlap-free situations share ONE rule instead of two that drift apart: a
    clear jump away from an ordinary cached fix, and a clear jump away from a
    trusted semantic anchor. On rejection this also retains the coarse fix as
    side information and counts the rejection, so the caller only has to honour
    the boolean.

    It is a COMPARISON, never an absolute cut-off. Its removed predecessor
    (``min_accuracy_threshold``, const default 100 m) discarded on the incoming
    radius alone and froze trackers; a fix with a wide radius still tells us the
    city, and without any fix we would not even know that. So nothing is
    discarded unless a better, reliable and still-fresh alternative is actually
    present.

    Deliberately NO own-report bypass, unlike the speed gate (whose bypass is
    about cryptographic provenance, i.e. whether the report can be trusted).
    This gate is about physical uncertainty, and a phone reports ITS OWN
    position, not the tracker's: an own fix with a 1600 m radius is exactly as
    coarse as a foreign one.

    Staleness is read from the shared ``stale_threshold`` option, not from a
    second time constant, and it is the age against the wall clock - not the
    distance between the two report timestamps the speed gate uses. An unknown
    age counts as not-trustworthy, so the incoming fix wins.

    No round-trip anchor is seeded or consumed here (F-CODEX-6 keeps anchor
    provenance bound to the speed gate's one-shot semantics).

    WHY A MODULE FUNCTION AND NOT A METHOD: every fusion suite builds its
    coordinator double as ``MagicMock(spec=CacheOperations)``. A method of that
    class is auto-mocked there and returns a truthy ``Mock``, so the gate would
    silently reject EVERY payload in eight existing suites - measured: nine
    round-trip cases went red on exactly that. A module-level function cannot be
    shadowed by the spec, so those doubles keep exercising the real rule, which
    is what they did while this code was still inline.
    """
    if not coord._accuracy_gate_enabled():
        return False
    # The Google Home filter substitutes the HOME ZONE's radius for the reported
    # accuracy before fusion, on all three inbound paths (poll, manual locate,
    # push). That number is not a measurement of the device, so comparing it
    # against the cached precision asks the wrong question: with a home zone of
    # 200 m or more the gate would refuse the filter's deliberate decision and
    # leave a tracker away from home instead of moving it there. The marker is
    # transient and popped before commit; it says "this radius was substituted",
    # which only the substituting site can know.
    if new_data.get("_accuracy_substituted"):
        return False
    # Mirrors the speed gate's incoming-side check: a missing, non-finite or
    # sentinel accuracy is not a measurement, and an unmeasured radius must
    # never be the reason to discard a fix.
    new_acc_measured = (
        metrics.new_acc_raw is not None
        and math.isfinite(metrics.new_acc_raw)
        and metrics.new_acc_raw >= MIN_PHYSICAL_ACCURACY_M
    )
    if not (
        new_acc_measured
        and metrics.new_acc >= ACCURACY_GATE_MIN_M
        and metrics.new_acc >= ACCURACY_GATE_RATIO * metrics.existing_acc
        and is_reliable_fix(existing)
    ):
        return False
    existing_age = location_age_seconds(existing, time.time())
    threshold = resolve_stale_threshold(coord)
    # A NEGATIVE age means the cached row is stamped in the future - clock skew
    # on the reporting device, tolerated up to
    # MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S elsewhere. Only checking ``> threshold``
    # would read such a row as maximally fresh and let it veto every coarse
    # update until wall time catches up AND the stale interval then elapses,
    # pinning the tracker for hours. The reference this gate compares against
    # has to be trustworthy in BOTH directions; ``_record_coarse_fix`` already
    # refuses a future stamp as an ordering authority for the same reason.
    if existing_age is None or existing_age < 0 or existing_age > threshold:
        return False
    # Temporal validity decides WHO may claim this drop, and it is checked
    # before the payload is classified as an accuracy rejection. A payload with
    # a corrupt or regressed stamp is dropped either way - but by the
    # significance gate one step later, which is the only place that sorts it
    # into ``invalid_ts_drop_count`` / ``future_ts_drop_count``. Claiming it
    # here would silence those counters and inflate ``accuracy_gate_rejects``
    # with drops that say nothing about the two thresholds this counter exists
    # to judge. Returning False costs nothing: the payload cannot commit, since
    # ``_is_significant_update`` rejects exactly these three classes.
    verdict = _stamp_verdict(coord, device_id, new_data)
    if verdict.rejected_downstream:
        _LOGGER.debug(
            "Accuracy gate not claiming coarse fix %s (%s); leaving the drop "
            "to the significance gate, which classifies it",
            device_id,
            verdict.reason,
        )
        return False
    # Drop any round-trip anchor INTENT the branches above pencilled onto this
    # payload. Both are deferred markers (F-CODEX-7): they are popped and
    # applied only after the payload commits, and this payload never commits -
    # the caller aborts on a False return - so leaving them behind has no
    # effect today. They are cleared anyway, because a rejected payload
    # carrying a live anchor intent is a trap for the next person to touch this
    # ordering, and because E7 ("no anchor contact") should be true of the data,
    # not just of its consequences. On the trusted-anchor call site no marker
    # can be set yet (that branch runs before the speed gate), so the pops are
    # no-ops there rather than a second semantics.
    new_data.pop("_supersede_round_trip_anchor", None)
    new_data.pop("_round_trip_anchor_seed", None)
    new_data.pop("_round_trip_anchor_consume", None)
    # Keep the coarse information before dropping the payload: the caller
    # aborts on False, so this must happen first.
    coord._record_coarse_fix(device_id, new_data)
    coord.increment_stat("accuracy_gate_rejects")
    _LOGGER.debug(
        "Accuracy gate rejected coarse fix %s: %.0fm vs cached %.0fm "
        "(>= %.0fx), %.0fm away, cached fix %.0fs old (<= %ss stale threshold)",
        device_id,
        metrics.new_acc,
        metrics.existing_acc,
        ACCURACY_GATE_RATIO,
        metrics.dist,
        existing_age,
        threshold,
    )
    return True


def _finite_or_none(value: Any) -> float | None:
    """Return *value* as a finite float, or ``None`` if it is not one.

    ``NaN`` never compares equal to itself, so letting it into an identity tuple would
    make every delivery of the same report look new. That is the direction this helper
    exists to close.
    """
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


# How many tallied identities are remembered per device. The case is a report that keeps
# coming back, and two such reports can alternate, so one slot is not enough; an unbounded
# set is not an option in state that persists. Eight covers alternation between a handful
# of stuck reports across a poll cycle and costs a few hundred bytes per device.
_TALLY_MEMORY = 8


def _has_countable_accuracy(row: Mapping[str, Any]) -> bool:
    """Say whether ``count_accuracy_class`` would actually tally this row.

    The claim and the tally have to agree on which reports they are about. A
    semantic-only response carries a timestamp but no usable accuracy, so nothing is
    counted for it - and a claim recorded anyway would later suppress the same report
    arriving through another path WITH its accuracy, silencing a measurement that was
    never taken. Same predicate as the tally, read from the same helpers rather than
    restated, so the two cannot drift apart.
    """
    raw = _coerce_float_impl(row.get("accuracy"))
    if not _is_valid_accuracy(raw):
        return False
    return _accuracy_bucket_impl(raw) is not None


def _tally_identity(row: Mapping[str, Any]) -> tuple[Any, ...] | None:
    """Return what identifies *row* for tally deduplication, or ``None``.

    The timestamp is the identity whenever there is one; it is what every other
    reference in this module keys on, and it distinguishes two genuinely different
    reports that happen to share a position.

    A report with no parseable ``last_seen`` still carries an accuracy, so it is still
    tallied - and being unidentifiable, it used to be tallied again on every retry, so
    the persisted distribution measured retry frequency rather than incoming fixes. Its
    position and radius are the only bounded thing left to key on. Two consecutive
    reports agreeing on all three fields and carrying no stamp are indistinguishable by
    construction, so counting them once is the honest reading rather than a compromise.

    ``None`` means the report carries nothing to recognise it by. The caller then lets
    it through, because refusing to count a fix is the worse error of the two: the
    distribution is what the accuracy gate is judged on.
    """
    ts = _normalize_epoch_seconds(row.get("last_seen"))
    if ts is not None:
        return ("ts", ts)
    fix = tuple(
        _finite_or_none(row.get(key)) for key in ("latitude", "longitude", "accuracy")
    )
    if all(value is None for value in fix):
        return None
    # Digest rather than the values: this identity is persisted, and a position in a
    # durable store is exactly what the rest of this feature is careful not to keep.
    # Equality is all the comparison needs, so the coordinates never have to leave the
    # process. blake2s with an 8-byte digest, because this guards against repetition,
    # not against an adversary.
    digest = hashlib.blake2s(repr(fix).encode("utf-8"), digest_size=8).hexdigest()
    return ("fix", digest)


# How many fused report identities are remembered per device. Same number and same
# reasoning as ``_TALLY_MEMORY``: the case is a report that keeps coming back, two such
# reports can alternate within one second, and a single slot would let them refuse each
# other on every delivery. The alternation is constructed, not measured live.
_FUSED_REPORT_MEMORY = 8

# What a repeated report takes over from the cached row: the fused geometry and the
# fields the fusion itself writes. Anything else in the report is news and commits.
_DUPLICATE_GEOMETRY_KEYS = (
    "latitude",
    "longitude",
    "accuracy",
    "accuracy_estimated",
    "status",
    "location_type",
)


def _fusion_report_identity(row: Mapping[str, Any]) -> str | None:
    """Return what identifies *row* as one report for the fusion, or ``None``.

    Not ``_tally_identity``: that one takes the timestamp as the whole identity, and
    two genuinely different reports of the same second would then be fused once
    between them. This one keys on the stamp AND the raw position and radius, so it
    answers "has exactly this report been fused already?".

    The fields are normalised here rather than trusted as they arrive, because the
    paths hand the fusion different shapes of one report: poll and manual locate run
    ``sanitize_decoder_row`` and drop an error-code accuracy BEFORE the fusion, the
    push path only after it. Without the normalisation the same report would carry a
    different identity per path and be fused once per path.

    A missing stamp is part of the identity (``None``), not a reason to give up: two
    identical stampless reports are indistinguishable by construction, the same
    reading ``_tally_identity`` takes. Only a row without a usable position has
    nothing to recognise it by. A digest rather than the values, so no coordinate is
    kept beside the cache; blake2s with 8 bytes, because this guards against
    repetition, not against an adversary.

    Whether the report arrives as a trusted anchor is part of the identity: a
    semantic mapping writes the configured position and radius into the report, so
    an ordinary report can carry the same geometry. Treating the anchor as its repeat
    would skip the promotion of the row to ``trusted``. A semantic report with the
    cached stamp is a replay and skips the mapping, so the case the tests pin is one
    of reports without a stamp.

    Whether a valid accuracy is only the fallback estimate is part of it as well: the
    fallback radius is the same number as a real fix of that size, and taking the
    measured report as the repeat of the estimated one would keep the row marked
    estimated. Compared with ``is True``, so a row that never carried the flag and
    one that carries ``False`` stay the same report.
    """
    latitude = _finite_or_none(row.get("latitude"))
    longitude = _finite_or_none(row.get("longitude"))
    if latitude is None or longitude is None:
        return None
    raw_accuracy = _coerce_float_impl(row.get("accuracy"))
    accuracy = raw_accuracy if _is_valid_accuracy(raw_accuracy) else None
    fix = (
        _normalize_epoch_seconds(row.get("last_seen")),
        latitude,
        longitude,
        accuracy,
        row.get("accuracy_estimated") is True,
        row.get("location_type") == "trusted",
    )
    return hashlib.blake2s(repr(fix).encode("utf-8"), digest_size=8).hexdigest()


def _is_fused_report(coord: Any, device_id: str, identity: str | None) -> bool:
    """True when *identity* was committed for *device_id* already.

    A module function, not a method, for the reason ``_accuracy_gate_rejects`` gives:
    the fusion suites build their coordinator as ``MagicMock(spec=CacheOperations)``,
    where a method would be auto-mocked into a truthy ``Mock`` and every report would
    look fused. The store is read with ``getattr`` because it is created lazily. A
    ``None`` identity is never booked, so it is never found either.
    """
    fused = getattr(coord, "_fused_report_ids", None) or {}
    return identity in (fused.get(device_id) or ())


def _book_fused_report(coord: Any, device_id: str, identity: str | None) -> None:
    """Remember that *identity* is now part of *device_id*'s cached row.

    Called only after the row is committed: a report the significance gate drops was
    never taken over, and remembering it would refuse its next delivery for nothing.
    Booked only when absent, in the shape of ``claim_report_for_tally``; appending on
    every delivery would let the repeats of one report evict the other report of the
    same second from the ring.
    """
    if identity is None or _is_fused_report(coord, device_id, identity):
        return
    fused = getattr(coord, "_fused_report_ids", None)
    if fused is None:
        fused = {}
        coord._fused_report_ids = fused
    ring = list(fused.get(device_id) or ())
    ring.append(identity)
    fused[device_id] = ring[-_FUSED_REPORT_MEMORY:]


class CacheOperations(_MixinBase):
    """Cache operations mixin for GoogleFindMyCoordinator.

    This class contains methods that manage the device location cache,
    including cache updates, location fusion, and metadata persistence.
    """

    def get_device_location_data(self, device_id: str) -> dict[str, Any] | None:
        """Return the cached location data for a single device (copy)."""
        raw = self._device_location_data.get(device_id)
        if not isinstance(raw, dict):
            return None
        return dict(raw)

    def get_display_location_data(self, device_id: str) -> dict[str, Any] | None:
        """Return the row that is actually published for a device (copy).

        The current fix when it carries a usable accuracy, otherwise the last
        accuracy-bearing fix (coordinator last-good), otherwise ``None``. This is
        the single source feeding both Plus Code consumers (the standalone sensor
        and the device_tracker ``plus_code`` attribute), so they are last-known
        by construction and never publish an accuracy-less coordinate the tracker
        hides (Codex #202 / PR #1179).
        """
        row = select_display_row(
            self.get_device_location_data(device_id),
            getattr(self, "_device_last_good_location", {}).get(device_id),
        )
        return dict(row) if row is not None else None

    def _record_last_good_location(
        self, device_id: str, row: Mapping[str, Any]
    ) -> None:
        """Store ``row`` as the device's last *reliable* fix (last-good).

        Only a reliable (non-estimated) fix advances the last-good, so a
        sanitized accuracy-less fix (``_is_significant_update`` replaces its
        missing/error-code accuracy with the 200 m fallback and sets
        ``accuracy_estimated=True``) never poisons the fallback the Plus Code
        display accessors read (#1179). A plain ``has_usable_accuracy`` check
        could not tell that sanitized row apart from a real fix; ``is_reliable_fix``
        excludes the estimated provenance. The ``elif`` is a bootstrap: when
        nothing reliable has been seen yet, the first (estimated/accuracy-less)
        fix is kept, mirroring the tracker's ``_last_good_accuracy_data``
        bootstrap. An *estimated* bootstrap is still published (it carries the
        200 m fallback, so ``select_display_row`` shows it); a *genuinely*
        accuracy-less bootstrap (e.g. a legacy/partial restore that never went
        through ``_is_significant_update``) is retained only so age / status /
        ``has_last_known`` stay available -- ``select_display_row`` applies the
        same ``has_usable_accuracy`` gate to it as to any other fallback and
        never publishes it as a coordinate (Codex PR #1181). The cache is lazily
        initialized (mirroring the ``_device_update_history`` idiom below) so
        coordinators built via ``__new__`` need no extra wiring.
        """
        cache = getattr(self, "_device_last_good_location", None)
        if cache is None:
            cache = {}
            self._device_last_good_location = cache
        if is_reliable_fix(row):
            cache[device_id] = dict(row)
        elif device_id not in cache:
            cache[device_id] = dict(row)

    def _record_coarse_fix(self, device_id: str, row: Mapping[str, Any]) -> None:
        """Remember a fix the accuracy gate discarded, as side information (#216).

        A coarse fix is discarded as a *position* because publishing it would
        move the tracker and, via the accuracy radius Home Assistant uses as the
        zone tolerance, could report it as home. Its coarse information (the
        city, roughly) is still the only thing we would have if no better fix
        existed, so it is kept here and surfaced as ``coarse_*`` attributes
        rather than thrown away. Deliberately NOT written into
        ``_device_location_data``: nothing downstream may mistake it for the
        published position.

        Must be called BEFORE the gate returns False: the caller aborts the
        whole payload on a False return (see the fusion call site), so a write
        placed after it would never run. Lazily initialized via ``getattr``,
        mirroring ``_record_last_good_location`` so coordinators built through
        ``__new__`` (or partial test doubles) need no extra wiring. RAM-only.
        """
        # A rejected payload never reaches ``_is_significant_update``, where a
        # corrupt, future or regressed timestamp would normally be dropped and
        # counted. Without this check a fix stamped hours ahead would be
        # retained here, and the age computed against the wall clock would come
        # out NEGATIVE - which the freshness test below reads as "very fresh".
        # The rule itself lives in ``_implausible_timestamp_reason`` because the
        # accuracy gate needs the same answer for a different purpose, and two
        # copies of "plausible" would drift apart.
        verdict = _stamp_verdict(self, device_id, row)
        seen = verdict.seen
        if verdict.reason is not None or seen is None:
            _LOGGER.debug(
                "Not retaining coarse fix for %s: %s", device_id, verdict.reason
            )
            return

        store = getattr(self, "_device_coarse_fix", None)
        # Never let an older report replace a newer one. ``_is_significant_update``
        # enforces forward order for the published row, but a rejected payload
        # never gets there, so an out-of-order crowd report would otherwise
        # overwrite a more recent coarse fix and make the side information go
        # backwards in time.
        if store is not None:
            previous = store.get(device_id)
            if previous is not None:
                previous_seen = _normalize_epoch_seconds(previous.get("last_seen"))
                # A retained stamp that lies in the future must not block its own
                # replacement. The bound above tolerates up to
                # MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S of clock skew, but the
                # reader discards a future stamp outright (negative age), so such
                # an entry is invisible anyway - letting it win the ordering test
                # would suppress every real coarse fix until wall time caught up.
                if previous_seen is not None and previous_seen > time.time():
                    previous_seen = None
                if previous_seen is not None and seen < previous_seen:
                    _LOGGER.debug(
                        "Not retaining coarse fix for %s: timestamp %s regresses "
                        "behind the retained %s",
                        device_id,
                        seen,
                        previous_seen,
                    )
                    return
        if store is None:
            store = {}
            self._device_coarse_fix = store
        store[device_id] = {
            "latitude": row.get("latitude"),
            "longitude": row.get("longitude"),
            "accuracy": row.get("accuracy"),
            # The NORMALIZED stamp, not the raw field. The guards above judge
            # ``seen``, but every reader of this store computes the age with
            # ``location_age_seconds``, which does a plain ``float()`` and is
            # NOT millisecond-tolerant. Storing the raw value would let a
            # millisecond stamp pass the guards here and then read as ~55 years
            # in the future downstream: the entity would hide the fix
            # (negative age) and the diagnostics would clamp it to ``0``, i.e.
            # report "just now" for a stamp it could not interpret.
            "last_seen": seen,
        }

    def count_accuracy_class(self, row: Mapping[str, Any]) -> None:
        """Tally the REPORTED accuracy class of an incoming fix (#216).

        The reject counter answers "how often did the gate fire"; it does not
        answer "do 200 m and factor 4 sit in the right place". Only the
        distribution answers that, and it has to be collected BEFORE the gate -
        otherwise it never sees the discarded fixes, which are the interesting
        half.

        MUST be called on every path that feeds a fix into the fusion, and
        exactly once per fix. There are three entry points (the poll loop, the
        manual locate and the FCM push) plus the ``update_device_cache``
        fallback that a device-list seed reaches, which makes four counting
        sites; ``test_every_counting_site_claims_the_report_first`` derives that
        set from the sources rather than trusting this sentence;
        the first two run the fusion themselves and mark the payload
        ``_fusion_preapplied``, so ``update_device_cache`` deliberately counts
        only when that marker is absent. ``tests/test_cache_accuracy_gate.py``
        pins that every caller of ``_apply_weighted_location_fusion`` also
        calls this, so a fourth entry point cannot silently skip it.

        A fix whose accuracy is missing, non-numeric or carries Android's
        no-accuracy sentinel (0.0, i.e. below ``MIN_PHYSICAL_ACCURACY_M``) is
        NOT counted. ``accuracy_bucket`` alone would file the sentinel under
        ``<10`` and skew the distribution towards precision - the exact opposite
        of what this measurement is for. Those fixes are counted by
        ``accuracy_sanitized_count`` instead.
        """
        raw = _coerce_float_impl(row.get("accuracy"))
        if not _is_valid_accuracy(raw):
            return
        bucket = _accuracy_bucket_impl(raw)
        if bucket is not None:
            self.increment_stat(ACCURACY_BUCKET_STATS[bucket])

    def is_replayed_report(self, device_id: str, row: Mapping[str, Any]) -> bool:
        """True when this report has already entered the accuracy distribution.

        ONE source of truth, deliberately: the identities this coordinator has actually
        tallied. Earlier revisions also compared against the published row and the
        retained coarse fix, and both were surrogates for the question rather than
        answers to it. A semantic-only response commits its timestamp after copying the
        previous coordinates and accuracy, so a cache row proves that a timestamp was
        SEEN, not that a bucket was counted for it. The same report arriving later
        through another path with its real accuracy was then suppressed - the very case
        the countability check exists to preserve.

        A bounded RING per device, not one replaceable value. Two reports that both fail
        to enter either cache and are delivered alternately overwrote each other's claim,
        so T1, T2, T1, T2 was counted on every delivery while the comment claimed it was
        counted twice. The ring holds the last ``_TALLY_MEMORY`` identities, which bounds
        the memory without pretending the alternating case does not exist.

        Deliberately separate from the ``is_replayed`` flag the inbound paths compute for
        the Google Home filter branch: that one compares against the published row only,
        and widening it here would change a behaviour this change never measured.
        """
        identity = _tally_identity(row)
        if identity is None:
            return False
        tallied = getattr(self, "_last_tallied_report_id", None) or {}
        return identity in (tallied.get(device_id) or ())

    def claim_report_for_tally(self, device_id: str, row: Mapping[str, Any]) -> bool:
        """Decide whether this report may enter the distribution, and claim it.

        Returns True exactly once per distinct report timestamp per device. The
        name says what it does: it is not a pure predicate, it RECORDS the
        timestamp it lets through, which is the only way to recognise the retry
        of a report that never reached either cache.

        A bounded ring of the last ``_TALLY_MEMORY`` identities per device, not a single
        value: two reports delivered alternately used to overwrite each other's claim and
        were then counted on every delivery. The ring is what bounds the memory; the
        declared limit is now the ring length, a number one can raise, rather than "one",
        a shape one could not.
        """
        if not _has_countable_accuracy(row):
            # Nothing to claim, because nothing will be counted. Returning False here
            # costs nothing - the caller would tally zero either way - and it keeps the
            # slot free for the delivery that does carry an accuracy.
            return False
        if self.is_replayed_report(device_id, row):
            return False
        identity = _tally_identity(row)
        if identity is None:  # pragma: no cover - unreachable
            # A countable accuracy is itself part of the fallback identity, so passing
            # the check above guarantees one. Kept as a typed guard rather than an
            # assertion, which would vanish under -O, and marked because a branch that
            # cannot be entered must not look like one that merely lacks a test.
            return True
        tallied = getattr(self, "_last_tallied_report_id", None)
        if tallied is None:
            tallied = {}
            self._last_tallied_report_id = tallied
        ring = list(tallied.get(device_id) or ())
        ring.append(identity)
        tallied[device_id] = ring[-_TALLY_MEMORY:]
        return True

    def clear_tally_claims(self) -> None:
        """Forget every claimed report identity, for all devices.

        Called when the accuracy histogram is reset. The claims and the
        histogram are two halves of one statement - "this report has already
        been counted" - and they are persisted in one record, so zeroing the
        counters without clearing the claims leaves a state that says nothing
        was counted and simultaneously refuses to count the reports that would
        refill it. A delivery whose identity is still in the ring is treated as
        a replay, so the reset distribution would stay empty for exactly the
        devices that were most active before it.

        Bounded, so bounded is the damage: the ring holds ``_TALLY_MEMORY``
        identities per device, and only an IDENTICAL identity is suppressed. A
        report with a new timestamp was never affected; what a reset without
        this loses is at most ``_TALLY_MEMORY`` already-claimed re-deliveries
        per device.

        A reset starts a new epoch, it does not draw a wall-clock boundary: a
        report that was counted into the previous histogram and is delivered
        again afterwards is counted into the new one. That is the intended
        reading of "reset", and it is the same report the claim was protecting.

        Kept here rather than in the caller because ``_last_tallied_report_id``
        is written in this module only; a second file naming it would drift the
        moment the store changes shape (it already changed once, from a single
        value per device to a bounded ring).
        """
        tallied = getattr(self, "_last_tallied_report_id", None)
        # isinstance rather than the `or {}` / `is None` reads the neighbours use
        # (``is_replayed_report`` above, ``_async_save_stats`` and ``purge_device`` in
        # main.py): theirs degrade to a no-op on an unexpected shape, whereas ``.clear()``
        # would raise into the caller's swallowing except and the reset would stop
        # clearing in silence. Different obligation, so deliberately a different read.
        if isinstance(tallied, dict):
            tallied.clear()

    def _expire_coarse_fix(self, device_id: str, committed: Mapping[str, Any]) -> None:
        """Drop a retained coarse fix that the just-committed row supersedes.

        Called after a payload commits. The coarse fix is side information for
        the case that nothing better exists; once a fix with an equal or newer
        timestamp is published, it is stale by definition, no matter how young
        its own stamp still looks to the reader's age rule.

        A committed row without a usable stamp expires nothing: it cannot be
        shown to be newer, and dropping side information on an unprovable claim
        would lose the only coarse position we have.
        """
        store = getattr(self, "_device_coarse_fix", None)
        if not store or device_id not in store:
            return
        committed_seen = _normalize_epoch_seconds(committed.get("last_seen"))
        if committed_seen is None:
            return
        coarse_seen = _normalize_epoch_seconds(store[device_id].get("last_seen"))
        if coarse_seen is None or coarse_seen <= committed_seen:
            store.pop(device_id, None)
            _LOGGER.debug(
                "Dropped coarse fix for %s: a newer position was published",
                device_id,
            )

    def get_coarse_fix(self, device_id: str) -> dict[str, Any] | None:
        """Return the last fix the accuracy gate discarded, or ``None`` (#216).

        The raw store window, without a freshness rule: producers and tests need to see
        what is actually retained. Readers that publish the fix want
        :meth:`get_fresh_coarse_fix` instead.
        """
        store = getattr(self, "_device_coarse_fix", None)
        if not store:
            return None
        row = store.get(device_id)
        return dict(row) if row is not None else None

    def get_fresh_coarse_fix(self, device_id: str) -> dict[str, Any] | None:
        """Return the retained coarse fix only while it is still worth showing.

        Nothing removes a retained fix when time merely passes - the store is pruned
        when a newer fix commits - so a rejection that is never followed by a better fix
        stays readable forever. The device tracker knew that and hid it after
        ``stale_threshold``; the diagnostics path did not, and kept exporting its bucket
        and an ever-growing age for side information the integration otherwise treats as
        expired.

        One rule in one place, rather than at each reader: the same ``stale_threshold``
        the gate and the tracker use, and a negative age counts as corrupt rather than
        as extremely fresh. The tracker keeps its own check as a second line, because
        reading a position is where the damage would show.
        """
        row = self.get_coarse_fix(device_id)
        if row is None:
            return None
        age = location_age_seconds(row, time.time())
        if age is None or age < 0 or age > resolve_stale_threshold(self):
            return None
        return row

    def prime_device_location_cache(self, device_id: str, data: dict[str, Any]) -> None:
        """Prime the internal location cache with externally-provided data.

        This is intended for test fixtures or bootstrap scenarios where the
        coordinator should start with pre-populated location information
        before its first poll cycle completes.
        """
        existing = self._device_location_data.get(device_id)
        if existing:
            merged = dict(existing)
            merged.update(data)
            self._device_location_data[device_id] = merged
        else:
            self._device_location_data[device_id] = dict(data)
        # A primed position replaces the geometry the remembered reports were
        # fused into (on a restart the first refresh can commit before the
        # tracker restores an older state over it). Unlike a clear jump, which
        # puts a newer report in place, the primed row can be older than the
        # reports in the ring, so forget them; otherwise every repeat of the
        # current report would keep the primed, older position.
        if "latitude" in data or "longitude" in data:
            fused_reports = getattr(self, "_fused_report_ids", None)
            if fused_reports is not None:
                fused_reports.pop(device_id, None)
        # Restore-seed the coordinator last-good from the primed row so the Plus
        # Code display accessors return a value immediately after a restart,
        # before the first poll. The device_tracker already calls this on restore
        # (device_tracker.py) with the same row its private last-good restores
        # from, keeping the two last-good caches in sync by construction.
        self._record_last_good_location(device_id, data)

    def seed_device_last_seen(self, device_id: str, timestamp: float) -> None:
        """Seed a device's last-seen timestamp for cache initialization."""
        self._present_last_seen[device_id] = timestamp

    def _track_device_interval(self, device_id: str, last_seen: float | None) -> None:
        """Track last_seen history to predict future poll targets."""
        if last_seen is None:
            return

        history_store = getattr(self, "_device_update_history", None)
        if history_store is None:
            history_store = {}
            self._device_update_history = history_store

        history = history_store.setdefault(device_id, deque(maxlen=4))

        if not history or last_seen > history[-1]:
            history.append(last_seen)

    def _persist_anchor_metadata(
        self,
        device_id: str,
        payload: dict[str, Any],
        *,
        clear_metadata_only: bool = False,
    ) -> None:
        """Persist anchor/identity metadata for EID resolution debugging.

        This method extracts metadata fields from a location payload and
        merges them into the device's cache entry without overwriting
        location fields. The metadata includes:

        - pair_date / pairDate / deviceRegistration.pairDate
        - secrets_creation_date / secretsCreationDate
        - encrypted_user_secrets_creation_date
        - time_anchors_debug

        These fields help the EID resolver reason about rotation windows.

        Args:
            device_id: Canonical device identifier.
            payload: Raw location payload containing potential metadata.
            clear_metadata_only: When True and a new location coordinate is
                available, clear the `metadata_only` flag from the cache entry
                so subsequent snapshot builds treat the data as fresh.
        """
        if not payload:
            return

        # Use module-level constant for metadata keys
        metadata_keys = set(_METADATA_KEYS)

        existing = self._device_location_data.get(device_id)
        if not isinstance(existing, dict):
            existing = {}

        updated = dict(existing)

        # Extract metadata from payload
        for key in metadata_keys:
            value = payload.get(key)
            if value is not None:
                updated[key] = value

        # Handle nested deviceRegistration
        device_reg = payload.get("deviceRegistration")
        if isinstance(device_reg, dict):
            pair_date = device_reg.get("pairDate")
            if pair_date is not None and updated.get("pair_date") is None:
                updated["pair_date"] = pair_date

        # Clear metadata_only flag when we have real coordinates
        if clear_metadata_only and _should_clear_metadata_only_flag_impl(
            updated,
            payload.get("metadata_only"),
        ):
            updated.pop("metadata_only", None)

        # Only write back if we actually have metadata to persist
        has_metadata = any(updated.get(k) is not None for k in metadata_keys)
        if has_metadata or updated != existing:
            self._device_location_data[device_id] = updated

        # Trigger EID resolver refresh when identity_key is present.
        if "identity_key" in payload or "identityKey" in payload:
            self._schedule_inline_eid_resolver_refresh(device_id)

    def _schedule_inline_eid_resolver_refresh(self, device_id: str) -> None:
        """Debounced inline EID-resolver refresh trigger (AP-B2).

        This is the separate inline trigger site (distinct from the central
        ``_schedule_eid_resolver_refresh`` helper in identity.py). During an FCM
        burst, ``_persist_anchor_metadata`` runs once per anchored payload, so
        without coalescing each identity-bearing payload would spawn its own
        ``async_create_task(resolver.async_refresh())``. We hold one pending
        timer handle for this site: the first trigger arms a ``loop.call_later``
        timer, further triggers within ``_EID_REFRESH_DEBOUNCE_S`` are dropped,
        and the handle is cleared *before* the task is created so a trigger after
        the window still produces a fresh task (no swallowed window-change). The
        rebuild-vs-skip decision remains with the AP-B1 skip guard.
        """
        hass_obj = getattr(self, "hass", None)
        if hass_obj is None:
            return
        domain_bucket = hass_obj.data.get(DOMAIN) if hasattr(hass_obj, "data") else None
        if not isinstance(domain_bucket, dict):
            return
        eid_resolver = domain_bucket.get(DATA_EID_RESOLVER)
        if eid_resolver is None:
            return
        refresh_coro = getattr(eid_resolver, "async_refresh", None)
        if not callable(refresh_coro):
            return

        create_task = getattr(hass_obj, "async_create_task", None)
        if not callable(create_task):
            return

        def _spawn_refresh_task() -> None:
            """Clear the debounce handle, then create the single refresh task."""
            # Clear first so a trigger after the window arms a fresh timer.
            self._eid_inline_refresh_debounce_handle = None
            _LOGGER.debug(
                "Triggering EID Resolver refresh for %s (identity_key in anchor_payload)",
                device_id,
            )
            create_task(refresh_coro())

        loop = getattr(hass_obj, "loop", None)
        call_later = getattr(loop, "call_later", None)
        if not callable(call_later):
            # No event loop available (e.g. minimal stubs): fall back to the
            # legacy immediate behaviour so a refresh is never lost.
            _LOGGER.debug(
                "Triggering EID Resolver refresh for %s (identity_key in anchor_payload)",
                device_id,
            )
            create_task(refresh_coro())
            return

        # A timer is already pending for this window: coalesce this trigger.
        if getattr(self, "_eid_inline_refresh_debounce_handle", None) is not None:
            return

        self._eid_inline_refresh_debounce_handle = call_later(
            _EID_REFRESH_DEBOUNCE_S, _spawn_refresh_task
        )

    def update_device_cache(
        self,
        device_id: str,
        location_data: dict[str, Any],
        *,
        source: str | None = None,
    ) -> None:
        """Public, encapsulated update of the internal location cache for one device.

        Used by the FCM receiver (push path) and by internal manual-commit call sites.
        Expects validated fields (decrypt layer performs fail-fast checks).

        Internal rules:
        - Applies type-aware **poll** cooldowns based on an internal `_report_hint` (if present).
        - Strips `_report_hint` from the cached payload to avoid exposing internal fields.
        - Applies weighted fusion and semantic-anchor protection to stabilize coordinates
          while still updating timestamps and metadata.
        """
        # Thread safety: marshal to HA loop if needed
        if not self._is_on_hass_loop():
            self._run_on_hass_loop(self.update_device_cache, device_id, location_data)
            return

        if not isinstance(location_data, dict):
            _LOGGER.debug(
                "Ignored cache update for %s: payload is not a dict", device_id
            )
            return

        # Shallow copy to avoid caller-side mutation
        slot = dict(location_data)

        # Normalize fields
        slot = _normalize_location_fields_impl(slot)
        slot = _normalize_metadata_keys(slot)

        previous_cached = self._device_location_data.get(device_id)
        if not isinstance(previous_cached, Mapping):
            previous_cached = None
        comparison_cached = previous_cached

        # Preserve metadata from existing cache
        if isinstance(previous_cached, Mapping):
            for metadata_key in _METADATA_KEYS:
                cached_value = previous_cached.get(metadata_key)
                incoming_value = slot.get(metadata_key)
                if cached_value is None or incoming_value is not None:
                    continue
                if isinstance(cached_value, dict):
                    slot[metadata_key] = dict(cached_value)
                elif isinstance(cached_value, list):
                    slot[metadata_key] = list(cached_value)
                else:
                    slot[metadata_key] = cached_value

        # Handle metadata_only flag
        incoming_metadata_only = location_data.get("metadata_only")
        has_location_payload = (
            slot.get("latitude") is not None or slot.get("longitude") is not None
        )
        if slot.get("metadata_only") and incoming_metadata_only is False:
            slot.pop("metadata_only", None)
        elif (
            slot.get("metadata_only")
            and has_location_payload
            and incoming_metadata_only is not True
        ):
            slot.pop("metadata_only", None)

        clear_metadata_only = (
            has_location_payload and incoming_metadata_only is not True
        )
        self._persist_anchor_metadata(
            device_id, slot, clear_metadata_only=clear_metadata_only
        )

        # Record semantic label if available
        record_semantic = getattr(self, "_record_semantic_label", None)
        if callable(record_semantic):
            record_semantic(slot, device_id=device_id)

        # Check for replay (same timestamp)
        cached_loc = comparison_cached
        is_replay = False
        if isinstance(cached_loc, Mapping):
            new_ts = _normalize_epoch_seconds(slot.get("last_seen"))
            old_ts = _normalize_epoch_seconds(cached_loc.get("last_seen"))
            if new_ts is not None and old_ts is not None and new_ts == old_ts:
                is_replay = True

        slot["is_replayed"] = is_replay

        # Tally the REPORTED accuracy class (#216) BEFORE the semantic mapping
        # below can replace the value with an anchor radius, and only if nobody
        # upstream has counted this payload already. The marker is explicit
        # rather than derived from ``_fusion_preapplied``: deriving it coupled
        # "was fused" to "was counted", and the push path is exactly the case
        # where those two come apart - its accuracy is substituted in
        # ``FcmReceiverHA._prepare_coordinator_payload``, i.e. before this
        # method is ever entered, so it has to count there and say so here.
        # ... and claim it here too. This is the FOURTH counting site, not one of the
        # three entry points: a device-list seed reaches this fallback with nobody
        # upstream having counted it. A seed row carries coordinates and a radius but
        # frequently no parseable ``last_seen``, and the freshness check above then
        # compares ``None`` with ``None`` and skips nothing, so every refresh added the
        # same radius again and the persisted histogram measured polling frequency. The
        # claim is what recognises that repetition; nothing else here can.
        if not slot.pop("_accuracy_counted", False) and self.claim_report_for_tally(
            device_id, slot
        ):
            self.count_accuracy_class(slot)

        # Apply semantic location mapping
        apply_mapping = getattr(self, "_apply_semantic_mapping", None)
        if callable(apply_mapping):
            apply_mapping(slot)

        slot.pop("is_replayed", None)

        report_hint = slot.get("_report_hint")
        fusion_preapplied = bool(slot.pop("_fusion_preapplied", False))

        if not fusion_preapplied and not self._apply_weighted_location_fusion(
            device_id, slot
        ):
            _LOGGER.debug(
                "Dropping cache update for %s: weighted fusion rejected payload",
                device_id,
            )
            return

        fused_applied = slot.pop("_fused_applied", False)
        # The identity the fusion pencilled onto this report, on every path: the poll
        # and the manual locate ran the fusion themselves, the push path just above.
        # Popped before sanitize/merge so it never reaches the cached row, and booked
        # only after the commit below, like the anchor intents that follow.
        fusion_report_id = slot.pop("_fusion_report_id", None)
        # F-FABLE-2: pop the supersede marker early (before sanitize/merge/commit)
        # so it never leaks into the cached row, but defer the action until AFTER
        # the payload commits (post-@580). If the significance gate below rejects
        # this payload (early return @512), the anchor is intentionally NOT
        # touched (F-CODEX-7): a discarded own-report must not invalidate it.
        supersede_anchor = bool(slot.pop("_supersede_round_trip_anchor", False))
        # Same deferral discipline for the round-trip anchor consume/seed intents
        # set mid-fusion: pop them HERE (before sanitize/merge/commit) so they never
        # leak into the cached row, and apply them only AFTER the payload commits
        # (post-@580). A payload the significance gate rejects (early return @512)
        # thus never consumes or seeds an anchor - the ordering fix (Fund A).
        anchor_seed = slot.pop("_round_trip_anchor_seed", None)
        anchor_consume = bool(slot.pop("_round_trip_anchor_consume", False))
        # Read by the accuracy gate during fusion above; must never reach the
        # cached row.
        slot.pop("_accuracy_substituted", None)

        status = slot.get("status")

        # Track fused update statistics
        if fused_applied and status == "Fused (Weighted)":
            self.increment_stat("fused_updates")

        # Report-type cooldown removed: location_poll_interval (default 300s)
        # already provides sufficient protection against excessive polling.
        # The previous 600s cooldown for crowdsourced reports caused a feedback
        # loop where replays kept extending the cooldown indefinitely.

        # Track crowd-sourced updates when hint is present
        if report_hint:
            self.increment_stat("crowd_sourced_updates")

        slot.pop("_report_hint", None)
        slot.pop("is_replayed", None)

        # Sanitize decoder row
        slot = _sanitize_decoder_row(slot)

        # Track device interval
        raw_last_seen = _normalize_epoch_seconds(slot.get("last_seen"))
        self._track_device_interval(device_id, raw_last_seen)

        # Significance check
        if not self._is_significant_update(device_id, slot):
            _LOGGER.debug(
                "Dropping cache update for %s: update failed significance checks",
                device_id,
            )
            return

        resolver_refresh_needed = False

        # Handle identity key changes
        cached_identity_key = None
        if isinstance(cached_loc, Mapping):
            cached_identity_key = self._normalize_identity_key(
                cached_loc.get("identity_key")
            )

        incoming_identity_key = self._normalize_identity_key(slot.get("identity_key"))
        if incoming_identity_key is not None:
            slot["identity_key"] = incoming_identity_key

        incoming_eik = self._normalize_identity_key(slot.get("encrypted_identity_key"))
        if incoming_eik is not None:
            slot["encrypted_identity_key"] = incoming_eik

        incoming_owner_key_version = slot.get("owner_key_version")

        identity_changed = (
            incoming_identity_key is not None
            and incoming_identity_key != cached_identity_key
        )

        cached_eik = None
        cached_owner_key_version = None
        if isinstance(cached_loc, Mapping):
            cached_eik = self._normalize_identity_key(
                cached_loc.get("encrypted_identity_key")
            )
            cached_owner_key_version = cached_loc.get("owner_key_version")

        encrypted_changed = False
        if incoming_eik is not None or incoming_owner_key_version is not None:
            encrypted_changed = (
                incoming_eik != cached_eik
                or incoming_owner_key_version != cached_owner_key_version
            )

        if identity_changed or encrypted_changed:
            resolver_refresh_needed = True
            _LOGGER.info(
                "Identity key update detected for %s (ownerKeyVersion=%s); "
                "scheduling EID resolver refresh.",
                device_id,
                incoming_owner_key_version,
            )

        # Ensure last_updated is present
        slot.setdefault("last_updated", time.time())

        # Merge with existing cache
        slot = self._merge_with_existing_cache_row(device_id, slot)

        # Keep name cache up-to-date
        name_cache_fn = getattr(self, "_ensure_device_name_cache", None)
        if callable(name_cache_fn):
            name_cache = name_cache_fn()
            name = slot.get("name")
            if isinstance(name, str) and name:
                name_cache[device_id] = name

        self._device_location_data[device_id] = slot
        # Every committed report is remembered, fused or not: a clear jump taken over
        # unaveraged overlaps itself on its next delivery and would be fused then.
        _book_fused_report(self, device_id, fusion_report_id)
        # A committed fix makes any retained coarse fix that is not newer than it
        # obsolete: the coarse row exists as side information for as long as we
        # have nothing better, and now we do. Without this it would keep naming a
        # city from an earlier report next to the fresh position for up to the
        # whole stale threshold - the reader's age rule alone cannot see that,
        # because it only knows the coarse row's own age. Producer-side on
        # purpose, so the tracker attributes and the diagnostics builder are both
        # covered by one rule rather than each growing its own.
        self._expire_coarse_fix(device_id, slot)
        # Coordinator last-good: advance only for reliable (non-estimated) fixes
        # so a sanitized accuracy-less update never overwrites the last reliable
        # position the Plus Code display accessors fall back to (non-poison).
        self._record_last_good_location(device_id, slot)

        # F-FABLE-2 (post-commit): now that the trusted own-report clear-jump has
        # been committed, invalidate any stale round-trip anchor so a later crowd
        # echo near the old anchor cannot roll the tracker back. Placed AFTER the
        # commit and the significance gate (early return @512) so a rejected
        # payload never triggers it - F-CODEX-7 semantics preserved automatically.
        if supersede_anchor and self._round_trip_confirm_enabled():
            _anchors = getattr(self, "_round_trip_anchors", None)
            if _anchors:
                _anchors.pop(device_id, None)

        # Round-trip anchor consume/seed (Fund A, ordering-vs-commit): applied here
        # POST-commit and POST-significance-gate, symmetric to the supersede pop
        # above. Consume and seed are mutually exclusive per report: the recovery
        # path leaves the fusion via ``return True`` (only sets consume), while the
        # accept path seeds and leaves via its own ``return True`` (only sets seed).
        # So at most one of the two markers is ever present on a committed payload.
        if anchor_consume and self._round_trip_confirm_enabled():
            _anchors = getattr(self, "_round_trip_anchors", None)
            if _anchors:
                _anchors.pop(device_id, None)
        if anchor_seed is not None and self._round_trip_confirm_enabled():
            # Lazy-init mirroring _record_last_good_location so a __new__-built
            # coordinator needs no extra wiring (moved here from the fusion).
            _anchors = getattr(self, "_round_trip_anchors", None)
            if _anchors is None:
                _anchors = {}
                self._round_trip_anchors = _anchors
            _anchors[device_id] = anchor_seed

        # FIX #155: Only count background_updates for non-poll sources
        # to avoid double-counting with polled_updates.
        if source != "poll":
            self.increment_stat("background_updates")

        # Register identity key and propagate to shared devices
        effective_identity_key = incoming_identity_key or cached_identity_key
        if effective_identity_key is not None:
            register_fn = getattr(self, "_register_identity_key", None)
            if callable(register_fn):
                register_fn(device_id, effective_identity_key)
            self._propagate_location_to_shared_devices(
                device_id,
                slot,
                supersede_anchors=supersede_anchor,
                seed_anchors=anchor_seed,
                fusion_report_id=fusion_report_id,
            )

        # Trigger resolver refresh if identity changed
        if resolver_refresh_needed:
            reset_fn = getattr(self, "_reset_resolver_offset", None)
            if callable(reset_fn):
                reset_fn(device_id)
            schedule_fn = getattr(self, "_schedule_eid_resolver_refresh", None)
            if callable(schedule_fn):
                schedule_fn()

    def _propagate_location_to_shared_devices(
        self,
        source_device_id: str,
        location: dict[str, Any],
        supersede_anchors: bool = False,
        seed_anchors: dict[str, Any] | None = None,
        *,
        fusion_report_id: str | None = None,
    ) -> None:
        """Propagate location updates to devices sharing the same tracker.

        When multiple accounts track the same physical device, they share
        an identity_key. This method finds all devices with the same
        identity_key and propagates location updates between them.

        Args:
            source_device_id: The device that received the update.
            location: The location data to propagate.
            supersede_anchors: When True, also drop each propagated target's
                stale round-trip anchor (F-CODEX-9), symmetric to the
                source-device pop, so a later crowd echo near the old anchor
                cannot roll a shared target back to the stale position.
            seed_anchors: When not None, also seed each adopting target's
                round-trip anchor with this (source-A) anchor as an independent
                copy, so a B-to-A return under a sibling device_id finds an
                anchor and recovers, symmetric to the source-device seed (@622).
            fusion_report_id: Identity of the report behind ``location``. Each
                adopting target remembers it, so the same report reaching that
                target itself is not fused with the propagated (already fused)
                position.
        """
        if not location:
            return

        # Get the identity key for the source device
        source_identity = self._normalize_identity_key(
            location.get("identity_key")
            or location.get("identityKey")
            or location.get("eik")
        )

        if source_identity is None:
            # Try to get from cache
            cached = self._device_location_data.get(source_device_id)
            if isinstance(cached, dict):
                source_identity = self._normalize_identity_key(
                    cached.get("identity_key")
                    or cached.get("identityKey")
                    or cached.get("eik")
                )

        if source_identity is None:
            return

        # Find devices sharing this identity key
        shared_devices = self._identity_key_to_devices.get(source_identity)
        if not shared_devices or len(shared_devices) <= 1:
            return

        # Propagate to other devices
        incoming_ts = _normalize_epoch_seconds(location.get("last_seen"))
        source_label = location.get("source_label", "unknown")

        for target_id in shared_devices:
            if target_id == source_device_id:
                continue

            target_cache = self._device_location_data.get(target_id)
            if not isinstance(target_cache, dict):
                target_cache = {}

            target_ts = _normalize_epoch_seconds(target_cache.get("last_seen"))

            # Only propagate if source is fresher
            if incoming_ts is not None and target_ts is not None:
                if incoming_ts <= target_ts:
                    continue

            # Create propagated location
            propagated = dict(location)
            propagated["_propagated_from"] = source_device_id

            # Merge with target's metadata
            merged = _merge_cache_row_impl(target_cache, propagated)
            merged["last_updated"] = time.time()

            self._device_location_data[target_id] = merged
            # Same rule as after a direct commit: this target just received a
            # newer position, so any coarse fix it retained is obsolete. The
            # propagation writes the row itself instead of going through
            # ``update_device_cache``, so the expiry has to be repeated here -
            # a sibling would otherwise keep showing an old city next to the
            # propagated position.
            self._expire_coarse_fix(target_id, merged)
            # Coordinator last-good for the shared target, same non-poison gate.
            self._record_last_good_location(target_id, merged)
            # The target now holds this report, so it remembers it like a direct
            # commit would; only targets that adopted the row get here.
            _book_fused_report(self, target_id, fusion_report_id)

            # F-CODEX-9: the superseding clear-jump was propagated to this
            # shared target, so invalidate its stale round-trip anchor too -
            # symmetric to the source-device pop after the commit. Otherwise a
            # later crowd echo near the old anchor could take the round-trip
            # recovery path and roll this propagated target back to the stale
            # position, even though the trusted location was already propagated
            # here (SA-8d data-flow breadth). Rides the exact propagation
            # data-flow: only targets that actually adopted the fresh location
            # (past the freshness gate above) are cleared; a fresher target
            # that skipped propagation keeps its own anchor.
            if supersede_anchors and self._round_trip_confirm_enabled():
                _anchors = getattr(self, "_round_trip_anchors", None)
                if _anchors:
                    _anchors.pop(target_id, None)

            # SA-8: seed and supersede are propagation-symmetric (one physical
            # tracker, one home A), so both ride this loop. The recovery CONSUME
            # pop (@611-614) is deliberately NOT propagated: a B-to-A return on
            # one sibling would otherwise delete another sibling's still-legitimate
            # A-anchor, which that sibling needs for its own return. Only seed +
            # supersede propagate. Seed and supersede are mutually exclusive per
            # report (accept-path seed vs own-report supersede), so this never
            # double-mutates a target.
            if seed_anchors is not None and self._round_trip_confirm_enabled():
                # No lazy-init needed here: a non-None seed_anchors means the
                # source post-commit seed block (@615-622, same confirm guard)
                # already ran and initialized the store before this propagation
                # call. Mirror the supersede sibling's getattr guard above; use
                # ``is not None`` (not truthiness) so an initialized-but-empty
                # store still seeds the adopting target.
                _anchors = getattr(self, "_round_trip_anchors", None)
                if _anchors is not None:
                    _anchors[target_id] = dict(seed_anchors)

            _LOGGER.debug(
                "Propagated location from %s to %s (shared tracker, source=%s)",
                source_device_id,
                target_id,
                source_label,
            )

    def _is_significant_update(
        self,
        device_id: str,
        new_data: dict[str, Any],
    ) -> bool:
        """Validate temporal ordering and data quality before committing cache updates.

        This gate rejects:
        1. Malformed payloads (not a dict)
        2. Timestamps that are too old (pre-Y2K) or too far in the future
        3. Timestamps that regress from the cached value

        Accuracy sanitization (not rejection):
        - Values < 0.001m (error code 0.0) are REMOVED from dict, not rejected
        - Valid sub-meter accuracy (e.g., 0.5m) is preserved
        - The report continues processing without precision data

        Args:
            device_id: Canonical device identifier.
            new_data: The latest location payload to evaluate.

        Returns:
            ``True`` when the caller should accept the payload.
        """
        # Use centralized constant from helpers/geo.py
        # MIN_PHYSICAL_ACCURACY_M = 0.001m (only error code 0.0 is filtered)

        if not isinstance(new_data, dict):
            _LOGGER.debug("Rejecting update for %s: payload is not a dict", device_id)
            return False

        # Sanitize error code accuracy values (0.0 in Android Location API).
        # The API uses 0.0 as "no accuracy available", not "perfect precision".
        # We ACCEPT the update and REPLACE invalid accuracy with a conservative fallback.
        # This ensures Home Assistant always gets a valid numeric gps_accuracy attribute.
        # Valid sub-meter values like 0.5m or 0.01m are preserved!
        # ``accuracy_estimated`` is a producer-side flag: True whenever the
        # accuracy below was replaced by the conservative fallback radius, False
        # when a real measurement survived. Only the producer can tell the two
        # apart, because once the value is the 200m fallback it is byte-for-byte
        # indistinguishable from a real 200m fix downstream (map_view, recorder).
        new_acc = new_data.get("accuracy")
        if new_acc is None:
            # Missing accuracy: set conservative fallback for HA state machine
            new_data["accuracy"] = DEFAULT_ACCURACY_FALLBACK_M
            new_data["accuracy_estimated"] = True
            self.increment_stat("accuracy_sanitized_count")
            _LOGGER.debug(
                "Setting fallback accuracy for %s: key was missing, using %sm",
                device_id,
                DEFAULT_ACCURACY_FALLBACK_M,
            )
        else:
            try:
                acc_f = float(new_acc)
                if not math.isfinite(acc_f) or acc_f < MIN_PHYSICAL_ACCURACY_M:
                    # Sanitize: replace error code with conservative fallback
                    new_data["accuracy"] = DEFAULT_ACCURACY_FALLBACK_M
                    new_data["accuracy_estimated"] = True
                    self.increment_stat("accuracy_sanitized_count")
                    _LOGGER.debug(
                        "Sanitizing update for %s: error code accuracy (%s) -> %sm",
                        device_id,
                        new_acc,
                        DEFAULT_ACCURACY_FALLBACK_M,
                    )
                    # Continue processing - the update is valid, with fallback precision
                # Numerically valid accuracy. Mark it as a non-estimated fix
                # ONLY when the producer did not already flag it estimated. A
                # preserved 200m fallback is numerically valid yet carries
                # accuracy_estimated=True from upstream; clobbering it to False
                # here would draw a solid "real measurement" circle for a
                # fallback radius. A carried True therefore wins; otherwise the
                # surviving real measurement is recorded as not estimated.
                elif new_data.get("accuracy_estimated") is not True:
                    new_data["accuracy_estimated"] = False
            except (TypeError, ValueError):
                # Non-numeric accuracy: replace with fallback
                new_data["accuracy"] = DEFAULT_ACCURACY_FALLBACK_M
                new_data["accuracy_estimated"] = True
                self.increment_stat("accuracy_sanitized_count")
                _LOGGER.debug(
                    "Sanitizing update for %s: non-numeric accuracy (%r) -> %sm",
                    device_id,
                    new_acc,
                    DEFAULT_ACCURACY_FALLBACK_M,
                )

        n_seen_norm = _normalize_epoch_seconds(new_data.get("last_seen"))
        if n_seen_norm is not None:
            if n_seen_norm < _Y2K_EPOCH_SECONDS:
                self.increment_stat("invalid_ts_drop_count")
                self.increment_stat("drop_reason_invalid_ts")
                # Corrupt (pre-Y2K) timestamp: alarming sub-bucket. Increment
                # only (cumulative), unlike the canonicless absolute assign.
                self.increment_stat("invalid_ts_drop_warn")
                _LOGGER.debug(
                    "Rejecting update for %s: timestamp too old (%s)",
                    device_id,
                    n_seen_norm,
                )
                return False
            if n_seen_norm > time.time() + MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S:
                self.increment_stat("future_ts_drop_count")
                _LOGGER.debug(
                    "Rejecting update for %s: timestamp too far in future (%s)",
                    device_id,
                    n_seen_norm,
                )
                return False

        existing = self._device_location_data.get(device_id)
        if not existing:
            return True

        e_seen_norm = _normalize_epoch_seconds(existing.get("last_seen"))
        if (
            n_seen_norm is not None
            and e_seen_norm is not None
            and n_seen_norm < e_seen_norm
        ):
            self.increment_stat("invalid_ts_drop_count")
            self.increment_stat("drop_reason_invalid_ts")
            # Regressed/out-of-order timestamp: benign sub-bucket. Increment
            # only (cumulative), unlike the canonicless absolute assign.
            self.increment_stat("invalid_ts_drop_benign")
            _LOGGER.debug(
                "Rejecting update for %s: timestamp regressed (%s < %s)",
                device_id,
                n_seen_norm,
                e_seen_norm,
            )
            return False

        return True

    def _merge_with_existing_cache_row(
        self,
        device_id: str,
        incoming: dict[str, Any],
    ) -> dict[str, Any]:
        """Merge incoming location data with existing cache entry.

        This method preserves important fields from the existing cache
        while updating with new data. It handles:
        - Coordinate preservation when incoming is semantic-only
        - Metadata field preservation
        - Timestamp monotonicity

        Args:
            device_id: Device identifier.
            incoming: New location data.

        Returns:
            Merged cache entry.
        """
        existing = self._device_location_data.get(device_id)
        if not isinstance(existing, dict):
            return dict(incoming)

        # Use the helper for core merge logic
        merged = _merge_cache_row_impl(existing, incoming)

        return merged

    def _haversine_distance(
        self,
        lat1: float,
        lon1: float,
        lat2: float,
        lon2: float,
    ) -> float:
        """Calculate the great-circle distance between two points (meters)."""
        return _haversine_distance_impl(lat1, lon1, lat2, lon2)

    def _speed_gate_enabled(self) -> bool:
        """Return whether the kinematic speed gate is active (Discussion #177)."""
        entry = getattr(self, "config_entry", None)
        if entry is None or getattr(entry, "options", None) is None:
            return DEFAULT_SPEED_GATE_ENABLED
        return bool(
            entry.options.get(OPT_SPEED_GATE_ENABLED, DEFAULT_SPEED_GATE_ENABLED)
        )

    def _round_trip_confirm_enabled(self) -> bool:
        """Return whether the round-trip escape hatch is active (Q2-A, #177)."""
        entry = getattr(self, "config_entry", None)
        if entry is None or getattr(entry, "options", None) is None:
            return DEFAULT_ROUNDTRIP_CONFIRM
        return bool(entry.options.get(OPT_ROUNDTRIP_CONFIRM, DEFAULT_ROUNDTRIP_CONFIRM))

    def _accuracy_gate_enabled(self) -> bool:
        """Return whether the comparative accuracy gate is active (#216)."""
        entry = getattr(self, "config_entry", None)
        if entry is None or getattr(entry, "options", None) is None:
            return DEFAULT_ACCURACY_GATE_ENABLED
        return bool(
            entry.options.get(OPT_ACCURACY_GATE_ENABLED, DEFAULT_ACCURACY_GATE_ENABLED)
        )

    def _apply_weighted_location_fusion(
        self,
        device_id: str,
        new_data: dict[str, Any],
    ) -> bool:
        """Fuse overlapping locations while honoring semantic anchors.

        The fusion pipeline keeps semantic locations authoritative and blends
        overlapping sensor fixes to minimize jitter:
        1. Cold starts accept any payload because there is no baseline.
        2. Trusted (semantic) updates always win immediately.
        3. When the cached location is trusted, overlapping sensor fixes snap
           back to the anchor instead of drifting.
        4. Standard sensor fixes are fused with inverse-square weighting when
           their accuracy circles overlap; clear jumps simply pass through.

        Returns True when the caller should continue processing the payload.
        """
        # Identify the report as it arrived, before any branch below rewrites its
        # coordinates in place. Set on every path, cold start included, because
        # every committed report is remembered (see ``update_device_cache``).
        report_id = _fusion_report_identity(new_data)
        new_data["_fusion_report_id"] = report_id

        existing = self._device_location_data.get(device_id)
        if not existing or existing.get("latitude") is None:
            return True

        new_lat = _coerce_float_impl(new_data.get("latitude"))
        new_lon = _coerce_float_impl(new_data.get("longitude"))
        if new_lat is None or new_lon is None:
            return True

        existing_lat = _coerce_float_impl(existing.get("latitude"))
        existing_lon = _coerce_float_impl(existing.get("longitude"))
        if existing_lat is None or existing_lon is None:
            return True

        # The same report again: the cached position already contains it, so
        # averaging it in once more would move the position towards the raw fix
        # without a new location. Keep the cached geometry and let the rest of
        # the report (battery, height, metadata) through. Before the trusted and
        # gate branches, because a remembered report has passed them already.
        if _is_fused_report(self, device_id, report_id):
            for key in _DUPLICATE_GEOMETRY_KEYS:
                if key in existing:
                    new_data[key] = existing[key]
                else:
                    new_data.pop(key, None)
            # Against a trusted anchor the first delivery was snapped back, height
            # included (branch below); the repeat keeps that height as well, or it
            # would undo half of the snap without a new location.
            if existing.get("location_type") == "trusted" and (
                existing.get("altitude") is not None
            ):
                new_data["altitude"] = existing["altitude"]
            _LOGGER.debug("Skipping re-fusion for %s: report already fused", device_id)
            return True

        existing_acc_raw = _coerce_float_impl(existing.get("accuracy"))
        new_acc_raw = _coerce_float_impl(new_data.get("accuracy"))

        def _safe_accuracy(value: float | None) -> float:
            """Convert raw accuracy to a safe value for fusion calculations.

            The Android Location API uses 0.0 as an error code meaning "no accuracy".
            We treat values < MIN_VALID_ACCURACY (0.001m) as this error code.

            Modern dual-frequency GNSS (L1+L5) can achieve sub-meter accuracy under
            ideal conditions, so valid values like 0.5m or 0.01m are preserved.

            The fallback (PRIVACY_ACCURACY_FALLBACK = 200m) is based on Bluetooth
            tracker physics (max Bluetooth range + GPS error margin). This ensures:
              - Error codes get lower weight in fusion than real GPS (200²/20² = 100x)
              - The fallback is still useful for finding a tracker (unlike 2km)

            SELF-HEALING: If the cache contains a corrupted value (0.0),
            treating it as 200m allows new valid data to properly override it.
            """
            if value is None or not math.isfinite(value):
                return DEFAULT_ACCURACY_FALLBACK_M
            # < MIN_VALID_ACCURACY (0.001m) is the error code 0.0
            # This also handles self-healing of corrupted cache entries
            if value < MIN_PHYSICAL_ACCURACY_M:
                return DEFAULT_ACCURACY_FALLBACK_M
            return value

        existing_acc = _safe_accuracy(existing_acc_raw)
        new_acc = _safe_accuracy(new_acc_raw)

        try:
            dist = _haversine_distance_impl(
                existing_lat, existing_lon, new_lat, new_lon
            )
        except Exception:
            return True

        radius_sum = existing_acc + new_acc

        # Incoming trusted update always wins
        if new_data.get("location_type") == "trusted":
            return True

        # Existing trusted anchor: snap back if overlapping
        if existing.get("location_type") == "trusted":
            if dist <= radius_sum:
                new_data["latitude"] = existing_lat
                new_data["longitude"] = existing_lon
                if existing_acc_raw is not None:
                    new_data["accuracy"] = existing_acc_raw
                if existing.get("altitude") is not None:
                    new_data["altitude"] = existing["altitude"]
                new_data["location_type"] = "trusted"
                new_data["status"] = "Stationary (at Anchor)"
                return True
            # Non-overlapping against a trusted anchor: the accuracy gate applies
            # here too (#216). The placement rationale F3 below, which keeps the
            # SPEED gate out of this branch, does not transfer: it rests on a
            # semantic anchor having no kinematics, and this gate has none - it
            # compares radii. Leaving the branch exempt would have exempted the
            # single best cached reference the gate exists to protect, so a fresh
            # 50 m anchor could still be displaced by a 1600 m fix, which is the
            # false zone transition this whole change is about. The speed gate
            # stays out; only the comparative accuracy check runs.
            if _accuracy_gate_rejects(
                self,
                device_id,
                new_data,
                existing,
                _GateMetrics(new_acc, new_acc_raw, existing_acc, dist),
            ):
                return False
            return True

        # FIX #155: When BOTH sides carry the fallback accuracy, fusion
        # degenerates to a meaningless 50/50 average that injects jitter.
        # Accept the newer data as-is instead of blending.
        # Placed after trusted-anchor checks so anchored devices stay pinned.
        if (
            existing_acc == DEFAULT_ACCURACY_FALLBACK_M
            and new_acc == DEFAULT_ACCURACY_FALLBACK_M
        ):
            return True

        # Clear jump - no overlap. Before accepting, apply the kinematic
        # plausibility gate (Discussion #177): a single FMDN crowd report can
        # land far away with huge uncertainty ("teleport"). Reject a far jump
        # whose implied speed is physically impossible. Falls through to accept
        # when speed is not computable (missing/degenerate timestamps) so no
        # regression occurs. Own-report GPS fixes bypass the gate entirely: they
        # are cryptographically trusted device positions that do not teleport,
        # so gating them would only risk false-blocking genuine fast travel.
        # Placement rationale (F3): the gate sits AFTER the trusted-anchor and
        # #155 double-fallback branches on purpose - those return earlier because
        # a semantic anchor has no kinematics and two 200m fallback circles give
        # radii too unreliable to gate on (would risk false-blocks, F4-a).
        # dt limit (F5): after a long offline gap dt is huge, implied_speed tiny,
        # so the gate never fires - intended (slow travel over long time is
        # plausible); a post-offline teleport passing ungated is the inherent
        # limit of a forward speed gate (Q2 round-trip gate would catch it).
        if dist > radius_sum:
            # Own-report bypass keyed off CRYPTOGRAPHIC provenance, not the raw
            # server flag: a network/foreign report can carry a spurious
            # server-supplied is_own_report=True (this integration's own uploader
            # stamps network reports that way, decrypt_locations.py:1991-1998).
            # The decrypt layer already hardens is_own_report = is_own_report and
            # not is_network_report (decrypted_location.py:39); mirror that
            # invariant here so a spoofed own-flag on a network teleport cannot
            # skip the gate on any path. Absent is_network_report is treated as
            # not-network (genuine own reports never carry the network flag).
            incoming_is_own = bool(new_data.get("is_own_report")) and not bool(
                new_data.get("is_network_report")
            )
            # Round-trip anchor supersede (F-FABLE-2): a trusted own-report that
            # is itself a clear jump (dist > radius_sum, this branch) away from
            # the previous position is a genuine relocation B->C. An anchor A
            # that survived from an earlier gated crowd jump A->B is now stale -
            # a later crowd echo near A must not roll the tracker off the trusted
            # current position back to A. Mark the slot so the anchor is
            # invalidated AFTER this payload commits (see supersede handling in
            # update_device_cache): an own-report bypasses the gate below
            # (not incoming_is_own) and never seeds/consumes, so a surviving
            # anchor is strictly pre-existing. DF-A4: keyed on the clear-jump
            # own-report only - an own-report near B never reaches this branch,
            # so the legitimate B->A return stays protected.
            # Known gap (F-3, TTL/radius-bounded): the trusted-return accept
            # paths above (trusted new_data, trusted existing, #155 dual 200m
            # fallback) return before this branch and never set the marker, so
            # a stale anchor can survive on those paths too. Deferred, not fixed
            # here (scope discipline at this iterated locus); a later crowd echo
            # is still TTL- and radius-bounded.
            # F-CODEX-9: the marker is set unconditionally for an own clear-jump,
            # NOT only when THIS source device holds an anchor. A shared tracker
            # propagates this fresh trusted position to sibling device ids (same
            # identity_key) post-commit, and a sibling may hold a stale anchor
            # even when the source does not. The post-commit handling pops the
            # source anchor AND every propagated target's anchor; all pops are
            # idempotent (pop(key, None)), so marking without a source anchor is a
            # harmless no-op on the source side while still clearing the siblings.
            if incoming_is_own and self._round_trip_confirm_enabled():
                new_data["_supersede_round_trip_anchor"] = True
            # Only gate a fix that carries a REAL measured accuracy. A crowd
            # report is "accuracy-less" not only when the key is missing
            # (new_acc_raw is None) but also when it carries Android's
            # no-accuracy sentinel (0.0) or any sub-physical/non-finite value
            # (< MIN_PHYSICAL_ACCURACY_M) - the same error-code set that
            # _safe_accuracy() maps to the 200m fallback. Such a fix is not a
            # clean teleport discriminant and must fall through so the
            # downstream sanitization in _is_significant_update can flag it
            # estimated (accuracy_estimated=True). Hard-dropping it here would
            # regress the #1181 last-good protection; that path is already
            # guarded by is_reliable_fix, not by this gate.
            new_acc_measured = (
                new_acc_raw is not None
                and math.isfinite(new_acc_raw)
                and new_acc_raw >= MIN_PHYSICAL_ACCURACY_M
            )
            # Symmetric cached-endpoint guard (F-CODEX-1): also require the
            # cached point ``existing`` to be a reliable fix. If it is an
            # estimated/accuracy-less row (the 200 m fallback carries
            # accuracy_estimated=True), implied_speed would be measured against
            # an unreliable reference and reject the incoming report against a
            # phantom position, stranding the tracker on the estimate. Mirrors
            # the incoming-side new_acc_measured check so the gate fires only when
            # BOTH endpoints carry real measured accuracy; is_reliable_fix is the
            # same predicate that guards anchor seeding below (@1125).
            # Trade-off (F-2): when ``existing`` is estimated, the ENTIRE gate
            # block below (incl. round-trip recovery, housekeeping and reject) is
            # skipped, so a measured crowd teleport is accepted ungated for that
            # one step. This is deliberate - gating against a phantom reference is
            # the worse failure (F-CODEX-1 stranding). Follow-up candidate: gate
            # against the last-good location instead of skipping entirely.
            if (
                self._speed_gate_enabled()
                and not incoming_is_own
                and new_acc_measured
                and is_reliable_fix(existing)
            ):
                existing_ts = _normalize_epoch_seconds(existing.get("last_seen"))
                new_ts = _normalize_epoch_seconds(new_data.get("last_seen"))
                if existing_ts is not None and new_ts is not None:
                    delta_t = new_ts - existing_ts
                    if delta_t > 0:
                        implied_speed = dist / delta_t
                        if implied_speed > DEFAULT_MAX_PLAUSIBLE_SPEED_MPS:
                            # Round-trip escape hatch (Q2-A, F-CODEX-4): the gate
                            # is about to reject this jump. If the device is
                            # returning near a remembered anchor A within the TTL,
                            # accept it instead - this is a legitimate return trip
                            # after an earlier accepted wide jump, not a teleport,
                            # and rejecting it would strand the tracker at the
                            # stale position. new_ts is guaranteed non-None here
                            # (the enclosing guard requires it).
                            # Anchor store is read defensively (getattr) mirroring
                            # the _record_last_good_location idiom, so coordinators
                            # built via __new__ (or partial test doubles) need no
                            # extra wiring; a real coordinator always has it (init).
                            if self._round_trip_confirm_enabled():
                                anchors = getattr(self, "_round_trip_anchors", None)
                                anchor = anchors.get(device_id) if anchors else None
                                if anchors and anchor is not None:
                                    delta_anchor = new_ts - anchor["ts"]
                                    return_dist = _haversine_distance_impl(
                                        anchor["lat"],
                                        anchor["lon"],
                                        new_lat,
                                        new_lon,
                                    )
                                    if (
                                        0 <= delta_anchor <= ROUND_TRIP_TTL_S
                                        and return_dist <= ROUND_TRIP_ANCHOR_RADIUS_M
                                    ):
                                        # One-shot: consume the anchor so a stale
                                        # echo near A cannot re-trigger recovery
                                        # (A<->B ping-pong bollwerk, DF-1). The
                                        # recovered fix leaves via THIS return, not
                                        # the accept-path return below, so it never
                                        # sets a new anchor of its own.
                                        # F-CODEX-7/ordering: defer the store
                                        # mutation. Mutating _round_trip_anchors here
                                        # (mid-fusion) consumes the anchor BEFORE the
                                        # significance gate/commit; a payload the
                                        # gate later rejects (early return @512) would
                                        # then have burnt the anchor. Mark the intent
                                        # on new_data and pop the anchor only AFTER
                                        # the commit (mirrors _supersede handling).
                                        # DECLARED LIMIT (#216): this return
                                        # leaves the fusion above the accuracy
                                        # gate, so a recovered fix is published
                                        # with whatever radius it carries - a
                                        # coarse one included. That is deliberate
                                        # and not an oversight: the recovery has
                                        # an INDEPENDENT confirmation of the
                                        # place (the fix landed within
                                        # ROUND_TRIP_ANCHOR_RADIUS_M of a
                                        # remembered anchor), which is exactly
                                        # what the accuracy gate lacks and
                                        # substitutes with a comparison. Gating
                                        # here would also burn the one-shot
                                        # anchor for nothing and strand the
                                        # tracker at the far position - the
                                        # failure this hatch exists to prevent.
                                        # Pinned by
                                        # test_round_trip_recovery_outranks_the_accuracy_gate.
                                        new_data["_round_trip_anchor_consume"] = True
                                        self.increment_stat("round_trip_recoveries")
                                        _LOGGER.debug(
                                            "Round-trip recovery %s: returned "
                                            "%.0fm from anchor within %.0fs "
                                            "(<= %.0fs TTL), speed gate bypassed",
                                            device_id,
                                            return_dist,
                                            delta_anchor,
                                            ROUND_TRIP_TTL_S,
                                        )
                                        return True
                                    # House-keep the anchor ONLY on a genuine TTL
                                    # expiry (delta_anchor > ROUND_TRIP_TTL_S); the
                                    # gate rejects as usual. Two non-expiry cases
                                    # deliberately LEAVE the anchor in place so a
                                    # later genuine B->A return within the original
                                    # window still recovers:
                                    #  - a within-TTL far reject (return_dist >
                                    #    ROUND_TRIP_ANCHOR_RADIUS_M): an unrelated
                                    #    noisy crowd fix between the trip legs must
                                    #    not burn the still-valid anchor.
                                    #  - an out-of-order report (delta_anchor < 0):
                                    #    a delayed fix near A whose last_seen sits
                                    #    between A and B is not an expiry.
                                    # The single "> TTL" test subsumes the old
                                    # F-FABLE-1 ">= 0" guard: every negative
                                    # delta_anchor is <= TTL, so it never pops.
                                    if delta_anchor > ROUND_TRIP_TTL_S:
                                        anchors.pop(device_id, None)
                            self.increment_stat("speed_gate_rejects")
                            _LOGGER.debug(
                                "Speed gate rejected crowd fix %s: %.0fm in "
                                "%.0fs = %.1fm/s > %.1fm/s cap",
                                device_id,
                                dist,
                                delta_t,
                                implied_speed,
                                DEFAULT_MAX_PLAUSIBLE_SPEED_MPS,
                            )
                            return False
                        # Accept path: this jump is non-own, carries a real
                        # measured accuracy, is timestamped and forward
                        # (delta_t > 0), and the speed gate evaluated it as
                        # plausible (implied_speed <= cap). Only such a
                        # gate-vetted jump may seed a return anchor (F-CODEX-6):
                        # seeding lives INSIDE the gated block so it shares the
                        # consume branch's exact provenance. An own-report or
                        # accuracy-less jump bypasses the gate entirely and must
                        # never seed an anchor that a later stale crowd report
                        # near A could ride back to (that would roll the tracker
                        # off a trusted, current B position). The anchor is set
                        # ONLY from a reliable A: an estimated/accuracy-less A
                        # carries the 200 m fallback with accuracy_estimated=True
                        # and must never become a phantom return target (R2, same
                        # poisoning class _record_last_good already guards).
                        # RAM-only (R7). DF-1: this is the sole anchor-set site
                        # and a recovered return fix exits earlier (return True
                        # above), so it can never seed - the ping-pong bollwerk.
                        # DF-2: the incoming fix is intentionally NOT re-checked
                        # against is_reliable_fix here; is_reliable_fix guards
                        # only the anchor source A, not the returning fix.
                        #
                        # Anchor TTL clock (F-CODEX-5): the anchor's ``ts`` is
                        # ``new_ts`` - the incoming B fix's last_seen, i.e. the
                        # time this A->B jump is accepted, NOT A's last_seen. The
                        # recovery check compares the return fix's new_ts against
                        # this ts (both in report-timestamp space), so anchoring
                        # on A's last_seen would start the TTL in the past:
                        # exactly the wide jumps this hatch targets are accepted
                        # because delta_t = new_ts(B) - last_seen(A) is large,
                        # and once that delta exceeds the TTL (the stale/offline
                        # device coming back online) the anchor would be born
                        # expired and never recover. The anchored COORDINATES
                        # stay A's (the return target).
                        if self._round_trip_confirm_enabled() and is_reliable_fix(
                            existing
                        ):
                            # F-CODEX-7/ordering: defer the store mutation. Seeding
                            # the anchor here (mid-fusion) writes _round_trip_anchors
                            # BEFORE the significance gate/commit; a payload the gate
                            # later rejects (early return @512) would then leave a
                            # phantom anchor behind. Mark the intent on new_data and
                            # let update_device_cache lazy-init the store and seed the
                            # anchor only AFTER the commit (mirrors _supersede). The
                            # anchored COORDINATES stay A's (existing_lat/lon); ts is
                            # new_ts per the F-CODEX-5 TTL-clock reasoning above.
                            new_data["_round_trip_anchor_seed"] = {
                                "lat": existing_lat,
                                "lon": existing_lon,
                                "ts": new_ts,
                            }

            # Comparative accuracy gate (#216, and the core of #211). The speed
            # gate above asks whether the jump is physically possible; this one
            # asks whether the incoming fix is good enough to displace what we
            # already have. It runs only where the circles do NOT overlap -
            # here and, since the trusted-anchor branch above, there as well -
            # because only an overlap-free pair is a real displacement: where
            # they overlap the inverse-square weighting already pulls a coarse
            # fix down to near-zero influence (200**2/20**2 = 100x less weight).
            #
            # Rule, rationale and the deliberate absence of an own-report
            # bypass live in ``_accuracy_gate_rejects``; not restated here, so
            # the two call sites cannot document it differently.
            if _accuracy_gate_rejects(
                self,
                device_id,
                new_data,
                existing,
                _GateMetrics(new_acc, new_acc_raw, existing_acc, dist),
            ):
                return False
            return True

        # Overlapping accuracy circles: fuse with inverse-square weighting
        # Use MIN_PHYSICAL_ACCURACY_M (0.001m) as floor to preserve sub-meter weight
        # and prevent division by zero. Valid sub-meter accuracy gets proper weight.
        w_old = 1 / (max(MIN_PHYSICAL_ACCURACY_M, existing_acc) ** 2)
        w_new = 1 / (max(MIN_PHYSICAL_ACCURACY_M, new_acc) ** 2)
        total_w = w_old + w_new
        if total_w == 0:
            return True

        lat_fused = (existing_lat * w_old + new_lat * w_new) / total_w
        lon_fused = (existing_lon * w_old + new_lon * w_new) / total_w

        new_data["latitude"] = lat_fused
        new_data["longitude"] = lon_fused

        # Calculate fused accuracy using inverse variance weighting.
        #
        # When fusing two measurements with variances σ₁² and σ₂², the optimal
        # combined variance is: σ_fused² = 1 / (1/σ₁² + 1/σ₂²) = 1 / total_w
        #
        # Since accuracy ≈ standard deviation, the fused accuracy is:
        #   accuracy_fused = sqrt(1 / total_w)
        #
        # This is statistically correct: combining two independent measurements
        # should yield a MORE precise result than either alone.
        #
        # Safety bounds:
        # - MIN_FUSED_ACCURACY_M: Physical limit (consumer GPS can't do better)
        # - limit_best: Never claim worse than the best input (conservative)

        MIN_FUSED_ACCURACY_M = 5.0  # Consumer GPS floor

        # NOTE: This predicate intentionally differs from the canonical
        # ``coordinator.helpers.geo.is_valid_accuracy`` and must NOT be
        # consolidated with it. geo uses ``>= MIN_VALID_ACCURACY`` (0.001m),
        # whereas fusion accepts any strictly positive accuracy (``> 0``).
        # For a sub-millimeter input in the open interval (0, 0.001) the two
        # disagree: this predicate keeps it as a valid measurement (driving the
        # both-valid inverse-variance branch), while geo would reject it and
        # collapse fusion to the single valid side. That changes the fused
        # accuracy (characterization: tests/test_cache_fusion_validity.py pins
        # ~11.978m here versus 12.0m under geo). The divergence is deliberate:
        # fusion treats a sub-mm fix as a real, high-weight measurement rather
        # than the 0.0 error code geo guards against.
        def _is_valid_accuracy(acc: float | None) -> bool:
            return (
                acc is not None
                and isinstance(acc, (int, float))
                and math.isfinite(acc)
                and acc > 0
            )

        valid_existing = _is_valid_accuracy(existing_acc_raw)
        valid_new = _is_valid_accuracy(new_acc_raw)

        best_accuracy: float | None
        if valid_existing and valid_new:
            # Both valid: use inverse variance formula
            # accuracy_fused = sqrt(1 / total_w) where total_w = 1/acc₁² + 1/acc₂²
            fused_accuracy = math.sqrt(1.0 / total_w)

            # Safety bounds:
            # - Never claim better than physics allows (MIN_FUSED_ACCURACY_M)
            # - Never claim worse than the best individual measurement
            # Note: assert helps mypy understand values are not None after validation
            assert existing_acc_raw is not None and new_acc_raw is not None
            limit_best = min(existing_acc_raw, new_acc_raw)
            best_accuracy = max(fused_accuracy, limit_best, MIN_FUSED_ACCURACY_M)
        elif valid_new:
            best_accuracy = new_acc_raw
        elif valid_existing:
            best_accuracy = existing_acc_raw
        else:
            # Neither is valid - use conservative fallback
            # This ensures Home Assistant always gets a valid gps_accuracy attribute
            best_accuracy = DEFAULT_ACCURACY_FALLBACK_M

        # ALWAYS write back a valid accuracy - never leave it as None or missing
        # Home Assistant requires a numeric gps_accuracy for the state machine
        new_data["accuracy"] = best_accuracy
        # Mirror the producer flag for the fused result: False only when at least
        # one input was a REAL measurement. An input counts as real when it is
        # numerically valid AND not itself flagged estimated. A numerically valid
        # value is not automatically a real measurement: a preserved 200m fallback
        # is valid (>0) yet carries accuracy_estimated=True, so it must not flip the
        # fused result to "real". The existing flag is read from the cached row and
        # the new flag from new_data. Deriving from input validity (not from
        # value-equality with the fallback) still avoids mislabeling a genuinely
        # fused measurement that happens to equal the 200m fallback value. This
        # assignment remains a guard: the significance gate (_is_significant_update)
        # is the authoritative writer downstream.
        real_existing = valid_existing and not existing.get("accuracy_estimated")
        real_new = valid_new and not new_data.get("accuracy_estimated")
        new_data["accuracy_estimated"] = not (real_existing or real_new)

        new_data["status"] = "Fused (Weighted)"
        new_data["_fused_applied"] = True
        return True
