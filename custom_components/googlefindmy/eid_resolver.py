# custom_components/googlefindmy/eid_resolver.py
"""Ephemeral identifier resolver for local BLE scans.

The resolver precomputes ephemeral identifiers (EIDs) for known trackers using
the FHNA/FMDN Table 10 PRF helpers and maps BLE scan payloads to Home Assistant
device registry identifiers. Heavy cryptography and key-unwrapping happen
outside the hot path; ``resolve_eid`` only performs lookups.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Coroutine, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    NamedTuple,
    Protocol,
    cast,
    runtime_checkable,
)

from homeassistant.core import CALLBACK_TYPE, HomeAssistant
from homeassistant.helpers.event import async_call_later, async_track_time_interval
from homeassistant.helpers.storage import Store
from homeassistant.util import dt as dt_util

from .Auth.username_provider import username_string
from .const import DOMAIN
from .coordinator import DeviceIdentity, GoogleFindMyCoordinator
from .coordinator.helpers.identity import normalize_component_count
from .FMDNCrypto._lazy_crypto import get_aesgcm_class, get_invalid_tag_exception
from .FMDNCrypto.curve_profile import SECP160R1, SECP256R1, ScalarDerivation
from .FMDNCrypto.eid_generator import (
    FHNA_COUNTER_MASK,
    LEGACY_EID_LENGTH,
    MODERN_EID_LENGTH,
    P256_ORDER,
    ROTATION_PERIOD,
    ROTATION_PERIOD_900,
    ROTATION_PERIOD_3600,
    VARIANT_DERIVATIONS,
    EidVariant,
    HeuristicBasis,
    HeuristicEidResult,
    compute_flags_xor_mask,
    generate_eid_variant,
    generate_heuristic_eid,
)
from .FMDNCrypto.mcu_utils import flip_bits, is_mcu_tracker
from .KeyBackup.cloud_key_decryptor import decrypt_eik
from .KeyBackup.shared_key_retrieval import async_get_shared_key
from .NovaApi.ExecuteAction.LocateTracker.foreign_reading_tracker import (
    FOREIGN_READING_TRACKER,
)
from .SpotApi.GetEidInfoForE2eeDevices.get_owner_key import (
    OwnerKeyInfo,
    async_get_owner_key,
)

if TYPE_CHECKING:
    from custom_components.googlefindmy.Auth.token_cache import TokenCache
else:
    try:
        from custom_components.googlefindmy.Auth.token_cache import TokenCache
    except Exception:  # pragma: no cover - optional type aid only
        TokenCache = None

_LOGGER = logging.getLogger(__name__)

STORAGE_VERSION = 1
STORAGE_KEY = f"{DOMAIN}_eid_locks"

FMDN_FRAME_TYPE = 0x40
MODERN_FRAME_TYPE = 0x41
RAW_HEADER_LENGTH = 1
SERVICE_DATA_OFFSET = 8
AESGCM_NONCE_LENGTH = 12

# FMDN hashed_flags byte: bit-extraction constants for the XOR-decoded byte.
# Why these constants exist: the FMDN spec numbers bits MSB-first while
# Python uses LSB-first arithmetic — a naive translation flips the
# battery and UWT fields silently. The constants make the mapping explicit
# and grep-stable; the canonical spec quote, bit-layout table and the
# anti-pattern warning live at the decode site (see
# GoogleFindMyEIDResolver._update_ble_battery — CANONICAL SPEC ANCHOR).
# Reference: https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn#hashed_flags
# (retrieved 2026-06-02). Drift between code and spec is guarded by
# tests/test_ble_battery_sensor.py::TestFmdnHashedFlagsConstantsMatchSpec.
FMDN_HASHED_FLAGS_BATTERY_MASK = 0x03  # 2-bit field after right-shift
FMDN_HASHED_FLAGS_BATTERY_SHIFT = 1
FMDN_HASHED_FLAGS_UWT_MODE_MASK = 0x01  # 1-bit field, standard LSB

# Parameters for compute_flags_xor_mask(), keyed by EidVariant:
# (curve_byte_len, curve_order, derivation). Derived from VARIANT_DERIVATIONS,
# so the mask uses the same scalar as the EID of its variant; truncated
# variants keep the full coordinate length of their curve. A plain dict so a
# test can remove a row and observe the loud failure in locked_curve_name.
_VARIANT_CURVE_PARAMS: dict[EidVariant, tuple[int, int, ScalarDerivation]] = {
    variant: (curve.coord_len, curve.order, derivation)
    for variant, (curve, derivation) in VARIANT_DERIVATIONS.items()
}

# Per-variant crypto memoization (AP-A).
# Both EID derivation (generate_eid_variant) and the flags XOR mask
# (compute_flags_xor_mask) are pure functions of their inputs and run once per
# (key, time_counter, variant/curve) tuple on every rotation refresh. A bounded
# LRU per resolver instance turns the repeated within-rotation and
# back-to-back-refresh recomputation into a dict lookup while staying byte-exact.
#
# functools.lru_cache is deliberately NOT used: as a method decorator it keys on
# ``self`` (and pins it), leaking the resolver and preventing per-instance caches.
# The minimal OrderedDict+Lock helper below keys only on the crypto inputs.
#
# Both bounds are sized so that one lookup build of 13 unlocked devices fits,
# with headroom, rounded up to the next power of two. The sizing case is an
# unlocked device with a single time anchor that is younger than about 237
# days: it spans 13 time windows per build, giving 13 x 7 variants = 91 EID
# keys and 13 x 4 mask keys = 52 mask keys (the mask key carries curve and
# scalar derivation, and the seven variants use four distinct pairs). 13 such
# devices need 1183 EID keys and 676 mask keys.
# Older devices span more windows (two more per further ~237 days of drift
# allowance, see _compute_relative_windows), and a device with both pair_date
# and secrets_creation_date gets a window set per anchor. For them these bounds
# are only the floor: each build sizes both memos to its own key count (see
# _MEMO_HEADROOM below). tests/test_eid_resolver_memoization.py measures the
# per-device key counts of the sizing case and fails if a new variant or
# derivation outgrows these floors.
_EID_MEMO_MAXSIZE = 2048
_EID_MASK_MEMO_MAXSIZE = 1024

# Per-build sizing. The key count grows with device age and anchors: one device
# with pair_date and secrets_creation_date five years back spans 54 windows per
# build (378 EID keys, 216 mask keys), ten years back 86 (602 and 344). For the
# relative bases the window count is only capped by max_window (85 per side and
# basis), reached after decades; the absolute Unix basis, off by default, spans
# at least MIN_UNIX_WINDOW_SIZE windows per side without that cap. Fixed bounds
# for the cap would size the memos for devices decades old, so
# _build_lookup_sync counts the exact keys of the build and sets each memo to
# _MEMO_HEADROOM times that count, never below the bounds above and never above
# the hard caps below. It grows a memo before generating and shrinks it only
# afterwards, when the least recently used entries are the ones this build did
# not touch; shrinking first would evict by the order of the previous build and
# drop keys a smaller build still needs. The headroom leaves room for the keys
# of the previous build next to those of the current one. For windows moving on
# by one an exact bound would do as well (measured for 13 such devices: 182 new
# EID and 104 new mask keys, exactly those recomputed). Measured with
# tracemalloc, the two memos of a build of 13 such devices (4914 EID and 2808
# mask entries) hold about 1.5 MB; the lookup table comes on top. Extrapolated,
# the hard caps bound both memos together to about 10 MB. A build that needs
# more than a cap recomputes all of its entries on every build (no entry of one
# build survives to the next); the values stay correct.
_MEMO_HEADROOM = 2
_EID_MEMO_HARD_CAP = 32768
_EID_MASK_MEMO_HARD_CAP = 16384


def _memo_capacity(needed: int, *, floor: int, cap: int) -> int:
    """Return the memo bound for a build that uses ``needed`` distinct keys."""

    return min(cap, max(floor, _MEMO_HEADROOM * needed))


# Heuristic phone-discovery memoization (AP-SWEEP).
# ``generate_heuristic_eid`` is the on-loop sibling of the build-side crypto: it
# runs inside ``_resolve_eid_internal`` -> ``_heuristic_resolve`` on every
# cache-miss FMDN advertisement (an HA-Bluetooth callback that fires on the
# event loop for EVERY advertisement). The slow path fans out over
# HEURISTIC_HYPOTHESES x HEURISTIC_VARIANTS_TO_TEST (4 x 4) and each call derives
# several EC points, yet its result list is a pure function of the rotation-
# aligned time window plus the hypothesis parameters. Consecutive advertisements
# from the same nearby phone fall in the same window and recompute identical
# lists; a bounded LRU per resolver instance collapses that into a dict lookup
# while staying byte-exact (the key carries every output-determining parameter,
# so a window-boundary mismatch can only cause a benign miss, never a stale hit).
#
# _HEURISTIC_MEMO_MAXSIZE = 256: <= identities (~13) x hypotheses (4) x variants
# (4) x ~2 concurrently live windows ~= 416 worst case for the learned + slow
# paths combined, but in practice only a handful of phones reach the slow path;
# 256 covers the realistic working set with LRU headroom and bounds memory.
_HEURISTIC_MEMO_MAXSIZE = 256


class _BoundedLRUCache:
    """Thread-safe bounded LRU keyed on hashable crypto inputs.

    A tiny instance-bound replacement for ``functools.lru_cache`` that keys on
    explicit input tuples rather than ``self``. The lock guards only the
    OrderedDict lookup/store, never the wrapped crypto call, so concurrent
    worker threads (AP-C) can run the actual derivation in parallel and only
    serialize on the cheap dict access.
    """

    def __init__(self, maxsize: int) -> None:
        """Initialize an empty cache bounded to ``maxsize`` entries."""

        self._maxsize = maxsize
        self._store: OrderedDict[Any, Any] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: Any) -> Any:
        """Return the cached value for ``key`` or ``None`` on a miss.

        ``None`` is never stored as a value, so a ``None`` return is an
        unambiguous miss signal for the callers in this module.
        """

        with self._lock:
            if key not in self._store:
                return None
            # Mark as most-recently-used.
            self._store.move_to_end(key)
            return self._store[key]

    def put(self, key: Any, value: Any) -> None:
        """Store ``value`` under ``key``, evicting the LRU entry past maxsize."""

        with self._lock:
            self._store[key] = value
            self._store.move_to_end(key)
            while len(self._store) > self._maxsize:
                self._store.popitem(last=False)

    @property
    def maxsize(self) -> int:
        """Return the current bound."""

        return self._maxsize

    def resize(self, maxsize: int) -> None:
        """Set a new bound, evicting least-recently-used entries past it."""

        with self._lock:
            self._maxsize = maxsize
            while len(self._store) > self._maxsize:
                self._store.popitem(last=False)

    def __len__(self) -> int:
        """Return the current number of cached entries."""

        with self._lock:
            return len(self._store)


# Strategy Configuration
# ENABLE_ABSOLUTE_UNIX_BASIS: If True, scans for EIDs based on absolute Unix time (Deep Scan).
# Proven unreliable for standard trackers (Motorola/Pebblebee), so disabled by default.
ENABLE_ABSOLUTE_UNIX_BASIS: bool = False

# MIN_UNIX_WINDOW_SIZE: Safety net for absolute scans. 128 * 1024s ~= 36h.
MIN_UNIX_WINDOW_SIZE: int = 128

# MIN_RELATIVE_WINDOW_SIZE: Safety net for relative scans (pair_date).
# 5 * 1024s ~= 85 mins. Essential to absorb 1h Daylight Saving Time (DST) shifts.
MIN_RELATIVE_WINDOW_SIZE: int = 5

# LOCK_FALLBACK_RELATIVE_WINDOW_SIZE: When a lock exists, we still generate a limited
# set of relative discovery windows as a self-healing fallback.
LOCK_FALLBACK_RELATIVE_WINDOW_SIZE: int = 3

EID_LENGTH = LEGACY_EID_LENGTH
LOCK_TTL_SECONDS = 7 * 24 * 60 * 60
LOCK_CONFIRMATION_TTL_SECONDS = 90 * 60
TRUNCATED_FRAME_LOG_WINDOW_SECONDS = 60
VALID_ANCHOR_BASES: set[str] = {"unix", "pair_date", "secrets_creation_date"}
KNOWN_OFFSET_KEY_LENGTH = 2

# LOCK_TRACKING_WINDOW_STEPS: Number of rotation periods to scan around the
# expected counter when tracking a locked device. ±2 periods covers ~34 minutes
# of clock drift at 1024s rotation.
LOCK_TRACKING_WINDOW_STEPS = 2

# =============================================================================
# Heuristic Phone Discovery Configuration
# =============================================================================
# Android phones with "Offline Finding" may use different parameters than
# standard FMDN trackers. The heuristic resolver tests multiple hypotheses
# when the standard lookup fails.

# ENABLE_HEURISTIC_PHONE_DISCOVERY: Enable the reactive heuristic check for
# packets that would otherwise be logged as MISS. This only runs when the
# standard cache lookup fails.
ENABLE_HEURISTIC_PHONE_DISCOVERY: bool = True

# HEURISTIC_HYPOTHESES: Ordered list of (rotation_period, basis) tuples to test.
# Ordered by likelihood based on Android phone behavior analysis.
HEURISTIC_HYPOTHESES: tuple[tuple[int, HeuristicBasis], ...] = (
    (ROTATION_PERIOD_900, HeuristicBasis.ABSOLUTE),  # Most likely for phones
    (ROTATION_PERIOD_3600, HeuristicBasis.ABSOLUTE),  # 1-hour rotation
    (ROTATION_PERIOD_900, HeuristicBasis.RELATIVE),  # 15-min with anchor
    (ROTATION_PERIOD, HeuristicBasis.ABSOLUTE),  # 1024s with absolute time
)

# HEURISTIC_VARIANTS_TO_TEST: EID variants to test during heuristic resolution.
# P-256 variants are most common for modern Android phones.
HEURISTIC_VARIANTS_TO_TEST: tuple[EidVariant, ...] = (
    EidVariant.MODERN_P256_X20_TRUNC_BE,
    EidVariant.MODERN_P256_X32_BE,
    EidVariant.MODERN_P256_X20_TRUNC_LE,
    EidVariant.LEGACY_SECP160R1_X20_BE,
)

# HEURISTIC_LOG_INTERVAL_SECONDS: Minimum interval between heuristic miss logs
# for the same device to prevent log spam.
HEURISTIC_LOG_INTERVAL_SECONDS: int = 300  # 5 minutes


class EIDMatch(NamedTuple):
    """Resolved mapping between an EID and a Home Assistant device."""

    device_id: str
    config_entry_id: str
    canonical_id: str
    time_offset: int
    is_reversed: bool


@dataclass(slots=True, frozen=True)
class DecryptionResult:
    """Decrypted identity key with contextual metadata."""

    key: bytes
    metadata: dict[str, Any]


@dataclass(slots=True)
class BLEBatteryState:
    """Decoded battery state from FMDN hashed-flags BLE advertisement.

    Attributes:
        battery_level: Raw FMDN value (0=UNSUPPORTED, 1=NORMAL, 2=LOW, 3=CRITICALLY_LOW).
        battery_pct: Mapped percentage (100, 25, 5) or None for UNSUPPORTED (0).
        uwt_mode: True if Unwanted Tracking mode is active.
        decoded_flags: Fully decoded flags byte (after XOR).
        observed_at_wall: Wall-clock timestamp of the BLE observation. This is
            the advertisement time, not the processing time: on a history
            replay (integration reload, Home Assistant restart) it is the
            original sighting.
        observed_at_monotonic: The same observation on the
            :func:`time.monotonic` clock. This is the ordering key: the
            writer keeps the newest sighting by this value, not by the wall
            clock, because the wall clock can be stepped backwards (NTP or a
            manual correction) while the process runs, and a later sighting
            with a smaller wall stamp must not be mistaken for an older one.
            Defaults to ``-inf`` ("no ordering knowledge") for states built
            elsewhere, so that any real sighting supersedes them.
    """

    battery_level: int
    battery_pct: int | None
    uwt_mode: bool
    decoded_flags: int
    observed_at_wall: float
    observed_at_monotonic: float = -math.inf


@dataclass(slots=True)
class BLEScanInfo:
    """Last observed BLE scan metadata for a device.

    Stored during EID resolution when a ``ble_address`` is provided by the
    caller (typically Bermuda or another BLE scanner).  Used by the future
    BLE ring fallback (Phase 2) to locate the device for a direct GATT
    connection.

    Attributes:
        ble_address: Current BLE MAC address (rotates every ~15 min on FMDN).
        observed_at: Monotonic timestamp (:func:`time.monotonic` clock) of
            the scan, i.e. of the advertisement, not of its processing.
        observed_at_wall: Wall-clock timestamp of the same scan.
    """

    ble_address: str
    observed_at: float
    observed_at_wall: float


class ObservationClock(NamedTuple):
    """The observation time of one advertisement on both clocks.

    Derived exactly once per ``resolve_eid``/``resolve_eid_all`` call by
    :func:`_observation_clock` and handed to every state writer, so that no
    writer reads a clock of its own.
    """

    monotonic: float
    wall: float


def _observation_clock(observed_at: float | None) -> ObservationClock:
    """Derive the observation time from an optional monotonic timestamp.

    ``observed_at`` is the advertisement time on the :func:`time.monotonic`
    clock (Home Assistant hands it over as ``BluetoothServiceInfoBleak.time``).
    Without it, the observation is taken to be *now*: that is the contract
    for callers that do not know the advertisement time, and it is exactly
    the behaviour every caller had before the parameter existed.

    A value ahead of ``time.monotonic()`` is clamped to *now* on both
    clocks. On the Home Assistant path it never occurs: advertisements are
    stamped with ``CLOCK_MONOTONIC_COARSE``, which lags ``time.monotonic()``
    by up to one tick and never runs ahead, and habluetooth drops any
    restored history entry dated in the future when it loads the store. The
    clamp guards callers that hand in a different clock (and test clocks),
    so that no sighting is ever dated into the future. Clamping the wall clock alone would not do:
    the monotonic component is what the scan-info writer compares sightings
    by, and a future value there would make every later, correctly dated
    sighting look older and freeze the stored address until the local clock
    caught up.
    """
    now_mono = time.monotonic()
    if observed_at is None:
        return ObservationClock(monotonic=now_mono, wall=time.time())
    monotonic = min(observed_at, now_mono)
    return ObservationClock(
        monotonic=monotonic, wall=time.time() - (now_mono - monotonic)
    )


EidLayout = Literal["framed", "bare", "window"]


@dataclass(slots=True)
class EidCandidate:
    """One EID slice extracted from a BLE payload, with its geometry.

    The offset is the load-bearing field: it is what turns "the EID matched"
    into "and therefore the optional hashed-flags byte sits at offset +
    len(eid)". Re-deriving that position from the payload afterwards guesses
    a second time at something the successful match already answered.

    ``layout`` -- not ``frame_type`` -- is the discriminator for that
    decision:

    ==========  ===========================================  ==================
    layout      where the candidate came from                flags position?
    ==========  ===========================================  ==================
    "framed"    a frame byte selected the layout             known
    "bare"      the payload *is* the EID (consumer stripped  known: there is
                the frame and flags bytes before handing it  none
                over)
    "window"    sliding-window search; the offset is a find  unknown
                position, not a parsed layout
    ==========  ===========================================  ==================

    "bare" and "window" both carry ``frame_type=None`` yet demand the exact
    opposite handling, which is why the frame type cannot carry this
    distinction.

    Deliberately unhashable. A candidate must never be usable as a dict key
    or a set member: those are the byte-keyed lookups (``self._lookup``,
    ``self._lookup_metadata``, the candidate set in ``_heuristic_resolve``)
    that must not silently miss. ``__hash__ = None`` is what turns that silent
    miss into a loud ``TypeError``; it is stated explicitly rather than left
    to the ``eq``/``frozen`` interaction, because that interaction is exactly
    the kind of implicit behaviour this guard exists to not depend on.

    Not ``frozen``: the sliding-window branch builds up to 128 of these per
    advertisement inside the BLE callback, on the event loop, and frozen
    instances cost roughly 3.4x as much to construct (measured: 75 us vs 31 us
    for a 63-byte payload). Immutability buys nothing here -- candidates are
    local and short-lived, discarded as soon as one of them matches -- while
    the cost is paid on every unresolvable advertisement.
    """

    eid: bytes
    offset: int  # index of eid[0] within the payload
    frame_type: int | None  # frame byte observed for this payload, if any
    layout: EidLayout

    __hash__ = None  # type: ignore[assignment]


def _guess_flags_byte(raw: bytes) -> int | None:  # noqa: PLR0911
    """Re-derive the hashed-flags position from the payload bytes alone.

    This is the pre-geometry derivation. It has one call site, which serves
    two situations: direct calls that supply no geometry, and sliding-window
    matches, whose offset is a search position rather than a parsed layout.
    Everything else takes the position from the matched candidate, which
    already answered the question.

    SPEC (Find Hub Network Accessory Specification, retrieved 2026-08-05):
    the hashed-flags byte sits at octet 28 for 160-bit-curve frames and at
    octet 40 for 256-bit-curve frames -- i.e. directly after the EID, whose
    own offset is 8 in a full service-data frame. The specification does not
    tie the frame type to the EID length, so a 0x41 frame with a 20-byte
    legacy EID is specification-conformant (a legacy-EID beacon in unwanted
    tracking protection mode), not an exception to be tolerated. The narrow
    length ranges below therefore encode observed payload shapes, not a
    hierarchy of "normal" and "defensive" cases.
    """

    length = len(raw)

    # Service-data format: [header(7)][frame(1)][EID(N)][flags(1)]
    if length >= SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH + 1 and raw[7] == (
        FMDN_FRAME_TYPE
    ):
        return raw[SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH]
    if (
        length >= SERVICE_DATA_OFFSET + MODERN_EID_LENGTH + 1
        and raw[7] == MODERN_FRAME_TYPE
    ):
        return raw[SERVICE_DATA_OFFSET + MODERN_EID_LENGTH]
    if (
        SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH + 1
        <= length
        <= SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH + 2
        and raw[7] == MODERN_FRAME_TYPE
    ):
        return raw[SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH]
    # Raw-header format: [frame(1)][EID(N)][flags(1)]
    if (
        length >= RAW_HEADER_LENGTH + LEGACY_EID_LENGTH + 1
        and raw[0] == FMDN_FRAME_TYPE
    ):
        return raw[RAW_HEADER_LENGTH + LEGACY_EID_LENGTH]
    if (
        length >= RAW_HEADER_LENGTH + MODERN_EID_LENGTH + 1
        and raw[0] == MODERN_FRAME_TYPE
    ):
        return raw[RAW_HEADER_LENGTH + MODERN_EID_LENGTH]
    if (
        RAW_HEADER_LENGTH + LEGACY_EID_LENGTH + 1
        <= length
        <= RAW_HEADER_LENGTH + LEGACY_EID_LENGTH + 2
        and raw[0] == MODERN_FRAME_TYPE
    ):
        return raw[RAW_HEADER_LENGTH + LEGACY_EID_LENGTH]

    return None


def _framed_eid_lengths(
    frame_type: int, payload_len: int, eid_offset: int
) -> tuple[int, ...]:
    """Return the EID lengths worth slicing at *eid_offset*, longest first.

    SPEC (Find Hub Network Accessory Specification, retrieved 2026-08-05): the
    frame type is *not* coupled to the EID length. ``0x41`` only states that
    unwanted tracking protection mode is active, so a 20-byte legacy EID in a
    ``0x41`` frame and a 32-byte P-256 EID in a ``0x40`` frame are both
    conformant shapes. Deriving the slice length from the frame byte therefore
    answers a question the frame byte does not carry, which is the same class
    of second guess that ``EidCandidate`` removed for the flags offset.

    Two asymmetries survive that reading deliberately:

    * The 32-byte reading is probed first wherever it fits, including in
      ``0x40`` frames. The first 20 bytes of a 32-byte EID are a precomputed
      lookup entry in their own right (``SPEC_P256_X20_TRUNC_BE``,
      ``MODERN_P256_X20_TRUNC_*``), so the
      shorter reading would otherwise match first and put the hashed-flags
      byte at octet 28 instead of 40 -- on EID material, which decodes into a
      stable but fabricated battery level and UWT bit.
    * For ``0x41`` the 20-byte reading is offered only when the payload ends
      right after the EID, with at most the optional flags byte behind it. A
      longer ``0x41`` payload that still falls short of a 32-byte EID is more
      plausibly a truncated modern frame, whose byte after the 20th EID octet
      is EID material rather than flags; slicing it as legacy would trade a
      missing candidate for a wrong one. ``0x40`` keeps the unconditional
      20-byte reading it has always had, because dropping it would cost
      resolutions that work today.

    Both framed geometries share this function: *eid_offset* is 8 for a full
    service-data frame and 1 for the bare header layout the integration's own
    BLE scanner hands over (``fmdn_finder/ble_scanner.py``). The mis-slicing
    is a property of the frame byte, not of the offset, so answering it in one
    place is the point.

    An empty result means no framed reading fits; the caller decides whether
    that is a truncation to diagnose or simply a payload for the sliding
    window.

    See custom_components/googlefindmy/AGENTS.md, "FHNA frame slicing
    reminder".
    """

    if payload_len < eid_offset + LEGACY_EID_LENGTH:
        return ()
    lengths: list[int] = []
    if payload_len >= eid_offset + MODERN_EID_LENGTH:
        lengths.append(MODERN_EID_LENGTH)
    legacy_fits_exactly = payload_len <= eid_offset + LEGACY_EID_LENGTH + 1
    if frame_type == FMDN_FRAME_TYPE or legacy_fits_exactly:
        lengths.append(LEGACY_EID_LENGTH)
    return tuple(lengths)


# Mapping from FMDN 2-bit battery level to percentage.
# Per Google FMDN spec: 0=Unsupported, 1=Normal, 2=Low, 3=Critically Low.
# HA icon thresholds in homeassistant/helpers/icon.py:
#   100% → mdi:battery, 25% → mdi:battery-20, 5% → mdi:battery-alert
FMDN_BATTERY_PCT: dict[int, int | None] = {0: None, 1: 100, 2: 25, 3: 5}


@runtime_checkable
class _IdentityProvider(Protocol):
    """Protocol implemented by coordinators that can provide device identities."""

    def get_active_device_identities(self) -> list[DeviceIdentity]: ...


@runtime_checkable
class _LocateRequestClock(Protocol):
    """Protocol implemented by coordinators that stamp sent location requests."""

    def locate_requested_mono(self, device_id: str) -> float | None: ...


@runtime_checkable
class GoogleFindMyEIDResolverProtocol(Protocol):  # pylint: disable=unnecessary-ellipsis
    """Protocol defining the public interface for EID resolution.

    This abstraction layer enables:
    - Bermuda and other integrations to resolve EIDs without tight coupling
    - Easy mocking in tests without depending on concrete implementation
    - Clear documentation of the public EID resolver API surface

    The resolver maintains a lookup table mapping ephemeral identifiers (EIDs)
    to device identities, refreshing periodically as EIDs rotate.
    """

    async def async_refresh(self) -> None:
        """Refresh the EID lookup table from all active coordinators.

        This rebuilds the mapping of EID bytes to device identities by:
        1. Collecting identities from all registered coordinators
        2. Decrypting identity keys where owner keys are available
        3. Generating EIDs for the current rotation window
        """
        ...

    def reset_device_offset(self, registry_id: str) -> None:
        """Clear cached time offset for a specific device.

        Use when a device's EID timing needs to be re-learned, e.g.,
        after firmware updates or clock drift corrections.
        """
        ...

    def resolve_eid(
        self,
        eid_bytes: bytes,
        *,
        ble_address: str | None = None,
        observed_at: float | None = None,
    ) -> EIDMatch | None:
        """Resolve EID bytes to a matching device identity.

        Args:
            eid_bytes: Raw EID bytes from a BLE advertisement.
            ble_address: Optional BLE MAC address of the advertising device.
                When provided, stored for future direct GATT connections.
            observed_at: Optional advertisement time on the
                :func:`time.monotonic` clock (``BluetoothServiceInfoBleak.time``).
                When provided, every observation timestamp the resolver
                records (battery state, scan info, lock confirmation) is the
                advertisement time rather than the processing time, which
                matters when Home Assistant replays its advertisement
                history on reload or restart. When omitted, *now* is used.

        Returns:
            EIDMatch with device identity info, or None if no match found.
            When multiple accounts share the same device, returns the match
            with the smallest time_offset (best match).
            Use resolve_eid_all() to get all matches for shared devices.
        """
        ...

    def resolve_eid_all(
        self,
        eid_bytes: bytes,
        *,
        ble_address: str | None = None,
        observed_at: float | None = None,
    ) -> list[EIDMatch]:
        """Resolve EID bytes to all matching device identities.

        This method supports shared devices: when the same physical tracker
        is shared between accounts, all accounts' matches are returned.

        Args:
            eid_bytes: Raw EID bytes from a BLE advertisement.
            ble_address: Optional BLE MAC address of the advertising device.
                When provided, stored for future direct GATT connections.
            observed_at: Optional advertisement time on the
                :func:`time.monotonic` clock; see :meth:`resolve_eid`.

        Returns:
            List of EIDMatch entries for all accounts that share this device.
            Empty list if no match found.
        """
        ...

    def get_ble_scan_info(self, device_id: str) -> BLEScanInfo | None:
        """Return last observed BLE scan metadata for a device, or None.

        Args:
            device_id: The canonical_id (Google API device identifier).
        """
        ...

    def stop(self) -> None:
        """Stop the resolver and release resources.

        Cancels periodic refresh timers and cleanup tasks.
        """
        ...


@dataclass(slots=True, frozen=True)
class WorkItem:
    """Normalized device identity with optional lock context."""

    registry_id: str
    config_entry_id: str
    canonical_id: str
    key_bytes: bytes
    identity: DeviceIdentity
    lock: EIDGenerationLock | None
    locked_variant: EidVariant | None
    rotation_ts: int | None
    basis_hint: str | None


@dataclass(slots=True, frozen=True)
class WindowCandidate:
    """Single timestamp candidate for EID generation."""

    timestamp: int
    semantic_offset: int
    time_basis: str
    candidate_value: int


@dataclass(slots=True, frozen=True)
class WindowSpec:
    """Time-basis specific window candidates derived from a work item."""

    time_basis: str
    candidate_value: int
    windows: tuple[WindowCandidate, ...]


@dataclass(slots=True, frozen=True)
class VariantSpec:
    """Variant generation request for a given window."""

    key_bytes: bytes
    variant: EidVariant
    window: WindowCandidate
    include_reverse: bool = True


@dataclass(slots=True, frozen=True)
class GeneratedEid:
    """Result of generating a single EID variant."""

    eid_bytes: bytes
    is_reversed: bool
    variant: EidVariant
    window: WindowCandidate


@dataclass(slots=True, frozen=True)
class RotationParams:
    """Collection of rotation-related constants for generation."""

    rotation_period: int
    min_unix_window: int
    min_relative_window: int
    max_window: int


@dataclass(slots=True)
class LearnedHeuristicParams:
    """Learned heuristic parameters for a device discovered via heuristic resolution.

    When a device is successfully matched using heuristic parameters, those
    parameters are cached here to speed up future lookups.
    """

    device_id: str
    canonical_id: str
    rotation_period: int
    basis: HeuristicBasis
    variant: EidVariant
    discovered_at: int  # Unix timestamp when discovered
    last_confirmed_at: int  # Unix timestamp of last successful match
    confirmation_count: int = 1  # Number of successful matches

    def to_dict(self) -> dict[str, Any]:
        """Serialize for storage."""
        return {
            "device_id": self.device_id,
            "canonical_id": self.canonical_id,
            "rotation_period": self.rotation_period,
            "basis": self.basis.value,
            "variant": self.variant.value,
            "discovered_at": self.discovered_at,
            "last_confirmed_at": self.last_confirmed_at,
            "confirmation_count": self.confirmation_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LearnedHeuristicParams:
        """Deserialize from storage."""
        return cls(
            device_id=str(data["device_id"]),
            canonical_id=str(data.get("canonical_id", "")),
            rotation_period=int(data["rotation_period"]),
            basis=HeuristicBasis(data["basis"]),
            variant=EidVariant(data["variant"]),
            discovered_at=int(data.get("discovered_at", 0)),
            last_confirmed_at=int(data.get("last_confirmed_at", 0)),
            confirmation_count=int(data.get("confirmation_count", 1)),
        )


@dataclass(slots=True)
class CacheBuilder:
    """Immutable-safe cache builder enforcing lookup/metadata invariants.

    Supports multiple matches per EID to handle shared devices across accounts.
    When the same physical tracker is shared between accounts, each account
    registers its own EIDMatch for the same EID bytes.
    """

    lookup: dict[bytes, list[EIDMatch]] = field(default_factory=dict)
    metadata: dict[bytes, dict[str, Any]] = field(default_factory=dict)

    def register_eid(  # noqa: PLR0912, PLR0913
        self,
        eid_bytes: bytes,
        *,
        match: EIDMatch,
        variant: EidVariant,
        window: WindowCandidate,
        advertisement_reversed: bool,
        flags_xor_mask: int | None = None,
        key_fingerprint: bytes | None = None,
    ) -> None:
        """Register an EID and metadata, supporting multiple matches per EID.

        When the same EID is registered by multiple accounts (shared devices),
        all matches are stored. For metadata, the entry with the smallest
        time_offset is used as the primary.
        """
        existing_matches = self.lookup.get(eid_bytes, [])
        existing_metadata = self.metadata.get(eid_bytes)
        existing_bases: set[str] | None = None
        if existing_metadata is not None:
            existing_bases = set(existing_metadata.get("timestamp_bases") or ())
            if (basis := existing_metadata.get("timestamp_basis")) and isinstance(
                basis, str
            ):
                existing_bases.add(basis)
            existing_bases.add(window.time_basis)

        # Check if this device_id already has a match for this EID
        existing_device_match: EIDMatch | None = None
        existing_device_idx: int | None = None
        for idx, existing in enumerate(existing_matches):
            if existing.device_id == match.device_id:
                existing_device_match = existing
                existing_device_idx = idx
                break

        # If this device already has a match with better or equal offset, skip
        if existing_device_match is not None and abs(
            existing_device_match.time_offset
        ) <= abs(match.time_offset):
            if existing_metadata is not None and existing_bases is not None:
                existing_metadata["timestamp_bases"] = existing_bases
            return

        # Add or update the match for this device
        if existing_device_idx is not None:
            # Replace existing match for this device
            existing_matches[existing_device_idx] = match
        else:
            # Add new match
            existing_matches.append(match)

        self.lookup[eid_bytes] = existing_matches

        # Update metadata - use the match with smallest time_offset as primary
        best_match = min(existing_matches, key=lambda m: abs(m.time_offset))
        timestamp_bases = existing_bases or {window.time_basis}

        # Only update metadata if this match is the best (smallest offset)
        if best_match.device_id == match.device_id:
            meta: dict[str, Any] = {
                "variant": variant.value,
                "rotation_timestamp": window.timestamp,
                "time_offset": match.time_offset,
                "timestamp_basis": window.time_basis,
                "timestamp_bases": timestamp_bases,
                "advertisement_reversed": advertisement_reversed,
            }
            if flags_xor_mask is not None:
                meta["flags_xor_mask"] = flags_xor_mask
            # Shared devices: the metadata of an EID comes from its best match
            # only, so it carries that device's fingerprint. Equal EIDs come
            # from equal identity keys (the EID is derived from the key), so
            # the fingerprint holds for every device sharing it; were that
            # wrong, the finder would report nothing rather than a wrong EID.
            if key_fingerprint is not None:
                meta["key_fingerprint"] = key_fingerprint
            self.metadata[eid_bytes] = meta
        elif existing_metadata is not None and existing_bases is not None:
            existing_metadata["timestamp_bases"] = existing_bases

        if __debug__:
            assert set(self.lookup.keys()).issuperset(self.metadata.keys()), (
                "metadata keys diverged after registration"
            )

    def finalize(
        self,
    ) -> tuple[dict[bytes, list[EIDMatch]], dict[bytes, dict[str, Any]]]:
        """Return the finalized lookup tables after invariant validation."""

        lookup_keys = set(self.lookup.keys())
        metadata_keys = set(self.metadata.keys())
        if lookup_keys != metadata_keys:
            missing_metadata = lookup_keys - metadata_keys
            missing_lookup = metadata_keys - lookup_keys
            _LOGGER.warning(
                "EID cache invariant violation: metadata_missing=%s lookup_missing=%s",
                missing_metadata,
                missing_lookup,
            )
            for key in missing_metadata:
                self.metadata[key] = {}
            for key in missing_lookup:
                self.lookup.pop(key, None)
        return self.lookup, self.metadata


def iter_rotation_windows(
    target_time: int,
    *,
    rotation_period: int,
    window_range: Iterable[int],
    include_neighbors: bool,
) -> tuple[int, ...]:
    """Return rotation-aligned timestamps for cache population."""

    rotation_start = target_time - (target_time % rotation_period)
    windows: list[int] = []

    for offset in window_range:
        timestamp = rotation_start + (offset * rotation_period)
        if include_neighbors:
            previous_window = timestamp - rotation_period
            next_window = timestamp + rotation_period
            if timestamp >= 0:
                windows.append(timestamp)
            if previous_window >= 0:
                windows.append(previous_window)
            if next_window >= 0:
                windows.append(next_window)
        else:
            windows.append(timestamp)

    return tuple(dict.fromkeys(windows))


def _normalize_optional_string(value: object) -> str | None:
    """Return a trimmed string or ``None`` when empty or missing."""

    if isinstance(value, str):
        value = value.strip()
        return value or None
    return None


def _normalize_anchor_basis(value: object) -> str | None:
    """Return a valid anchor basis or ``None``."""

    if not isinstance(value, str):
        return None
    basis = value.strip()
    return basis if basis in VALID_ANCHOR_BASES else None


def _normalize_encrypted_blob(value: object) -> bytes | None:
    """Normalize encrypted payloads encoded as bytes or hex strings."""

    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        try:
            return bytes.fromhex(value)
        except ValueError:
            return None
    return None


_KEY_FINGERPRINT_SIZE = 32
_KEY_FINGERPRINT_DOMAIN = b"gfmy-finder-eik\x00"


def identity_key_fingerprint(key: object) -> bytes | None:
    """Return a one-way fingerprint of an identity key, or ``None``.

    The finder may report a sighting only under the identity key the sighting
    was matched under. The resolver records this fingerprint with each
    sighting and the finder compares it with the fingerprints of the
    candidate keys it holds now, so the sighting never stores a second copy
    of the key.

    The key is normalized as for the lookup (bytes or a hex string), so both
    sides derive the same value from the same key. The hash is SHA3-256 over
    a domain prefix and the key (BSI TR-02102-1, table 4.1), not SHA-256 of
    the key: SHA-256 of the identity key is the AES key of the device's own
    location reports, so it must not be kept anywhere else. The prefix
    separates this use from any other hash of the key.
    """
    key_bytes = _normalize_encrypted_blob(key)
    if key_bytes is None:
        return None
    return hashlib.sha3_256(_KEY_FINGERPRINT_DOMAIN + key_bytes).digest()


class UwtSource(StrEnum):
    """Where the unwanted-tracking protection mode of a sighting came from."""

    FLAGS_BIT = "flags_bit"
    FRAME_TYPE = "frame_type"
    CONFLICT = "conflict"
    UNKNOWN = "unknown"


FlagsOrigin = Literal["framed", "guessed", "absent"]


class _FlagsReading(NamedTuple):
    """What the hashed-flags decoder saw for one matched advertisement.

    ``uwt_bit`` is the decoded spec bit 7, or ``None`` when nothing was
    decoded. ``origin`` says how the byte was located: ``"framed"`` at the
    position the matched geometry defines, ``"guessed"`` by
    ``_guess_flags_byte``, ``"absent"`` when no byte was found. ``decoded``
    is the whole XOR-decoded flags byte, ``None`` when nothing was decoded;
    the measurement channel logs it next to the mode.
    """

    uwt_bit: bool | None
    origin: FlagsOrigin
    decoded: int | None = None


def _effective_uwt_mode(
    flags_bit: bool | None,
    flags_origin: FlagsOrigin,
    frame_type: int | None,
) -> tuple[bool | None, UwtSource]:
    """Return the protection mode a sighting shows, and where it came from.

    The Find Hub Network Accessory Specification carries the mode twice: in
    the frame type (``0x41`` while unwanted tracking protection mode is
    active, ``0x40`` otherwise) and in spec bit 7 of the hashed flags. The
    decoded bit counts when it was read at a known position (``"framed"``); a
    byte found by ``_guess_flags_byte`` may be EID material and counts like no
    byte at all. When a usable bit and a ``0x40``/``0x41`` frame type
    disagree, the advertisement contradicts itself: the mode is ``None`` with
    source ``CONFLICT``, so no caller reports either value. Without a usable
    bit the frame type decides. Without either the mode is unknown, and
    callers must not report it as "off".
    """
    frame_mode: bool | None = None
    if frame_type == MODERN_FRAME_TYPE:
        frame_mode = True
    elif frame_type == FMDN_FRAME_TYPE:
        frame_mode = False
    if flags_origin == "framed" and flags_bit is not None:
        if frame_mode is not None and frame_mode != flags_bit:
            return None, UwtSource.CONFLICT
        return flags_bit, UwtSource.FLAGS_BIT
    if frame_mode is not None:
        return frame_mode, UwtSource.FRAME_TYPE
    return None, UwtSource.UNKNOWN


# Measurement channel for protection-mode changes (FMDN_UWT_TRANSITION and the
# ``uwt_transitions`` diagnostics block). Both sizes are derived from the
# field data in docs/UWT_FIELD_DATA.md and the public report BSkando#210:
#
# - 12 INFO lines per device and rolling hour. The highest rate measured for
#   one device is 24 changes in 11 days (about 2.2 a day); 12 an hour are 288
#   a day, more than a hundred times that, so the rates measured so far stay
#   far below the limit and only a tracker that flaps continuously reaches
#   it; the shortest reported
#   episode is 38 s, so changes a minute apart do occur.
# - 200 events per config entry. At the #210 rate (32 changes in 11 days,
#   about 2.9 a day) that is about 69 days of history, at the rate seen on the
#   maintainer's instance (about 2.6 a day) about 77 days. Memory stays fixed
#   per entry instead of growing with the number of devices.
_UWT_TRANSITION_INFO_PER_HOUR = 12
_UWT_TRANSITION_RING_SIZE = 200
_UWT_TRANSITION_WINDOW_SECONDS = 3600.0
# One line per recorded event; every field is a fixed label, a number, the
# UTC time of the observation or a class (b) identifier (AGENTS.md section 5):
# no name, entity id, address, coordinate or key material. ``suppressed`` counts the lines of this device
# written at DEBUG since its last INFO line.
_UWT_TRANSITION_FORMAT = (
    "FMDN_UWT_TRANSITION kind=%s device=%s canonical=%s old=%s new=%s "
    "source=%s flags=%s frame=%s decoded=%s payload_len=%d poll_age_s=%s "
    "prev_seen_age_s=%s observed=%s seq=%d suppressed=%d"
)


class UwtTransitionEvent(NamedTuple):
    """One recorded change of a device's unwanted-tracking protection mode.

    ``canonical_id`` is written to the log line as ``canonical=`` (a class (b)
    identifier, AGENTS.md section 5); the diagnostics export replaces it by
    the device's position in the entry (``device_index``).
    ``old`` is ``"unknown"`` for the first known mode of a device in this
    process. ``poll_age_s`` is the age, at the observation, of the last
    location request the integration sent for the device (attempts that
    stopped before the POST leave no stamp, see
    ``coordinator/helpers/locate_clock.py``); ``prev_seen_age_s``
    the age of the last observation of the old mode. Both are ``None`` when
    there is nothing to measure them against.
    """

    canonical_id: str
    kind: Literal["change", "resync"]
    old: Literal["on", "off", "unknown"]
    new: Literal["on", "off"]
    source: str
    flags: FlagsOrigin
    frame: str
    payload_len: int
    poll_age_s: int | None
    prev_seen_age_s: int | None
    observed: str
    seq: int


class UwtTransitionSnapshot(NamedTuple):
    """Recorded protection-mode changes of one config entry, oldest first."""

    capacity: int
    dropped: int
    info_lines_per_device_hour: int
    events: tuple[UwtTransitionEvent, ...]


@dataclass(slots=True)
class _UwtTrack:
    """Protection-mode state of one device in one config entry.

    ``state`` is the last known mode (``None`` before the first one);
    ``last_mono`` the newest observation on the monotonic clock, known or
    not, which orders later observations; ``last_seen_mono_by_state`` the
    newest observation of each mode. ``seq`` numbers the recorded events.
    ``info_times`` and ``suppressed`` belong to the INFO budget of the log
    line; ``skipped_source`` remembers why the last observation carried no
    mode, so the DEBUG line for it is written once per reason, not once per
    advertisement.
    """

    state: bool | None = None
    last_mono: float | None = None
    last_seen_mono_by_state: dict[bool, float] = field(default_factory=dict)
    seq: int = 0
    info_times: deque[float] = field(
        default_factory=lambda: deque(maxlen=_UWT_TRANSITION_INFO_PER_HOUR)
    )
    suppressed: int = 0
    skipped_source: UwtSource | None = None


@dataclass(slots=True)
class _UwtEventRing:
    """Fixed-size event history of one config entry."""

    events: deque[UwtTransitionEvent] = field(
        default_factory=lambda: deque(maxlen=_UWT_TRANSITION_RING_SIZE)
    )
    dropped: int = 0


def _uwt_label(mode: bool | None) -> Literal["on", "off", "unknown"]:
    """Return the log label of a protection mode."""
    if mode is None:
        return "unknown"
    return "on" if mode else "off"


def _uwt_frame_label(frame_type: int | None) -> str:
    """Return ``0x40``/``0x41`` for an FMDN frame type, else ``none``."""
    if frame_type in (FMDN_FRAME_TYPE, MODERN_FRAME_TYPE):
        return f"0x{frame_type:02x}"
    return "none"


@dataclass(slots=True, frozen=True)
class ConfirmedSighting:
    """The last sighting of a device the resolver confirmed by a lookup match.

    It is an observation, not a prediction: ``variant`` and ``window_counter``
    are those of the EID the device advertised, ``window_counter`` aligned to
    the rotation window, and ``observed_at`` is the wall-clock second of the
    sighting. Callers judge its age themselves.

    ``uwt_mode`` is the unwanted-tracking protection mode the advertisement
    showed, ``None`` when it showed none that can be trusted; ``uwt_source``
    says where it came from (see ``_effective_uwt_mode``).

    ``key_fingerprint`` binds the sighting to the identity key it was matched
    under (``identity_key_fingerprint``). The window counter belongs to that
    key: an EID computed from another key with this counter was never
    advertised. ``None`` means the binding is unknown, and the finder then
    reports nothing.
    """

    variant: EidVariant
    window_counter: int
    observed_at: int
    uwt_mode: bool | None = None
    uwt_source: UwtSource = UwtSource.UNKNOWN
    key_fingerprint: bytes | None = field(default=None, repr=False)


@dataclass(slots=True)
class EIDGenerationLock:
    """Persisted per-device generation profile."""

    device_id: str
    canonical_id: str
    variant: str
    advertisement_reversed: bool
    eid_length: int
    rotation_timestamp: int | None = None
    frame_type: int | None = None
    time_basis: str | None = None
    created_at: int = field(default_factory=lambda: int(time.time()))
    drift_offset: int = (
        0  # Tracked drift in seconds (positive = ahead, negative = behind)
    )
    last_seen_at: int | None = None  # Last successful EID match timestamp

    def to_dict(self) -> dict[str, Any]:
        """Serialize lock for storage."""

        return {
            "device_id": self.device_id,
            "canonical_id": self.canonical_id,
            "variant": self.variant,
            "advertisement_reversed": self.advertisement_reversed,
            "eid_length": self.eid_length,
            "rotation_timestamp": self.rotation_timestamp,
            "frame_type": self.frame_type,
            "time_basis": self.time_basis,
            "created_at": self.created_at,
            "drift_offset": self.drift_offset,
            "last_seen_at": self.last_seen_at,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> EIDGenerationLock:
        """Deserialize a stored lock."""

        variant = payload.get("variant") or ""
        advertisement_reversed = bool(payload.get("advertisement_reversed") or False)
        rotation_timestamp = payload.get("rotation_timestamp")
        rotation_ts = (
            int(rotation_timestamp)
            if isinstance(rotation_timestamp, int)
            and not isinstance(rotation_timestamp, bool)
            else None
        )
        if not variant:
            scalar_endianness = str(payload.get("scalar_endianness") or "big")
            length = int(payload["eid_length"])
            if length == LEGACY_EID_LENGTH and scalar_endianness == "little":
                variant = EidVariant.MODERN_P256_X20_TRUNC_LE.value
            elif length == LEGACY_EID_LENGTH:
                variant = EidVariant.LEGACY_SECP160R1_X20_BE.value
            elif scalar_endianness == "little":
                variant = EidVariant.MODERN_P256_X32_LE_SCALAR.value
            else:
                variant = EidVariant.MODERN_P256_X32_BE.value

        return cls(
            device_id=str(payload["device_id"]),
            canonical_id=str(payload.get("canonical_id") or ""),
            variant=variant,
            advertisement_reversed=advertisement_reversed,
            eid_length=int(payload["eid_length"]),
            rotation_timestamp=rotation_ts,
            frame_type=payload.get("frame_type"),
            time_basis=str(payload.get("time_basis") or "") or None,
            created_at=int(payload.get("created_at") or int(time.time())),
            drift_offset=int(payload.get("drift_offset") or 0),
            last_seen_at=int(payload["last_seen_at"])
            if payload.get("last_seen_at") is not None
            else None,
        )


def _normalize_counter_candidate(candidate_value: object, *, basis: str) -> int | None:
    """Return a sane u32 counter candidate or ``None`` when unusable.

    Rejects values that are:
    - Not integers
    - Booleans (which are technically int subclass in Python)
    - Negative or zero (handles phones with pair_date=0 that lack deviceRegistration)
    """
    if (
        not isinstance(candidate_value, int)
        or isinstance(candidate_value, bool)
        or candidate_value <= 0
    ):
        return None

    max_millis = FHNA_COUNTER_MASK
    candidate_seconds = candidate_value // 1000
    if (
        candidate_value > max_millis
        and candidate_value % 1000 == 0
        and candidate_seconds > 0
    ):
        _LOGGER.debug(
            "Converted %s basis %s from milliseconds to seconds: %s",
            candidate_value,
            basis,
            candidate_seconds,
        )
        return candidate_seconds & FHNA_COUNTER_MASK

    return candidate_value


@dataclass(slots=True)
class GoogleFindMyEIDResolver:
    """Resolver that precalculates rotating EIDs for known trackers.

    Supports shared devices: when the same physical tracker is shared between
    accounts, all accounts receive EID match updates via resolve_eid_all().
    """

    hass: HomeAssistant
    _lookup: dict[bytes, list[EIDMatch]] = field(init=False, default_factory=dict)
    _lookup_metadata: dict[bytes, dict[str, Any]] = field(
        init=False, default_factory=dict
    )
    _known_offsets: dict[tuple[str, str], int] = field(init=False, default_factory=dict)
    # Newest sighting per device on the monotonic clock, in memory only:
    # orders the lock writes within this process (see _update_match_state).
    _lock_last_seen_monotonic: dict[str, float] = field(
        init=False, default_factory=dict
    )
    # Last confirmed sighting per device, in memory only: the variant and
    # window counter of the EID the device advertised and when it was seen.
    # The finder reports this observation; see last_confirmed_sighting().
    _confirmed_sightings: dict[str, ConfirmedSighting] = field(
        init=False, default_factory=dict
    )
    # Protection-mode measurement channel, in memory only: the state per
    # (config entry, device) and the event history per config entry behind
    # FMDN_UWT_TRANSITION and the ``uwt_transitions`` diagnostics block. Keyed
    # by entry as well as device so that a tracker shared between two
    # accounts keeps one history per account, and no entry sees another's.
    _uwt_track: dict[tuple[str, str], _UwtTrack] = field(
        init=False, default_factory=dict
    )
    _uwt_events: dict[str, _UwtEventRing] = field(init=False, default_factory=dict)
    _known_advertisement_reversed: dict[str, bool] = field(
        init=False, default_factory=dict
    )
    _known_timebases: dict[str, str] = field(init=False, default_factory=dict)
    _persisted_locks: dict[str, EIDGenerationLock] = field(
        init=False, default_factory=dict
    )
    _decryption_status: dict[str, str] = field(init=False, default_factory=dict)
    _last_lock_confirmation: dict[str, int] = field(init=False, default_factory=dict)
    _provisioning_warn_at: dict[str, float] = field(init=False, default_factory=dict)
    _locks: dict[str, EIDGenerationLock] = field(init=False, default_factory=dict)
    _truncated_frame_log_at: dict[tuple[int, int], float] = field(
        init=False, default_factory=dict
    )
    _store: Store[list[dict[str, Any]] | None] = field(init=False)
    _unsub_interval: CALLBACK_TYPE | None = field(init=False, default=None)
    _unsub_alignment: CALLBACK_TYPE | None = field(init=False, default=None)
    _refresh_lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)
    _pending_refresh: bool = field(init=False, default=False)
    _load_task: asyncio.Task[None] | None = field(init=False, default=None)
    # Heuristic phone discovery state
    _learned_heuristic_params: dict[str, LearnedHeuristicParams] = field(
        init=False, default_factory=dict
    )
    _heuristic_miss_log_at: dict[str, float] = field(init=False, default_factory=dict)
    _flags_logged_devices: set[str] = field(init=False, default_factory=set)
    _decode_failure_types_logged: set[str] = field(init=False, default_factory=set)
    _frame_conflict_logged_devices: set[str] = field(init=False, default_factory=set)
    _frame_conflict_log_at: dict[tuple[str, int], float] = field(
        init=False, default_factory=dict
    )
    _ble_battery_state: dict[str, BLEBatteryState] = field(
        init=False, default_factory=dict
    )
    _ble_scan_info: dict[str, BLEScanInfo] = field(init=False, default_factory=dict)
    _cached_identities: list[DeviceIdentity] = field(init=False, default_factory=list)
    # Per-variant crypto memoization caches (AP-A). Declared as slots-backed
    # dataclass fields; stubs that bypass __init__ get them seeded by
    # _ensure_cache_defaults under its hasattr guard.
    _eid_memo: _BoundedLRUCache = field(
        init=False, default_factory=lambda: _BoundedLRUCache(_EID_MEMO_MAXSIZE)
    )
    _flags_mask_memo: _BoundedLRUCache = field(
        init=False,
        default_factory=lambda: _BoundedLRUCache(_EID_MASK_MEMO_MAXSIZE),
    )
    # On-loop heuristic phone-discovery memoization (AP-SWEEP). Same shape as the
    # two AP-A caches; absorbs the per-advertisement ``generate_heuristic_eid``
    # fan-out on the resolve hot path.
    _heuristic_memo: _BoundedLRUCache = field(
        init=False,
        default_factory=lambda: _BoundedLRUCache(_HEURISTIC_MEMO_MAXSIZE),
    )
    # Deterministic signature of the last successful build phase (AP-B1).
    # ``None`` means "no successful build yet", so the first refresh never
    # skips. Declared as a slots-backed dataclass field; stubs that bypass
    # __init__ get it seeded by _ensure_cache_defaults under its hasattr guard.
    _last_build_signature: str | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        """Set up caches and schedule the first rotation-aligned refresh."""

        self._ensure_cache_defaults()
        self._store = Store(self.hass, STORAGE_VERSION, STORAGE_KEY)
        FOREIGN_READING_TRACKER.set_curve_provider(self.locked_curve_name)
        load_coro = self._async_load_locks()
        self._load_task = self.hass.async_create_task(load_coro)
        if self._load_task is None or not isinstance(self._load_task, asyncio.Task):
            # Task creation returned None or non-Task (e.g., in tests with mocks)
            # Close the coroutine to prevent "coroutine never awaited" warnings
            try:
                load_coro.close()
            except Exception:  # pragma: no cover - defensive close
                pass
            self._load_task = None
        self._start_alignment_timer()

    def _ensure_cache_defaults(self) -> None:  # noqa: PLR0912, PLR0915
        """Initialize optional caches when stubs bypass __init__."""

        if not hasattr(self, "_known_offsets"):
            self._known_offsets = {}
        if not hasattr(self, "_known_advertisement_reversed"):
            self._known_advertisement_reversed = {}
        if not hasattr(self, "_known_timebases"):
            self._known_timebases = {}
        if not hasattr(self, "_persisted_locks"):
            self._persisted_locks = {}
        if not hasattr(self, "_decryption_status"):
            self._decryption_status = {}
        if not hasattr(self, "_last_lock_confirmation"):
            self._last_lock_confirmation = {}
        if not hasattr(self, "_lock_last_seen_monotonic"):
            self._lock_last_seen_monotonic = {}
        if not hasattr(self, "_confirmed_sightings"):
            self._confirmed_sightings = {}
        if not hasattr(self, "_uwt_track"):
            self._uwt_track = {}
        if not hasattr(self, "_uwt_events"):
            self._uwt_events = {}
        if not hasattr(self, "_provisioning_warn_at"):
            self._provisioning_warn_at = {}
        if not hasattr(self, "_locks"):
            self._locks = {}
        if not hasattr(self, "_truncated_frame_log_at"):
            self._truncated_frame_log_at = {}
        if not hasattr(self, "_frame_conflict_logged_devices"):
            self._frame_conflict_logged_devices = set()
        if not hasattr(self, "_frame_conflict_log_at"):
            self._frame_conflict_log_at = {}
        # Heuristic phone discovery state
        if not hasattr(self, "_learned_heuristic_params"):
            self._learned_heuristic_params = {}
        if not hasattr(self, "_heuristic_miss_log_at"):
            self._heuristic_miss_log_at = {}
        if not hasattr(self, "_flags_logged_devices"):
            self._flags_logged_devices = set()
        if not hasattr(self, "_decode_failure_types_logged"):
            self._decode_failure_types_logged = set()
        if not hasattr(self, "_ble_battery_state"):
            self._ble_battery_state = {}
        if not hasattr(self, "_ble_scan_info"):
            self._ble_scan_info = {}
        if not hasattr(self, "_cached_identities"):
            self._cached_identities = []
        # Per-variant crypto memoization caches (AP-A). Initialized here under
        # the same hasattr guard as the _known_* defaults so stubs that bypass
        # __init__ (and call _ensure_cache_defaults directly) get them too.
        if not hasattr(self, "_eid_memo"):
            self._eid_memo = _BoundedLRUCache(_EID_MEMO_MAXSIZE)
        if not hasattr(self, "_flags_mask_memo"):
            self._flags_mask_memo = _BoundedLRUCache(_EID_MASK_MEMO_MAXSIZE)
        # On-loop heuristic discovery memoization (AP-SWEEP), same guard.
        if not hasattr(self, "_heuristic_memo"):
            self._heuristic_memo = _BoundedLRUCache(_HEURISTIC_MEMO_MAXSIZE)
        # Build-phase skip-guard signature (AP-B1). Same hasattr guard as the
        # AP-A memo caches so stubs that bypass __init__ get the default too.
        if not hasattr(self, "_last_build_signature"):
            self._last_build_signature = None

    def _clear_lock_state(self, device_id: str) -> bool:
        """Remove all cached state associated with a device lock."""

        removed = False
        if self._purge_known_offsets_for_device(device_id):
            removed = True

        for attr in (
            "_locks",
            "_persisted_locks",
            "_known_advertisement_reversed",
            "_known_timebases",
            "_last_lock_confirmation",
            "_lock_last_seen_monotonic",
            "_confirmed_sightings",
        ):
            mapping = getattr(self, attr, None)
            if isinstance(mapping, dict) and device_id in mapping:
                mapping.pop(device_id, None)
                removed = True
        return removed

    def _purge_known_offsets_for_device(self, device_id: str) -> bool:
        """Remove any stored known offsets for the device across bases."""

        mapping = getattr(self, "_known_offsets", None)
        if not isinstance(mapping, dict) or not mapping:
            return False

        to_delete: list[tuple[str, str]] = []
        for key in list(mapping.keys()):
            if (
                isinstance(key, tuple)
                and len(key) == KNOWN_OFFSET_KEY_LENGTH
                and isinstance(key[0], str)
                and key[0] == device_id
            ):
                to_delete.append(key)
        for key in to_delete:
            mapping.pop(key, None)

        return bool(to_delete)

    def _start_alignment_timer(self) -> None:
        """Schedule the first refresh on the next rotation boundary."""
        now_unix = int(time.time())
        seconds_until_boundary = ROTATION_PERIOD - (now_unix % ROTATION_PERIOD)
        delay = seconds_until_boundary or ROTATION_PERIOD
        self._unsub_alignment = async_call_later(
            self.hass, delay, self._handle_alignment_refresh
        )

    def _start_interval(self) -> None:
        """Start the recurring refresh interval aligned to rotations."""
        if self._unsub_interval is None:
            self._unsub_interval = async_track_time_interval(
                self.hass,
                self._handle_refresh_interval,
                timedelta(seconds=ROTATION_PERIOD),
            )

    async def _handle_alignment_refresh(self, _now: datetime) -> None:
        """Run the initial rotation-aligned refresh then start the timer."""
        rotation_start = int(time.time())
        rotation_start -= rotation_start % ROTATION_PERIOD
        _LOGGER.debug("Boundary-aligned EID cache refresh at %s", rotation_start)
        await self.async_refresh()
        self._start_interval()

    async def _handle_refresh_interval(self, _now: datetime) -> None:
        """Refresh the cached EID lookup table on the rotation cadence."""
        await self.async_refresh()

    async def _async_load_locks(self) -> None:
        """Load persisted locks from storage."""

        payload = await self._store.async_load()
        if not payload:
            return

        for entry in payload:
            try:
                lock = EIDGenerationLock.from_dict(entry)
            except Exception:  # pragma: no cover - defensive
                continue
            self._locks[lock.device_id] = lock
            # Restore drift offset to _known_offsets for EID calculation
            if lock.drift_offset != 0 and lock.time_basis:
                self._known_offsets[(lock.device_id, lock.time_basis)] = (
                    lock.drift_offset
                )
                self._known_timebases[lock.device_id] = lock.time_basis

    async def _async_save_locks(self) -> None:
        """Persist locks to storage."""

        await self._store.async_save([lock.to_dict() for lock in self._locks.values()])

    def _schedule_lock_save(self) -> None:
        """Schedule persistence of EID locks."""

        lock_save: Coroutine[Any, Any, None] | None = None
        try:
            task_name = "googlefindmy_eid_resolver_save"
            create_task = getattr(
                self.hass, "async_create_background_task", None
            ) or getattr(self.hass, "async_create_task")
            if create_task is None:
                raise AttributeError("hass is missing async_create_task helper")
            lock_save = self._async_save_locks()
            try:
                scheduled = create_task(lock_save, name=task_name)
            except TypeError:
                scheduled = create_task(lock_save)
            if scheduled is None:
                asyncio.create_task(lock_save)
                lock_save = None  # Ownership transferred to asyncio.create_task
                _LOGGER.warning(
                    "EID lock save was not scheduled (task helper returned None)"
                )
            elif asyncio.iscoroutine(scheduled):
                asyncio.create_task(scheduled)
                lock_save = None  # Ownership transferred
            elif not isinstance(scheduled, asyncio.Task):
                try:
                    lock_save.close()
                except Exception:  # pragma: no cover - defensive close
                    pass
                lock_save = None
                _LOGGER.warning(
                    "EID lock save task helper returned non-awaitable %s; coroutine closed",
                    type(scheduled).__name__,
                )
            else:
                lock_save = None  # Ownership transferred to task
        except Exception as err:  # pragma: no cover - defensive log
            _LOGGER.error("Failed to schedule EID lock persistence: %s", err)
            if lock_save is not None:
                try:
                    lock_save.close()
                except Exception:  # pragma: no cover - defensive close
                    pass

    def _purge_stale_locks(self, *, now: int) -> None:  # noqa: PLR0912
        """Drop expired generation locks to keep cache fresh."""

        expired_by_confirmation: dict[str, int | None] = {}
        expired_by_created: list[str] = []
        for device_id, lock in list(self._locks.items()):
            last_confirmation = self._last_lock_confirmation.get(device_id)
            if isinstance(last_confirmation, int) and not isinstance(
                last_confirmation, bool
            ):
                confirmation_age: int | None = now - last_confirmation
                last_activity = last_confirmation
            else:
                confirmation_age = None
                last_activity = lock.created_at
            if confirmation_age is None:
                if now - lock.created_at > LOCK_CONFIRMATION_TTL_SECONDS:
                    expired_by_confirmation[device_id] = None
                    continue
            elif confirmation_age > LOCK_CONFIRMATION_TTL_SECONDS:
                expired_by_confirmation[device_id] = confirmation_age
                continue

            if now - last_activity > LOCK_TTL_SECONDS:
                expired_by_created.append(device_id)

        removed: list[str] = []
        for device_id, confirmation_age in expired_by_confirmation.items():
            if self._clear_lock_state(device_id):
                removed.append(device_id)
                _LOGGER.debug(
                    "Purged stale EID lock for %s after %s seconds without confirmation",
                    device_id,
                    confirmation_age if confirmation_age is not None else "unknown",
                )
        for device_id in expired_by_created:
            if device_id in expired_by_confirmation:
                continue
            if self._clear_lock_state(device_id):
                removed.append(device_id)
        if removed:
            self._schedule_lock_save()
            _LOGGER.debug("Purged %d stale EID locks: %s", len(removed), removed)

    async def async_refresh(self) -> None:
        """Trigger a cache refresh immediately."""
        if self._refresh_lock.locked():
            self._pending_refresh = True
            return

        async with self._refresh_lock:
            self._pending_refresh = False
            await self._refresh_cache()
            while self._pending_refresh:
                self._pending_refresh = False
                await self._refresh_cache()
            # Unloading an entry also runs a refresh (or stop() for the last
            # one), so this drops the history of unloaded entries as well.
            self._purge_uwt_state()

    def _purge_uwt_state(self) -> None:
        """Drop measurement-channel state of devices and entries now gone.

        An entry keeps its event history while it is loaded
        (``hass.data[DOMAIN]["entries"]``), even when none of its identities
        could be prepared in this refresh (a key that does not decrypt for a
        moment must not erase the history). A device keeps its track only
        while its entry is loaded and an identity of that entry still names
        it, by canonical id (as ``EIDMatch.canonical_id`` carries it, without
        an entry prefix) or by registry id; a device that returns later starts
        again with ``old=unknown``.
        """
        self._ensure_cache_defaults()
        bucket = self.hass.data.get(DOMAIN)
        entries = bucket.get("entries") if isinstance(bucket, dict) else None
        loaded: set[str] = set(entries) if isinstance(entries, dict) else set()
        known_pairs: set[tuple[str, str]] = set()
        for identity in self._cached_identities:
            entry_id = identity.config_entry_id
            if not entry_id or entry_id not in loaded:
                continue
            if identity.canonical_id:
                known_pairs.add((entry_id, identity.canonical_id.split(":")[-1]))
            if identity.registry_id:
                known_pairs.add((entry_id, identity.registry_id))
        for key in [key for key in self._uwt_track if key not in known_pairs]:
            del self._uwt_track[key]
        for entry_id in [e for e in self._uwt_events if e not in loaded]:
            del self._uwt_events[entry_id]

    def reset_device_offset(self, registry_id: str) -> None:
        """Forget cached offsets/locks for a given device registry ID."""

        if not registry_id:
            return

        self._ensure_cache_defaults()
        removed = self._clear_lock_state(registry_id)
        removed = self._purge_known_offsets_for_device(registry_id) or removed
        removed = (
            self._known_advertisement_reversed.pop(registry_id, None) is not None
            or removed
        )
        removed = self._known_timebases.pop(registry_id, None) is not None or removed
        removed = (
            self._last_lock_confirmation.pop(registry_id, None) is not None or removed
        )

        if removed:
            try:
                self._schedule_lock_save()
            except Exception as err:  # pragma: no cover - defensive log
                _LOGGER.debug(
                    "Failed to schedule lock persistence after reset for %s: %s",
                    registry_id,
                    err,
                )

        refresh = getattr(self, "async_refresh", None)
        create_task = getattr(self.hass, "async_create_task", None)
        if callable(refresh) and callable(create_task):
            self._pending_refresh = True
            try:
                refresh_coro = refresh()
                scheduled = create_task(
                    refresh_coro, name="googlefindmy_eid_refresh_after_reset"
                )
                if scheduled is None:
                    refresh_coro.close()
                elif asyncio.iscoroutine(scheduled):
                    asyncio.create_task(scheduled)
            except Exception as err:  # pragma: no cover - defensive log
                _LOGGER.debug(
                    "Failed to schedule refresh after reset for %s: %s",
                    registry_id,
                    err,
                )

    def _build_rotation_params(self) -> RotationParams:
        """Return the rotation constants for deterministic refresh calculations."""

        return RotationParams(
            rotation_period=ROTATION_PERIOD,
            min_unix_window=MIN_UNIX_WINDOW_SIZE,
            min_relative_window=MIN_RELATIVE_WINDOW_SIZE,
            max_window=math.ceil((24 * 60 * 60) / ROTATION_PERIOD),
        )

    def _prepare_work_item(
        self,
        identity: DeviceIdentity,
        *,
        now_unix: int,
    ) -> WorkItem | None:
        """Normalize an identity and lock into a work item."""

        basis_hint = self._known_timebases.get(identity.registry_id)
        if (
            not identity.config_entry_id
            or identity.identity_key is None
            or identity.registry_id is None
        ):
            _LOGGER.debug(
                "Skipping work item for %s: config_entry_id=%s, identity_key=%s, registry_id=%s",
                identity.canonical_id,
                bool(identity.config_entry_id),
                identity.identity_key is not None,
                bool(identity.registry_id),
            )
            return None

        clean_canonical_id = identity.canonical_id
        if ":" in clean_canonical_id:
            clean_canonical_id = clean_canonical_id.split(":")[-1]

        # FIX: Handle both bytes and hex string formats for identity_key
        key_bytes = _normalize_encrypted_blob(identity.identity_key)
        if key_bytes is None:
            return None

        lock = self._locks.get(identity.registry_id)
        locked_variant: EidVariant | None = None
        rotation_ts: int | None = None

        if lock is not None:
            raw_rotation_ts = lock.rotation_timestamp
            rotation_ts = (
                raw_rotation_ts
                if isinstance(raw_rotation_ts, int)
                and not isinstance(raw_rotation_ts, bool)
                else None
            )
            lock_time_basis = _normalize_optional_string(lock.time_basis)
            if lock_time_basis:
                basis_hint = lock_time_basis
                self._known_timebases[identity.registry_id] = lock_time_basis

            is_legacy = rotation_ts is None
            unknown_variant = False
            try:
                locked_variant = EidVariant(lock.variant)
            except ValueError:
                # A variant value this version does not know (for example one
                # written by a newer release before a rollback) must not be
                # reinterpreted as another variant. Discard the lock and clear
                # the locked variant so ``_compute_variants`` tries every known
                # variant again.
                unknown_variant = True
                locked_variant = None

            if is_legacy or unknown_variant:
                valid_hint = (
                    lock_time_basis
                    if lock_time_basis
                    in {
                        "unix",
                        "pair_date",
                        "secrets_creation_date",
                    }
                    else None
                )
                _LOGGER.warning(
                    "Discarding invalid/legacy lock for %s (legacy=%s, unknown_variant=%s). Force re-discovery.",
                    identity.registry_id,
                    is_legacy,
                    unknown_variant,
                )
                self._locks.pop(identity.registry_id, None)
                self._persisted_locks.pop(identity.registry_id, None)
                if valid_hint:
                    basis_hint = valid_hint
                    self._known_timebases[identity.registry_id] = valid_hint
                else:
                    self._known_timebases.pop(identity.registry_id, None)
                lock = None
                if unknown_variant:
                    # Persist the discard like the other lock-removal paths;
                    # otherwise the unchanged on-disk lock is reloaded at
                    # every start and the discard repeats.
                    self._schedule_lock_save()
            elif lock.canonical_id != clean_canonical_id:
                _LOGGER.debug(
                    "Updating canonical_id to UUID-only for %s: %s -> %s",
                    identity.registry_id,
                    lock.canonical_id,
                    clean_canonical_id,
                )
                lock.canonical_id = clean_canonical_id
                self._schedule_lock_save()

        return WorkItem(
            registry_id=identity.registry_id,
            config_entry_id=str(identity.config_entry_id),
            canonical_id=clean_canonical_id,
            key_bytes=key_bytes,
            identity=identity,
            lock=lock,
            locked_variant=locked_variant,
            rotation_ts=rotation_ts,
            basis_hint=basis_hint,
        )

    def _collect_work_items(
        self,
        identities: Iterable[DeviceIdentity],
        *,
        now_unix: int,
    ) -> list[WorkItem]:
        """Normalize all identities into work items."""

        items: list[WorkItem] = []
        for identity in identities:
            work_item = self._prepare_work_item(identity, now_unix=now_unix)
            if work_item is not None:
                items.append(work_item)
        return items

    def _compute_time_windows(
        self,
        work_item: WorkItem,
        *,
        now_unix: int,
        params: RotationParams,
    ) -> tuple[list[WindowSpec], bool]:
        """Compute time windows for a work item and report hint validity."""

        lock_windows = self._compute_lock_windows(
            work_item, now_unix=now_unix, params=params
        )
        fallback_discovery = bool(lock_windows)
        min_relative_window = (
            LOCK_FALLBACK_RELATIVE_WINDOW_SIZE
            if fallback_discovery
            else params.min_relative_window
        )
        relative_windows, invalid_hint = self._compute_relative_windows(
            work_item,
            now_unix=now_unix,
            params=params,
            min_relative_window=min_relative_window,
            allow_unix_basis=not fallback_discovery,
        )
        if lock_windows:
            lock_windows.extend(relative_windows)
            return self._dedupe_windows(lock_windows), invalid_hint

        return self._dedupe_windows(relative_windows), invalid_hint

    def _compute_lock_windows(
        self,
        work_item: WorkItem,
        *,
        now_unix: int,
        params: RotationParams,
    ) -> list[WindowSpec]:
        """Return windows derived from a valid lock rotation timestamp."""

        if work_item.rotation_ts is None or work_item.lock is None:
            return []

        counter_windows: list[WindowCandidate] = []
        time_since_lock = max(0, now_unix - work_item.lock.created_at)
        periods_elapsed = time_since_lock // params.rotation_period
        expected_counter = work_item.rotation_ts + (
            periods_elapsed * params.rotation_period
        )
        for step in range(-LOCK_TRACKING_WINDOW_STEPS, LOCK_TRACKING_WINDOW_STEPS + 1):
            offset_periods = periods_elapsed + step
            window_ts = work_item.rotation_ts + (
                offset_periods * params.rotation_period
            )
            if window_ts < 0:
                continue
            semantic_offset = window_ts - expected_counter
            counter_windows.append(
                WindowCandidate(
                    timestamp=window_ts,
                    semantic_offset=semantic_offset,
                    time_basis="lock_tracking",
                    candidate_value=expected_counter,
                )
            )

        if not counter_windows:
            return []

        return [
            WindowSpec(
                time_basis="lock_tracking",
                candidate_value=expected_counter,
                windows=tuple(counter_windows),
            )
        ]

    def _compute_relative_windows(  # noqa: PLR0912, PLR0915
        self,
        work_item: WorkItem,
        *,
        now_unix: int,
        params: RotationParams,
        min_relative_window: int,
        allow_unix_basis: bool,
    ) -> tuple[list[WindowSpec], bool]:
        """Return relative/absolute windows and whether the basis hint was invalid."""

        counter_windows: list[WindowSpec] = []
        counter_bases: tuple[tuple[str, str], ...] = (
            ("unix", "unix"),
            ("pair_date", "pair_date"),
            ("secrets_creation_date", "secrets_creation_date"),
        )
        if not ENABLE_ABSOLUTE_UNIX_BASIS:
            counter_bases = tuple(
                (basis, label) for basis, label in counter_bases if basis != "unix"
            )
        if not allow_unix_basis:
            counter_bases = tuple(
                (basis, label) for basis, label in counter_bases if basis != "unix"
            )
        anchor_candidates: list[int] = []
        if (
            isinstance(work_item.identity.pair_date, int)
            and work_item.identity.pair_date > 0
        ):
            anchor_candidates.append(work_item.identity.pair_date)
        if (
            isinstance(work_item.identity.secrets_creation_date, int)
            and work_item.identity.secrets_creation_date > 0
        ):
            anchor_candidates.append(work_item.identity.secrets_creation_date)
        best_anchor = max(anchor_candidates) if anchor_candidates else 0
        provisioning_counter = max(0, now_unix - best_anchor) if best_anchor > 0 else 0
        drift_seconds = provisioning_counter * 0.00005
        drift_windows = math.ceil(drift_seconds / params.rotation_period)

        available_bases = {basis for basis, _ in counter_bases}
        known_basis = (
            work_item.basis_hint if work_item.basis_hint in available_bases else None
        )
        invalid_hint = bool(work_item.basis_hint and known_basis is None)
        filtered_bases = (
            tuple(
                (basis, label) for basis, label in counter_bases if basis == known_basis
            )
            if known_basis
            else counter_bases
        )

        for basis, label in filtered_bases:
            candidate_value: int | None = None
            reference_ts = now_unix
            total_window: int = 0

            if basis == "unix":
                if not ENABLE_ABSOLUTE_UNIX_BASIS:
                    continue
                candidate_value = now_unix
                reference_ts = now_unix
                try:
                    tz_offset = dt_util.now().utcoffset()
                    tz_windows = (
                        int(
                            math.ceil(
                                abs(tz_offset.total_seconds()) / params.rotation_period
                            )
                        )
                        if tz_offset
                        else 0
                    )
                except Exception:
                    tz_windows = 0
                total_window = max(
                    params.min_unix_window, 3 + drift_windows + tz_windows
                )
            else:
                raw_anchor = getattr(work_item.identity, basis, None)
                anchor_value = _normalize_counter_candidate(raw_anchor, basis=basis)
                if anchor_value is None:
                    _LOGGER.debug(
                        "Skipping basis %s for %s: raw_anchor=%s (normalized=None)",
                        basis,
                        work_item.registry_id,
                        raw_anchor,
                    )
                    continue
                candidate_value = now_unix - anchor_value
                reference_ts = candidate_value

                safety_buffer = 2
                known_offset = self._known_offsets.get((work_item.registry_id, basis))
                if known_offset is not None:
                    max_offset = 5 * params.rotation_period
                    if abs(known_offset) <= max_offset:
                        candidate_value = candidate_value + known_offset
                        reference_ts = candidate_value
                        total_window = min(3, params.max_window)
                    else:
                        _LOGGER.debug(
                            "Ignoring implausible known_offset=%s for %s (basis=%s); falling back to discovery window",
                            known_offset,
                            work_item.registry_id,
                            basis,
                        )
                        known_offset = None
                if known_offset is None:
                    total_window = min(
                        max(min_relative_window, 3 + drift_windows + safety_buffer),
                        params.max_window,
                    )

            normalized = _normalize_counter_candidate(candidate_value, basis=basis)
            if normalized is None:
                _LOGGER.debug(
                    "Skipping basis %s for %s: candidate_value=%s (counter normalized=None)",
                    basis,
                    work_item.registry_id,
                    candidate_value,
                )
                continue
            reference_ts = normalized

            window_candidates: list[WindowCandidate] = []
            for ts in iter_rotation_windows(
                normalized,
                rotation_period=params.rotation_period,
                window_range=range(-total_window, total_window + 1),
                include_neighbors=False,
            ):
                if ts < 0:
                    continue
                semantic_offset = ts - reference_ts
                window_candidates.append(
                    WindowCandidate(
                        timestamp=ts,
                        semantic_offset=semantic_offset,
                        time_basis=label,
                        candidate_value=normalized,
                    )
                )
            if window_candidates:
                counter_windows.append(
                    WindowSpec(
                        time_basis=label,
                        candidate_value=normalized,
                        windows=tuple(window_candidates),
                    )
                )

        return counter_windows, invalid_hint

    @staticmethod
    def _dedupe_windows(specs: list[WindowSpec]) -> list[WindowSpec]:
        """Remove duplicate windows by (basis, timestamp) while preserving order."""

        basis_order: list[str] = []
        basis_windows: dict[str, list[WindowCandidate]] = {}
        basis_candidate_value: dict[str, int] = {}
        key_index: dict[tuple[str, int], int] = {}

        for spec in specs:
            if spec.time_basis not in basis_order:
                basis_order.append(spec.time_basis)
                basis_windows.setdefault(spec.time_basis, [])
            for window in spec.windows:
                key = (spec.time_basis, window.timestamp)
                if key not in key_index:
                    key_index[key] = len(basis_windows[spec.time_basis])
                    basis_windows[spec.time_basis].append(window)
                    basis_candidate_value.setdefault(
                        spec.time_basis, window.candidate_value
                    )
                    continue

                idx = key_index[key]
                current = basis_windows[spec.time_basis][idx]
                if abs(window.semantic_offset) < abs(current.semantic_offset):
                    basis_windows[spec.time_basis][idx] = window
                    basis_candidate_value[spec.time_basis] = window.candidate_value

        return [
            WindowSpec(
                time_basis=basis,
                candidate_value=basis_candidate_value[basis],
                windows=tuple(basis_windows[basis]),
            )
            for basis in basis_order
            if basis_windows.get(basis)
        ]

    def _compute_variants(
        self,
        work_item: WorkItem,
        window: WindowSpec,
    ) -> tuple[VariantSpec, ...]:
        """Return the variants to generate for a window."""

        if work_item.locked_variant is not None:
            return tuple(
                VariantSpec(
                    key_bytes=work_item.key_bytes,
                    variant=work_item.locked_variant,
                    window=window_candidate,
                )
                for window_candidate in window.windows
            )

        return tuple(
            VariantSpec(
                key_bytes=work_item.key_bytes,
                variant=variant,
                window=window_candidate,
            )
            # Specification variants before the heuristic MODERN_P256_* ones.
            for variant in (
                EidVariant.LEGACY_SECP160R1_X20_BE,
                EidVariant.SPEC_P256_X32_BE,
                EidVariant.SPEC_P256_X20_TRUNC_BE,
                EidVariant.MODERN_P256_X32_BE,
                EidVariant.MODERN_P256_X20_TRUNC_BE,
                EidVariant.MODERN_P256_X32_LE_SCALAR,
                EidVariant.MODERN_P256_X20_TRUNC_LE,
            )
            for window_candidate in window.windows
        )

    def _generate_eids_from_spec(
        self,
        variant_spec: VariantSpec,
    ) -> Iterable[GeneratedEid]:
        """Generate EIDs for a variant specification."""

        try:
            eid_bytes = self._generate_variant(
                variant_spec.key_bytes,
                time_counter=variant_spec.window.timestamp,
                variant=variant_spec.variant,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            _LOGGER.debug(
                "Skipping EID generation for variant=%s basis=%s window_ts=%s: %s",
                variant_spec.variant.value,
                variant_spec.window.time_basis,
                variant_spec.window.timestamp,
                exc,
            )
            return []

        generated: list[GeneratedEid] = [
            GeneratedEid(
                eid_bytes=eid_bytes,
                is_reversed=False,
                variant=variant_spec.variant,
                window=variant_spec.window,
            )
        ]
        if variant_spec.include_reverse:
            generated.append(
                GeneratedEid(
                    eid_bytes=eid_bytes[::-1],
                    is_reversed=True,
                    variant=variant_spec.variant,
                    window=variant_spec.window,
                )
            )
        return generated

    def _build_signature(
        self,
        work_items: list[WorkItem],
        *,
        now_unix: int,
        params: RotationParams,
    ) -> str:
        """Return a deterministic signature of the active-device build inputs.

        The signature is the skip-guard key for the expensive build phase: if it
        is unchanged, re-running the build would reproduce a byte-identical
        lookup table, so it can be safely skipped.

        Correctness model (Variant B -- fold the computed specs, not a time
        formula). The expensive, skipped part of the build is the per-variant
        crypto in ``_generate_eids_from_spec`` / ``_compute_flags_xor_mask``. Its
        only inputs are the ``VariantSpec`` items the build loop fans out, which
        are a deterministic, *pure* function of each work item and ``now_unix``:
        ``_compute_time_windows`` (-> ``_compute_lock_windows`` /
        ``_compute_relative_windows`` / ``_dedupe_windows``) reads the learned
        ``_known_offsets`` snapshot but mutates no resolver state, and
        ``_compute_variants`` is pure. The signature therefore replays exactly
        that window/variant derivation and folds its *content* in: if the spec
        set is identical the crypto output is byte-identical, so the skip is
        correct by construction.

        Folding the realized window timestamps (rather than a ``now_unix //
        period`` term) closes the phase-relative drift gap: the lock-tracking
        index is ``(now_unix - created_at) // period`` and the relative index is
        ``(now_unix - anchor) // period``, so a non-period-aligned anchor rolls
        a drift window at a different ``now_unix`` than the absolute rollover --
        two clocks could share the old absolute-counter signature yet need
        different window sets. By hashing the windows the build actually
        produces, a missing or added drift window always changes the digest.

        Per work item the digest additionally folds the identity fields that
        reach the build output but never the window math -- ``registry_id`` /
        ``canonical_id`` / ``config_entry_id`` are emitted verbatim into every
        ``EIDMatch``. Sorting makes the digest independent of iteration order.
        """

        hasher = hashlib.blake2b(digest_size=32)

        def _item_key(item: WorkItem) -> tuple[str, ...]:
            # Identity fields that flow straight into ``EIDMatch`` output but are
            # invisible to the window/variant derivation below.
            fields: list[str] = [
                item.registry_id,
                item.canonical_id,
                item.config_entry_id,
                item.key_bytes.hex(),
                "" if item.locked_variant is None else item.locked_variant.value,
            ]
            # Replay the pure window/variant derivation and fold the realized
            # specs. ``_compute_time_windows`` is read-only with respect to
            # ``self`` (it only reads the ``_known_offsets`` snapshot); see the
            # AP-C offload contract. The realized spec set fully determines the
            # skipped crypto output, so identical specs => identical lookup.
            windows, invalid_hint = self._compute_time_windows(
                item, now_unix=now_unix, params=params
            )
            fields.append("1" if invalid_hint else "0")
            for window in windows:
                for variant_spec in self._compute_variants(item, window):
                    fields.append(
                        "|".join(
                            (
                                variant_spec.variant.value,
                                str(int(variant_spec.include_reverse)),
                                window.time_basis,
                                str(variant_spec.window.timestamp),
                                str(variant_spec.window.semantic_offset),
                            )
                        )
                    )
            return tuple(fields)

        for key in sorted(_item_key(item) for item in work_items):
            hasher.update(b"W")
            for field_value in key:
                hasher.update(field_value.encode())
                hasher.update(b"\x00")

        return hasher.hexdigest()

    async def _refresh_cache(self) -> None:
        """Rebuild the EID lookup table for all active devices."""

        self._ensure_cache_defaults()
        if self._load_task is not None:
            await asyncio.shield(self._load_task)

        now_unix = int(time.time())
        self._purge_stale_locks(now=now_unix)
        rotation_params = self._build_rotation_params()

        identities = await self._collect_device_secrets()
        # Cache identities for heuristic phone discovery
        self._cached_identities = list(identities)
        _LOGGER.debug("Refresh start: %d identities discovered", len(identities))
        work_items = self._collect_work_items(identities, now_unix=now_unix)
        _LOGGER.debug("Refresh stage: collected %d work items", len(work_items))

        # Skip-guard (AP-B1): the build phase below is a pure function of the
        # work items and the per-period rotation counters. When the signature is
        # unchanged, rebuilding would reproduce a byte-identical lookup table, so
        # we keep the existing one and skip the expensive CacheBuilder fan-out.
        # The cheap mandatory side effects above (_purge_stale_locks,
        # _cached_identities, _collect_device_secrets) and the invalid-hint
        # bookkeeping in _collect_work_items still run on every trigger. The
        # build-loop's own ``_known_timebases.pop`` (invalid-hint) is safe to
        # skip: ``invalid_hint`` is a pure function of the work item's
        # ``basis_hint`` (plus its lock-derived window basis), so an unchanged
        # signature means the hint state is unchanged; any newly invalid hint
        # changes ``basis_hint`` (read back from _known_timebases), which changes
        # the signature and forces a rebuild that runs the pop.
        build_signature = self._build_signature(
            work_items, now_unix=now_unix, params=rotation_params
        )
        if self._last_build_signature == build_signature:
            _LOGGER.debug(
                "Refresh stage: build signature unchanged, skipping rebuild "
                "(%d work items, %d cached EIDs)",
                len(work_items),
                len(self._lookup),
            )
            return

        # Offload the pure synchronous build to a worker thread (AP-C). The
        # build block touches ``self`` only through the two thread-safe AP-A
        # memo caches (``_eid_memo``/``_flags_mask_memo``), whose locks guard
        # the dict access only, and reads the immutable learned ``_known_offsets``
        # snapshot via ``_compute_time_windows``. Every genuine ``self.*``
        # mutation stays on the loop: the invalid-hint ``_known_timebases.pop``
        # below (driven by the registry ids returned from the build) and the
        # ``_lookup``/``_lookup_metadata`` assignment after the executor returns.
        # The cooperative ``asyncio.sleep(0)`` yield is gone because no event
        # loop exists inside the worker thread; running the whole build off-loop
        # keeps the loop reactive without per-item yields.
        (
            lookup,
            lookup_metadata,
            invalid_hint_ids,
        ) = await self.hass.async_add_executor_job(
            self._build_lookup_sync,
            work_items,
            now_unix,
            rotation_params,
        )

        # Loop-side ``self.*`` mutations (single-threaded, no executor in flight).
        for registry_id in invalid_hint_ids:
            self._known_timebases.pop(registry_id, None)
        self._lookup, self._lookup_metadata = lookup, lookup_metadata
        # Record the signature only after a successful build so a failure leaves
        # the guard cleared and the next trigger rebuilds (AP-B1).
        self._last_build_signature = build_signature
        _LOGGER.debug(
            "Refresh stage: finalize complete (lookup=%d, metadata=%d)",
            len(self._lookup),
            len(self._lookup_metadata),
        )
        _LOGGER.debug(
            "Refreshed EID cache for %d devices (%d cached EIDs)",
            len(work_items),
            len(self._lookup),
        )

    def _build_lookup_sync(
        self,
        work_items: list[WorkItem],
        now_unix: int,
        rotation_params: RotationParams,
    ) -> tuple[
        dict[bytes, list[EIDMatch]],
        dict[bytes, dict[str, Any]],
        list[str],
    ]:
        """Build the EID lookup table off the event loop (AP-C).

        Pure synchronous build over the loop-side inputs ``work_items`` /
        ``now_unix`` / ``rotation_params``. Designed to run inside a worker
        thread via ``hass.async_add_executor_job``: it performs no ``await``,
        touches no event loop, and mutates no resolver state except the two
        thread-safe AP-A memo caches (``_eid_memo`` / ``_flags_mask_memo``),
        which serialize only on the bounded dict access. ``_compute_time_windows``
        and its callees read the learned ``_known_offsets`` snapshot but never
        write it, and the ``CacheBuilder`` is thread-local to this call.

        Returns the finalized ``(lookup, metadata)`` tables plus the list of
        registry ids whose basis hint was invalid; the caller pops their
        ``_known_timebases`` entries on the loop, where it is safe to mutate.
        """

        builder = CacheBuilder()
        invalid_hint_ids: list[str] = []

        # Pass one: the variant specs of the whole build, and the exact memo
        # keys they will use (same tuples as _generate_variant and
        # _compute_flags_xor_mask build), so both memos can be sized first.
        planned: list[tuple[WorkItem, list[VariantSpec]]] = []
        eid_keys: set[tuple[bytes, int, EidVariant]] = set()
        mask_keys: set[tuple[bytes, int, int, int, ScalarDerivation]] = set()
        for work_item in work_items:
            windows, invalid_hint = self._compute_time_windows(
                work_item, now_unix=now_unix, params=rotation_params
            )
            if invalid_hint:
                invalid_hint_ids.append(work_item.registry_id)
            _LOGGER.debug(
                "Refresh stage: %s produced %d window groups",
                work_item.registry_id,
                len(windows),
            )
            specs = [
                spec
                for window in windows
                for spec in self._compute_variants(work_item, window)
            ]
            for spec in specs:
                timestamp = spec.window.timestamp
                eid_keys.add((spec.key_bytes, timestamp, spec.variant))
                curve_params = _VARIANT_CURVE_PARAMS.get(spec.variant)
                if curve_params is not None:
                    mask_keys.add((spec.key_bytes, timestamp, *curve_params))
            planned.append((work_item, specs))

        eid_bound = _memo_capacity(
            len(eid_keys), floor=_EID_MEMO_MAXSIZE, cap=_EID_MEMO_HARD_CAP
        )
        mask_bound = _memo_capacity(
            len(mask_keys), floor=_EID_MASK_MEMO_MAXSIZE, cap=_EID_MASK_MEMO_HARD_CAP
        )
        # Grow now, shrink after pass two (see _MEMO_HEADROOM).
        self._eid_memo.resize(max(eid_bound, self._eid_memo.maxsize))
        self._flags_mask_memo.resize(max(mask_bound, self._flags_mask_memo.maxsize))

        try:
            # Pass two: generate and register.
            for work_item, specs in planned:
                fingerprint = identity_key_fingerprint(work_item.key_bytes)
                for variant_spec in specs:
                    xor_mask: int | None = None
                    try:
                        curve_len, curve_ord, derivation = _VARIANT_CURVE_PARAMS[
                            variant_spec.variant
                        ]
                        xor_mask = self._compute_flags_xor_mask(
                            variant_spec.key_bytes,
                            variant_spec.window.timestamp,
                            curve_byte_len=curve_len,
                            curve_order=curve_ord,
                            derivation=derivation,
                        )
                    except Exception:  # noqa: BLE001
                        pass
                    for generated in self._generate_eids_from_spec(variant_spec):
                        match = EIDMatch(
                            device_id=work_item.registry_id,
                            config_entry_id=work_item.config_entry_id,
                            canonical_id=work_item.canonical_id,
                            time_offset=generated.window.semantic_offset,
                            is_reversed=generated.is_reversed,
                        )
                        builder.register_eid(
                            generated.eid_bytes,
                            match=match,
                            variant=generated.variant,
                            window=generated.window,
                            advertisement_reversed=generated.is_reversed,
                            flags_xor_mask=xor_mask,
                            key_fingerprint=fingerprint,
                        )
        finally:
            # Shrink even when pass two raised, so a failed build does not
            # leave the grown bound in place until the next one.
            self._eid_memo.resize(eid_bound)
            self._flags_mask_memo.resize(mask_bound)

        lookup, lookup_metadata = builder.finalize()
        return lookup, lookup_metadata, invalid_hint_ids

    def _normalize_variant_value(self, raw: object, *, eid_len: int) -> str:
        """Return a valid `EidVariant` value string for persistence."""

        try:
            return EidVariant(str(raw)).value
        except Exception:
            pass

        if isinstance(raw, str):
            stripped = raw.strip()
            if stripped:
                try:
                    return EidVariant(stripped).value
                except Exception:
                    if stripped.isdigit():
                        try:
                            return EidVariant(str(int(stripped))).value
                        except Exception:
                            pass
        elif isinstance(raw, int) and not isinstance(raw, bool):
            try:
                return EidVariant(str(raw)).value
            except Exception:
                pass

        inferred = self._infer_variant_from_length(eid_len)
        try:
            return EidVariant(inferred).value
        except Exception:
            return inferred

    @staticmethod
    def _infer_variant_from_length(eid_length: int) -> str:
        """Infer a reasonable variant string from the observed EID length."""

        if eid_length == LEGACY_EID_LENGTH:
            return EidVariant.LEGACY_SECP160R1_X20_BE.value
        if eid_length == MODERN_EID_LENGTH:
            return EidVariant.MODERN_P256_X32_BE.value
        return EidVariant.MODERN_P256_X20_TRUNC_BE.value

    def _generate_variant(
        self,
        key_bytes: bytes,
        *,
        time_counter: int,
        variant: EidVariant,
    ) -> bytes:
        """Generate an EID for a specific profile.

        Memoized per resolver instance on ``(key_bytes, time_counter,
        variant)``. The derivation is pure, so a hit returns the byte-identical
        EID without recomputation. Cache invalidation is implicit: key rotation
        changes ``time_counter`` and re-keying changes ``key_bytes``, both of
        which produce a fresh key and let stale entries age out via the LRU.
        """

        cache_key = (key_bytes, time_counter, variant)
        cached = self._eid_memo.get(cache_key)
        if cached is not None:
            return cast(bytes, cached)

        # Compute outside the cache lock so parallel workers (AP-C) are not
        # serialized on the crypto call.
        eid_bytes = generate_eid_variant(
            key_bytes,
            time_counter,
            variant,
            strict=False,
        )
        self._eid_memo.put(cache_key, eid_bytes)
        return eid_bytes

    def _compute_flags_xor_mask(
        self,
        key_bytes: bytes,
        time_counter: int,
        *,
        curve_byte_len: int,
        curve_order: int | None,
        derivation: ScalarDerivation,
    ) -> int:
        """Memoized wrapper around ``compute_flags_xor_mask``.

        Keyed per resolver instance on ``(key_bytes, time_counter,
        curve_byte_len, curve_order, derivation)``. The mask is a pure function
        of these inputs, so a hit returns the identical integer without
        recomputation. ``derivation`` belongs in the key: two variants on the
        same curve with different derivations have different masks.
        Invalidation is implicit via key rotation, identical to
        ``_generate_variant``.
        """

        cache_key = (key_bytes, time_counter, curve_byte_len, curve_order, derivation)
        cached = self._flags_mask_memo.get(cache_key)
        if cached is not None:
            return cast(int, cached)

        # Compute outside the cache lock so parallel workers (AP-C) are not
        # serialized on the crypto call.
        mask = compute_flags_xor_mask(
            key_bytes,
            time_counter,
            curve_byte_len=curve_byte_len,
            curve_order=curve_order,
            derivation=derivation,
        )
        self._flags_mask_memo.put(cache_key, mask)
        return mask

    def _generate_heuristic_eid(  # noqa: PLR0913
        self,
        key_bytes: bytes,
        now_unix: int,
        *,
        rotation_period: int,
        basis: HeuristicBasis,
        variant: EidVariant,
        anchor: int | None,
        drift_offsets: tuple[int, ...] = (-1, 0, 1),
    ) -> list[HeuristicEidResult]:
        """Memoized wrapper around ``generate_heuristic_eid`` (AP-SWEEP).

        ``generate_heuristic_eid`` runs on the event loop for every cache-miss
        FMDN advertisement (``_resolve_eid_internal`` -> ``_heuristic_resolve``).
        Its output list is a pure function of the rotation-aligned time window
        and the hypothesis parameters, so consecutive advertisements from the
        same nearby phone within one window recompute identical lists. This
        wrapper turns that repeat into a dict lookup.

        The cache key carries every parameter that influences the output --
        ``key_bytes``, the rotation-aligned window start, ``rotation_period``,
        ``basis``, ``variant`` and ``anchor`` -- so a stale value can never be
        served: a window-boundary rounding difference can at worst miss the
        cache and recompute (still byte-exact), never collide across distinct
        inputs. The window start mirrors ``_align_to_rotation_flexible``'s
        ``(t // period) * period`` over the same time value the crypto aligns
        (``now_unix`` for ABSOLUTE, ``now_unix - anchor`` for RELATIVE).
        Invalidation is implicit: a new window or re-keying yields a fresh key
        and stale entries age out via the LRU. ``drift_offsets`` defaults to the
        same value the underlying generator uses; it is folded into the key (and
        forwarded explicitly rather than relying on the callee default) so a
        future caller that varies it can never be served a stale entry. The
        added key cardinality is negligible (one constant tuple in practice).
        """

        if basis == HeuristicBasis.RELATIVE and anchor is not None and anchor > 0:
            aligned_source = max(now_unix - anchor, 0)
        else:
            aligned_source = now_unix
        window_start = (
            (aligned_source // rotation_period) * rotation_period
            if rotation_period > 0
            else aligned_source
        )

        cache_key = (
            key_bytes,
            window_start,
            rotation_period,
            basis,
            variant,
            anchor,
            drift_offsets,
        )
        cached = self._heuristic_memo.get(cache_key)
        if cached is not None:
            return cast(list[HeuristicEidResult], cached)

        results = generate_heuristic_eid(
            key_bytes,
            now_unix,
            rotation_period=rotation_period,
            basis=basis,
            variant=variant,
            anchor=anchor,
            drift_offsets=drift_offsets,
        )
        self._heuristic_memo.put(cache_key, results)
        return results

    async def _collect_device_secrets(self) -> list[DeviceIdentity]:
        """Retrieve active tracker identities from all loaded coordinators."""
        bucket = self.hass.data.get(DOMAIN)
        if not isinstance(bucket, dict):
            _LOGGER.warning(
                "EID pipeline disabled: hass.data[%s] is not a dict (type=%s). "
                "This may indicate the integration was not properly initialized.",
                DOMAIN,
                type(bucket).__name__,
            )
            return []

        entries = bucket.get("entries")
        if not isinstance(entries, dict):
            _LOGGER.warning(
                "EID pipeline disabled: hass.data[%s]['entries'] is not a dict (type=%s). "
                "Falling back to empty identity list.",
                DOMAIN,
                type(entries).__name__ if entries is not None else "None",
            )
            return []

        identities: list[DeviceIdentity] = []
        for runtime in entries.values():
            coordinator: _IdentityProvider | None = None
            if isinstance(runtime, GoogleFindMyCoordinator):
                coordinator = runtime
            else:
                candidate = getattr(runtime, "coordinator", None)
                if isinstance(candidate, _IdentityProvider):
                    coordinator = candidate
                elif isinstance(runtime, _IdentityProvider):
                    coordinator = runtime

            if coordinator is None:
                continue

            identities.extend(
                await self._normalize_identities(
                    coordinator.get_active_device_identities(),
                    cache=getattr(coordinator, "cache", None),
                )
            )

        return identities

    async def _normalize_identities(
        self,
        identities: Sequence[DeviceIdentity | Mapping[str, Any]],
        *,
        cache: TokenCache | None,
    ) -> list[DeviceIdentity]:
        """Ensure each device identity has a usable plaintext key."""

        normalized: list[DeviceIdentity] = []
        for identity in identities:
            if isinstance(identity, Mapping):
                registry_id = identity.get("registry_id")
                canonical_id = identity.get("canonical_id")
                if not isinstance(registry_id, str) or not isinstance(
                    canonical_id, str
                ):
                    continue

                manufacturer = _normalize_optional_string(identity.get("manufacturer"))
                model = _normalize_optional_string(identity.get("model"))
                encrypted_account_key = _normalize_encrypted_blob(
                    identity.get("encrypted_account_key")
                    or identity.get("encryptedAccountKey")
                )
                public_key_address = _normalize_encrypted_blob(
                    identity.get("public_key_address")
                    or identity.get("encryptedSha256AccountKeyPublicAddress")
                )

                target_identity = DeviceIdentity(
                    registry_id=registry_id,
                    canonical_id=canonical_id,
                    identity_key=identity.get("identity_key"),
                    encrypted_identity_key=identity.get("encrypted_identity_key"),
                    owner_key_version=identity.get("owner_key_version"),
                    device_type=identity.get("device_type"),
                    config_entry_id=identity.get("config_entry_id"),
                    fast_pair_model_id=identity.get("fast_pair_model_id"),
                    manufacturer=manufacturer,
                    model=model,
                    pair_date=identity.get("pair_date"),
                    secrets_creation_date=identity.get("secrets_creation_date"),
                    encrypted_account_key=encrypted_account_key,
                    public_key_address=public_key_address,
                    time_anchors_debug=identity.get("time_anchors_debug"),
                    trackable_components=normalize_component_count(
                        identity.get("trackable_components")
                    ),
                )
            elif isinstance(identity, DeviceIdentity):
                target_identity = identity
            else:
                continue

            if target_identity.identity_key is None:
                result = await self._try_decrypt_identity_key(
                    target_identity, cache=cache
                )
                if result is None:
                    continue
                normalized.append(replace(target_identity, identity_key=result.key))
                continue

            normalized.append(target_identity)

        return normalized

    async def _try_decrypt_identity_key(
        self,
        identity: DeviceIdentity,
        *,
        cache: TokenCache | None,
    ) -> DecryptionResult | None:
        """Decrypt encrypted identity key when owner/shared key material is available."""
        # Lazy-load crypto exception for startup performance
        InvalidTag = get_invalid_tag_exception()

        self._ensure_cache_defaults()
        if cache is None:
            return None

        # FIX: Handle both bytes and hex string formats for encrypted_identity_key
        encrypted_identity_key = _normalize_encrypted_blob(
            identity.encrypted_identity_key
        )
        if encrypted_identity_key is None:
            return None

        owner_key_info = await self._resolve_owner_key(identity, cache=cache)
        if owner_key_info is None:
            return None

        key_sources: list[tuple[str, bytes]] = [("owner", owner_key_info.key)]
        try:
            _cached_user = await cache.get(username_string)
            shared_key = await async_get_shared_key(
                cache=cache,
                username=_cached_user if isinstance(_cached_user, str) else None,
            )
        except Exception:  # pragma: no cover - best-effort
            shared_key = None
        if shared_key is not None:
            key_sources.append(("shared", shared_key))
        suggested_mcu = is_mcu_tracker(
            device_type=identity.device_type,
            fast_pair_model_id=identity.fast_pair_model_id,
        )
        candidates = [suggested_mcu, not suggested_mcu]

        for key_source, wrapping_key in key_sources:
            for flip_mcu in candidates:
                candidate_key = (
                    flip_bits(encrypted_identity_key, True)
                    if flip_mcu
                    else encrypted_identity_key
                )
                try:
                    decrypted = await asyncio.to_thread(
                        decrypt_eik, wrapping_key, candidate_key
                    )
                except InvalidTag:
                    envelope = self._unwrap_aesgcm_envelope(
                        envelope=candidate_key,
                        wrapping_key=wrapping_key,
                        key_source=key_source,
                        aad_label="registry_id",
                        aad_value=identity.registry_id,
                    )
                    if envelope is not None:
                        self._decryption_status[identity.registry_id] = (
                            envelope.metadata.get("status", "")
                        )
                        return envelope
                    continue
                except Exception:  # noqa: BLE001
                    continue

                if isinstance(decrypted, (bytes, bytearray)):
                    metadata: dict[str, Any] = {
                        "status": "decrypted",
                        "key_source": key_source,
                    }
                    if owner_key_info.version is not None:
                        metadata["key_version"] = owner_key_info.version

                    result = DecryptionResult(key=bytes(decrypted), metadata=metadata)
                    self._decryption_status[identity.registry_id] = metadata["status"]
                    return result

        return None

    def _unwrap_aesgcm_envelope(
        self,
        *,
        envelope: bytes,
        wrapping_key: bytes,
        key_source: str,
        aad_label: str,
        aad_value: str,
    ) -> DecryptionResult | None:
        """Attempt to unwrap an AES-GCM envelope using the provided key."""
        # Lazy-load crypto classes for startup performance
        AESGCM = get_aesgcm_class()
        InvalidTag = get_invalid_tag_exception()

        if len(envelope) <= AESGCM_NONCE_LENGTH:
            return None

        nonce = envelope[:AESGCM_NONCE_LENGTH]
        ciphertext = envelope[AESGCM_NONCE_LENGTH:]
        aad = aad_value.encode()

        try:
            plaintext = AESGCM(wrapping_key).decrypt(nonce, ciphertext, aad)
        except InvalidTag:
            return None

        metadata: dict[str, Any] = {
            "status": "decrypted",
            "mode": "aesgcm_envelope",
            "aad_label": aad_label,
            "key_source": key_source,
        }
        return DecryptionResult(key=plaintext, metadata=metadata)

    async def _resolve_owner_key(
        self,
        identity: DeviceIdentity,
        *,
        cache: TokenCache,
    ) -> OwnerKeyInfo | None:
        """Fetch and refresh owner key material when needed."""

        async def _fetch(*, force_refresh: bool) -> OwnerKeyInfo | None:
            try:
                return await async_get_owner_key(
                    cache=cache, force_refresh=force_refresh
                )
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug(
                    "Failed to retrieve owner key for %s (force_refresh=%s): %s",
                    identity.canonical_id,
                    force_refresh,
                    err,
                )
                return None

        owner_key_info = await _fetch(force_refresh=False)
        if owner_key_info is None:
            return None

        if (
            identity.owner_key_version is not None
            and owner_key_info.version is not None
            and owner_key_info.version < identity.owner_key_version
        ):
            _LOGGER.info(
                "Owner Key Version mismatch detected: Tracker requires V%s, "
                "Cache has V%s. Refreshing...",
                identity.owner_key_version,
                owner_key_info.version,
            )
            refreshed = await _fetch(force_refresh=True)
            if refreshed is not None:
                return refreshed

        return owner_key_info

    # =========================================================================
    # Heuristic Phone Discovery
    # =========================================================================
    # These methods implement a reactive heuristic check for Android phones
    # that don't follow the standard FMDN tracker protocol. When a packet
    # fails the normal cache lookup, these methods test alternative hypotheses
    # (different rotation periods and time bases) to find a match.
    # =========================================================================

    def _heuristic_resolve(
        self,
        candidates: list[bytes],
        *,
        now_unix: int,
    ) -> EIDMatch | None:
        """Attempt heuristic resolution for packets that missed the standard cache.

        This method tests multiple hypotheses about the EID generation parameters:
        - Rotation periods: 900s, 1024s, 3600s
        - Time bases: ABSOLUTE (now // period) vs RELATIVE ((now - anchor) // period)

        Args:
            candidates: EID byte candidates extracted from the BLE payload.
            now_unix: Current Unix timestamp in seconds.

        Returns:
            EIDMatch if a heuristic match is found, None otherwise.
        """
        if not ENABLE_HEURISTIC_PHONE_DISCOVERY:
            return None

        if not self._cached_identities:
            _LOGGER.debug("Heuristic resolve skipped: no cached identities")
            return None

        # Build a lookup set for fast candidate matching
        candidate_set = set(candidates)

        # First, check devices with learned parameters (fast path)
        for device_id, learned in self._learned_heuristic_params.items():
            match = self._heuristic_check_learned(
                device_id,
                learned,
                candidate_set,
                now_unix=now_unix,
            )
            if match is not None:
                return match

        # Fall back to full hypothesis testing (slow path)
        for identity in self._cached_identities:
            if identity.identity_key is None:
                continue

            match = self._heuristic_test_hypotheses(
                identity,
                candidate_set,
                now_unix=now_unix,
            )
            if match is not None:
                return match

        return None

    def _heuristic_check_learned(
        self,
        device_id: str,
        learned: LearnedHeuristicParams,
        candidate_set: set[bytes],
        *,
        now_unix: int,
    ) -> EIDMatch | None:
        """Check if a payload matches a device with previously learned parameters.

        This is the fast path for devices that have been successfully matched
        using heuristic parameters before.
        """
        # Find the identity for this device
        identity: DeviceIdentity | None = None
        for ident in self._cached_identities:
            if ident.registry_id == device_id:
                identity = ident
                break

        if identity is None or identity.identity_key is None:
            return None

        key_bytes = self._normalize_key_bytes(identity.identity_key)
        if key_bytes is None:
            return None

        anchor = self._get_best_anchor(identity)

        try:
            results = self._generate_heuristic_eid(
                key_bytes,
                now_unix,
                rotation_period=learned.rotation_period,
                basis=learned.basis,
                variant=learned.variant,
                anchor=anchor if learned.basis == HeuristicBasis.RELATIVE else None,
            )
        except Exception as exc:
            _LOGGER.debug(
                "Heuristic learned check failed for %s: %s",
                device_id,
                exc,
            )
            return None

        for result in results:
            if result.eid_bytes in candidate_set:
                # Update confirmation tracking
                learned.last_confirmed_at = now_unix
                learned.confirmation_count += 1

                _LOGGER.info(
                    "HEURISTIC HIT (learned): device=%s period=%ds basis=%s "
                    "variant=%s drift=%d reversed=%s",
                    device_id,
                    learned.rotation_period,
                    learned.basis.value,
                    learned.variant.value,
                    result.drift_offset,
                    result.is_reversed,
                )

                return EIDMatch(
                    device_id=device_id,
                    config_entry_id=str(identity.config_entry_id or ""),
                    canonical_id=identity.canonical_id,
                    time_offset=result.drift_offset * learned.rotation_period,
                    is_reversed=result.is_reversed,
                )

        return None

    def _heuristic_test_hypotheses(
        self,
        identity: DeviceIdentity,
        candidate_set: set[bytes],
        *,
        now_unix: int,
    ) -> EIDMatch | None:
        """Test all hypotheses against a device identity.

        This is the slow path that tests all combinations of rotation periods,
        time bases, and EID variants.
        """
        key_bytes = self._normalize_key_bytes(identity.identity_key)
        if key_bytes is None:
            return None

        anchor = self._get_best_anchor(identity)
        device_id = identity.registry_id

        for rotation_period, basis in HEURISTIC_HYPOTHESES:
            # Skip RELATIVE basis if no valid anchor
            if basis == HeuristicBasis.RELATIVE and (anchor is None or anchor <= 0):
                continue

            for variant in HEURISTIC_VARIANTS_TO_TEST:
                try:
                    results = self._generate_heuristic_eid(
                        key_bytes,
                        now_unix,
                        rotation_period=rotation_period,
                        basis=basis,
                        variant=variant,
                        anchor=anchor if basis == HeuristicBasis.RELATIVE else None,
                    )
                except Exception as exc:
                    _LOGGER.debug(
                        "Heuristic generation failed: device=%s period=%d basis=%s variant=%s: %s",
                        device_id,
                        rotation_period,
                        basis.value,
                        variant.value,
                        exc,
                    )
                    continue

                for result in results:
                    if result.eid_bytes in candidate_set:
                        # Found a match! Store the learned parameters
                        self._learned_heuristic_params[device_id] = (
                            LearnedHeuristicParams(
                                device_id=device_id,
                                canonical_id=identity.canonical_id,
                                rotation_period=rotation_period,
                                basis=basis,
                                variant=variant,
                                discovered_at=now_unix,
                                last_confirmed_at=now_unix,
                                confirmation_count=1,
                            )
                        )

                        _LOGGER.info(
                            "HEURISTIC HIT (discovery): device=%s period=%ds basis=%s "
                            "variant=%s drift=%d reversed=%s. Caching parameters.",
                            device_id,
                            rotation_period,
                            basis.value,
                            variant.value,
                            result.drift_offset,
                            result.is_reversed,
                        )

                        return EIDMatch(
                            device_id=device_id,
                            config_entry_id=str(identity.config_entry_id or ""),
                            canonical_id=identity.canonical_id,
                            time_offset=result.drift_offset * rotation_period,
                            is_reversed=result.is_reversed,
                        )

        return None

    def _normalize_key_bytes(self, key: Any) -> bytes | None:
        """Normalize an identity key to bytes."""
        if isinstance(key, (bytes, bytearray)):
            return bytes(key)
        if isinstance(key, str):
            try:
                return bytes.fromhex(key)
            except ValueError:
                return None
        return None

    def _get_best_anchor(self, identity: DeviceIdentity) -> int | None:
        """Get the best available time anchor for an identity."""
        anchors: list[int] = []
        if isinstance(identity.pair_date, int) and identity.pair_date > 0:
            anchors.append(identity.pair_date)
        if (
            isinstance(identity.secrets_creation_date, int)
            and identity.secrets_creation_date > 0
        ):
            anchors.append(identity.secrets_creation_date)
        return max(anchors) if anchors else None

    def _should_log_heuristic_miss(self, raw_prefix: str) -> bool:
        """Check if a heuristic miss should be logged (rate limited)."""
        now = time.time()
        last_log = self._heuristic_miss_log_at.get(raw_prefix)
        if last_log is not None and now - last_log < HEURISTIC_LOG_INTERVAL_SECONDS:
            return False
        self._heuristic_miss_log_at[raw_prefix] = now
        return True

    def _confirm_lock(self, device_id: str, now: int) -> None:
        """Record ``now`` as the lock confirmation unless a newer one is on record.

        ``now`` is the observation, not the processing time, and observations
        do not arrive in order: HA before 2026.8 replays its advertisement
        history in dict order, so an older sighting of a rotating tracker can
        follow a newer one. The confirmation feeds the lock TTL and must never
        move backwards.
        """
        previous = self._last_lock_confirmation.get(device_id)
        if previous is None or now > previous:
            self._last_lock_confirmation[device_id] = now

    def _note_confirmed_sighting(
        self,
        device_id: str,
        metadata: Mapping[str, Any],
        *,
        observed_at: int,
        uwt_mode: bool | None = None,
        uwt_source: UwtSource = UwtSource.UNKNOWN,
    ) -> None:
        """Record the sighting of a lookup match for the finder.

        The caller passes only sightings that are not older than the newest
        one on the monotonic clock. What is left are advertisements that
        Bermuda delivers late without a scanner timestamp and that are
        therefore dated "now": such a sighting carries an older window, and it
        must not replace a newer window seen at most one rotation period
        earlier. A smaller counter seen more than one period later on the wall
        clock is taken as a device that restarted its counter and replaces; a
        wall clock stepped forward by more than one period looks the same.
        After any step back of the wall clock, a smaller counter is either such
        a late window or a restarted device, and the two cannot be told apart:
        the entry is dropped, so nothing is reported until the next sighting
        records the window the device shows. A step back smaller than the time
        since the previous sighting leaves a positive gap and is not detected.
        The same counter replaces: the sighting is newer on the monotonic
        clock, also after such a step. A sighting matched under another
        identity key (by ``key_fingerprint``) always replaces, before any of
        these comparisons: its counter is not comparable with the previous
        one, and after the device is paired again it starts anew.

        Every sighting carries only the protection mode it showed itself,
        also within the same window: nothing is carried over from the
        sighting it replaces. The mode can change within one rotation window,
        and a mode taken from an earlier sighting would be reported with the
        later sighting's time. A bare EID therefore records an unknown mode
        (the finder then sends nothing), and a later reading of the frame
        type alone decides for itself, also after a ``CONFLICT``.

        Never raises: a variant this version does not know, or a window
        timestamp that is not an int in ``[0, FHNA_COUNTER_MASK]``, records
        nothing; a key fingerprint that is not exactly ``bytes`` of the
        fingerprint length is recorded as ``None``.
        """
        window_ts = metadata.get("rotation_timestamp")
        if (
            not isinstance(window_ts, int)
            or isinstance(window_ts, bool)
            or not 0 <= window_ts <= FHNA_COUNTER_MASK
        ):
            return
        raw_variant = metadata.get("variant")
        if not isinstance(raw_variant, str):
            return
        try:
            variant = EidVariant(raw_variant)
        except ValueError:
            return
        window_counter = window_ts - window_ts % ROTATION_PERIOD
        raw_fingerprint = metadata.get("key_fingerprint")
        # The exact type, not isinstance: a bytes subclass could raise from
        # len() or ==, and this method never raises.
        key_fingerprint = (
            raw_fingerprint
            if type(raw_fingerprint) is bytes
            and len(raw_fingerprint) == _KEY_FINGERPRINT_SIZE
            else None
        )
        previous = self._confirmed_sightings.get(device_id)
        if (
            previous is not None
            and previous.key_fingerprint == key_fingerprint
            and window_counter < previous.window_counter
        ):
            wall_gap = observed_at - previous.observed_at
            if wall_gap < 0:
                del self._confirmed_sightings[device_id]
                return
            if wall_gap <= ROTATION_PERIOD:
                return
        self._confirmed_sightings[device_id] = ConfirmedSighting(
            variant=variant,
            window_counter=window_counter,
            observed_at=observed_at,
            uwt_mode=uwt_mode,
            uwt_source=uwt_source,
            key_fingerprint=key_fingerprint,
        )

    def _sighting_is_older_than_the_lock(
        self,
        device_id: str,
        existing_lock: EIDGenerationLock | None,
        *,
        now: int,
        now_monotonic: float | None,
    ) -> bool:
        """Whether the lock already reflects a newer sighting than this one.

        An older sighting can be delivered after a newer one (HA before
        2026.8 replays the advertisement history in dict order, and a tracker
        rotates its address, so the history can hold several of its
        sightings); drift and ``last_seen_at`` must not roll back to it.
        Within this process the sightings are ordered on the monotonic
        clock: the wall clock can be stepped backwards while HA runs, and a
        guard on it would then hold every live sighting as older until wall
        time caught up. Only when no sighting of the device is on record in
        this process (first sighting after a restart) is the persisted wall
        stamp the reference; it is whole seconds, so that comparison resolves
        to the second: two sightings within one wall second are applied in
        delivery order, the next live packet settles the drift.
        """
        last_seen_monotonic = self._lock_last_seen_monotonic.get(device_id)
        if now_monotonic is not None and last_seen_monotonic is not None:
            return now_monotonic < last_seen_monotonic
        return (
            existing_lock is not None
            and existing_lock.last_seen_at is not None
            and now < existing_lock.last_seen_at
        )

    def _update_match_state(  # noqa: PLR0913
        self,
        match: EIDMatch,
        *,
        metadata: dict[str, Any],
        candidate: bytes,
        observed_frame: int | None,
        candidate_prefix: str,
        raw_prefix: str,
        now: int,
        now_monotonic: float | None = None,
        uwt_mode: bool | None = None,
        uwt_source: UwtSource = UwtSource.UNKNOWN,
    ) -> None:
        """Update internal state (locks, offsets, etc.) for a single match.

        ``now`` is the observation on the wall clock (whole seconds, the
        persisted lock schema); ``now_monotonic`` is the same observation on
        the monotonic clock and orders the writes within this process.
        """
        self._confirm_lock(match.device_id, now)
        previous_basis = _normalize_anchor_basis(
            self._known_timebases.get(match.device_id)
        )
        raw_time_basis = metadata.get("timestamp_basis")
        anchor_basis = _normalize_anchor_basis(raw_time_basis)
        basis_explicitly_invalid = raw_time_basis is not None and anchor_basis is None
        if anchor_basis is None:
            if raw_time_basis is None:
                anchor_basis = previous_basis
        elif raw_time_basis != anchor_basis:
            self._known_timebases.pop(match.device_id, None)
        if anchor_basis is None and not basis_explicitly_invalid:
            self._known_timebases.pop(match.device_id, None)

        existing_lock = self._locks.get(match.device_id)
        stale = self._sighting_is_older_than_the_lock(
            match.device_id, existing_lock, now=now, now_monotonic=now_monotonic
        )
        if existing_lock is None:
            variant_value = self._normalize_variant_value(
                metadata.get("variant"),
                eid_len=len(candidate),
            )
            rotation_timestamp = metadata.get("rotation_timestamp")
            lock = EIDGenerationLock(
                device_id=match.device_id,
                canonical_id=match.canonical_id,
                variant=variant_value,
                advertisement_reversed=match.is_reversed,
                eid_length=len(candidate),
                rotation_timestamp=int(rotation_timestamp)
                if isinstance(rotation_timestamp, int)
                and not isinstance(rotation_timestamp, bool)
                else None,
                frame_type=observed_frame,
                time_basis=anchor_basis,
                created_at=now,
                drift_offset=match.time_offset,
                last_seen_at=now,
            )
            self._locks[match.device_id] = lock
            self._persisted_locks[match.device_id] = lock
            self._schedule_lock_save()
        elif stale:
            pass
        else:
            # Update drift tracking on existing lock
            drift_changed = existing_lock.drift_offset != match.time_offset
            existing_lock.drift_offset = match.time_offset
            existing_lock.last_seen_at = now
            if drift_changed and match.time_offset != 0:
                _LOGGER.debug(
                    "Drift update for %s: offset=%d (using %s window)",
                    match.device_id,
                    match.time_offset,
                    "+1/+2" if match.time_offset > 0 else "-1/-2",
                )
            self._schedule_lock_save()

        if not stale and now_monotonic is not None:
            self._lock_last_seen_monotonic[match.device_id] = now_monotonic

        # Same ordering rule as drift_offset and last_seen_at: an older,
        # replayed sighting is ``stale`` (monotonic clock within this process)
        # and must not replace a newer one.
        if not stale:
            self._note_confirmed_sighting(
                match.device_id,
                metadata,
                observed_at=now,
                uwt_mode=uwt_mode,
                uwt_source=uwt_source,
            )

        self._known_advertisement_reversed[match.device_id] = match.is_reversed

        if anchor_basis is not None and not basis_explicitly_invalid and not stale:
            # Mirrors lock.drift_offset (restored from the lock on load), so
            # it follows the same newest-sighting rule.
            self._known_offsets[(match.device_id, anchor_basis)] = match.time_offset
            self._known_timebases[match.device_id] = anchor_basis

        _LOGGER.debug(
            (
                "HIT: device=%s canonical=%s reversed=%s offset=%s "
                "candidate_prefix=%s raw_prefix=%s"
            ),
            match.device_id,
            match.canonical_id,
            match.is_reversed,
            match.time_offset,
            candidate_prefix,
            raw_prefix,
        )

    def _resolve_eid_internal(  # noqa: PLR0911, PLR0912
        self, eid_bytes: bytes, *, clock: ObservationClock | None = None
    ) -> tuple[list[EIDMatch], bytes | None, int | None]:
        """Internal EID resolution returning all matches.

        Args:
            eid_bytes: Raw EID bytes from a BLE advertisement.
            clock: Observation time of the advertisement, derived once by
                the public entry point. Direct callers may omit it, in which
                case the observation is dated *now*.

        Returns:
            Tuple of (matches, matched_candidate, observed_frame).
            matches is empty if no match found.
        """
        self._ensure_cache_defaults()
        if clock is None:
            clock = _observation_clock(None)
        if not isinstance(eid_bytes, (bytes, bytearray)):
            return [], None, None

        raw = bytes(eid_bytes)
        # The payload-level frame type is not used here: every consumer below
        # takes it from the candidate that actually matched.
        candidates, _payload_frame = self._extract_eid_candidates(raw)
        raw_prefix = raw[:4].hex()
        candidate_prefixes = [candidate.eid[:4].hex() for candidate in candidates]

        if not candidates:
            _LOGGER.debug(
                "No candidates extracted from payload (raw_prefix=%s, len=%d)",
                raw_prefix,
                len(raw),
            )
            return [], None, None

        if not self._lookup:
            is_locked = self._refresh_lock.locked()
            if self._pending_refresh or is_locked:
                _LOGGER.debug(
                    "RESOLVER NOT READY: cache priming (pending=%s locked=%s raw_prefix=%s)",
                    self._pending_refresh,
                    is_locked,
                    raw_prefix,
                )
                return [], None, None

            _LOGGER.debug(
                "RESOLVER NOT READY: empty cache; scheduling refresh for raw_prefix=%s",
                raw_prefix,
            )
            refresh = getattr(self, "async_refresh", None)
            create_task = getattr(self.hass, "async_create_task", None)
            if callable(refresh) and callable(create_task):
                self._pending_refresh = True
                try:
                    refresh_coro = refresh()
                    scheduled = create_task(refresh_coro)
                    if scheduled is None:
                        refresh_coro.close()
                    elif asyncio.iscoroutine(scheduled):
                        asyncio.create_task(scheduled)
                except Exception:  # pragma: no cover
                    self._pending_refresh = False
            return [], None, None

        for candidate_prefix, candidate in zip(candidate_prefixes, candidates):
            matches = self._lookup.get(candidate.eid)
            if not matches:
                continue

            metadata: dict[str, Any] = self._lookup_metadata.get(candidate.eid) or {}
            # The lock confirmation is dated with the observation, not with
            # the processing: a replayed advertisement confirms the lock as
            # of its sighting. The same value becomes ``created_at`` and
            # ``last_seen_at`` of the EIDGenerationLock the match creates or
            # refreshes (persisted), which is where the sighting belongs.
            now = int(clock.wall)

            # ---------------------------------------------------------
            # FMDN BLE battery: decode flags + store per device
            # ---------------------------------------------------------
            # Runs before the match state is updated: the sighting recorded
            # there carries the protection mode decoded here.
            uwt_mode, uwt_source = self._observe_uwt_mode(
                raw, candidate, metadata, matches, clock=clock
            )

            # Update state for ALL matches (shared devices)
            for match in matches:
                # Intended, and worth naming because the value is persisted
                # via EIDGenerationLock.frame_type / to_dict(): for a
                # sliding-window hit this now stores None where the
                # payload-level byte used to be stored. None is the honest
                # answer when the match carries no frame geometry, and the
                # field has no production reader beyond the diagnostic dump.
                self._update_match_state(
                    match,
                    metadata=metadata,
                    candidate=candidate.eid,
                    observed_frame=candidate.frame_type,
                    candidate_prefix=candidate_prefix,
                    raw_prefix=raw_prefix,
                    now=now,
                    now_monotonic=clock.monotonic,
                    uwt_mode=uwt_mode,
                    uwt_source=uwt_source,
                )

            return matches, candidate.eid, candidate.frame_type

        # =================================================================
        # Heuristic Phone Discovery: Reactive check before logging MISS
        # =================================================================
        # If the standard cache lookup failed, attempt heuristic resolution
        # for Android phones that may use different rotation periods or
        # time bases than standard FMDN trackers.
        now_unix = int(time.time())
        heuristic_match = self._heuristic_resolve(
            [candidate.eid for candidate in candidates], now_unix=now_unix
        )
        if heuristic_match is not None:
            # Update standard tracking state for the heuristic match. The
            # search above runs against "now" (which EID would be current);
            # the confirmation stamp is the observation, like on the cache
            # hit path.
            self._confirm_lock(heuristic_match.device_id, int(clock.wall))
            self._known_advertisement_reversed[heuristic_match.device_id] = (
                heuristic_match.is_reversed
            )
            return [heuristic_match], None, None

        # Log MISS with rate limiting for heuristic failures
        max_prefixes = 8
        prefix_log = ", ".join(candidate_prefixes[:max_prefixes])
        if len(candidate_prefixes) > max_prefixes:
            prefix_log = f"{prefix_log}, ..."

        if self._should_log_heuristic_miss(raw_prefix):
            _LOGGER.debug(
                "MISS (after heuristic): candidate_prefixes=%s raw_prefix=%s "
                "cache_size=%d identities=%d",
                prefix_log or "<none>",
                raw_prefix,
                len(self._lookup),
                len(self._cached_identities),
            )
        return [], None, None

    # ------------------------------------------------------------------
    # FMDN BLE battery decode + store
    # ------------------------------------------------------------------
    def _observe_uwt_mode(
        self,
        raw: bytes,
        candidate: EidCandidate,
        metadata: dict[str, Any],
        matches: list[EIDMatch],
        *,
        clock: ObservationClock,
    ) -> tuple[bool | None, UwtSource]:
        """Decode the flags of a matched advertisement and return its mode.

        The match itself locates the payload: the optional hashed-flags byte,
        when present, is the byte right after the matched EID. The candidate
        is passed WHOLE, not pre-reduced to an offset: the callee must be able
        to tell "no geometry supplied" (direct test call) from "geometry
        known, no flags byte present" (production). An `int | None` cannot
        carry that distinction.

        The match path must never fail on the decoder: the caller updates the
        lock after this call, so an exception is caught and the mode falls
        back to what the frame type shows. The first failure of each exception
        type is logged at WARNING, later ones at DEBUG: before the decoder was
        guarded, a failure surfaced as an ERROR from the Bluetooth callback,
        and it must not become invisible now. Only the type name is logged,
        never the exception text, which could carry payload bytes.
        """
        try:
            reading = self._update_ble_battery(
                raw,
                candidate.frame_type,
                metadata,
                matches,
                geometry=candidate,
                clock=clock,
            )
        except Exception as err:  # noqa: BLE001 - see docstring
            error_type = type(err).__name__
            if error_type in self._decode_failure_types_logged:
                _LOGGER.debug("ble_battery_decode_failed error_type=%s", error_type)
            else:
                self._decode_failure_types_logged.add(error_type)
                _LOGGER.warning(
                    "ble_battery_decode_failed error_type=%s (battery and "
                    "protection-mode sensors keep their last value; further "
                    "failures of this type are logged at DEBUG)",
                    error_type,
                )
            reading = _FlagsReading(None, "absent")
        mode, source = _effective_uwt_mode(
            reading.uwt_bit, reading.origin, candidate.frame_type
        )
        try:
            self._note_uwt_observation(
                matches,
                mode=mode,
                source=source,
                reading=reading,
                frame_type=candidate.frame_type,
                payload_len=len(raw),
                clock=clock,
            )
        except Exception as err:  # noqa: BLE001 - the match path must not fail
            _LOGGER.debug("uwt_transition_failed error_type=%s", type(err).__name__)
        return mode, source

    def _note_uwt_observation(  # noqa: PLR0913
        self,
        matches: list[EIDMatch],
        *,
        mode: bool | None,
        source: UwtSource,
        reading: _FlagsReading,
        frame_type: int | None,
        payload_len: int,
        clock: ObservationClock,
    ) -> None:
        """Record a protection-mode change per (config entry, device).

        A change is recorded when a device shows a mode other than its last
        known one, including its first known mode in this process (``old`` is
        then ``unknown``). An observation older than the newest one of the
        track changes nothing: history replay may deliver sightings out of
        order. An observation without a trustworthy mode (unknown, or frame
        type and flags bit in conflict) is no change either; why it carried
        none is logged at DEBUG once per reason. Only matches from the EID
        cache arrive here; a heuristic phone match (``_heuristic_resolve``)
        records nothing.
        """
        self._ensure_cache_defaults()
        for match in matches:
            device_key = match.canonical_id or match.device_id
            key = (match.config_entry_id, device_key)
            track = self._uwt_track.get(key)
            if track is None:
                track = _UwtTrack()
                self._uwt_track[key] = track
            if track.last_mono is not None and track.last_mono > clock.monotonic:
                continue
            track.last_mono = clock.monotonic
            if mode is None:
                if track.skipped_source is not source:
                    track.skipped_source = source
                    _LOGGER.debug(
                        "uwt_observation_skipped device=%s canonical=%s source=%s",
                        match.device_id,
                        match.canonical_id,
                        source.value,
                    )
                continue
            track.skipped_source = None
            old = track.state
            old_seen = (
                track.last_seen_mono_by_state.get(old) if old is not None else None
            )
            track.last_seen_mono_by_state[mode] = clock.monotonic
            if old == mode:
                # No change. If lines were held back at DEBUG and the budget
                # is free again, write the current mode at INFO, so that a
                # throttled stretch ends in the log with its end state as soon
                # as the device is seen again.
                if track.suppressed and self._uwt_budget_free(track, clock.monotonic):
                    event = self._record_uwt_event(
                        match,
                        track,
                        kind="resync",
                        old=mode,
                        new=mode,
                        source=source,
                        reading=reading,
                        frame_type=frame_type,
                        payload_len=payload_len,
                        clock=clock,
                        old_seen_mono=None,
                    )
                    self._log_uwt_event(
                        match, track, event, decoded=reading.decoded, clock=clock
                    )
                continue
            # The state moves only after the event was built and stored: if
            # building it fails, the next sighting of this mode is still a
            # change and is recorded then.
            event = self._record_uwt_event(
                match,
                track,
                kind="change",
                old=old,
                new=mode,
                source=source,
                reading=reading,
                frame_type=frame_type,
                payload_len=payload_len,
                clock=clock,
                old_seen_mono=old_seen,
            )
            track.state = mode
            self._log_uwt_event(
                match, track, event, decoded=reading.decoded, clock=clock
            )

    @staticmethod
    def _uwt_budget_free(track: _UwtTrack, now: float) -> bool:
        """Return whether the device may write another INFO line now.

        The budget is ``_UWT_TRANSITION_INFO_PER_HOUR`` lines per rolling hour
        on the observation clock; times older than the hour are dropped first.
        """
        times = track.info_times
        while times and now - times[0] >= _UWT_TRANSITION_WINDOW_SECONDS:
            times.popleft()
        return len(times) < _UWT_TRANSITION_INFO_PER_HOUR

    def _log_uwt_event(
        self,
        match: EIDMatch,
        track: _UwtTrack,
        event: UwtTransitionEvent,
        *,
        decoded: int | None,
        clock: ObservationClock,
    ) -> None:
        """Write ``FMDN_UWT_TRANSITION`` at INFO within the budget, else DEBUG.

        Every event is written; the budget only decides the level. The event
        history keeps all of them either way.
        """
        if self._uwt_budget_free(track, clock.monotonic):
            level = logging.INFO
            track.info_times.append(clock.monotonic)
            suppressed = track.suppressed
            track.suppressed = 0
        else:
            level = logging.DEBUG
            track.suppressed += 1
            suppressed = track.suppressed
        _LOGGER.log(
            level,
            _UWT_TRANSITION_FORMAT,
            event.kind,
            match.device_id,
            match.canonical_id,
            event.old,
            event.new,
            event.source,
            event.flags,
            event.frame,
            f"0x{decoded:02x}" if decoded is not None else "none",
            event.payload_len,
            event.poll_age_s if event.poll_age_s is not None else "none",
            event.prev_seen_age_s if event.prev_seen_age_s is not None else "none",
            event.observed,
            event.seq,
            suppressed,
        )

    def _record_uwt_event(  # noqa: PLR0913
        self,
        match: EIDMatch,
        track: _UwtTrack,
        *,
        kind: Literal["change", "resync"],
        old: bool | None,
        new: bool,
        source: UwtSource,
        reading: _FlagsReading,
        frame_type: int | None,
        payload_len: int,
        clock: ObservationClock,
        old_seen_mono: float | None,
    ) -> UwtTransitionEvent:
        """Append one event to the history of the match's config entry.

        ``track.seq`` advances only once the event is stored, so a failure
        while building it leaves the track as it was.
        """
        seq = track.seq + 1
        prev_seen_age_s: int | None = None
        if old_seen_mono is not None:
            prev_seen_age_s = int(max(0.0, clock.monotonic - old_seen_mono))
        event = UwtTransitionEvent(
            canonical_id=match.canonical_id or match.device_id,
            kind=kind,
            old=_uwt_label(old),
            new="on" if new else "off",
            source=source.value,
            flags=reading.origin,
            frame=_uwt_frame_label(frame_type),
            payload_len=payload_len,
            poll_age_s=self._uwt_poll_age_s(match, clock.monotonic),
            prev_seen_age_s=prev_seen_age_s,
            observed=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(clock.wall)),
            seq=seq,
        )
        ring = self._uwt_events.get(match.config_entry_id)
        if ring is None:
            ring = _UwtEventRing()
            self._uwt_events[match.config_entry_id] = ring
        if len(ring.events) == ring.events.maxlen:
            ring.dropped += 1
        ring.events.append(event)
        track.seq = seq
        return event

    def _uwt_poll_age_s(self, match: EIDMatch, observed_mono: float) -> int | None:
        """Return seconds from the last sent location request to the sighting.

        Only the coordinator of the match's own config entry is asked, by the
        match's canonical id (the coordinator's ``device["id"]``); there is no
        fallback to another entry. ``None`` when that coordinator has sent no
        request for the device, or when the request was sent after the
        observation (the advertisement clock may lag ``time.monotonic``).
        """
        bucket = self.hass.data.get(DOMAIN)
        entries = bucket.get("entries") if isinstance(bucket, dict) else None
        if not isinstance(entries, dict):
            return None
        runtime = entries.get(match.config_entry_id)
        provider: _LocateRequestClock | None = None
        candidate = getattr(runtime, "coordinator", None)
        if isinstance(candidate, _LocateRequestClock):
            provider = candidate
        elif isinstance(runtime, _LocateRequestClock):
            provider = runtime
        if provider is None:
            return None
        stamp = provider.locate_requested_mono(match.canonical_id)
        if not isinstance(stamp, (int, float)) or isinstance(stamp, bool):
            return None
        age = observed_mono - stamp
        if age < 0:
            _LOGGER.debug(
                "uwt_poll_age_unavailable device=%s reason=poll_after_observation",
                match.device_id,
            )
            return None
        return int(age)

    def uwt_transition_snapshot(self, entry_id: str) -> UwtTransitionSnapshot:
        """Return the recorded protection-mode changes of one config entry.

        Only events of ``entry_id`` are returned; an entry without history
        yields an empty snapshot.
        """
        rings: dict[str, _UwtEventRing] = getattr(self, "_uwt_events", {})
        ring = rings.get(entry_id)
        return UwtTransitionSnapshot(
            capacity=_UWT_TRANSITION_RING_SIZE,
            dropped=ring.dropped if ring is not None else 0,
            info_lines_per_device_hour=_UWT_TRANSITION_INFO_PER_HOUR,
            events=tuple(ring.events) if ring is not None else (),
        )

    def _update_ble_battery(  # noqa: PLR0912, PLR0913
        self,
        raw: bytes,
        observed_frame: int | None,
        metadata: dict[str, Any],
        matches: list[EIDMatch],
        *,
        geometry: EidCandidate | None = None,
        clock: ObservationClock | None = None,
    ) -> _FlagsReading:
        """Decode the FMDN hashed-flags byte and store battery state.

        Extracts the optional flags byte from the BLE payload, XOR-decodes
        it, and persists a :class:`BLEBatteryState` keyed by
        ``canonical_id`` (the Google API device identifier) for **every**
        matched device (shared-device propagation).

        The canonical_id key must match the ``device["id"]`` used by the
        coordinator snapshot and :class:`GoogleFindMyBLEBatterySensor` so
        that :meth:`get_ble_battery_state` lookups succeed.

        On first successful decode per device an INFO-level
        ``FMDN_FLAGS_PROBE`` log is emitted; subsequent updates log at
        DEBUG level only when the battery level changes.

        Returns the decoded protection-mode bit and how the flags byte was
        located, for the sighting the caller records.
        """
        if not matches:
            return _FlagsReading(None, "absent")

        length = len(raw)
        xor_mask: int | None = metadata.get("flags_xor_mask")

        # ---- Determine the hashed-flags byte position ----
        # Three distinct states, deliberately not collapsed into one value.
        # The discriminator is `layout`, NOT `frame_type`: "bare" and "window"
        # both carry frame_type=None and demand opposite handling.
        #
        #   geometry is None
        #       No caller-supplied geometry (the characterization suite calls
        #       this method directly). Re-derive as before.
        #
        #   geometry.layout == "window"
        #       A candidate matched, but it came from the sliding window: its
        #       offset is a search position, not a parsed layout, and it says
        #       nothing about where an optional flags byte would sit. Treating
        #       it as authoritative would make an arbitrary payload byte the
        #       flags byte -- a NEW fabrication class, not the one being fixed.
        #
        #   geometry.layout in ("framed", "bare")
        #       The geometry is known. For "framed" the byte after the EID is
        #       the flags byte -- or there is none, and then there is none. For
        #       "bare" the payload IS the EID, so there is provably none: the
        #       consumer stripped the frame and flags bytes before handing it
        #       over. Guessing here is what fabricates a battery level and a
        #       UWT bit out of EID material.
        #
        # The condition is written POSITIVELY on the authoritative layouts, not
        # negatively on "window": an unexpected layout value must fall back to
        # guessing, never into the authoritative branch. Failing open into
        # "geometry is authoritative" would turn an arbitrary payload byte into
        # the flags byte -- the very class this removes.
        flags_origin: FlagsOrigin
        if geometry is not None and geometry.layout in ("framed", "bare"):
            flags_offset = geometry.offset + len(geometry.eid)
            flags_byte = raw[flags_offset] if flags_offset < length else None
            flags_origin = "framed" if flags_byte is not None else "absent"
        else:
            flags_byte = _guess_flags_byte(raw)
            flags_origin = "guessed" if flags_byte is not None else "absent"
        uwt_bit: bool | None = None
        decoded_flags: int | None = None

        # ---- Decode and store ----
        # CANONICAL SPEC ANCHOR for FMDN hashed_flags decoding.
        # SPEC QUOTE (verbatim, retrieved 2026-06-02):
        #   "bits are referenced from most significant to least significant"
        # https://developers.google.com/nearby/fast-pair/specifications/extensions/fmdn#hashed_flags
        #
        # Spec bit numbering vs. standard LSB-first arithmetic:
        #   spec bit:  0    1    2    3    4    5    6    7
        #   value:    0x80 0x40 0x20 0x10 0x08 0x04 0x02 0x01   (spec bit 0 = MSB)
        #   meaning:  rsv  rsv  rsv  rsv  rsv  bat  bat  uwt
        #
        # Mapping to standard (LSB-first) bit indices used below:
        #   spec bits 5-6 (battery, 2 bit)  == standard bits 2-1 (mask 0x06, shift >> 1)
        #   spec bit  7   (UWT mode, 1 bit) == standard bit  0   (mask 0x01)
        # Module constants `FMDN_HASHED_FLAGS_BATTERY_MASK`,
        # `FMDN_HASHED_FLAGS_BATTERY_SHIFT`, `FMDN_HASHED_FLAGS_UWT_MODE_MASK`
        # in `eid_resolver.py` (top of module, FMDN frame-type cluster)
        # encode this mapping; their numeric values are drift-guarded by
        # `tests/test_ble_battery_sensor.py::TestFmdnHashedFlagsConstantsMatchSpec`.
        #
        # ANTI-PATTERN -- DO NOT "fix" this to (decoded >> 5) & 0x03 / decoded & 0x80.
        # That reads spec bits 2-3 (reserved zeros) instead of the battery field
        # and silently breaks normal/low/critical advertisements. The MSB-first
        # qualifier in the spec is the load-bearing detail. Doing so also fails:
        #   tests/test_ble_battery_sensor.py::TestHashedFlagsMsbFirstConvention
        #   tests/test_ble_battery_sensor.py::TestMsbFirstNotLsbFirstDemonstratesCodexMisinterpretation
        # See also docs/BLE_BATTERY_SENSOR.md L45.
        if flags_byte is not None and xor_mask is not None:
            decoded = flags_byte ^ xor_mask
            decoded_flags = decoded
            # Mask/shift centralized in FMDN_HASHED_FLAGS_* constants
            # (top of module, FMDN frame-type cluster).
            battery_raw = (
                decoded >> FMDN_HASHED_FLAGS_BATTERY_SHIFT
            ) & FMDN_HASHED_FLAGS_BATTERY_MASK
            uwt_mode = bool(decoded & FMDN_HASHED_FLAGS_UWT_MODE_MASK)
            uwt_bit = uwt_mode
            battery_pct = FMDN_BATTERY_PCT.get(battery_raw)
            # Observation time, not processing time (see ObservationClock);
            # direct callers without a clock get the old "now" behaviour.
            observation = clock if clock is not None else _observation_clock(None)
            now_wall = observation.wall

            # Observe -- do not arbitrate -- the two redundant channels.
            #
            # SPEC (Find Hub Network Accessory Specification,
            # retrieved 2026-10-11), on entering unwanted tracking protection
            # mode: "The frame type should be set to 0x41. The state is also
            # reflected in the hashed flags section." On leaving it: "The
            # frame type should be set back to 0x40." The changelog entry for
            # the frame type reads "Added a cleartext indication of unwanted
            # tracking protection mode." Frame type and hashed-flags spec
            # bit 7 should therefore always agree.
            #
            # An older reading in this repository tied 0x41 to the accessory
            # generation (32-byte P-256 EIDs) instead of the mode. It goes back
            # at least to 588b0e02 (2025-12-15, resolver API doc: "0x41: Modern
            # / 256-bit frame."); ed340df5 (2026-03-06) carried it into the
            # battery decoder docs. Neither commit records an observation.
            # Field data now exist in docs/UWT_FIELD_DATA.md: frame and bit
            # agreed on every logged probe line (0x40 with bit 0, 0x41 with
            # bit 1), and no conflict line was logged. They cover 20-byte EIDs
            # only; for 32-byte EIDs the equivalence rests on the specification.
            #
            # This code still passes the decoded bit through unchanged and
            # only logs a disagreement: a conflict points at a decoding or
            # firmware fault and should stay visible, not be smoothed over.
            # The finder's upload takes the mode from _effective_uwt_mode(),
            # which uses the bit read at a known position, falls back to the
            # frame type, and reports no mode at all when the two disagree.
            if observed_frame is not None:
                self._log_frame_conflict(
                    matches=matches,
                    observed_frame=observed_frame,
                    uwt_mode=uwt_mode,
                    decoded=decoded,
                    payload_len=length,
                )

            state = BLEBatteryState(
                battery_level=battery_raw,
                battery_pct=battery_pct,
                uwt_mode=uwt_mode,
                decoded_flags=decoded,
                observed_at_wall=now_wall,
                observed_at_monotonic=observation.monotonic,
            )

            battery_labels = {
                0: "UNSUPPORTED",
                1: "NORMAL",
                2: "LOW",
                3: "CRITICALLY_LOW",
            }
            battery_label = battery_labels.get(battery_raw, f"UNKNOWN({battery_raw})")

            # Store for ALL matches (shared-device propagation).
            # Key by canonical_id (Google API device ID) — this is the same
            # identifier used by the coordinator snapshot (device["id"]) and
            # by GoogleFindMyBLEBatterySensor._device_id so that
            # get_ble_battery_state() lookups succeed.
            for match in matches:
                storage_key = match.canonical_id or match.device_id
                prev = self._ble_battery_state.get(storage_key)
                if (
                    prev is not None
                    and prev.observed_at_monotonic > observation.monotonic
                ):
                    # Older sighting delivered after a newer one (history
                    # replay in dict order on HA before 2026.8): the state on
                    # record is the more recent observation, keep it. Ordered
                    # on the monotonic clock: the wall clock can be stepped
                    # backwards while HA runs, and a guard on it would then
                    # reject every live sighting until wall time caught up.
                    continue
                self._ble_battery_state[storage_key] = state

                # First decode per device → INFO probe log (once per device)
                if storage_key not in self._flags_logged_devices:
                    _LOGGER.info(
                        "FMDN_FLAGS_PROBE device=%s canonical=%s "
                        "flags_byte=0x%02x xor_mask=0x%02x decoded=0x%02x "
                        "battery=%s(%d) battery_pct=%s uwt_mode=%s "
                        "observed_frame=%s payload_len=%d",
                        match.device_id,
                        match.canonical_id,
                        flags_byte,
                        xor_mask,
                        decoded,
                        battery_label,
                        battery_raw,
                        battery_pct,
                        uwt_mode,
                        f"0x{observed_frame:02x}"
                        if observed_frame is not None
                        else None,
                        length,
                    )
                    self._flags_logged_devices.add(storage_key)
                elif prev is not None and prev.battery_level != battery_raw:
                    # Battery level changed → DEBUG log
                    _LOGGER.debug(
                        "BLE battery changed device=%s %s(%d)→%s(%d)",
                        storage_key,
                        battery_labels.get(prev.battery_level, "?"),
                        prev.battery_level,
                        battery_label,
                        battery_raw,
                    )
        else:
            # Cannot decode — log once per device at INFO for diagnostics
            for match in matches:
                storage_key = match.canonical_id or match.device_id
                if storage_key not in self._flags_logged_devices:
                    _max_hex = 40  # noqa: PLR2004
                    raw_hex = (
                        raw.hex()
                        if length <= _max_hex
                        else raw[:_max_hex].hex() + "..."
                    )
                    _LOGGER.info(
                        "FMDN_FLAGS_PROBE device=%s canonical=%s "
                        "CANNOT_DECODE observed_frame=%s payload_len=%d "
                        "has_xor_mask=%s flags_byte_found=%s raw_hex=%s",
                        match.device_id,
                        match.canonical_id,
                        f"0x{observed_frame:02x}"
                        if observed_frame is not None
                        else None,
                        length,
                        xor_mask is not None,
                        flags_byte is not None,
                        raw_hex,
                    )
                    self._flags_logged_devices.add(storage_key)
        return _FlagsReading(uwt_bit, flags_origin, decoded_flags)

    # ------------------------------------------------------------------
    # Public BLE battery API
    # ------------------------------------------------------------------
    def get_ble_battery_state(self, device_id: str) -> BLEBatteryState | None:
        """Return the last observed BLE battery state for a device, or None.

        The *device_id* parameter is the **canonical_id** (Google API device
        identifier, i.e. ``device["id"]`` from the coordinator snapshot),
        not the HA device-registry ID.
        """
        return self._ble_battery_state.get(device_id)

    # ------------------------------------------------------------------
    # Public BLE scan info API (Phase 2.2 preparation)
    # ------------------------------------------------------------------
    def get_ble_scan_info(self, device_id: str) -> BLEScanInfo | None:
        """Return last observed BLE scan metadata for a device, or None.

        The *device_id* parameter is the **canonical_id** (Google API device
        identifier, i.e. ``device["id"]`` from the coordinator snapshot),
        not the HA device-registry ID.

        The returned :class:`BLEScanInfo` contains the current (rotated) BLE
        MAC address and the timestamp of the last observation.  FMDN trackers
        rotate their MAC every ~15 minutes, so callers should check
        ``observed_at`` freshness before attempting GATT connections.
        """
        return self._ble_scan_info.get(device_id)

    def _record_ble_scan_info(
        self,
        matches: list[EIDMatch],
        ble_address: str,
        *,
        clock: ObservationClock | None = None,
    ) -> None:
        """Store the BLE address for all matched devices.

        Called from :meth:`resolve_eid` when the caller provides a
        ``ble_address``.  Uses the same canonical_id keying pattern
        as :attr:`_ble_battery_state`. The timestamps are the observation
        time handed in by the caller; without a clock the scan is dated
        *now*.
        """
        if clock is None:
            clock = _observation_clock(None)
        for match in matches:
            storage_key = match.canonical_id or match.device_id
            existing = self._ble_scan_info.get(storage_key)
            if existing is not None and existing.observed_at > clock.monotonic:
                # Older sighting delivered after a newer one (history replay
                # in dict order on HA before 2026.8). The address on record
                # is the newer one; an older address would be stale for a
                # rotating tracker.
                continue
            self._ble_scan_info[storage_key] = BLEScanInfo(
                ble_address=ble_address,
                observed_at=clock.monotonic,
                observed_at_wall=clock.wall,
            )

    def resolve_eid(  # noqa: PLR0911, PLR0912, PLR0915
        self,
        eid_bytes: bytes,
        *,
        ble_address: str | None = None,
        observed_at: float | None = None,
    ) -> EIDMatch | None:
        """Resolve a scanned payload to a Home Assistant device registry ID.

        For shared devices (same tracker across multiple accounts), this returns
        the match with the smallest time_offset (best match).
        Use resolve_eid_all() to get all matches.

        Args:
            eid_bytes: Raw EID bytes from a BLE advertisement.
            ble_address: Optional BLE MAC address of the advertising device.
                When provided, the address is stored for future direct GATT
                connections (e.g. BLE ring fallback).  This parameter is
                backward-compatible: existing callers that omit it are
                unaffected.
            observed_at: Optional advertisement time on the
                :func:`time.monotonic` clock (``BluetoothServiceInfoBleak.time``).
                Every observation timestamp recorded for the match (battery
                state, scan info, lock confirmation) is then the advertisement
                time. Home Assistant replays its advertisement history when
                the callback is registered (reload) and restores that history
                across restarts, so without this parameter a replayed
                advertisement of up to 15 minutes age would count as seen
                *now*. Omitting it keeps the old behaviour.
        """
        clock = _observation_clock(observed_at)
        matches, _, _ = self._resolve_eid_internal(eid_bytes, clock=clock)
        if not matches:
            return None
        if ble_address is not None:
            self._record_ble_scan_info(matches, ble_address, clock=clock)
        # Return the match with the smallest absolute time_offset (best match)
        return min(matches, key=lambda m: abs(m.time_offset))

    def resolve_eid_all(
        self,
        eid_bytes: bytes,
        *,
        ble_address: str | None = None,
        observed_at: float | None = None,
    ) -> list[EIDMatch]:
        """Resolve a scanned payload to all matching Home Assistant device registry IDs.

        This method supports shared devices: when the same physical tracker
        is shared between accounts, all accounts' matches are returned.
        Each match represents a different Home Assistant device registry entry
        for the same physical tracker.

        Args:
            eid_bytes: Raw EID bytes from a BLE advertisement.
            ble_address: Optional BLE MAC address of the advertising device.
                When provided, the address is stored for future direct GATT
                connections (e.g. BLE ring fallback).
            observed_at: Optional advertisement time on the
                :func:`time.monotonic` clock; see :meth:`resolve_eid`.
        """
        clock = _observation_clock(observed_at)
        matches, _, _ = self._resolve_eid_internal(eid_bytes, clock=clock)
        if matches and ble_address is not None:
            self._record_ble_scan_info(matches, ble_address, clock=clock)
        return matches

    def _extract_candidates(self, payload: bytes) -> tuple[list[bytes], int | None]:
        """Legacy view: candidate bytes plus the payload-level frame type.

        Retained for the characterization suite. The *candidate list* is
        unchanged; the second element is not. Without the exclusive
        service-data gate, the raw-header branch also runs after a
        service-data hit and overwrites ``observed_frame``, so this now
        reports the last frame byte seen for the payload rather than the one
        belonging to the first candidate. Production code uses
        :meth:`_extract_eid_candidates` and takes the frame type from the
        candidate that matched, which is the only reading that is well-defined
        when both geometries produce a candidate.
        """

        candidates, observed_frame = self._extract_eid_candidates(payload)
        return [candidate.eid for candidate in candidates], observed_frame

    def _extract_eid_candidates(  # noqa: PLR0912
        self, payload: bytes
    ) -> tuple[list[EidCandidate], int | None]:
        """Extract possible EID slices from a BLE payload, with geometry."""

        length = len(payload)
        candidates: list[EidCandidate] = []
        observed_frame: int | None = None
        allow_sliding_window = True

        if length in (LEGACY_EID_LENGTH, MODERN_EID_LENGTH):
            if not (
                length == MODERN_EID_LENGTH
                and payload[0] in (FMDN_FRAME_TYPE, MODERN_FRAME_TYPE)
            ):
                # The payload IS the EID: a consumer stripped the frame and
                # the optional hashed-flags byte before handing it over (see
                # the Bermuda fallback path). The geometry is fully known,
                # and it says there is no flags byte -- which is why this is
                # "bare" and not "window".
                candidates.append(
                    EidCandidate(eid=payload, offset=0, frame_type=None, layout="bare")
                )
                return candidates, None

        force_sliding_window = False
        if length >= SERVICE_DATA_OFFSET + LEGACY_EID_LENGTH:
            frame_type = payload[7]
            if frame_type in (FMDN_FRAME_TYPE, MODERN_FRAME_TYPE):
                eid_lengths = _framed_eid_lengths(
                    frame_type, length, SERVICE_DATA_OFFSET
                )
                if eid_lengths:
                    observed_frame = frame_type
                # A 0x41 frame in the exact legacy geometry had no framed
                # candidate before this reading existed, so the sliding window
                # was what resolved it. Letting the new candidate switch that
                # window off would cost exactly those resolutions whenever
                # octet 7 is EID material rather than a frame byte -- the same
                # 1-in-256 coincidence the raw-header gate below was removed
                # for. The window therefore stays available here, at the price
                # of building it on the hit path too.
                if frame_type == MODERN_FRAME_TYPE and eid_lengths == (
                    LEGACY_EID_LENGTH,
                ):
                    force_sliding_window = True
                for eid_length in eid_lengths:
                    candidates.append(
                        EidCandidate(
                            eid=payload[
                                SERVICE_DATA_OFFSET : SERVICE_DATA_OFFSET + eid_length
                            ],
                            offset=SERVICE_DATA_OFFSET,
                            frame_type=frame_type,
                            layout="framed",
                        )
                    )

        # No `not candidates` gate any more. Byte 7 being 0x40/0x41 is a
        # 1-in-256 coincidence for any payload whose EID happens to carry that
        # value at that position; today the service-data probe then wins
        # exclusively, the raw-header candidate is never built, and the device
        # stays unresolvable for a full rotation window (~1024 s, the EID is
        # constant within it). Probing both geometries is monotone: no
        # candidate that the previous code produced is lost, one dict lookup is
        # added, and a collision between two independent 160-/256-bit EIDs is
        # not a practical concern. See custom_components/googlefindmy/AGENTS.md,
        # "FHNA frame slicing reminder".
        if length >= RAW_HEADER_LENGTH + LEGACY_EID_LENGTH:
            had_service_data_candidate = bool(candidates)
            frame_type = payload[0]
            if frame_type in (FMDN_FRAME_TYPE, MODERN_FRAME_TYPE):
                modern_required_length = RAW_HEADER_LENGTH + MODERN_EID_LENGTH
                eid_lengths = _framed_eid_lengths(frame_type, length, RAW_HEADER_LENGTH)
                # Reported even when no reading fits, unlike the service-data
                # branch above. That asymmetry is a contract, not an oversight:
                # a truncated 0x41 raw-header frame must still report its frame
                # byte (tests/test_eid_resolver_candidates.py, "BUG E"), which
                # is what routes it to the truncation diagnostic below. Octet 0
                # is the payload's own first byte here, so reading a frame type
                # there is a weaker claim than reading one at octet 7.
                observed_frame = frame_type
                if eid_lengths:
                    for eid_length in eid_lengths:
                        candidates.append(
                            EidCandidate(
                                eid=payload[
                                    RAW_HEADER_LENGTH : RAW_HEADER_LENGTH + eid_length
                                ],
                                offset=RAW_HEADER_LENGTH,
                                frame_type=frame_type,
                                layout="framed",
                            )
                        )
                elif not had_service_data_candidate:
                    # Only diagnose truncation when this payload produced
                    # no candidate at all. Without the `not candidates`
                    # gate above, this branch is now also reached for
                    # payloads the service-data probe already resolved,
                    # where a truncation warning would be a misdiagnosis.
                    allow_sliding_window = length >= modern_required_length - 1
                    self._log_truncated_frame(
                        frame_type=frame_type,
                        payload_len=length - RAW_HEADER_LENGTH,
                        raw_len=length,
                    )

        if (
            (not candidates or force_sliding_window)
            and length > LEGACY_EID_LENGTH
            and allow_sliding_window
        ):
            window = min(length - LEGACY_EID_LENGTH + 1, 64)
            for i in range(window):
                slice_20 = payload[i : i + LEGACY_EID_LENGTH]
                if len(slice_20) == LEGACY_EID_LENGTH:
                    # The offset is a search position, not a parsed layout: it
                    # says nothing about where an optional flags byte sits.
                    candidates.append(
                        EidCandidate(
                            eid=slice_20,
                            offset=i,
                            frame_type=None,
                            layout="window",
                        )
                    )
                slice_32 = payload[i : i + MODERN_EID_LENGTH]
                if len(slice_32) == MODERN_EID_LENGTH:
                    candidates.append(
                        EidCandidate(
                            eid=slice_32,
                            offset=i,
                            frame_type=None,
                            layout="window",
                        )
                    )

        return candidates, observed_frame

    def _log_frame_conflict(
        self,
        *,
        matches: list[EIDMatch],
        observed_frame: int,
        uwt_mode: bool,
        decoded: int,
        payload_len: int,
    ) -> None:
        """Record a disagreement between the frame type and the UWT flag bit.

        Two-stage on purpose. The first disagreement per device is INFO, the
        same level as ``FMDN_FLAGS_PROBE``, so a reporter can contribute the
        observation without turning on debug logging. INFO is still hidden at
        Home Assistant's default level (WARNING without ``--verbose``): the
        reporter raises this module's logger to ``info`` with the
        ``logger.set_level`` action first (README, "Reporting unwanted
        tracking protection mode changes"). Everything after the first line is
        DEBUG and throttled, because a disagreeing beacon disagrees on every
        single advertisement.

        The emitted fields mirror ``FMDN_FLAGS_PROBE`` exactly -- both the
        ``device``/``canonical`` pair and the throttle key. Emitting only the
        storage key would look joinable and not be: the two identifiers differ
        whenever a device carries a canonical id, and the probe log puts
        ``device_id`` in the field this log would have filled with the
        canonical one. A measurement channel that cannot be joined with the
        probe log it is meant to explain is worthless as a diagnostic.
        """

        frame_says_uwt = observed_frame == MODERN_FRAME_TYPE
        if frame_says_uwt == uwt_mode:
            return

        now = time.time()
        for match in matches:
            storage_key = match.canonical_id or match.device_id
            if storage_key not in self._frame_conflict_logged_devices:
                self._frame_conflict_logged_devices.add(storage_key)
                self._frame_conflict_log_at[(storage_key, observed_frame)] = now
                _LOGGER.info(
                    "FMDN_FLAGS_CONFLICT device=%s canonical=%s frame=0x%02x "
                    "uwt_bit=%s decoded=0x%02x payload_len=%d; frame type and "
                    "flags bit disagree, sensor value passed through unchanged",
                    match.device_id,
                    match.canonical_id,
                    observed_frame,
                    uwt_mode,
                    decoded,
                    payload_len,
                )
                continue

            key = (storage_key, observed_frame)
            last_log = self._frame_conflict_log_at.get(key)
            if (
                last_log is not None
                and now - last_log < TRUNCATED_FRAME_LOG_WINDOW_SECONDS
            ):
                continue

            self._frame_conflict_log_at[key] = now
            _LOGGER.debug(
                "FMDN_FLAGS_CONFLICT device=%s canonical=%s frame=0x%02x "
                "uwt_bit=%s decoded=0x%02x payload_len=%d; frame type and "
                "flags bit disagree, sensor value passed through unchanged",
                match.device_id,
                match.canonical_id,
                observed_frame,
                uwt_mode,
                decoded,
                payload_len,
            )

    def _log_truncated_frame(
        self, *, frame_type: int, payload_len: int, raw_len: int
    ) -> None:
        """Rate-limit warnings for truncated framed payloads."""

        now = time.time()
        key = (frame_type, payload_len)
        last_log = self._truncated_frame_log_at.get(key)
        if last_log is not None and now - last_log < TRUNCATED_FRAME_LOG_WINDOW_SECONDS:
            return

        self._truncated_frame_log_at[key] = now
        _LOGGER.warning(
            "Truncated or unexpected framed BLE payload: frame=0x%02x payload_len=%s raw_len=%s; "
            "falling back to sliding-window extraction.",
            frame_type,
            payload_len,
            raw_len,
        )

    @staticmethod
    def _cancel_callback(unsub: CALLBACK_TYPE | None, name: str) -> None:
        """Cancel a callback or close a coroutine, logging any errors."""
        if unsub is None:
            return
        try:
            if callable(unsub):
                unsub()
            elif asyncio.iscoroutine(unsub):
                unsub.close()
        except Exception as err:  # pragma: no cover - defensive
            _LOGGER.debug("Failed to cancel %s: %s", name, err)

    def locked_curve_name(self, canonical_id: str) -> str | None:
        """Return the curve of the EID variant a device is locked to.

        The decryption path asks this through ``FOREIGN_READING_TRACKER`` to
        tell a key-neutral foreign report from an authentication failure: a
        device locked to a P-256 variant cannot be served by a report with a
        20-byte ``Sx`` (see ``docs/Ephemeral_Identifier_Resolver_API.md``).
        Only the current locks are read, so expiry, discarding and
        :meth:`stop` take effect at once.

        Both sides are reduced to the UUID part, as the work items are built,
        and additionally lowercased, as ``foreign_device_key`` does: a lock
        loaded from storage may still carry an ``account:``-namespaced ID until
        the next refresh rewrites it, and the server may change the hex casing.

        Args:
            canonical_id: Canonical ID in any casing, with or without namespace.

        Returns:
            ``SECP256R1.name`` or ``SECP160R1.name``; ``None`` without a lock,
            for an unknown variant, or when two locks of the ID disagree.
        """
        wanted = canonical_id.rsplit(":", 1)[-1].lower()
        curves: set[str] = set()
        for lock in list(self._locks.values()):
            if lock.canonical_id.rsplit(":", 1)[-1].lower() != wanted:
                continue
            try:
                variant = EidVariant(lock.variant)
            except ValueError:
                continue
            # Every EidVariant has a row; test_every_eid_variant_is_covered
            # fails with KeyError for a new variant without one.
            order = _VARIANT_CURVE_PARAMS[variant][1]
            curves.add(SECP256R1.name if order == P256_ORDER else SECP160R1.name)
        return curves.pop() if len(curves) == 1 else None

    def last_confirmed_sighting(self, registry_id: str) -> ConfirmedSighting | None:
        """Return the last sighting of a device that a lookup match confirmed.

        The finder encrypts a report to the EID of this sighting. It is an
        observation, not a prediction of the EID the device advertises now:
        the owner resolves a report against the recent past and near future,
        and the report carries the time of the sighting. The caller judges
        the age. Matches of the heuristic phone path record no sighting, and
        the value lives in memory only, so it is gone after a restart until
        the device is seen again.

        Args:
            registry_id: Home Assistant device registry ID of the device.

        Returns:
            The sighting, or ``None`` if the device was not matched since
            start or its lock state was cleared.
        """
        return self._confirmed_sightings.get(registry_id)

    def stop(self) -> None:
        """Cancel background timers and clear cached state."""

        FOREIGN_READING_TRACKER.clear_curve_provider(self.locked_curve_name)
        self._cancel_callback(self._unsub_alignment, "alignment timer")
        self._unsub_alignment = None

        self._cancel_callback(self._unsub_interval, "refresh interval")
        self._unsub_interval = None

        if self._load_task is not None:
            load_task = self._load_task
            self._load_task = None
            if not load_task.done():
                load_task.cancel()

        self._lookup.clear()
        self._lookup_metadata.clear()
        self._locks.clear()
        self._persisted_locks.clear()
        # Stubs that bypass __init__ may lack the field.
        confirmed_sightings = getattr(self, "_confirmed_sightings", None)
        if isinstance(confirmed_sightings, dict):
            confirmed_sightings.clear()
        for attr in ("_uwt_track", "_uwt_events"):
            mapping = getattr(self, attr, None)
            if isinstance(mapping, dict):
                mapping.clear()
