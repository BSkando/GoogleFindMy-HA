# custom_components/googlefindmy/const.py
"""Constants for Google Find My Device integration.

All constants defined here are intended to be import-safe across the integration.
Keep comments and docstrings in English; user-facing strings belong in translations.
"""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Final, Literal

# --------------------------------------------------------------------------------------
# Core identifiers
# --------------------------------------------------------------------------------------
DOMAIN: str = "googlefindmy"
# Shared hass.data key for the global EID resolver instance
DATA_EID_RESOLVER: Final[Literal["eid_resolver"]] = "eid_resolver"
# Latest config entry schema version handled by this integration.
CONFIG_ENTRY_VERSION: int = 2
# Integration version. MUST be bumped together with manifest.json "version" and
# pyproject.toml [tool.poetry] version on every release; the three are a coupled
# triple, kept in sync automatically by semantic-release (pyproject.toml
# [tool.semantic_release] version_variables). manifest.json is strict JSON
# (hassfest-validated) and cannot carry this cross-reference, so the manifest side is
# anchored in this directory's AGENTS.md ("Version bump touches three files").
# pyproject.toml carries the reverse reference in a TOML comment.
# NOTE: no ": str" annotation on purpose -- semantic-release's version_variables
# regex only matches `NAME = "x"`, not `NAME: str = "x"`. Re-adding the annotation
# would silently skip this file on the automated version bump.
INTEGRATION_VERSION = "1.9.0"

# --------------------------------------------------------------------------------------
# Sound dispatch outcome (API -> coordinator boundary)
# --------------------------------------------------------------------------------------


class SoundDispatchOutcome(StrEnum):
    """Why a sound command did or did not reach Google, as classified by ``api.py``.

    ``api.py`` already reaches nine distinct exits (missing action token, empty
    submitter reply, HTTP 200, NovaAuthError, HTTP 401/403, other HTTP status,
    rate limit, network error, unexpected exception). Until this type existed,
    all of them collapsed into a single ``False``, and the coordinator had no
    choice but to read that ``False`` as "the push transport is broken". A server
    saying no and a network that never answered then produced the same 90-second
    cooldown and the same ``FcmStatus.DEGRADED``. This is the same failure class
    ``StopSoundOutcome`` documents one layer up: a bool cannot carry the state
    space.

    Only ``TRANSPORT_FAILED`` justifies a push cooldown. Only ``ACCEPTED`` proves
    that the credentials worked.

    See IRR-CA-SOUND-FAILURE-CLASS in ``docs/PLAY_SOUND_ARCHITECTURE.md`` for the
    per-member "may a caller arm a push cooldown" table this type carries, and
    IRR-CA-STOP-BREAKS-SELF-INFLICTED-COOLDOWN for the qualifier on the stop
    path. There the readiness gate lets a stop through a running window when the
    stop carries our own fresh cancel key, so a window armed by a play cannot
    lock out a stop this integration can correlate; and when such a stop then
    fails on the transport, the cooldown is reported through
    ``_note_stop_transport_problem_without_extending()``, which puts a window
    that was already running back instead of restarting it, so a stop cannot
    feed the window it just broke through.
    """

    ACCEPTED = "accepted"
    """Nova answered HTTP 200. Acceptance of the submission, not proof of a ring.

    See IRR-CA-NO-RING-CONFIRMATION in ``docs/PLAY_SOUND_ARCHITECTURE.md``.
    """

    REJECTED_AUTH = "rejected_auth"
    """The server answered and refused on credentials: HTTP 401 or 403.

    The transport worked. A cooldown would mislabel an expired sign-in as a
    network outage and hide it behind a self-clearing timer.

    The criterion is the STATUS, not the exception type. ``NovaAuthError`` is
    raised for every non-retryable 4xx (its own docstring says "every
    non-retryable 4xx client error", and ``HTTP_RETRY_ELIGIBLE`` holds no 4xx
    besides 408 and 429), so
    reading the type alone filed a deleted device under "check your sign-in".
    ``NovaAuthPermanentError`` and any error flagged ``is_permanent`` belong
    here whatever their status: they say re-authentication is required.
    """

    REJECTED_RATE_LIMIT = "rejected_rate_limit"
    """The server answered HTTP 429. The transport worked; only the pace was wrong."""

    REJECTED_SERVER = "rejected_server"
    """The server answered and refused for any other reason.

    Covers 5xx, a Nova logic error, and the non-credential client rejections --
    400, 404, 405, 409, 422 -- that arrive as ``NovaAuthError`` despite naming
    no credential problem. The enum has no ``REJECTED_CLIENT`` member because
    no consumer branches on the distinction; what the two share, and what the
    name is chosen for, is that the SERVER answered, so the transport worked
    and no cooldown may be armed. For an HTTP refusal the log line names the
    status; a Nova logic error carries no HTTP status (the request was a 200)
    and its log line names the payload error code instead.
    """

    TRANSPORT_FAILED = "transport_failed"
    """No usable answer was obtained: DNS, connect refused/timeout, disconnect,
    read timeout, or a NovaError leaving the transport after its retries.

    This is the ONLY outcome that justifies arming the push cooldown.
    """

    NOT_SENT = "not_sent"
    """A local precondition failed before any transport was used (no action token).

    Neither the server nor the network said anything, so neither may be blamed.
    """

    INTERNAL_ERROR = "internal_error"
    """This integration failed on its own: an unexpected exception, or a contract
    violation such as an empty reply from the submitter.

    Classifying such a bug as a network outage is what this type was written to
    stop; it is logged with a traceback and never arms the cooldown.
    """


@dataclass(frozen=True, slots=True)
class PlaySoundResult:
    """Result of ``api.async_play_sound``: a classification plus the cancel key.

    ``cancel_key`` is non-None in exactly the cases where the device may be
    ringing and a later Stop needs the handle: an accepted command, or a failure
    that latched dispatch (``NovaError.dispatched``). Read ``cancel_key`` ONLY for
    the question "may the device be ringing"; read ``outcome`` for every question
    about the cause. Deriving the cause from the key is the out-of-band channel
    this type replaces.
    """

    outcome: SoundDispatchOutcome
    cancel_key: str | None = None

    @property
    def accepted(self) -> bool:
        """Return True when Nova accepted the submission (HTTP 200)."""
        return self.outcome is SoundDispatchOutcome.ACCEPTED


# --------------------------------------------------------------------------------------
# Stop Sound outcome
# --------------------------------------------------------------------------------------

# Earliest point, in seconds after Nova ACCEPTED our own play, at which a stop
# for that play is put on the wire. An earlier stop is held back until then.
#
# Measured, not derived, and with a stated limit. In a live series against a
# moto tag 2 (2026-10-17/18) a stop that reached the tracker around the moment
# its ring was about to start left the tracker silent -- no BLE advertisements
# until a long press on its button -- in two of two runs (stops 11.8 s and
# 9.4 s after acceptance, ring not yet started). Stops 11.6 s or later after
# acceptance, with the ring already running, went through in four of four. The
# ring itself started 5.6 to 10.3 s after acceptance in the runs where it was
# heard. Twelve seconds clears every start that was heard, but not every start:
# in one of the failed runs the ring had still not begun 11.8 s after
# acceptance. It is a boundary drawn from six runs on one tracker model, not a
# protocol constant, and Nova queues commands, so a ring may start later than
# any value here. Waiting for a confirmed start would be the stronger rule; no
# such signal is decoded yet.
STOP_SOUND_MIN_DELAY_AFTER_PLAY_S: Final = 12.0


class StopSoundOutcome(StrEnum):
    """Outcome of a Stop Sound attempt.

    Four states, because the state space is genuinely four-valued and a bool
    cannot carry it: a stop that was submitted without a cancel key is neither a
    success (nothing proves it reached a ring) nor a rejection (the server did
    accept the submission). Collapsing the middle state into ``True`` is what
    made the integration report success for a ring that kept playing
    (BSkando#195).

    The two failure states are kept apart for the same reason the middle state
    exists: they differ in what the user has to do about them. ``SUPPRESSED``
    clears itself and is answered by waiting; ``FAILED`` may need credentials, a
    network, or patience with a rate limit. One message cannot advise on both
    without misleading half its readers.

    They are NOT told apart by how far the request travelled. Both contain a
    member that never left the machine -- see ``FAILED`` on
    ``SoundDispatchOutcome.NOT_SENT`` -- and reading "did it reach the wire"
    as the criterion is what makes a missing action token look suppressible.

    Note that even ``CANCELLED`` means "submitted with a correlated cancel key",
    not "the device stopped". Nova returns no parsable ExecuteActionResponse, so
    no reply of the cloud API can prove an effect on the device; see
    ``docs/PLAY_SOUND_ARCHITECTURE.md`` and IRR-CA-NO-RING-CONFIRMATION.
    """

    CANCELLED = "cancelled"
    """Submitted WITH a cancel key whose correlation this integration can prove.

    Proof means the key is our own, still fresh, cached Play Sound handle. A key
    supplied by the caller qualifies only when it IS that handle; an arbitrary
    string is a claim we cannot check and yields ``UNCORRELATED`` instead.
    """

    UNCORRELATED = "uncorrelated"
    """Submitted without a provably correlated key; the effect may be nil.

    Covers both "no key at all" and "a key we cannot vouch for". Both leave the
    server unable to match the stop to a running ring, so both are reported to
    the user rather than silently counted as success.
    """

    SUPPRESSED = "suppressed"
    """Never sent, because this integration declined to send it.

    Today the sole cause is a push transport that is not ready yet. The command
    never reached the network, the condition is local and transient, and the
    only useful advice is to retry shortly.

    "Never sent" alone does not qualify: ``SoundDispatchOutcome.NOT_SENT`` (the
    readiness gate let the command through, but no action token was to be had)
    never leaves the machine either and is filed under ``FAILED``. The dividing
    line is the remedy, not the distance travelled -- see ``FAILED``.
    """

    FAILED = "failed"
    """The attempt was made, and it did not succeed.

    Covers every way it can die once this integration has committed to making
    it: authentication failure, HTTP 401/403, a server rejection, a rate limit,
    a network error, an empty reply -- and the missing action token, which
    never reaches the wire.

    That last member is why this docstring does not read "handed to the
    transport". ``SoundDispatchOutcome.NOT_SENT`` is filed here rather than
    under ``SUPPRESSED`` because the two failure states are told apart by the
    REMEDY they call for, not by how far the request travelled. ``SUPPRESSED``
    means "the push transport is not up yet, try again in a moment", and it is
    reached only by the readiness gate saying so. ``NOT_SENT`` is what the gate
    could NOT see: it inspects the receiver's state (``is_push_ready``) while
    the token is fetched separately (``_get_fcm_token_for_action``), so a
    command the gate waved through can still find no action token. That is a
    setup or credential problem far more often than a moment's patience, and
    the advice such a user needs is the one ``stop_sound_rejected`` already
    gives -- "an expired sign-in, a missing token, a server or network error,
    or a rate limit", plus a look at the log. Re-authentication is the usual
    remedy across this member, which is why "try again in a moment" would be
    wrong for most of it. Routing ``NOT_SENT`` to ``SUPPRESSED`` would answer a
    missing token with an unbounded "try again shortly".
    """


class StopSoundUncorrelatedReason(StrEnum):
    """Why a stop is ``StopSoundOutcome.UNCORRELATED``; selects the user message.

    The outcome stays ONE state on purpose: every member below means "submitted,
    effect unproven", and the service layer reports each of them as an error,
    never as success (BSkando#195). What differs is what the user can do about
    it, so each member has its own message instead of one text that lists every
    cause and fits none.

    Decided by ``classify_stop_correlation`` on the coordinator, from the same
    state ``async_stop_sound`` reads before anything is sent. A key that this
    integration sent earlier is NOT a member: since IRR-CA-STOP-KEEPS-CANCEL-KEY
    a stop does not spend the key, so a repeated stop with our own fresh key is
    ``CANCELLED`` again.
    """

    NO_KEY = "no_key"
    """No key was supplied and this instance holds none for the device.

    The ring was started elsewhere (the Find Hub app, another installation), or
    by this instance so long ago that a reload dropped its key.
    """

    AGED_KEY = "aged_key"
    """The key at hand is this instance's own, but older than SOUND_UUID_MAX_AGE_S.

    It is still sent (it may be the handle on the audible ring), but it cannot
    vouch for that ring.
    """

    FOREIGN_KEY = "foreign_key"
    """The caller supplied a key that is not this instance's cached key."""


# --------------------------------------------------------------------------------------
# Play Sound outcome (coordinator -> service boundary)
# --------------------------------------------------------------------------------------


class PlaySoundOutcome(StrEnum):
    """Outcome of a Play Sound attempt, as classified by the coordinator.

    Three states, told apart by the REMEDY they call for -- the same dividing
    line ``StopSoundOutcome`` draws, and for the same reason: one message
    cannot advise both the user who only has to wait and the user who has to
    act. Before this type existed the play path returned a plain ``bool``, so
    six unrelated conditions -- a push cooldown that clears itself after ninety
    seconds, a device that cannot ring at all, a missing action token, a server
    rejection, a rate limit and a bug of our own -- reached the service handler
    as one ``False`` and were answered with one message that had to list every
    possibility and point at the log. See IRR-CA-PLAY-REMEDY-SPLIT.

    This is deliberately NOT ``SoundDispatchOutcome``. That type is the verdict
    of ``api.py`` on a command that was handed to it, and the coordinator holds
    three states it never produces: the local readiness gate declining to send,
    a ``ConfigEntryAuthFailed`` from an ``api`` implementation that raises it
    (the one in this repository does not on the sound paths; the handler is
    defensive, because ``api`` is a Protocol), and a failure in this method's
    own body around the call. Passing the API type through the coordinator
    would leave those three without a truthful value. The layers keep their own
    vocabulary; the mapping between them lives in ``async_play_sound``.

    There is no analogue to ``StopSoundOutcome.UNCORRELATED`` here. Nova returns
    no parsable ExecuteActionResponse, so no reply proves the device rang -- but
    that caveat holds for EVERY accepted play, not for a subset, and it is
    already stated once in IRR-CA-NO-RING-CONFIRMATION rather than encoded as a
    fourth member.
    """

    ACCEPTED = "accepted"
    """Nova took the command.

    "Accepted" is a statement about the submission, not about the device: no
    reply of the cloud API can prove a ring started (IRR-CA-NO-RING-CONFIRMATION).
    """

    SUPPRESSED = "suppressed"
    """Never sent, because a local condition that clears itself said no.

    Exactly one condition qualifies today, and it is narrower than "the gate
    said no": an active push cooldown, reached only while this integration does
    not yet know whether the device can ring at all. ``can_play_sound`` answers
    a known capability first and never consults the cooldown for such a device,
    and its remaining branches end optimistically -- an unknown device is waved
    through, not suppressed. So the cooldown is the whole of it, it passes on
    its own within ninety seconds, and "try again in a moment" is the whole of
    the advice.

    A device whose capability says it cannot ring does NOT qualify, however
    local that verdict is: waiting never changes it. It is filed under
    ``FAILED`` -- the dividing line is the remedy, not where the decision was
    taken.
    """

    FAILED = "failed"
    """The attempt was made or refused, and waiting is not the answer.

    Covers everything ``SUPPRESSED`` does not: a device that cannot ring, a
    missing action token, an expired sign-in, a server rejection, a rate limit,
    a network error, and a bug of this integration's own.

    Two of these deserve a word, because both look like patience cases and are
    not. A rate limit does pass with time, but it is filed here for symmetry
    with ``StopSoundOutcome.FAILED``, whose docstring names it explicitly; two
    sibling paths sorting the same condition differently would be worse than
    one blunt edge, and the user message names the rate limit. And
    ``SoundDispatchOutcome.NOT_SENT`` never reaches the wire either, yet a
    missing action token is a setup or credential problem far more often than a
    moment's patience.

    Note what this member does NOT claim. An authentication rejection arrives
    here as part of ``REJECTED_AUTH``, which also carries a plain HTTP 403 --
    and 403 is an authorization verdict that a server must return even for a
    resource that does not exist (Google AIP-193). Reading it as "your sign-in
    expired" would tell the owner of a deleted device to check their
    credentials, which is the misreading ``is_credential_rejection`` already
    removed once. The user-facing message therefore names the log as the carrier
    of the specific cause instead of guessing at it.
    """


# --------------------------------------------------------------------------------------
# Shared textual constants
# --------------------------------------------------------------------------------------
# Use the Greek small letter mu instead of the deprecated micro sign.
MICRO: str = "\u03bc"

# --------------------------------------------------------------------------------------
# Service device metadata & helpers (to enforce consistent identifiers across platforms)
# --------------------------------------------------------------------------------------
SERVICE_DEVICE_NAME: str = "Google Find My Integration"
SERVICE_DEVICE_MODEL: str = "Find My Device Integration"
SERVICE_DEVICE_MANUFACTURER: str = "BSkando"
SERVICE_DEVICE_TRANSLATION_KEY: str = "google_find_hub_service"

# Identifier pattern for the per-entry "integration device"
SERVICE_DEVICE_IDENTIFIER_PREFIX: str = "integration_"
# Legacy, pre-namespaced identifier used by older releases (kept for migrations)
LEGACY_SERVICE_IDENTIFIER: str = "integration"

# --------------------------------------------------------------------------------------
# Entity registry subentry helpers
# --------------------------------------------------------------------------------------
SERVICE_SUBENTRY_KEY: str = "service"
TRACKER_SUBENTRY_KEY: str = "core_tracking"
# Config entry data key reserved for subentry identifiers. Hub/root entries
# should store ``None`` to avoid being misclassified as a subentry during
# discovery lookups.
DATA_SUBENTRY_KEY: str = "subentry_key"
SUBENTRY_TYPE_SERVICE: str = "service"
SUBENTRY_TYPE_HUB: str = "hub"
SUBENTRY_TYPE_TRACKER: str = "tracker"
# Subentry types that never carry ``visible_device_ids``. Five consumers depend
# on this set -- through four literal ``in`` comparisons, because two of them
# share one predicate -- so the offering, the indexing and the writing side
# cannot drift apart:
# the options flow refuses them as assignment targets
# (``config_flow._accepts_device_assignment``); the feature sync refuses them
# as write *targets* for the tracker group and folds them onto the service key
# instead (``config_flow._canonical_core_key_of``); the runtime index folds
# them the same way and exempts their ids from its *in-memory* assignment
# bookkeeping, leaving both the stored subentry and that group's own
# allow-list alone (``coordinator/subentry.py``); and two write sinks
# refuse them, the manager's ``update_visible_device_ids`` and the
# visibility write-back in ``async_setup_entry``, which bypasses the manager
# and therefore needs its own guard (both in ``__init__.py``).
NON_DEVICE_SUBENTRY_TYPES: frozenset[str] = frozenset(
    {SUBENTRY_TYPE_SERVICE, SUBENTRY_TYPE_HUB}
)
# The *key* axis of the same judgement, shared for the same reason the type
# axis is: ``config_flow._accepts_device_assignment`` reads the group key
# before the type, and a second spelling of that set on the writing side is
# exactly the drift ``LITERAL_CORE_KEY_OWNER`` below was extracted to prevent.
# The two axes are not redundant: a subentry can be managed under a non-device
# key while its type says nothing useful, and only the key catches that.
NON_DEVICE_SUBENTRY_KEYS: frozenset[str] = frozenset({SERVICE_SUBENTRY_KEY})
# The type that *literally* owns each core key, as opposed to folding onto it.
# ``hub`` folds onto ``SERVICE_SUBENTRY_KEY`` for the assignment predicate, the
# feature sync and the runtime index, but the entity platforms match
# ``subentry_type == "service"`` literally (``known_ids_for_subentry_type``), so
# the two are not interchangeable. Where both answer for the same key, this
# table decides which one keeps it, and it lives here rather than in one of the
# consumers because the config flow (``_resolve_existing``, the stale-subentry
# sweep), the runtime index (``coordinator/subentry.py``) and the runtime
# manager (``ConfigEntrySubEntryManager._candidate_score`` in ``__init__.py``)
# must rank the same pair identically: a slot won on one side and lost on
# another is exactly the drift the shared set above exists to prevent. Sharing
# the *definition* is the point; each consumer keeps its own reader, and they
# spell the order differently -- the flow and the index rank "lower wins", the
# manager "higher wins" -- so only the *relative* order of the fields is
# shared, not the tuples themselves. The manager was the one site that did not
# read this table; it does now.
#
# "Identically" is deliberately about those shared fields and not about the
# whole tuple, because one difference survives and hiding it would make this
# comment the drift it warns against: the manager breaks a full tie on the raw
# ``subentry_id`` where the index uses the sanitised, provisional-filtered one
# -- the same flow/index difference named further down, now inherited by a
# third reader.
#
# A second difference is gone rather than named, and the reason it lasted is
# worth more than the field it removed. The manager carried two provenance
# fields (entry match, unique-id substring) *behind* the shared ones, and
# "behind" was argued to make them harmless. It does not: the contract's last
# criterion is the lowest identifier, so a field between "identifier presence"
# and that tie-break still decides pairs the contract wanted decided by id.
# The reachability argument that kept them was inverted, not merely
# optimistic. It read "it takes a subentry ``unique_id`` without the entry id,
# which no writer in ``custom_components/`` produces". True of a wrongly
# *shaped* identifier, irrelevant to the pair that actually diverged, whose
# left operand is ``unique_id=None`` -- absence needs no writer. The core
# loads the field with ``subentry_data.get("unique_id")`` and defaults it to
# ``None`` in ``ConfigSubentryFlow.async_create_entry``. How long the shape
# lasts is a *window* rather than a permanent state, and an earlier draft of
# this paragraph said "never repaired", which is the same kind of overreach:
# the subentry write-backs pass an existing value through unchanged, but
# ``_async_sync_feature_subentries`` assigns ``f"{entry_id}-{key}"`` to the
# core subentry it resolved, so the next options-flow pass normally does
# repair it. The window is entry load to next flow run, which the runtime
# index and the visibility write-back both sit inside. Both fields are
# removed; the manager's tuple is the three shared fields and nothing else.
#
# Two asymmetries stay beyond the rank, named so the next reader does not take
# "must rank the same pair identically" for more than it says. The rank block
# no longer covers only the service key -- it covers both core keys -- but the
# two *folds* still differ, and the rank cannot repair a disagreement about
# which subentries reach a key at all. The two sides therefore agree on
# ``core_tracking`` exactly where both folds land there, which is the parked
# shape (a ``tracker`` storing the service key), pinned by
# ``::test_ap4_the_manager_and_the_index_agree_on_the_tracker_slot``. They stay
# asymmetric where only the manager folds: for a legacy per-account key and for
# a ``hub`` storing ``core_tracking``, the manager names the stored subentry
# while the index names its synthetic placeholder -- measured, not inferred, so
# do not read ``managed_subentries["core_tracking"]`` as "the subentry the
# index describes" without checking the type.
#
# Within the manager itself, ``_candidate_score`` is the only reader of this
# table. ``_async_adopt_existing_unique_id`` is not: it takes the first match
# (``break``). Neither is ``_deduplicate_subentries``, and that is a correction
# rather than a standing fact -- a type rank against this table sat in its
# ``_select_canonical`` for two stages of PR #1236, and the version of this
# paragraph written then described that rank as the resting shape. It is gone.
# What ``_select_canonical`` sorts on is identifier *presence*, then the
# identifier itself, then the iteration index, then the ``subentry_id``: the
# index is the third field, not the fourth, and no type is compared anywhere in
# that tuple.
#
# Why it went is worth keeping, because it is the argument against putting one
# back. On the ``unique_id`` axis the two fields above the index tie by
# construction -- the candidates are grouped *by* ``unique_id`` -- so the
# survivor was whichever subentry the entry happened to yield first and the
# loser went to ``async_remove_subentry``, registry bindings included. The
# production shape reaching that axis is a ``hub`` and a ``service`` sharing
# ``f"{entry_id}-service"``. Ranking the literal key owner first made that
# deterministic rather than random, which on an irreversible path fixes the
# noise and not the deletion: it turned the foreign type into the *reliable*
# loser. What fixes the deletion is the guard on the removal itself, and it
# compares the full ``(group_key, subentry_type)`` identity -- the same pair the
# group axis keys on -- so a candidate differing in *either* half is spared
# entirely instead of merely ranked below. A type-only form of that guard was
# the stage in between and had a victim of its own: two ``tracker`` groups with
# distinct keys sharing one identifier read as duplicate copies, though several
# tracker groups are a supported shape. With the full identity compared, every
# foreign candidate is spared and the rank therefore decides nothing at all --
# measured over all 19 683 three-subentry shapes and a 19 987-shape sample of
# four- and five-subentry ones, not reasoned about -- and a ranking field on a
# removal path that decides nothing still reads as load-bearing to the next
# reader. The guard's own halves are graded in
# ``tests/test_subentry_manager_registry_resolution.py``: narrowing it back to
# the type comparison kills three tests, neutralising it entirely kills nine.
#
# The absence of the rank has an observer of its own, and it needs one for a
# specific reason: putting the field back changes no behaviour any test can
# see. Measured -- with it reinstated, all 130 *behavioural* tests across
# ``test_subentry_manager_registry_resolution.py`` and
# ``test_coordinator_subentry_visibility.py`` stay green (the count is the
# one before the ratchet below was added, which is the state the
# measurement describes), which is precisely
# the property that made three separate paragraphs (this one, and both scoped
# ``AGENTS.md`` files) drift into describing an intermediate stage as the
# implementation. ``::test_select_canonical_ranks_no_subentry_type`` freezes
# the tuple's field count and rejects any read of ``subentry_type`` or this
# table anywhere in ``_sort_key``, its body included, so a rank computed into a
# local above the ``return`` does not slip past it either.
#
# Mind the direction when comparing, and note that the sites do not split two
# against one: the flow and the index rank "lower wins" while
# ``_candidate_score`` alone spells it "higher wins"
# (``candidate_score > existing_score``). A rank tuple copied from either of the
# first two into that method, or the other way round, has to be inverted; one
# copied between the first two does not. ``_select_canonical`` is deliberately
# absent from that list: it picks with ``min``, but it ranks no type, so it is
# not a fourth reader of this table and a tuple must not be copied into it.
#
# On the ``(group_key, subentry_type)`` axis the type is part of the grouping
# key, so no type rank could discriminate there in the first place. That axis
# still keeps the *higher* identifier of a pair whose lower one carries
# ``unique_id=None`` -- the opposite of what the rankers above pick for it --
# and it still *removes* the loser, with ``async_sync`` calling it
# unconditionally before anything else. That remainder is tracked as ``B16`` in
# ``PLAN_GFMY_SUBENTRY_DELETION_TYPE_AXIS``, where the input shape is measured
# rather than assumed.
# Read-only on purpose: unlike the plain ``dict`` constants elsewhere in this
# module, this one is imported by two packages at once, and an in-place mutation
# in either would silently reach the other.
LITERAL_CORE_KEY_OWNER: Mapping[str, str] = MappingProxyType(
    {
        SERVICE_SUBENTRY_KEY: SUBENTRY_TYPE_SERVICE,
        TRACKER_SUBENTRY_KEY: SUBENTRY_TYPE_TRACKER,
    }
)
SERVICE_SUBENTRY_TRANSLATION_KEY: str = SERVICE_SUBENTRY_KEY
TRACKER_SUBENTRY_TRANSLATION_KEY: str = TRACKER_SUBENTRY_KEY

# Canonical feature lists for config subentries (kept sorted and import-safe)
TRACKER_FEATURE_PLATFORMS: tuple[str, ...] = (
    "button",
    "device_tracker",
    "sensor",
)
SERVICE_FEATURE_PLATFORMS: tuple[str, ...] = (
    "binary_sensor",
    "button",
    "sensor",
)


def service_device_identifier(entry_id: str) -> tuple[str, str]:
    """Return the (domain, identifier) tuple for the per-entry 'service device'.

    Args:
        entry_id: Home Assistant config entry id.

    Returns:
        A tuple suitable for DeviceInfo.identifiers, e.g. ('googlefindmy', 'integration_<entry_id>')
    """
    return (DOMAIN, f"{SERVICE_DEVICE_IDENTIFIER_PREFIX}{entry_id}")


# --------------------------------------------------------------------------------------
# Configuration keys (data vs. options separation)
# NOTE: Keep keys stable to avoid migration churn across releases.
# --------------------------------------------------------------------------------------
# Data (immutable / credentials): stored in config_entry.data
CONF_OAUTH_TOKEN: str = "oauth_token"  # kept for backward compatibility
# NOTE: This is a TokenCache key (persistent, rotating credential stored in HA Store),
# NOT a config_entry.data key. Do not persist this inside entry.data.
DATA_AAS_TOKEN: str = "aas_token"  # AAS token (TokenCache key; not in entry.data)
CONF_GOOGLE_EMAIL: str = "google_email"  # helper key when individual tokens are used
DATA_SECRET_BUNDLE: str = "secrets_data"  # full GoogleFindMyTools secrets.json content
DATA_AUTH_METHOD: str = "auth_method"  # "secrets_json" | "individual_tokens"
# Credential keys an account may or may not carry. Credentials that replace
# existing ones express "this account has no bundle / no AAS token any more" by
# leaving the key out, which is why every surface that writes credentials has to
# be able to *remove* them (config_flow._merge_credential_updates) and why the
# credential seed in async_setup_entry must not recover them from the cache
# behind a removal's back.
OPTIONAL_CREDENTIAL_KEYS: tuple[str, ...] = (DATA_SECRET_BUNDLE, DATA_AAS_TOKEN)

# Options (user-changeable): stored in config_entry.options
# (tracked_devices removed in Step 2; device inclusion is managed via HA device enable/disable)
OPT_LOCATION_POLL_INTERVAL: str = "location_poll_interval"
OPT_DEVICE_POLL_DELAY: str = "device_poll_delay"
OPT_MIN_POLL_INTERVAL: str = "min_poll_interval"
OPT_ALLOW_HISTORY_FALLBACK: str = "allow_history_fallback"
OPT_ENABLE_STATS_ENTITIES: str = "enable_stats_entities"
OPT_GOOGLE_HOME_FILTER_ENABLED: str = "google_home_filter_enabled"
OPT_GOOGLE_HOME_FILTER_KEYWORDS: str = "google_home_filter_keywords"
OPT_MAP_VIEW_TOKEN_EXPIRATION: str = "map_view_token_expiration"
OPT_MAP_VIEW_ENABLED: str = "map_view_enabled"
OPT_SEMANTIC_LOCATIONS: str = "semantic_locations"
OPT_CONTRIBUTOR_MODE: str = "contributor_mode"
OPT_IGNORED_DEVICES: str = "ignored_devices"
OPT_DELETE_CACHES_ON_REMOVE: str = "delete_caches_on_remove"
OPT_STALE_THRESHOLD: str = "stale_threshold"
OPT_SHOW_LOCATION_AGE: str = "show_location_age"
OPT_SPEED_GATE_ENABLED: str = "speed_gate_enabled"
OPT_ROUNDTRIP_CONFIRM: str = "roundtrip_confirm_enabled"
OPT_ACCURACY_GATE_ENABLED: str = "accuracy_gate_enabled"
# Legacy option key - kept for reading old configurations, no longer used
OPT_STALE_THRESHOLD_ENABLED: str = "stale_threshold_enabled"

# Canonical list of option keys supported by the integration (without tracked_devices)
OPTION_KEYS: tuple[str, ...] = (
    OPT_IGNORED_DEVICES,
    OPT_LOCATION_POLL_INTERVAL,
    OPT_DEVICE_POLL_DELAY,
    OPT_MIN_POLL_INTERVAL,
    OPT_ALLOW_HISTORY_FALLBACK,
    OPT_ENABLE_STATS_ENTITIES,
    OPT_GOOGLE_HOME_FILTER_ENABLED,
    OPT_GOOGLE_HOME_FILTER_KEYWORDS,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    OPT_MAP_VIEW_ENABLED,
    OPT_SEMANTIC_LOCATIONS,
    OPT_DELETE_CACHES_ON_REMOVE,
    OPT_CONTRIBUTOR_MODE,
    OPT_STALE_THRESHOLD,
    OPT_SHOW_LOCATION_AGE,
    OPT_SPEED_GATE_ENABLED,
    OPT_ROUNDTRIP_CONFIRM,
    OPT_ACCURACY_GATE_ENABLED,
)

# Keys which may exist historically in entry.data and should be soft-copied to entry.options
MIGRATE_DATA_KEYS_TO_OPTIONS: tuple[str, ...] = OPTION_KEYS

# --------------------------------------------------------------------------------------
# Defaults (aligned with the current implementation; adjust carefully)
# --------------------------------------------------------------------------------------
UPDATE_INTERVAL: int = 60  # seconds; DataUpdateCoordinator "tick" (lightweight)
# Minimum spacing for full device list refreshes (seconds); keeps discovery cheap
# while allowing per-device polling to stay responsive.
DEVICE_LIST_POLL_INTERVAL: int = 300

# Polling cadence
DEFAULT_LOCATION_POLL_INTERVAL: int = 300  # seconds; start a new polling cycle
DEFAULT_DEVICE_POLL_DELAY: int = 5  # seconds; inter-device delay within one cycle
DEFAULT_MIN_POLL_INTERVAL: int = 60  # seconds; hard lower bound between cycles

# Manual locate policy (button/service)
LOCATE_COOLDOWN_S: int = DEFAULT_MIN_POLL_INTERVAL
"""Cooldown window (seconds) applied after a manual locate trigger."""

# Token regeneration policy (buttons)
TOKEN_REFRESH_COOLDOWN_S: int = 180
"""Cooldown window (seconds) between token regeneration requests (3 minutes).

This window is applied per entry and per token type, so the FCM and ADM
regeneration buttons are rate-limited independently (a refresh of one does
not disable the sibling button). See ``token_refresh._cooldown_key``.
"""

# EID resolver refresh trigger debounce policy
_EID_REFRESH_DEBOUNCE_S: float = 3.0
"""Debounce window (seconds) coalescing EID-resolver refresh *triggers*.

Several read-only call sites and one inline cache path each request a resolver
refresh whenever an active device set or identity changes. Under an FCM burst
these requests arrive back-to-back, and without coalescing each one spawns its
own ``async_create_task(resolver.async_refresh())``. This window collapses such
bursts to a single scheduled task.

The value (3.0 s, sensible band 2-5 s) is deliberately small relative to the
device poll delay (``device_poll_delay`` 5-15 s) and the location poll interval
(``location_poll_interval`` 300-600 s), so a legitimate change is still picked
up promptly while same-burst triggers within 3 s coalesce. The debounce only
delays *task creation*; the actual rebuild-vs-skip decision stays with the
AP-B1 skip guard inside the resolver, so a genuine change is never dropped.
"""

# Quality/logic thresholds
DEFAULT_ALLOW_HISTORY_FALLBACK: bool = False
DEFAULT_SEMANTIC_DETECTION_RADIUS: float = (
    50.0  # meters; soft floor for semantic locations
)

# GPS accuracy fallback for invalid/missing values.
# When accuracy is reported as 0.0m, negative, NaN, or Inf, it indicates missing
# or corrupted data (0.0m GPS accuracy is physically impossible - real GPS: 3-50m).
# This fallback represents a conservative estimate based on:
#   - Bluetooth range: ~40-80m (tracker could be anywhere within BLE range)
#   - GPS error margin: ~10-30m (finder device uncertainty)
# Using 50m prevents the "Fusion Lock-in" effect where 0.0m gets extreme weight
# in weighted averaging (1/0² vs 1/20² = infinite vs 0.0025), while still being
# easily overridden by real GPS fixes.
DEFAULT_FALLBACK_ACCURACY_M: float = 50.0

# Location timestamp acceptance window
MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S: float = 24 * 60 * 60  # 24 hours

# Stats entities
DEFAULT_ENABLE_STATS_ENTITIES: bool = True

# Google Home filter
DEFAULT_GOOGLE_HOME_FILTER_ENABLED: bool = True
DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS: str = (
    "nest,google,home,mini,hub,display,chromecast,speaker"
)
GOOGLE_HOME_SPAM_THRESHOLD_MINUTES: int = 15  # debounce for repeated detections

# Map View token behavior
# Default remains "no expiration" for backwards compatibility.
DEFAULT_MAP_VIEW_TOKEN_EXPIRATION: bool = False
# Map View feature switch. Defaults to enabled, preserving existing behavior.
DEFAULT_MAP_VIEW_ENABLED: bool = True

DEFAULT_DELETE_CACHES_ON_REMOVE: bool = True

# Stale threshold: After this many seconds without a location update,
# the tracker state becomes "unknown". This is always enabled.
# Users who need the last known location can use the "Last Location" entity.
#
# Situation (empirically observed, HA history July 2026):
# A stationary device at home (smartphone with a dark display, or a Bluetooth
# tag scanned only by the household's own dark-display phones) reports through
# the FMDN network roughly every ~30 minutes. Every spurious "unknown" flip in
# the field carried last_seen ~1800-1835 s old, i.e. exactly at the previous
# 1800 s threshold: the threshold sat directly on the natural home reporting
# cadence, so any minor delay tipped an otherwise-present device into "stale"
# and blanked its coordinates until the next report arrived.
#
# The earlier 1800 s were calibrated to ACTIVE FMDN tracker-scan statistics
# (median ~3.4 min, p95 ~8 min, p99 ~14 min) that hold when many foreign
# phones continuously scan a tracker. They do NOT hold for a device sitting at
# home: a dozing smartphone reports its own position only occasionally, and a
# tag at home is seen only by the few (also dozing) household phones. A shorter
# poll does not help - Google returns the last reported fix for a dozing
# device, so last_seen only advances when the device itself reports.
#
# Rationale for the default: 3900 s (65 minutes) ~= 2x the
# observed ~30 min home cadence plus margin to tolerate one missed report
# cycle, which removes the flapping for both smartphones and tags while still
# flagging a genuinely absent device within ~1 h. The threshold stays
# user-configurable (min 300 s, max 86400 s); only the default was mistuned.
#
# Note: EID rotation (1024s) is NOT relevant here.
# Default: 3900 seconds (65 minutes) - ~2x the observed home reporting cadence
# Minimum: 300 seconds (5 minutes) - allows ~2-3 active-scan update cycles
DEFAULT_STALE_THRESHOLD: int = 3900
DEFAULT_SHOW_LOCATION_AGE: bool = True

DEFAULT_SPEED_GATE_ENABLED: bool = True
# Kinematic plausibility cap (m/s). ~1440 km/h: above the record jetstream
# airliner ground speed (~369 m/s, BA Feb-2020) and far above cruise/ICE/car,
# so even a strong-tailwind flight passes; blocks the physically impossible
# FMDN crowd-report "teleport" (Discussion #177). Own-report GPS fixes bypass
# the gate entirely (crowd-awareness), so this cap only bounds crowd reports.
DEFAULT_MAX_PLAUSIBLE_SPEED_MPS: float = 400.0

# Round-trip escape hatch (Q2-A, Discussion #177 F-CODEX-4). When the speed gate
# accepts a wide jump A->B (device seen far away), it remembers A as a return
# anchor. A later crowd fix that the forward gate would otherwise reject as a
# "teleport" is accepted instead if it lands back near A within the TTL, which
# recovers a legitimate return trip without stranding the tracker. Anchor is set
# only from a reliable A (never an estimated/accuracy-less fix), one-shot, RAM-only.
DEFAULT_ROUNDTRIP_CONFIRM: bool = True
# Anchor lifetime (s). 250 km/400 m/s general recovery ~10.4 min < 15 min, so the
# window bounds A<->B ping-pong from stale echo reports without rejecting a real
# slow move as a round trip.
ROUND_TRIP_TTL_S: int = 900
# Fixed return radius (m). Deliberately NOT radius_sum (which inflates to ~400 m on
# a double fallback and couples tolerance to unreliable crowd accuracy). Value equals
# the established BT-tracker fallback DEFAULT_ACCURACY_FALLBACK_M (200.0); kept as an
# own constant (not an alias) so it stays one-line adjustable.
ROUND_TRIP_ANCHOR_RADIUS_M: float = 200.0

# Accuracy gate (#216, and the core of #211). A coarse crowd fix must not
# displace a better, still-fresh position, and must not trip Home Assistant's
# zone logic: the reported accuracy radius is the TOLERANCE of the zone
# assignment (zone/__init__.py: ``zone_dist - zone_radius < radius``), so a
# 460 m fix 346 m away from a 32 m home zone is published as "home".
#
# Deliberately a switch, not a numeric field. The predecessor option
# ``min_accuracy_threshold`` (default 100 m, range 25-500) was an absolute
# threshold and was removed one day after it shipped ("causing too many
# problems", 47a18cc5). Its default sat right ON the median reported FMDN
# accuracy (94-148 m, Boettger et al., PoPETs 2025(4), Tab. 5+6), so it
# discarded about half of all genuine reports. The comparative rule below has
# no such failure mode: it never discards without a better alternative.
DEFAULT_ACCURACY_GATE_ENABLED: bool = True

CONTRIBUTOR_MODE_HIGH_TRAFFIC: str = "high_traffic"
CONTRIBUTOR_MODE_IN_ALL_AREAS: str = "in_all_areas"
DEFAULT_CONTRIBUTOR_MODE: str = CONTRIBUTOR_MODE_IN_ALL_AREAS

CACHE_KEY_CONTRIBUTOR_MODE: str = "nova_contributor_mode"
CACHE_KEY_LAST_MODE_SWITCH: str = "nova_last_network_mode_switch"

# Aggregate defaults dictionary for option-first reading patterns
DEFAULT_OPTIONS: dict[str, object] = {
    # Store ignored devices as mapping {device_id: {"name": str, "aliases": [str], "ignored_at": int, "source": str}}
    # Backwards-compatible: old list[str] or dict[str,str] is auto-migrated on first write.
    OPT_IGNORED_DEVICES: {},
    OPT_LOCATION_POLL_INTERVAL: DEFAULT_LOCATION_POLL_INTERVAL,
    OPT_DEVICE_POLL_DELAY: DEFAULT_DEVICE_POLL_DELAY,
    OPT_MIN_POLL_INTERVAL: DEFAULT_MIN_POLL_INTERVAL,
    OPT_ALLOW_HISTORY_FALLBACK: DEFAULT_ALLOW_HISTORY_FALLBACK,
    OPT_ENABLE_STATS_ENTITIES: DEFAULT_ENABLE_STATS_ENTITIES,
    OPT_GOOGLE_HOME_FILTER_ENABLED: DEFAULT_GOOGLE_HOME_FILTER_ENABLED,
    OPT_GOOGLE_HOME_FILTER_KEYWORDS: DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS,
    OPT_MAP_VIEW_TOKEN_EXPIRATION: DEFAULT_MAP_VIEW_TOKEN_EXPIRATION,
    OPT_MAP_VIEW_ENABLED: DEFAULT_MAP_VIEW_ENABLED,
    OPT_SEMANTIC_LOCATIONS: {},
    OPT_DELETE_CACHES_ON_REMOVE: DEFAULT_DELETE_CACHES_ON_REMOVE,
    OPT_CONTRIBUTOR_MODE: DEFAULT_CONTRIBUTOR_MODE,
    OPT_STALE_THRESHOLD: DEFAULT_STALE_THRESHOLD,
    OPT_SHOW_LOCATION_AGE: DEFAULT_SHOW_LOCATION_AGE,
    OPT_SPEED_GATE_ENABLED: DEFAULT_SPEED_GATE_ENABLED,
    OPT_ROUNDTRIP_CONFIRM: DEFAULT_ROUNDTRIP_CONFIRM,
    OPT_ACCURACY_GATE_ENABLED: DEFAULT_ACCURACY_GATE_ENABLED,
}

# -------------------- Options schema versioning (lightweight) --------------------
# Used to mark that OPT_IGNORED_DEVICES is in "v2" (mapping with metadata).
OPT_OPTIONS_SCHEMA_VERSION = "options_schema_version"
_IGN_KEY_NAME = "name"
_IGN_KEY_ALIASES = "aliases"
_IGN_KEY_IGNORED_AT = "ignored_at"
_IGN_KEY_SOURCE = "source"


def _now_epoch() -> int:
    """Return current epoch timestamp as an integer."""
    return int(time.time())


IgnoredMetadata = dict[str, object]
IgnoredMapping = dict[str, IgnoredMetadata]


def _coerce_aliases(value: object) -> list[str]:
    """Return a list of string aliases from arbitrary input."""

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [alias for alias in value if isinstance(alias, str)]
    return []


def _finite_int_or_none(value: object) -> int | None:
    """Convert a finite ``int``/``float`` to ``int``; return ``None`` otherwise.

    ``bool``, non-numeric types, and non-finite floats (``NaN``/``±inf``) all
    yield ``None``. This exists because ``int(float("nan"))`` raises
    ``ValueError`` and ``int(float("inf"))`` raises ``OverflowError`` -- either
    would defeat the drop-invalid, never-raise contract of the coercer below
    when a corrupt ``.storage`` edit or a direct-options writer stores such a
    value. Guarding on ``math.isfinite`` keeps that coercer total.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return int(value)
    return None


def coerce_ignored_mapping(raw: object) -> tuple[IgnoredMapping, bool]:
    """Coerce various legacy shapes into v2 mapping.
    Accepted inputs:
      v0: list[str]                   -> ids only
      v1: dict[str, str]             -> id -> name
      v2: dict[str, dict[str, Any]]  -> id -> metadata
    Returns (mapping, changed_flag).
    """
    changed = False
    out: IgnoredMapping = {}
    if isinstance(raw, list):
        # v0 -> v2
        for dev_id in raw:
            if isinstance(dev_id, str):
                out[dev_id] = {
                    _IGN_KEY_NAME: dev_id,
                    _IGN_KEY_ALIASES: [],
                    _IGN_KEY_IGNORED_AT: _now_epoch(),
                    _IGN_KEY_SOURCE: "migrated_v0",
                }
        changed = bool(out)
    elif isinstance(raw, Mapping):
        # str->str ? (v1)
        mapping = raw
        if all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in mapping.items()
        ):
            changed = True
            for dev_id, name in mapping.items():
                out[dev_id] = {
                    _IGN_KEY_NAME: name,
                    _IGN_KEY_ALIASES: [],
                    _IGN_KEY_IGNORED_AT: _now_epoch(),
                    _IGN_KEY_SOURCE: "migrated_v1",
                }
        else:
            # assume v2-ish; normalize keys
            for dev_id, meta in mapping.items():
                if not isinstance(dev_id, str):
                    continue
                aliases: list[str] = []
                ignored_at: int = _now_epoch()
                source = "registry"
                name = dev_id
                if isinstance(meta, Mapping):
                    str_meta: Mapping[str, object] = {
                        key: value
                        for key, value in meta.items()
                        if isinstance(key, str)
                    }
                    name_value = str_meta.get(_IGN_KEY_NAME)
                    if isinstance(name_value, str) and name_value:
                        name = name_value
                    aliases = _coerce_aliases(str_meta.get(_IGN_KEY_ALIASES))
                    ignored_value = str_meta.get(_IGN_KEY_IGNORED_AT)
                    coerced_at = _finite_int_or_none(ignored_value)
                    if coerced_at is not None:
                        ignored_at = coerced_at
                    source_value = str_meta.get(_IGN_KEY_SOURCE)
                    if isinstance(source_value, str) and source_value:
                        source = source_value
                else:
                    name = str(meta)
                out[dev_id] = {
                    _IGN_KEY_NAME: name,
                    _IGN_KEY_ALIASES: aliases,
                    _IGN_KEY_IGNORED_AT: ignored_at,
                    _IGN_KEY_SOURCE: source,
                }
    else:
        out = {}
    return out, changed


def ignored_choices_for_ui(
    ignored_map: Mapping[str, Mapping[str, object]],
) -> dict[str, str]:
    """Build UI labels 'Name (id)' directly from the stored mapping."""
    return {
        dev_id: f"{str(meta.get(_IGN_KEY_NAME) or dev_id)} ({dev_id})"
        for dev_id, meta in ignored_map.items()
    }


# --------------------------------------------------------------------------------------
# CONFIG_FIELDS — server-side validation contract for config/options flows
# --------------------------------------------------------------------------------------
# Used by config_flow.py to apply strong validators (type/min/max/step) for known keys.
# Keep keys in sync with OPTION_KEYS and default ranges used in schemas.
CONFIG_FIELDS: dict[str, dict[str, object]] = {
    OPT_LOCATION_POLL_INTERVAL: {
        "type": "int",
        "min": 60,
        "max": 3600,
        "step": 1,
    },
    OPT_DEVICE_POLL_DELAY: {
        "type": "int",
        "min": 1,
        "max": 60,
        "step": 1,
    },
    OPT_MIN_POLL_INTERVAL: {
        "type": "int",
        "min": 30,
        "max": 3600,
        "step": 1,
    },
    OPT_ALLOW_HISTORY_FALLBACK: {
        "type": "bool",
    },
    OPT_ENABLE_STATS_ENTITIES: {
        "type": "bool",
    },
    OPT_GOOGLE_HOME_FILTER_ENABLED: {
        "type": "bool",
    },
    OPT_GOOGLE_HOME_FILTER_KEYWORDS: {
        "type": "str",
    },
    OPT_MAP_VIEW_TOKEN_EXPIRATION: {
        "type": "bool",
    },
    OPT_MAP_VIEW_ENABLED: {
        "type": "bool",
    },
    OPT_STALE_THRESHOLD: {
        "type": "int",
        "min": 300,  # 5 minutes - allows ~2-3 typical FMDN update cycles
        "max": 86400,  # max 24 hours
        "step": 60,
    },
    OPT_SPEED_GATE_ENABLED: {
        "type": "bool",
    },
    OPT_ROUNDTRIP_CONFIRM: {
        "type": "bool",
    },
    OPT_ACCURACY_GATE_ENABLED: {
        "type": "bool",
    },
    # OPT_IGNORED_DEVICES is intentionally omitted: it is managed by a dedicated
    # visibility flow and not edited as a raw field (list of ids).
}

# --------------------------------------------------------------------------------------
# Services (align with services.yaml and translations)
# --------------------------------------------------------------------------------------
SERVICE_LOCATE_DEVICE: str = "locate_device"
SERVICE_PLAY_SOUND: str = "play_sound"
SERVICE_STOP_SOUND: str = "stop_sound"
SERVICE_LOCATE_EXTERNAL: str = "locate_external"

SERVICE_REFRESH_DEVICE_URLS: str = "refresh_device_urls"
# Optional compatibility alias (remove once all imports use SERVICE_REFRESH_DEVICE_URLS)
SERVICE_REFRESH_URLS: str = SERVICE_REFRESH_DEVICE_URLS

SERVICE_REBUILD_REGISTRY: str = "rebuild_registry"

# Optional attrs/modes for rebuild service
ATTR_MODE: str = "mode"
ATTR_DEVICE_IDS: str = "device_ids"
MODE_REBUILD: str = "rebuild"
MODE_MIGRATE: str = "migrate"
REBUILD_REGISTRY_MODES: tuple[str, str] = (MODE_REBUILD, MODE_MIGRATE)

# --------------------------------------------------------------------------------------
# Optional request timeouts (prefer central constants over scattered literals)
# --------------------------------------------------------------------------------------
LOCATION_REQUEST_TIMEOUT_S: int = 30

# Total budget for a single Nova HTTP round-trip. Single source of truth for the
# aiohttp ``ClientTimeout(total=...)`` used in NovaApi/nova_request.py. The outer
# poll guard below budgets against this value, so the two MUST move together;
# nova_request.py imports this constant instead of repeating the literal.
NOVA_REQUEST_TOTAL_TIMEOUT_S: int = 30

# Outer per-device poll guard. A single location request runs two *sequential*
# phases: first the Nova HTTP round-trip (capped at NOVA_REQUEST_TOTAL_TIMEOUT_S),
# then the FCM wait (capped at LOCATION_REQUEST_TIMEOUT_S). The outer guard must
# cover BOTH phases plus a small grace, otherwise a slow-but-successful HTTP call
# pushes the inner FCM wait past the guard and the outer wait_for raises a
# spurious TimeoutError before the inner request can return its clean empty
# result (Nygard: stagger nested timeout budgets, decreasing from outer to
# inner). The +5s grace absorbs scheduling/setup overhead between the phases.
# Note: this budgets a single HTTP attempt; a multi-retry backoff sequence inside
# nova_request can still exceed it, in which case the guard correctly intervenes.
POLL_DEVICE_OUTER_TIMEOUT_S: int = (
    NOVA_REQUEST_TOTAL_TIMEOUT_S + LOCATION_REQUEST_TIMEOUT_S + 5
)

# --------------------------------------------------------------------------------------
# HTTP headers / User-Agent (Nova API)
# --------------------------------------------------------------------------------------
NOVA_API_USER_AGENT: str = "fmd/20006320; gzip"
"""Canonical User-Agent for Nova API calls.

Used by `NovaApi/nova_request.py` for all upstream requests. Keep stable unless
there is a server-side change in expectations. Includes `gzip` to advertise
support for compressed responses.
"""

# --------------------------------------------------------------------------------------
# FCM socket tuning (used by Auth.firebase_messaging client)
# --------------------------------------------------------------------------------------
FCM_CLIENT_HEARTBEAT_INTERVAL_S: int = 20
FCM_SERVER_HEARTBEAT_INTERVAL_S: int = 10
FCM_IDLE_RESET_AFTER_S: float = 90.0
FCM_CONNECTION_RETRY_COUNT: int = 5
FCM_MONITOR_INTERVAL_S: int = 1
FCM_ABORT_ON_SEQ_ERROR_COUNT: int = 3

# --------------------------------------------------------------------------------------
# FCM data-starvation liveness watchdog (re-arming zombie self-heal)
# --------------------------------------------------------------------------------------
# A "zombie" FCM session is STARTED and keeps acking heartbeats (so the activity
# clock and readiness gate both look healthy), yet has stopped delivering
# ``push_received`` data messages despite locates being sent. The one-shot
# first-locate reconnect only covers a *young* session (age < _CHURN_WINDOW_S);
# a long-running session that goes silent is only healed by a manual reload
# today (empirically sometimes twice). The watchdog below detects this and
# forces a cooperative, backoff-limited reconnect that re-arms after each cycle.
#
# FCM_DATA_STARVATION_S: minimum gap (seconds) since the last real data delivery
# before a STARTED, heartbeating, locate-active session is treated as starved.
# Calibration (live DEBUG capture 2026-07-04, 2h23m healthy window): normal gaps
# between data messages reach ~349s (they grow with the observation window; only
# ~305s over a 71-min window), so the threshold must be a *generous* multiple of
# the poll cycle, well above 349s, never a value near it. 900s ≈ 3 default poll
# cycles (300s) and ≈ 30× the 30s locate timeout — conservatively large so the
# watchdog reacts later, never more aggressively (storm-safe by construction).
# The MCS session lifetime is ~2h, so 15-minute detection is fast enough.
FCM_DATA_STARVATION_S: int = 900

# FCM_ZOMBIE_CHECK_INTERVAL_S: how often the watchdog *evaluates* starvation.
# The supervisor monitor loop ticks every FCM_MONITOR_INTERVAL_S (1s); evaluating
# the predicate every tick is wasteful, so the watchdog is throttled to run at
# most once per this interval (30× rarer than the tick, far denser than the
# starvation window, so detection latency stays well under a minute).
FCM_ZOMBIE_CHECK_INTERVAL_S: int = 30

# FCM_ZOMBIE_RECONNECT_BACKOFF_BASE_S: first backoff window after the first
# watchdog reconnect. Subsequent windows double (60 → 120 → 240 → …) up to the
# cap below, so a session that stays starved is retried with growing patience.
FCM_ZOMBIE_RECONNECT_BACKOFF_BASE_S: int = 60

# FCM_ZOMBIE_RECONNECT_BACKOFF_CAP_S: upper bound of the exponential backoff
# (equals the starvation window: never retry faster than the detection cadence).
FCM_ZOMBIE_RECONNECT_BACKOFF_CAP_S: int = 900

# FCM_ZOMBIE_MAX_RECONNECTS: hard ceiling on watchdog reconnects per starvation
# episode. The count (and backoff) reset on the first successful data delivery
# after a reconnect. Together with the cap and the one-in-flight lock this bounds
# the worst case to a slow, finite series — never a reconnect storm / self-DoS.
FCM_ZOMBIE_MAX_RECONNECTS: int = 5

# --------------------------------------------------------------------------------------
# Feature Flags (compile-time toggles for optional functionality)
# --------------------------------------------------------------------------------------
# FMDN Finder: Upload location reports to Google for FMDN beacons detected by Bermuda.
# This feature is prepared but disabled by default. Set to True to enable.
# When disabled, no Bermuda listeners are registered and no logs are produced.
FEATURE_FMDN_FINDER_ENABLED: bool = False

# --------------------------------------------------------------------------------------
# Events & Repairs (auth status)
# --------------------------------------------------------------------------------------
# Events fired by the coordinator to allow user automations.
EVENT_AUTH_ERROR: str = f"{DOMAIN}.authentication_error"
EVENT_AUTH_OK: str = f"{DOMAIN}.authentication_ok"

# Translation key for the dedicated auth-status binary_sensor entity.
TRANSLATION_KEY_AUTH_STATUS: str = "nova_auth_status"

# Translation key for the dedicated encryption-key-status (ENUM) sensor entity.
TRANSLATION_KEY_ENCRYPTION_KEY_STATUS: str = "encryption_key_status"

# Issue key used for Repairs (translations use the same key).
ISSUE_AUTH_EXPIRED_KEY: str = "auth_expired"

# Issue/translation keys for common repair issues (keep aligned with translations/*.json).
ISSUE_MULTIPLE_CONFIG_ENTRIES: str = "multiple_config_entries"
TRANSLATION_KEY_CACHE_PURGED: str = "cache_purged"
TRANSLATION_KEY_UNIQUE_ID_COLLISION: str = "unique_id_collision"
TRANSLATION_KEY_DUPLICATE_ACCOUNT: str = "duplicate_account_entries"

# Global Repairs issue raised when HACS has written new integration code to disk
# but the running process still executes the previously loaded version.
ISSUE_RESTART_REQUIRED_KEY: str = "restart_required"
TRANSLATION_KEY_RESTART_REQUIRED: str = "restart_required"

# Repairs issue per config entry: the device_tracker attributes location_age,
# location_status and (main tracker) coarse_* keep the value of the last
# observation and will be removed. Two translation keys, with and without the
# list of automations and scripts that reference a tracker of the entry.
ISSUE_TRACKER_ATTRIBUTES_FROZEN_KEY: str = "tracker_attributes_frozen"
TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN: str = "tracker_attributes_frozen"
TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN_NO_AUTOMATIONS: str = (
    "tracker_attributes_frozen_no_automations"
)

# Integration-owned deprecation (AGENTS.md, "Integration-owned deprecations"):
# the frozen tracker attributes are removed in the first integration release
# on or after this Home Assistant version. Set as an assumption when the
# repair issue was introduced (that month plus eight months, a buffer for the
# upstream release) and checked again before that release. It may be raised
# with a reason in the PR description, never lowered.
TRACKER_ATTRS_BREAKS_IN_HA_VERSION: str = "2027.6.0"


def frozen_attributes_issue_id(entry_id: str) -> str:
    """Return the Repairs issue id announcing the held tracker attributes.

    Pattern: 'tracker_attributes_frozen_<entry_id>'
    """
    return f"{ISSUE_TRACKER_ATTRIBUTES_FROZEN_KEY}_{entry_id}"


def issue_id_for(entry_id: str) -> str:
    """Return a stable Repairs issue_id for a given config entry.

    Pattern: 'auth_expired_<entry_id>'
    """
    return f"{ISSUE_AUTH_EXPIRED_KEY}_{entry_id}"


# --------------------------------------------------------------------------------------
# Storage (entry-scoped key prefix; each entry gets its own Store file)
# --------------------------------------------------------------------------------------
STORAGE_KEY: str = f"{DOMAIN}_secrets"
STORAGE_VERSION: int = 1

# --------------------------------------------------------------------------------------
# Map token helpers (centralized, import-safe)
# --------------------------------------------------------------------------------------
WEEK_SECONDS: int = 7 * 24 * 60 * 60


def map_token_secret_seed(
    ha_uuid: str,
    entry_id: str,
    expiration_enabled: bool,
    now: int | None = None,
) -> str:
    """Return the secret seed string used to derive map-view tokens.

    Callers should pass the seed to :func:`map_token_hex_digest` to obtain the
    user-facing token string used in map URLs and entity configuration links.

    Args:
        ha_uuid: Home Assistant instance UUID (e.g., `hass.data["core.uuid"]`).
        entry_id: Config entry id used to namespace tokens per entry.
        expiration_enabled: If True, use a weekly-rolling bucket; else static.
        now: Optional epoch seconds, useful for tests; defaults to current time.

    Returns:
        A deterministic seed string in the form:
            "<uuid>:<entry_id>:<week>"  (rolling)
        or  "<uuid>:<entry_id>:static" (static)
    """
    safe_uuid = ha_uuid or "ha"
    if expiration_enabled:
        if now is None:
            now = int(time.time())
        week = now // WEEK_SECONDS
        return f"{safe_uuid}:{entry_id}:{week}"
    return f"{safe_uuid}:{entry_id}:static"


def map_token_hex_digest(seed: str) -> str:
    """Return the 16-character hex token derived from a seed value.

    The helper generates a SHA-256 digest of ``seed`` and truncates it to the
    first 16 hexadecimal characters, ensuring a consistent token format across
    all components.
    """

    return hashlib.sha256(seed.encode()).hexdigest()[:16]


# Options key holding an optional list of *additional* ``secrets.json`` paths the
# discovery watcher observes on top of the default ``Auth/secrets.json``.
SECRETS_EXTRA_WATCH_PATHS: str = "secrets_extra_watch_paths"


__all__ = [
    "DOMAIN",
    "INTEGRATION_VERSION",
    "SERVICE_DEVICE_NAME",
    "SERVICE_DEVICE_MODEL",
    "SERVICE_DEVICE_MANUFACTURER",
    "SERVICE_DEVICE_TRANSLATION_KEY",
    "SERVICE_DEVICE_IDENTIFIER_PREFIX",
    "LEGACY_SERVICE_IDENTIFIER",
    "SERVICE_SUBENTRY_KEY",
    "TRACKER_SUBENTRY_KEY",
    "DATA_SUBENTRY_KEY",
    "SUBENTRY_TYPE_SERVICE",
    "SUBENTRY_TYPE_HUB",
    "SUBENTRY_TYPE_TRACKER",
    "NON_DEVICE_SUBENTRY_TYPES",
    "NON_DEVICE_SUBENTRY_KEYS",
    "LITERAL_CORE_KEY_OWNER",
    "service_device_identifier",
    "CONF_OAUTH_TOKEN",
    "DATA_AAS_TOKEN",
    "CONF_GOOGLE_EMAIL",
    "DATA_SECRET_BUNDLE",
    "DATA_AUTH_METHOD",
    "OPTIONAL_CREDENTIAL_KEYS",
    "OPT_IGNORED_DEVICES",
    "OPT_LOCATION_POLL_INTERVAL",
    "OPT_DEVICE_POLL_DELAY",
    "OPT_MIN_POLL_INTERVAL",
    "OPT_ALLOW_HISTORY_FALLBACK",
    "OPT_ENABLE_STATS_ENTITIES",
    "OPT_GOOGLE_HOME_FILTER_ENABLED",
    "OPT_GOOGLE_HOME_FILTER_KEYWORDS",
    "OPT_MAP_VIEW_TOKEN_EXPIRATION",
    "OPT_MAP_VIEW_ENABLED",
    "OPTION_KEYS",
    "OPT_DELETE_CACHES_ON_REMOVE",
    "OPT_STALE_THRESHOLD",
    "OPT_SHOW_LOCATION_AGE",
    "OPT_SPEED_GATE_ENABLED",
    "OPT_ROUNDTRIP_CONFIRM",
    "OPT_ACCURACY_GATE_ENABLED",
    "OPT_STALE_THRESHOLD_ENABLED",
    "MIGRATE_DATA_KEYS_TO_OPTIONS",
    "UPDATE_INTERVAL",
    "DEFAULT_SPEED_GATE_ENABLED",
    "DEFAULT_MAX_PLAUSIBLE_SPEED_MPS",
    "DEFAULT_ROUNDTRIP_CONFIRM",
    "DEFAULT_ACCURACY_GATE_ENABLED",
    "ROUND_TRIP_TTL_S",
    "ROUND_TRIP_ANCHOR_RADIUS_M",
    "DEFAULT_LOCATION_POLL_INTERVAL",
    "DEFAULT_DEVICE_POLL_DELAY",
    "DEFAULT_MIN_POLL_INTERVAL",
    "LOCATE_COOLDOWN_S",
    "DEFAULT_ALLOW_HISTORY_FALLBACK",
    "DEFAULT_ENABLE_STATS_ENTITIES",
    "DEFAULT_GOOGLE_HOME_FILTER_ENABLED",
    "DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS",
    "GOOGLE_HOME_SPAM_THRESHOLD_MINUTES",
    "DEFAULT_MAP_VIEW_TOKEN_EXPIRATION",
    "DEFAULT_MAP_VIEW_ENABLED",
    "DEFAULT_DELETE_CACHES_ON_REMOVE",
    "DEFAULT_STALE_THRESHOLD",
    "DEFAULT_SHOW_LOCATION_AGE",
    "DEFAULT_OPTIONS",
    "CONFIG_FIELDS",
    "TOKEN_REFRESH_COOLDOWN_S",
    "_EID_REFRESH_DEBOUNCE_S",
    "SERVICE_LOCATE_DEVICE",
    "SERVICE_PLAY_SOUND",
    "SERVICE_STOP_SOUND",
    "SERVICE_LOCATE_EXTERNAL",
    "SERVICE_REFRESH_DEVICE_URLS",
    "SERVICE_REFRESH_URLS",
    "SERVICE_REBUILD_REGISTRY",
    "ATTR_MODE",
    "ATTR_DEVICE_IDS",
    "MODE_REBUILD",
    "MODE_MIGRATE",
    "REBUILD_REGISTRY_MODES",
    "LOCATION_REQUEST_TIMEOUT_S",
    "NOVA_REQUEST_TOTAL_TIMEOUT_S",
    "POLL_DEVICE_OUTER_TIMEOUT_S",
    "NOVA_API_USER_AGENT",
    "FCM_CLIENT_HEARTBEAT_INTERVAL_S",
    "FCM_SERVER_HEARTBEAT_INTERVAL_S",
    "FCM_IDLE_RESET_AFTER_S",
    "FCM_CONNECTION_RETRY_COUNT",
    "FCM_MONITOR_INTERVAL_S",
    "FCM_ABORT_ON_SEQ_ERROR_COUNT",
    "FCM_DATA_STARVATION_S",
    "FCM_ZOMBIE_CHECK_INTERVAL_S",
    "FCM_ZOMBIE_RECONNECT_BACKOFF_BASE_S",
    "FCM_ZOMBIE_RECONNECT_BACKOFF_CAP_S",
    "FCM_ZOMBIE_MAX_RECONNECTS",
    "EVENT_AUTH_ERROR",
    "EVENT_AUTH_OK",
    "TRANSLATION_KEY_AUTH_STATUS",
    "TRANSLATION_KEY_ENCRYPTION_KEY_STATUS",
    "ISSUE_AUTH_EXPIRED_KEY",
    "ISSUE_MULTIPLE_CONFIG_ENTRIES",
    "TRANSLATION_KEY_CACHE_PURGED",
    "TRANSLATION_KEY_UNIQUE_ID_COLLISION",
    "TRANSLATION_KEY_DUPLICATE_ACCOUNT",
    "ISSUE_RESTART_REQUIRED_KEY",
    "ISSUE_TRACKER_ATTRIBUTES_FROZEN_KEY",
    "frozen_attributes_issue_id",
    "TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN",
    "TRANSLATION_KEY_TRACKER_ATTRIBUTES_FROZEN_NO_AUTOMATIONS",
    "TRACKER_ATTRS_BREAKS_IN_HA_VERSION",
    "TRANSLATION_KEY_RESTART_REQUIRED",
    "issue_id_for",
    "STORAGE_KEY",
    "STORAGE_VERSION",
    "coerce_ignored_mapping",
    "ignored_choices_for_ui",
    "WEEK_SECONDS",
    "map_token_secret_seed",
    "map_token_hex_digest",
    "SECRETS_EXTRA_WATCH_PATHS",
]
