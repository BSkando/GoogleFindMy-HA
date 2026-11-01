"""Registry utilities for the coordinator.

This module contains pure functions for device registry operations extracted from
coordinator.py for improved testability and maintainability (Phase 4).

Contents:
- extract_device_display_name(): Get human-friendly device name
- build_legacy_device_registry_kwargs(): Translate modern kwargs to legacy
- needs_legacy_kwarg_retry(): Check if legacy retry is needed
- parse_device_identifier(): Parse identifier tuple with multi-account support
"""

from __future__ import annotations

import inspect
import logging
import re
from collections.abc import Callable, Collection, Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Final, Literal

# Re-export constants used by this module's functions
from ...const import (
    LEGACY_SERVICE_IDENTIFIER,
    SERVICE_DEVICE_IDENTIFIER_PREFIX,
)

_LOGGER = logging.getLogger(__name__)

__all__ = [
    "LEGACY_SERVICE_IDENTIFIER",
    "NOT_GIVEN",
    "OWNERSHIP_ADD_KWARGS",
    "OWNERSHIP_REMOVE_KWARGS",
    "SERVICE_DEVICE_IDENTIFIER_PREFIX",
    "DeviceOwnership",
    "DeviceRegistryCapabilities",
    "DeviceRegistryOperation",
    "NotGivenType",
    "OwnershipIntent",
    "STALE_REMOVAL_REQUIRED_MISSES",
    "StaleDevicePlan",
    "build_canonical_unique_id",
    "build_entity_unique_id_candidates",
    "build_legacy_device_registry_kwargs",
    "detect_device_registry_capabilities",
    "device_belongs_to_entry",
    "device_owning_entry_ids",
    "execute_ownership_plan",
    "extract_canonical_device_id",
    "extract_device_display_name",
    "extract_service_subentry_ids",
    "extract_subentry_links",
    "has_hub_link",
    "has_subentry_link",
    "is_hub_device_check",
    "iter_all_devices",
    "match_entity_by_device_id",
    "needs_legacy_kwarg_retry",
    "normalize_device_name",
    "parse_device_identifier",
    "plan_device_ownership",
    "plan_stale_device_removals",
    "read_device_ownership",
    "resolve_device_by_identifiers",
    "resolve_tracker_subentry_candidate",
    "should_defer_service_subentry",
]


# ---------------------------------------------------------------------------
# Device Display Name
# ---------------------------------------------------------------------------


def extract_device_display_name(
    name_by_user: str | None,
    name: str | None,
    fallback: str | None,
) -> str:
    """Return the best human-friendly device name without sensitive data.

    Priority order:
    1. User-set name (name_by_user)
    2. Device name (name)
    3. Fallback

    Args:
        name_by_user: User-customized name from device registry.
        name: Default device name from device registry.
        fallback: Fallback name if others are unavailable.

    Returns:
        The best available name, stripped of leading/trailing whitespace.
        Returns empty string if all inputs are None/empty.
    """
    return (name_by_user or name or fallback or "").strip()


# ---------------------------------------------------------------------------
# Legacy Device Registry Kwargs
# ---------------------------------------------------------------------------


def build_legacy_device_registry_kwargs(
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """Translate ownership keywords for cores that predate the current names.

    Mapping applied here:
    - add_config_entry_id -> config_entry_id
    - add_config_subentry_id -> config_subentry_id
    - remove_config_subentry_id -> dropped, it has no legacy counterpart

    **The surviving half, and why it stays.** ``AGENTS.md`` forbids dropping one
    half of the removal pair, because on a subentry-aware core
    ``remove_config_entry_id`` alone means "remove every subentry link of this
    entry" and deletes a device that held only one. The rule is not waived here;
    what carries this exception is reach, not a claim that such a core has no
    subentry links. It may well have them, under the older
    ``config_subentry_id`` spelling, which is exactly the core this translator
    models.

    **Reach, derived from the code rather than assumed:**
    :func:`needs_legacy_kwarg_retry` returns ``False`` immediately while
    :attr:`DeviceRegistryCapabilities.subentry_kwarg_for_shim` resolves to
    ``add_config_subentry_id``, that is while the signature carries no bare
    ``config_subentry_id``. At tag ``2025.9.1`` ``async_update_device`` carries
    ``add_config_subentry_id`` and no bare spelling, and the declared minimum is
    that tag. This translator therefore serves registry doubles and cores below
    the declared minimum. The behaviour it produces is pinned by
    ``tests/test_coordinator.py::test_device_registry_wrapper_retries_with_legacy_remove_config_subentry_kwarg``;
    if a supported core ever reintroduced the bare spelling as an alias, this
    exception would have to be revisited rather than inherited.

    Version note corrected: the rename predates our declared minimum.
    Tag ``2025.9.1`` already ships ``add_config_subentry_id`` on
    ``async_update_device`` (``homeassistant/helpers/device_registry.py``, line
    1014 in that tag). This *naming* branch therefore serves registry doubles
    rather than supported cores, unlike the legacy *ownership* path that emits
    the ``add_*``/``remove_*`` quadruple, which is live on the declared minimum.
    Note that ``async_get_or_create`` keeps
    ``config_subentry_id`` in *every* release up to 2026.9: the keyword choice
    is a distinction between callers, not between core versions.

    Do not read this translator as a licence to write the old keywords. From
    Core 2026.8 ``add_config_entry_id`` attaches nothing and
    ``remove_config_entry_id`` on the owning entry deletes the device; see
    ``docs/AI_DEPRECATIONS_GUIDE.md``, section VI.

    Args:
        kwargs: Modern keyword arguments for device registry calls.

    Returns:
        A new dict with legacy keyword argument names.
        The original dict is not modified.
    """
    legacy_kwargs = dict(kwargs)

    if "add_config_entry_id" in legacy_kwargs:
        legacy_kwargs["config_entry_id"] = legacy_kwargs.pop("add_config_entry_id")

    if "add_config_subentry_id" in legacy_kwargs:
        legacy_kwargs["config_subentry_id"] = legacy_kwargs.pop(
            "add_config_subentry_id"
        )

    if "remove_config_subentry_id" in legacy_kwargs:
        legacy_kwargs.pop("remove_config_subentry_id")

    return legacy_kwargs


# ---------------------------------------------------------------------------
# Legacy Retry Detection
# ---------------------------------------------------------------------------


def needs_legacy_kwarg_retry(
    kwarg_name: str | None,
    err_str: str,
    kwargs: Mapping[str, Any],
) -> bool:
    """Determine if a TypeError requires legacy kwargs retry.

    When calling device registry APIs, modern Home Assistant versions accept
    new keyword names (add_config_entry_id, add_config_subentry_id). Older
    versions will raise TypeError for these unknown keywords.

    Args:
        kwarg_name: The config_subentry kwarg name supported by the API.
                    If "add_config_subentry_id", the API is modern and no retry needed.
        err_str: The string representation of the TypeError.
        kwargs: The keyword arguments that caused the error.

    Returns:
        True if the error indicates a legacy API that needs kwargs rewriting.
        False if the error is unrelated or the API is modern.
    """
    # Modern registries accept the renamed "add_config_subentry_id" keyword
    # and should surface the original TypeError to callers. Only older
    # versions that reject the new keyword should trigger a legacy rewrite.
    if kwarg_name == "add_config_subentry_id":
        return False

    # Check if each modern kwarg appears in both the error and our kwargs
    if "add_config_entry_id" in kwargs and "add_config_entry_id" in err_str:
        return True

    if "add_config_subentry_id" in kwargs and "add_config_subentry_id" in err_str:
        return True

    if "remove_config_subentry_id" in kwargs and "remove_config_subentry_id" in err_str:
        return True

    return False


# ---------------------------------------------------------------------------
# Device Registry Capability Profile and Ownership Planner
# ---------------------------------------------------------------------------
#
# The minimum supported core is 2025.9.1 (hacs.json, pyproject.toml). It knows
# neither ``new_config_entry_id`` nor ``new_config_subentry_id``, so the switch
# below is a runtime signature probe, not a version comparison. Do not replace
# it with a version check and do not delete the legacy branches: they are the
# only ones that run on the declared minimum. Raising the minimum to 2026.8.0 is
# not permitted before six months after that release; see AGENTS.md.
#
# See docs/AI_DEPRECATIONS_GUIDE.md, section VI, for why the old keywords
# changed meaning rather than name.

#: The ownership keywords that *add* a link on cores below 2026.8. From 2026.8
#: on, ``add_config_entry_id`` alone attaches nothing; it only arms a pending
#: move that a matching ``remove_config_entry_id`` then completes.
OWNERSHIP_ADD_KWARGS: tuple[str, ...] = (
    "add_config_entry_id",
    "add_config_subentry_id",
)

#: The ownership keywords that *drop* a link. From 2026.8 on,
#: ``remove_config_entry_id`` on the owning entry deletes the device and every
#: entity attached to it.
OWNERSHIP_REMOVE_KWARGS: tuple[str, ...] = (
    "remove_config_entry_id",
    "remove_config_subentry_id",
)


@dataclass(frozen=True, slots=True)
class DeviceRegistryCapabilities:
    """What the installed device registry callable accepts.

    Derived from the callable's signature, never from a version string: a fork,
    a backport or a patched core can carry any number, but the signature is what
    the call actually has to satisfy.
    """

    has_new_config_entry_id: bool
    has_new_config_subentry_id: bool
    has_add_config_subentry_id: bool
    has_legacy_config_subentry_id: bool
    accepts_var_keyword: bool

    @property
    def single_owner_model(self) -> bool:
        """True on Core 2026.8+, where a device has exactly one owner.

        Both new keywords are required, not just one: they arrived together in
        2026.8.0 and a callable carrying only one of them is a partial double,
        not a core. Verified against tags 2025.9.1, 2026.8.0 and 2026.9.0.
        """
        return self.has_new_config_entry_id and self.has_new_config_subentry_id

    @property
    def subentry_kwarg_for_update(self) -> str | None:
        """Keyword that expresses "the device shall sit in this subentry"."""
        if self.single_owner_model:
            return "new_config_subentry_id"
        if self.has_add_config_subentry_id:
            return "add_config_subentry_id"
        if self.has_legacy_config_subentry_id or self.accepts_var_keyword:
            return "config_subentry_id"
        return None

    @property
    def subentry_kwarg_for_shim(self) -> str | None:
        """Keyword a plain compatibility rename may use.

        Deliberately never ``new_config_subentry_id``: that keyword *moves* a
        device, and a rename shim must not become an ownership change. Ownership
        goes through :func:`plan_device_ownership` instead.

        This also answers the ``async_get_or_create`` question without a special
        case. Measured at tags 2025.9.1, 2026.8.0 and 2026.9.0: that call takes
        ``config_subentry_id`` in every one of them and never takes a ``new_*``
        keyword, so the preference order below already picks the right name for
        it.
        """
        if self.has_legacy_config_subentry_id:
            return "config_subentry_id"
        if self.has_add_config_subentry_id:
            return "add_config_subentry_id"
        if self.accepts_var_keyword:
            return "config_subentry_id"
        return None


def detect_device_registry_capabilities(
    call: Callable[..., Any],
) -> DeviceRegistryCapabilities:
    """Derive the capability profile from ``call``'s signature.

    An unreadable signature degrades to the all-false profile, which selects the
    legacy branches everywhere. That is the safe direction: guessing
    "single owner" would send ``new_config_subentry_id`` at a test double that
    swallows unknown keywords, and the test would pass while production broke.
    """
    parameters: Mapping[str, inspect.Parameter]
    try:
        parameters = inspect.signature(call).parameters
    except (TypeError, ValueError):  # pragma: no cover - defensive fallback
        parameters = {}
    return DeviceRegistryCapabilities(
        has_new_config_entry_id="new_config_entry_id" in parameters,
        has_new_config_subentry_id="new_config_subentry_id" in parameters,
        has_add_config_subentry_id="add_config_subentry_id" in parameters,
        has_legacy_config_subentry_id="config_subentry_id" in parameters,
        accepts_var_keyword=any(
            param.kind is inspect.Parameter.VAR_KEYWORD for param in parameters.values()
        ),
    )


class _Sentinel(Enum):
    """One-member enum used as a typed "argument not given" marker.

    ``None`` cannot carry that meaning here: for ``detach_subentry_id`` it is a
    real value, namely the hub link (a device sitting directly on the entry).
    An enum rather than a bare ``object()`` so that ``mypy --strict`` can narrow
    on it, the same shape Home Assistant uses for ``UNDEFINED``.
    """

    NOT_GIVEN = "not_given"


NOT_GIVEN: Final = _Sentinel.NOT_GIVEN

NotGivenType = Literal[_Sentinel.NOT_GIVEN]
"""Type of :data:`NOT_GIVEN`, for signatures that pass the marker through."""


class OwnershipIntent(Enum):
    """What the caller wants, independent of the core version."""

    MOVE = "move"
    """Pattern A: the device shall live in ``target_subentry_id``."""

    ENSURE = "ensure"
    """Pattern B: make sure it already does."""

    DETACH = "detach"
    """Pattern C: give up one specific ownership link."""


@dataclass(frozen=True, slots=True)
class DeviceOwnership:
    """Who owns a device right now, as read from the device entry.

    ``None`` instead of an instance means *unknown*, which is not the same as
    "owned by nobody". DETACH refuses to act on an unknown state, because on a
    single-owner core the removal of the owning entry deletes the device. That
    distinction is the reason this is a class rather than two ``str | None``
    parameters: with two parameters both cases collapse into ``None``.
    """

    entry_id: str | None
    subentry_id: str | None


@dataclass(frozen=True, slots=True)
class DeviceRegistryOperation:
    """One executable registry step."""

    method: str
    """Either ``async_update_device`` or ``async_remove_device``."""

    kwargs: Mapping[str, Any]


def plan_device_ownership(  # noqa: PLR0913 - one parameter per ownership axis
    intent: OwnershipIntent,
    *,
    caps: DeviceRegistryCapabilities,
    device_id: str,
    entry_id: str,
    target_subentry_id: str | None = None,
    detach_subentry_id: str | None | NotGivenType = NOT_GIVEN,
    current: DeviceOwnership | None = None,
    extra: Mapping[str, Any] | None = None,
) -> tuple[DeviceRegistryOperation, ...]:
    """Translate an ownership intent into registry operations.

    ``current`` is the ownership read from the device entry, or ``None`` when the
    caller did not resolve the device. On cores below 2026.8 a device entry has
    no ``config_entry_id`` attribute, so ``current`` is ``None`` there. The
    legacy DETACH and ENSURE branches ignore it; the legacy MOVE reads
    ``current.subentry_id`` as a fallback, but only when ``detach_subentry_id``
    was omitted.

    ``detach_subentry_id`` names the link to give up: ``None`` means the hub link
    (the device sitting directly on the entry). It is *not* interchangeable with
    ``target_subentry_id``. Core only removes ownership when the removed subentry
    matches the one the device actually sits in; see
    ``homeassistant/helpers/device_registry.py``, ``async_update_device``, the
    branch guarded by ``remove_config_entry_id == old.config_entry_id`` and a
    matching ``remove_config_subentry_id``.

    Args:
        intent: What the caller wants to achieve.
        caps: Capability profile of the installed ``async_update_device``.
        device_id: The device to act on.
        entry_id: Our config entry.
        target_subentry_id: Where the device shall sit (MOVE, ENSURE).
        detach_subentry_id: Which link to give up (DETACH, legacy MOVE).
        current: Ownership read from the device entry, or ``None`` if unknown.
        extra: Additional keyword arguments for an ``async_update_device``
            operation. Ownership and subentry keywords do not belong in here;
            use the parameters above. A removal and an empty plan carry no
            keywords at all, so ``extra`` has no effect in those branches.

    Returns:
        Zero or more operations, in execution order. An empty tuple means
        "nothing to do", which is observably different from an update that
        changes nothing: it produces no deprecation report and no registry event.
        It is only returned when there is genuinely nothing left: an ENSURE on a
        device that already sits right still emits the update when ``extra``
        carries metadata, because that metadata is not part of the ownership
        question and must not disappear with it.

    Raises:
        ValueError: On a single-owner core, when the intent cannot be carried
            out without knowing the current ownership: DETACH always, ENSURE
            when no ``target_subentry_id`` was given.

    Caller responsibility, because this function cannot check it: ENSURE on a
    device owned by *another* config entry takes that device over, silently and
    by design, because "make sure it sits here" says nothing about where it sat
    before. Every call site today resolves its device through the entry-scoped
    :func:`resolve_device_by_identifiers` or behind
    :func:`device_belongs_to_entry`, so a foreign device never reaches it. A
    future call site that hands over a device from a registry-wide scan would
    pull it into our entry together with its entities. Resolve first, then
    ENSURE.
    """
    payload: dict[str, Any] = {"device_id": device_id, **dict(extra or {})}

    # DETACH always names a link; an omitted one means the hub link, which is
    # what ``None`` meant before this parameter learned to tell the two apart.
    detach_link: str | None = (
        None if detach_subentry_id is NOT_GIVEN else detach_subentry_id
    )

    if intent is OwnershipIntent.DETACH:
        if caps.single_owner_model:
            if current is None:
                # Deleting on a guess is not an option: on this core the removal
                # of the owning entry removes the device and all its entities.
                raise ValueError(
                    "DETACH needs the current ownership; resolve the device first"
                )
            if current.entry_id != entry_id or current.subentry_id != detach_link:
                # Core would not touch the device either: the link the caller
                # wants to drop is not the link the device sits on. No-op, and
                # explicitly not a deletion.
                return ()
            return (
                DeviceRegistryOperation(
                    "async_remove_device", {"device_id": device_id}
                ),
            )
        payload["remove_config_entry_id"] = entry_id
        payload["remove_config_subentry_id"] = detach_link
        return (DeviceRegistryOperation("async_update_device", payload),)

    if intent is OwnershipIntent.ENSURE:
        if caps.single_owner_model:
            if (
                current is not None
                and current.entry_id == entry_id
                and (
                    target_subentry_id is None
                    or current.subentry_id == target_subentry_id
                )
            ):
                if len(payload) > 1:
                    # Ownership is already right, so no ownership keyword is
                    # emitted. ``extra`` is a different matter: it carries the
                    # metadata the call site wanted written (name, identifiers,
                    # manufacturer, ``via_device_id=None``). Returning an empty
                    # plan here would silently drop it, and the call site cannot
                    # tell that apart from "nothing to do".
                    return (DeviceRegistryOperation("async_update_device", payload),)
                return ()
            if target_subentry_id is None and current is None:
                # Omitting ``new_config_subentry_id`` is not neutral: core sets
                # the subentry to None alongside the new entry (pinned against
                # real core as ``new_entry_moves_immediately`` in
                # tests/test_device_registry_single_owner_contract.py). Without
                # a target and without a known current owner this branch would
                # therefore move a device to the entry root on a guess.
                raise ValueError(
                    "ENSURE without a target subentry needs the current "
                    "ownership; resolve the device first"
                )
            # Setting the owning entry is idempotent in core (the update is only
            # recorded when the value differs), so it is set unconditionally
            # rather than depending on a ``current`` the caller may not have.
            payload["new_config_entry_id"] = entry_id
            if target_subentry_id is not None:
                payload["new_config_subentry_id"] = target_subentry_id
            return (DeviceRegistryOperation("async_update_device", payload),)
        payload["add_config_entry_id"] = entry_id
        if target_subentry_id is not None and (kwarg := caps.subentry_kwarg_for_update):
            payload[kwarg] = target_subentry_id
        return (DeviceRegistryOperation("async_update_device", payload),)

    # OwnershipIntent.MOVE
    if caps.single_owner_model:
        # Both keywords together: passing new_config_subentry_id alone makes core
        # resolve the subentry against the *old* owning entry and raise
        # HomeAssistantError when the device still belongs to another entry.
        payload["new_config_entry_id"] = entry_id
        payload["new_config_subentry_id"] = target_subentry_id
        return (DeviceRegistryOperation("async_update_device", payload),)

    # Three cases, and the third one is the reason this is not a one-liner.
    #
    # * A named link (including an explicit ``None`` for the hub link) is given
    #   up as named. Reading the current subentry instead would cancel exactly
    #   that removal whenever the device already carries the target link as
    #   well: on a pre-2026.8 core a device holds a *set* of links, so sitting
    #   in the target subentry and on the entry root at the same time is the
    #   normal case this branch exists for.
    # * Nothing named, current ownership known: the current subentry is the
    #   surplus one.
    # * Nothing named, current ownership unknown: **no removal at all**. Falling
    #   back to ``None`` here would give up the hub link, which is a different
    #   link and, when it is the only one, the device itself. This is not an
    #   edge case but the normal state on the declared minimum core: at tag
    #   ``2025.9.1`` a device entry carries neither ``config_entry_id`` nor
    #   ``config_subentry_id`` (class ``DeviceEntry``, ownership fields are
    #   ``config_entries`` and ``config_entries_subentries``), so ``current`` is
    #   ``None`` there for every caller that does not name a link. The add half
    #   alone is emitted, which is exactly what the pre-intent call sites did.
    emit_removal = True
    if detach_subentry_id is NOT_GIVEN:
        if current is None:
            emit_removal = False
            surplus_subentry_id = None
        else:
            surplus_subentry_id = current.subentry_id
    else:
        surplus_subentry_id = detach_subentry_id
    if emit_removal and surplus_subentry_id != target_subentry_id:
        # Removing the link we are about to add would cancel the move out. On a
        # pre-2026.8 core that empties the entry's subentry set, and if it was
        # the only link the device is deleted. Emit the add half alone instead.
        payload["remove_config_entry_id"] = entry_id
        payload["remove_config_subentry_id"] = surplus_subentry_id
    payload["add_config_entry_id"] = entry_id
    if kwarg := caps.subentry_kwarg_for_update:
        payload[kwarg] = target_subentry_id
    return (DeviceRegistryOperation("async_update_device", payload),)


def execute_ownership_plan(
    dev_reg: Any, operations: Collection[DeviceRegistryOperation]
) -> Any:
    """Run the operations :func:`plan_device_ownership` produced.

    This is the executor for call sites that have no coordinator instance: the
    services module, the config flow and the integration's ``__init__.py``. The coordinator itself keeps its
    own richer path (``RegistryOperations._apply_device_ownership``), which adds
    the unmigrated-keyword brake and the ``config_subentry_id`` compatibility
    shim that only its call sites need. Both paths take their keywords from the
    same planner, which is the single translation point the contract names; see
    ``AGENTS.md``, "Registry updates", and ``docs/AI_DEPRECATIONS_GUIDE.md``,
    section VI.

    The ``TypeError`` retry below is the shared version of the hand-written one
    that used to sit in ``services.py``, and its reach is **narrower** than that
    one's. The hand-written version looked at the error message alone; this one
    asks :func:`needs_legacy_kwarg_retry` with
    :attr:`DeviceRegistryCapabilities.subentry_kwarg_for_shim`, which answers
    ``add_config_subentry_id`` for the signature of the declared minimum
    ``2025.9.1`` and therefore refuses the retry there outright. What is left is
    a callable whose signature carries the bare ``config_subentry_id`` or a
    ``**kwargs``, that is doubles and cores below the declared minimum. That is
    also why nothing is lost: ``async_update_device`` already accepts
    ``remove_config_subentry_id`` at tag ``2025.9.1`` (line 1035), so no
    supported core reaches the branch at all.

    Args:
        dev_reg: The device registry.
        operations: The plan, in execution order.

    Returns:
        The device entry the last update returned, or ``None`` after a removal.
        An empty plan returns ``None``; callers that need to tell "nothing to do"
        from "removed" ask the plan, not this return value.

    Raises:
        Whatever the registry raises. Call sites keep their own error handling,
        because their log messages differ.
    """
    result: Any = None
    for operation in operations:
        if operation.method == "async_remove_device":
            remove_call = getattr(dev_reg, "async_remove_device", None)
            if not callable(remove_call):
                raise AttributeError(
                    "device registry has no async_remove_device; cannot execute "
                    "the removal this plan requires"
                )
            remove_call(operation.kwargs["device_id"])
            result = None
            continue

        update_call = getattr(dev_reg, "async_update_device", None)
        if not callable(update_call):
            raise AttributeError(
                "device registry has no async_update_device; cannot execute "
                "the update this plan requires"
            )
        kwargs = dict(operation.kwargs)
        try:
            result = update_call(**kwargs)
        except TypeError as err:
            # ``subentry_kwarg_for_shim``, deliberately not
            # ``subentry_kwarg_for_update``: the question here is which spelling
            # the callable understands, not which one would move a device.
            caps = detect_device_registry_capabilities(update_call)
            if not needs_legacy_kwarg_retry(
                caps.subentry_kwarg_for_shim, str(err), kwargs
            ):
                raise
            _LOGGER.debug(
                "Retrying %s with legacy keyword arguments after %s",
                getattr(update_call, "__qualname__", repr(update_call)),
                err,
            )
            result = update_call(**build_legacy_device_registry_kwargs(kwargs))
    return result


# ---------------------------------------------------------------------------
# Device Lookup
# ---------------------------------------------------------------------------


def resolve_device_by_identifiers(
    dev_reg: Any, candidates: tuple[tuple[str, str], ...], *, entry_id: str
) -> Any | None:
    """Resolve a registry entry from ``candidates``, highest priority first.

    This is the single place in the integration that looks a device up by
    identifier. Call it instead of ``async_get_device``; do not build the entry
    scoping by hand at a call site.

    Priority is explicit: the caller orders ``candidates`` and the first hit
    wins. Before Core 2026.8 the call sites passed the whole set to a single
    ``async_get_device`` call, whose result depended on set iteration order and
    could resolve to a device owned by a *different* GoogleFindMy config entry
    that still carried the legacy unscoped identifier. Scoping the lookup to
    ``entry_id`` removes that cross-entry hit. That is a deliberate narrowing,
    not a regression: a device owned by another entry was never a valid answer.

    Core matrix:

    * ``2026.8.0`` and newer: ``async_get_device_by_identifier`` takes **one**
      identifier tuple plus the owning entry id and cannot be ambiguous
      (``homeassistant/helpers/device_registry.py`` at tag ``2026.9.0``, line
      1999).
    * up to ``2026.7``: neither that method nor ``async_get_devices`` exists, so
      the legacy branch below is the only one that runs on our declared minimum
      core ``2025.9.1``. Do not delete it as dead code.

    ``async_get_devices`` is deliberately not used as a middle step. It arrives
    in the very same core release (``2026.8.0``), so a registry that lacks
    ``async_get_device_by_identifier`` lacks it too and a registry that has the
    former never reaches the legacy branch. It also returns an unordered list,
    which would put the priority back into implicit code. It stays the right
    call wherever a *set* of devices is wanted, which is nowhere here.

    Core 2026.9 adds a main/child device distinction where this lookup searches
    main devices only (tag ``2026.9.0``, lines 1999-2021). This integration
    registers no child devices: it never calls ``async_get_or_create_child`` and
    never sets ``parent_device_id``. ``via_device`` is a different relation and
    does not make a device a child. Revisit this function if that changes.

    Args:
        dev_reg: The device registry, or any object with the same surface.
        candidates: ``(domain, identifier)`` tuples, highest priority first.
        entry_id: The config entry that must own the device.

    Returns:
        The matching registry entry, or ``None``.
    """
    if not candidates:
        return None

    by_identifier = getattr(dev_reg, "async_get_device_by_identifier", None)
    if callable(by_identifier):  # HA 2026.8+
        for identifier in candidates:
            # A ``TypeError`` from here propagates on purpose. Catching it and
            # retrying unscoped would silently undo the entry scoping this
            # branch exists for, and the repository already rules that modern
            # registries surface their ``TypeError`` instead of being rewritten
            # into a legacy call (``tests/AGENTS.md``, the modern registry
            # TypeError propagation reminder).
            device = by_identifier(identifier, entry_id)
            if device is not None:
                return device
        return None

    legacy = getattr(dev_reg, "async_get_device", None)  # HA <= 2026.7
    if callable(legacy):
        # A ``TypeError`` propagates here too, and that is a decision, not an
        # oversight. The call sites that used to wrap this lookup in
        # ``except TypeError: device = None`` swallowed a registry whose
        # ``async_get_device`` does not take ``identifiers`` as a keyword, and
        # turned it into "no such device". Every core from the declared minimum
        # upward takes the keyword, so the only callers that can trigger it are
        # registry doubles, where a silent ``None`` is the worse answer: it makes
        # a broken double look like an empty registry.
        # One identifier per call, in the caller's order. Handing the whole
        # candidate set to ``async_get_device`` would leave two things to the
        # registry's set iteration order (``get_entry`` returns the first
        # identifier that matches, tag ``2025.9.1``, lines 684-686): which of
        # two *own* devices wins, and whether a *foreign* device on the
        # low-priority identifier shadows the own device on the high-priority
        # one. The second case turned into a false miss: the foreign hit was
        # filtered out below, and the own device was never looked up.
        for identifier in candidates:
            device = legacy(identifiers={identifier})
            if device is None:
                continue
            # The entry scoping has to be applied here too, and it was not
            # until AP-16.  ``async_get_device`` searches by identifier alone,
            # so on the declared minimum core it can hand back a device of a
            # *different* GoogleFindMy entry that still carries the legacy
            # unscoped identifier -- exactly the cross-entry hit the docstring
            # above says this function removes.  Applying it in the modern
            # branch only made the promise true on 2026.8+ and false on
            # 2025.9.1, which is where the legacy branch is the only one that
            # runs.  A device this entry does not own is not an answer, so it
            # is skipped like a miss, not a hit to be filtered by the caller:
            # every call site would otherwise have to rebuild the scoping by
            # hand, which the contract forbids
            # (``agents/runtime_patterns/AGENTS.md``).
            if device_belongs_to_entry(device, entry_id):
                return device
        return None
    return None


# ---------------------------------------------------------------------------
# Device Identifier Parsing
# ---------------------------------------------------------------------------


def device_belongs_to_entry(device: Any, entry_id: str | None) -> bool:
    """Return True when ``device`` is owned by ``entry_id``.

    Two core generations, one question. From Core 2026.8 a device entry names
    its single owner in ``config_entry_id``; before that it carried a set of
    owning entries in ``config_entries``, which does not exist there.

    ``config_entries`` is **not** gone on the newer core: it survives as a
    deprecated compatibility shim (a property, checked at tag ``2026.8.0``,
    lines 454-463). Reading it therefore still works and is still wrong, for two
    reasons that have nothing to do with availability. It is the deprecated
    spelling this migration exists to remove, and on a device split from a
    pre-migration composite it answers from ``_composite_subentries`` and can
    name **several** entries where ``config_entry_id`` names one. Preferring
    ``config_entry_id`` is the same deliberate narrowing that
    :func:`resolve_device_by_identifiers` applies: an entry we do not own was
    never a valid answer.

    An unknown state answers ``False``: a device entry that describes no
    ownership at all is not evidence of ownership. An **empty**
    ``config_entry_id`` is such a state and not a fourth answer: it names no
    entry, so the shim is asked instead. :func:`device_owning_entry_ids` reads it
    the same way, and the pair is documented as behaving alike in
    ``agents/typing_guidance/AGENTS.md``.

    Args:
        device: A device registry entry, or anything with the same surface.
        entry_id: The config entry to test for.

    Returns:
        True when the device belongs to that entry.
    """
    if device is None or not entry_id:
        return False

    owner = getattr(device, "config_entry_id", None)
    if isinstance(owner, str) and owner:
        return owner == entry_id

    legacy_owners = getattr(device, "config_entries", None)
    if isinstance(legacy_owners, Iterable) and not isinstance(
        legacy_owners, (str, bytes)
    ):
        return any(candidate == entry_id for candidate in legacy_owners)
    return False


def device_owning_entry_ids(device: Any) -> tuple[str, ...]:
    """Return the config entries that own ``device``, best spelling first.

    The set-returning counterpart to :func:`device_belongs_to_entry`, for the
    call sites that need the owners themselves rather than a yes/no answer.

    From Core 2026.8 a device has exactly one owning entry, named in
    ``config_entry_id``; the tuple then holds one element. Below that core the
    owners live in ``config_entries``, and the tuple holds as many as the device
    carries. Reading ``config_entries`` on the newer core still works -- it
    survives as a deprecated shim -- and is still wrong for the two reasons
    :func:`device_belongs_to_entry` spells out: it is the spelling this
    migration removes, and on a device split from a pre-migration composite it
    can name several entries where ``config_entry_id`` names one.

    Order is the registry's, not ours. On the newer core there is nothing to
    order; below it ``config_entries`` is a ``set``, so a caller that picks "the
    first" is picking an arbitrary element and has to say why that is allowed.

    Args:
        device: A device registry entry, or anything with the same surface.

    Returns:
        The owning config entry ids. Empty when the device describes no
        ownership at all, which is not the same statement as "owned by nobody"
        but is the only answer this function can give.
    """
    if device is None:
        return ()

    owner = getattr(device, "config_entry_id", None)
    if isinstance(owner, str) and owner:
        return (owner,)

    legacy_owners = getattr(device, "config_entries", None)
    if isinstance(legacy_owners, Iterable) and not isinstance(
        legacy_owners, (str, bytes)
    ):
        return tuple(
            candidate for candidate in legacy_owners if isinstance(candidate, str)
        )
    return ()


def iter_all_devices(dev_reg: Any) -> tuple[Any, ...]:
    """Return every device entry in ``dev_reg``, whole registry, no filter.

    Use this only where the caller genuinely needs *all* devices: a collision
    check across entries, or a one-time normalisation pass. Whenever the answer
    is "the devices of this config entry", call
    ``dr.async_entries_for_config_entry(dev_reg, entry_id)`` instead; it is not
    deprecated on any supported core and says what it means.

    Why this wrapper exists rather than a bare ``dev_reg.devices.values()`` at
    the call site: from Core 2026.9 the ``devices`` attribute is a view whose
    *mapping* surface is deprecated (``breaks_in_ha_version="2027.9.0"``).
    ``__getitem__``, ``values()``, ``get()`` and ``keys()`` each report; plain
    iteration via ``__iter__`` does not, and it yields the ``DeviceEntry``
    objects directly (checked at tag ``2026.9.0``, lines 1550-1553 and 1560).

    That makes ``list(dev_reg.devices)`` right on the newer core and *wrong* on
    the declared minimum ``2025.9.1``, where ``devices`` is an ordinary mapping
    whose iteration yields device **ids**.  One expression, two meanings: that
    asymmetry is why this lives in one place instead of at every call site, and
    why the ``Mapping`` branch below is not defensive padding but the correct
    answer on the minimum core.

    Args:
        dev_reg: The device registry, or any object with the same surface.

    Returns:
        The device entries, in the registry's own order. Empty when the registry
        exposes no ``devices`` attribute at all.
    """
    devices = getattr(dev_reg, "devices", None)
    if devices is None:
        return ()

    # ``2025.9.1``: a plain mapping, whose ``__iter__`` yields keys.
    # ``2026.9``: a view, whose ``__iter__`` yields the entries themselves.
    if isinstance(devices, Mapping):
        return tuple(devices.values())

    try:
        return tuple(devices)
    except TypeError:
        return ()


def read_device_ownership(device: Any) -> DeviceOwnership | None:
    """Read who owns ``device`` right now, or ``None`` when that is unknown.

    Unknown ownership and "owned by nobody" are two different statements, and on
    a single-owner core the difference decides between a refusal and a deletion.
    A device entry that carries neither ownership attribute answers neither
    question, so it yields ``None`` rather than an empty
    :class:`DeviceOwnership`. That is the normal state on the declared minimum
    core: at tag ``2025.9.1`` a device entry carries neither ``config_entry_id``
    nor ``config_subentry_id`` (its ownership fields are ``config_entries`` and
    ``config_entries_subentries``).

    The predicate is an ``or``, and the two half-known shapes are deliberately
    not symmetric, so no blanket "it changes nothing" applies. An entry carrying
    only ``config_subentry_id`` yields owner ``None``, which matches no entry,
    and a DETACH planned from it answers with an empty plan. An entry carrying
    only ``config_entry_id`` and naming the caller's entry yields subentry
    ``None``, which *is* the hub link, and a DETACH planned from it answers with
    the removal, correctly, because that is what the state says. No supported
    core produces either shape (``2025.9.1`` has neither field, ``2026.8`` has
    both); they are a half-migrated double's shapes.

    Args:
        device: A device registry entry, or anything with the same surface, or
            ``None`` when the caller did not resolve one.

    Returns:
        The ownership the entry describes, or ``None`` when it describes none.
    """
    if device is None:
        return None
    if not (
        hasattr(device, "config_entry_id") or hasattr(device, "config_subentry_id")
    ):
        return None
    return DeviceOwnership(
        getattr(device, "config_entry_id", None),
        getattr(device, "config_subentry_id", None),
    )


def parse_device_identifier(
    identifier: Any,
    domain: str,
    entry_id: str | None,
    service_prefix: str,
    legacy_service_id: str,
) -> str | None:
    """Parse a device identifier tuple and extract the device ID.

    Multi-account compatibility:
    - Since 2025.5+ we use **entry-scoped device identifiers** in the Device Registry
      to guarantee global uniqueness across multiple accounts:
          (DOMAIN, f"{entry_id}:{device_id}")
    - For backward compatibility we also recognize legacy identifiers:
          (DOMAIN, device_id)

    Args:
        identifier: A (domain, identifier) tuple or list from device registry.
        domain: The integration domain to match (e.g., "googlefindmy").
        entry_id: The current config entry ID for namespaced matching.
        service_prefix: Prefix for service device identifiers to filter out.
        legacy_service_id: Legacy service device identifier to filter out.

    Returns:
        The canonical device_id if the identifier belongs to this entry.
        None if the identifier doesn't match, is malformed, or is a service device.
    """
    # Robust check: strict unpacking causes crashes with 3-tuple identifiers
    if not isinstance(identifier, (tuple, list)) or len(identifier) != 2:
        return None

    ident_domain, ident = identifier

    # Must be our domain with a non-empty string identifier
    if ident_domain != domain or not isinstance(ident, str) or not ident:
        return None

    # Handle namespaced format "<entry_id>:<device_id>"
    if ":" in ident:
        if entry_id and ident.startswith(entry_id + ":"):
            return ident.split(":", 1)[1]  # return canonical device_id
        # Identifier belongs to a different entry; ignore.
        return None

    # Skip service device identifiers
    if ident.startswith(service_prefix) or ident == legacy_service_id:
        return None

    # Legacy format -> accept as-is
    return ident


# ---------------------------------------------------------------------------
# Phase 8: Device Name Normalization
# ---------------------------------------------------------------------------


def normalize_device_name(name: Any) -> str | None:
    """Normalize device name to lowercase for comparison.

    Strips whitespace and converts to lowercase (casefold).

    Args:
        name: Device name (any type).

    Returns:
        Normalized lowercase string, or None if invalid/empty.
    """
    if not isinstance(name, str):
        return None
    stripped = name.strip()
    if not stripped:
        return None
    return stripped.casefold()


# ---------------------------------------------------------------------------
# Phase 8: Subentry Links Extraction
# ---------------------------------------------------------------------------


def extract_subentry_links(device: Any, entry_id: str | None) -> set[str | None]:
    """Return the subentry links ``device`` has under ``entry_id``.

    ``None`` in the result is the hub link: the device belongs to the entry
    itself rather than to one of its subentries. An empty set means the device
    has no link to ``entry_id`` at all.

    Read order, newest spelling first, so the call site does not branch on the
    core version:

    1. From Core 2026.8 a device has exactly one owner, in the scalar fields
       ``config_entry_id`` and ``config_subentry_id``. When ``config_entry_id``
       is a non-empty string it is the whole answer. For the owning entry that
       is ``{config_subentry_id}`` when it names a subentry, the hub link
       ``{None}`` when it is ``None`` or empty, and an empty set for any other
       type; for any other entry it is an empty set.
    2. Below 2026.8 the scalar fields do not exist and the many-to-many
       ``config_entries_subentries`` mapping is the real data. It is read only
       when step 1 found no owner, which is also the case for registry doubles
       that model the mapping alone.
    3. A device that names neither carries at most a bare ``config_subentry_id``
       or legacy ownership; :func:`device_belongs_to_entry` answers the latter.

    Why the order matters: ``config_entries_subentries`` (and
    ``config_entries``) survive on 2026.8+ as compatibility shims, and from Core
    2026.10 every read of them calls ``report_usage`` with
    ``breaks_in_ha_version="2027.10.0"`` (``homeassistant/helpers/
    device_registry.py`` at tag ``2026.10.0``,
    ``_report_deprecated_config_entries_property``). For a custom integration
    that is a report on every call, logged once per call site; from core code,
    which is what the test suite counts as, it is a ``RuntimeError``. Reading the scalar fields first
    keeps both shims out of the path on every core that has them.

    One deliberate narrowing, the same one :func:`device_belongs_to_entry`
    states: a restored composite device (synthesized by ``async_get`` for a
    pre-migration composite id) reports several entries through the shim but
    one through ``config_entry_id``. The callers of this function take their
    devices from ``async_entries_for_config_entry`` or an identifier lookup,
    which return stored devices only, never the synthesized composite.

    Args:
        device: Device registry entry object.
        entry_id: The config entry ID to look up.

    Returns:
        Set of subentry link strings (may include None for hub links).
    """
    if device is None or not entry_id:
        return set()

    subentry = getattr(device, "config_subentry_id", None)
    owner = getattr(device, "config_entry_id", None)
    if isinstance(owner, str) and owner:
        if owner != entry_id:
            return set()
        if isinstance(subentry, str) and subentry:
            return {subentry}
        if subentry is None or subentry == "":
            return {None}
        return set()

    mapping_obj = getattr(device, "config_entries_subentries", None)
    if isinstance(mapping_obj, Mapping):
        raw_links = mapping_obj.get(entry_id)
        if isinstance(raw_links, str):
            # A lone string is one link, per ``agents/typing_guidance/AGENTS.md``.
            return {raw_links}
        if isinstance(raw_links, Collection) and not isinstance(
            raw_links, (bytes, Mapping)
        ):
            typed_links: set[str | None] = set()
            for item in raw_links:
                if item is None:
                    typed_links.add(None)
                elif isinstance(item, str):
                    typed_links.add(item)
            return typed_links
        if raw_links is None:
            return set()

    if isinstance(subentry, str):
        return {subentry}

    if subentry is None and device_belongs_to_entry(device, entry_id):
        return {None}

    return set()


# ---------------------------------------------------------------------------
# Stale devices: tracker devices that left the Google account
# ---------------------------------------------------------------------------

#: Consecutive fresh device lists a tracker device must be missing from before
#: it is removed. One miss is not enough: a single truncated fetch can drop a
#: tracker as a ``benign`` canonicless device without any other trace.
STALE_REMOVAL_REQUIRED_MISSES: Final = 2

#: Removal attempts a device gets before it is left alone until the next
#: setup: the first one and one retry. ``_apply_device_ownership`` logs a
#: DETACH it refuses as an error, so an unbounded retry could add one error
#: per device list.
STALE_REMOVAL_MAX_ATTEMPTS: Final = 2

#: The tail the registry sync adds to a tracker name that collides with a hub
#: name: `` (<last 6 characters of the subentry, device or entry id>)``.
_HUB_COLLISION_SUFFIX: Final = re.compile(r" \([^()]{1,6}\)$")


@dataclass(frozen=True)
class StaleDevicePlan:
    """Outcome of :func:`plan_stale_device_removals`.

    Attributes:
        remove: ``(device, google_ids)`` pairs to remove now.
        first_misses: ``(device, misses)`` pairs missing from this list but not
            yet often enough to be removed.
        misses: The miss counter to keep for the next fresh list, keyed by
            device registry id. Devices that are listed again drop out of it.
        skipped_reason: Set when this list must not count at all; the counter
            is then returned unchanged.
        name_baseline: The drop counts to keep for the next fresh list, keyed
            by Google id: for every id of a counting list, the canonicless drop
            counts of that list per normalized name and kind (S-R3d).
    """

    remove: tuple[tuple[Any, frozenset[str]], ...] = ()
    first_misses: tuple[tuple[Any, int], ...] = ()
    misses: dict[str, int] = field(default_factory=dict)
    skipped_reason: str | None = None
    name_baseline: dict[str, Mapping[tuple[str, str], int]] = field(
        default_factory=dict
    )


def _normalized_name_counts(
    raw: Mapping[tuple[str, str], int],
) -> dict[tuple[str, str], int]:
    """Fold raw ``(name, kind)`` drops into ``normalize_device_name`` keys.

    The kind stays part of the key: two devices of one name but different
    kinds are two counts (R3-5).
    """
    counts: dict[tuple[str, str], int] = {}
    for item, count in raw.items():
        if not isinstance(item, tuple) or len(item) != 2:
            continue
        name, kind = item
        key = normalize_device_name(name)
        if key is None or not isinstance(kind, str) or not kind:
            continue
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            continue
        counts[(key, kind)] = counts.get((key, kind), 0) + count
    return counts


def _registry_name_keys(device: Any) -> frozenset[str]:
    """Return the normalized names a canonicless drop of ``device`` can carry.

    The registry ``name`` follows the Google name (the sync keeps it current
    also under a user name, as core does), stripped of edge spaces. The hub
    collision suffix is the one change the sync makes to it, so the name
    without it counts as well.
    """
    name = getattr(device, "name", None)
    if not isinstance(name, str):
        return frozenset()
    keys = {
        normalize_device_name(name),
        normalize_device_name(_HUB_COLLISION_SUFFIX.sub("", name)),
    }
    return frozenset(key for key in keys if key is not None)


def _is_still_listed_by_name(
    device: Any,
    google_ids: frozenset[str],
    dropped: Mapping[tuple[str, str], int],
    name_baseline: Mapping[str, Mapping[tuple[str, str], int]],
) -> bool:
    """S-R3d: tell whether a canonicless drop stands for ``device``.

    A drop under the device's name stands for it when that name has more
    drops of one kind now than in the last counting list that still carried
    the device with its id; a drop that was already there belongs to another
    device of the same name (R2-6). Counting per kind keeps a role swap
    visible: when a phone of the tracker's name gets its id in the same
    listing in which the tracker loses its own, the drop moves from
    ``android`` to another kind and is new (R3-5). Without such a list
    (after a restart) the name alone decides, which keeps a device that may
    still be in the account.
    """
    names = _registry_name_keys(device)
    for (key, kind), now in dropped.items():
        if key not in names or now <= 0:
            continue
        before = [
            name_baseline[google_id].get((key, kind), 0)
            for google_id in google_ids
            if google_id in name_baseline
        ]
        if not before or now > min(before):
            return True
    return False


def _device_google_ids(device: Any, domain: str) -> frozenset[str]:
    """Return every Google id one device carries.

    The id is the last ``:`` segment of each identifier of ``domain``: the
    namespaced ``<entry>:<id>``, the subentry form ``<entry>:<subentry>:<id>``
    and the legacy bare ``<id>`` all end in it. A device merged from two
    trackers of the same name carries two ids; all of them are returned, so a
    caller never depends on the iteration order of the identifier set.
    """
    ids: set[str] = set()
    for identifier in getattr(device, "identifiers", None) or ():
        if not isinstance(identifier, (tuple, list)) or len(identifier) != 2:
            continue
        ident_domain, ident_value = identifier
        if ident_domain != domain or not isinstance(ident_value, str):
            continue
        google_id = ident_value.rsplit(":", 1)[-1].strip()
        if google_id:
            ids.add(google_id)
    return frozenset(ids)


def plan_stale_device_removals(  # noqa: PLR0913 - one parameter per protection rule
    devices: Iterable[Any],
    *,
    entry_id: str | None,
    domain: str,
    service_identifiers: Collection[tuple[str, str]],
    fresh_list_ids: Collection[str] | None,
    dropped_candidates: int,
    canonicless_warn: int,
    canonicless_names: Mapping[tuple[str, str], int],
    previous_misses: Mapping[str, int],
    name_baseline: Mapping[str, Mapping[tuple[str, str], int]] | None = None,
    required_misses: int = STALE_REMOVAL_REQUIRED_MISSES,
) -> StaleDevicePlan:
    """Decide which tracker devices left the Google account.

    Pure: no ``hass``, no registry access, no I/O. The caller passes the
    devices of its config entry and the device list it fetched in this cycle,
    and executes the returned plan. Integration Quality Scale rule
    ``stale-devices``: when the service delivers a complete device list, the
    integration removes devices missing from it itself.

    A list only counts when it is fresh, not empty and complete:

    * ``fresh_list_ids`` is ``None`` or empty (cached, deferred or accepted
      empty list): nothing is planned (S-R1, S-R2).
    * ``dropped_candidates > 0``: the fetch held entries without a valid id,
      so the list is incomplete (S-R3a).
    * ``canonicless_warn > 0``: the decoder dropped a tracker-like device
      without canonical id (S-R3b). ``benign`` drops do not block; live they
      occur on every fetch (S-R3c).

    Per device:

    * the service device is never a candidate (S-R4);
    * only devices with a link to ``entry_id`` are candidates (S-R5);
    * a device without a readable Google id is never removed, and one is
      removed only when *none* of its ids is listed (S-R8);
    * a device whose name the decoder dropped from this fetch for lack of a
      canonical id is still in the account and counts as listed (S-R3d). The
      class tally behind S-R3b misses it: a phone or an accessory without an
      ``information`` block drops as ``benign`` even when it was a tracker
      device before, and the decoder flags that transition for one fetch only.
      Names compare normalized and without the hub collision suffix, and a
      drop counts for the device only when its name has more drops of one
      kind than in the last counting list that carried the device (R2-1,
      R2-6, R3-5). Two devices of one name and one kind that swap roles in
      one listing stay indistinguishable; different names in the Find Hub
      app tell them apart;
    * it must be missing from ``required_misses`` consecutive counting lists
      (S-R9).

    Args:
        devices: Device registry entries of this config entry.
        entry_id: The config entry the devices belong to.
        domain: The integration domain of the identifiers.
        service_identifiers: Identifiers that mark the service device.
        fresh_list_ids: Google ids of the list fetched in this cycle.
        dropped_candidates: List entries without a valid id in this fetch.
        canonicless_warn: The decoder's ``warn`` drop count for this fetch.
        canonicless_names: ``(raw name, kind)`` of the devices the decoder
            dropped from this fetch for lack of a canonical id, whatever
            their class, with the number of drops under each.
        previous_misses: The counter returned by the previous call.
        name_baseline: The ``name_baseline`` returned by the previous call.
        required_misses: Consecutive misses that remove a device.

    Returns:
        The plan; see :class:`StaleDevicePlan`.
    """
    baseline = dict(name_baseline or {})
    skipped_reason: str | None = None
    if not entry_id or not fresh_list_ids:
        skipped_reason = "no_fresh_list"
    elif dropped_candidates > 0:
        skipped_reason = "dropped_candidates"
    elif canonicless_warn > 0:
        skipped_reason = "canonicless_warn"
    if skipped_reason is not None:
        return StaleDevicePlan(
            misses=dict(previous_misses),
            skipped_reason=skipped_reason,
            name_baseline=baseline,
        )

    listed = frozenset(fresh_list_ids or ())
    dropped = _normalized_name_counts(canonicless_names)
    service = frozenset(service_identifiers)
    remove: list[tuple[Any, frozenset[str]]] = []
    first_misses: list[tuple[Any, int]] = []
    misses: dict[str, int] = {}
    for device in devices:
        device_id = getattr(device, "id", None)
        if not isinstance(device_id, str) or not device_id:
            continue
        if getattr(device, "entry_type", None) is not None or service.intersection(
            getattr(device, "identifiers", None) or ()
        ):
            continue
        if not extract_subentry_links(device, entry_id):
            continue
        google_ids = _device_google_ids(device, domain)
        if not google_ids or google_ids & listed:
            continue
        if _is_still_listed_by_name(device, google_ids, dropped, baseline):
            continue
        count = previous_misses.get(device_id, 0) + 1
        if count >= required_misses:
            remove.append((device, google_ids))
        else:
            misses[device_id] = count
            first_misses.append((device, count))
    next_baseline = {
        google_id: counts
        for google_id, counts in baseline.items()
        if not any(google_id in ids for _device, ids in remove)
    }
    for google_id in listed:
        next_baseline[google_id] = dropped
    return StaleDevicePlan(
        remove=tuple(remove),
        first_misses=tuple(first_misses),
        misses=misses,
        name_baseline=next_baseline,
    )


# ---------------------------------------------------------------------------
# Phase 8: Subentry Link Checks
# ---------------------------------------------------------------------------


def has_subentry_link(links: set[str | None], target_id: str | None) -> bool:
    """Check if target subentry ID is in links set.

    Args:
        links: Set of subentry links (may include None).
        target_id: Target subentry ID to check.

    Returns:
        True if target_id is in links and target_id is not None.
    """
    if target_id is None:
        return False
    return target_id in links


def has_hub_link(links: set[str | None]) -> bool:
    """Check if device has a hub link (None in links set).

    Hub links are represented by None in the subentry links set,
    indicating the device is linked to the config entry without
    a specific subentry.

    Args:
        links: Set of subentry links.

    Returns:
        True if None is in links.
    """
    return None in links


# ---------------------------------------------------------------------------
# Phase 8: Hub Device Detection
# ---------------------------------------------------------------------------


def is_hub_device_check(
    device_id: str | None,
    hub_device_id: str | None,
    identifiers: Any,
    parent_identifier: tuple[str, str],
) -> bool:
    """Check if a device is the hub/service anchor device.

    A device is considered the hub if:
    1. Its ID matches the hub_device_id, OR
    2. Its identifiers contain the parent_identifier

    Args:
        device_id: The device's ID.
        hub_device_id: The known hub device ID.
        identifiers: The device's identifiers (set of tuples).
        parent_identifier: The parent/service device identifier tuple.

    Returns:
        True if the device is the hub/service device.
    """
    # Check by device ID match
    if (
        hub_device_id is not None
        and device_id is not None
        and device_id == hub_device_id
    ):
        return True

    # Check by identifier match
    if isinstance(identifiers, Collection) and not isinstance(
        identifiers, (str, bytes, Mapping)
    ):
        return parent_identifier in identifiers

    return False


# ---------------------------------------------------------------------------
# Phase 8: Tracker Subentry Resolution
# ---------------------------------------------------------------------------


def resolve_tracker_subentry_candidate(
    candidate: str | None,
    entry_tracker_id: str | None,
    tracker_subentry_ids: set[str],
) -> str | None:
    """Resolve a tracker subentry candidate to a valid subentry ID.

    Resolution rules:
    1. If candidate is None, return None
    2. If entry_tracker_id is set:
       - candidate must equal entry_tracker_id
       - if tracker_subentry_ids is non-empty, candidate must be in it
    3. If entry_tracker_id is None:
       - if tracker_subentry_ids is non-empty, candidate must be in it
       - if tracker_subentry_ids is empty, accept candidate as-is

    Args:
        candidate: The candidate subentry ID to validate.
        entry_tracker_id: The entry's tracker subentry ID (if set).
        tracker_subentry_ids: Set of valid tracker subentry IDs.

    Returns:
        The validated subentry ID, or None if validation fails.
    """
    if candidate is None:
        return None

    if entry_tracker_id is not None:
        # Must match entry_tracker_id
        if candidate != entry_tracker_id:
            return None
        # If tracker_subentry_ids exists, must be in it
        if tracker_subentry_ids and candidate not in tracker_subentry_ids:
            return None
        return candidate

    # No entry_tracker_id set
    if tracker_subentry_ids:
        # Must be in tracker_subentry_ids
        if candidate in tracker_subentry_ids:
            return candidate
        return None

    # No restrictions - accept as-is
    return candidate


# ---------------------------------------------------------------------------
# Phase 9: Service Device Helper Functions
# ---------------------------------------------------------------------------


def extract_service_subentry_ids(
    entry_subentries: Any,
    entry_service_subentry_id: str | None,
    subentry_type_service: str,
    service_subentry_key: str,
) -> set[str]:
    """Extract service subentry IDs from entry.subentries mapping.

    Identifies subentries that are service-related by checking:
    1. subentry_type == subentry_type_service
    2. data.group_key == service_subentry_key

    Provisional subentries (ending in '-provisional') are skipped unless
    they match entry_service_subentry_id.

    Args:
        entry_subentries: The entry.subentries mapping.
        entry_service_subentry_id: The entry's service subentry ID (if set).
        subentry_type_service: The subentry type constant for service.
        service_subentry_key: The group_key constant for service.

    Returns:
        Set of service subentry IDs.
    """
    if not isinstance(entry_subentries, Mapping):
        return set()

    result: set[str] = set()
    for subentry_id, subentry in entry_subentries.items():
        # Skip invalid subentry IDs
        if not isinstance(subentry_id, str) or not subentry_id:
            continue

        # Skip provisional unless it matches entry_service_subentry_id
        if (
            subentry_id.endswith("-provisional")
            and subentry_id != entry_service_subentry_id
        ):
            continue

        # Check subentry_type
        subentry_type = getattr(subentry, "subentry_type", None)

        # Check group_key in data
        group_key: Any = None
        data_obj = getattr(subentry, "data", None)
        if isinstance(data_obj, Mapping):
            group_key = data_obj.get("group_key")

        # Include if it's a service subentry
        if subentry_type == subentry_type_service or (
            isinstance(group_key, str) and group_key == service_subentry_key
        ):
            result.add(subentry_id)

    return result


def should_defer_service_subentry(
    service_subentry_id: str | None,
    current_subentries: Any,
    entry_id: str | None,
    service_subentry_key: str,
) -> bool:
    """Check if config_subentry_id should be deferred until registry catches up.

    Returns True if the subentry_id is not in current_subentries and is not
    the stable default pattern.

    Args:
        service_subentry_id: The service config_subentry_id to check.
        current_subentries: The current entry.subentries mapping.
        entry_id: The config entry ID.
        service_subentry_key: The group_key constant for service.

    Returns:
        True if the subentry_id should be deferred.
    """
    if service_subentry_id is None:
        return False

    if not isinstance(current_subentries, Mapping):
        return False

    # Check if subentry is in registry
    if service_subentry_id in current_subentries:
        return False

    # Check for stable default pattern
    if isinstance(entry_id, str) and entry_id:
        stable_default = f"{entry_id}-{service_subentry_key}-subentry"
        if service_subentry_id == stable_default:
            return False

    # Defer if not found
    return True


# ---------------------------------------------------------------------------
# Phase 12: Entity Lookup Helper Functions
# ---------------------------------------------------------------------------


def extract_canonical_device_id(
    identifiers: Collection[Any] | None,
    domain: str,
    entry_id: str | None = None,
    service_prefix: str = "",
    legacy_service_id: str = "",
) -> str | None:
    """Extract canonical device ID from device registry identifiers.

    Scans identifiers looking for a device ID matching the domain.
    Supports both simple and namespaced (entry_id:device_id) formats.

    Args:
        identifiers: Set of identifier tuples from device registry.
        domain: The integration domain to match.
        entry_id: Optional entry_id for namespaced identifier matching.
        service_prefix: Prefix for service device identifiers to skip.
        legacy_service_id: Legacy service device identifier to skip.

    Returns:
        The canonical device ID, or None if not found.
    """
    if identifiers is None:
        return None

    if not isinstance(identifiers, Collection) or isinstance(
        identifiers, (str, bytes, Mapping)
    ):
        return None

    namespaced_result: str | None = None
    simple_result: str | None = None

    for identifier in identifiers:
        # Must be 2-tuple
        if not isinstance(identifier, (tuple, list)) or len(identifier) != 2:
            continue

        ident_domain, ident_value = identifier

        # Must match domain
        if ident_domain != domain:
            continue

        # Must be non-empty string
        if not isinstance(ident_value, str) or not ident_value:
            continue

        # Skip service device identifiers
        if service_prefix and ident_value.startswith(service_prefix):
            continue
        if legacy_service_id and ident_value == legacy_service_id:
            continue

        # Handle namespaced format
        if ":" in ident_value:
            if entry_id and ident_value.startswith(entry_id + ":"):
                namespaced_result = ident_value.split(":", 1)[1]
            # If entry_id doesn't match, skip this identifier
            continue

        # Simple format
        simple_result = ident_value

    # Prefer namespaced result when entry_id is provided
    if namespaced_result is not None:
        return namespaced_result

    return simple_result


def build_entity_unique_id_candidates(
    device_id: str,
    entry_id: str | None,
    subentry_identifier: str | None,
    domain: str,
    subentry_key: str | None = None,
) -> list[str]:
    """Generate list of unique_id candidates for entity lookup.

    Produces various unique_id formats in priority order for finding
    an entity that may have been created with different ID schemes.

    Args:
        device_id: The device ID.
        entry_id: The config entry ID.
        subentry_identifier: The stable subentry identifier.
        domain: The integration domain.
        subentry_key: Optional subentry key if different from identifier.

    Returns:
        List of unique_id candidates in priority order (canonical first).
    """
    candidates: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        if candidate and candidate not in seen:
            candidates.append(candidate)
            seen.add(candidate)

    # Build canonical format first (highest priority)
    if entry_id and subentry_identifier and device_id:
        add(f"{entry_id}:{subentry_identifier}:{device_id}")

    # Subentry key variant (if different from identifier)
    if entry_id and subentry_key and subentry_key != subentry_identifier and device_id:
        add(f"{entry_id}:{subentry_key}:{device_id}")

    # Entry:device format
    if entry_id and device_id:
        add(f"{entry_id}:{device_id}")

    # Domain_entry_device format
    if entry_id and device_id:
        add(f"{domain}_{entry_id}_{device_id}")

    # Legacy domain_device format
    if device_id:
        add(f"{domain}_{device_id}")

    return candidates


def build_canonical_unique_id(
    entry_id: str | None,
    subentry_identifier: str | None,
    device_id: str | None,
) -> str | None:
    """Build canonical unique_id from components.

    Format: entry_id:subentry_identifier:device_id
    Skips empty components (except device_id which is required).

    Args:
        entry_id: The config entry ID.
        subentry_identifier: The stable subentry identifier.
        device_id: The device ID.

    Returns:
        Canonical unique_id string, or None if required parts are missing.
    """
    # Entry_id is required
    if not entry_id or not isinstance(entry_id, str):
        return None
    entry_id = entry_id.strip()
    if not entry_id:
        return None

    # Device_id is required
    if not device_id or not isinstance(device_id, str):
        return None
    device_id = device_id.strip()
    if not device_id:
        return None

    # Subentry_identifier is optional
    parts: list[str] = [entry_id]

    if subentry_identifier and isinstance(subentry_identifier, str):
        stripped = subentry_identifier.strip()
        if stripped:
            parts.append(stripped)

    parts.append(device_id)

    return ":".join(parts)


def match_entity_by_device_id(  # noqa: PLR0917
    unique_id: Any,
    config_entry_id: str | None,
    device_id: str,
    target_entry_id: str | None,
    domain: str,
    platform: str,
    entity_domain: str,
    entity_platform: str,
) -> bool:
    """Check if an entity registry entry matches device criteria.

    Used for fallback entity lookup when direct unique_id match fails.

    Args:
        unique_id: The entity's unique_id.
        config_entry_id: The entity's config_entry_id.
        device_id: The device ID to match (must be contained in unique_id).
        target_entry_id: The target entry ID to filter by (or None for no filter).
        domain: The expected domain (e.g., "device_tracker").
        platform: The expected platform (e.g., "googlefindmy").
        entity_domain: The entity's actual domain.
        entity_platform: The entity's actual platform.

    Returns:
        True if the entity matches all criteria.
    """
    # Validate required inputs
    if not device_id or not isinstance(device_id, str):
        return False

    # Check domain and platform match
    if entity_domain != domain or entity_platform != platform:
        return False

    # Check config_entry_id if target is specified
    if target_entry_id is not None and config_entry_id is not None:
        if config_entry_id != target_entry_id:
            return False

    # Check unique_id contains device_id
    if not isinstance(unique_id, str):
        return False

    return device_id in unique_id
