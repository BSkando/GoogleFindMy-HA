# tests/test_options_flow_subentries.py

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType, SimpleNamespace
from typing import Any

import pytest
from homeassistant.config_entries import ConfigEntryState, ConfigSubentry
from homeassistant.helpers import frame

from custom_components.googlefindmy import config_flow
from custom_components.googlefindmy.const import (
    OPT_CONTRIBUTOR_MODE,
    OPT_IGNORED_DEVICES,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_HUB,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_SUBENTRY_KEY,
)
from tests.helpers.config_flow import prepare_flow_hass_config_entries

pytestmark = pytest.mark.asyncio


def _stable_subentry_id(entry_id: str, key: str) -> str:
    """Return deterministic config_subentry identifiers for options tests."""

    return f"{entry_id}-{key}-subentry"


@dataclass
class _ManagerStub:
    """Minimal config_entries manager capturing subentry operations."""

    entry: _EntryStub

    def __post_init__(self) -> None:
        self.updated: list[tuple[str, dict[str, Any]]] = []
        self.removed: list[str] = []
        self.reloads: list[str] = []
        self.scheduled_reloads: list[str] = []
        self.setup_calls: list[str] = []

    def async_update_entry(self, entry: _EntryStub, *, data: dict[str, Any]) -> None:
        assert entry is self.entry
        entry.data = data

    def async_update_subentry(  # noqa: PLR0913
        self,
        entry: _EntryStub,
        subentry: ConfigSubentry,
        *,
        data: dict[str, Any],
        title: str | None = None,
        unique_id: str | None = None,
        translation_key: str | None = None,
    ) -> None:
        assert entry is self.entry
        subentry.data = MappingProxyType(dict(data))
        if title is not None:
            subentry.title = title
        if unique_id is not None:
            subentry.unique_id = unique_id
        if translation_key is not None:
            subentry.translation_key = translation_key
        self.updated.append((subentry.subentry_id, dict(subentry.data)))

    def async_remove_subentry(self, entry: _EntryStub, subentry_id: str) -> bool:  # noqa: FBT001
        assert entry is self.entry
        entry.discard_subentry(subentry_id)
        self.removed.append(subentry_id)
        return True

    def async_schedule_reload(self, entry_id: str) -> None:
        """Record the call; deliberately do not chain into ``async_reload``.

        Synchronous on purpose: ``ConfigEntries.async_schedule_reload`` is a
        ``@callback`` returning ``None``, and a coroutine here would make the
        production call site look awaitable when it is not.

        Two core behaviours are left out knowingly, so nothing here is mistaken
        for a faithful replica: the core raises ``UnknownEntry`` for an entry id
        it does not know, and it hands the reload to ``async_create_task``. This
        recorder does neither, which is what lets an assertion tell the two call
        styles apart -- but it also means the failure branch of
        ``_schedule_claimed_reload`` cannot be reached from this file.

        Latent coupling, deliberately named here: ``_core_auto_schedules_subentries``
        only consults this attribute once the manager also exposes
        ``async_setup_subentry`` **and** ``async_get_subentries``. This stub has the
        second but not the first, so the probe still answers ``False``. Adding
        ``async_setup_subentry`` here later would flip that probe silently.
        """

        self.scheduled_reloads.append(entry_id)

    async def async_reload(self, entry_id: str) -> None:
        """Kept next to :meth:`async_schedule_reload` as a regression tripwire.

        A later fall back to the awaited variant would otherwise pass silently;
        with both recorders present the assertions can tell the two apart.
        """

        self.reloads.append(entry_id)

    def async_get_entry(self, entry_id: str) -> _EntryStub | None:
        if entry_id == self.entry.entry_id:
            return self.entry
        return None

    def async_get_subentries(self, entry_id: str) -> list[ConfigSubentry]:
        entry = self.async_get_entry(entry_id)
        if entry is None:
            return []
        return list(entry.subentries.values())

    async def async_setup(self, entry_id: str) -> bool:
        self.setup_calls.append(entry_id)
        return True


class _EntryStub:
    """Config entry stub exposing subentries and mutable options."""

    def __init__(self) -> None:
        self.entry_id = "entry-test"
        self.title = "Entry Title"
        self.data: dict[str, Any] = {}
        self.options: dict[str, Any] = {}
        # Home Assistant hands ``ConfigEntry.subentries`` out as a
        # ``MappingProxyType`` and never as a ``dict``
        # (``config_entries.py``: ``_setter(self, "subentries",
        # MappingProxyType(subentries))``). A stub exposing a plain ``dict``
        # here is not a harmless simplification: it makes an
        # ``isinstance(..., dict)`` guard in production pass in the test world
        # and skip every subentry in the real one, which is how the options
        # flow's subentry selection stayed dead for nine months while this file
        # was green. The mutable store below stays reachable for the two
        # writers that mirror the core's own -- ``add_subentry`` and
        # ``discard_subentry`` -- so what the flow gets to see is the read-only
        # view and nothing else.
        self._subentry_store: dict[str, ConfigSubentry] = {}
        self.subentries: Mapping[str, ConfigSubentry] = MappingProxyType(
            self._subentry_store
        )
        self.runtime_data = SimpleNamespace(coordinator=SimpleNamespace(data=[]))
        # The three fields the reload gate in ``_schedule_claimed_reload``
        # reads. Without them every ``getattr(entry, ..., None)`` in
        # ``_entry_reload_is_hopeless`` falls back to ``None``, the gate can
        # only ever answer "not hopeless", and a test that claims to exercise
        # the gate would in fact exercise nothing (fail-open, so silently).
        # Defaults are the healthy case; a test that wants the gate to bite
        # assigns a terminal ``state`` (see the guard test at the end of this
        # file). ``ConfigEntryState`` is imported from the submodule because
        # that is the module world the production code compares against.
        self.state = ConfigEntryState.LOADED
        self.source = "user"
        self.disabled_by = None

    def add_subentry(
        self,
        *,
        key: str,
        title: str,
        visible_device_ids: list[str] | None = None,
        feature_flags: dict[str, Any] | None = None,
        subentry_type: str | None = SUBENTRY_TYPE_TRACKER,
        identity: str | None = None,
    ) -> ConfigSubentry:
        """Add a subentry to the stub.

        ``subentry_type`` is a parameter rather than a constant because the
        assignability predicate reads *both* axes: the stored ``group_key`` and
        the subentry type. A stub that could only produce ``tracker`` would let
        a key-only predicate pass every test while still missing a legacy
        subentry whose ``group_key`` drifted from its type -- the alias case
        ``agents/config_flow/AGENTS.md`` describes under
        ``Subentry alias handling``.

        ``None`` is admitted for the same reason the type is a parameter at
        all: an untyped legacy subentry is a distinct shape from every named
        type, and it is the one that separates "prefers a ``tracker``-typed
        group" from "prefers any group that accepts devices". Every production
        reader already spells the access ``getattr(subentry, "subentry_type",
        None)``, so ``None`` is the value they are written for rather than an
        invention of this stub.

        ``identity`` decouples the Home Assistant identifiers from ``key`` and
        is what makes the *collision* half of that same alias case expressible.
        Without it this helper derives ``subentry_id`` from ``key`` and stores
        the result under that id, so two calls sharing a ``group_key`` silently
        overwrite one another and leave a single subentry behind -- the stub
        would quietly refuse to build the very shape
        ``tests/test_config_flow_subentry_sync.py`` pins for the manager.
        """

        payload = {
            "group_key": key,
            "feature_flags": feature_flags or {},
        }
        if visible_device_ids is not None:
            payload["visible_device_ids"] = list(visible_device_ids)
        slug = identity or key
        subentry = ConfigSubentry(
            data=payload,
            subentry_type=subentry_type,
            title=title,
            unique_id=f"{self.entry_id}-{slug}",
            subentry_id=_stable_subentry_id(self.entry_id, slug),
            translation_key=key,
        )
        self._subentry_store[subentry.subentry_id] = subentry
        return subentry

    def discard_subentry(self, subentry_id: str) -> None:
        """Remove a subentry through the store rather than through the view.

        Kept on the entry rather than done inline in ``_ManagerStub`` because
        ``self.subentries`` is a read-only view, and a double that could delete
        straight through it would be modelling an entry Home Assistant does not
        hand out.

        Two divergences from ``ConfigEntries.async_remove_subentry``, named so
        this is not mistaken for a replica. The core raises ``UnknownSubEntry``
        for an id it does not hold while this discards silently, and the core
        rebuilds the mapping into a fresh ``MappingProxyType`` while this
        mutates the shared store in place -- so a reference to
        ``entry.subentries`` taken before a removal stays live here and would be
        a stale snapshot against the core. Both are the lenient direction; a
        test that needs either behaviour cannot use this double.
        """

        self._subentry_store.pop(subentry_id, None)


class _HassStub:
    """Home Assistant stub exposing config entry helpers to the flow."""

    def __init__(self, entry: _EntryStub) -> None:
        self._entry = entry
        # Load-bearing since the repair steps schedule through
        # ``_schedule_claimed_reload``: the tests at the end of this file read
        # and pre-set the latch through it. The
        # reload latch lives in ``hass.data``: without this mapping
        # ``_domain_data`` raises ``AttributeError`` inside
        # ``_claim_entry_reload``, which swallows it and fails open with ``True``.
        # The stand-down branch would then be unreachable and its mutation probe
        # meaningless.
        self.data: dict[str, Any] = {}
        # ``tests/AGENTS.md`` point 8 asks for this and the file did not have it:
        # ``entry_reload_gate.entry_reload_is_hopeless`` ends on
        # ``domain in hass.config.components``, and where it cannot read that
        # container it fails **closed** and calls every entry hopeless. Tests for
        # a non-terminal but not-loaded entry would then measure the terminal
        # branch instead of their own.
        self.config = SimpleNamespace(components={config_flow.DOMAIN})

    @classmethod
    async def create(cls, entry: _EntryStub) -> _HassStub:
        hass = cls(entry)
        prepare_flow_hass_config_entries(
            hass,
            lambda: _ManagerStub(entry),
            frame_module=frame,
        )
        return hass

    def async_create_task(self, coro: Any) -> asyncio.Task[Any]:
        return asyncio.create_task(coro)


async def _build_flow(entry: _EntryStub) -> config_flow.OptionsFlowHandler:
    flow = config_flow.OptionsFlowHandler()
    flow.hass = await _HassStub.create(entry)  # type: ignore[assignment]
    flow.config_entry = entry  # type: ignore[attr-defined]
    return flow


def _offered_keys_in_order(result: dict[str, Any], field: str) -> tuple[str, ...]:
    """Return the selectable keys for ``field`` in the order the form lists them.

    Reads the ``vol.In`` container out of the rendered schema instead of
    calling the production helper that built it. That is deliberate: the point
    of these tests is what the *user* can pick, so a later rename or a second
    helper must not be able to keep them green while the form changes.

    Order is kept rather than discarded because the fallback the preselection
    guards against *is* label order: a test that wants to show the form
    preselects something other than "whatever sorts first" has to be able to
    name that first entry.
    """

    schema = result["data_schema"].schema
    for marker, validator in schema.items():
        if str(marker) != field:
            continue
        container = getattr(validator, "container", None)
        if container is None:  # pragma: no cover - schema shape changed
            raise AssertionError(f"field {field!r} is not a vol.In selector")
        return tuple(container)
    raise AssertionError(f"field {field!r} not present in the shown form")


def _offered_keys(result: dict[str, Any], field: str) -> set[str]:
    """Return the selectable keys a shown form offers for ``field``.

    Set-shaped view of :func:`_offered_keys_in_order` for the callers that ask
    membership questions, so the *offered keys* are read out of the schema in
    exactly one place. :func:`_rendered_default` reads the same schema for a
    different question -- which marker carries the preselection -- and shares
    no code with it on purpose: one walks the validator, the other the key.
    """

    return set(_offered_keys_in_order(result, field))


def _rendered_default(result: dict[str, Any], field: str) -> Any:
    """Return the value a shown form preselects for ``field``.

    Sibling of :func:`_offered_keys` and written for the same reason its
    docstring gives: read the rendered schema, not the production helper that
    computed the value. The helper-level tests further down call
    ``_default_subentry_key`` directly, so they stay green even where a step
    stops passing its result into the marker -- which is precisely the
    regression a user would see, because the marker default *is* the
    preselection.

    Deliberately not narrowed to ``vol.Optional``: both fields this pins are
    ``vol.Required``. ``default`` is *not* on the shared ``vol.Marker`` base --
    measured against voluptuous 0.15.2, ``Marker.__slots__`` is
    ``('schema', '_schema', 'msg', 'description', '__hash__')`` and carries no
    ``__dict__``, so ``vol.Marker('x').default`` raises ``AttributeError``.
    Only ``Optional.__init__`` and ``Required.__init__`` assign it. Hence the
    generic ``getattr`` rather than either subclass: a missing attribute means
    the key is not a default-carrying marker at all (a bare ``str`` key, say),
    which is a different fault from a marker whose default is the ``UNDEFINED``
    sentinel, and the two are reported apart. Every stored default is a
    factory, hence the call; ``default=None`` yields a factory returning
    ``None``, never a bare ``None``, so the two guards never collide.

    Scope of the pin: this reads ``default``, and in a real Home Assistant
    form a ``suggested_value`` in the marker's ``description`` outranks it.
    ``FlowHandler.add_suggested_values_to_schema`` sets that key on every
    ``vol.Marker`` whose name it finds in the suggested mapping (measured
    against core 2026.2.3,
    which is what binds under this suite -- the conftest double is the
    fallback for a missing core, not what runs here). Neither field this pins
    is an options key, so neither reaches that mapping today; the day one does,
    this helper would still read the ``default`` while the user sees the
    suggestion.
    """

    schema = result["data_schema"].schema
    for marker, _validator in schema.items():
        if str(marker) != field:
            continue
        default = getattr(marker, "default", None)
        if default is None:
            raise AssertionError(
                f"field {field!r} is not a default-carrying vol.Marker subclass"
            )
        if default is config_flow.vol.UNDEFINED:
            raise AssertionError(f"field {field!r} carries no default")
        value = default()
        if value is config_flow.vol.UNDEFINED:  # pragma: no cover - factory shape
            raise AssertionError(f"field {field!r} carries no default")
        return value
    raise AssertionError(f"field {field!r} not present in the shown form")


async def test_subentry_options_are_gathered_from_the_real_read_only_mapping() -> None:
    """The ground every other test in this file stands on.

    Home Assistant exposes ``ConfigEntry.subentries`` as a ``MappingProxyType``,
    not as a ``dict``. That distinction is not cosmetic here: production reaches
    the subentries through an ``isinstance`` guard, and a guard narrowed to
    ``dict`` passes every test in this file while skipping every subentry a real
    user has. It did exactly that from 2025-10-27 until this commit, which is
    why the check runs against a genuine ``ConfigEntry`` rather than against the
    local stub -- a stub asserted against itself could only ever confirm its own
    shape.

    The identity assertion is the load-bearing one, and two of the assertions
    below are deliberately *not*: when the guard skips everything,
    ``_gather_subentry_options`` appends a synthetic option that carries
    ``TRACKER_SUBENTRY_KEY`` and is likewise exactly one, so neither
    ``option.key`` nor ``len(options)`` can tell the two paths apart. They are
    kept as context, not as evidence. What separates the paths is that the
    synthetic option has no backing subentry, so ``subentry_id`` is the cell
    that has to be checked, and the label -- the subentry's title rather than
    the entry's -- is checked next to it because it is what the user reads.
    Measured rather than reasoned: with the guard narrowed back to ``dict``,
    this test fails on the ``subentry_id`` line, not before it.

    Reaching the genuine class takes a detour that is worth naming, because it
    is the second reason the suite was blind here. ``tests/conftest.py`` puts a
    synthetic ``homeassistant.config_entries`` into ``sys.modules``, so the
    ``from homeassistant.config_entries import ...`` form at the top of this
    file resolves to the stub -- whose ``ConfigEntry`` is a bare placeholder
    without subentries. The parent package still carries the real submodule as
    an attribute, so the ``import ... as`` form below reaches it (measured: the
    two disagree, one reports the installed file, the other reports no file).
    Both imports are function-local as a consequence, which incidentally keeps
    the rest of this file independent of an optional plugin.
    """

    import homeassistant.config_entries as real_config_entries

    if not hasattr(real_config_entries, "ConfigSubentryData"):  # pragma: no cover
        pytest.fail(
            "The real homeassistant package must be importable for this test: "
            "it grounds the stub's subentry mapping in the core's own shape, "
            "and the suite's config_entries stub cannot stand in for it.",
            pytrace=False,
        )
    try:
        from pytest_homeassistant_custom_component.common import MockConfigEntry
    except ModuleNotFoundError:  # pragma: no cover - environment guard
        pytest.fail(
            "pytest-homeassistant-custom-component must be installed alongside "
            "homeassistant; see tests/AGENTS.md on the stub pairing.",
            pytrace=False,
        )

    real_entry = MockConfigEntry(
        domain=config_flow.DOMAIN,
        # Distinct from the subentry title below, so the fallback option (which
        # labels itself with the *entry* title) cannot be mistaken for the real
        # one.
        title="Account title",
        subentries_data=[
            real_config_entries.ConfigSubentryData(
                data={"group_key": TRACKER_SUBENTRY_KEY},
                subentry_type=SUBENTRY_TYPE_TRACKER,
                title="Subentry title",
                unique_id=None,
            )
        ],
    )

    # Asserted in both directions: ``Mapping`` alone would also hold for a
    # ``dict``, and it is the negative half that pins the core's actual shape.
    assert isinstance(real_entry.subentries, Mapping)
    assert not isinstance(real_entry.subentries, dict)

    # Built through this file's own helper and then pointed at the real entry:
    # the plumbing an options flow needs is beside the point here, the shape of
    # the entry it reads is the whole point.
    flow = await _build_flow(_EntryStub())
    flow.config_entry = real_entry  # type: ignore[attr-defined]

    options = flow._gather_subentry_options()

    assert len(options) == 1
    (option,) = options
    assert option.subentry_id == next(iter(real_entry.subentries))
    assert option.label == "Subentry title"
    assert option.key == TRACKER_SUBENTRY_KEY

    # The stub the rest of this file builds on carries the same shape, so those
    # tests exercise the production guard instead of a friendlier stand-in.
    assert not isinstance(_EntryStub().subentries, dict)


async def test_settings_schedules_the_reload_its_options_need() -> None:
    """The settings step asks for its own reload, through the owner latch.

    These options are read at setup time, so they only take effect after a
    reload. The step used to inherit one from ``OptionsFlowWithReload``; that
    base is gone (the core refuses it next to an update listener), so the
    reload has to be requested explicitly. Without this test the step would
    save its options and quietly do nothing until the next restart.

    The update listener is not a substitute: it reloads only for
    credential-relevant keys, and this form writes none.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    flow = await _build_flow(entry)

    result = await flow.async_step_settings({OPT_MAP_VIEW_TOKEN_EXPIRATION: True})

    assert result["type"] == "create_entry"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == [entry.entry_id]


async def test_settings_stands_down_but_still_writes_the_options(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A foreign owner costs the reload, never the write.

    The settings form is the one step whose options would otherwise be lost:
    standing down must leave the entry unchanged in every other respect, so the
    reload that owner is bringing carries this write too. The log assertion is
    what makes the stand-down branch itself measured rather than merely
    executed -- without it, ignoring the return value would look the same.
    """

    caplog.set_level(logging.DEBUG, logger=config_flow._LOGGER.name)
    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    flow = await _build_flow(entry)
    _claim_latch(flow.hass, entry.entry_id)

    result = await flow.async_step_settings({OPT_MAP_VIEW_TOKEN_EXPIRATION: True})

    assert result["type"] == "create_entry"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    assert manager.reloads == []
    assert result["data"][OPT_MAP_VIEW_TOKEN_EXPIRATION] is True
    assert manager.updated == []
    assert any(
        "were saved without scheduling a reload" in record.message
        for record in caplog.records
    )


async def _settled_settings_state(
    user_input: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], str]:
    """Return the options, subentry data and title a settings submit leaves behind.

    Used to build the *unchanged* starting point for the test below. Taken
    from a throwaway entry with its own hass double on purpose: submitting twice
    against one double would leave the shared reload latch claimed from the first
    run, and the second run would then stand down as a foreign owner -- passing
    for the wrong reason and proving nothing about the change detection.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    flow = await _build_flow(entry)
    result = await flow.async_step_settings(dict(user_input))
    assert result["type"] == "create_entry"
    subentry = next(iter(entry.subentries.values()))
    return dict(result["data"]), dict(subentry.data), subentry.title


async def test_settings_skips_the_reload_when_nothing_changed() -> None:
    """Confirming the form without an edit must not tear the entry down.

    This reproduces what the step inherited, it does not refine it: the core
    scheduled on ``async_update_entry(...) and automatic_reload``, and
    ``async_update_entry`` returns ``False`` for an unchanged payload
    (``OptionsFlowManager.async_finish_flow``, cores 2026.1.3 and 2026.2.3).
    An unconditional claim would make a no-op save remove every entity of the
    account and repeat the whole setup, and would hold the single-owner latch
    while doing it -- which is why the latch is asserted free at the end rather
    than only the recorders being empty.
    """

    user_input = {
        OPT_MAP_VIEW_TOKEN_EXPIRATION: True,
        OPT_CONTRIBUTOR_MODE: "high_traffic",
    }
    options, subentry_data, subentry_title = await _settled_settings_state(user_input)

    entry = _EntryStub()
    subentry = entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title=subentry_title)
    subentry.data = MappingProxyType(dict(subentry_data))
    entry.options = options
    flow = await _build_flow(entry)

    result = await flow.async_step_settings(dict(user_input))

    assert result["type"] == "create_entry"
    assert dict(result["data"]) == options
    manager = flow.hass.config_entries  # type: ignore[assignment]
    # Both sides (``tests/AGENTS.md`` rule 8): neither the scheduling nor the
    # awaited variant may fire.
    assert manager.scheduled_reloads == []
    assert manager.reloads == []
    # The latch was never taken, so a reload another owner brings is unaffected.
    assert _latch_is_free(flow.hass, entry.entry_id) is True


async def test_settings_reloads_for_an_entry_without_any_subentry() -> None:
    """An entry without any subentry saves its options and reloads once.

    The options are account-wide, so the step neither needs nor touches a
    subentry: nothing is written to a group, and the changed options alone
    schedule the reload.
    """

    entry = _EntryStub()
    assert entry.subentries == {}
    flow = await _build_flow(entry)

    result = await flow.async_step_settings({OPT_MAP_VIEW_TOKEN_EXPIRATION: True})

    assert result["type"] == "create_entry"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.updated == []
    assert manager.scheduled_reloads == [entry.entry_id]
    assert manager.reloads == []


async def test_visibility_assigns_devices_to_target_subentry() -> None:
    """Visibility step should attach restored devices to the chosen subentry."""

    entry = _EntryStub()
    sub = entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.options = {
        OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}},
    }

    flow = await _build_flow(entry)
    result = await flow.async_step_visibility(
        {"subentry": TRACKER_SUBENTRY_KEY, "unignore_devices": ["dev-1"]}
    )

    assert result["type"] == "create_entry"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.updated
    updated_id, payload = manager.updated[-1]
    assert updated_id == sub.subentry_id
    assert payload["visible_device_ids"] == ("dev-1",)
    # Assert both sides (``tests/AGENTS.md`` rule 8): the step routes through
    # ``_schedule_claimed_reload`` like its two sibling assignment sites, so the
    # scheduling side has to be claimed and the direct side has to stay empty.
    # Unignoring without a reload leaves the device without any entity, because
    # the platform known-sets never drop an id -- see the comment at the call
    # site and the platform-level characterisation in
    # ``test_device_tracker_scanner.py``.
    assert manager.scheduled_reloads == [entry.entry_id]
    assert manager.reloads == []


async def test_visibility_neither_offers_nor_accepts_the_service_subentry() -> None:
    """The service group is gone from the target list, and submitting it fails.

    This is the inversion of the characterisation recorded before the fix: back
    then the form offered the key *and* the sink wrote device ids into it. Both
    halves are asserted here for the same reason they were asserted then. A fix
    that only hid the option while leaving the submission path writable would
    flip the first assertion and keep the second, which is exactly the half-fix
    this pairing exists to catch.

    Why the write was a dead end: ``coordinator/subentry.py`` forces
    ``visible_device_ids`` back to ``()`` for the service key on every index
    refresh, and the next entry setup clears the stored value a second time --
    see ``test_service_subentry_visibility_is_cleared_by_the_setup_roundtrip``
    in ``test_options_flow_registry_updates.py``.

    Both reload directions are asserted (``tests/AGENTS.md`` rule 8): a refused
    submission writes nothing and therefore claims no reload either. Leaving
    that open would let a future change tear the entry down for an action it
    just rejected.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}

    flow = await _build_flow(entry)

    form = await flow.async_step_visibility()
    offered = _offered_keys(form, "subentry")
    assert SERVICE_SUBENTRY_KEY not in offered
    assert TRACKER_SUBENTRY_KEY in offered

    result = await flow.async_step_visibility(
        {"subentry": SERVICE_SUBENTRY_KEY, "unignore_devices": ["dev-1"]}
    )

    assert result["type"] == "form"
    assert result["errors"] == {"subentry": "invalid_subentry"}
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.updated == []
    assert manager.scheduled_reloads == []
    assert manager.reloads == []


async def test_visibility_still_assigns_and_reloads_for_a_device_group() -> None:
    """The narrowing must not cost the legitimate target its assignment.

    The companion to the test above: hiding the service group is only correct
    if the tracker group still works end to end. Both sides again -- the write
    lands, and the reload claim introduced by PR #1228 is still taken, because
    a device returning from the ignore list has no entity until a reload
    rebuilds the platform known-sets.
    """

    entry = _EntryStub()
    core = entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}

    flow = await _build_flow(entry)

    result = await flow.async_step_visibility(
        {"subentry": TRACKER_SUBENTRY_KEY, "unignore_devices": ["dev-1"]}
    )
    await asyncio.sleep(0)

    assert result["type"] == "create_entry"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    updated = {sid: payload for sid, payload in manager.updated}
    assert tuple(updated[core.subentry_id]["visible_device_ids"]) == ("dev-1",)
    assert manager.scheduled_reloads == [entry.entry_id]
    assert manager.reloads == []


async def test_repairs_move_does_not_offer_the_service_subentry() -> None:
    """Second of the three assignment steps: the move target list is narrowed."""

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )

    flow = await _build_flow(entry)

    form = await flow.async_step_repairs_move()
    offered = _offered_keys(form, "target_subentry")
    assert SERVICE_SUBENTRY_KEY not in offered
    assert TRACKER_SUBENTRY_KEY in offered


async def test_repairs_delete_narrows_the_fallback_but_not_the_deletion() -> None:
    """Third assignment step, and the one place where the two lists differ.

    What may be *deleted* is a different question from what may *receive*
    devices. The service group stays removable -- the core repair path recreates
    a missing one, and taking that away would be an unrelated product decision
    (veto point ``V-3-T`` of the plan) -- while it must not be offered as the
    fallback that inherits the deleted group's devices.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.add_subentry(key="second", title="Second")
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )

    flow = await _build_flow(entry)

    form = await flow.async_step_repairs_delete()
    assert SERVICE_SUBENTRY_KEY not in _offered_keys(form, "fallback_subentry")
    assert SERVICE_SUBENTRY_KEY in _offered_keys(form, "delete_subentry")


async def test_the_non_assigning_steps_keep_the_service_subentry() -> None:
    """The narrowing stops at the three steps that assign devices.

    ``_subentry_choice_map`` fed all six steps before the split. Filtering
    inside it would have been the smaller diff and the larger mistake:
    ``async_step_credentials`` legitimately targets the service group (the
    settings step no longer offers a group at all), and ``async_step_repairs``
    only asks whether *any* subentry exists, so a shared filter could send an
    entry whose sole subentry is the service group into
    ``repairs_no_subentries``. Both are checked here, the last one in its worst
    case. The third remaining caller, the ``removable_choices`` of
    ``async_step_repairs_delete``, has its own test.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )
    flow = await _build_flow(entry)

    credentials_form = await flow.async_step_credentials()
    assert SERVICE_SUBENTRY_KEY in _offered_keys(credentials_form, "subentry")

    service_only = _EntryStub()
    service_only.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )
    service_only_flow = await _build_flow(service_only)

    menu = await service_only_flow.async_step_repairs()
    assert menu["type"] == "menu"


async def test_the_sink_refuses_a_service_target_on_a_direct_call() -> None:
    """The guard sits at the data-flow chokepoint, not only in the forms.

    The forms can no longer produce this call, so any occurrence is a caller
    bug. It is still worth pinning: ``_async_assign_devices_to_subentry`` is
    the single writer able to break an invariant that three other production
    sites already keep, and a fourth caller added later would inherit the
    protection without knowing about it.

    Standing down rather than stripping the ids elsewhere is asserted too: the
    tracker group keeps what it legitimately holds.
    """

    entry = _EntryStub()
    core = entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY, title="Core", visible_device_ids=["dev-1"]
    )
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )
    flow = await _build_flow(entry)

    changed = await flow._async_assign_devices_to_subentry(
        entry,  # type: ignore[arg-type]
        SERVICE_SUBENTRY_KEY,
        ["dev-1"],
    )

    assert SERVICE_SUBENTRY_KEY not in changed
    assert changed == set()
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.updated == []
    # The device stays where it legitimately was.
    assert tuple(core.data["visible_device_ids"]) == ("dev-1",)


async def test_a_legacy_subentry_typed_service_is_excluded_by_its_type() -> None:
    """The key alone does not carry the proof, so the type is read as well.

    ``ConfigEntrySubEntryManager._refresh_from_entry`` derives the canonical key
    primarily from ``subentry_type`` and keeps a diverging stored ``group_key``
    as an alias, which ``agents/config_flow/AGENTS.md`` requires under
    ``Subentry alias handling``. A predicate comparing only the key would let
    this subentry through in all three steps.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.add_subentry(key="second", title="Second")
    entry.add_subentry(
        key="owner@example.com",
        title="Legacy Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}

    flow = await _build_flow(entry)

    visibility = await flow.async_step_visibility()
    move = await flow.async_step_repairs_move()
    delete = await flow.async_step_repairs_delete()

    assert "owner@example.com" not in _offered_keys(visibility, "subentry")
    assert "owner@example.com" not in _offered_keys(move, "target_subentry")
    assert "owner@example.com" not in _offered_keys(delete, "fallback_subentry")


@pytest.mark.parametrize(
    ("service_title", "tracker_title", "service_sorts_first"),
    [
        ("Alpha service", "Zulu trackers", True),
        ("Zulu service", "Alpha trackers", False),
    ],
    ids=["service-sorts-first", "tracker-sorts-first"],
)
async def test_a_group_key_shared_by_two_types_still_lands_on_the_tracker(
    service_title: str, tracker_title: str, service_sorts_first: bool
) -> None:
    """The two orderings fail differently, so both are exercised.

    ``Subentry alias handling`` lets one legacy label sit on a service and a
    tracker subentry at once, and ``_gather_subentry_options`` sorts by label,
    so the alphabet decided which subentry a key resolved to. Resolving the
    first holder while writing to every holder meant: service first -> the
    guard judged the wrong twin and refused, so the move reported success
    without moving anything; tracker first -> the guard passed and the write
    fanned out onto the service group, storing exactly the ids the invariant
    forbids.

    Both assertions are needed. Only checking the return value misses the
    fan-out, only checking the service payload misses the silent refusal.
    """

    entry = _EntryStub()
    service = entry.add_subentry(
        key="owner@example.com",
        title=service_title,
        subentry_type=SUBENTRY_TYPE_SERVICE,
        identity="legacy-service",
    )
    tracker = entry.add_subentry(
        key="owner@example.com",
        title=tracker_title,
        visible_device_ids=[],
        identity="legacy-tracker",
    )
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}
    flow = await _build_flow(entry)

    offered = _offered_keys(await flow.async_step_visibility(), "subentry")
    options = flow._gather_subentry_options()
    target_key = next(option.key for option in options if option.subentry is tracker)

    # The ordering this case claims to exercise is pinned against a value from
    # the parameter list, not derived from the titles. Deriving it would only
    # restate the sort and would still pass if both parameter sets happened to
    # order the same way -- which is exactly how the first version of this test
    # left the second half of the asymmetry uncovered while staying green.
    assert (options[0].subentry is service) is service_sorts_first
    assert target_key in offered

    changed = await flow._async_assign_devices_to_subentry(
        entry,  # type: ignore[arg-type]
        target_key,
        ["dev-1"],
    )

    assert changed == {target_key}
    assert tuple(tracker.data.get("visible_device_ids", ())) == ("dev-1",)
    assert tuple(service.data.get("visible_device_ids", ())) == ()


async def test_the_option_key_identifies_exactly_one_subentry() -> None:
    """The property the two choice maps and the deletion step depend on.

    Both ``_subentry_choice_map`` and ``_device_target_choice_map`` collapse the
    key into a ``dict``, and ``async_step_repairs_delete`` resolves both the
    deletion target and the devices it hands on through that mapping. A key
    carried by two subentries therefore hides one of them from every form and
    can delete the other. Asserted as an invariant over the whole list rather
    than against a fixed expectation, so a future shape inherits the guarantee.

    Three holders rather than two, and one of them a service group, so the
    disambiguation is not read as a two-element swap. The last two subentries
    build the chain that defeats a rewrite touching only the duplicates: the
    fourth stores the ``subentry_id`` a duplicate would move to, and the fifth
    stores the ``subentry_id`` of the fourth. Any rewrite bounded by a fixed
    number of passes hands out a fresh duplicate here, which is why the
    production rule moves *every* option once one duplicate exists.

    Both identifiers are derived from ``entry.entry_id`` rather than written
    out. A literal would stop matching a real ``subentry_id`` the moment that
    attribute changed, the collision would not arise, and the invariant below
    would hold trivially while covering nothing.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.add_subentry(key="shared", title="First", identity="first")
    entry.add_subentry(key="shared", title="Second", identity="second")
    entry.add_subentry(
        key="shared",
        title="Legacy service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        identity="third",
    )
    entry.add_subentry(
        key=_stable_subentry_id(entry.entry_id, "first"),
        title="Collides with a replacement",
        identity="fourth",
    )
    entry.add_subentry(
        key=_stable_subentry_id(entry.entry_id, "fourth"),
        title="Collides one step further out",
        identity="fifth",
    )
    flow = await _build_flow(entry)

    options = flow._gather_subentry_options()

    assert len(options) == 6
    assert len({option.key for option in options}) == len(options)
    # All-or-nothing: once one duplicate exists every option carries its own
    # ``subentry_id``, including the healthy tracker group. Pinned so the
    # difference to the narrower rule stays visible, and asserted through the
    # subentries rather than against written-out ids.
    assert {option.key for option in options} == set(entry.subentries)
    assert TRACKER_SUBENTRY_KEY not in {option.key for option in options}


async def test_a_subentry_without_a_stored_group_key_falls_back_to_its_id() -> None:
    """The other source of an option key, and the reason it is already unique.

    A subentry whose ``group_key`` is missing or blank takes its
    ``subentry_id`` as the key. That branch is what makes the disambiguation
    above a no-op for such a subentry, and it is why the rewrite can claim not
    to change what ``_async_refresh_subentry_entry_title`` would store.
    """

    entry = _EntryStub()
    blank = entry.add_subentry(key="   ", title="Blank", identity="blank")
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")

    flow = await _build_flow(entry)

    options = flow._gather_subentry_options()
    keys = {option.subentry_id: option.key for option in options}

    assert keys[blank.subentry_id] == blank.subentry_id
    assert len({option.key for option in options}) == len(options)


async def test_a_shared_group_key_leaves_both_groups_reachable_for_deletion() -> None:
    """What the injectivity buys at the consumer, not just as a property.

    ``async_step_repairs_delete`` resolves both the deletion target and the
    devices it hands to the fallback through the ``dict`` of
    ``_subentry_choice_map``. While two subentries shared one key that mapping
    held a single entry, so one group was invisible, the coupling
    ``fallback_key != target_key`` could never be satisfied and the whole step
    aborted on ``subentry_delete_invalid``: the user could not delete either
    group.
    """

    entry = _EntryStub()
    service = entry.add_subentry(
        key="owner@example.com",
        title="Zulu service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        identity="legacy-service",
    )
    tracker = entry.add_subentry(
        key="owner@example.com",
        title="Alpha trackers",
        visible_device_ids=["dev-1"],
        identity="legacy-tracker",
    )
    flow = await _build_flow(entry)

    form = await flow.async_step_repairs_delete()
    keys = {
        option.subentry_id: option.key for option in flow._gather_subentry_options()
    }

    assert form["type"] == "form"
    assert _offered_keys(form, "delete_subentry") == {keys[service.subentry_id]}
    assert _offered_keys(form, "fallback_subentry") == {keys[tracker.subentry_id]}


async def test_a_move_onto_a_shared_key_reports_success_only_when_it_moved() -> None:
    """The refusal and "nothing to do" must not look alike to the caller.

    ``async_step_repairs_move`` maps an empty return value onto
    ``subentry_move_success``. While the guard could refuse on behalf of a twin
    the user never picked, that abort told the user their device had moved when
    it had not.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key="owner@example.com",
        title="Alpha service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        identity="legacy-service",
    )
    tracker = entry.add_subentry(
        key="owner@example.com",
        title="Zulu trackers",
        visible_device_ids=[],
        identity="legacy-tracker",
    )
    entry.runtime_data.coordinator.data = [{"device_id": "dev-1", "name": "Device 1"}]
    flow = await _build_flow(entry)

    form = await flow.async_step_repairs_move()
    target_key = next(
        option.key
        for option in flow._gather_subentry_options()
        if option.subentry is tracker
    )

    assert target_key in _offered_keys(form, "target_subentry")

    result = await flow.async_step_repairs_move(
        {"target_subentry": target_key, "device_ids": ["dev-1"]}
    )
    await asyncio.sleep(0)

    assert result["reason"] == "subentry_move_success"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    updated = {sid: payload for sid, payload in manager.updated}
    assert tuple(updated[tracker.subentry_id]["visible_device_ids"]) == ("dev-1",)


async def test_an_entry_with_only_a_service_group_still_offers_a_target() -> None:
    """The empty arm of the narrowing is exercised through to its effect.

    Filtering an entry whose only subentry is a service group would leave
    ``vol.In`` with an empty mapping, which renders a form the user cannot
    submit. The fallback mirrors the one ``_gather_subentry_options`` already
    applies for an entry without any subentry (invariant LC-1: a set operation
    needs its clear counterpart spelled out).

    Both sides are asserted, because the offered key is a *synthesised* option
    with no backing subentry, so the assignment writes nothing. That is not a
    dead end: an id no subentry claims is merged into the tracker group by
    ``coordinator/subentry.py::_refresh_subentry_index``, which synthesises the
    tracker metadata when the entry has none. What the user asked for -- the
    device out of the ignore list and visible again -- therefore happens, and
    the un-ignore write plus the reload claim are what carry it.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}

    flow = await _build_flow(entry)

    form = await flow.async_step_visibility()
    offered = _offered_keys(form, "subentry")

    assert offered == {TRACKER_SUBENTRY_KEY}
    assert SERVICE_SUBENTRY_KEY not in offered

    result = await flow.async_step_visibility(
        {"subentry": TRACKER_SUBENTRY_KEY, "unignore_devices": ["dev-1"]}
    )
    await asyncio.sleep(0)

    assert result["type"] == "create_entry"
    assert result["data"][OPT_IGNORED_DEVICES] == {}
    manager = flow.hass.config_entries  # type: ignore[assignment]
    # No subentry write: the only real subentry is the service group, and it
    # must not receive the id.
    assert manager.updated == []
    assert manager.scheduled_reloads == [entry.entry_id]


async def test_the_synthesised_fallback_never_borrows_a_key_in_use() -> None:
    """The fallback must not take the identity of a group it filtered out.

    An entry whose only subentry is typed ``service`` while storing the legacy
    ``group_key`` ``core_tracking`` is removed from the target list by its
    type, and the fallback used to offer *that* key back. The sink resolves a
    key against the **unfiltered** set, so it found the real, non-assignable
    subentry and refused the write.

    Asserted on the identity, not on a literal: the point is that no real
    option holds the offered key, not which substitute the helper picked.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Legacy service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=["dev-1"],
    )
    flow = await _build_flow(entry)

    choices, option_map = flow._device_target_choice_map()
    (offered_key,) = choices
    stored_keys = {opt.key for opt in flow._gather_subentry_options()}

    assert option_map[offered_key].subentry is None
    assert offered_key not in stored_keys


async def test_the_fallback_key_search_is_total_across_several_holders() -> None:
    """One substitute is not enough, so the search walks until it is free.

    A second subentry may store the very substitute the first one is displaced
    to. A fixed number of attempts would hand out a borrowed key again, one
    step further out, which is the same defect the search removes.
    """

    entry = _EntryStub()
    for index, key in enumerate(
        (TRACKER_SUBENTRY_KEY, f"{TRACKER_SUBENTRY_KEY}_2", f"{TRACKER_SUBENTRY_KEY}_3")
    ):
        entry.add_subentry(
            key=key,
            title=f"Legacy service {index}",
            subentry_type=SUBENTRY_TYPE_SERVICE,
            identity=f"legacy-{index}",
        )
    flow = await _build_flow(entry)

    choices, _ = flow._device_target_choice_map()
    (offered_key,) = choices

    assert offered_key not in {opt.key for opt in flow._gather_subentry_options()}


async def test_repairs_move_refuses_an_entry_without_a_real_destination() -> None:
    """A move needs a destination, and saying otherwise was the defect.

    Without a backing subentry the assignment writes nothing, ``changed``
    stays empty, and the step used to abort on ``subentry_move_success`` --
    a success message for a move that could not happen. The synthesised
    fallback is therefore not a candidate here, mirroring the
    ``real_fallback_keys`` line ``async_step_repairs_delete`` already draws
    for its own fallback field. The abort string already exists, so no
    translation key is added.
    """

    entry = _EntryStub()
    legacy = entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Legacy service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=["dev-1"],
    )
    flow = await _build_flow(entry)

    result = await flow.async_step_repairs_move()

    assert result["type"] == "abort"
    assert result["reason"] == "repairs_no_subentries"
    # The devices stay where they were: no strip, no loss.
    assert tuple(legacy.data["visible_device_ids"]) == ("dev-1",)
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.updated == []


async def test_the_sink_stands_down_when_no_option_carries_the_target() -> None:
    """Without a destination the loop must not run at all.

    Its ``else`` branch strips the ids from every group that holds them. With
    a real target that is the second half of a move; without one it is a loss
    -- and it would be reported as a success, because a strip fills
    ``changed``.
    """

    entry = _EntryStub()
    legacy = entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Legacy service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=["dev-1"],
    )
    flow = await _build_flow(entry)

    changed = await flow._async_assign_devices_to_subentry(
        entry,  # type: ignore[arg-type]
        "a-key-no-option-carries",
        ["dev-1"],
    )

    assert changed == set()
    assert tuple(legacy.data["visible_device_ids"]) == ("dev-1",)
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.updated == []


async def test_repairs_delete_stays_submittable_in_the_common_two_group_shape() -> None:
    """Narrowing the fallback must re-derive what is deletable, not just filter.

    The two lists are coupled through ``fallback_key != target_key``. In the
    shape the integration provisions for itself -- one tracker group and one
    service group -- narrowing only the fallback side left a form whose single
    fallback value was always the deletion target, so every submission failed
    on ``invalid_subentry`` with no way out. That is the same defect the step
    was narrowed to remove, one field higher up.

    The tracker group therefore drops out of the deletion list here: nothing
    else could inherit its devices. The service group stays, and deleting it
    works end to end.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    service = entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
    )

    flow = await _build_flow(entry)

    form = await flow.async_step_repairs_delete()
    assert form["type"] == "form"
    assert _offered_keys(form, "delete_subentry") == {SERVICE_SUBENTRY_KEY}
    assert _offered_keys(form, "fallback_subentry") == {TRACKER_SUBENTRY_KEY}

    result = await flow.async_step_repairs_delete(
        {
            "delete_subentry": SERVICE_SUBENTRY_KEY,
            "fallback_subentry": TRACKER_SUBENTRY_KEY,
        }
    )
    await asyncio.sleep(0)

    assert result["type"] == "abort"
    assert result["reason"] == "subentry_delete_success"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert service.subentry_id in manager.removed


async def test_repairs_move_assigns_devices_to_selected_subentry() -> None:
    """Repair move step should remove devices from other subentries."""

    entry = _EntryStub()
    target = entry.add_subentry(key="target", title="Target", visible_device_ids=[])
    other = entry.add_subentry(key="other", title="Other", visible_device_ids=["dev-2"])
    entry.runtime_data.coordinator.data = [
        {"device_id": "dev-1", "name": "Device 1"},
        {"device_id": "dev-2", "name": "Device 2"},
    ]

    flow = await _build_flow(entry)

    async def _invoke() -> dict[str, Any]:
        result = await flow.async_step_repairs_move(
            {"target_subentry": "target", "device_ids": ["dev-1", "dev-2"]}
        )
        await asyncio.sleep(0)
        return result

    result = await _invoke()

    assert result["type"] == "abort"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.updated
    updated = {sid: payload for sid, payload in manager.updated}
    assert tuple(updated[target.subentry_id]["visible_device_ids"]) == (
        "dev-1",
        "dev-2",
    )
    assert tuple(updated[other.subentry_id]["visible_device_ids"]) == ()
    # Test D: exactly one reload, and it goes through the scheduling lever.
    assert manager.scheduled_reloads == [entry.entry_id]
    assert manager.reloads == []


async def test_repairs_delete_moves_devices_and_removes_subentry() -> None:
    """Deleting a subentry moves devices to fallback and removes the source."""

    entry = _EntryStub()
    removable = entry.add_subentry(
        key="remove", title="Remove", visible_device_ids=["dev-1", "dev-2"]
    )
    fallback = entry.add_subentry(key="keep", title="Keep", visible_device_ids=[])

    flow = await _build_flow(entry)

    async def _invoke_delete() -> dict[str, Any]:
        result = await flow.async_step_repairs_delete(
            {"delete_subentry": "remove", "fallback_subentry": "keep"}
        )
        await asyncio.sleep(0)
        return result

    result = await _invoke_delete()

    assert result["type"] == "abort"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert removable.subentry_id in manager.removed
    updated = {sid: payload for sid, payload in manager.updated}
    assert tuple(updated[fallback.subentry_id]["visible_device_ids"]) == (
        "dev-1",
        "dev-2",
    )
    assert manager.scheduled_reloads == [entry.entry_id]
    assert manager.reloads == []


async def test_the_scheduling_double_matches_the_core_call_shape() -> None:
    """The recorder has to be reachable and synchronous, or a typo hides here.

    Declared ``async`` only because this module carries a file-wide
    ``pytestmark = pytest.mark.asyncio``; the check itself needs no event loop.

    The repair tests now assert on ``scheduled_reloads``, so a misspelled method
    name no longer passes unnoticed. What stays worth pinning is the *shape*: an
    awaitable double would let the production call site look awaitable when it is
    not, and a recorder chaining into ``async_reload`` would blur the two routes.
    """

    entry = _EntryStub()
    manager = _ManagerStub(entry)

    schedule = getattr(manager, "async_schedule_reload", None)
    assert callable(schedule)
    assert not inspect.iscoroutinefunction(schedule), (
        "the core's async_schedule_reload is a @callback; an awaitable double "
        "would hide that the production call site does not await it"
    )

    schedule(entry.entry_id)
    assert manager.scheduled_reloads == [entry.entry_id]
    assert manager.reloads == [], (
        "the two recorders have to stay distinguishable; that is the whole "
        "point of keeping async_reload next to it"
    )


# --- AP5: the repair steps stand behind the single reload owner --------------


def _claim_latch(hass: Any, entry_id: str) -> None:
    """Take the shared latch on behalf of a foreign reload owner."""

    integration = config_flow.import_integration_package()
    assert integration.claim_pending_entry_reload(hass, entry_id) is True


def _latch_is_free(hass: Any, entry_id: str) -> bool:
    """Whether the shared reload latch of ``entry_id`` can be claimed again.

    Claims the latch as a side effect, so this belongs at the end of a test.
    """

    integration = config_flow.import_integration_package()
    return bool(integration.claim_pending_entry_reload(hass, entry_id))


async def test_repairs_move_stands_down_but_still_writes_the_assignment() -> None:
    """Test E: a foreign owner costs the reload, never the assignment."""

    entry = _EntryStub()
    target = entry.add_subentry(key="target", title="Target", visible_device_ids=[])
    entry.add_subentry(key="other", title="Other", visible_device_ids=["dev-1"])
    entry.runtime_data.coordinator.data = [{"device_id": "dev-1", "name": "Device 1"}]

    flow = await _build_flow(entry)
    _claim_latch(flow.hass, entry.entry_id)

    result = await flow.async_step_repairs_move(
        {"target_subentry": "target", "device_ids": ["dev-1"]}
    )
    await asyncio.sleep(0)

    assert result["type"] == "abort"
    assert result["reason"] == "subentry_move_success"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    assert manager.reloads == []
    updated = {sid: payload for sid, payload in manager.updated}
    assert tuple(updated[target.subentry_id]["visible_device_ids"]) == ("dev-1",)


async def test_repairs_delete_stands_down_but_still_removes_the_subentry() -> None:
    """Test F: the removal happens before the claim and survives it."""

    entry = _EntryStub()
    removable = entry.add_subentry(
        key="remove", title="Remove", visible_device_ids=["dev-1"]
    )
    entry.add_subentry(key="keep", title="Keep", visible_device_ids=[])

    flow = await _build_flow(entry)
    _claim_latch(flow.hass, entry.entry_id)

    result = await flow.async_step_repairs_delete(
        {"delete_subentry": "remove", "fallback_subentry": "keep"}
    )
    await asyncio.sleep(0)

    assert result["type"] == "abort"
    assert result["reason"] == "subentry_delete_success"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    assert manager.reloads == []
    assert removable.subentry_id in manager.removed


async def test_a_move_that_changes_nothing_does_not_burn_the_latch() -> None:
    """Test G: an ineffective step schedules nothing and leaves the latch free.

    The second assertion is the one that matters. A claim pulled in front of the
    ``if not changed`` exit would satisfy the first (nothing is scheduled on that
    branch anyway) while leaving the latch set for the lifetime of the process,
    swallowing every later reload of this entry.
    """

    entry = _EntryStub()
    entry.add_subentry(key="target", title="Target", visible_device_ids=["dev-1"])
    entry.runtime_data.coordinator.data = [{"device_id": "dev-1", "name": "Device 1"}]

    flow = await _build_flow(entry)

    result = await flow.async_step_repairs_move(
        {"target_subentry": "target", "device_ids": ["dev-1"]}
    )
    await asyncio.sleep(0)

    assert result["type"] == "abort"
    assert result["reason"] == "subentry_move_success"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    assert manager.reloads == []
    assert _latch_is_free(flow.hass, entry.entry_id) is True


async def test_a_hopeless_entry_stops_the_repair_step_from_claiming_the_latch() -> None:
    """Test H: the state gate is reachable from this file, and it bites.

    Tests D to G above exercise the latch half of ``_schedule_claimed_reload``
    while the state gate in front of it can only ever answer "not hopeless",
    because ``_EntryStub`` now carries healthy values for the three fields it
    reads. That makes those tests honest but leaves the gate itself unmeasured
    from here -- and the gate is the reason the repair steps were routed through
    the single owner at all. This test closes that hole for the repair path: an
    entry in a terminal state must not have its latch claimed, because no setup
    would follow to hand it back, and the leaked latch would then swallow every
    later reload of this entry.

    Removing ``_entry_reload_is_hopeless`` from ``_schedule_claimed_reload``
    turns the first two assertions red; the third one guards against "fixing"
    that by skipping the write as well.
    """

    entry = _EntryStub()
    entry.state = ConfigEntryState.MIGRATION_ERROR
    entry.add_subentry(key="target", title="Target", visible_device_ids=[])
    entry.runtime_data.coordinator.data = [{"device_id": "dev-1", "name": "Device 1"}]

    flow = await _build_flow(entry)

    result = await flow.async_step_repairs_move(
        {"target_subentry": "target", "device_ids": ["dev-1"]}
    )
    await asyncio.sleep(0)

    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    assert _latch_is_free(flow.hass, entry.entry_id) is True
    # The step still did its job: standing down is about the reload, never
    # about the change the user asked for.
    assert result["type"] == "abort"
    assert result["reason"] == "subentry_move_success"
    assert "dev-1" in entry.subentries[next(iter(entry.subentries))].data.get(
        "visible_device_ids", []
    )


async def test_visibility_without_a_selection_does_not_schedule_a_reload() -> None:
    """Test I: closing the form without restoring anything must stay inert.

    The claim sits inside ``if to_restore:`` for this reason. Pulled in front of
    it, every visit to the visibility page that the user simply confirms would
    reload the entry, and a claim that no assignment justifies would still occupy
    the single owner for the duration of that reload.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}

    flow = await _build_flow(entry)
    result = await flow.async_step_visibility(
        {"subentry": TRACKER_SUBENTRY_KEY, "unignore_devices": []}
    )
    await asyncio.sleep(0)

    assert result["type"] == "create_entry"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    assert manager.reloads == []
    assert _latch_is_free(flow.hass, entry.entry_id) is True


async def test_a_hopeless_entry_stops_the_visibility_step_from_claiming(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test J: the state gate is reachable from the visibility step too.

    Test H does this for the repair path. The visibility step took its claim
    later and would otherwise be the one assignment site whose gate nobody
    measures: a terminal entry has no setup coming that would hand the latch
    back, so a claim taken here would swallow every later reload of this entry.
    Two assertions guard against "fixing" a red gate by skipping the user's
    change as well -- standing down is about the reload, never about the
    assignment.

    The last one pins the warning. Here a stand-down costs the whole thing, not
    a half: the restored device has no entity to fall back on, so this branch
    has to say so in the log rather than return a silent success.
    """

    entry = _EntryStub()
    entry.state = ConfigEntryState.MIGRATION_ERROR
    sub = entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}

    flow = await _build_flow(entry)
    result = await flow.async_step_visibility(
        {"subentry": TRACKER_SUBENTRY_KEY, "unignore_devices": ["dev-1"]}
    )
    await asyncio.sleep(0)

    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    # Both sides (rule 8): without the second one a later rewrite that swaps
    # ``_schedule_claimed_reload`` for a direct ``async_reload`` would bypass the
    # state gate and still leave this test green.
    assert manager.reloads == []
    assert _latch_is_free(flow.hass, entry.entry_id) is True
    assert result["type"] == "create_entry"
    updated_id, payload = manager.updated[-1]
    assert updated_id == sub.subentry_id
    assert payload["visible_device_ids"] == ("dev-1",)
    assert any(
        record.levelname == "WARNING" and "no reload was scheduled" in record.message
        for record in caplog.records
    )


async def test_a_hub_typed_group_is_not_offered_as_a_device_target() -> None:
    """The hub type is excluded from assignment targets, not just the service one.

    ``_NON_DEVICE_SUBENTRY_TYPES`` is now shared with the runtime index rather
    than restated here, so a type dropped on one side would silently reappear
    as an assignable target on the other. This pins the hub half of that set:
    without it only ``service`` was covered by a test, and the coupling could
    regress unnoticed.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Legacy hub",
        subentry_type=SUBENTRY_TYPE_HUB,
        visible_device_ids=["dev-1"],
    )
    flow = await _build_flow(entry)

    choices, option_map = flow._device_target_choice_map()
    (offered_key,) = choices

    assert option_map[offered_key].subentry is None, (
        "a hub-typed group must not be an assignment target, so the only "
        "offered key has to be the synthesised fallback"
    )
    assert offered_key not in {opt.key for opt in flow._gather_subentry_options()}


@pytest.mark.parametrize(
    "parked_first", [True, False], ids=["parked_first", "twin_first"]
)
async def test_a_tracker_parked_on_the_service_key_is_not_offered_as_a_target(
    parked_first: bool,
) -> None:
    """The reserved-key axis survives the collision rewrite of ``option.key``.

    ``_gather_subentry_options`` rewrites *every* option key to its
    ``subentry_id`` as soon as one key is duplicated, which is what makes the
    keys injective. That rewrite turns ``key`` into an identity and strips its
    meaning, so a predicate that keeps reading it for meaning silently stops
    working: a legacy ``tracker`` storing ``SERVICE_SUBENTRY_KEY`` no longer
    matches ``_NON_DEVICE_SUBENTRY_KEYS`` and is offered as an independently
    assignable target, while ``coordinator/subentry.py`` still indexes it under
    the stored service key and forces its visible ids to ``()``. The user picks
    it, the flow reports success, and the devices land in no group -- the exact
    failure ``e8114585`` closed.

    The duplicate is supplied by the *real* service subentry, so the shape is
    the one a legacy entry actually has rather than a contrived pair.

    The second iteration order does *not* discriminate today and is not claimed
    to: ``_gather_subentry_options`` ends in ``options.sort`` by label, and both
    the duplicate count and the all-or-nothing rewrite are order-invariant, so
    the two parameters produce byte-identical option lists. It is kept as cheap
    insurance for the day that sort changes, which is the same reason the
    neighbouring resolver tests carry it -- named here so nobody reads a
    measurement into it.
    """

    entry = _EntryStub()
    parked_args = {
        "key": SERVICE_SUBENTRY_KEY,
        "title": "Legacy parked tracker",
        "subentry_type": SUBENTRY_TYPE_TRACKER,
        "visible_device_ids": ["dev-parked"],
        "identity": "parked",
    }
    twin_args = {
        "key": SERVICE_SUBENTRY_KEY,
        "title": "Service",
        "subentry_type": SUBENTRY_TYPE_SERVICE,
        "identity": "service-twin",
    }
    if parked_first:
        parked = entry.add_subentry(**parked_args)
        entry.add_subentry(**twin_args)
    else:
        entry.add_subentry(**twin_args)
        parked = entry.add_subentry(**parked_args)
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Core tracking",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        visible_device_ids=["dev-1"],
        identity="canonical",
    )
    flow = await _build_flow(entry)

    # Precondition, asserted rather than assumed: without the duplicate the
    # rewrite does not run and the key axis would refuse the parked subentry
    # for the wrong reason, leaving this test green on a broken predicate.
    assert {opt.key for opt in flow._gather_subentry_options()} == {
        opt.subentry_id for opt in flow._gather_subentry_options()
    }, "the collision rewrite must have run, otherwise this pins nothing"

    choices, option_map = flow._device_target_choice_map()

    assert parked.subentry_id not in {
        option.subentry_id for option in option_map.values()
    }, (
        "a tracker-typed subentry storing the reserved service key must not be "
        "offered as a device target, whatever the rewrite did to its key"
    )
    # The other half: the canonical tracker stays offered, so the fix refuses
    # the parked subentry rather than emptying the form.
    assert any(
        option.subentry is not None
        and option.subentry.data.get("group_key") == TRACKER_SUBENTRY_KEY
        for option in option_map.values()
    )

    # The sink is the second, independent barrier and reads the same predicate.
    parked_key = next(
        opt.key
        for opt in flow._gather_subentry_options()
        if opt.subentry_id == parked.subentry_id
    )
    assert parked_key not in choices
    changed = await flow._async_assign_devices_to_subentry(entry, parked_key, ["dev-1"])
    assert changed == set(), (
        "even a direct call naming the parked subentry must stand down rather "
        "than write ids the runtime index will drop"
    )


async def test_an_option_without_a_stored_key_is_still_judged_by_its_key() -> None:
    """The ``stored_key`` fallback in the key axis is a guard, not decoration.

    ``_gather_subentry_options`` cannot currently produce an option that has no
    ``stored_key`` while carrying the reserved key: the synthesised fallbacks
    use a tracker key and a subentry without a stored ``group_key`` falls back
    to its ``subentry_id``. The fallback therefore killed no test on its own,
    which by this module's own standard makes it decorative unless pinned. It
    is kept rather than dropped because ``_accepts_device_assignment`` is the
    checkpoint the assignment sink consults as well, and the dataclass already
    has three construction sites; a fourth one that fills ``key`` and forgets
    ``stored_key`` is the shape this guard is for. This test is that pin: it
    constructs the option directly, which is the only way to reach the branch.
    """

    option = config_flow._SubentryOption(
        key=SERVICE_SUBENTRY_KEY,
        label="Hand-built option",
        subentry=None,
        visible_device_ids=(),
    )

    assert config_flow._accepts_device_assignment(option) is False


async def test_the_preselected_group_survives_the_collision_rewrite() -> None:
    """``_default_subentry_key`` is the second reader that asks ``key`` for meaning.

    It answers "which group should the form preselect" with a membership test
    for the tracker key. After the collision rewrite no key equals it any more,
    so the helper falls through to ``next(iter(choices))`` and preselects
    whichever group sorts first by label. A user who opens "move devices",
    picks devices and submits without touching the target field then writes
    them to that group instead of the tracker one.

    Same class as the predicate above and introduced by the same commit
    (``7c799592``), so it is fixed here rather than deferred: the fix that
    leaves a sibling site of its own error class broken is the anti-pattern.
    The alias collision is supplied by two tracker groups sharing one legacy
    ``group_key``, which is the shape the alias rule explicitly permits.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Zulu core tracking",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        visible_device_ids=["dev-1"],
        identity="canonical",
    )
    for suffix in ("one", "two"):
        entry.add_subentry(
            key="alias@example.com",
            title=f"Alpha legacy {suffix}",
            subentry_type=SUBENTRY_TYPE_TRACKER,
            visible_device_ids=[],
            identity=f"legacy-{suffix}",
        )
    flow = await _build_flow(entry)

    options = flow._gather_subentry_options()
    assert {opt.key for opt in options} == {opt.subentry_id for opt in options}, (
        "the collision rewrite must have run, otherwise this pins nothing"
    )

    choices, option_map = flow._device_target_choice_map()
    default_key = flow._default_subentry_key(choices, option_map)

    assert option_map[default_key].stored_key == TRACKER_SUBENTRY_KEY, (
        "the preselected group must still be the tracker group, whatever the "
        "rewrite did to its key"
    )
    # The label ordering is what would win if the helper fell through, so this
    # asserts the fix rather than an accident of the fixture.
    assert default_key != next(iter(choices))


@pytest.mark.parametrize("collides", [False, True], ids=["plain", "collision"])
async def test_the_preselected_group_is_judged_on_both_axes(collides: bool) -> None:
    """The preselection must not fall for a non-device type on the tracker key.

    ``agents/config_flow/AGENTS.md`` forbids the hand-written key comparison
    this helper used to be, because the alias rule lets a legacy subentry keep
    a stored ``group_key`` that disagrees with its type. The fix for the
    collision rewrite replaced one key-only question with another one: the
    loop asked ``stored_key == TRACKER_SUBENTRY_KEY`` and nothing about the
    type, so a ``service``-typed subentry parked on the tracker key wins the
    preselection over the group that actually holds devices.

    Both callers that pass the *unfiltered* ``_subentry_choice_map`` reach it
    (``async_step_settings`` and ``async_step_credentials``), so a user who
    submits either form without touching the group selector writes feature
    flags or refreshes the entry title on a subentry that cannot hold devices.

    The two cases have different provenance and are pinned together because
    one fix answers both:

    ``plain``
        pre-existing. Without a collision the old body's literal test
        ``TRACKER_SUBENTRY_KEY in choices`` matched the parked subentry just
        as the new loop does. Fixing only the loop would move the same wrong
        answer one branch down.
    ``collision``
        introduced here. After the rewrite no key equals the literal any
        more, so the old body fell through to label order -- wrong by
        accident. The loop makes it wrong deterministically, which is worse.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Alpha parked service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=[],
        identity="parked-service",
    )
    entry.add_subentry(
        key="alias@example.com",
        title="Zulu real tracker",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        visible_device_ids=["dev-1"],
        identity="real-tracker",
    )
    if collides:
        entry.add_subentry(
            key="alias@example.com",
            title="Yankee real tracker twin",
            subentry_type=SUBENTRY_TYPE_TRACKER,
            visible_device_ids=[],
            identity="real-tracker-twin",
        )
    flow = await _build_flow(entry)

    options = flow._gather_subentry_options()
    rewritten = {opt.key for opt in options} == {opt.subentry_id for opt in options}
    assert rewritten is collides, (
        "the fixture must produce the rewrite exactly in the collision case, "
        "otherwise the parametrisation pins the same branch twice"
    )

    choices, option_map = flow._subentry_choice_map()
    default_key = flow._default_subentry_key(choices, option_map)

    preselected = option_map.get(default_key)
    assert preselected is not None, "the preselection must name a real option"
    assert config_flow._accepts_device_assignment(preselected), (
        "the preselected group must be able to hold devices; a service-typed "
        "subentry parked on the tracker key must not win on the key axis alone"
    )


@pytest.mark.parametrize(
    ("tracker_key", "service_title"),
    [("owner@example.com", "Alpha service"), ("owner@example.com", "Zulu service")],
    ids=["service-sorts-first", "service-sorts-last"],
)
async def test_the_preselection_is_unchanged_where_nothing_is_parked(
    tracker_key: str, service_title: str
) -> None:
    """The both-axes guard must not move the default where it had no work to do.

    ``async_step_settings`` and ``async_step_credentials`` pass the
    *unfiltered* ``_subentry_choice_map`` and legitimately target the service
    group (``agents/config_flow/AGENTS.md``). The first repair for the parked
    subentry preferred any device-accepting option in its final branch, which
    also fires on an ordinary legacy entry -- a tracker group on an
    email-style key beside a real service group, where nothing is parked at
    all. There the old body answered ``next(iter(choices))``, so the fix
    silently moved the preselection of both steps away from the service group
    for every such installation. Measured: ``'service'`` before, the tracker
    key after, in both label orders.

    Narrowing the branch to the case it was written for restores that.

    Only ``service-sorts-first`` measures the fix; the other case is cheap
    insurance rather than a second measurement, and is labelled as such
    because an undeclared decorative case is the defect this file keeps
    finding. Where the tracker label sorts first, ``next(iter(choices))`` and
    "the first device-accepting option" name the same group, so the case stays
    green even with the branch run unconditionally. It is kept because the old
    answer came from label order, so a future change to the sort would move
    which of the two carries the measurement.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=tracker_key,
        title="Middle devices",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        visible_device_ids=["dev-1"],
        identity="legacy-tracker",
    )
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title=service_title,
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=[],
        identity="service",
    )
    flow = await _build_flow(entry)

    choices, option_map = flow._subentry_choice_map()
    assert not any(
        (opt.stored_key or opt.key) == TRACKER_SUBENTRY_KEY
        for opt in option_map.values()
    ), "the fixture must contain no candidate on the tracker key, else it pins nothing"

    assert flow._default_subentry_key(choices, option_map) == next(iter(choices)), (
        "with nothing parked on the tracker key the preselection must stay the "
        "one the step had before the guard existed"
    )


async def test_a_choice_without_an_option_never_wins_the_preselection() -> None:
    """The two loops of ``_default_subentry_key`` must fail in the same direction.

    Both look the key up in ``option_map`` and both can come back empty, but
    they used to disagree about what that means: the first treated a missing
    option as "cannot judge, skip", the second as "cannot judge, take it".
    A key with no option behind it therefore beat every group that had one,
    including the device-accepting group the second loop exists to find.

    No caller can produce the shape today -- all five build ``choices`` from
    the same map they pass as ``option_map`` -- so this constructs the
    mismatch directly, the way
    ``::test_an_option_without_a_stored_key_is_still_judged_by_its_key``
    reaches the other unreachable guard. The branch is kept rather than
    dropped because it is the cheaper half of a pair: as long as the first
    loop guards, the second must guard too, or the guard reads as a decision
    where it is an inability.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Alpha parked service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=[],
        identity="parked-service",
    )
    entry.add_subentry(
        key="alias@example.com",
        title="Zulu real tracker",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        visible_device_ids=["dev-1"],
        identity="real-tracker",
    )
    flow = await _build_flow(entry)

    real_choices, option_map = flow._subentry_choice_map()
    # The ghost sorts first so the second loop meets it before the group it
    # should answer with; otherwise the assertion would hold by accident.
    choices = {"ghost-key": "A group with no option", **real_choices}
    assert "ghost-key" not in option_map
    assert any(
        (opt.stored_key or opt.key) == TRACKER_SUBENTRY_KEY
        and not config_flow._accepts_device_assignment(opt)
        for opt in option_map.values()
    ), (
        "the fixture must park a non-device group on the tracker key, else the second loop never runs"
    )

    default_key = flow._default_subentry_key(choices, option_map)

    assert default_key != "ghost-key", (
        "a choice the helper cannot judge must not be preferred over one it can"
    )
    preselected = option_map.get(default_key)
    assert preselected is not None and config_flow._accepts_device_assignment(
        preselected
    ), "the preselection must still name the group that can hold devices"


async def test_the_synthesised_fallback_does_not_borrow_a_rewritten_key() -> None:
    """``_unclaimed_fallback_key`` is the third reader the rewrite disarms.

    Its docstring promises a key "no existing option already holds", and the
    call site feeds it ``{option.key for option in all_options}``. Once any
    two options collide, every one of those keys is a ``subentry_id``, so the
    set it searches no longer contains a single stored ``group_key`` and the
    fallback happily takes ``core_tracking`` back from a subentry that stores
    it. Same class and same introducing commit (``7c799592``) as the predicate
    and the preselection, which is why it is closed here rather than deferred.

    The shape is two ``service``-typed subentries parked on the tracker key:
    both are filtered out of the target list, so the fallback is synthesised,
    and their duplicate key triggers the rewrite. Measured without the fix:
    the fallback offers ``core_tracking`` while a real subentry stores it;
    with a single such subentry it correctly steps aside to
    ``core_tracking_2``, which is the answer this test pins for both.

    No live loss follows today, because ``_async_assign_devices_to_subentry``
    resolves by option identity rather than by key and stands down; that is
    the second half of the repair this helper belongs to, and it is exactly
    why the borrow must not be left to it alone.
    """

    entry = _EntryStub()
    for identity in ("parked-one", "parked-two"):
        entry.add_subentry(
            key=TRACKER_SUBENTRY_KEY,
            title=f"Parked {identity}",
            subentry_type=SUBENTRY_TYPE_SERVICE,
            visible_device_ids=[],
            identity=identity,
        )
    flow = await _build_flow(entry)

    all_options = flow._gather_subentry_options()
    assert {opt.key for opt in all_options} == {
        opt.subentry_id for opt in all_options
    }, "the collision rewrite must have run, otherwise this pins nothing"
    stored = {opt.stored_key for opt in all_options if opt.stored_key}
    assert stored == {TRACKER_SUBENTRY_KEY}

    _choices, option_map = flow._device_target_choice_map()
    synthesised = [key for key, opt in option_map.items() if opt.subentry is None]
    assert synthesised, "no real option accepts devices, so one must be synthesised"

    assert not stored.intersection(synthesised), (
        "the synthesised fallback must not take a key a real subentry stores, "
        "whatever the rewrite did to that subentry's identity"
    )


@pytest.mark.parametrize(
    "with_typed_tracker",
    [True, False],
    ids=["typed-tracker-present", "no-typed-tracker"],
)
async def test_a_parked_tracker_key_prefers_the_typed_tracker_group(
    with_typed_tracker: bool,
) -> None:
    """The parked branch preselects on the type axis, not on label order.

    The branch already existed and already answered "the first option that
    accepts devices", which is label order wearing a predicate. Where a
    ``service``-typed subentry sits on the tracker key, the group the form
    means to offer is the one whose *type* says tracker, and on a legacy entry
    that group wears an email-style ``group_key`` -- so the key axis cannot
    name it and the fold cannot either: ``_canonical_core_key_of`` returns
    ``None`` for a tracker on its own key, deliberately, because several
    tracker groups with distinct keys are a supported shape. A direct type
    comparison is therefore the only reader that can identify it, which is why
    this does not route through the shared helper.

    The boundary of the existing branch is left exactly where
    ``::test_the_preselection_is_unchanged_where_nothing_is_parked`` put it.
    Measured rather than assumed: extending the *first* loop with the same
    type axis -- the literal shape the plan proposed -- turns that pin red
    (``'owner@example.com'`` where it demands ``'service'``) and takes
    ``::test_the_preselected_group_survives_the_collision_rewrite`` with it,
    because nothing is parked there and the branch has no work to do. The axis
    belongs inside the parked case, not in front of it.

    ``no-typed-tracker`` is not decoration: it exercises the fallback, which
    keeps the older guarantee that the preselection accepts devices even where
    no candidate carries the tracker type at all. Without it the type
    preference would read as a requirement.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Alpha parked service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=[],
        identity="parked-service",
    )
    entry.add_subentry(
        key="alias@example.com",
        title="Bravo untyped legacy",
        subentry_type=None,
        visible_device_ids=["dev-1"],
        identity="untyped-legacy",
    )
    if with_typed_tracker:
        entry.add_subentry(
            key="owner@example.com",
            title="Zulu legacy tracker",
            subentry_type=SUBENTRY_TYPE_TRACKER,
            visible_device_ids=["dev-2"],
            identity="legacy-tracker",
        )
    flow = await _build_flow(entry)

    choices, option_map = flow._subentry_choice_map()
    default_key = flow._default_subentry_key(choices, option_map)
    preselected = option_map.get(default_key)
    assert preselected is not None

    first_device_accepting = next(
        key
        for key in choices
        if (opt := option_map.get(key)) is not None
        and config_flow._accepts_device_assignment(opt)
    )

    if with_typed_tracker:
        assert preselected.stored_key == "owner@example.com", (
            "the group whose type says tracker must win over the one that "
            "merely sorts first among the device-accepting options"
        )
        # Asserts the fix rather than an accident of the fixture: the untyped
        # legacy group is what label order would have handed back.
        assert default_key != first_device_accepting
    else:
        assert default_key == first_device_accepting
        assert config_flow._accepts_device_assignment(preselected), (
            "with no tracker-typed candidate the branch must still fall back "
            "to a group that can hold devices"
        )


@pytest.mark.parametrize(
    ("step", "field", "service_is_offered"),
    [
        ("credentials", "subentry", True),
        ("repairs_move", "target_subentry", False),
    ],
    ids=["credentials", "repairs-move"],
)
async def test_the_shown_form_preselects_the_group_the_helper_chose(
    step: str, field: str, service_is_offered: bool
) -> None:
    """The preselection is pinned where the user meets it: in the form.

    Five tests above call ``_default_subentry_key`` directly, and that is the
    narrower question. A step can compute the right key and still render a
    different one -- pass ``next(iter(choices))`` into the marker, drop the
    argument, rebuild the schema after the call -- and every helper-level test
    stays green while the form a user opens preselects the wrong group. All
    three of those mutations were run against this test and all three turned
    it red, the rebuild included: ``add_suggested_values_to_schema`` binds to
    the real core here and preserves the marker's default through its
    ``copy.copy``, so a rebuild that drops it is visible. The
    submit path makes that costly rather than cosmetic: ``repairs_move`` writes
    the chosen devices to whatever the target field holds when the user submits
    without touching it.

    Two steps rather than one because they reach the helper through different
    choice maps, and ``service_is_offered`` pins that difference instead of
    assuming it: ``credentials`` passes the unfiltered ``_subentry_choice_map``
    and legitimately offers the service group, ``repairs_move`` passes
    ``_device_target_choice_map``, whose filter removes it. A fixture that only
    exercised one of the two would leave the other step's wiring unpinned.

    The shape is deliberately the *first* loop of the helper -- a
    tracker-keyed group that accepts devices, sorting last by label -- and not
    the parked branch: the parked branch is unreachable from ``repairs_move``
    by construction, because "parked" means "refuses devices" and that is
    exactly the filter standing in front of it. Picking the parked shape here
    would have pinned nothing on that half, which the first draft of this test
    did before it was measured.

    ``rendered != first_offered`` is what makes this test about the wiring
    rather than about the fixture: label order hands back ``Alpha`` in both
    steps, so a step that stopped passing the helper's answer along would show
    ``Alpha`` and fail here while every helper-level test stayed green.
    """

    entry = _EntryStub()
    entry.add_subentry(
        key="alias@example.com",
        title="Alpha legacy tracker",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        visible_device_ids=["dev-1"],
        identity="legacy-tracker",
    )
    entry.add_subentry(
        key=SERVICE_SUBENTRY_KEY,
        title="Mike service",
        subentry_type=SUBENTRY_TYPE_SERVICE,
        visible_device_ids=[],
        identity="service",
    )
    entry.add_subentry(
        key=TRACKER_SUBENTRY_KEY,
        title="Zulu core tracking",
        subentry_type=SUBENTRY_TYPE_TRACKER,
        visible_device_ids=["dev-2"],
        identity="canonical",
    )
    entry.runtime_data.coordinator.data = [
        {"device_id": "dev-1", "name": "Device 1"},
        {"device_id": "dev-2", "name": "Device 2"},
    ]
    flow = await _build_flow(entry)

    form = await getattr(flow, f"async_step_{step}")()
    assert form["type"] == "form", f"{step} did not render a form"

    offered_in_order = _offered_keys_in_order(form, field)
    # Each step's own map, not one map for both: ``repairs_move`` renders out
    # of ``_device_target_choice_map``, and resolving its keys through the
    # unfiltered ``_subentry_choice_map`` would make the ``service_is_offered``
    # assertion below fail open -- it would also pass if *no* offered key
    # resolved at all, which is the opposite of what it claims to pin.
    option_map = (
        flow._subentry_choice_map()[1]
        if step == "credentials"
        else flow._device_target_choice_map()[1]
    )
    assert set(offered_in_order) <= set(option_map), (
        f"{step} offers keys its own choice map cannot resolve: "
        f"{sorted(set(offered_in_order) - set(option_map))}"
    )

    offered_service = {
        key
        for key in offered_in_order
        if option_map[key].stored_key == SERVICE_SUBENTRY_KEY
    }
    assert bool(offered_service) is service_is_offered, (
        "the two steps must keep reaching the helper through different choice "
        "maps, otherwise this parametrisation pins one wiring twice"
    )

    rendered = _rendered_default(form, field)
    assert rendered in set(offered_in_order), (
        "a preselection outside the offered set renders a form the user cannot "
        "submit unchanged"
    )

    preselected = option_map.get(str(rendered))
    assert preselected is not None
    assert preselected.stored_key == TRACKER_SUBENTRY_KEY, (
        "the shown form must preselect the group the helper chose, not "
        "whichever group sorts first"
    )
    assert rendered != offered_in_order[0]
