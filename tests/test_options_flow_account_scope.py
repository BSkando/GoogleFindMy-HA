# tests/test_options_flow_account_scope.py
"""Settings and credentials are account-wide; their forms must not pretend otherwise.

Both options steps used to offer a "feature group" selector (field ``subentry``).
Neither step's effect depends on it: settings are stored on ``entry.options``
and credentials on ``entry.data`` for the whole account, whichever group is
picked. The selector only decided which subentry received a copy of some
options (``feature_flags``, which no runtime code evaluates) and a refreshed
``entry_title`` (setup writes it as well), and a mere change of the selection
scheduled a full reload of every entity of the account.

Every absence asserted here is paired with a presence, so a test does not go
green merely because it stopped looking: a form must still offer its remaining
fields, a write must still land where it belongs, a reload must still happen
when an option changes (T3, with the same doubles T4 uses), every other label
must stay, and two controls show the recorders are not blind: one drives a real
visibility write through the settings doubles
(``test_the_subentry_recorders_see_a_real_subentry_write``), the other drives
the production write helper into the credentials recorder
(``test_the_credentials_subentry_recorder_sees_the_production_helper``).

Deliberately a new file: ``tests/test_options_flow_credentials_cache.py`` is on
the ``LEGACY_ALLOWLIST`` of ``test_guard_asyncio_run_antipattern.py``, and the
two neighbouring files keep their own scope. Entry, hass and latch doubles are
imported from them rather than copied; the read-only ``subentries`` wrapper
(``tests/AGENTS.md`` rule 10) and the credentials subentry recorder are local,
because no neighbour provides them.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from homeassistant.config_entries import ConfigSubentry

from custom_components.googlefindmy import config_flow
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    OPT_CONTRIBUTOR_MODE,
    OPT_IGNORED_DEVICES,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
    TRACKER_SUBENTRY_KEY,
)
from tests.test_options_flow_credentials_reload import (
    _DummyHass,
    _LatchRecorder,
    _make_entry,
    _MemoryCache,
)
from tests.test_options_flow_subentries import (
    _build_flow,
    _EntryStub,
    _latch_is_free,
)

pytestmark = pytest.mark.asyncio

# The doubles are duck-typed stand-ins for Home Assistant objects. Each scoped
# ``type: ignore[...]`` in this file sits on a line that hands a double to a
# slot typed for the real object, which mypy cannot accept without a cast that
# would hide the same fact; the neighbouring options-flow tests use the same
# markers for the same reason.

_FIELD = "subentry"
_PACKAGE = Path(config_flow.__file__).resolve().parent


def _shown_keys(result: dict[str, Any]) -> set[str]:
    """Return the field names a shown form renders, read from its schema."""

    assert result["type"] == "form", result
    return {str(marker) for marker in result["data_schema"].schema}


def _expected_settings_keys() -> set[str]:
    """Return the option fields the settings form must keep offering.

    Spelled out field by field rather than read from production, and mirrors
    the optional-constant guards of ``async_step_settings`` (the three names
    can be ``None`` on an old core). This is the presence half of the form
    tests: a form that lost a real option fails here just like one that still
    carries the selector.
    """

    keys = {
        config_flow.OPT_LOCATION_POLL_INTERVAL,
        config_flow.OPT_DEVICE_POLL_DELAY,
        config_flow.OPT_MAP_VIEW_TOKEN_EXPIRATION,
        config_flow.OPT_MAP_VIEW_ENABLED,
        config_flow.OPT_DELETE_CACHES_ON_REMOVE,
        config_flow.OPT_CONTRIBUTOR_MODE,
        config_flow.OPT_STALE_THRESHOLD,
        config_flow.OPT_SHOW_LOCATION_AGE,
        config_flow.OPT_SPEED_GATE_ENABLED,
        config_flow.OPT_ROUNDTRIP_CONFIRM,
        config_flow.OPT_ACCURACY_GATE_ENABLED,
        config_flow.SECRETS_EXTRA_WATCH_PATHS,
    }
    for optional in (
        config_flow.OPT_GOOGLE_HOME_FILTER_ENABLED,
        config_flow.OPT_GOOGLE_HOME_FILTER_KEYWORDS,
        config_flow.OPT_ENABLE_STATS_ENTITIES,
    ):
        if optional is not None:
            keys.add(optional)
    return keys


# --- Positive controls: the recorders are not blind --------------------------


async def test_the_subentry_recorders_see_a_real_subentry_write() -> None:
    """The settings doubles record a subentry write the production flow makes.

    Control for the ``manager.updated == []`` assertions below: visibility keeps
    its group selector on purpose and writes ``visible_device_ids`` into the
    chosen group through the same manager double. If this goes red, an empty
    recorder elsewhere in this file proves nothing.
    """

    entry = _EntryStub()
    sub = entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    entry.options = {OPT_IGNORED_DEVICES: {"dev-1": {"name": "Device 1"}}}
    flow = await _build_flow(entry)

    shown = await flow.async_step_visibility()
    assert _FIELD in _shown_keys(shown)

    result = await flow.async_step_visibility(
        {_FIELD: TRACKER_SUBENTRY_KEY, "unignore_devices": ["dev-1"]}
    )

    assert result["type"] == "create_entry"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert [subentry_id for subentry_id, _ in manager.updated] == [sub.subentry_id]


class _SubentryWriteRecorder:
    """Callable stand-in for ``async_update_subentry`` that keeps every call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(
        self,
        entry: Any,
        subentry: ConfigSubentry,
        *,
        data: dict[str, Any] | None = None,
        title: str | None = None,
        unique_id: str | None = None,
        translation_key: str | None = None,
    ) -> bool:
        self.calls.append((subentry.subentry_id, dict(data or {})))
        return True


def _credentials_world(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Any, _DummyHass, _SubentryWriteRecorder, _LatchRecorder]:
    """Build a loaded entry, its hass double, a subentry recorder and a latch."""

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-account-scope", cache=cache)
    # ``tests/AGENTS.md`` rule 10: the core hands ``subentries`` out read-only.
    entry.subentries = MappingProxyType(dict(entry.subentries))
    hass = _DummyHass(entry, cache)
    recorder = _SubentryWriteRecorder()
    manager = hass.config_entries
    manager.async_update_subentry = recorder  # type: ignore[method-assign]
    latch = _LatchRecorder()
    monkeypatch.setattr(
        config_flow, "import_integration_package", lambda: latch, raising=True
    )
    return entry, hass, recorder, latch


def _credentials_flow(entry: Any, hass: _DummyHass) -> config_flow.OptionsFlowHandler:
    flow = config_flow.OptionsFlowHandler()
    flow.hass = hass  # type: ignore[assignment]
    flow.config_entry = entry  # type: ignore[attr-defined]
    return flow


async def test_the_credentials_subentry_recorder_sees_the_production_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The credentials recorder catches the helper every subentry write uses.

    Control for ``subentry_updates == []`` in the credential success test:
    ``ConfigFlow._async_update_subentry`` is the one door subentry writes go
    through, including the title refresh this change removes.
    """

    entry, hass, recorder, _latch = _credentials_world(monkeypatch)
    flow = _credentials_flow(entry, hass)
    subentry = next(iter(entry.subentries.values()))

    outcome = config_flow.ConfigFlow._async_update_subentry(
        flow,  # type: ignore[arg-type]
        entry,
        subentry,
        data={"group_key": "probe"},
        title="probe",
        unique_id=subentry.unique_id,
    )
    if inspect.isawaitable(outcome):
        await outcome

    assert recorder.calls == [(subentry.subentry_id, {"group_key": "probe"})]


# --- T1, T2: the forms offer no group field, and keep everything else -------


async def test_settings_form_offers_no_feature_group_field() -> None:
    """T1: exactly the account options, no ``subentry`` marker."""

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    flow = await _build_flow(entry)

    result = await flow.async_step_settings()

    assert result["step_id"] == "settings"
    assert _shown_keys(result) == _expected_settings_keys()


@pytest.mark.parametrize("with_selector", [True, False], ids=["selector", "plain"])
async def test_credentials_form_offers_no_feature_group_field(
    monkeypatch: pytest.MonkeyPatch, with_selector: bool
) -> None:
    """T2: both schema branches offer the secrets field and nothing else.

    ``new_oauth_token`` stays disabled in both branches; asserting the exact
    set also pins that it did not come back with this change.
    """

    if not with_selector:
        monkeypatch.setattr(config_flow, "selector", None)
    entry, hass, _recorder, _latch = _credentials_world(monkeypatch)
    flow = _credentials_flow(entry, hass)

    result = await flow.async_step_credentials()

    assert result["step_id"] == "credentials"
    assert _shown_keys(result) == {"new_secrets_json"}


# --- T3, T4: settings write account-wide and reload only for a real change ---


async def test_settings_change_writes_no_subentry_and_reloads_once() -> None:
    """T3: a changed option lands on the entry, reloads once, touches no group."""

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    flow = await _build_flow(entry)

    result = await flow.async_step_settings(
        {OPT_MAP_VIEW_TOKEN_EXPIRATION: True, OPT_CONTRIBUTOR_MODE: "high_traffic"}
    )

    assert result["type"] == "create_entry"
    # Presence: the options are stored account-wide.
    assert result["data"][OPT_MAP_VIEW_TOKEN_EXPIRATION] is True
    assert result["data"][OPT_CONTRIBUTOR_MODE] == "high_traffic"
    manager = flow.hass.config_entries  # type: ignore[assignment]
    # Presence: the reload the options need, both sides (``tests/AGENTS.md`` 8).
    assert manager.scheduled_reloads == [entry.entry_id]
    assert manager.reloads == []
    # Absence: no group receives a copy.
    assert manager.updated == []


async def test_settings_confirm_without_edit_neither_writes_nor_reloads_even_with_stale_subentry_metadata() -> (
    None
):
    """T4: confirming unchanged options costs nothing, whatever the group holds.

    The group holds what a previous save left there, with a stale
    ``entry_title``. Before the fix, the step rewrote the group on every
    save, and that stale title alone made it also schedule a reload of every
    entity of the account (measured: the same fixture with a current title
    rewrote the group but scheduled no reload). The settled options
    and the synchronised group data come from a throwaway entry with its own
    double, so the shared latch is not left claimed (``tests/AGENTS.md`` rule 8
    names the trap).
    The presence half is the save itself (same payload returned) and T3, which
    proves the same doubles do schedule a reload once an option changes.
    """

    user_input = {
        OPT_MAP_VIEW_TOKEN_EXPIRATION: True,
        OPT_CONTRIBUTOR_MODE: "high_traffic",
    }
    throwaway = _EntryStub()
    synced = throwaway.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    settled = await (await _build_flow(throwaway)).async_step_settings(dict(user_input))
    assert settled["type"] == "create_entry"
    options = dict(settled["data"])

    entry = _EntryStub()
    subentry = entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    # Whatever the throwaway save left in its group, then only the title stale.
    # At the base that is the full copy (flags, filter, current title); after
    # the fix the save leaves the group alone, so only the stub's own
    # ``group_key`` and empty ``feature_flags`` are there.
    subentry.data = MappingProxyType(
        {**dict(synced.data), "entry_title": "Stale title"}
    )
    entry.options = options
    flow = await _build_flow(entry)

    result = await flow.async_step_settings(dict(user_input))

    assert result["type"] == "create_entry"
    assert dict(result["data"]) == options
    manager = flow.hass.config_entries  # type: ignore[assignment]
    assert manager.scheduled_reloads == []
    assert manager.reloads == []
    assert _latch_is_free(flow.hass, entry.entry_id) is True
    assert manager.updated == []
    assert subentry.data["entry_title"] == "Stale title"


# --- T5: credentials are stored for the account and touch no group -----------


async def test_credentials_success_does_not_touch_any_subentry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T5: the token lands on the entry, the reload runs, no group is written."""

    async def _fake_pick(
        _hass: Any,
        _email: str,
        candidates: list[tuple[str, str]],
        *,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> str | None:
        return candidates[0][1] if candidates else None

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)
    entry, hass, recorder, latch = _credentials_world(monkeypatch)
    flow = _credentials_flow(entry, hass)

    bundle = {
        "google_email": entry.data[CONF_GOOGLE_EMAIL],
        "oauth_token": "oauth-token-account-scope-123456",
        "shared_key": "DDEEFF",
    }
    outcome = await flow.async_step_credentials(
        {"new_secrets_json": json.dumps(bundle)}
    )
    if inspect.isawaitable(outcome):
        outcome = await outcome
    await hass.settle()

    assert outcome["type"] == "abort"
    assert outcome["reason"] == "reconfigure_successful"
    # Presence: stored on the entry, reload claimed and run as before.
    assert entry.data[CONF_OAUTH_TOKEN] == "oauth-token-account-scope-123456"
    assert latch.claims == [entry.entry_id]
    assert hass.config_entries.reloads == [entry.entry_id]
    # Absence: no group receives a title refresh.
    assert recorder.calls == []


# --- T6: the translations follow the forms -----------------------------------


# The descriptions the settings form keeps, measured identical in all eleven
# locale files; spelled out so a fix that drops the whole block fails here.
_SETTINGS_DESCRIPTIONS = {
    "accuracy_gate_enabled",
    "contributor_mode",
    "delete_caches_on_remove",
    "map_view_enabled",
    "map_view_token_expiration",
    "roundtrip_confirm_enabled",
    "secrets_extra_watch_paths",
    "show_location_age",
    "speed_gate_enabled",
    "stale_threshold",
}


def _locale_files() -> list[Path]:
    return [
        _PACKAGE / "strings.json",
        *sorted((_PACKAGE / "translations").glob("*.json")),
    ]


async def test_settings_and_credentials_translations_carry_no_feature_group_label() -> (
    None
):
    """T6: no group label on the two forms, every other label kept.

    The presence half guards against removing too much: visibility keeps its
    selector label and description, the shared ``invalid_subentry`` error
    stays, and the settings and credentials forms keep their remaining labels.
    """

    files = _locale_files()
    # Vacuum guard: strings.json plus the ten shipped locales.
    assert len(files) == 11
    for path in files:
        steps = json.loads(path.read_text(encoding="utf-8"))["options"]["step"]
        errors = json.loads(path.read_text(encoding="utf-8"))["options"]["error"]
        for step in ("settings", "credentials"):
            assert _FIELD not in steps[step].get("data", {}), (path.name, step)
            assert _FIELD not in steps[step].get("data_description", {}), (
                path.name,
                step,
            )
        assert _FIELD in steps["visibility"]["data"], path.name
        assert _FIELD in steps["visibility"]["data_description"], path.name
        assert "invalid_subentry" in errors, path.name
        assert "new_secrets_json" in steps["credentials"]["data"], path.name
        assert set(steps["settings"]["data"]) == _expected_settings_keys(), path.name
        assert (
            set(steps["settings"].get("data_description", {})) - {_FIELD}
            == _SETTINGS_DESCRIPTIONS
        ), path.name


# --- T7: a stale submission carrying the old field is refused ----------------


async def test_a_stale_subentry_key_is_rejected_by_the_form_schema() -> None:
    """T7: the shown schema refuses ``subentry`` and still accepts the options.

    Characterises the schema contract after the removal: a client that still
    sends ``subentry`` to a freshly shown settings form is refused with an
    extra-key error. Forms opened before an update are not affected, because
    Home Assistant keeps flows in memory only and the update needs a restart.
    """

    entry = _EntryStub()
    entry.add_subentry(key=TRACKER_SUBENTRY_KEY, title="Core")
    flow = await _build_flow(entry)
    schema = (await flow.async_step_settings())["data_schema"]

    # Presence: a regular submission passes the same schema.
    assert (
        schema({OPT_MAP_VIEW_TOKEN_EXPIRATION: True})[OPT_MAP_VIEW_TOKEN_EXPIRATION]
        is True
    )
    with pytest.raises(config_flow.vol.Invalid):
        schema({_FIELD: TRACKER_SUBENTRY_KEY})
