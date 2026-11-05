# tests/test_options_flow_credentials_reload.py
"""The credential step must not promise a reload that cannot arrive.

Claiming the shared reload latch is a *promise to reload*, and the release
points (unload, setup, entry removal) all presuppose that a reload actually
arrives. Where it cannot -- a terminal lifecycle state, a disabled entry, an
ignored one -- the promise must not be made at all: the latch would stay set
for the life of the process and every later credential write would stand down
with its own credentials ineffective.

Deliberately a new file rather than an addition to
``tests/test_options_flow_credentials_cache.py``: that one is on the
``LEGACY_ALLOWLIST`` of ``test_guard_asyncio_run_antipattern.py`` and drives its
coroutines through ``asyncio.run``. Appending here would slip past the
file-set guard while still violating ``tests/AGENTS.md``.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import json
from collections.abc import Awaitable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from homeassistant.config_entries import ConfigEntryState, ConfigSubentry
from homeassistant.helpers import frame

from custom_components.googlefindmy import config_flow
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    DOMAIN,
    SERVICE_SUBENTRY_KEY,
    SUBENTRY_TYPE_SERVICE,
    SUBENTRY_TYPE_TRACKER,
    TRACKER_SUBENTRY_KEY,
)
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.config_flow import prepare_flow_hass_config_entries

pytestmark = pytest.mark.asyncio


class _MemoryCache:
    """In-memory token cache implementing the contract the flow uses."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    async def get(self, name: str) -> Any:
        return self._data.get(name)

    async def async_set_cached_value(self, name: str, value: Any) -> None:
        self.set(name, value)

    def set(self, name: str, value: Any) -> None:
        if value is None:
            self._data.pop(name, None)
        else:
            self._data[name] = value


@dataclass
class _RuntimeData:
    """Runtime data stub exposing the cache the credential step clears."""

    token_cache: _MemoryCache

    @property
    def cache(self) -> _MemoryCache:
        return self.token_cache


def _make_entry(
    *,
    entry_id: str,
    cache: _MemoryCache,
    state: Any = ConfigEntryState.LOADED,
    disabled_by: str | None = None,
    source: str = "user",
) -> SimpleNamespace:
    """Build the canonical entry stub with the fields the state gate reads.

    ``state``, ``source`` and ``disabled_by`` are not decoration:
    ``_entry_reload_is_hopeless`` reads all three through ``getattr(..., None)``
    and fails open, so an entry stub missing them answers "not hopeless"
    unconditionally and a test claiming to exercise the gate exercises nothing
    (``tests/AGENTS.md``, options-flow reload doubles).
    """

    entry = make_config_entry(
        entry_id=entry_id,
        data={
            CONF_GOOGLE_EMAIL: "user@example.com",
            CONF_OAUTH_TOKEN: "oauth-original-token-123456",
        },
        state=state,
        source=source,
        disabled_by=disabled_by,
        title="user@example.com",
        runtime_data=_RuntimeData(cache),
    )
    entry.subentries = {}
    for key, subentry_type, title in (
        (SERVICE_SUBENTRY_KEY, SUBENTRY_TYPE_SERVICE, "Service"),
        (TRACKER_SUBENTRY_KEY, SUBENTRY_TYPE_TRACKER, "Google Find My devices"),
    ):
        data: dict[str, Any] = {"group_key": key}
        if key == TRACKER_SUBENTRY_KEY:
            data["feature_flags"] = {}
        subentry = ConfigSubentry(
            data=data,
            subentry_type=subentry_type,
            title=title,
            unique_id=f"{entry_id}-{key}",
            subentry_id=f"{entry_id}-{key}-subentry",
        )
        entry.subentries[subentry.subentry_id] = subentry
    return entry


class _DummyConfigEntries:
    """The config-entries surface the credential step touches."""

    def __init__(self, entry: SimpleNamespace) -> None:
        self._entry = entry
        self.reloads: list[str] = []
        self.updated_payloads: list[dict[str, Any]] = []
        # The awaited variant is what this step uses; the synchronous recorder
        # sits next to it as a tripwire so an assertion can tell a silent
        # fall-back to ``async_schedule_reload`` from the intended path.
        self.scheduled_reloads: list[str] = []
        self.reload_result: Any = True
        self.reload_raises: BaseException | None = None

    def async_get_entry(self, entry_id: str) -> SimpleNamespace | None:
        return self._entry if entry_id == self._entry.entry_id else None

    def async_get_subentries(self, entry_id: str) -> list[ConfigSubentry]:
        entry = self.async_get_entry(entry_id)
        return list(entry.subentries.values()) if entry is not None else []

    def async_update_entry(
        self, entry: SimpleNamespace, *, data: dict[str, Any]
    ) -> None:
        entry.data = data
        self.updated_payloads.append(data)

    def async_update_subentry(
        self,
        entry: SimpleNamespace,
        subentry: ConfigSubentry,
        *,
        data: dict[str, Any] | None = None,
        title: str | None = None,
        unique_id: str | None = None,
        translation_key: str | None = None,
    ) -> None:
        if title is not None:
            subentry.title = title

    def async_schedule_reload(self, entry_id: str) -> None:
        self.scheduled_reloads.append(entry_id)

    async def async_reload(self, entry_id: str) -> Any:
        self.reloads.append(entry_id)
        if self.reload_raises is not None:
            raise self.reload_raises
        return self.reload_result


class _DummyHass:
    """Home Assistant stub collecting the tasks the step fires and forgets."""

    def __init__(self, entry: SimpleNamespace, cache: _MemoryCache) -> None:
        prepare_flow_hass_config_entries(
            self,
            lambda: _DummyConfigEntries(entry),
            frame_module=frame,
        )
        self.data: dict[str, Any] = {
            DOMAIN: {"entries": {entry.entry_id: _RuntimeData(cache)}}
        }
        self.tasks: list[asyncio.Task[Any]] = []

    def async_create_task(self, coro: Awaitable[Any]) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        self.tasks.append(task)
        return task

    async def settle(self) -> None:
        """Let the fired tasks finish, including their done-callbacks.

        ``gather`` returns the instant a task is done, which is not the instant
        every done-callback has run: the release callback and gather's own are
        both queued through ``loop.call_soon``. The extra turn removes the
        dependency on their registration order; without it the latch assertions
        are flaky.
        """

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        await asyncio.sleep(0)
        self.tasks.clear()


class _LatchRecorder:
    """Stand-in for the integration package's reload latch.

    Records every question asked of it, which is what lets a test assert that
    the latch was *never claimed* rather than the far weaker "was free
    afterwards" -- the latter is true with and without the gate, because the
    release callback hands a stranded claim back as soon as the loop turns.
    """

    def __init__(self, *, granted: bool = True) -> None:
        self.claims: list[str] = []
        self.discards: list[str] = []
        self._granted = granted

    def claim_pending_entry_reload(self, _hass: Any, entry_id: str) -> bool:
        self.claims.append(entry_id)
        return self._granted

    def discard_pending_entry_reload(self, _hass: Any, entry_id: str) -> None:
        self.discards.append(entry_id)


@pytest.fixture(name="latch")
def _latch_fixture(monkeypatch: pytest.MonkeyPatch) -> _LatchRecorder:
    recorder = _LatchRecorder()
    monkeypatch.setattr(
        config_flow, "import_integration_package", lambda: recorder, raising=True
    )
    return recorder


async def _rotate(
    monkeypatch: pytest.MonkeyPatch,
    hass: _DummyHass,
    entry: SimpleNamespace,
    token: str = "oauth-token-rotate-123456",
) -> Any:
    """Drive the credential step to its success path and return its result."""

    async def _fake_pick(
        _hass: Any,
        _email: str,
        candidates: list[tuple[str, str]],
        *,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> str | None:
        return candidates[0][1] if candidates else None

    monkeypatch.setattr(config_flow, "async_pick_working_token", _fake_pick)

    flow = config_flow.OptionsFlowHandler()
    flow.hass = hass  # type: ignore[assignment]
    flow.config_entry = entry  # type: ignore[attr-defined]

    outcome = await flow.async_step_credentials({"new_oauth_token": token})
    if inspect.isawaitable(outcome):
        outcome = await outcome
    await hass.settle()
    return outcome


# --- T-N1 to T-N4: the gate refuses the doomed reload ------------------------


async def test_a_terminal_entry_gets_no_doomed_reload_task(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """No task is fired for an entry whose reload can only raise.

    ``FAILED_UNLOAD`` is not recoverable, so ``async_unload`` rejects it with
    ``OperationNotAllowed`` and the fire-and-forget task dies with it.
    """

    cache = _MemoryCache()
    entry = _make_entry(
        entry_id="entry-terminal", cache=cache, state=ConfigEntryState.FAILED_UNLOAD
    )
    hass = _DummyHass(entry, cache)

    await _rotate(monkeypatch, hass, entry)

    assert hass.config_entries.reloads == []
    assert hass.config_entries.scheduled_reloads == [], (
        "not a silent fall-back to the scheduling variant either"
    )
    assert hass.tasks == []


async def test_a_terminal_entry_never_claims_the_latch(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """The latch is never *asked for*, not merely free again afterwards.

    The weaker phrasing would be mutation-blind: without the gate the claim is
    taken, the task dies, and ``_release_claim_when_reload_fails`` hands the
    latch back on the next loop turn -- so "free afterwards" holds either way.
    """

    cache = _MemoryCache()
    entry = _make_entry(
        entry_id="entry-terminal-latch",
        cache=cache,
        state=ConfigEntryState.MIGRATION_ERROR,
    )
    hass = _DummyHass(entry, cache)

    await _rotate(monkeypatch, hass, entry)

    assert latch.claims == []
    assert latch.discards == []


async def test_a_terminal_entry_is_warned_about_not_whispered(
    monkeypatch: pytest.MonkeyPatch,
    latch: _LatchRecorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The stranded credentials are logged at warning level, with the reason."""

    cache = _MemoryCache()
    entry = _make_entry(
        entry_id="entry-loud", cache=cache, state=ConfigEntryState.FAILED_UNLOAD
    )
    hass = _DummyHass(entry, cache)

    with caplog.at_level("WARNING", logger=config_flow._LOGGER.name):
        await _rotate(monkeypatch, hass, entry)

    warnings = [rec for rec in caplog.records if rec.levelname == "WARNING"]
    assert warnings, "a silent stand-down leaves no trace at all"
    message = warnings[-1].getMessage()
    assert "entry-loud" in message
    assert "failed_unload" in message.lower()


async def test_a_disabled_entry_is_caught_and_named_by_all_three_inputs(
    monkeypatch: pytest.MonkeyPatch,
    latch: _LatchRecorder,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The second damage class, and why the log names more than the state.

    A disabled entry stays ``LOADED``-shaped from the outside and its reload
    even returns the truthy unload result, so neither the state nor the return
    value tells it apart from a reload that landed. A message naming the state
    alone would therefore read as "nothing wrong here" in exactly the case that
    is wrong, which is why all three inputs are logged.
    """

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-off", cache=cache, disabled_by="user")
    hass = _DummyHass(entry, cache)

    with caplog.at_level("WARNING", logger=config_flow._LOGGER.name):
        result = await _rotate(monkeypatch, hass, entry)

    assert latch.claims == [], "a disabled entry never reaches a setup either"
    assert result["reason"] == "credentials_saved_not_reloaded"
    message = [r for r in caplog.records if r.levelname == "WARNING"][-1].getMessage()
    assert "disabled_by=user" in message
    assert "loaded" in message.lower(), (
        "the state is reported as-is; on its own it would be reassuring, "
        "which is the reason it is not reported on its own"
    )


async def test_a_terminal_entry_tells_the_user_the_reload_did_not_happen(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """The user is told the credentials are stored but not yet in effect.

    ``reconfigure_successful`` reads as "the new credentials apply now", and in
    this branch they do not until the entry can be set up again.
    """

    cache = _MemoryCache()
    entry = _make_entry(
        entry_id="entry-message", cache=cache, state=ConfigEntryState.FAILED_UNLOAD
    )
    hass = _DummyHass(entry, cache)

    result = await _rotate(monkeypatch, hass, entry)

    assert result["type"] == "abort"
    assert result["reason"] == "credentials_saved_not_reloaded"
    assert hass.config_entries.updated_payloads, (
        "the credentials are written either way; only the reload is skipped"
    )


# --- T-N5, T-N6, T-N9: the healthy path is untouched -------------------------


async def test_a_loaded_entry_still_reloads(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """The regular path keeps claiming and reloading."""

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-loaded", cache=cache)
    hass = _DummyHass(entry, cache)

    await _rotate(monkeypatch, hass, entry)

    assert latch.claims == ["entry-loaded"]
    assert hass.config_entries.reloads == ["entry-loaded"]


async def test_setup_in_progress_still_reloads_from_the_options_step(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """A transient state is not a hopeless one.

    The rejected alternative -- gating on ``entry.state.recoverable`` -- would
    have dropped this reload silently, which is the regression the positive
    list of terminal states exists to avoid.
    """

    cache = _MemoryCache()
    entry = _make_entry(
        entry_id="entry-transient",
        cache=cache,
        state=ConfigEntryState.SETUP_IN_PROGRESS,
    )
    hass = _DummyHass(entry, cache)

    await _rotate(monkeypatch, hass, entry)

    assert hass.config_entries.reloads == ["entry-transient"]


async def test_a_healthy_entry_keeps_the_plain_success_message(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """Only the defect branch reports differently.

    Pins the branch refinement: the obvious simplification -- always return the
    new key -- turns the regular case into a false statement with the sign
    reversed.
    """

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-happy", cache=cache)
    hass = _DummyHass(entry, cache)

    result = await _rotate(monkeypatch, hass, entry)

    assert result["reason"] == "reconfigure_successful"


# --- T-N7, T-N8: a task that ends without reloading frees the latch ----------


async def test_a_reload_that_returns_false_hands_the_latch_back(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """No exception is not the same as "reloaded".

    ``async_reload`` returns ``False`` without raising when the unload failed
    or the component could not be set up. None of the release points runs then,
    so a latch kept here would be as permanent as after a raising task.
    """

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-falsy", cache=cache)
    hass = _DummyHass(entry, cache)
    hass.config_entries.reload_result = False

    await _rotate(monkeypatch, hass, entry)

    assert latch.claims == ["entry-falsy"]
    assert latch.discards == ["entry-falsy"], (
        "the reload never landed, so the promise has to be given back"
    )


async def test_a_falsy_reload_keeps_the_latch_when_a_release_point_ran(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """Falsy alone is not a dead end, and treating it as one costs more than it saves.

    Of the three ways ``async_reload`` returns falsy, two already passed a
    release point: a failed unload runs our ``async_unload_entry`` whose
    ``finally`` hands the latch back, and a setup returning ``False`` ran our
    ``async_setup_entry`` whose head does the same. Discarding there would drop
    a claim someone else may have taken during that setup -- the very double
    reload this latch exists to prevent. Only a component that could not be set
    up at all leaves the latch behind, and that one is visible from outside.
    """

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-loaded-component", cache=cache)
    hass = _DummyHass(entry, cache)
    hass.config_entries.reload_result = False
    hass.config = SimpleNamespace(components={entry.domain})

    await _rotate(monkeypatch, hass, entry)

    assert latch.claims == ["entry-loaded-component"]
    assert latch.discards == [], (
        "the component is loaded, so our setup ran and released the latch "
        "already; a second discard could take someone else's claim"
    )


async def test_a_cancelled_reload_still_hands_the_latch_back(
    monkeypatch: pytest.MonkeyPatch, latch: _LatchRecorder
) -> None:
    """The pre-existing cancellation branch keeps working."""

    cache = _MemoryCache()
    entry = _make_entry(entry_id="entry-cancelled", cache=cache)
    hass = _DummyHass(entry, cache)
    hass.config_entries.reload_raises = asyncio.CancelledError()

    await _rotate(monkeypatch, hass, entry)

    assert latch.discards == ["entry-cancelled"]


# --- T-N10: the abort reason resolves to a translation key -------------------


def _abort_reasons_of(function_name: str) -> set[str]:
    """Return the literal ``async_abort`` reasons raised inside a function.

    Reads them out of the production module rather than repeating them, so a
    rename in the code cannot pass a test that only knows the old literal.
    """

    tree = ast.parse(Path(config_flow.__file__).read_text(encoding="utf-8"))
    target: ast.AsyncFunctionDef | None = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == function_name:
            target = node
            break
    assert target is not None, (
        f"{function_name} was renamed or removed; this guard can no longer "
        "enumerate the abort reasons it is meant to cover"
    )

    reasons: set[str] = set()
    dynamic = 0
    for sub in ast.walk(target):
        if not (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)):
            continue
        if sub.func.attr != "async_abort":
            continue
        for kw in sub.keywords:
            if kw.arg != "reason":
                continue
            if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                reasons.add(kw.value.value)
            else:
                dynamic += 1
    assert not dynamic, (
        "a non-literal abort reason cannot be checked statically; extend this "
        "guard rather than letting it pass over one silently"
    )
    return reasons


async def test_the_abort_reasons_resolve_to_translation_keys() -> None:
    """Every reason this step returns must exist where the UI looks it up.

    Home Assistant resolves an options-flow abort from the shipped
    ``translations/<lang>.json``, not from ``strings.json``; a reason present
    only in the source strings renders as the raw key. Both are asserted, and
    the reasons come from the code, so this also fails when the branch stops
    using the new key at all.
    """

    reasons = _abort_reasons_of("_finalize_success")
    assert "credentials_saved_not_reloaded" in reasons, (
        "the defect branch no longer reports the stranded credentials"
    )
    assert "reconfigure_successful" in reasons, (
        "the regular path lost its plain success message"
    )

    root = Path(config_flow.__file__).resolve().parent
    for relative in ("strings.json", "translations/en.json"):
        catalogue = json.loads((root / relative).read_text(encoding="utf-8"))
        missing = reasons - set(catalogue["options"]["abort"])
        assert not missing, f"{relative} is missing {sorted(missing)}"
