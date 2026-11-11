"""Reconfigure flow integration tests."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from homeassistant import config_entries

from custom_components.googlefindmy import _async_setup_new_subentries, config_flow
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    CONF_OAUTH_TOKEN,
    DATA_AUTH_METHOD,
    DEFAULT_ENABLE_STATS_ENTITIES,
    DEFAULT_GOOGLE_HOME_FILTER_ENABLED,
    OPT_ENABLE_STATS_ENTITIES,
    OPT_GOOGLE_HOME_FILTER_ENABLED,
    OPT_MAP_VIEW_TOKEN_EXPIRATION,
)
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.config_flow import (
    ConfigEntriesDomainUniqueIdLookupMixin,
    attach_config_entries_flow_manager,
    set_config_flow_unique_id,
)
from tests.test_config_flow_subentry_sync import _ConfigEntriesManagerStub


def _build_reconfigure_flow(entry: SimpleNamespace) -> config_flow.ConfigFlow:
    flow = config_flow.ConfigFlow()
    hass = SimpleNamespace()

    class _ConfigEntries(ConfigEntriesDomainUniqueIdLookupMixin):
        def __init__(self) -> None:
            attach_config_entries_flow_manager(self)

        def async_entries(self, domain: str) -> list[Any]:
            assert domain == config_flow.DOMAIN
            return [entry]

        def async_get_entry(self, entry_id: str) -> SimpleNamespace | None:
            if entry_id == entry.entry_id:
                return entry
            return None

    hass.config_entries = _ConfigEntries()
    hass.tasks: list[asyncio.Task[Any]] = []

    def _async_create_task(coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        hass.tasks.append(task)
        return task

    hass.async_create_task = _async_create_task

    flow.hass = hass  # type: ignore[assignment]
    flow.context = {
        "source": getattr(config_entries, "SOURCE_RECONFIGURE", "reconfigure"),
        "entry_id": entry.entry_id,
    }
    set_config_flow_unique_id(flow, None)
    flow._auth_data = {
        CONF_GOOGLE_EMAIL: entry.data[CONF_GOOGLE_EMAIL],
        CONF_OAUTH_TOKEN: "token",
        DATA_AUTH_METHOD: config_flow._AUTH_METHOD_INDIVIDUAL,
    }
    return flow


@pytest.mark.asyncio
async def test_reconfigure_flow_skips_already_configured_abort() -> None:
    """Reconfigure source should route to async_step_reconfigure without aborting."""

    entry = make_config_entry(
        entry_id="entry-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"
    entry.unique_id = entry.data[CONF_GOOGLE_EMAIL]

    flow = _build_reconfigure_flow(entry)

    async def _fake_reconfigure(
        self: config_flow.ConfigFlow, user_input: dict[str, Any] | None = None
    ) -> dict[str, str]:
        return {"type": "form", "step_id": "reconfigure"}

    flow.async_step_reconfigure = _fake_reconfigure.__get__(
        flow, config_flow.ConfigFlow
    )  # type: ignore[assignment]

    result = await flow.async_step_user()

    assert result["type"] == "form"
    assert result["step_id"] == "reconfigure"


@pytest.mark.asyncio
async def test_reconfigure_reload_recreates_subentries_and_platforms() -> None:
    """Reload after reconfigure should recreate subentries with stable IDs."""

    entry = make_config_entry(
        entry_id="entry-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"
    flow = config_flow.ConfigFlow()
    hass = SimpleNamespace()
    hass.config_entries = _ConfigEntriesManagerStub(entry)
    hass.config_entries.forward_setup_calls: list[
        tuple[SimpleNamespace, tuple[str, ...]]
    ] = []
    hass.config_entries.setup_calls = []
    hass.verify_event_loop_thread = lambda *_args, **_kwargs: None

    async def _forward_setups(
        entry_to_forward: SimpleNamespace, platforms: tuple[str, ...]
    ) -> None:
        hass.config_entries.forward_setup_calls.append(
            (entry_to_forward, tuple(platforms))
        )

    hass.config_entries.async_forward_entry_setups = _forward_setups  # type: ignore[attr-defined]
    hass.data = {config_flow.DOMAIN: {"entries": {entry.entry_id: entry}}}
    hass.async_create_task = asyncio.create_task
    flow.hass = hass  # type: ignore[assignment]
    flow.context = {"entry_id": entry.entry_id}
    flow._auth_data = {
        DATA_AUTH_METHOD: "manual",
        CONF_OAUTH_TOKEN: "token",
        CONF_GOOGLE_EMAIL: entry.data[CONF_GOOGLE_EMAIL],
    }
    flow._available_devices = [("Device", "dev-1")]
    set_config_flow_unique_id(flow, None)
    context_map = flow._ensure_subentry_context()

    await flow._async_sync_feature_subentries(  # type: ignore[attr-defined]
        entry,
        options_payload={
            OPT_MAP_VIEW_TOKEN_EXPIRATION: False,
            OPT_GOOGLE_HOME_FILTER_ENABLED: False,
            OPT_ENABLE_STATS_ENTITIES: True,
        },
        defaults={
            OPT_GOOGLE_HOME_FILTER_ENABLED: DEFAULT_GOOGLE_HOME_FILTER_ENABLED,
            OPT_ENABLE_STATS_ENTITIES: DEFAULT_ENABLE_STATS_ENTITIES,
        },
        context_map=context_map,
    )

    await _async_setup_new_subentries(flow.hass, entry, entry.subentries.values())

    created_ids = [
        payload["config_subentry_id"] for payload in hass.config_entries.created
    ]
    setup_calls_first = list(hass.config_entries.setup_calls)
    assert setup_calls_first, "Subentry setup should record config_subentry_id"
    assert setup_calls_first == created_ids

    expected_platforms = (
        "binary_sensor",
        "button",
        "device_tracker",
        "sensor",
        "switch",
    )
    assert hass.config_entries.forward_setup_calls == [(entry, expected_platforms)]

    hass.config_entries.forward_setup_calls.clear()

    entry.subentries.clear()
    hass.config_entries.created.clear()
    hass.config_entries.setup_calls.clear()

    await flow._async_sync_feature_subentries(  # type: ignore[attr-defined]
        entry,
        options_payload={
            OPT_MAP_VIEW_TOKEN_EXPIRATION: False,
            OPT_GOOGLE_HOME_FILTER_ENABLED: True,
            OPT_ENABLE_STATS_ENTITIES: True,
        },
        defaults={
            OPT_GOOGLE_HOME_FILTER_ENABLED: DEFAULT_GOOGLE_HOME_FILTER_ENABLED,
            OPT_ENABLE_STATS_ENTITIES: DEFAULT_ENABLE_STATS_ENTITIES,
        },
        context_map=context_map,
    )

    recreated_ids = [
        payload["config_subentry_id"] for payload in hass.config_entries.created
    ]

    await _async_setup_new_subentries(flow.hass, entry, entry.subentries.values())

    setup_calls_second = list(hass.config_entries.setup_calls)
    assert setup_calls_second == recreated_ids
    assert hass.config_entries.forward_setup_calls == [(entry, expected_platforms)]

    assert recreated_ids == created_ids
    assert setup_calls_first == created_ids


@pytest.mark.asyncio
async def test_reconfigure_reload_logs_false(caplog: pytest.LogCaptureFixture) -> None:
    """Warn when a synchronous reload returns False after reconfigure."""

    caplog.set_level(logging.WARNING)

    entry = make_config_entry(
        entry_id="entry-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    flow = config_flow.ConfigFlow()

    scheduled: list[str] = []

    class _ConfigEntries:
        def async_reload(self, entry_id: str) -> bool:
            assert entry_id == entry.entry_id
            return False

        def async_schedule_reload(self, entry_id: str) -> None:
            scheduled.append(entry_id)

    hass = SimpleNamespace(config_entries=_ConfigEntries())
    flow.hass = hass  # type: ignore[assignment]

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert any(
        "returned False" in record.message and "deferred" not in record.message
        for record in caplog.records
    )
    assert scheduled == [entry.entry_id]


@pytest.mark.asyncio
async def test_reconfigure_stands_down_when_a_reload_is_already_on_its_way() -> None:
    """A reconfigure rewrites the credential keys the update listener watches.

    That listener reloads the entry so they take effect, and Home Assistant's
    ``async_schedule_reload`` does not coalesce. Whichever side claims the latch
    first reloads; the other must not add a second unload/setup cycle.
    """

    entry = make_config_entry(
        entry_id="entry-latch",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    flow = config_flow.ConfigFlow()

    reloaded: list[str] = []
    scheduled: list[str] = []

    class _ConfigEntries:
        def async_reload(self, entry_id: str) -> bool:
            reloaded.append(entry_id)
            return True

        def async_schedule_reload(self, entry_id: str) -> None:
            scheduled.append(entry_id)

    hass = SimpleNamespace(config_entries=_ConfigEntries(), data={})
    flow.hass = hass  # type: ignore[assignment]

    integration = config_flow.import_integration_package()
    assert integration.claim_pending_entry_reload(hass, entry.entry_id) is True

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert reloaded == [], "a reload is already on its way"
    assert scheduled == [], "and no deferred one may be scheduled either"


@pytest.mark.asyncio
async def test_reconfigure_forces_device_list_refresh() -> None:
    """Mark forced device list refresh on reconfigure and call coordinator hook."""

    entry = make_config_entry(
        entry_id="entry-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    refresh_calls: list[tuple[str | None, bool]] = []
    marked: list[float | None] = []

    def _request_device_list_refresh(
        *, reason: str | None = None, schedule_refresh: bool = True
    ) -> None:
        refresh_calls.append((reason, schedule_refresh))

    def _mark_recent_reconfigure(marker: float | None = None) -> None:
        marked.append(marker)

    coordinator = SimpleNamespace(
        request_device_list_refresh=_request_device_list_refresh,
        mark_recent_reconfigure=_mark_recent_reconfigure,
    )
    runtime = SimpleNamespace(coordinator=coordinator)

    class _ConfigEntries:
        def async_reload(self, entry_id: str) -> bool:
            assert entry_id == entry.entry_id
            return True

    hass = SimpleNamespace(
        data={config_flow.DOMAIN: {"entries": {entry.entry_id: runtime}}},
        config_entries=_ConfigEntries(),
        async_create_task=asyncio.create_task,
    )

    flow = config_flow.ConfigFlow()
    flow.hass = hass  # type: ignore[assignment]
    flow.context = {"entry_id": entry.entry_id}
    flow._auth_data = {
        DATA_AUTH_METHOD: "manual",
        CONF_OAUTH_TOKEN: "token",
        CONF_GOOGLE_EMAIL: entry.data[CONF_GOOGLE_EMAIL],
    }

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert refresh_calls == [("reconfigure", True)]
    assert marked and marked[0] is not None

    pending_refresh = hass.data[config_flow.DOMAIN][
        "pending_reconfigure_device_list_refresh"
    ]
    assert isinstance(pending_refresh, set)
    assert entry.entry_id in pending_refresh

    markers = hass.data[config_flow.DOMAIN]["recent_reconfigure_markers"]
    assert isinstance(markers, dict)
    assert entry.entry_id in markers


@pytest.mark.asyncio
async def test_reconfigure_reload_logs_deferred_failures(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Warn when a deferred reload resolves to False."""

    caplog.set_level(logging.WARNING)

    entry = make_config_entry(
        entry_id="entry-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    flow = config_flow.ConfigFlow()
    hass = SimpleNamespace(tasks=[])
    scheduled: list[str] = []

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            assert entry_id == entry.entry_id
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _retry() -> bool:
                return False

            return _retry()

        def async_schedule_reload(self, entry_id: str) -> None:
            scheduled.append(entry_id)

    hass.config_entries = _ConfigEntries()

    def _async_create_task(coro: Any) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        hass.tasks.append(task)
        return task

    hass.async_create_task = _async_create_task

    def _immediate_call_later(_hass: Any, _delay: Any, callback: Callable[[Any], None]):
        callback(None)

    monkeypatch.setattr(config_flow, "async_call_later", _immediate_call_later)

    flow.hass = hass  # type: ignore[assignment]

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]
    await asyncio.gather(*hass.tasks)

    assert any(
        "returned False" in record.message and "deferred" in record.message
        for record in caplog.records
    )
    assert scheduled == [entry.entry_id, entry.entry_id]


@pytest.mark.asyncio
async def test_reconfigure_reload_logs_when_scheduler_missing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Emit an error when a deferred reload cannot be scheduled."""

    caplog.set_level(logging.ERROR)

    entry = make_config_entry(
        entry_id="entry-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    flow = config_flow.ConfigFlow()
    hass = SimpleNamespace()

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            assert entry_id == entry.entry_id
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _retry() -> bool:
                return True

            return _retry()

    hass.config_entries = _ConfigEntries()

    def _immediate_call_later(_hass: Any, _delay: Any, callback: Callable[[Any], None]):
        callback(None)

    monkeypatch.setattr(config_flow, "async_call_later", _immediate_call_later)

    flow.hass = hass  # type: ignore[assignment]

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert any("could not be scheduled" in record.message for record in caplog.records)


def _latch_is_free(hass: Any, entry_id: str) -> bool:
    """Whether the shared reload latch of ``entry_id`` can be claimed again."""

    integration = config_flow.import_integration_package()
    return bool(integration.claim_pending_entry_reload(hass, entry_id))


def _reconfigure_flow_with_latch(
    entry: Any, config_entries_obj: Any
) -> config_flow.ConfigFlow:
    """A reconfigure flow whose hass carries a real ``data`` bucket.

    The latch lives in ``hass.data``; without that bucket ``_claim_entry_reload``
    falls back to "reload anyway" and a release could not be observed at all.
    """

    flow = config_flow.ConfigFlow()
    hass = SimpleNamespace()
    hass.data = {}
    hass.config_entries = config_entries_obj
    flow.hass = hass  # type: ignore[assignment]
    return flow


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_scheduler_fails(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Both fallbacks failing means no reload arrives, so the claim must go back.

    A claim is a promise to reload, and the release points (unload, setup, entry
    removal) only run once a reload actually happened. Keeping the latch here
    would silence every later credential reload of this entry for the lifetime
    of the process.
    """

    entry = make_config_entry(
        entry_id="entry-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def async_reload(self, entry_id: str) -> Any:
            raise config_flow.OperationNotAllowed()

        def async_schedule_reload(self, entry_id: str) -> None:
            raise RuntimeError("scheduler is unhappy")

    flow = _reconfigure_flow_with_latch(entry, _ConfigEntries())
    monkeypatch.setattr(
        config_flow,
        "async_call_later",
        lambda _hass, _delay, callback: callback(None),
    )

    with caplog.at_level(logging.WARNING):
        await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert any(
        "Failed to schedule reload after reconfigure" in record.getMessage()
        for record in caplog.records
    )
    assert any(
        "was rejected by Home Assistant" in record.getMessage()
        for record in caplog.records
    )
    assert _latch_is_free(flow.hass, entry.entry_id)


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_deferred_reload_dies(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The deferred retry is the last chance; its failure ends the promise."""

    entry = make_config_entry(
        entry_id="entry-2",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()
            raise RuntimeError("the entry is gone")

    flow = _reconfigure_flow_with_latch(entry, _ConfigEntries())
    monkeypatch.setattr(
        config_flow,
        "async_call_later",
        lambda _hass, _delay, callback: callback(None),
    )

    with caplog.at_level(logging.ERROR):
        await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert any(
        "Deferred reload after reconfigure" in record.getMessage()
        for record in caplog.records
    )
    assert _latch_is_free(flow.hass, entry.entry_id)


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_reload_raises_outright(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Anything but the documented rejection kills this path along with the claim."""

    entry = make_config_entry(
        entry_id="entry-3",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def async_reload(self, entry_id: str) -> Any:
            raise RuntimeError("the core is unwell")

    flow = _reconfigure_flow_with_latch(entry, _ConfigEntries())
    monkeypatch.setattr(
        config_flow,
        "async_call_later",
        lambda _hass, _delay, callback: callback(None),
    )

    with pytest.raises(RuntimeError, match="the core is unwell"):
        await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert _latch_is_free(flow.hass, entry.entry_id)


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_deferred_task_is_cancelled(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A cancelled reload task is a dead end like any other.

    ``Task.result()`` raises ``CancelledError`` for a cancelled task, and that
    derives from ``BaseException``: a handler catching ``Exception`` would let
    it sail past and the promise would stay open. The done-callback therefore
    asks whether the task was cancelled before it takes the result.
    """

    entry = make_config_entry(
        entry_id="entry-4",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _CancelledTask:
        """Drives the done-callback synchronously with a cancelled outcome."""

        def cancelled(self) -> bool:
            return True

        def result(self) -> Any:
            raise asyncio.CancelledError

        def add_done_callback(self, callback: Callable[[Any], None]) -> None:
            callback(self)

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _reload() -> bool:
                return True

            return _reload()

    flow = _reconfigure_flow_with_latch(entry, _ConfigEntries())

    def _create_task(coro: Any) -> Any:
        # The reload never runs; close the coroutine so Python does not warn
        # about it, and hand back a task that has already been cancelled.
        coro.close()
        return _CancelledTask()

    flow.hass.async_create_task = _create_task  # type: ignore[attr-defined]
    monkeypatch.setattr(
        config_flow,
        "async_call_later",
        lambda _hass, _delay, callback: callback(None),
    )

    with caplog.at_level(logging.DEBUG, logger=config_flow._LOGGER.name):
        await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert any("was cancelled" in record.getMessage() for record in caplog.records)
    assert _latch_is_free(flow.hass, entry.entry_id)


@pytest.mark.asyncio
async def test_reconfigure_stands_down_for_an_entry_that_is_already_hopeless() -> None:
    """A reconfigure never claims the latch when no reload can reach a setup.

    Inverted from the characterization test that pinned the previous defect. Its
    docstring foresaw only one way out, a check on the entry *after* a truthy
    reload; the way actually taken is the check *before* the claim, and it turns
    this expectation around just the same. The contract paragraph in
    ``agents/runtime_patterns/AGENTS.md`` is corrected in the same change, as
    that docstring demanded.

    The mechanics it described still hold and are the reason for the gate. If
    the entry is disabled while the form stands open, the core sets
    ``disabled_by`` before it reloads, ``ConfigEntry.async_unload`` returns
    ``True`` for the already unloaded entry without running our
    ``async_unload_entry``, and ``async_reload`` returns that truthy unload
    result without calling ``async_setup``. No release point fires, and the
    result cannot be told apart from a reload that landed, which is why the
    question has to be asked before the promise is made rather than after.

    What the gate does *not* cover is an entry that becomes hopeless between the
    check and the reload; see the residual test below.
    """

    entry = make_config_entry(
        entry_id="entry-disabled-in-between",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="not_loaded",
        source="user",
        disabled_by="user",
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def __init__(self) -> None:
            self.reloads: list[str] = []
            self.scheduled: list[str] = []

        async def async_reload(self, entry_id: str) -> bool:
            # What the core returns for a disabled entry: the truthy unload
            # result, with no setup behind it.
            self.reloads.append(entry_id)
            return True

        def async_schedule_reload(self, entry_id: str) -> None:
            self.scheduled.append(entry_id)

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    # No reload was attempted at all, on either lever: a reload that cannot
    # reach a setup is not worth an unload, and the write has already landed.
    assert manager.reloads == []
    assert manager.scheduled == []
    # And no promise was made, so there is nothing that could be stranded.
    assert _latch_is_free(flow.hass, entry.entry_id), (
        "the gate stands down before the claim, so the latch must stay free"
    )


@pytest.mark.asyncio
async def test_reconfigure_strands_the_latch_when_the_entry_is_disabled_in_flight() -> (
    None
):
    """Characterization of the residual the pre-claim gate cannot close.

    The gate reads the entry *before* the claim while the reload runs after it,
    so an entry that is still healthy at the check can be disabled in between:
    ``ConfigEntries.async_set_disabled_by`` sets the field before it reloads, and
    the flow abort it triggers filters on ``SOURCE_REAUTH`` and therefore cancels
    no reconfigure flow. The reload then returns the truthy unload result without
    reaching a setup, no release point fires, and the claim stays behind.

    This is bounded, not silent: the latch is a per-entry set, so nothing but
    this entry is blocked, and the claim is handed back as soon as the entry is
    switched on again (the setup releases it at its head) or removed. Closing it
    outright would need a latch that knows *who* holds it, because releasing a
    claim after a truthy reload cannot tell "we still hold it" from "the unload
    already released it and somebody else claimed". That is a separate change
    with its own contract discussion, not a line in this one.

    Should the latch ever gain owner semantics, this test turns red, and the
    contract paragraph has to be corrected in the same change.
    """

    entry = make_config_entry(
        entry_id="entry-disabled-in-flight",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="not_loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def __init__(self) -> None:
            self.reloads: list[str] = []
            self.scheduled: list[str] = []

        async def async_reload(self, entry_id: str) -> bool:
            self.reloads.append(entry_id)
            # The core disables the entry before it reloads, so by the time the
            # unload half has run the field is set and ``async_reload`` returns
            # the truthy unload result without calling ``async_setup``.
            entry.disabled_by = "user"
            return True

        def async_schedule_reload(self, entry_id: str) -> None:
            self.scheduled.append(entry_id)

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    # The gate let it through, because at that moment nothing was hopeless.
    assert manager.reloads == [entry.entry_id]
    assert manager.scheduled == []
    assert not _latch_is_free(flow.hass, entry.entry_id), (
        "the residual: a truthy reload without a setup leaves the claim behind, "
        "and this path cannot safely release it without owner semantics"
    )


@pytest.mark.asyncio
async def test_reconfigure_keeps_the_latch_when_the_scheduler_took_the_reload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful hand-off to the core scheduler is no dead end.

    ``async_schedule_reload`` puts the reload into a core-owned task, and that
    task will reach the unload which releases the latch. Releasing here as well
    would leave the entry unlatched while its reload is still on its way, and
    the next claimant would queue a second teardown: exactly what the latch
    exists to prevent. The deferred retry may therefore fail without giving the
    promise back.
    """

    entry = make_config_entry(
        entry_id="entry-5",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def __init__(self) -> None:
            self.scheduled: list[str] = []

        def async_reload(self, entry_id: str) -> Any:
            # Both the first attempt and the deferred retry are rejected.
            raise config_flow.OperationNotAllowed()

        def async_schedule_reload(self, entry_id: str) -> None:
            self.scheduled.append(entry_id)

    entries = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, entries)
    monkeypatch.setattr(
        config_flow,
        "async_call_later",
        lambda _hass, _delay, callback: callback(None),
    )

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert entries.scheduled == [entry.entry_id]
    assert not _latch_is_free(flow.hass, entry.entry_id), (
        "the scheduled reload owns the promise; handing the latch back here "
        "would let a second teardown in behind it"
    )


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_retry_cannot_be_armed(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failure to arm the retry is not caught by the sibling handler.

    ``async_call_later`` sits inside the ``except OperationNotAllowed`` block,
    and Python does not consult further ``except`` clauses for an error raised
    from within a handler. Without its own guard, a hass whose loop is already
    closed (shutdown during a reconfigure) would leave the claim behind for the
    life of the process.
    """

    entry = make_config_entry(
        entry_id="entry-6",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def async_reload(self, entry_id: str) -> Any:
            raise config_flow.OperationNotAllowed()

    flow = _reconfigure_flow_with_latch(entry, _ConfigEntries())

    def _cannot_arm(_hass: Any, _delay: Any, _callback: Any) -> None:
        raise RuntimeError("event loop is closed")

    monkeypatch.setattr(config_flow, "async_call_later", _cannot_arm)

    with caplog.at_level(logging.ERROR, logger=config_flow._LOGGER.name):
        await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert any("could not be armed" in record.getMessage() for record in caplog.records)
    assert _latch_is_free(flow.hass, entry.entry_id)


@pytest.mark.asyncio
async def test_reconfigure_skips_a_task_handle_that_cannot_report_its_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A handle without ``cancelled()`` gets no callback instead of an AttributeError.

    Both call sites take whatever the task runner hands back. Asking only for
    ``add_done_callback`` would attach a callback whose very first statement
    raises into the loop, and the latch would stay claimed: the state this path
    exists to avoid.
    """

    entry = make_config_entry(
        entry_id="entry-7",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _HalfTask:
        """Carries the callback protocol but cannot report its outcome."""

        def __init__(self) -> None:
            self.callbacks: list[Any] = []

        def add_done_callback(self, callback: Any) -> None:
            self.callbacks.append(callback)

    handle = _HalfTask()

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _reload() -> bool:
                return True

            return _reload()

    flow = _reconfigure_flow_with_latch(entry, _ConfigEntries())

    def _create_task(coro: Any) -> Any:
        coro.close()
        return handle

    flow.hass.async_create_task = _create_task  # type: ignore[attr-defined]
    monkeypatch.setattr(
        config_flow,
        "async_call_later",
        lambda _hass, _delay, callback: callback(None),
    )

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert handle.callbacks == [], (
        "a handle that cannot answer must not be given a callback that would "
        "raise into the loop"
    )


@pytest.mark.asyncio
async def test_reconfigure_uses_the_loop_when_hass_cannot_create_tasks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop fallback has to gate its callback like the primary path does.

    A hass without ``async_create_task`` (a slim stub, or a shutdown-time
    object) falls through to ``loop.create_task``. That branch attaches the same
    done-callback and therefore needs the same handle check; a real task passes
    it, which is what this pins.
    """

    entry = make_config_entry(
        entry_id="entry-8",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    reloaded: list[str] = []

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _reload() -> bool:
                reloaded.append(entry_id)
                return True

            return _reload()

    flow = _reconfigure_flow_with_latch(entry, _ConfigEntries())
    # No ``async_create_task`` on this hass, only a loop.
    flow.hass.loop = asyncio.get_running_loop()  # type: ignore[attr-defined]
    monkeypatch.setattr(
        config_flow,
        "async_call_later",
        lambda _hass, _delay, callback: callback(None),
    )

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]
    for _ in range(3):
        await asyncio.sleep(0)

    assert reloaded == [entry.entry_id]
    assert not _latch_is_free(flow.hass, entry.entry_id), (
        "the reload arrived; unload and setup release the latch themselves"
    )


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_hand_off_is_refused() -> None:
    """A refused hand-off at the end of the path is a dead end like any other.

    When the direct reload returns ``False``, this path asks the core scheduler
    to take over. If that hand-off is refused -- no ``async_schedule_reload`` on
    the manager, or the call raises -- nothing else follows, so the promise has
    to go back. Keeping it would silence every later credential write for this
    entry until Home Assistant restarts.
    """

    entry = make_config_entry(
        entry_id="entry-refused-1",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        """No ``async_schedule_reload`` at all: the hand-off cannot be taken."""

        def __init__(self) -> None:
            self.reloads: list[str] = []

        async def async_reload(self, entry_id: str) -> bool:
            self.reloads.append(entry_id)
            return False

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert manager.reloads == [entry.entry_id]
    assert _latch_is_free(flow.hass, entry.entry_id), (
        "the reload failed and nobody took over, so the claim must go back"
    )


@pytest.mark.asyncio
async def test_reconfigure_keeps_the_latch_when_the_hand_off_is_accepted() -> None:
    """The counterpart: an accepted hand-off is no dead end and keeps the claim.

    The core task that ``async_schedule_reload`` creates will reach the unload
    that releases the latch. Releasing here as well would leave the entry
    unlatched while its reload is still on its way, and the next claimant would
    queue a second teardown.
    """

    entry = make_config_entry(
        entry_id="entry-refused-2",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def __init__(self) -> None:
            self.reloads: list[str] = []
            self.scheduled: list[str] = []

        async def async_reload(self, entry_id: str) -> bool:
            self.reloads.append(entry_id)
            return False

        def async_schedule_reload(self, entry_id: str) -> None:
            self.scheduled.append(entry_id)

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert manager.scheduled == [entry.entry_id]
    assert not _latch_is_free(flow.hass, entry.entry_id), (
        "the scheduler owns the reload now; its unload releases the latch"
    )


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_deferred_hand_off_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deferred retry is the last chance, and a refused hand-off ends it.

    First attempt rejected, deferred retry returns ``False``, and no scheduler is
    there to take over. Nothing follows, so the promise goes back.
    """

    entry = make_config_entry(
        entry_id="entry-refused-3",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()
            # Plain value, not an awaitable: this is the synchronous half of the
            # deferred retry.
            return False

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    def _immediate_call_later(
        _hass: Any, _delay: Any, callback: Callable[[Any], None]
    ) -> None:
        callback(None)

    monkeypatch.setattr(config_flow, "async_call_later", _immediate_call_later)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert manager.calls == 2, "the deferred retry has to have run"
    assert _latch_is_free(flow.hass, entry.entry_id), (
        "the last chance failed and nobody took over, so the claim must go back"
    )


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_deferred_task_hand_off_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Same dead end, reached through the deferred *task* rather than a value.

    If the deferred retry returns an awaitable, its result is read in a done
    callback. A ``False`` there with a refused hand-off is the end of the path
    just the same.
    """

    entry = make_config_entry(
        entry_id="entry-refused-4",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    tasks: list[asyncio.Task[Any]] = []

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _retry() -> bool:
                return False

            return _retry()

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    def _async_create_task(coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        task = asyncio.create_task(coro)
        tasks.append(task)
        return task

    flow.hass.async_create_task = _async_create_task  # type: ignore[attr-defined]

    def _immediate_call_later(
        _hass: Any, _delay: Any, callback: Callable[[Any], None]
    ) -> None:
        callback(None)

    monkeypatch.setattr(config_flow, "async_call_later", _immediate_call_later)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]
    await asyncio.gather(*tasks)
    # Done callbacks run through the loop, so yield once before observing.
    await asyncio.sleep(0)

    assert manager.calls == 2, "the deferred retry has to have run"
    assert _latch_is_free(flow.hass, entry.entry_id), (
        "the deferred task ended without a reload and nobody took over"
    )


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_task_handle_cannot_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A handle that cannot report its outcome is a dead end, not a hand-off.

    ``_handle_reports_its_outcome`` promises to stand down together with the
    callback when the handle carries neither ``add_done_callback`` nor
    ``cancelled``. Without that, the reload runs but nobody watches how it ends,
    and a task that ends without reloading keeps the promise open for the life of
    the process.
    """

    entry = make_config_entry(
        entry_id="entry-mute-handle",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    closed: list[str] = []

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _retry() -> bool:
                return True

            return _retry()

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    def _mute_create_task(coro: Any, *, name: str | None = None) -> Any:
        # A handle without the future protocol: the reload is under way, but its
        # outcome is unobservable from here.
        closed.append("scheduled")
        task = asyncio.ensure_future(coro)
        return SimpleNamespace(_task=task)

    flow.hass.async_create_task = _mute_create_task  # type: ignore[attr-defined]

    def _immediate_call_later(
        _hass: Any, _delay: Any, callback: Callable[[Any], None]
    ) -> None:
        callback(None)

    monkeypatch.setattr(config_flow, "async_call_later", _immediate_call_later)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]
    await asyncio.sleep(0)

    assert closed == ["scheduled"], "the deferred retry has to have been scheduled"
    assert _latch_is_free(flow.hass, entry.entry_id), (
        "nobody can observe how this reload ends, so the promise must not stay open"
    )


@pytest.mark.asyncio
async def test_reconfigure_gives_the_latch_back_when_the_loop_handle_cannot_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The loop fallback needs the same stand-down as the task path.

    Where ``hass`` carries no ``async_create_task``, the deferred retry goes
    through ``loop.create_task``. A handle from there that cannot report its
    outcome is the same dead end, and the fallback must not be the one branch
    that keeps the promise open.
    """

    entry = make_config_entry(
        entry_id="entry-mute-loop",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    scheduled: list[str] = []

    class _ConfigEntries:
        def __init__(self) -> None:
            self.calls = 0

        def async_reload(self, entry_id: str) -> Any:
            self.calls += 1
            if self.calls == 1:
                raise config_flow.OperationNotAllowed()

            async def _retry() -> bool:
                return True

            return _retry()

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)

    class _Loop:
        def create_task(self, coro: Any, *, name: str | None = None) -> Any:
            scheduled.append(name or "unnamed")
            task = asyncio.ensure_future(coro)
            # Deliberately not the task itself: a handle without the future
            # protocol is what this branch has to survive.
            return SimpleNamespace(_task=task)

    flow.hass.loop = _Loop()  # type: ignore[attr-defined]

    def _immediate_call_later(
        _hass: Any, _delay: Any, callback: Callable[[Any], None]
    ) -> None:
        callback(None)

    monkeypatch.setattr(config_flow, "async_call_later", _immediate_call_later)

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]
    await asyncio.sleep(0)

    assert scheduled, "the loop fallback has to have scheduled the retry"
    assert _latch_is_free(flow.hass, entry.entry_id), (
        "the loop fallback must stand down just like the task path"
    )


@pytest.mark.asyncio
async def test_reconfigure_keeps_a_newer_claim_after_a_refused_hand_off() -> None:
    """A refused hand-off must not discard somebody else's claim.

    A falsy ``async_reload`` result has more than one cause, and two of them mean
    one of our lifecycle hooks already handed the latch back: a failed unload
    releases in ``async_unload_entry``, a failed setup at the head of
    ``async_setup_entry``. Between that release and the refused hand-off another
    writer can claim the latch. Releasing blindly there would discard *their*
    promise, and the next caller would queue the second teardown this latch
    exists to prevent -- ``discard_pending_entry_reload`` is a bare
    ``set.discard`` and cannot tell whose claim it drops.

    Modelled with the real integration helpers rather than a hand-rolled set, so
    the test cannot drift from the latch it is about: the reload releases like the
    unload half would and a stranger claims immediately afterwards.
    """

    entry = make_config_entry(
        entry_id="entry-newer-claim",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    integration = config_flow.import_integration_package()

    class _ConfigEntries:
        """Falsy reload, no scheduler: the hand-off cannot be taken."""

        def __init__(self) -> None:
            self.reloads: list[str] = []
            self.hass: Any = None

        async def async_reload(self, entry_id: str) -> bool:
            self.reloads.append(entry_id)
            # What the unload half does on the way out, then a stranger.
            integration.discard_pending_entry_reload(self.hass, entry_id)
            assert integration.claim_pending_entry_reload(self.hass, entry_id), (
                "the stranger has to get the latch for this test to mean anything"
            )
            return False

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)
    manager.hass = flow.hass
    # The component is loaded, which is how the classifier sees that a release
    # point ran: without it the answer falls back to "still ours" and releases.
    flow.hass.config = SimpleNamespace(components={entry.domain})  # type: ignore[attr-defined]

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert manager.reloads == [entry.entry_id]
    assert not _latch_is_free(flow.hass, entry.entry_id), (
        "the stranger's claim must survive our refused hand-off"
    )


@pytest.mark.asyncio
async def test_reconfigure_releases_when_no_release_point_ran() -> None:
    """The counterpart: where no release point ran, the claim is still ours.

    Same refused hand-off, but the component never came up, so neither our unload
    nor our setup head fired and the latch is the one this path took. Keeping it
    would swallow every later reload of the entry, so it has to go back. This is
    the branch that proves the new check is a *classifier* and not a blanket
    "never release".
    """

    entry = make_config_entry(
        entry_id="entry-still-ours",
        title="Find My",
        subentries={},
        runtime_data=SimpleNamespace(),
        state="loaded",
        source="user",
        disabled_by=None,
    )
    entry.data[CONF_GOOGLE_EMAIL] = "existing@example.com"

    class _ConfigEntries:
        def __init__(self) -> None:
            self.reloads: list[str] = []

        async def async_reload(self, entry_id: str) -> bool:
            self.reloads.append(entry_id)
            return False

        def async_get_entry(self, entry_id: str) -> Any:
            return entry if entry_id == entry.entry_id else None

    manager = _ConfigEntries()
    flow = _reconfigure_flow_with_latch(entry, manager)
    # Readable component list that does *not* contain the domain: proof that the
    # reload took the "component could not be set up" short circuit.
    flow.hass.config = SimpleNamespace(components={"persistent_notification"})  # type: ignore[attr-defined]

    await flow._async_reload_entry_after_reconfigure(entry)  # type: ignore[attr-defined]

    assert manager.reloads == [entry.entry_id]
    assert _latch_is_free(flow.hass, entry.entry_id), (
        "no release point ran, so the claim this path took must go back"
    )
