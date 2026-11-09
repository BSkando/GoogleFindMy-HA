# tests/test_fcm_receiver_ha_short_run_cap.py
"""Defense-2 regression tests for the FCM supervisor short-run crash cap.

Covers PLAN_HOTFIX_FCM_CASCADING_FAILURE AP-2 (`fcm_receiver_ha.py`):
- 7 building-block tests (threshold trigger, break, reset, re-registration,
  per-entry independence, orthogonality to `_fatal_retry_counts`, unload-cleanup)
- 1 boundary test that pins `<` vs. `<=` drift on `_SHORT_RUN_THRESHOLD_S`
- 1 cross-locale symmetry test for the `fcm_short_run_crash_loop` translation key
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.googlefindmy.Auth import fcm_receiver_ha
from custom_components.googlefindmy.Auth.fcm_receiver_ha import (
    _MAX_CONSECUTIVE_SHORT_RUNS,
    _SHORT_RUN_THRESHOLD_S,
    DOMAIN,
    FcmReceiverHA,
)
from tests.helpers.config_entries_stub import make_config_entry


class _MonoClock:
    """Stateful auto-increment monotonic clock for deterministic run-duration math.

    Each call returns ``value`` then advances by ``step``. ``jump(seconds)``
    skips ahead without consuming a call slot (used to simulate a long run
    between ``entry_start`` and the run-duration check).
    """

    def __init__(self, step: float = 0.01) -> None:
        self.value = 0.0
        self.step = step

    def __call__(self) -> float:
        v = self.value
        self.value += self.step
        return v

    def jump(self, seconds: float) -> None:
        self.value += seconds


class _SupervisorPushClientStub:
    """Lightweight ``FcmPushClient`` substitute for supervisor-loop tests.

    Mirrors only the surface the supervisor reads/writes (``start``, ``stop``,
    ``run_state``, ``do_listen``, ``_observe_counter``).

    Two clock-injection hooks model the supervisor's snapshot boundary
    correctly:

    * ``jump_on_start`` advances the clock inside ``start()`` BEFORE the
      supervisor takes the ``entry_start = time.monotonic()`` snapshot in
      ``_start_supervisor_for_entry``. It models
      pre-monitor elapsed time and does NOT influence ``run_duration``.
    * ``jump_on_run_state_read`` advances the clock on the first
      ``pc.run_state`` access INSIDE the monitor loop
      (``getattr(pc, "run_state", None)``), which is
      AFTER the ``entry_start`` snapshot. It is the only correct way to
      inject elapsed-run time that ``run_duration = time.monotonic() -
      entry_start`` actually observes. Use this for boundary math.

    Iter-11 ``become_started``: defaults to ``True`` so the first
    ``run_state`` read returns ``FcmPushClientRunState.STARTED`` (setting
    the supervisor's per-run ``became_started`` latch) and the second
    read returns ``None`` (forcing the ``state is None`` break path on
    the next iteration with ``last_activity is None``). This mirrors the
    poison-message scenario the cap is designed to catch: client
    connects, decodes, dies, repeat. Set ``become_started=False`` to
    model a pre-start failure (MCS unreachable / login failed) where the
    client NEVER reaches ``STARTED`` and the cap MUST NOT advance the
    counter.
    """

    def __init__(
        self,
        *,
        clock: _MonoClock | None = None,
        jump_on_start: float = 0.0,
        jump_on_run_state_read: float = 0.0,
        become_started: bool = True,
        has_been_started: bool | None = None,
        credential_error: object | None = None,
    ) -> None:
        self.clock = clock
        self.jump_on_start = jump_on_start
        self.jump_on_run_state_read = jump_on_run_state_read
        self.become_started = become_started
        # Finding 1: mirror the production ``credential_error`` signal the
        # supervisor reads after the monitor loop ends to escalate corrupt
        # FCM key material to ``_invalidate_fcm_tokens`` + re-registration.
        self.credential_error = credential_error
        # Iter-12 ``has_been_started``: mirrors the production subclass'
        # ``_has_been_started`` latch which is set by the vendored
        # library the moment ``run_state = STARTED`` is assigned,
        # independent of polling cadence. Default mirrors
        # ``become_started`` so existing tests behave as before; pass
        # explicitly to model the micro-window crash scenario
        # (``become_started=False, has_been_started=True``: client
        # reached STARTED and crashed before the first monitor poll)
        # or a true pre-start failure
        # (``become_started=False, has_been_started=False``: client
        # never reached STARTED at all).
        if has_been_started is None:
            has_been_started = become_started
        self._has_been_started = has_been_started
        self.do_listen = True
        self.start_calls = 0
        self.stop_calls = 0
        self._observed_short_run_counter: int | None = None
        self._run_state_reads = 0

    @property
    def run_state(self):  # noqa: D401 - mirror attribute access
        self._run_state_reads += 1
        if (
            self._run_state_reads == 1
            and self.clock is not None
            and self.jump_on_run_state_read
        ):
            self.clock.jump(self.jump_on_run_state_read)
        if self.become_started and self._run_state_reads == 1:
            from custom_components.googlefindmy.Auth.firebase_messaging import (
                FcmPushClientRunState,
            )

            return FcmPushClientRunState.STARTED
        # Implicit ``None`` return forces the ``state is None`` break path
        # in the supervisor monitor loop on the next iteration. With
        # ``become_started=True`` this also gives ``last_activity is
        # None`` break exactly one iteration AFTER ``STARTED`` was
        # observed, which is the poison-message run shape (client
        # connects, no message processed, restart).

    async def start(self) -> None:
        self.start_calls += 1
        if self.clock is not None and self.jump_on_start:
            self.clock.jump(self.jump_on_start)

    async def stop(self) -> None:
        self.stop_calls += 1

    def _observe_counter(self, counter: int) -> None:
        # Last writer wins so tests observe the most recent supervisor pass.
        self._observed_short_run_counter = counter


def _install_supervisor_mocks(
    monkeypatch: pytest.MonkeyPatch,
    receiver: FcmReceiverHA,
    *,
    clock: _MonoClock,
    ensure_clients: list,
) -> AsyncMock:
    """Wire the common supervisor-loop mock surface and return the register mock."""

    monkeypatch.setattr(fcm_receiver_ha.time, "monotonic", clock)

    ensure_iter = iter(ensure_clients)

    async def _ensure(entry_id: str, cache, generation=None):  # noqa: ARG001
        # Codex Iter-7 (PR #1086): modelling exhaustion as ``None`` mirrors
        # production's ``if not pc`` path, which is designed to back off and
        # retry **forever**. The non-cap tests (long-run reset,
        # re-registration, entry-B independence, 30s boundary) all rely on
        # natural supervisor completion after the configured client list is
        # exhausted, so we MUST stop the supervisor explicitly here instead
        # of falling into the retry-forever branch (CA-TEST-TERMINATION-001).
        try:
            return next(ensure_iter)
        except StopIteration:
            stop_evt = receiver._stop_evts.get(entry_id)
            if stop_evt is not None:
                stop_evt.set()
            return None

    monkeypatch.setattr(receiver, "_ensure_client_for_entry", _ensure)

    register_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(receiver, "_register_for_fcm_entry", register_mock)

    monkeypatch.setattr(fcm_receiver_ha.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(fcm_receiver_ha.random, "uniform", lambda *_a, **_kw: 0.0)

    real_wait_for = asyncio.wait_for

    async def _instant_wait_for(fut, *, timeout=None):
        if timeout is not None and timeout > 0:
            coro_name = getattr(fut, "__name__", "") or getattr(
                getattr(fut, "cr_code", None), "co_name", ""
            )
            if coro_name == "wait":
                if asyncio.iscoroutine(fut):
                    fut.close()
                raise TimeoutError
        return await real_wait_for(fut, timeout=timeout)

    monkeypatch.setattr(fcm_receiver_ha.asyncio, "wait_for", _instant_wait_for)
    return register_mock


def _install_ir_capture(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    """Capture ``ir.async_create_issue`` and ``ir.async_delete_issue`` calls."""

    create_issue = MagicMock()
    delete_issue = MagicMock()
    monkeypatch.setattr(fcm_receiver_ha.ir, "async_create_issue", create_issue)
    monkeypatch.setattr(fcm_receiver_ha.ir, "async_delete_issue", delete_issue)
    monkeypatch.setattr(
        fcm_receiver_ha.ir,
        "IssueSeverity",
        SimpleNamespace(WARNING="warning", ERROR="error"),
    )
    return create_issue, delete_issue


# --------------------------------------------------------------------------
# Building blocks
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_short_run_triggers_repair_issue_after_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """10 short runs in one supervisor pass produce exactly one ERROR repair issue."""
    entry_id = "entry-trigger"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)  # 1ms per call -> every run is short
    clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    register_mock = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert register_mock.await_count == _MAX_CONSECUTIVE_SHORT_RUNS
    assert create_issue.call_count == 1
    call = create_issue.call_args
    assert call.args[1] == DOMAIN
    assert call.args[2] == f"fcm_short_run_crash_loop_{entry_id}"
    assert call.kwargs["is_fixable"] is False
    assert call.kwargs["severity"] == fcm_receiver_ha.ir.IssueSeverity.ERROR
    assert call.kwargs["translation_key"] == "fcm_short_run_crash_loop"
    assert receiver._fatal_errors[entry_id].startswith("FCM short-run crash loop")


@pytest.mark.asyncio
async def test_credential_error_invalidates_tokens_and_bypasses_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A listener ``credential_error`` escalates to re-registration, not the cap.

    Codex Finding 1: when the listener reports corrupt FCM key material via
    ``pc.credential_error``, the supervisor must invalidate the bad tokens
    (forcing re-registration with the device identity preserved) instead of
    charging the short run to the crash cap. Even
    ``_MAX_CONSECUTIVE_SHORT_RUNS + 2`` such runs must NOT fire the cap
    repair issue -- corrective action is being taken, this is not a
    poison-message loop.
    """
    entry_id = "entry-cred"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)  # every run is short
    cred_err = ValueError("Credentials key material failed base64 decode")
    clients = [
        _SupervisorPushClientStub(clock=clock, credential_error=cred_err)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS + 2)
    ]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    invalidate_mock = AsyncMock()
    monkeypatch.setattr(receiver, "_invalidate_fcm_tokens", invalidate_mock)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # Every credential-error run escalated to re-registration ...
    assert invalidate_mock.await_count == _MAX_CONSECUTIVE_SHORT_RUNS + 2
    # AP4 (finding 4b): the supervisor passes its captured generation (0 here,
    # no teardown occurred) so a superseded run cannot write back invalidated
    # creds after the tombstone is cleared.
    invalidate_mock.assert_awaited_with(entry_id, 0)
    # ... and NONE of them tripped the short-run crash cap.
    assert create_issue.call_count == 0
    assert entry_id not in receiver._fatal_errors


@pytest.mark.asyncio
async def test_short_run_break_after_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The supervisor task completes after the 10th short run (no further iters)."""
    entry_id = "entry-break"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    # Provide more clients than the threshold to detect "loop kept going".
    clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS + 5)
    ]
    register_mock = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert receiver.supervisors[entry_id].done()
    assert register_mock.await_count == _MAX_CONSECUTIVE_SHORT_RUNS


@pytest.mark.asyncio
async def test_long_run_resets_closure_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """9 short + 1 long + 9 short runs must not trigger the threshold."""
    entry_id = "entry-reset"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    # Build 19 clients: short, short, ..., LONG at index 9, short, short, ...
    # The LONG run must use ``jump_on_run_state_read`` (not ``jump_on_start``)
    # so the 60s elapse AFTER the supervisor captures ``entry_start``
    # (``entry_start = time.monotonic()``); otherwise the jump is consumed by the snapshot itself and
    # ``run_duration`` collapses to ~0, making this run count as short.
    clients = []
    for idx in range(19):
        if idx == 9:
            clients.append(
                _SupervisorPushClientStub(clock=clock, jump_on_run_state_read=60.0)
            )
        else:
            clients.append(_SupervisorPushClientStub(clock=clock))
    register_mock = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # No repair issue: the long run at index 9 resets the counter, so the
    # remaining 9 short runs never reach the threshold of 10.
    assert create_issue.call_count == 0
    assert entry_id not in receiver._fatal_errors
    assert register_mock.await_count == 19


@pytest.mark.asyncio
async def test_re_registration_does_not_inherit_old_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancelled supervisor's counter must not bleed into a fresh supervisor."""
    entry_id = "entry-rereg"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    # First supervisor: 9 short runs then we cancel.
    clients_first = [_SupervisorPushClientStub(clock=clock) for _ in range(9)]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients_first
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    # Run the first supervisor to natural completion (runs out of clients).
    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)
    # Pre-condition: 9 short runs did NOT trigger the cap.
    assert create_issue.call_count == 0
    assert entry_id not in receiver._fatal_errors

    # Remove the spent stop_evt so a fresh supervisor can start.
    receiver._stop_evts.pop(entry_id, None)
    receiver.supervisors.pop(entry_id, None)

    # Second supervisor: 9 more short runs. If the counter were shared, the
    # combined 18 runs would have crossed the threshold and re-fired.
    clients_second = [_SupervisorPushClientStub(clock=clock) for _ in range(9)]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients_second
    )
    create_issue_b, _ = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert create_issue_b.call_count == 0
    assert entry_id not in receiver._fatal_errors


@pytest.mark.asyncio
async def test_per_entry_supervisors_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Entry A crash-loops, entry B stays healthy: only A produces a repair issue."""
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock_a = _MonoClock(step=0.001)
    clock_b = _MonoClock(step=0.001)
    # Entry A: 10 short runs (will trip the cap).
    clients_a = [
        _SupervisorPushClientStub(clock=clock_a)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    # Entry B: 5 healthy long runs (well above threshold).
    # ``jump_on_run_state_read`` (not ``jump_on_start``) is required so the
    # 60s elapse AFTER ``entry_start`` snapshot (``entry_start = time.monotonic()``) and actually mark
    # each run as a long run; otherwise the test passes only by accident
    # because 5 short runs also stay below the cap of 10.
    clients_b = [
        _SupervisorPushClientStub(clock=clock_b, jump_on_run_state_read=60.0)
        for _ in range(5)
    ]

    # Single shared monkeypatch fixture but per-receiver wiring.
    # We can't run both supervisors against the same monkeypatched function,
    # so we sequence A then B.
    register_a = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock_a, ensure_clients=clients_a
    )
    create_issue, _ = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry("entry-a", None)
    await asyncio.wait_for(receiver.supervisors["entry-a"], timeout=5.0)

    register_b = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock_b, ensure_clients=clients_b
    )
    await receiver._start_supervisor_for_entry("entry-b", None)
    await asyncio.wait_for(receiver.supervisors["entry-b"], timeout=5.0)

    # Exactly one issue was raised, and it targets entry A.
    assert create_issue.call_count == 1
    assert create_issue.call_args.args[2] == "fcm_short_run_crash_loop_entry-a"
    assert receiver._fatal_errors.get("entry-a", "").startswith(
        "FCM short-run crash loop"
    )
    assert "entry-b" not in receiver._fatal_errors
    assert register_a.await_count == _MAX_CONSECUTIVE_SHORT_RUNS
    assert register_b.await_count == 5


@pytest.mark.asyncio
async def test_fatal_retry_counts_orthogonal_to_short_run_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_fatal_retry_counts` and the closure-counter advance independently.

    A short run pumps the closure counter; a `FatalRegistrationError` from
    the registration path pumps `_fatal_retry_counts`. They share no state
    and must not double-fire the short-run repair issue.
    """
    from custom_components.googlefindmy.exceptions import FatalRegistrationError

    entry_id = "entry-orthogonal"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    clients = [_SupervisorPushClientStub(clock=clock) for _ in range(6)]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _ = _install_ir_capture(monkeypatch)

    # Override register: first 5 successes (short runs), 6th raises fatal endpoint.
    side_effects: list = [True, True, True, True, True]
    err = FatalRegistrationError("registration failed")
    err.is_auth_error = False  # endpoint path, not auth
    side_effects.append(err)
    register_mock = AsyncMock(side_effect=side_effects)
    monkeypatch.setattr(receiver, "_register_for_fcm_entry", register_mock)

    # 6th iteration's FatalRegistrationError triggers endpoint path
    # which retries up to _MAX_FATAL_ENDPOINT_RETRIES (7) before giving up.
    # Refill register with the same error so the endpoint counter exhausts.
    side_effects.extend([err] * 6)

    # Bounded run via wait_for to avoid hanging on register-retry backoff.
    await receiver._start_supervisor_for_entry(entry_id, None)
    try:
        await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)
    except TimeoutError:  # noqa: PERF203 - test bounded fallback
        receiver._stop_evts[entry_id].set()
        await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # The short-run counter reached 5 (< 10), so no short-run issue.
    short_run_calls = [
        c
        for c in create_issue.call_args_list
        if c.args[2].startswith("fcm_short_run_crash_loop_")
    ]
    assert len(short_run_calls) == 0
    # _fatal_retry_counts was incremented at least once by the endpoint path.
    counter_key = f"{entry_id}:endpoint"
    assert receiver._fatal_retry_counts.get(counter_key, 0) >= 1


def test_unload_clears_fatal_errors_via_force() -> None:
    """`_clear_fatal_error_for_entry(force=True)` (unload path) releases the latch.

    Defense 2 iter-8: the unregister path must clear the cap latch alongside
    the fatal-error map because the entry itself goes away. Without ``force``
    the clear-path is a no-op while the latch is set (Codex second finding).
    """
    receiver = FcmReceiverHA()
    receiver._fatal_errors["entry-unload"] = "FCM short-run crash loop: ..."
    receiver._fatal_error = "FCM short-run crash loop: ..."
    receiver._short_run_cap_latched.add("entry-unload")

    receiver._clear_fatal_error_for_entry(
        "entry-unload", reason="Entry unregistered", force=True
    )

    assert "entry-unload" not in receiver._fatal_errors
    assert "entry-unload" not in receiver._short_run_cap_latched
    assert receiver._fatal_error is None


def test_force_teardown_leaves_fixable_reauth_repairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Teardown (force=True) must NOT delete the fixable reauth repairs.

    Codex finding (PR #1104 follow-up): ``force=True`` is reached from
    ``unregister_coordinator``, which fires via ``async_on_unload`` on EVERY
    unload -- including the reload Home Assistant performs right after a
    successful reauth flow. Deleting ``fcm_reauth_required`` / legacy
    ``fcm_stuck`` here would dismiss a still-active prompt before the new
    supervisor has proven recovery. Only the transient short-run crash-loop
    issue (the DELETE half of the fatal-error CREATE/DELETE pair) is cleared.
    Actual removal cleanup lives in ``async_delete_entry_repair_issues``.
    """
    entry_id = "entry-teardown"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())
    _create_issue, delete_issue = _install_ir_capture(monkeypatch)

    receiver._clear_fatal_error_for_entry(
        entry_id, reason="Entry unregistered", force=True
    )

    deleted = {call.args[2] for call in delete_issue.call_args_list}
    assert f"fcm_short_run_crash_loop_{entry_id}" in deleted
    assert f"fcm_reauth_required_{entry_id}" not in deleted
    assert f"fcm_stuck_{entry_id}" not in deleted


def test_async_delete_entry_repair_issues_retires_all_on_removal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On actual entry removal ALL entry-scoped FCM repairs are retired.

    Codex finding (PR #1104): once the entry is gone no supervisor will run
    again, so ``async_remove_entry`` must delete the fixable reauth prompt and
    its legacy ``fcm_stuck`` predecessor alongside the transient short-run
    issue, or they stay pinned for a config entry that no longer exists. This
    is the removal-only counterpart to the reload-safe teardown above.
    """
    entry_id = "entry-removed"
    hass = SimpleNamespace()
    _create_issue, delete_issue = _install_ir_capture(monkeypatch)

    FcmReceiverHA.async_delete_entry_repair_issues(hass, entry_id)

    deleted = {call.args[2] for call in delete_issue.call_args_list}
    assert f"fcm_short_run_crash_loop_{entry_id}" in deleted
    assert f"fcm_reauth_required_{entry_id}" in deleted
    assert f"fcm_stuck_{entry_id}" in deleted


def test_non_teardown_clear_leaves_reauth_repair_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A recovery clear (force=False) must NOT delete the reauth repairs.

    Non-teardown clears (credentials updated, registration success) rely on the
    supervisor success path to retire ``fcm_reauth_required`` / ``fcm_stuck``;
    deleting them here would dismiss a still-valid reauth prompt before the
    supervisor has actually re-registered successfully. Only the short-run
    issue (the DELETE half of the fatal-error CREATE/DELETE pair) is cleared.
    """
    entry_id = "entry-recovery"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())
    _create_issue, delete_issue = _install_ir_capture(monkeypatch)

    receiver._clear_fatal_error_for_entry(entry_id, reason="Credentials updated")

    deleted = {call.args[2] for call in delete_issue.call_args_list}
    assert f"fcm_short_run_crash_loop_{entry_id}" in deleted
    assert f"fcm_reauth_required_{entry_id}" not in deleted
    assert f"fcm_stuck_{entry_id}" not in deleted


# --------------------------------------------------------------------------
# Defense 2 iter-8: cap-latch lifecycle (Codex second finding)
# --------------------------------------------------------------------------
#
# Codex objected that the cap state lives in the same ``_fatal_errors`` map
# that ``_register_for_fcm_entry`` unconditionally clears after a successful
# registration. Registration is NOT a recovery proof for a poison-message
# crash loop: only a real healthy supervisor run (>= 30s) shows that the
# poisoned notification is gone. The tests below pin the new lifecycle
# contract (CA-STATE-LIFECYCLE-ASYMMETRY-001):
#
#   * ``_short_run_cap_latched`` is set at cap-fire time alongside the
#     ``_fatal_errors`` entry and the Repairs issue.
#   * ``_clear_fatal_error_for_entry`` is a no-op while the latch is set
#     (unless ``force=True`` is passed, which only the unregister path does).
#   * Only the healthy-run branch of the supervisor loop releases the latch,
#     pops the cap-related ``_fatal_errors`` entry, and re-derives the
#     aggregate ``_fatal_error``.


def test_clear_fatal_error_is_noop_while_cap_latched() -> None:
    """Registration-success paths cannot drop the latch (Codex iter-8 finding 2)."""
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())
    receiver._fatal_errors["entry-latched"] = "FCM short-run crash loop: ..."
    receiver._fatal_error = "FCM short-run crash loop: ..."
    receiver._short_run_cap_latched.add("entry-latched")

    # ``force`` defaults to ``False``: this is the path that
    # ``_register_for_fcm_entry`` and the credential-update handler take.
    receiver._clear_fatal_error_for_entry(
        "entry-latched", reason="Registration succeeded"
    )

    # Nothing changed: the latch, the fatal-error map, and the aggregate
    # all survive because the registration did not prove a healthy run.
    assert "entry-latched" in receiver._short_run_cap_latched
    assert receiver._fatal_errors["entry-latched"].startswith(
        "FCM short-run crash loop"
    )
    assert receiver._fatal_error == "FCM short-run crash loop: ..."


def test_clear_without_latch_still_clears_fatal_errors() -> None:
    """Without an active cap latch, the clear-path behaves idempotently."""
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())
    receiver._fatal_errors["entry-clean"] = "FCM auth failed ..."
    receiver._fatal_error = "FCM auth failed ..."

    receiver._clear_fatal_error_for_entry(
        "entry-clean", reason="Registration succeeded"
    )

    assert "entry-clean" not in receiver._fatal_errors
    assert receiver._fatal_error is None


@pytest.mark.asyncio
async def test_cap_fire_sets_short_run_cap_latched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cap-fire installs the latch alongside the fatal-error map and the issue."""
    entry_id = "entry-cap-latch"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _ = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert create_issue.call_count == 1
    assert entry_id in receiver._short_run_cap_latched
    assert receiver._fatal_errors[entry_id].startswith("FCM short-run crash loop")


@pytest.mark.asyncio
async def test_cap_latch_survives_registration_success_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A simulated registration-success path cannot drop the latch.

    Defense 2 iter-8 (Codex second finding): without the latch, a reload or
    credential update after cap-fire would clear the fatal-error map and the
    Repairs issue before the supervisor proved the poison message is gone,
    leaving the user blind during the next 10-run burst.
    """
    entry_id = "entry-survive"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert entry_id in receiver._short_run_cap_latched
    pre_message = receiver._fatal_errors[entry_id]

    # Simulate the ``_register_for_fcm_entry`` success path and the
    # credential-update path (``_on_credentials_updated_for_entry``).
    # Neither must release the latch.
    receiver._clear_fatal_error_for_entry(entry_id, reason="Registration succeeded")
    receiver._clear_fatal_error_for_entry(
        entry_id, reason="Credentials updated for entry"
    )

    assert entry_id in receiver._short_run_cap_latched
    assert receiver._fatal_errors[entry_id] == pre_message


# --------------------------------------------------------------------------
# Defense 2 iter-10: cap-latch guard granularity (Codex follow-up finding)
# --------------------------------------------------------------------------
#
# Codex objected that the iter-8 latch guard was unconditional and blocked
# every later ``_clear_fatal_error_for_entry`` call while the latch was set.
# Scenario: the cap fires, then the user reloads or re-registers; the new
# registration hits a terminal 401/404 and overwrites ``_fatal_errors[entry_id]``
# with a non-cap message; a credentials recovery later tries to clear that
# fatal but is blocked by the latch guard, so the coordinator keeps seeing
# the stale auth-fatal even after the underlying problem is fixed.
#
# CA-GUARD-GRANULARITY-001: the latch guard is now SELECTIVE:
#
#   * Latch active AND stored fatal IS the cap message
#     -> full no-op (cap state + Repairs UI artefact preserved).
#   * Latch active AND stored fatal is a NON-cap message
#     -> clear the non-cap fatal, but keep the latch and the Repairs UI
#        artefact (cap warning stays visible until a healthy run releases it).
#   * Latch active AND no fatal stored
#     -> full no-op (treat as cap-only state).
#   * Latch not active OR force=True
#     -> normal clear-all path (unchanged).


def test_clear_clears_non_cap_fatal_while_cap_latched() -> None:
    """Non-cap fatals must clear normally even while the cap latch is active.

    Defense 2 iter-10 (Codex follow-up): the iter-8 guard was too broad and
    blocked every clear-call, so a terminal 401/404 written into
    ``_fatal_errors`` by a re-registration attempt after cap-fire could never
    be dropped by a subsequent credentials recovery. The guard now blocks
    only when the stored fatal IS the cap message; non-cap fatals are
    dropped while latch and Repairs UI artefact stay in place.
    """
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    # Cap is latched (set by an earlier cap-fire); a subsequent
    # re-registration then overwrote the fatal map with a non-cap auth error.
    receiver._short_run_cap_latched.add("entry-mixed")
    receiver._fatal_errors["entry-mixed"] = (
        "FCM registration failed: HTTP 401 Unauthorized"
    )
    receiver._fatal_error = "FCM registration failed: HTTP 401 Unauthorized"

    # Credentials recovery path: ``_register_for_fcm_entry`` calls this after
    # a new successful registration. The non-cap fatal MUST clear so the
    # coordinator does not see a stale auth-fatal.
    receiver._clear_fatal_error_for_entry(
        "entry-mixed", reason="Credentials updated for entry"
    )

    # Non-cap fatal cleared.
    assert "entry-mixed" not in receiver._fatal_errors
    assert receiver._fatal_error is None
    # Cap latch SURVIVES: only a real healthy supervisor run may drop it.
    assert "entry-mixed" in receiver._short_run_cap_latched


def test_clear_preserves_repair_issue_when_clearing_non_cap_fatal() -> None:
    """Clearing a non-cap fatal while latched must not delete the Repairs issue.

    The Repairs UI artefact (``fcm_short_run_crash_loop_<entry_id>``) is the
    user-visible cap warning. Dropping it during an unrelated credentials
    recovery would invalidate the cap warning before the listener proves the
    poison message is gone (the very failure mode iter-8 pinned).
    """
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    deleted_issues: list[tuple[str, str]] = []

    def fake_async_delete_issue(hass, domain: str, issue_id: str) -> None:
        deleted_issues.append((domain, issue_id))

    monkey = pytest.MonkeyPatch()
    try:
        monkey.setattr(
            fcm_receiver_ha.ir, "async_delete_issue", fake_async_delete_issue
        )

        receiver._short_run_cap_latched.add("entry-keep-issue")
        receiver._fatal_errors["entry-keep-issue"] = (
            "FCM registration failed: HTTP 404 Not Found"
        )
        receiver._fatal_error = "FCM registration failed: HTTP 404 Not Found"

        receiver._clear_fatal_error_for_entry(
            "entry-keep-issue", reason="Credentials updated for entry"
        )
    finally:
        monkey.undo()

    # Non-cap fatal cleared but the Repairs UI artefact must remain (no
    # ``async_delete_issue`` call for the cap issue id).
    assert "entry-keep-issue" not in receiver._fatal_errors
    assert deleted_issues == []
    assert "entry-keep-issue" in receiver._short_run_cap_latched


def test_clear_no_stored_fatal_while_latched_is_full_noop() -> None:
    """Latch active and no fatal stored: full no-op, latch survives."""
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    receiver._short_run_cap_latched.add("entry-no-fatal")
    # No entry in ``_fatal_errors``.

    receiver._clear_fatal_error_for_entry(
        "entry-no-fatal", reason="Registration succeeded"
    )

    assert "entry-no-fatal" in receiver._short_run_cap_latched
    assert "entry-no-fatal" not in receiver._fatal_errors


# --------------------------------------------------------------------------
# Iter-16 regression: latch-restoration on pass-through clear
# (Codex 8cfd5b25 follow-up)
# --------------------------------------------------------------------------
#
# Pinning contract for ``_short_run_cap_messages``:
#   * Populated by the real cap-fire branch in ``_supervisor`` (snapshot of
#     the user-visible cap message).
#   * Restored into ``_fatal_errors[entry_id]`` by the pass-through clear
#     branch in ``_clear_fatal_error_for_entry`` so the per-entry fatal
#     stays visible to the coordinator/diag binary sensor across non-cap
#     overwrites.
#   * Dropped in lockstep with the latch in the two release paths:
#       (a) ``force=True`` teardown
#       (b) healthy-run cleanup branch in ``_supervisor``
#
# These are unit-tests of the helper contract (no supervisor loop).


def test_iter16_pass_through_restores_cap_message_from_snapshot() -> None:
    """Pass-through clear must restore the cap-fire snapshot.

    Iter-16 (Codex follow-up on commit 8cfd5b25): without restoration the
    coordinator and the diagnostic binary sensor lose visibility of the
    cap-fatal state during the recovery/retry window even though
    ``_short_run_cap_latched`` is still set.
    """
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    cap_message = (
        "FCM short-run crash loop: 10 consecutive runs ended within 30s "
        "(persistent poison message suspected)"
    )

    # Simulate the state right after a cap fire + a later non-cap fatal
    # overwriting the per-entry fatal map slot.
    receiver._short_run_cap_latched.add("entry-restore")
    receiver._short_run_cap_messages["entry-restore"] = cap_message
    receiver._fatal_errors["entry-restore"] = (
        "FCM registration failed: HTTP 401 Unauthorized"
    )
    receiver._fatal_error = "FCM registration failed: HTTP 401 Unauthorized"

    receiver._clear_fatal_error_for_entry(
        "entry-restore", reason="Credentials updated for entry"
    )

    # The non-cap fatal is gone, the cap snapshot is restored, the latch
    # and the snapshot cache survive (only a healthy run or force=True may
    # drop them).
    assert receiver._fatal_errors["entry-restore"] == cap_message
    assert receiver._fatal_error == cap_message
    assert "entry-restore" in receiver._short_run_cap_latched
    assert receiver._short_run_cap_messages["entry-restore"] == cap_message


def test_iter16_force_true_clear_drops_cap_message_snapshot() -> None:
    """A force=True teardown must also drop the cap-fire snapshot."""
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    receiver._short_run_cap_latched.add("entry-force")
    receiver._short_run_cap_messages["entry-force"] = "FCM short-run crash loop: ..."
    receiver._fatal_errors["entry-force"] = "FCM short-run crash loop: ..."
    receiver._fatal_error = "FCM short-run crash loop: ..."

    receiver._clear_fatal_error_for_entry("entry-force", reason="Unload", force=True)

    assert "entry-force" not in receiver._short_run_cap_latched
    assert "entry-force" not in receiver._short_run_cap_messages
    assert "entry-force" not in receiver._fatal_errors
    assert receiver._fatal_error is None


def test_iter16_pass_through_with_empty_snapshot_falls_back_to_iter10() -> None:
    """When no snapshot was captured the iter-10 behaviour must survive.

    Edge case: a stale latch is set manually (test harness) or the snapshot
    was already dropped while the latch remained. The pass-through clear
    must NOT fabricate a cap message - it simply clears the non-cap fatal
    and lets the latch keep blocking future cap-fatal clears.
    """
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    receiver._short_run_cap_latched.add("entry-empty-snapshot")
    # NO entry in ``_short_run_cap_messages`` for this entry_id.
    receiver._fatal_errors["entry-empty-snapshot"] = (
        "FCM registration failed: HTTP 401 Unauthorized"
    )
    receiver._fatal_error = "FCM registration failed: HTTP 401 Unauthorized"

    receiver._clear_fatal_error_for_entry(
        "entry-empty-snapshot", reason="Credentials updated for entry"
    )

    # Iter-10 contract: non-cap fatal cleared, latch survives, no
    # restoration because nothing was snapshotted.
    assert "entry-empty-snapshot" not in receiver._fatal_errors
    assert receiver._fatal_error is None
    assert "entry-empty-snapshot" in receiver._short_run_cap_latched
    assert "entry-empty-snapshot" not in receiver._short_run_cap_messages


@pytest.mark.asyncio
async def test_healthy_run_releases_cap_latch_and_fatal_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A healthy run (>= 30s) clears the latch, the fatal map entry, and the issue."""
    entry_id = "entry-recover"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    # 10 short runs trip the cap; one healthy run afterwards must release it.
    # The healthy run is modelled as a long supervisor pass (jump after the
    # ``entry_start`` snapshot via ``jump_on_run_state_read``).
    short_clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    healthy_client = _SupervisorPushClientStub(clock=clock, jump_on_run_state_read=60.0)
    _install_supervisor_mocks(
        monkeypatch,
        receiver,
        clock=clock,
        ensure_clients=[*short_clients, healthy_client],
    )
    create_issue, delete_issue = _install_ir_capture(monkeypatch)

    # Phase 1: 10 short runs trip the cap and stop the supervisor.
    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert create_issue.call_count == 1
    assert entry_id in receiver._short_run_cap_latched
    assert receiver._fatal_errors[entry_id].startswith("FCM short-run crash loop")

    # Phase 2: simulate a reload (fresh stop_evt + fresh supervisor task).
    receiver._stop_evts.pop(entry_id, None)
    receiver.supervisors.pop(entry_id, None)

    # The healthy run drops the latch from inside the supervisor loop's
    # else branch (counter reset path).
    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert entry_id not in receiver._short_run_cap_latched
    assert entry_id not in receiver._fatal_errors
    assert receiver._fatal_error is None
    # Iter-16: the healthy-run cleanup branch must also retire the
    # cap-fire snapshot so a later non-cap fatal cannot accidentally
    # restore a stale cap message.
    assert entry_id not in receiver._short_run_cap_messages
    # The Repairs issue is deleted at least once on the healthy path.
    delete_calls_for_entry = [
        call
        for call in delete_issue.call_args_list
        if call.args[2] == f"fcm_short_run_crash_loop_{entry_id}"
    ]
    assert delete_calls_for_entry, (
        "expected a delete_issue call for the short-run crash loop key"
    )


# --------------------------------------------------------------------------
# Boundary test (Iteration-2-L3)
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_long_run_boundary_exactly_30s_does_not_count_as_short(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`<` vs. `<=` drift sentry: run_duration == 30.0 must NOT count as short."""
    assert _SHORT_RUN_THRESHOLD_S == 30.0  # pin the constant against silent drift

    entry_id = "entry-boundary"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    # Frozen clock (``step=0.0``): every ``time.monotonic()`` call returns
    # the same value unless ``.jump(seconds)`` is invoked. This is the only
    # way to drive ``run_duration`` to *exactly* 30.0 — any auto-increment
    # would push the ``run_duration = time.monotonic() - entry_start`` read past the boundary by a few ``step``s
    # and let an off-by-one bug (`<=` instead of `<`) hide inside the noise.
    clock = _MonoClock(step=0.0)

    # Critical ordering (Codex Iter-5 finding):
    # ``jump_on_start`` would fire BEFORE the supervisor records
    # ``entry_start = time.monotonic()``, so the jump is baked
    # into the snapshot and ``run_duration`` ends up ~0s. Instead, hook
    # the jump on the first ``pc.run_state`` read
    # (``getattr(pc, "run_state", None)``), which is the
    # first clock-observable event INSIDE the monitor loop, AFTER the
    # snapshot. Then ``time.monotonic() - entry_start == 30.0`` exactly.
    stub = _SupervisorPushClientStub(clock=clock, jump_on_run_state_read=30.0)
    _install_supervisor_mocks(monkeypatch, receiver, clock=clock, ensure_clients=[stub])
    create_issue, _ = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # The else-branch was hit (counter reset to 0), not the short-run branch.
    # If `<` ever drifts to `<=`, ``run_duration == 30.0`` falls into the
    # short-run path and ``_observed_short_run_counter`` becomes 1.
    assert stub._observed_short_run_counter == 0, (
        f"Boundary 30.0s was counted as short — `<` drifted to `<=`? "
        f"Counter={stub._observed_short_run_counter}"
    )
    assert create_issue.call_count == 0
    assert entry_id not in receiver._fatal_errors


# --------------------------------------------------------------------------
# Cross-locale symmetry (Iteration-2-L2)
# --------------------------------------------------------------------------


def test_translation_key_present_in_all_locales(integration_root: Path) -> None:
    """`fcm_short_run_crash_loop` exists in every locale file under `issues`."""
    key = "fcm_short_run_crash_loop"
    files = {
        "strings.json": integration_root / "strings.json",
        "de.json": integration_root / "translations" / "de.json",
        "en.json": integration_root / "translations" / "en.json",
        "es.json": integration_root / "translations" / "es.json",
        "fr.json": integration_root / "translations" / "fr.json",
        "he.json": integration_root / "translations" / "he.json",
        "it.json": integration_root / "translations" / "it.json",
        "nl.json": integration_root / "translations" / "nl.json",
        "pl.json": integration_root / "translations" / "pl.json",
        "pt-BR.json": integration_root / "translations" / "pt-BR.json",
        "pt.json": integration_root / "translations" / "pt.json",
    }

    titles: dict[str, str] = {}
    descriptions: dict[str, str] = {}
    for label, path in files.items():
        assert path.exists(), f"locale file missing: {path}"
        data = json.loads(path.read_text(encoding="utf-8"))
        assert "issues" in data, f"{label}: no issues section"
        assert key in data["issues"], f"{label}: missing key {key!r}"
        entry = data["issues"][key]
        assert isinstance(entry, dict), f"{label}: {key!r} must be a dict"
        assert entry.get("title", "").strip(), f"{label}: empty title for {key!r}"
        assert entry.get("description", "").strip(), (
            f"{label}: empty description for {key!r}"
        )
        assert len(entry["description"]) >= 50, (
            f"{label}: description too short ({len(entry['description'])} chars) "
            f"for a repair-issue body"
        )
        titles[label] = entry["title"]
        descriptions[label] = entry["description"]

    # No copy-paste drift between German and English description.
    assert descriptions["de.json"] != descriptions["en.json"], (
        "Copy-paste drift: de.json description matches en.json — translation missing"
    )
    # Strings.json (master) matches en.json (English locale)
    assert descriptions["strings.json"] == descriptions["en.json"], (
        "strings.json description must mirror en.json"
    )


# --------------------------------------------------------------------------
# Defense 2 iter-9 (Codex PR #1086 follow-up):
# Cap-fire must TERMINATE the supervisor; the inner-loop ``break`` alone
# falls through to the per-iteration restart block and keeps spawning fresh
# FCM clients (retry pressure + log spam continue indefinitely despite the
# cap latch). See CA-DEFENSE-TERMINATION-001.
#
# These tests deliberately do NOT rely on the helper's
# StopIteration->stop_evt.set() emergency brake (CA-TEST-TERMINATION-001):
# they install their own ``_ensure_client_for_entry`` mock that fails the
# test the moment the supervisor calls it AFTER the cap should have fired.
# That isolates the production-side termination contract from the test-helper
# safety net.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cap_fire_terminates_outer_loop_without_helper_safety_net(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cap-fire must stop the supervisor in production, not via the test helper.

    Codex iter-9 (PR #1086): ``break`` after the cap fires only exits the
    inner monitor loop; the outer ``while not stop_evt.is_set()`` keeps
    spinning and ``_ensure_client_for_entry`` is called for an 11th client.
    The production fix sets ``stop_evt`` and pops ``_stop_evts[entry_id]``
    inside the cap-fire branch, so the outer loop exits before any further
    ensure call.
    """
    entry_id = "entry-cap-terminates"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS + 5)
    ]
    ensure_iter = iter(clients)
    ensure_call_count = 0

    async def _ensure(entry_id_inner: str, cache, generation=None):  # noqa: ARG001
        # NOTE: no StopIteration->stop_evt.set() escape hatch here. If the
        # production cap fails to terminate the supervisor, this returns the
        # 11th client and we explicitly fail the test below.
        nonlocal ensure_call_count
        ensure_call_count += 1
        return next(ensure_iter)

    monkeypatch.setattr(fcm_receiver_ha.time, "monotonic", clock)
    monkeypatch.setattr(receiver, "_ensure_client_for_entry", _ensure)
    register_mock = AsyncMock(return_value=True)
    monkeypatch.setattr(receiver, "_register_for_fcm_entry", register_mock)
    monkeypatch.setattr(fcm_receiver_ha.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(fcm_receiver_ha.random, "uniform", lambda *_a, **_kw: 0.0)

    real_wait_for = asyncio.wait_for

    async def _instant_wait_for(fut, *, timeout=None):
        if timeout is not None and timeout > 0:
            coro_name = getattr(fut, "__name__", "") or getattr(
                getattr(fut, "cr_code", None), "co_name", ""
            )
            if coro_name == "wait":
                if asyncio.iscoroutine(fut):
                    fut.close()
                raise TimeoutError
        return await real_wait_for(fut, timeout=timeout)

    monkeypatch.setattr(fcm_receiver_ha.asyncio, "wait_for", _instant_wait_for)
    _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # Production-side termination contract: exactly _MAX ensure calls.
    # An 11th call would mean the outer loop kept running after cap-fire.
    assert ensure_call_count == _MAX_CONSECUTIVE_SHORT_RUNS, (
        f"Supervisor kept spinning after cap-fire: ensure was called "
        f"{ensure_call_count} times (expected {_MAX_CONSECUTIVE_SHORT_RUNS}). "
        f"Defense 2 cap is a paper tiger."
    )
    assert receiver.supervisors[entry_id].done()
    assert entry_id in receiver._short_run_cap_latched


@pytest.mark.asyncio
async def test_cap_fire_pops_stop_evt_for_legitimate_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After cap-fire, ``_stop_evts[entry_id]`` is gone so a reload restarts cleanly.

    The cap-fire branch calls ``stop_evt.set()`` AND
    ``self._stop_evts.pop(entry_id, None)``. Without the pop, the next
    legitimate restart via ``_start_supervisor_for_entry`` would reuse the
    already-set event through ``setdefault`` and the new supervisor would
    terminate immediately.
    """
    entry_id = "entry-cap-pop"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # After cap-fire the stop event has been consumed AND removed so that a
    # legitimate ``_register_for_fcm_entry`` or reload constructs a fresh one.
    assert entry_id not in receiver._stop_evts, (
        "Stale stop event lingers in _stop_evts after cap-fire; a "
        "legitimate restart would short-circuit because setdefault would "
        "reuse the already-set event."
    )


# --------------------------------------------------------------------------
# Iter-11: Defense-Specificity (pre-start failures excluded from cap)
# --------------------------------------------------------------------------
# Pins the ``became_started`` per-run latch contract:
# - Pre-start failures (MCS unreachable / login failed, client never
#   reaches STARTED) MUST NOT advance the short-run counter and MUST
#   NOT clear an existing cap latch.
# - Real poison-message short-runs (became_started=True + run<30s) MUST
#   still advance the counter as before. The cap stays specific to the
#   scenario it was designed for.
# - A mixed cascade (poison-message runs interleaved with transient
#   outages) still accumulates correctly: the outages are no-ops, the
#   poison-message runs add up to the cap at iteration N+M_short.
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_start_failure_does_not_advance_cap_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """15 pre-start failures (no STARTED reached) must NOT fire the cap.

    Models the Codex iter-11 scenario: an ordinary network/Google outage
    where ``_listen()`` exhausts its 5 connection retries (~15-20s) and
    the client never reaches ``STARTED``. Before the gate, 10 such
    bursts would have permanently capped the supervisor and disabled
    FCM push until reload.
    """
    entry_id = "entry-pre-start"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    # 15 stubs that NEVER set become_started -> run_state stays None on
    # every read -> ``became_started`` stays False in the supervisor.
    clients = [
        _SupervisorPushClientStub(clock=clock, become_started=False) for _ in range(15)
    ]
    register_mock = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # The supervisor only terminates when the helper exhausts the client
    # list (StopIteration -> stop_evt.set()). We MUST see all 15 registration
    # attempts -- none was prematurely cut off by a cap fire.
    assert register_mock.await_count == 15, (
        f"Pre-start failures advanced the cap counter and terminated "
        f"early -- expected 15 attempts, got {register_mock.await_count}"
    )
    assert create_issue.call_count == 0, (
        "Cap repair issue was created from pre-start failures; the "
        "``became_started`` gate is broken."
    )
    assert entry_id not in receiver._fatal_errors
    assert entry_id not in receiver._short_run_cap_latched


@pytest.mark.asyncio
async def test_mixed_burst_only_counts_real_short_runs_toward_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """5 real short-runs + 3 pre-start failures + 5 real short-runs -> cap fires at 10 real.

    Pins the "counter unchanged across pre-start failures" semantics: a
    poison-message cascade interleaved with outages must still
    accumulate correctly. The outages are no-ops, the real short-runs
    accumulate 5+5=10 and the cap fires on the 10th REAL short-run.
    """
    entry_id = "entry-mixed"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    clients: list[_SupervisorPushClientStub] = []
    # 5 real poison-message-style short-runs (became_started=True, run<30s)
    for _ in range(5):
        clients.append(_SupervisorPushClientStub(clock=clock))
    # 3 transient outages (become_started=False) -- must be ignored by the cap
    for _ in range(3):
        clients.append(_SupervisorPushClientStub(clock=clock, become_started=False))
    # 5 more real short-runs -- together with the first 5 these are the 10
    # consecutive REAL short-runs that fire the cap.
    for _ in range(5):
        clients.append(_SupervisorPushClientStub(clock=clock))
    # Extra clients beyond the cap-fire would expose "loop kept going".
    for _ in range(3):
        clients.append(_SupervisorPushClientStub(clock=clock))

    register_mock = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # Cap fires after the 13th client (5 real + 3 outage + 5th real = 13).
    # Anything beyond 13 indicates the cap miscounted or did not terminate.
    assert register_mock.await_count == 13, (
        f"Mixed burst miscounted -- expected 13 client attempts "
        f"(5 real + 3 outage + 5 real, cap on the 13th), got "
        f"{register_mock.await_count}"
    )
    assert create_issue.call_count == 1
    assert entry_id in receiver._short_run_cap_latched
    assert receiver._fatal_errors[entry_id].startswith("FCM short-run crash loop")


@pytest.mark.asyncio
async def test_long_pre_start_does_not_release_cap_latch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Long-running pre-start failure (run>=30s) does NOT clear the cap latch.

    Models a slow-to-fail connection attempt that hangs in pre-start for
    more than 30s. Before the gate, the supervisor would have hit the
    ``else`` branch (long run) and released the cap latch + Repairs UI
    issue without ever proving the listener can stay up. The
    ``became_started`` gate makes this a no-op so the latch remains.
    """
    entry_id = "entry-long-prestart"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    # Pre-seed the latch as if a prior cap-fire was still active. This
    # mirrors the production state right after iter-9 termination but
    # before any healthy run has cleared it.
    receiver._short_run_cap_latched.add(entry_id)
    receiver._fatal_errors[entry_id] = (
        "FCM short-run crash loop: 10 consecutive runs ended within 30s "
        "(persistent poison message suspected)"
    )

    clock = _MonoClock(step=0.0)
    # become_started=False AND a 60s jump after the snapshot. Without the
    # gate, the supervisor would see run_duration >= 30s and hit the
    # ``else`` branch, releasing the latch.
    stub = _SupervisorPushClientStub(
        clock=clock, jump_on_run_state_read=60.0, become_started=False
    )
    _install_supervisor_mocks(monkeypatch, receiver, clock=clock, ensure_clients=[stub])
    _create_issue, delete_issue = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # Latch survives the long pre-start (no healthy proof was observed).
    assert entry_id in receiver._short_run_cap_latched, (
        "Long pre-start failure cleared the cap latch without ever "
        "proving a healthy run -- gate is broken."
    )
    assert entry_id in receiver._fatal_errors
    assert receiver._fatal_errors[entry_id].startswith("FCM short-run crash loop")
    # Repairs UI issue for the short-run cap was NOT deleted by the long
    # pre-start. (The supervisor may issue unrelated ``fcm_stuck`` delete
    # calls after a successful registration; we filter on the specific
    # ``fcm_short_run_crash_loop_*`` key.)
    cap_issue_key = f"fcm_short_run_crash_loop_{entry_id}"
    cap_deletes = [
        call
        for call in delete_issue.call_args_list
        if len(call.args) >= 3 and call.args[2] == cap_issue_key
    ]
    assert cap_deletes == [], (
        f"Long pre-start deleted the cap Repairs issue without a healthy "
        f"run; cap_deletes={cap_deletes}"
    )


# --------------------------------------------------------------------------
# Iter-12 (Codex follow-up): sampling-gap defense
#
# When the full lifecycle CREATED -> STARTED -> CRASH happens between two
# monitor polls (>=0.5s), the supervisor's monitor loop NEVER reads
# ``state == STARTED`` and ``became_started`` would remain false. The
# vendored ``_ObservableFcmPushClient`` subclass latches
# ``_has_been_started`` via a ``__setattr__`` hook the moment the library
# assigns ``run_state = STARTED`` -- independent of polling cadence.
# The supervisor's cap branch consults this latch before deciding whether
# a short run advances the counter. (CA-DEFENSE-OBSERVABILITY-001)
# --------------------------------------------------------------------------


def test_observable_subclass_latches_started_transition() -> None:
    """The ``__setattr__`` hook latches ``_has_been_started`` on STARTED.

    Unit-tests the observer mechanic in isolation: bypass the library
    ``__init__`` and only exercise the attribute hook. The latch is
    monotonic (set on STARTED, never cleared) and ignores non-STARTED
    state assignments.
    """
    pc_cls = fcm_receiver_ha._ObservableFcmPushClientCls
    if pc_cls is None:
        pytest.skip("Vendored library not available; subclass not active")

    from custom_components.googlefindmy.Auth.firebase_messaging import (
        FcmPushClientRunState,
    )

    # Bypass library ``__init__`` -- we only exercise the ``__setattr__``
    # hook. Seed the latch the same way the subclass' ``__init__`` does.
    pc = pc_cls.__new__(pc_cls)
    object.__setattr__(pc, "_has_been_started", False)

    # Non-STARTED transitions do NOT set the latch.
    pc.run_state = FcmPushClientRunState.CREATED
    assert pc._has_been_started is False
    pc.run_state = FcmPushClientRunState.STARTING_LOGIN
    assert pc._has_been_started is False

    # STARTED sets the latch.
    pc.run_state = FcmPushClientRunState.STARTED
    assert pc._has_been_started is True

    # Latch is monotonic -- subsequent non-STARTED transitions do NOT
    # clear it (the cap needs evidence the client was EVER started, not
    # whether it is started right now).
    pc.run_state = FcmPushClientRunState.STOPPING
    assert pc._has_been_started is True
    pc.run_state = FcmPushClientRunState.STOPPED
    assert pc._has_been_started is True


@pytest.mark.asyncio
async def test_micro_window_crash_advances_cap_via_observable_latch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Micro-window CREATED -> STARTED -> CRASH between polls still counts.

    Models the exact scenario the cap is meant to detect: the client
    connects, decodes a poison message, and crashes before the supervisor's
    first ``await asyncio.sleep(FCM_MONITOR_INTERVAL_S)`` sample. The
    monitor loop reads STOPPED/!do_listen and ``became_started`` would
    remain false WITHOUT the subclass latch. The latch
    (``_has_been_started=True``, set by the vendored ``__setattr__`` hook
    during ``pc.start()``) lets the cap correctly classify the run as a
    short crash and advance the counter. 10 such micro-window crashes
    fire the cap.
    """
    entry_id = "entry-micro-window"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    # become_started=False: the monitor loop never reads ``run_state ==
    # STARTED`` (state read returns None on the first call, forcing the
    # ``state is None`` break path immediately).
    # has_been_started=True: the subclass latch was set during the
    # library's CREATED -> STARTED transition that completed BEFORE the
    # first monitor poll.
    clients = [
        _SupervisorPushClientStub(
            clock=clock, become_started=False, has_been_started=True
        )
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    register_mock = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # Cap fired despite the supervisor monitor loop never reading STARTED.
    assert register_mock.await_count == _MAX_CONSECUTIVE_SHORT_RUNS, (
        f"Expected exactly {_MAX_CONSECUTIVE_SHORT_RUNS} register calls "
        f"(one per micro-window crash); got {register_mock.await_count}. "
        f"Without the subclass latch, became_started=False would keep the "
        f"cap from firing and the supervisor would retry indefinitely."
    )
    assert create_issue.call_count == 1
    cap_issue_call = create_issue.call_args
    assert cap_issue_call.args[2] == f"fcm_short_run_crash_loop_{entry_id}"
    assert entry_id in receiver._short_run_cap_latched
    assert receiver._fatal_errors[entry_id].startswith("FCM short-run crash loop")


@pytest.mark.asyncio
async def test_mixed_burst_micro_window_and_pre_start_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-start failures interleaved with micro-window crashes count only the latter.

    Cascade: 5 micro-window crashes (has_been_started=True) + 3 true
    pre-start failures (has_been_started=False) + 5 micro-window crashes.
    The cap MUST fire at exactly 10 cumulative micro-window crashes
    (counter reaches threshold on the 13th supervisor pass). Pre-start
    failures leave the counter AND the latch UNTOUCHED. This pins the
    orthogonality of the sampling-gap defense to the pre-start-failure
    gate from iter-11 (CA-DEFENSE-SPECIFICITY-001).
    """
    entry_id = "entry-mixed-burst"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())

    clock = _MonoClock(step=0.001)
    cascade = (
        [
            _SupervisorPushClientStub(
                clock=clock, become_started=False, has_been_started=True
            )
            for _ in range(5)
        ]
        + [
            _SupervisorPushClientStub(
                clock=clock, become_started=False, has_been_started=False
            )
            for _ in range(3)
        ]
        + [
            _SupervisorPushClientStub(
                clock=clock, become_started=False, has_been_started=True
            )
            for _ in range(5)
        ]
    )
    register_mock = _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=cascade
    )
    create_issue, _delete_issue = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    # Cap fired on the 13th iteration (5 + 3 + 5 = 13 stubs total, cap
    # advances on micro-window crashes only).
    assert register_mock.await_count == 13, (
        f"Expected 13 register calls (5 + 3 + 5 cascade); got "
        f"{register_mock.await_count}. Pre-start failures must NOT count "
        f"toward the cap (iter-11), but real micro-window crashes MUST "
        f"(iter-12)."
    )
    assert create_issue.call_count == 1
    cap_issue_call = create_issue.call_args
    assert cap_issue_call.args[2] == f"fcm_short_run_crash_loop_{entry_id}"
    assert entry_id in receiver._short_run_cap_latched


# --------------------------------------------------------------------------
# CI follow-up (PR #1086 iter-13): Class-Identity substitution defense
#
# Iter-12 added _ObservableFcmPushClient as a subclass of FcmPushClient. The
# production factory must construct that subclass. Tests, however, may
# substitute the module-level FcmPushClient symbol via monkeypatch (the
# documented seam) to inject test doubles -- e.g. test_receiver_reuses_
# hass_managed_session in tests/test_fcm_receiver_guard.py asserts
# ``isinstance(created, DummyPushClient)``. A factory that always returns
# the production subclass would break isinstance identity against the
# patched symbol because the subclass inherits from the ORIGINAL class,
# not from the patched one. The factory therefore checks at CALL TIME
# whether the module symbol still points at the snapshot of the original
# class, and instantiates the patched class directly when not.
# (CA-SUBCLASS-IDENTITY-001)
# --------------------------------------------------------------------------


def test_factory_uses_observable_subclass_in_production() -> None:
    """When the FcmPushClient symbol is untouched the factory returns the subclass.

    Pins the production path: the runtime check
    ``current is _OriginalFcmPushClient`` is true, so the factory MUST
    instantiate ``_ObservableFcmPushClient`` (not the plain library class).
    """
    if fcm_receiver_ha._ObservableFcmPushClientCls is None:
        pytest.skip("Vendored library not available; subclass not active")

    from custom_components.googlefindmy.Auth.firebase_messaging import (
        FcmPushClientConfig,
        FcmRegisterConfig,
    )

    config = FcmPushClientConfig()
    register_cfg = FcmRegisterConfig(
        project_id="p",
        app_id="a",
        api_key="k",
        messaging_sender_id="s",
    )

    async def _noop_cb(*_args: object, **_kw: object) -> None:
        return None

    pc = fcm_receiver_ha._FcmPushClientFactory(
        _noop_cb,
        register_cfg,
        {},
        _noop_cb,
        config=config,
    )

    assert isinstance(pc, fcm_receiver_ha._ObservableFcmPushClientCls)
    # Latch was seeded before super().__init__ so it is observable here.
    assert pc._has_been_started is False


def test_factory_honours_monkeypatched_fcm_push_client_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Substituting FcmPushClient via monkeypatch is the documented seam.

    Regression pin for CA-SUBCLASS-IDENTITY-001: when tests patch the
    module-level ``FcmPushClient`` symbol to inject a test double, the
    factory MUST instantiate the patched class DIRECTLY -- not a subclass
    of the original library class. Otherwise ``isinstance`` assertions on
    the patched symbol (as used by tests in tests/test_fcm_receiver_guard
    .py) fail because the production subclass diverges from the patched
    class hierarchy.
    """

    captured: dict[str, object] = {}

    class _PatchedClient:
        def __init__(self, *args: object, **kwargs: object) -> None:
            captured["args"] = args
            captured["kwargs"] = kwargs

    monkeypatch.setattr(
        fcm_receiver_ha,
        "FcmPushClient",
        _PatchedClient,
    )

    pc = fcm_receiver_ha._FcmPushClientFactory(
        "callback",
        "config",
        {"creds": True},
        "creds_cb",
        http_client_session="session",
    )

    # The factory honoured the patch: identity matches the patched class,
    # not the production subclass and not the original library class.
    assert isinstance(pc, _PatchedClient)
    if fcm_receiver_ha._ObservableFcmPushClientCls is not None:
        assert not isinstance(pc, fcm_receiver_ha._ObservableFcmPushClientCls)
    assert captured["args"][0] == "callback"
    assert captured["kwargs"]["http_client_session"] == "session"


# --------------------------------------------------------------------------
# PR A — entry-state purge symmetry (AP1/AP2/AP3/AP7) + race tombstone (AP4)
# --------------------------------------------------------------------------


def test_purge_entry_tokens_clears_fatal_retry_counts() -> None:
    """AP1 (finding 2a): the teardown purge drops both fatal-retry counters."""
    receiver = FcmReceiverHA()
    eid = "entry-purge-counts"
    receiver._fatal_retry_counts[f"{eid}:auth"] = 4
    receiver._fatal_retry_counts[f"{eid}:endpoint"] = 2
    receiver._fatal_retry_counts["other:auth"] = 1  # foreign entry untouched

    receiver._purge_entry_tokens(eid)

    assert f"{eid}:auth" not in receiver._fatal_retry_counts
    assert f"{eid}:endpoint" not in receiver._fatal_retry_counts
    assert receiver._fatal_retry_counts["other:auth"] == 1


def test_purge_entry_tokens_clears_entry_health() -> None:
    """AP2 (finding 2b): per-entry health snapshots are purged on teardown."""
    receiver = FcmReceiverHA()
    eid = "entry-purge-health"
    receiver._entry_health[eid] = True
    receiver._entry_last_connected_wall[eid] = 123.0

    receiver._purge_entry_tokens(eid)

    assert eid not in receiver._entry_health
    assert eid not in receiver._entry_last_connected_wall


def test_purge_entry_tokens_clears_creds() -> None:
    """AP3 (finding 2c): in-memory creds + pending creds are purged on teardown."""
    receiver = FcmReceiverHA()
    eid = "entry-purge-creds"
    receiver.creds[eid] = {"gcm": {"token": "x"}}
    receiver._pending_creds[eid] = {"gcm": {"token": "y"}}

    receiver._purge_entry_tokens(eid)

    assert eid not in receiver.creds
    assert eid not in receiver._pending_creds


@pytest.mark.asyncio
async def test_reregister_returns_false_after_purge() -> None:
    """AP3 behaviour: a purged entry can no longer be revived via reregister.

    Before the creds purge, ``async_reregister_fcm``'s guard
    (``entry_id in self.creds``) stayed True after teardown and could restart a
    zombie supervisor for a dead entry. With the creds purged the guard now
    correctly rejects the revival.
    """
    receiver = FcmReceiverHA()
    eid = "entry-zombie"
    receiver.creds[eid] = {"gcm": {"token": "x"}}
    # pcs is empty (teardown stopped the client); only creds kept the guard True.

    receiver._purge_entry_tokens(eid)

    assert await receiver.async_reregister_fcm(eid) is False


@pytest.mark.asyncio
async def test_reregister_returns_false_when_tombstoned_with_residual_client() -> None:
    """Codex follow-up to AP3: a tombstoned entry whose live client still lingers.

    unregister_coordinator runs synchronously and cannot await ``pc.stop()``, so the
    client survives in ``self.pcs`` until the cancelled supervisor (or async_stop)
    drains it. The creds purge alone left the ``entry_id in self.pcs`` half of the
    reregister guard open, so a reregister in that window restarted a zombie
    supervisor for an entry mid-teardown. The tombstone (``_unregistered``) is the
    single source of truth for "torn down" and now blocks the revival.

    Distinct from a tombstoned supervisor that is *already running*: that one is
    allowed to terminate with its writes suppressed (see
    ``test_tombstone_suppresses_cap_latch``). Only the external revival path
    (``async_reregister_fcm``) is refused here.
    """
    receiver = FcmReceiverHA()
    eid = "entry-zombie-residual"
    # Post-unregister_coordinator state: creds purged, tombstone set, but the live
    # client still lingers in self.pcs (sync teardown could not await pc.stop()).
    receiver._unregistered.add(eid)
    receiver.pcs[eid] = object()  # residual client; must not be revived

    assert await receiver.async_reregister_fcm(eid) is False
    # The residual client is left untouched for the running supervisor / async_stop
    # to drain; the revival path must not have started reconstructing it.
    assert eid not in receiver.supervisors


@pytest.mark.asyncio
async def test_purge_cancels_pending_debounce_flush_tasks() -> None:
    """AP7 (finding 2d): pending flush tasks are cancelled and the trias dropped."""
    receiver = FcmReceiverHA()
    eid = "entry-debounce"
    key = (eid, "device-1")
    foreign = ("other-entry", "device-9")

    async def _never() -> None:
        await asyncio.sleep(60)

    task = asyncio.create_task(_never())
    foreign_task = asyncio.create_task(_never())
    receiver._flush_tasks[key] = task
    receiver._flush_tasks[foreign] = foreign_task
    receiver._pending[key] = {"payload": 1}
    receiver._pending_targets[key] = {"target"}
    receiver._pending[foreign] = {"payload": 2}

    try:
        receiver._purge_entry_tokens(eid)

        assert key not in receiver._flush_tasks
        assert key not in receiver._pending
        assert key not in receiver._pending_targets
        assert task.cancelled() or task.cancelling()
        # Foreign entry's debounce state is untouched.
        assert foreign in receiver._flush_tasks
        assert foreign in receiver._pending
    finally:
        foreign_task.cancel()
        for t in (task, foreign_task):
            try:
                await t
            except asyncio.CancelledError:
                pass


def test_unregister_coordinator_tombstones_and_purges() -> None:
    """AP4 (finding 4a): unregister tombstones the entry and purges its state."""
    receiver = FcmReceiverHA()
    eid = "entry-unreg-tombstone"
    coord = SimpleNamespace(config_entry=make_config_entry(entry_id=eid), cache=None)
    receiver._fatal_retry_counts[f"{eid}:auth"] = 3
    receiver._entry_health[eid] = True
    receiver.creds[eid] = {"gcm": {}}

    receiver.unregister_coordinator(coord)

    assert eid in receiver._unregistered
    assert f"{eid}:auth" not in receiver._fatal_retry_counts
    assert eid not in receiver._entry_health
    assert eid not in receiver.creds


def test_register_coordinator_clears_tombstone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AP4 (finding 4a, guard B): a setup un-tombstones the entry."""
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())
    eid = "entry-reload"
    receiver._unregistered.add(eid)
    coord = SimpleNamespace(config_entry=make_config_entry(entry_id=eid), cache=None)

    # The supervisor dispatch is irrelevant here; neutralise it so the test
    # does not leak an un-awaited coroutine.
    def _consume(coro: object, *, label: str | None = None) -> None:
        if asyncio.iscoroutine(coro):
            coro.close()

    monkeypatch.setattr(receiver, "_dispatch_to_hass_loop", _consume)

    receiver.register_coordinator(coord)

    assert eid not in receiver._unregistered
    assert receiver._entry_writes_suppressed(eid) is False


@pytest.mark.asyncio
async def test_invalidate_fcm_tokens_skipped_when_tombstoned() -> None:
    """AP4 (finding 4a): _invalidate_fcm_tokens does not persist for a dead entry."""
    receiver = FcmReceiverHA()
    eid = "entry-invalidate"
    creds = {"gcm": {"token": "keep"}, "fcm": {"x": 1}}
    receiver.creds[eid] = creds
    receiver._unregistered.add(eid)

    await receiver._invalidate_fcm_tokens(eid)

    # Suppressed: creds dict is left exactly as-is (fcm not stripped).
    assert receiver.creds[eid]["fcm"] == {"x": 1}


def test_credentials_callback_suppressed_when_tombstoned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AP4 (finding 4a, Codex follow-up): a late creds callback cannot resurrect.

    The old FCM client is not awaited during ``unregister_coordinator``'s
    synchronous teardown, so it can still fire ``_on_credentials_updated_for_entry``
    after ``_purge_entry_tokens`` cleared the entry. While tombstoned the callback
    must not re-write ``self.creds[entry_id]``, restore routing or queue
    persistence — otherwise the removed entry looks known to ``async_reregister_fcm``
    again. Once the tombstone is cleared (a real reload), fresh creds are accepted.
    """
    receiver = FcmReceiverHA()
    eid = "entry-creds-callback"

    dispatched: list[str | None] = []

    def _capture(coro: object, *, label: str | None = None) -> None:
        if asyncio.iscoroutine(coro):
            coro.close()
        dispatched.append(label)

    monkeypatch.setattr(receiver, "_dispatch_to_hass_loop", _capture)

    # Tombstoned: the late callback is dropped wholesale.
    receiver._unregistered.add(eid)
    receiver._on_credentials_updated_for_entry(eid, {"fcm": {"token": "late"}})

    assert eid not in receiver.creds
    assert eid not in receiver._pending_creds
    assert dispatched == []  # no routing/persistence dispatch happened

    # Real reload clears the tombstone; the next callback is honoured again.
    receiver._unregistered.discard(eid)
    receiver._on_credentials_updated_for_entry(eid, {"fcm": {"token": "fresh"}})

    assert receiver.creds[eid] == {"fcm": {"token": "fresh"}}


def test_credentials_callback_gates_on_driving_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AP4 (finding 4c): a superseded supervisor's mid-handshake callback is dropped.

    On a fast reload ``unregister_coordinator`` bumps the generation but does NOT
    pop the reused client, and ``register_coordinator`` clears the tombstone for
    the new incarnation. A cancelled-but-unwinding supervisor still inside
    ``checkin_or_register`` fires the synchronous credentials callback; the
    tombstone is already gone, so a tombstone-only gate would let it clobber the
    LIVE generation's creds and fatal latch. The callback must instead gate on
    the *driving* generation published by the supervisor for the handshake: a
    superseded generation is suppressed, the matching live generation writes.
    """
    receiver = FcmReceiverHA()
    eid = "entry-driving-gen"

    dispatched: list[str | None] = []

    def _capture(coro: object, *, label: str | None = None) -> None:
        if asyncio.iscoroutine(coro):
            coro.close()
        dispatched.append(label)

    monkeypatch.setattr(receiver, "_dispatch_to_hass_loop", _capture)
    # Isolate the callback's own effects from the supervisor routing block.
    monkeypatch.setattr(receiver, "get_fcm_token", lambda _eid=None: None)

    # Live incarnation: tombstone cleared, generation bumped to 1, fatal latched.
    receiver._unregistered.discard(eid)
    receiver._entry_generation[eid] = 1
    receiver._fatal_errors[eid] = "BadAuthentication"
    receiver._fatal_error = "BadAuthentication"

    # A superseded supervisor (captured generation 0) drives the handshake.
    with receiver._driving_generation_scope(eid, 0):
        receiver._on_credentials_updated_for_entry(eid, {"fcm": {"token": "stale"}})

    # Dropped wholesale: no creds resurrection, no dispatch, LIVE latch survives.
    assert eid not in receiver.creds
    assert eid not in receiver._pending_creds
    assert dispatched == []
    assert receiver._fatal_errors[eid] == "BadAuthentication"
    assert receiver._fatal_error == "BadAuthentication"

    # The live supervisor (matching generation 1) is honoured: creds written,
    # latch cleared, persistence dispatched.
    with receiver._driving_generation_scope(eid, 1):
        receiver._on_credentials_updated_for_entry(eid, {"fcm": {"token": "live"}})

    assert receiver.creds[eid] == {"fcm": {"token": "live"}}
    assert eid not in receiver._fatal_errors
    assert receiver._fatal_error is None
    assert dispatched  # save_credentials dispatch happened


@pytest.mark.asyncio
async def test_register_handshake_publishes_generation_for_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AP4 (finding 4c): the handshake publishes its generation end to end.

    ``firebase_messaging`` fires the credentials callback synchronously from
    inside ``checkin_or_register``. This pins the wiring: a superseded supervisor
    running ``_register_for_fcm_entry`` must suppress that mid-handshake callback,
    while a live supervisor's identical callback writes. A plain instance
    attribute could not survive firebase's internal awaits without a concurrent
    task clobbering it; the task-local ContextVar is what makes this hold.
    """
    eid = "entry-handshake-gen"

    def _build_receiver() -> FcmReceiverHA:
        receiver = FcmReceiverHA()
        monkeypatch.setattr(receiver, "get_fcm_token", lambda _eid=None: None)

        def _capture(coro: object, *, label: str | None = None) -> None:
            if asyncio.iscoroutine(coro):
                coro.close()

        monkeypatch.setattr(receiver, "_dispatch_to_hass_loop", _capture)
        receiver._unregistered.discard(eid)
        return receiver

    class _FakePc:
        """Mimics firebase firing the creds callback synchronously mid-checkin."""

        credentials = {"gcm": {"t": 1}, "fcm": {"t": 2}}  # not a re-register path

        def __init__(self, receiver: FcmReceiverHA) -> None:
            self._receiver = receiver

        async def checkin_or_register(self) -> dict[str, dict[str, str]]:
            # Yield like the real client so the ContextVar must survive an await.
            await asyncio.sleep(0)
            self._receiver._on_credentials_updated_for_entry(
                eid, {"fcm": {"token": "from-handshake"}}
            )
            return {"fcm": {"token": "from-handshake"}}

    # Superseded supervisor (captured generation 0; a reload bumped it to 1).
    superseded = _build_receiver()
    superseded._entry_generation[eid] = 1
    superseded.pcs[eid] = _FakePc(superseded)
    assert await superseded._register_for_fcm_entry(eid, generation=0) is True
    assert eid not in superseded.creds  # mid-handshake callback was suppressed

    # Live supervisor (captured generation matches current).
    live = _build_receiver()
    live._entry_generation[eid] = 0
    live.pcs[eid] = _FakePc(live)
    assert await live._register_for_fcm_entry(eid, generation=0) is True
    assert live.creds[eid] == {"fcm": {"token": "from-handshake"}}


@pytest.mark.asyncio
async def test_tombstone_suppresses_cap_latch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AP4 (finding 4a, guard A): a post-unregister supervisor write is dropped.

    The short-run cap fires while the entry is tombstoned (it was torn down
    mid-flight). The cap latch, the fatal-error map and the Repairs issue must
    NOT be (re-)created; the supervisor still terminates.
    """
    entry_id = "entry-cap-tombstoned"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())
    receiver._unregistered.add(entry_id)  # tombstoned before the cap fires

    clock = _MonoClock(step=0.001)
    clients = [
        _SupervisorPushClientStub(clock=clock)
        for _ in range(_MAX_CONSECUTIVE_SHORT_RUNS)
    ]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    create_issue, _ = _install_ir_capture(monkeypatch)

    await receiver._start_supervisor_for_entry(entry_id, None)
    await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert create_issue.call_count == 0
    assert entry_id not in receiver._short_run_cap_latched
    assert entry_id not in receiver._fatal_errors
    assert entry_id not in receiver._short_run_cap_messages


@pytest.mark.asyncio
async def test_tombstone_suppresses_fatal_retry_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AP4 (finding 4a, guard A): the fatal-retry counter is not re-created.

    A tombstoned entry hitting the endpoint-404 fatal path must not repopulate
    ``_fatal_retry_counts`` (which the teardown just purged).
    """
    from custom_components.googlefindmy.exceptions import FatalRegistrationError

    entry_id = "entry-fatal-tombstoned"
    receiver = FcmReceiverHA()
    receiver.attach_hass(SimpleNamespace())
    receiver._unregistered.add(entry_id)

    clock = _MonoClock(step=0.001)
    clients = [_SupervisorPushClientStub(clock=clock) for _ in range(4)]
    _install_supervisor_mocks(
        monkeypatch, receiver, clock=clock, ensure_clients=clients
    )
    _install_ir_capture(monkeypatch)

    err = FatalRegistrationError("registration failed")
    err.is_auth_error = False  # endpoint path
    register_mock = AsyncMock(side_effect=[err] * 4)
    monkeypatch.setattr(receiver, "_register_for_fcm_entry", register_mock)

    await receiver._start_supervisor_for_entry(entry_id, None)
    try:
        await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)
    except TimeoutError:  # pragma: no cover - bounded fallback
        receiver._stop_evts[entry_id].set()
        await asyncio.wait_for(receiver.supervisors[entry_id], timeout=5.0)

    assert f"{entry_id}:endpoint" not in receiver._fatal_retry_counts
    assert f"{entry_id}:auth" not in receiver._fatal_retry_counts
