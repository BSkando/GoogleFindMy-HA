# tests/test_fcmpushclient_stop_task_cleanup.py
"""Regression suite for the FCM client stop contract (upstream #238).

A generic exception in the listener used to leave ``do_listen`` set, and
``stop()`` returned early once the listener had reached ``STOPPED`` on its own,
so the task cleanup never ran. Each supervisor restart therefore left one
heartbeat task behind, and that task kept the whole client (TLS connection
included) alive until Home Assistant ran out of memory.

The tests use the real constructor and replace only the I/O boundary
(``_connect_with_retry``, ``_login``, ``_receive_msg``, ``_do_writer_close``).
Each test pins one clause of the contract in ``Auth/AGENTS.md``
("FCM client stop contract"). Every wait is bounded by ``asyncio.timeout`` so a
regression fails instead of hanging the suite.
"""

from __future__ import annotations

import asyncio
import gc
import weakref
from typing import Any
from unittest.mock import MagicMock

import pytest

from custom_components.googlefindmy.Auth.firebase_messaging.fcmpushclient import (
    FcmPushClient,
    FcmPushClientConfig,
    FcmPushClientRunState,
)

pytestmark = pytest.mark.asyncio

HEARTBEAT_TASK_NAME = "googlefindmy.fcm_heartbeat"
HANG_BOUND_S = 10.0


class _Io:
    """I/O boundary stubs; the listener fails only when the test says so.

    The listener task is created before the heartbeat task, so by the time a
    waiter on ``receiving`` resumes, the heartbeat has entered its ``sleep``. Failing later,
    via ``fail``, reproduces #238: the heartbeat is asleep when the listener
    dies, so it cannot end by itself through ``do_listen``.
    """

    def __init__(self, *, block_close: bool = False) -> None:
        self.receiving = asyncio.Event()
        self.fail = asyncio.Event()
        self.close_entered = asyncio.Event()
        self.close_release = asyncio.Event()
        if not block_close:
            self.close_release.set()

    async def connect(self) -> bool:
        return True

    async def login(self) -> None:
        return None

    async def receive(self) -> None:
        self.receiving.set()
        await self.fail.wait()
        raise RuntimeError("listener failure")

    async def close(self) -> None:
        self.close_entered.set()
        await self.close_release.wait()


def _client(io: _Io, writer_close_timeout: float = 2.0) -> FcmPushClient[None]:
    """Build a real client with only the I/O boundary replaced."""
    pc: FcmPushClient[None] = FcmPushClient(
        MagicMock(),
        MagicMock(),
        config=FcmPushClientConfig(
            client_heartbeat_interval=3600,
            writer_close_timeout=writer_close_timeout,
        ),
    )
    pc._connect_with_retry = io.connect  # type: ignore[method-assign]
    pc._login = io.login  # type: ignore[method-assign]
    pc._receive_msg = io.receive  # type: ignore[method-assign]
    pc._do_writer_close = io.close  # type: ignore[method-assign]
    return pc


async def _start_and_fail(
    pc: FcmPushClient[None], io: _Io
) -> tuple[asyncio.Task[None], asyncio.Task[None]]:
    """Start the client and let the listener fail while the heartbeat sleeps."""
    await pc.start()
    listener = pc._listen_task
    assert listener is not None
    heartbeat = next(t for t in pc.tasks if t is not listener)
    await io.receiving.wait()
    io.fail.set()
    return listener, heartbeat


async def _reap(*tasks: asyncio.Task[Any] | None) -> None:
    """Cancel and await whatever a failing test left running."""
    live = {t for t in tasks if t is not None and not t.done()}
    for t in live:
        t.cancel()
    if live:
        await asyncio.wait(live)


async def test_generic_listener_error_clears_do_listen() -> None:
    """The generic listener arm clears ``do_listen`` like every other arm."""
    io = _Io()
    pc = _client(io)
    listener: asyncio.Task[None] | None = None
    heartbeat: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(HANG_BOUND_S):
            listener, heartbeat = await _start_and_fail(pc, io)
            await asyncio.wait({listener})
        assert pc.do_listen is False
    finally:
        await _reap(listener, heartbeat)


async def test_restart_cycles_leave_no_heartbeat_or_client() -> None:
    """Supervisor-style restarts leave no heartbeat task and no client alive."""
    refs: list[weakref.ref[FcmPushClient[None]]] = []
    try:
        for _ in range(20):
            io = _Io()
            pc = _client(io)
            async with asyncio.timeout(HANG_BOUND_S):
                listener, heartbeat = await _start_and_fail(pc, io)
                # Wait until the listener reached STOPPED by itself, so stop()
                # takes the early path and not the regular one.
                await asyncio.wait({listener})
                await pc.stop()
            refs.append(weakref.ref(pc))
            del pc, io, listener, heartbeat
        gc.collect()
        alive_heartbeats = [
            t
            for t in asyncio.all_tasks()
            if t.get_name() == HEARTBEAT_TASK_NAME and not t.done()
        ]
        assert alive_heartbeats == []
        assert sum(r() is not None for r in refs) == 0
    finally:
        await _reap(
            *(t for t in asyncio.all_tasks() if t.get_name() == HEARTBEAT_TASK_NAME)
        )


async def test_stop_waits_for_listener_writer_close_without_cancelling_it() -> None:
    """stop() awaits the listener's own writer close instead of cancelling it."""
    io = _Io(block_close=True)
    # A large bound keeps stop()'s timeout out of the picture.
    pc = _client(io, writer_close_timeout=3600)
    listener: asyncio.Task[None] | None = None
    heartbeat: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(HANG_BOUND_S):
            listener, heartbeat = await _start_and_fail(pc, io)
            await io.close_entered.wait()
            stop_task = asyncio.create_task(pc.stop())
            # The heartbeat ends only when stop() cancels it, so stop() is now
            # waiting for the listener.
            await asyncio.wait({heartbeat})
            assert not stop_task.done()
            io.close_release.set()
            await asyncio.wait({stop_task})
        assert heartbeat.cancelled()
        assert listener.done()
        assert not listener.cancelled()
        assert stop_task.exception() is None
        assert pc.run_state is FcmPushClientRunState.STOPPED
    finally:
        io.close_release.set()
        await _reap(listener, heartbeat, stop_task)


async def test_stop_bounds_wait_for_hanging_listener() -> None:
    """A listener that never finishes its close is cancelled after the bound."""
    io = _Io(block_close=True)
    pc = _client(io, writer_close_timeout=0.05)
    listener: asyncio.Task[None] | None = None
    heartbeat: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(HANG_BOUND_S):
            listener, heartbeat = await _start_and_fail(pc, io)
            await io.close_entered.wait()
            stop_task = asyncio.create_task(pc.stop())
            await asyncio.wait({stop_task})
        assert not stop_task.cancelled()
        assert stop_task.exception() is None
        assert listener.cancelled()
        assert heartbeat.cancelled()
        assert pc.run_state is FcmPushClientRunState.STOPPED
    finally:
        await _reap(listener, heartbeat, stop_task)


async def test_cancelled_stop_does_not_cancel_spared_listener() -> None:
    """Cancelling stop() itself leaves the listener to finish its close."""
    io = _Io(block_close=True)
    # A large bound keeps stop()'s timeout out of the picture.
    pc = _client(io, writer_close_timeout=3600)
    listener: asyncio.Task[None] | None = None
    heartbeat: asyncio.Task[None] | None = None
    stop_task: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(HANG_BOUND_S):
            listener, heartbeat = await _start_and_fail(pc, io)
            await io.close_entered.wait()
            stop_task = asyncio.create_task(pc.stop())
            await asyncio.wait({heartbeat})
            stop_task.cancel()
            await asyncio.wait({stop_task})
            assert stop_task.cancelled()
            assert not listener.done()
            io.close_release.set()
            await asyncio.wait({listener})
        assert not listener.cancelled()
    finally:
        io.close_release.set()
        await _reap(listener, heartbeat, stop_task)


async def test_concurrent_stop_returns_after_running_stop_finished() -> None:
    """A second stop() during a regular stop() returns only once all tasks end."""
    io = _Io(block_close=True)
    pc = _client(io)
    await pc.start()
    listener = pc._listen_task
    assert listener is not None
    heartbeat = next(t for t in pc.tasks if t is not listener)
    first: asyncio.Task[None] | None = None
    second: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(HANG_BOUND_S):
            await io.receiving.wait()
            first = asyncio.create_task(pc.stop())
            await io.close_entered.wait()
            second = asyncio.create_task(pc.stop())
            io.close_release.set()
            await asyncio.wait({second})
            assert listener.done()
            assert heartbeat.done()
            assert first.done()
        assert second.exception() is None
    finally:
        io.close_release.set()
        await _reap(listener, heartbeat, first, second)
