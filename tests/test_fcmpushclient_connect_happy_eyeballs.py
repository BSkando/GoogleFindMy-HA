# tests/test_fcmpushclient_connect_happy_eyeballs.py
"""The MCS connect survives a dead IPv6 path, and its socket work ends inside the limit.

On a host whose IPv6 route exists but drops packets, the sequential connect hung
on the AAAA address until the kernel gave up, and the idle watchdog restarted the
client first, so IPv4 was never tried. These tests drive the real ``_connect``
against loopback servers; only the resolver and the dead addresses are scripted
(``tests/helpers/dead_ipv6_path.py``). A server-side observation carries every
success claim. asyncio's real timers stay, because the traffic is real loopback
traffic that a frozen clock would stall. No result depends on how fast they run:
REACH_BOUND_S only turns a hang into a red test, the limit is patched down so that
no test waits for 12 s, and a limit can only be overrun, never undercut, by load.
"""

from __future__ import annotations

import asyncio
import logging
import random
import socket
import ssl
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import pytest_asyncio

from custom_components.googlefindmy import const as integration_const
from custom_components.googlefindmy.Auth.firebase_messaging import const as mcs_const
from custom_components.googlefindmy.Auth.firebase_messaging import fcmpushclient
from custom_components.googlefindmy.Auth.firebase_messaging.fcmpushclient import (
    FcmPushClient,
    FcmPushClientConfig,
    FcmPushClientRunState,
)
from tests.helpers.dead_ipv6_path import DeadPath, install_dead_path, v4_info, v6_info
from tests.helpers.self_signed_tls import self_signed_server_context

pytestmark = pytest.mark.asyncio

TEST_HOST = "mtalk.test"
V4_LIVE = "127.0.0.1"
V6_LIVE = "::1"
V4_DEAD = "192.0.2.1"
V6_DEAD_A = "2001:db8::a"
V6_DEAD_B = "2001:db8::b"
V6_DEAD_C = "2001:db8::c"
DEAD_ADDRESSES = frozenset({V4_DEAD, V6_DEAD_A, V6_DEAD_B, V6_DEAD_C})
# Failure bound of a single test, not a timing claim: only a hang reaches it, which
# is the red result on the unfixed code. A bound by asyncio.timeout would raise the
# same type as the limit under test and hide a missing limit.
REACH_BOUND_S = 10.0
PATCHED_LIMIT_S = 0.5
# How long a hang must hold. Load can only extend this window, so it cannot flake.
HANG_WINDOW_S = 0.1
# Taken before any test patches the name the client resolves on the ssl module.
REAL_CREATE_DEFAULT_CONTEXT = ssl.create_default_context


@dataclass
class Certificate:
    """Self-signed certificate for ``TEST_HOST`` and the contexts built on it."""

    path: Path
    server: ssl.SSLContext

    def client_context(self) -> ssl.SSLContext:
        """Return a verifying client context; TLS verification stays enabled."""
        return REAL_CREATE_DEFAULT_CONTEXT(cafile=str(self.path))


@dataclass
class TlsServer:
    """Loopback TLS server recording what actually arrived."""

    port: int = 0
    handshakes: int = 0
    payload: bytearray = field(default_factory=bytearray)
    received: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class SilentServer:
    """Loopback TCP server that accepts and never answers the TLS hello."""

    port: int = 0
    accepted: int = 0
    closed_by_peer: asyncio.Event = field(default_factory=asyncio.Event)


@pytest.fixture
def certificate(tmp_path: Path) -> Certificate:
    """Create a certificate valid for ``TEST_HOST``."""
    cert_path, server = self_signed_server_context(tmp_path, TEST_HOST)
    return Certificate(path=cert_path, server=server)


ServerFactory = Callable[[str], Awaitable[TlsServer]]


@pytest_asyncio.fixture
async def tls_server(certificate: Certificate) -> AsyncIterator[ServerFactory]:
    """Start recording TLS servers on demand and close them with their handlers."""
    servers: list[asyncio.Server] = []
    handlers: set[asyncio.Task[None]] = set()

    async def start(host: str) -> TlsServer:
        record = TlsServer()

        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            task = asyncio.current_task()
            assert task is not None
            handlers.add(task)
            record.handshakes += 1
            try:
                record.payload.extend(await reader.read(1))
                record.received.set()
                await reader.read()
            finally:
                writer.close()

        server = await asyncio.start_server(handle, host, 0, ssl=certificate.server)
        servers.append(server)
        record.port = server.sockets[0].getsockname()[1]
        return record

    yield start
    # abort_clients first: wait_closed() waits for open connections, so a test that
    # failed before closing its client would turn into a hang instead of a red result.
    for server in servers:
        server.close()
        server.abort_clients()
        await server.wait_closed()
    for task in handlers:
        task.cancel()
    await asyncio.gather(*handlers, return_exceptions=True)


@pytest_asyncio.fixture(autouse=True)
async def join_default_executor() -> AsyncIterator[None]:
    """Join the executor thread of ``_connect``; ``verify_cleanup`` counts threads."""
    yield
    await asyncio.get_running_loop().shutdown_default_executor()


def make_client(
    monkeypatch: pytest.MonkeyPatch, certificate: Certificate | None, port: int
) -> FcmPushClient:
    """Build a client with the real connect path and only what it reads."""
    client = FcmPushClient.__new__(FcmPushClient)
    client.logger = logging.getLogger(f"{fcmpushclient.__name__}.FcmPushClient.test")
    client.config = FcmPushClientConfig()
    client.do_listen = True
    client.reader = None
    client.writer = None
    monkeypatch.setattr(fcmpushclient, "MCS_HOST", TEST_HOST)
    monkeypatch.setattr(fcmpushclient, "MCS_PORT", port)
    if certificate is not None:
        monkeypatch.setattr(
            fcmpushclient.ssl, "create_default_context", certificate.client_context
        )
    return client


async def close_client(client: FcmPushClient) -> None:
    """Close the stream the connect left open."""
    writer = client.writer
    assert writer is not None
    writer.close()
    await writer.wait_closed()


async def run_bounded(task: asyncio.Task[bool]) -> tuple[bool, float]:
    """Return the result of ``task``; a task that outlives the bound fails the test."""
    started = time.monotonic()
    try:
        done, _ = await asyncio.wait({task}, timeout=REACH_BOUND_S)
        assert done, f"_connect still hanging after {REACH_BOUND_S} s"
        return task.result(), time.monotonic() - started
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def start_connect(client: FcmPushClient, *, eager: bool) -> asyncio.Task[bool]:
    """Start ``_connect`` as a normal or eagerly started task."""
    if eager:
        return asyncio.Task(
            client._connect(), loop=asyncio.get_running_loop(), eager_start=True
        )
    return asyncio.create_task(client._connect())


REACH_LISTS: list[tuple[str, tuple[str, ...]]] = [
    ("one_dead_v6", (V6_DEAD_A,)),
    ("two_dead_v6", (V6_DEAD_A, V6_DEAD_B)),
    ("three_dead_v6", (V6_DEAD_A, V6_DEAD_B, V6_DEAD_C)),
]
REACH_CASES = [
    pytest.param(dead, eager, id=f"{name}-{'eager' if eager else 'normal'}")
    for name, dead in REACH_LISTS
    for eager in (False, True)
]


@pytest.mark.parametrize(("dead_v6", "eager"), REACH_CASES)
async def test_live_ipv4_is_reached_behind_dead_ipv6(
    monkeypatch: pytest.MonkeyPatch,
    certificate: Certificate,
    tls_server: ServerFactory,
    dead_v6: tuple[str, ...],
    eager: bool,
) -> None:
    """The IPv4 server gets handshake and payload while the IPv6 path hangs."""
    server = await tls_server(V4_LIVE)
    loop = asyncio.get_running_loop()
    infos = [v6_info(addr, server.port) for addr in dead_v6]
    infos.append(v4_info(V4_LIVE, server.port))
    state = install_dead_path(monkeypatch, loop, infos, dead=DEAD_ADDRESSES)
    client = make_client(monkeypatch, certificate, server.port)

    spy_kwargs: dict[str, Any] = {}
    real_create_connection = loop.create_connection

    async def spy(*args: Any, **kwargs: Any) -> Any:
        spy_kwargs.update(kwargs)
        return await real_create_connection(*args, **kwargs)

    monkeypatch.setattr(loop, "create_connection", spy)

    connected, _ = await run_bounded(start_connect(client, eager=eager))

    assert connected is True
    assert state.hung_addresses[0] == V6_DEAD_A
    assert state.abandoned_hangs == len(state.hung_addresses)
    assert spy_kwargs["happy_eyeballs_delay"] == 0.25
    client.writer.write(b"x")
    await client.writer.drain()
    await asyncio.wait_for(server.received.wait(), REACH_BOUND_S)
    assert server.handshakes == 1
    assert bytes(server.payload) == b"x"
    assert client.writer.get_extra_info("socket").family == socket.AF_INET
    await close_client(client)


async def test_control_live_ipv4_first_never_enters_dead_path(
    monkeypatch: pytest.MonkeyPatch,
    certificate: Certificate,
    tls_server: ServerFactory,
) -> None:
    """Control: with IPv4 first, the dead address is never entered."""
    server = await tls_server(V4_LIVE)
    infos = [v4_info(V4_LIVE, server.port), v6_info(V6_DEAD_A, server.port)]
    state = install_dead_path(
        monkeypatch, asyncio.get_running_loop(), infos, dead=DEAD_ADDRESSES
    )
    client = make_client(monkeypatch, certificate, server.port)

    connected, _ = await run_bounded(start_connect(client, eager=False))

    assert connected is True
    assert state.hung_addresses == []
    client.writer.write(b"x")
    await client.writer.drain()
    await asyncio.wait_for(server.received.wait(), REACH_BOUND_S)
    assert server.handshakes == 1
    await close_client(client)


async def test_control_healthy_ipv6_stays_preferred(
    monkeypatch: pytest.MonkeyPatch,
    certificate: Certificate,
    tls_server: ServerFactory,
) -> None:
    """Control: a working IPv6 path is used; the fix does not force IPv4."""
    v6_server = await tls_server(V6_LIVE)
    v4_server = await tls_server(V4_LIVE)
    infos = [v6_info(V6_LIVE, v6_server.port), v4_info(V4_LIVE, v4_server.port)]
    state = install_dead_path(monkeypatch, asyncio.get_running_loop(), infos)
    client = make_client(monkeypatch, certificate, v6_server.port)

    connected, _ = await run_bounded(start_connect(client, eager=False))

    assert connected is True
    assert state.hung_addresses == []
    assert client.writer.get_extra_info("socket").family == socket.AF_INET6
    client.writer.write(b"y")
    await client.writer.drain()
    await asyncio.wait_for(v6_server.received.wait(), REACH_BOUND_S)
    assert bytes(v6_server.payload) == b"y"
    assert v4_server.handshakes == 0
    await close_client(client)


async def _assert_limit_ended_attempt(
    client: FcmPushClient,
    caplog: pytest.LogCaptureFixture,
    test_owned_tasks: set[asyncio.Task[Any]] | None = None,
) -> None:
    """Run ``_connect`` and assert the limit ended it, with nothing left behind."""
    tasks_before = asyncio.all_tasks()
    with caplog.at_level(logging.DEBUG):
        connected, elapsed = await run_bounded(start_connect(client, eager=False))
    assert connected is False
    assert elapsed >= PATCHED_LIMIT_S * 0.9
    assert client.writer is None
    assert asyncio.all_tasks() - tasks_before - (test_owned_tasks or set()) == set()
    assert any(
        "Could not connect" in rec.getMessage() and "TimeoutError" in rec.getMessage()
        for rec in caplog.records
    )


DEAD_TCP_CASES = [
    pytest.param((V6_DEAD_A,), id="only_v6"),
    pytest.param((V4_DEAD,), id="only_v4"),
    pytest.param((V6_DEAD_A, V4_DEAD), id="v6_and_v4"),
]


async def test_limit_ends_hanging_resolver(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A resolver that never answers cannot outlast the limit."""
    monkeypatch.setattr(
        fcmpushclient, "MCS_CONNECT_TIMEOUT_S", PATCHED_LIMIT_S, raising=False
    )
    state = install_dead_path(
        monkeypatch, asyncio.get_running_loop(), [], resolver_hangs=True
    )
    client = make_client(monkeypatch, None, 5228)

    await _assert_limit_ended_attempt(client, caplog)

    assert state.resolver_entered is True


@pytest.mark.parametrize("dead", DEAD_TCP_CASES)
async def test_limit_ends_all_dead_addresses(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    dead: tuple[str, ...],
) -> None:
    """All addresses dead: the limit ends the attempt and closes every socket."""
    monkeypatch.setattr(
        fcmpushclient, "MCS_CONNECT_TIMEOUT_S", PATCHED_LIMIT_S, raising=False
    )
    # Every address must be entered before the limit, whatever the load.
    monkeypatch.setattr(
        fcmpushclient, "MCS_HAPPY_EYEBALLS_DELAY_S", 0.001, raising=False
    )
    infos = [
        v4_info(addr, 5228) if addr == V4_DEAD else v6_info(addr, 5228) for addr in dead
    ]
    state = install_dead_path(
        monkeypatch, asyncio.get_running_loop(), infos, dead=DEAD_ADDRESSES
    )
    client = make_client(monkeypatch, None, 5228)

    await _assert_limit_ended_attempt(client, caplog)

    assert state.hung_addresses == list(dead)
    assert [sock.fileno() for sock in state.hung_sockets] == [-1] * len(dead)


async def test_limit_ends_silent_tls_peer(
    monkeypatch: pytest.MonkeyPatch,
    certificate: Certificate,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A peer that accepts and stays silent is abandoned and sees the close."""
    record = SilentServer()
    server_handlers: set[asyncio.Task[Any]] = set()

    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        task = asyncio.current_task()
        assert task is not None
        server_handlers.add(task)
        record.accepted += 1
        try:
            await reader.read()
            record.closed_by_peer.set()
        finally:
            writer.close()

    server = await asyncio.start_server(handle, V4_LIVE, 0)
    record.port = server.sockets[0].getsockname()[1]
    monkeypatch.setattr(
        fcmpushclient, "MCS_CONNECT_TIMEOUT_S", PATCHED_LIMIT_S, raising=False
    )
    state = install_dead_path(
        monkeypatch,
        asyncio.get_running_loop(),
        [v4_info(V4_LIVE, record.port)],
    )
    client = make_client(monkeypatch, certificate, record.port)

    try:
        await _assert_limit_ended_attempt(client, caplog, server_handlers)
        await asyncio.wait_for(record.closed_by_peer.wait(), REACH_BOUND_S)
    finally:
        server.close()
        server.abort_clients()
        await server.wait_closed()

    assert record.accepted == 1
    assert state.hung_addresses == []


def _lower_bound(low: float, _high: float) -> float:
    return low


def _upper_bound(_low: float, high: float) -> float:
    return high


@pytest.mark.parametrize(
    ("uniform", "expected_total"),
    [
        pytest.param(_lower_bound, 77.05, id="lowest_jitter"),
        pytest.param(_upper_bound, 78.6, id="highest_jitter"),
    ],
)
async def test_five_failed_attempts_end_before_watchdog(
    monkeypatch: pytest.MonkeyPatch,
    uniform: Callable[[float, float], float],
    expected_total: float,
) -> None:
    """The real retry loop gives up before the idle watchdog restarts the client."""
    assert mcs_const.MCS_HAPPY_EYEBALLS_DELAY_S == 0.25
    assert mcs_const.MCS_CONNECT_TIMEOUT_S == 12.0
    assert integration_const.FCM_CONNECTION_RETRY_COUNT == 5
    assert integration_const.FCM_IDLE_RESET_AFTER_S == 90.0
    assert FcmPushClientConfig().connection_retry_count == 5
    idle_reset_s = integration_const.FCM_IDLE_RESET_AFTER_S

    clock = 0.0
    attempts = 0
    client = FcmPushClient.__new__(FcmPushClient)
    client.logger = logging.getLogger(f"{fcmpushclient.__name__}.FcmPushClient.test")
    client.do_listen = True
    client.config = FcmPushClientConfig(
        connection_retry_count=integration_const.FCM_CONNECTION_RETRY_COUNT
    )

    async def failed_attempt() -> bool:
        nonlocal clock, attempts
        attempts += 1
        clock += mcs_const.MCS_CONNECT_TIMEOUT_S
        return False

    async def fake_sleep(delay: float) -> None:
        nonlocal clock
        clock += delay

    monkeypatch.setattr(client, "_connect", failed_attempt)
    # The stub answers with the bounds the code asks for, so a wider range shows.
    monkeypatch.setattr(random, "uniform", uniform)
    with monkeypatch.context() as scoped:
        scoped.setattr(fcmpushclient.asyncio, "sleep", fake_sleep)
        result = await client._connect_with_retry()

    assert result is False
    assert client.run_state == FcmPushClientRunState.STARTING_CONNECTION
    assert attempts == 5
    assert clock == pytest.approx(expected_total)
    assert clock <= 78.6 + 1e-9
    assert clock < idle_reset_s


async def test_receiver_passes_retry_count_from_const() -> None:
    """The receiver hands the client the retry count the budget is computed from."""
    source = (Path(fcmpushclient.__file__).parents[1] / "fcm_receiver_ha.py").read_text(
        encoding="utf-8"
    )
    assert source.count("connection_retry_count=int(FCM_CONNECTION_RETRY_COUNT)") == 1


DIAGNOSTIC_FAMILIES = [
    pytest.param(V4_LIVE, "AF_INET", id="ipv4"),
    pytest.param(V6_LIVE, "AF_INET6", id="ipv6"),
]


@pytest.mark.parametrize(("address", "family"), DIAGNOSTIC_FAMILIES)
async def test_diagnostics_name_true_family_and_no_address(
    monkeypatch: pytest.MonkeyPatch,
    certificate: Certificate,
    tls_server: ServerFactory,
    caplog: pytest.LogCaptureFixture,
    address: str,
    family: str,
) -> None:
    """The log shows the attempt and the connected family, never an IP address."""
    server = await tls_server(address)
    info = v4_info if family == "AF_INET" else v6_info
    install_dead_path(
        monkeypatch, asyncio.get_running_loop(), [info(address, server.port)]
    )
    client = make_client(monkeypatch, certificate, server.port)

    with caplog.at_level(logging.DEBUG):
        connected, _ = await run_bounded(start_connect(client, eager=False))

    assert connected is True
    messages = [rec.getMessage() for rec in caplog.records]
    before = [m for m in messages if m.startswith("Connecting to MCS endpoint")]
    after = [m for m in messages if m.startswith("Connected to MCS endpoint")]
    assert len(before) == 1
    assert TEST_HOST in before[0]
    assert str(server.port) in before[0]
    assert "12.0" in before[0]
    assert len(after) == 1
    assert after[0].endswith(f"over {family}")
    assert not any(address in m for m in before + after)
    assert messages.index(before[0]) < messages.index(after[0])
    await close_client(client)


async def test_diagnostics_failed_attempt_has_no_success_line(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A failed attempt logs the attempt and no success line."""
    monkeypatch.setattr(
        fcmpushclient, "MCS_CONNECT_TIMEOUT_S", PATCHED_LIMIT_S, raising=False
    )
    install_dead_path(
        monkeypatch,
        asyncio.get_running_loop(),
        [v6_info(V6_DEAD_A, 5228)],
        dead=DEAD_ADDRESSES,
    )
    client = make_client(monkeypatch, None, 5228)

    with caplog.at_level(logging.DEBUG):
        connected, _ = await run_bounded(start_connect(client, eager=False))

    assert connected is False
    messages = [rec.getMessage() for rec in caplog.records]
    assert sum(m.startswith("Connecting to MCS endpoint") for m in messages) == 1
    assert not any(m.startswith("Connected to MCS endpoint") for m in messages)
    assert not any(V6_DEAD_A in m for m in messages)


async def test_diagnostics_survive_a_stream_without_socket(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A stream that reports no socket still connects; the log says unknown."""
    client = make_client(monkeypatch, None, 5228)
    writer = SimpleNamespace(get_extra_info=lambda _name: None)

    async def open_without_socket(
        **_kwargs: Any,
    ) -> tuple[object, object]:
        return object(), writer

    monkeypatch.setattr(client, "_open_connection", open_without_socket)

    with caplog.at_level(logging.DEBUG):
        connected, _ = await run_bounded(start_connect(client, eager=False))

    assert connected is True
    assert client.writer is writer
    messages = [rec.getMessage() for rec in caplog.records]
    after = [m for m in messages if m.startswith("Connected to MCS endpoint")]
    assert after == [f"Connected to MCS endpoint ({TEST_HOST},5228) over unknown"]


async def test_run_bounded_fails_and_cancels_a_hanging_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A hang must turn the test red and leave no task behind, not stall the suite."""
    monkeypatch.setattr(f"{__name__}.REACH_BOUND_S", 0.01)
    cancelled = asyncio.Event()

    async def hang() -> bool:
        never: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        try:
            return await never
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.create_task(hang())
    await asyncio.sleep(0)
    with pytest.raises(AssertionError, match="still hanging"):
        await run_bounded(task)
    assert cancelled.is_set()
    assert task.cancelled()


async def test_helper_hang_is_real_without_happy_eyeballs(
    monkeypatch: pytest.MonkeyPatch, certificate: Certificate, tls_server: ServerFactory
) -> None:
    """Helper check: a plain sequential connect stays stuck on the dead address."""
    server = await tls_server(V4_LIVE)
    infos = [v6_info(V6_DEAD_A, server.port), v4_info(V4_LIVE, server.port)]
    state: DeadPath = install_dead_path(
        monkeypatch, asyncio.get_running_loop(), infos, dead=DEAD_ADDRESSES
    )
    task = asyncio.create_task(
        asyncio.open_connection(
            host=TEST_HOST,
            port=server.port,
            ssl=certificate.client_context(),
        )
    )
    await asyncio.wait_for(state.entered.wait(), REACH_BOUND_S)
    done, _ = await asyncio.wait({task}, timeout=HANG_WINDOW_S)
    assert done == set()
    assert state.hung_addresses == [V6_DEAD_A]
    assert server.handshakes == 0
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert state.hung_sockets[0].fileno() == -1


async def test_helper_without_dead_entry_passes_through(
    monkeypatch: pytest.MonkeyPatch, certificate: Certificate, tls_server: ServerFactory
) -> None:
    """Helper check: without a dead address the connection reaches the server."""
    server = await tls_server(V4_LIVE)
    state = install_dead_path(
        monkeypatch,
        asyncio.get_running_loop(),
        [v4_info(V4_LIVE, server.port)],
    )
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(
            host=TEST_HOST, port=server.port, ssl=certificate.client_context()
        ),
        REACH_BOUND_S,
    )
    assert reader is not None
    writer.write(b"z")
    await writer.drain()
    await asyncio.wait_for(server.received.wait(), REACH_BOUND_S)
    assert bytes(server.payload) == b"z"
    assert state.hung_addresses == []
    writer.close()
    await writer.wait_closed()
