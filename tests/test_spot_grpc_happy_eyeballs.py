# tests/test_spot_grpc_happy_eyeballs.py
"""Spot requests survive a dead IPv6 path, and an exhausted channel heals.

The shared channel connected sequentially, so on a host whose IPv6 route drops
packets every attempt hung on the AAAA address until the request deadline, and
IPv4 was never tried. These tests drive the public gRPC call path against a
loopback HTTP/2 server; only the resolver and the dead addresses are scripted
(``tests/helpers/dead_ipv6_path.py``). What the server received carries every
success claim. They run in real time on purpose: the behavior under test is the
timing itself, and deadlines are short so that no test waits for long.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import logging
import socket
import ssl
import textwrap
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock

import grpclib.client
import h2.config
import h2.connection
import h2.events
import pytest
import pytest_asyncio
from grpclib.client import UnaryUnaryMethod
from grpclib.config import Configuration

from custom_components.googlefindmy.Auth.token_cache import TokenCache
from custom_components.googlefindmy.SpotApi import spot_grpc_transport, spot_request
from custom_components.googlefindmy.SpotApi.spot_grpc_transport import (
    Channel,
    RawCodec,
    SpotGrpcTransport,
)
from custom_components.googlefindmy.SpotApi.spot_request import SpotNetworkError
from tests.helpers.dead_ipv6_path import (
    AddressInfo,
    DeadPath,
    install_dead_path,
    v4_info,
    v6_info,
)
from tests.helpers.self_signed_tls import self_signed_server_context

pytestmark = pytest.mark.asyncio

TEST_HOST = "spot.test"
OVERRIDE_HOST = "override.test"
METHOD_PATH = "/google.internal.spot.v1.SpotService/Test"
V4_LIVE = "127.0.0.1"
V6_LIVE = "::1"
V6_DEAD_A = "2001:db8::a"
V6_DEAD_B = "2001:db8::b"
V6_DEAD_C = "2001:db8::c"
V4_DEAD = "192.0.2.1"
DEAD_ADDRESSES = frozenset({V4_DEAD, V6_DEAD_A, V6_DEAD_B, V6_DEAD_C})
# A connect that hangs misses this deadline, which is the red result on the
# unfixed code.
REACH_DEADLINE_S = 2.0
# Up to here the delay of 0.25 s has run out and the second address has connected.
REACH_MAX_ELAPSED_S = 1.5
SHORT_DEADLINE_S = 0.1
# The token lookup that reads the cache is patched out.
UNUSED_CACHE = cast(TokenCache, object())

# Copy of grpclib's method (0.4.7 to 0.4.9). The subclass mirrors its arguments, so
# a change here means the subclass needs the same change. 0.4.5 differs: see
# test_upstream_create_connection_matches_the_mirrored_copy.
UPSTREAM_CREATE_CONNECTION = """
async def _create_connection(self) -> H2Protocol:
    if self._path is not None:
        _, protocol = await self._loop.create_unix_connection(
            self._protocol_factory,
            self._path,
            ssl=self._ssl,
            server_hostname=(
                self._config.ssl_target_name_override
                if self._ssl is not None else None
            ),
        )
    else:
        _, protocol = await self._loop.create_connection(
            self._protocol_factory,
            self._host,
            self._port,
            ssl=self._ssl,
            server_hostname=(
                self._config.ssl_target_name_override
                if self._ssl is not None else None
            ),
        )
    return protocol
"""


@dataclass
class GrpcServer:
    """Loopback HTTP/2 server recording what actually arrived."""

    port: int = 0
    connections: int = 0
    requests: list[bytes] = field(default_factory=list)
    negotiated_protocols: list[str | None] = field(default_factory=list)
    peer_families: list[socket.AddressFamily] = field(default_factory=list)


ServerFactory = Callable[..., Awaitable[GrpcServer]]


def _grpc_payload(frames: bytes) -> bytes:
    """Strip the gRPC length prefix of the single message in ``frames``."""
    return frames[5:]


async def _serve_http2(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    record: GrpcServer,
) -> None:
    """Answer each gRPC request of one connection with the message ``ok``."""
    connection = h2.connection.H2Connection(
        config=h2.config.H2Configuration(client_side=False)
    )
    connection.initiate_connection()
    writer.write(connection.data_to_send())
    await writer.drain()
    bodies: dict[int, bytearray] = {}
    while data := await reader.read(65535):
        for event in connection.receive_data(data):
            if isinstance(event, h2.events.RequestReceived):
                assert event.stream_id is not None
                bodies[event.stream_id] = bytearray()
            elif isinstance(event, h2.events.DataReceived):
                assert event.stream_id is not None
                assert event.data is not None
                assert event.flow_controlled_length is not None
                bodies[event.stream_id].extend(event.data)
                connection.acknowledge_received_data(
                    event.flow_controlled_length, event.stream_id
                )
            elif isinstance(event, h2.events.StreamEnded):
                assert event.stream_id is not None
                record.requests.append(_grpc_payload(bytes(bodies[event.stream_id])))
                reply = b"ok"
                connection.send_headers(
                    event.stream_id,
                    [(":status", "200"), ("content-type", "application/grpc")],
                )
                connection.send_data(
                    event.stream_id,
                    b"\x00" + len(reply).to_bytes(4, "big") + reply,
                )
                connection.send_headers(
                    event.stream_id, [("grpc-status", "0")], end_stream=True
                )
        writer.write(connection.data_to_send())
        await writer.drain()


@pytest_asyncio.fixture
async def grpc_server() -> AsyncIterator[ServerFactory]:
    """Start recording servers on demand and close them with their handlers."""
    servers: list[asyncio.Server] = []
    handlers: set[asyncio.Task[None]] = set()

    async def start(
        host: str = V4_LIVE, *, tls: ssl.SSLContext | None = None
    ) -> GrpcServer:
        record = GrpcServer()

        async def handle(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            task = asyncio.current_task()
            assert task is not None
            handlers.add(task)
            record.connections += 1
            record.peer_families.append(writer.get_extra_info("socket").family)
            ssl_object = writer.get_extra_info("ssl_object")
            if ssl_object is not None:
                record.negotiated_protocols.append(ssl_object.selected_alpn_protocol())
            try:
                await _serve_http2(reader, writer, record)
            finally:
                writer.close()

        server = await asyncio.start_server(handle, host, 0, ssl=tls)
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
    """Join the SSL context thread; ``verify_cleanup`` counts threads."""
    yield
    await asyncio.get_running_loop().shutdown_default_executor()


def tls_server_context(tmp_path: Path, host: str) -> tuple[Path, ssl.SSLContext]:
    """Return a certificate path and a server context that negotiates ``h2``."""
    cert_path, server = self_signed_server_context(tmp_path, host)
    server.set_alpn_protocols(["h2"])
    return cert_path, server


def verifying_client_context(cert_path: Path) -> ssl.SSLContext:
    """Return a client context that verifies and offers ``h2``."""
    context = ssl.create_default_context(cafile=str(cert_path))
    context.set_alpn_protocols(["h2"])
    return context


async def call_ping(channel: grpclib.client.Channel, deadline: float) -> bytes | None:
    """Send ``ping`` over the public call path and return the reply."""
    method = UnaryUnaryMethod(channel, METHOD_PATH, bytes, bytes)
    async with method.open(timeout=deadline) as stream:
        await stream.send_message(b"ping", end=True)
        return await stream.recv_message()


ReachAddresses = Callable[[int], list[AddressInfo]]


def _reach_dead_one(port: int) -> list[AddressInfo]:
    return [v6_info(V6_DEAD_A, port), v4_info(V4_LIVE, port)]


def _reach_dead_two(port: int) -> list[AddressInfo]:
    return [v6_info(V6_DEAD_A, port), v6_info(V6_DEAD_B, port), v4_info(V4_LIVE, port)]


def _reach_dead_three(port: int) -> list[AddressInfo]:
    return [
        v6_info(V6_DEAD_A, port),
        v6_info(V6_DEAD_B, port),
        v6_info(V6_DEAD_C, port),
        v4_info(V4_LIVE, port),
    ]


REACH_CASES = [
    pytest.param(_reach_dead_one, id="one-dead-v6"),
    pytest.param(_reach_dead_two, id="two-dead-v6"),
    pytest.param(_reach_dead_three, id="three-dead-v6"),
]
USE_SSL_CASES = [pytest.param(False, id="plain"), pytest.param(True, id="tls")]


async def test_reach_case_set_is_complete() -> None:
    """The case set must not shrink unnoticed."""
    assert len(REACH_CASES) == 3
    assert len(USE_SSL_CASES) == 2


async def _start_for(
    use_ssl: bool,
    grpc_server: ServerFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    host: str = V4_LIVE,
) -> tuple[GrpcServer, SpotGrpcTransport]:
    """Start a server and a transport whose lazy context trusts that server."""
    if not use_ssl:
        server = await grpc_server(host)
        return server, SpotGrpcTransport(host=TEST_HOST, port=443, use_ssl=False)
    cert_path, server_context = tls_server_context(tmp_path, TEST_HOST)
    client_context = verifying_client_context(cert_path)
    monkeypatch.setattr(
        spot_grpc_transport.ssl,
        "create_default_context",
        lambda: client_context,
    )
    server = await grpc_server(host, tls=server_context)
    return server, SpotGrpcTransport(host=TEST_HOST, port=443, use_ssl=True)


def assert_request_arrived(server: GrpcServer, *, use_ssl: bool) -> None:
    """Assert the server-side observation that carries a success claim."""
    assert server.requests == [b"ping"]
    assert server.connections == 1
    assert server.negotiated_protocols == (["h2"] if use_ssl else [])


@pytest.mark.parametrize("use_ssl", USE_SSL_CASES)
@pytest.mark.parametrize("addresses", REACH_CASES)
async def test_live_ipv4_is_reached_behind_dead_ipv6(
    addresses: ReachAddresses,
    use_ssl: bool,
    grpc_server: ServerFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A request reaches IPv4 inside the deadline after entering the dead path."""
    server, transport = await _start_for(use_ssl, grpc_server, tmp_path, monkeypatch)
    state = install_dead_path(
        monkeypatch,
        asyncio.get_running_loop(),
        addresses(server.port),
        dead=DEAD_ADDRESSES,
    )
    channel = await transport.get_channel()
    started = time.monotonic()
    try:
        reply = await call_ping(channel, REACH_DEADLINE_S)
    finally:
        await transport.async_close()

    assert time.monotonic() - started < REACH_MAX_ELAPSED_S
    assert reply == b"ok"
    assert_request_arrived(server, use_ssl=use_ssl)
    assert server.peer_families == [socket.AF_INET]
    # asyncio alternates the families, so IPv4 runs second and only the first
    # dead address is entered, however many follow it.
    assert state.hung_addresses == [V6_DEAD_A]
    assert state.abandoned_hangs == 1
    assert [sock.fileno() for sock in state.hung_sockets] == [-1]


@pytest.mark.parametrize("use_ssl", USE_SSL_CASES)
async def test_control_live_ipv4_first_never_enters_dead_path(
    use_ssl: bool,
    grpc_server: ServerFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: a working first address connects without touching the dead one."""
    server, transport = await _start_for(use_ssl, grpc_server, tmp_path, monkeypatch)
    state = install_dead_path(
        monkeypatch,
        asyncio.get_running_loop(),
        [v4_info(V4_LIVE, server.port), v6_info(V6_DEAD_A, server.port)],
        dead=DEAD_ADDRESSES,
    )
    channel = await transport.get_channel()
    try:
        assert await call_ping(channel, REACH_DEADLINE_S) == b"ok"
    finally:
        await transport.async_close()

    assert_request_arrived(server, use_ssl=use_ssl)
    assert server.peer_families == [socket.AF_INET]
    assert state.hung_addresses == []


@pytest.mark.parametrize("use_ssl", USE_SSL_CASES)
async def test_control_healthy_ipv6_stays_preferred(
    use_ssl: bool,
    grpc_server: ServerFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: a working IPv6 path is still used; no failure without a reason."""
    server, transport = await _start_for(
        use_ssl, grpc_server, tmp_path, monkeypatch, host=V6_LIVE
    )
    state = install_dead_path(
        monkeypatch,
        asyncio.get_running_loop(),
        [v6_info(V6_LIVE, server.port), v4_info(V4_DEAD, server.port)],
        dead=DEAD_ADDRESSES,
    )
    channel = await transport.get_channel()
    try:
        assert await call_ping(channel, REACH_DEADLINE_S) == b"ok"
    finally:
        await transport.async_close()

    assert_request_arrived(server, use_ssl=use_ssl)
    assert server.peer_families == [socket.AF_INET6]
    assert state.hung_addresses == []


@dataclass
class ConnectionSpy:
    """Record ``create_connection`` calls and return a marker instead of connecting."""

    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = field(default_factory=list)
    protocol: object = field(default_factory=object)

    async def __call__(self, *args: Any, **kwargs: Any) -> tuple[None, object]:
        self.calls.append((args, kwargs))
        return None, self.protocol


ARGUMENT_CASES = [
    pytest.param(False, None, None, id="plain-no-override"),
    pytest.param(True, None, None, id="tls-no-override"),
    pytest.param(False, "x", None, id="plain-override"),
    pytest.param(True, "x", "x", id="tls-override"),
]


async def test_argument_case_set_is_complete() -> None:
    """The case set must not shrink unnoticed."""
    assert len(ARGUMENT_CASES) == 4


@pytest.mark.parametrize(("use_ssl", "override", "server_hostname"), ARGUMENT_CASES)
async def test_arguments_match_upstream_plus_delay(
    use_ssl: bool,
    override: str | None,
    server_hostname: str | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The subclass passes what upstream passes, plus exactly the delay."""
    loop = asyncio.get_running_loop()
    spy = ConnectionSpy()
    monkeypatch.setattr(loop, "create_connection", spy)
    context = ssl.create_default_context() if use_ssl else None
    channel = Channel(
        TEST_HOST,
        443,
        ssl=context,
        codec=RawCodec(),
        config=Configuration(ssl_target_name_override=override),
    )

    assert await channel._create_connection() is spy.protocol
    assert await grpclib.client.Channel._create_connection(channel) is spy.protocol

    ours, upstream = spy.calls
    assert ours[0] == upstream[0] == (channel._protocol_factory, TEST_HOST, 443)
    assert upstream[1] == {"ssl": context, "server_hostname": server_hostname}
    assert ours[1] == {
        "ssl": context,
        "server_hostname": server_hostname,
        "happy_eyeballs_delay": 0.25,
    }


async def test_public_call_path_reaches_the_override(
    grpc_server: ServerFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """grpclib must still call the override from its public call path."""
    loop = asyncio.get_running_loop()
    seen: list[dict[str, Any]] = []
    real = loop.create_connection

    async def recording(*args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs)
        return await real(*args, **kwargs)

    server = await grpc_server()
    monkeypatch.setattr(loop, "create_connection", recording)
    transport = SpotGrpcTransport(host=V4_LIVE, port=server.port, use_ssl=False)
    channel = await transport.get_channel()
    try:
        assert await call_ping(channel, REACH_DEADLINE_S) == b"ok"
    finally:
        await transport.async_close()

    assert isinstance(channel, grpclib.client.Channel)
    assert server.requests == [b"ping"]
    assert [call.get("happy_eyeballs_delay") for call in seen] == [0.25]


async def test_tls_uses_target_name_override_behind_dead_ipv6(
    grpc_server: ServerFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The certificate is valid only for the override, so a lost name cannot pass."""
    cert_path, server_context = tls_server_context(tmp_path, OVERRIDE_HOST)
    server = await grpc_server(tls=server_context)
    state = install_dead_path(
        monkeypatch,
        asyncio.get_running_loop(),
        [v6_info(V6_DEAD_A, server.port), v4_info(V4_LIVE, server.port)],
        dead=DEAD_ADDRESSES,
    )
    channel = Channel(
        TEST_HOST,
        443,
        ssl=verifying_client_context(cert_path),
        codec=RawCodec(),
        config=Configuration(ssl_target_name_override=OVERRIDE_HOST),
    )
    try:
        assert await call_ping(channel, REACH_DEADLINE_S) == b"ok"
    finally:
        channel.close()

    assert_request_arrived(server, use_ssl=True)
    assert state.hung_addresses == [V6_DEAD_A]


async def test_plain_channel_ignores_target_name_override(
    grpc_server: ServerFactory,
) -> None:
    """asyncio rejects a server name without TLS, so the override must stay out."""
    server = await grpc_server()
    channel = Channel(
        V4_LIVE,
        server.port,
        codec=RawCodec(),
        config=Configuration(ssl_target_name_override=OVERRIDE_HOST),
    )
    try:
        assert await call_ping(channel, REACH_DEADLINE_S) == b"ok"
    finally:
        channel.close()

    assert server.requests == [b"ping"]


FALLBACK_CASES = [
    pytest.param("_host", False, id="host"),
    pytest.param("_port", False, id="port"),
    pytest.param("_ssl", False, id="ssl"),
    # _config is only read for a TLS channel.
    pytest.param("_config", True, id="config"),
]


async def test_fallback_case_set_is_complete() -> None:
    """The case set must not shrink unnoticed."""
    assert len(FALLBACK_CASES) == 4


@pytest.mark.parametrize(("attribute", "use_ssl"), FALLBACK_CASES)
async def test_missing_private_attribute_falls_back_with_warning(
    attribute: str,
    use_ssl: bool,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Changed grpclib internals mean the old connect, and a visible warning."""
    marker = object()
    upstream = AsyncMock(return_value=marker)
    monkeypatch.setattr(grpclib.client.Channel, "_create_connection", upstream)
    connection_spy = ConnectionSpy()
    monkeypatch.setattr(asyncio.get_running_loop(), "create_connection", connection_spy)
    channel = Channel(
        TEST_HOST,
        443,
        ssl=ssl.create_default_context() if use_ssl else None,
        codec=RawCodec(),
    )
    delattr(channel, attribute)

    with caplog.at_level(logging.WARNING, logger=spot_grpc_transport.__name__):
        assert await channel._create_connection() is marker

    upstream.assert_awaited_once()
    assert connection_spy.calls == []
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert [r.getMessage() for r in warnings] == [
        "grpclib internals changed; connecting without Happy Eyeballs"
    ]


async def test_control_network_error_does_not_fall_back(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Control: a refused connection stays an error and is no internals change."""
    upstream = AsyncMock()
    monkeypatch.setattr(grpclib.client.Channel, "_create_connection", upstream)
    with socket.socket() as probe:
        probe.bind((V4_LIVE, 0))
        closed_port = probe.getsockname()[1]
    channel = Channel(V4_LIVE, closed_port, codec=RawCodec())

    with (
        caplog.at_level(logging.WARNING, logger=spot_grpc_transport.__name__),
        pytest.raises(ConnectionRefusedError),
    ):
        await channel._create_connection()

    upstream.assert_not_awaited()
    assert caplog.records == []


async def test_upstream_create_connection_matches_the_mirrored_copy() -> None:
    """Fail on the first grpclib whose method differs from what the subclass mirrors."""

    def normal_form(source: str) -> str:
        return ast.dump(ast.parse(textwrap.dedent(source)))

    actual = inspect.getsource(grpclib.client.Channel._create_connection)
    assert normal_form(actual) == normal_form(UPSTREAM_CREATE_CONNECTION), (
        f"grpclib {grpclib.__version__} changed Channel._create_connection. Compare "
        "Channel._create_connection in SpotApi/spot_grpc_transport.py, then update "
        f"the subclass and this literal. New source:\n{actual}"
    )


@dataclass
class ResetProbe:
    """Count real resets and the connect attempts entered before each of them."""

    transport: SpotGrpcTransport
    state: DeadPath
    attempts_at_reset: list[int] = field(default_factory=list)
    expected: list[Channel | None] = field(default_factory=list)

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        real_reset = self.transport.reset

        async def reset(*, expected: Channel | None = None) -> None:
            self.attempts_at_reset.append(len(self.state.hung_addresses))
            self.expected.append(expected)
            await real_reset(expected=expected)

        monkeypatch.setattr(self.transport, "reset", reset)


def patch_request_budget(
    monkeypatch: pytest.MonkeyPatch,
    *,
    before_retry: Callable[[], None] | None = None,
) -> None:
    """Shorten the deadline, skip the backoff and supply a token."""

    async def no_sleep(_delay: float) -> None:
        if before_retry is not None:
            before_retry()

    monkeypatch.setattr(spot_request, "_SPOT_REQUEST_TIMEOUT_S", SHORT_DEADLINE_S)
    monkeypatch.setattr(spot_request, "_async_sleep", no_sleep)
    monkeypatch.setattr(
        spot_request,
        "_pick_auth_token_async",
        AsyncMock(return_value=("token", "aas", "user")),
    )


async def test_single_timeout_then_success_keeps_the_channel(
    grpc_server: ServerFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retry that succeeds reuses the shared channel and resets nothing."""
    loop = asyncio.get_running_loop()
    server = await grpc_server()
    state = install_dead_path(
        monkeypatch, loop, [v6_info(V6_DEAD_A, server.port)], dead=DEAD_ADDRESSES
    )
    transport = SpotGrpcTransport(host=TEST_HOST, port=443, use_ssl=False)
    probe = ResetProbe(transport, state)
    probe.install(monkeypatch)
    patch_request_budget(
        monkeypatch,
        before_retry=lambda: install_dead_path(
            monkeypatch, loop, [v4_info(V4_LIVE, server.port)]
        ),
    )
    channel = await transport.get_channel()
    try:
        reply = await spot_request.async_spot_request(
            "Test", b"ping", cache=UNUSED_CACHE, transport=transport
        )
        assert await transport.get_channel() is channel
    finally:
        await transport.async_close()

    assert reply == b"ok"
    assert state.hung_addresses == [V6_DEAD_A]
    assert server.requests == [b"ping"]
    assert probe.attempts_at_reset == []


async def test_exhausted_timeouts_reset_the_channel_after_the_last_attempt(
    grpc_server: ServerFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dead channel is dropped once, at the end, and the next request heals."""
    loop = asyncio.get_running_loop()
    server = await grpc_server()
    state = install_dead_path(
        monkeypatch, loop, [v6_info(V6_DEAD_A, server.port)], dead=DEAD_ADDRESSES
    )
    transport = SpotGrpcTransport(host=TEST_HOST, port=443, use_ssl=False)
    probe = ResetProbe(transport, state)
    probe.install(monkeypatch)
    patch_request_budget(monkeypatch)
    channel = await transport.get_channel()
    try:
        with pytest.raises(SpotNetworkError, match="Timeout after retries"):
            await spot_request.async_spot_request(
                "Test", b"ping", cache=UNUSED_CACHE, transport=transport
            )

        # One attempt plus three retries, and the reset only after all four.
        assert state.hung_addresses == [V6_DEAD_A] * 4
        assert probe.attempts_at_reset == [4]
        assert probe.expected == [channel]

        install_dead_path(monkeypatch, loop, [v4_info(V4_LIVE, server.port)])
        replacement = await transport.get_channel()
        assert replacement is not channel
        assert await call_ping(replacement, REACH_DEADLINE_S) == b"ok"
    finally:
        await transport.async_close()

    assert server.requests == [b"ping"]


async def test_late_reset_leaves_a_channel_another_caller_rebuilt_open(
    grpc_server: ServerFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed request must not close the healthy channel that replaced its own."""
    loop = asyncio.get_running_loop()
    server = await grpc_server()
    state = install_dead_path(
        monkeypatch, loop, [v6_info(V6_DEAD_A, server.port)], dead=DEAD_ADDRESSES
    )
    transport = SpotGrpcTransport(host=TEST_HOST, port=443, use_ssl=False)
    real_reset = transport.reset
    replaced: list[Channel] = []
    late_resets: list[Channel | None] = []

    async def reset_after_replacement(*, expected: Channel | None = None) -> None:
        # Another caller got here first: it dropped the channel, built a new one
        # and already uses it, before this late reset takes the lock.
        await real_reset()
        install_dead_path(monkeypatch, loop, [v4_info(V4_LIVE, server.port)])
        replacement = await transport.get_channel()
        assert await call_ping(replacement, REACH_DEADLINE_S) == b"ok"
        replaced.append(replacement)
        late_resets.append(expected)
        await real_reset(expected=expected)

    monkeypatch.setattr(transport, "reset", reset_after_replacement)
    patch_request_budget(monkeypatch)
    channel = await transport.get_channel()
    try:
        with pytest.raises(SpotNetworkError, match="Timeout after retries"):
            await spot_request.async_spot_request(
                "Test", b"ping", cache=UNUSED_CACHE, transport=transport
            )

        assert late_resets == [channel]
        assert len(replaced) == 1
        assert await transport.get_channel() is replaced[0]
        assert await call_ping(replaced[0], REACH_DEADLINE_S) == b"ok"
    finally:
        await transport.async_close()

    assert state.hung_addresses == [V6_DEAD_A] * 4
    # Two requests over one connection: the channel stayed open between them.
    assert server.requests == [b"ping", b"ping"]
    assert server.connections == 1
