# tests/helpers/dead_ipv6_path.py
"""Reproduce a dead network path without leaving the test process.

A real IPv6 blackhole drops the SYN, so ``connect()`` hangs until the kernel
gives up. The helper patches the two loop methods asyncio's connection code
calls: ``getaddrinfo`` returns a scripted address list, and ``sock_connect``
never returns for a listed address. All other addresses reach the real socket,
so a loopback server still sees a real connection.

``hung_addresses`` and ``hung_sockets`` record what was entered, ``entered`` lets a
test wait for that without a clock, and ``abandoned_hangs`` counts hangs that stayed
stuck until asyncio cancelled them. A test asserts on them, because a hang that was
never entered proves nothing.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import pytest

AddressInfo = tuple[socket.AddressFamily, socket.SocketKind, int, str, tuple[Any, ...]]


def v4_info(address: str, port: int) -> AddressInfo:
    """Return a ``getaddrinfo`` entry for an IPv4 address."""
    return (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (address, port))


def v6_info(address: str, port: int) -> AddressInfo:
    """Return a ``getaddrinfo`` entry for an IPv6 address."""
    return (
        socket.AF_INET6,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        "",
        (address, port, 0, 0),
    )


@dataclass
class DeadPath:
    """Record of what the patched loop methods saw."""

    hung_addresses: list[str] = field(default_factory=list)
    hung_sockets: list[socket.socket] = field(default_factory=list)
    entered: asyncio.Event = field(default_factory=asyncio.Event)
    abandoned_hangs: int = 0
    resolver_entered: bool = False


def install_dead_path(
    monkeypatch: pytest.MonkeyPatch,
    loop: asyncio.AbstractEventLoop,
    addresses: Sequence[AddressInfo],
    *,
    dead: frozenset[str] = frozenset(),
    resolver_hangs: bool = False,
) -> DeadPath:
    """Patch ``loop`` so that every address in ``dead`` hangs on connect."""
    state = DeadPath()
    real_sock_connect = loop.sock_connect

    async def fake_getaddrinfo(*_args: Any, **_kwargs: Any) -> list[AddressInfo]:
        state.resolver_entered = True
        if resolver_hangs:
            await asyncio.Event().wait()
        return list(addresses)

    async def fake_sock_connect(sock: socket.socket, address: Any) -> None:
        if address[0] in dead:
            state.hung_addresses.append(address[0])
            state.hung_sockets.append(sock)
            state.entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                state.abandoned_hangs += 1
                raise
        await real_sock_connect(sock, address)

    monkeypatch.setattr(loop, "getaddrinfo", fake_getaddrinfo)
    monkeypatch.setattr(loop, "sock_connect", fake_sock_connect)
    return state
