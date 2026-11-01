# custom_components/googlefindmy/SpotApi/spot_grpc_transport.py
from __future__ import annotations

import asyncio
import logging
import ssl
from functools import cache
from typing import Final, override

from grpclib.client import Channel as _GrpclibChannel
from grpclib.encoding.base import CodecBase
from grpclib.protocol import H2Protocol

__all__ = [
    "RawCodec",
    "SpotGrpcTransport",
    "SPOT_GRPC_TRANSPORT",
    "is_ssl_transport_teardown_error",
]

_LOGGER = logging.getLogger(__name__)

# RFC 8305 section 5 recommended delay; matches aiohttp's default in Home Assistant.
_HAPPY_EYEBALLS_DELAY_S: Final[float] = 0.25


@cache
def _warn_without_happy_eyeballs() -> None:
    """Warn once per process: every reconnect would otherwise repeat the line."""
    _LOGGER.warning("grpclib internals changed; connecting without Happy Eyeballs")


def is_ssl_transport_teardown_error(exc: BaseException) -> bool:
    """Return True for the asyncio SSL-transport teardown race (AP9, finding 6a).

    During a connection teardown there is a narrow window in which asyncio has
    already detached the SSL transport (``_SSLProtocolTransport._ssl_protocol``
    is ``None``) but grpclib's ``handler.connection_lost`` flag is not set yet.
    A write that lands in that window raises
    ``AttributeError: 'NoneType' object has no attribute '_write_appdata'`` from
    deep inside ``asyncio.sslproto`` — code we do not own, so we cannot attach a
    custom exception type at the throw site. Isolating the fragile asyncio
    signature in this single predicate is the legitimate boundary discriminator:
    every other ``AttributeError`` is a real bug and must propagate.
    """
    return isinstance(exc, AttributeError) and "_write_appdata" in str(exc)


class RawCodec(CodecBase):
    """
    A grpclib codec that treats messages as opaque bytes.

    This integration intentionally bypasses protobuf serialization in grpclib.
    Callers provide already-serialized request bytes and receive raw response bytes.

    Compatibility requirement (critical):
    Google's SPOT frontend is strict about the gRPC media type. The on-wire HTTP/2
    header must be exactly:

        content-type: application/grpc

    and must not include any "+proto" or other suffix.

    grpclib derives content-type validation from `__content_subtype__` but allows
    codecs to advertise a custom base content type. This codec disables the
    subtype and explicitly reports the base "application/grpc" media type so
    strict servers accept it without a "+proto" suffix.
    """

    __content_subtype__: str = "proto"

    @property
    def content_type(self) -> str:
        """
        Return the base gRPC media type.

        This must remain exactly "application/grpc" to avoid strict backend rejects.
        """
        return "application/grpc"

    def encode(self, message: bytes, _message_type: type) -> bytes:
        """
        Return outgoing message bytes without transformation.

        Raises:
            TypeError: If `message` is not bytes-like.
        """
        if isinstance(message, bytes):
            return message
        if isinstance(message, (bytearray, memoryview)):
            return bytes(message)
        raise TypeError(f"RawCodec expects bytes-like, got {type(message)!r}")

    def decode(self, data: bytes, _message_type: type) -> bytes:
        """
        Return response payload bytes without transformation.

        Raises:
            TypeError: If `data` is not bytes-like.
        """
        if isinstance(data, bytes):
            return data
        if isinstance(data, (bytearray, memoryview)):
            return bytes(data)
        raise TypeError(f"RawCodec expects bytes-like, got {type(data)!r}")


class Channel(_GrpclibChannel):
    """grpclib channel that falls back between address families (RFC 8305).

    grpclib offers no public parameter, so this overrides a private method.
    TCP only; the transport never passes ``path``. If grpclib's private
    attributes change, fall back to its own sequential connect. If grpclib renames
    the method, it stops calling this override; ``SpotGrpcTransport.get_channel``
    warns about that.
    """

    @override
    async def _create_connection(self) -> H2Protocol:
        try:
            factory = self._protocol_factory
            host, port = self._host, self._port
            ssl_context = self._ssl
            server_hostname = (
                self._config.ssl_target_name_override
                if ssl_context is not None
                else None
            )
        except AttributeError:
            _warn_without_happy_eyeballs()
            return await super()._create_connection()
        transport, protocol = await asyncio.get_running_loop().create_connection(
            factory,
            host,
            port,
            ssl=ssl_context,
            server_hostname=server_hostname,
            happy_eyeballs_delay=_HAPPY_EYEBALLS_DELAY_S,
        )
        _LOGGER.debug(
            "Connected to Spot endpoint (%s,%s) over %s",
            host,
            port,
            transport.get_extra_info("socket").family.name,
        )
        return protocol


class SpotGrpcTransport:
    """
    Shared grpclib transport for SPOT gRPC calls.

    Design goals:
    - Lazy initialization: no sockets/SSL at import time.
    - Non-blocking SSL context creation using asyncio.to_thread.
    - HTTP/2 ALPN negotiation via "h2".
    - Concurrency safety for channel lifecycle using an internal asyncio.Lock.
    - Explicit reset/close primitives for callers to use on fatal transport errors.
    """

    def __init__(
        self,
        *,
        host: str = "spot-pa.googleapis.com",
        port: int = 443,
        use_ssl: bool = True,
        codec: CodecBase | None = None,
    ) -> None:
        self._host: Final[str] = host
        self._port: Final[int] = port
        self._use_ssl: Final[bool] = use_ssl
        self._codec: Final[CodecBase] = codec or RawCodec()

        self._lock = asyncio.Lock()
        self._channel: Channel | None = None
        self._ssl_context: ssl.SSLContext | None = None

    async def _get_ssl_context(self) -> ssl.SSLContext:
        """
        Lazily create and cache an SSLContext without blocking the event loop.

        Returns:
            ssl.SSLContext: Context configured for gRPC over HTTP/2 (ALPN "h2").
        """
        if self._ssl_context is not None:
            return self._ssl_context

        ctx = await asyncio.to_thread(ssl.create_default_context)
        try:
            ctx.set_alpn_protocols(["h2"])
        except NotImplementedError:
            pass
        except AttributeError:
            pass

        self._ssl_context = ctx
        return ctx

    async def get_channel(self) -> Channel:
        """
        Return a shared grpclib Channel, creating it lazily and safely.

        Returns:
            grpclib.client.Channel: A reusable HTTP/2 channel suitable for multiplexing.

        Notes:
            This method is concurrency-safe. Exactly one channel is created per transport
            instance unless `reset()` / `async_close()` clears it.
        """
        if self._channel is not None:
            return self._channel

        async with self._lock:
            if self._channel is not None:
                return self._channel

            if not hasattr(_GrpclibChannel, "_create_connection"):
                _warn_without_happy_eyeballs()
            ssl_ctx = await self._get_ssl_context() if self._use_ssl else None
            self._channel = Channel(
                self._host, self._port, ssl=ssl_ctx, codec=self._codec
            )
            return self._channel

    async def reset(self, *, expected: Channel | None = None) -> None:
        """
        Close and drop the cached channel so the next call recreates it.

        This is intended for callers to use on fatal channel/protocol failures
        where a clean reconnection is required.

        Args:
            expected: Close only if this is still the cached channel. A caller
                whose request failed passes its own channel, so a late reset
                cannot close a healthy channel another caller already rebuilt.
        """
        async with self._lock:
            if self._channel is None:
                return
            if expected is not None and self._channel is not expected:
                return
            self._channel.close()
            self._channel = None

    async def async_close(self) -> None:
        """
        Idempotently close the cached channel.

        This should be awaited during integration unload when the transport is no
        longer needed.
        """
        async with self._lock:
            if self._channel is None:
                return
            self._channel.close()
            self._channel = None


SPOT_GRPC_TRANSPORT = SpotGrpcTransport()
