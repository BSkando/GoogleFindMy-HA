# custom_components/googlefindmy/SpotApi/spot_request.py
from __future__ import annotations

import asyncio
import logging
import random
import socket
from collections.abc import Collection
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

if TYPE_CHECKING:

    class _UnaryStreamContext(Protocol):
        async def __aenter__(self) -> _UnaryStreamContext: ...
        async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> Any: ...
        async def send_message(self, payload: bytes, end: bool) -> None: ...
        async def recv_message(self) -> bytes | None: ...

    class UnaryUnaryMethod:
        def __init__(self, *args: Any, **kwargs: Any) -> None: ...

        def open(self, *args: Any, **kwargs: Any) -> _UnaryStreamContext: ...

    class Status:
        UNAUTHENTICATED: Status
        PERMISSION_DENIED: Status
        RESOURCE_EXHAUSTED: Status
        UNAVAILABLE: Status
        INTERNAL: Status
        UNKNOWN: Status
        DEADLINE_EXCEEDED: Status
        name: str

    class _GrpcError(Exception):
        status: Status

    class _ProtocolError(Exception): ...

    class _GrpclibExceptions(Protocol):
        GRPCError: type[_GrpcError]
        ProtocolError: type[_ProtocolError]
        StreamTerminatedError: type[_ProtocolError]

    class _GrpclibClient(Protocol):
        USER_AGENT: str

    grpclib_client = cast(_GrpclibClient, object())
    grpclib_exceptions = cast(_GrpclibExceptions, object())
else:
    import grpclib.client as grpclib_client
    import grpclib.exceptions as grpclib_exceptions
    from grpclib.client import UnaryUnaryMethod
    from grpclib.const import Status

from custom_components.googlefindmy.Auth.adm_token_retrieval import (
    async_get_adm_token as async_get_adm_token_api,
)
from custom_components.googlefindmy.Auth.spot_token_retrieval import (
    async_get_spot_token,
)
from custom_components.googlefindmy.Auth.token_cache import TokenCache
from custom_components.googlefindmy.Auth.token_retrieval import InvalidAasTokenError
from custom_components.googlefindmy.Auth.username_provider import async_get_username
from custom_components.googlefindmy.const import DATA_AAS_TOKEN
from custom_components.googlefindmy.exceptions import MissingTokenCacheError
from custom_components.googlefindmy.SpotApi.spot_grpc_transport import (
    SPOT_GRPC_TRANSPORT,
    SpotGrpcTransport,
    is_ssl_transport_teardown_error,
)

_LOGGER = logging.getLogger(__name__)

_SPOT_MAX_RETRIES: Final[int] = 3
_SPOT_INITIAL_BACKOFF_S: Final[float] = 1.0
_SPOT_BACKOFF_FACTOR: Final[float] = 2.0
_SPOT_MAX_BACKOFF_S: Final[float] = 60.0
_SPOT_REQUEST_TIMEOUT_S: Final[float] = 30.0
_USER_AGENT: Final[str] = (
    "com.google.android.gms/244433022 grpc-java-cronet/1.69.0-SNAPSHOT"
)

# WARNING: This mutates global grpclib state and affects all users in the process.
# Ideally this should be set per-channel, but grpclib doesn't support that cleanly.
cast(Any, grpclib_client).USER_AGENT = _USER_AGENT

# Indirection for test mocking
_async_sleep = asyncio.sleep


class SpotError(Exception):
    """Base exception for SPOT request failures."""


class SpotAuthPermanentError(SpotError):
    """Authentication failed after refresh; re-authentication is required."""


class SpotRateLimitError(SpotError):
    """Rate limited after bounded retries."""


class SpotGrpcStatusError(SpotError):
    """Non-auth gRPC status error outside the retry policy."""


class SpotNetworkError(SpotError):
    """Transport-layer error after bounded retries."""


class SpotTrailersOnlyError(SpotError):
    """OK status with missing or empty payload after retries."""


class SpotRequestFailedAfterRetries(SpotError):
    """Transient failures exhausted the retry budget."""


def _format_grpc_detail(err: Any) -> str:
    """Render the server-sent gRPC detail (message/details) for a SpotError message.

    R9a: ``GRPCError.message`` is the server-set ``grpc-message`` trailer and
    ``GRPCError.details`` the optional structured details (grpclib 0.4.9). Both are
    server status text, not credentials, so they are safe to surface; the auth
    bearer token is never part of this exception. Returns a leading ``": ..."``
    suffix when any detail is present, otherwise an empty string. Only the
    ``GRPCError`` branch may call this; the SpotNetworkError branches carry no
    ``.message`` and would raise ``AttributeError``.
    """
    parts: list[str] = []
    message = getattr(err, "message", None)
    if message:
        parts.append(str(message))
    details = getattr(err, "details", None)
    if details:
        parts.append(str(details))
    return f": {' '.join(parts)}" if parts else ""


def _compute_delay(attempt: int) -> float:
    """Compute exponential backoff with jitter bounded to sixty seconds."""

    base = (_SPOT_BACKOFF_FACTOR ** (attempt - 1)) * _SPOT_INITIAL_BACKOFF_S
    return min(random.uniform(0.0, base), _SPOT_MAX_BACKOFF_S)


async def _pick_auth_token_async(
    *, prefer_adm: bool = False, cache: TokenCache
) -> tuple[str, str, str]:
    """Select an authentication token using the entry-scoped cache."""

    if cache is None:
        raise MissingTokenCacheError()

    username = await async_get_username(cache=cache)
    if not username:
        raise RuntimeError("Username is not configured; cannot select auth token.")

    if not prefer_adm:
        try:
            spot_token = await async_get_spot_token(username, cache=cache)
        except InvalidAasTokenError:
            raise
        except Exception as err:  # pragma: no cover - defensive fallback to ADM
            _LOGGER.debug("SPOT token retrieval failed: %s", err)
        else:
            if spot_token:
                return spot_token, "spot", username

    adm_token = await async_get_adm_token_api(username, cache=cache)
    if adm_token:
        return adm_token, "adm", username

    raise RuntimeError("No valid SPOT or ADM token available for the current user.")


async def _invalidate_token_async(
    kind: str, username: str, *, cache: TokenCache | None = None
) -> None:
    """Invalidate cached tokens in the entry-scoped cache only."""

    if cache is None:
        raise MissingTokenCacheError()

    if kind == "spot":
        await cache.set(f"spot_token_{username}", None)
    elif kind == "adm":
        await cache.set(f"adm_token_{username}", None)
    elif kind == "aas":
        # Intentional, explicit clear of the quasi-permanent AAS master token.
        # This branch is only taken for a genuinely non-scoped kind; a scoped
        # (spot/adm) auth failure is routinely transient (an expired scoped
        # token), and nulling the AAS there would destroy the parent credential
        # in the volatile cache and force a user-visible reauth, even though the
        # retry can mint a fresh scoped token from the still-valid AAS. A
        # genuinely revoked AAS is normally handled via _clear_aas_token_async /
        # InvalidAasTokenError; this explicit "aas" kind mirrors that intent.
        await cache.set(DATA_AAS_TOKEN, None)
    else:
        # Defensive whitelist tail: an unrecognized token kind must NEVER fall
        # through to the destructive AAS-master clear. The sole caller
        # (_pick_auth_token_async) currently yields only "spot"/"adm"; a future
        # third scoped kind added there would otherwise silently null the parent
        # AAS credential (the exact destructive default this hardening removes).
        # Log and no-op instead, leaving every cached token untouched.
        _LOGGER.warning(
            "Unexpected token kind %r in _invalidate_token_async; no token "
            "cleared (AAS master preserved)",
            kind,
        )


async def _clear_aas_token_async(*, cache: TokenCache | None = None) -> None:
    """Clear the cached AAS token in the entry-scoped cache."""

    if cache is None:
        raise MissingTokenCacheError()

    await cache.set(DATA_AAS_TOKEN, None)


async def async_spot_request(
    api_scope: str,
    payload: bytes,
    *,
    cache: TokenCache,
    transport: SpotGrpcTransport | None = None,
) -> bytes:
    """
    Perform a SPOT unary gRPC request using grpclib.

    Design intent:
    - Reuse the shared grpclib channel for HTTP/2 multiplexing.
    - Preserve entry-scoped token isolation for multi-account setups.
    - Retry bounded times for transient statuses and rate limits.
    - Refresh authentication once before surfacing permanent failures.
    - Treat empty replies as transport anomalies before raising trailers-only.

    Retry budget:
    - Up to _SPOT_MAX_RETRIES (3) retries for transient errors.
    - One auth refresh attempt on UNAUTHENTICATED/PERMISSION_DENIED.
    - One AAS token clear attempt on InvalidAasTokenError (counts toward retry budget).
    - Maximum total attempts: _SPOT_MAX_RETRIES + 1 (initial) + 1 (auth refresh).
    """

    active_transport = transport or SPOT_GRPC_TRANSPORT
    method_path = f"/google.internal.spot.v1.SpotService/{api_scope}"

    refreshed_once = False
    retries_used = 0
    aas_cleared_once = False

    while True:
        attempt = retries_used + 1
        prefer_adm = refreshed_once

        try:
            token, token_kind, token_user = await _pick_auth_token_async(
                prefer_adm=prefer_adm,
                cache=cache,
            )
        except InvalidAasTokenError as err:
            if not aas_cleared_once:
                aas_cleared_once = True
                await _clear_aas_token_async(cache=cache)
                # Count this as a retry to prevent infinite loops
                retries_used += 1
                if retries_used > _SPOT_MAX_RETRIES:
                    raise SpotAuthPermanentError(
                        "AAS token invalid; retry budget exhausted."
                    ) from err
                continue
            raise SpotAuthPermanentError("AAS token invalid after refresh.") from err

        metadata: Collection[tuple[str, str]] = (("authorization", f"Bearer {token}"),)
        channel = await active_transport.get_channel()
        method = UnaryUnaryMethod(channel, method_path, bytes, bytes)

        try:
            async with method.open(
                metadata=metadata, timeout=_SPOT_REQUEST_TIMEOUT_S
            ) as stream:
                await stream.send_message(payload, end=True)
                reply_bytes = await stream.recv_message()
        except grpclib_exceptions.GRPCError as err:
            status = err.status

            if status in (Status.UNAUTHENTICATED, Status.PERMISSION_DENIED):
                if not refreshed_once:
                    refreshed_once = True
                    await _invalidate_token_async(token_kind, token_user, cache=cache)
                    continue
                raise SpotAuthPermanentError(
                    "Authentication failed after refresh."
                ) from err

            if status == Status.RESOURCE_EXHAUSTED:
                if retries_used < _SPOT_MAX_RETRIES:
                    retries_used += 1
                    await _async_sleep(_compute_delay(attempt))
                    continue
                raise SpotRateLimitError("Rate limited after retries.") from err

            if status in (
                Status.UNAVAILABLE,
                Status.INTERNAL,
                Status.UNKNOWN,
                Status.DEADLINE_EXCEEDED,
            ):
                if retries_used < _SPOT_MAX_RETRIES:
                    retries_used += 1
                    await _async_sleep(_compute_delay(attempt))
                    continue
                raise SpotRequestFailedAfterRetries(
                    f"Transient gRPC error ({status.name}) after retries"
                    f"{_format_grpc_detail(err)}."
                ) from err

            raise SpotGrpcStatusError(
                f"gRPC error: {status.name}{_format_grpc_detail(err)}"
            ) from err

        except (
            grpclib_exceptions.ProtocolError,
            grpclib_exceptions.StreamTerminatedError,
            ConnectionResetError,
            BrokenPipeError,
        ) as err:
            await active_transport.reset()
            if retries_used < _SPOT_MAX_RETRIES:
                retries_used += 1
                await _async_sleep(_compute_delay(attempt))
                continue
            raise SpotNetworkError("Fatal transport error after retries.") from err

        except AttributeError as err:
            # AP9 (finding 6a): asyncio SSL-transport teardown race. grpclib
            # wrote to an SSL transport whose protocol was already detached
            # (idle/serverside close, GOAWAY, or a concurrent reset), raising
            # ``AttributeError: 'NoneType' ... '_write_appdata'`` from inside
            # asyncio.sslproto. Treat it like the other transient transport
            # failures: reset the (possibly poisoned) channel and retry within
            # budget, converting to SpotNetworkError when exhausted. Any other
            # AttributeError is a genuine bug in our own stream handling and is
            # re-raised unchanged.
            if not is_ssl_transport_teardown_error(err):
                raise
            await active_transport.reset()
            if retries_used < _SPOT_MAX_RETRIES:
                retries_used += 1
                await _async_sleep(_compute_delay(attempt))
                continue
            raise SpotNetworkError("SSL transport teardown after retries.") from err

        except TimeoutError as err:
            if retries_used < _SPOT_MAX_RETRIES:
                retries_used += 1
                await _async_sleep(_compute_delay(attempt))
                continue
            # The channel is shared, so only the last attempt may close it, and
            # only if no other caller has replaced it in the meantime.
            await active_transport.reset(expected=channel)
            raise SpotNetworkError("Timeout after retries.") from err

        except (OSError, socket.gaierror) as err:
            if retries_used < _SPOT_MAX_RETRIES:
                retries_used += 1
                await _async_sleep(_compute_delay(attempt))
                continue
            await active_transport.reset()
            raise SpotNetworkError("Transport error after retries.") from err

        if reply_bytes is None or len(reply_bytes) == 0:
            if retries_used < _SPOT_MAX_RETRIES:
                retries_used += 1
                await _async_sleep(_compute_delay(attempt))
                continue
            raise SpotTrailersOnlyError("OK status but empty reply payload.")

        assert isinstance(reply_bytes, (bytes, bytearray))
        return bytes(reply_bytes)


def spot_request(*_args: object, **_kwargs: object) -> bytes:
    """Deprecated sync shim preserved for legacy call sites."""

    raise RuntimeError(
        "spot_request is no longer synchronous; use async_spot_request with cache="
    )
