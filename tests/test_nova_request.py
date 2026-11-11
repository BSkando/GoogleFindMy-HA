# tests/test_nova_request.py
"""Tests for Nova API async request helpers and TTL policy."""

from __future__ import annotations

import ast
import asyncio
import logging
import re
import time
from collections import Counter
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import aiohttp
import pytest

MAX_INITIAL_CALLS = 2
UNAUTHORIZED_STATUS = 401
SUCCESS_STATUS = 200
EXPECTED_RETRY_COUNT = 2

from custom_components.googlefindmy.api import _EphemeralCache
from custom_components.googlefindmy.Auth.token_cache import TokenCache
from custom_components.googlefindmy.Auth.token_retrieval import InvalidAasTokenError
from custom_components.googlefindmy.Auth.username_provider import username_string
from custom_components.googlefindmy.const import (
    DATA_AAS_TOKEN,
    NOVA_REQUEST_TOTAL_TIMEOUT_S,
)
from custom_components.googlefindmy.NovaApi.ListDevices.nbe_list_devices import (
    async_request_device_list,
)
from custom_components.googlefindmy.NovaApi.nova_request import (
    AsyncTTLPolicy,
    NovaAuthError,
    NovaAuthPermanentError,
    NovaError,
    NovaHTTPError,
    NovaRateLimitError,
    TTLPolicy,
    async_nova_request,
    is_credential_rejection,
    observe_request_dispatch,
    register_cache_provider,
    unregister_cache_provider,
)


class _DummyResponse:
    """Minimal async context manager mimicking aiohttp.ClientResponse."""

    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body
        self.headers: dict[str, str] = {}

    async def read(self) -> bytes:
        return self._body

    async def __aenter__(self) -> _DummyResponse:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _DummySession:
    """Async session stub returning pre-seeded responses."""

    def __init__(self, responses: list[_DummyResponse]) -> None:
        self._responses = responses
        self.calls: list[dict[str, Any]] = []

    def post(self, *_args: object, **_kwargs: object) -> _DummyResponse:
        if not self._responses:
            raise AssertionError("No responses left for nova_request test")
        self.calls.append({"args": _args, "kwargs": _kwargs})
        return self._responses.pop(0)


class _FakeHass:
    """Minimal Home Assistant stub for TokenCache interactions."""

    async def async_add_executor_job(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        return func(*args, **kwargs)


class _StubCache:
    """Entry-scoped cache stub implementing the minimal async API."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    async def get(self, key: str) -> Any:
        return self._data.get(key)

    async def set(self, key: str, value: Any) -> None:
        if value is None:
            self._data.pop(key, None)
            return
        self._data[key] = value

    async def get_or_set(
        self, key: str, generator: Callable[[], Awaitable[Any] | Any]
    ) -> Any:
        if key in self._data:
            return self._data[key]
        result = generator()
        if asyncio.iscoroutine(result):
            result = await result
        await self.set(key, result)
        return result


async def test_async_nova_request_returns_auth_error_on_repeated_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure NovaAuthError is raised instead of NameError on 401 responses.

    After the initial 401 and token refresh, the code now retries 3 times
    with exponential backoff (6s, 12s, 24s) before raising NovaAuthError.
    This test provides 4 consecutive 401 responses to trigger the error.
    """

    cache = _StubCache()
    # Need 4 responses: 1 initial + 3 retries with backoff
    session = _DummySession(
        [
            _DummyResponse(401, b"<html><body>Unauthorized</body></html>"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
        ]
    )

    async def _exercise() -> None:
        refresh_results: asyncio.Queue[str] = asyncio.Queue()
        refresh_results.put_nowait("token-one")
        refresh_results.put_nowait("token-two")

        async def _refresh() -> str:
            return await refresh_results.get()

        async def _seed_initial(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        # Mock asyncio.sleep to skip the 6s+12s+24s backoff delays
        async def _instant_sleep(_: float) -> None:
            pass

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _seed_initial,
        )
        monkeypatch.setattr("asyncio.sleep", _instant_sleep)

        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            token="initial-token",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

    with pytest.raises(NovaAuthError) as err:
        await _exercise()

    assert err.value.status == 401
    assert isinstance(err.value.detail, str)
    assert "Unauthorized" in err.value.detail


async def test_async_nova_request_refreshes_token_after_initial_401(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 401 triggers an ADM refresh and retries with the rotated token."""

    cache = _StubCache()
    session = _DummySession(
        [
            _DummyResponse(401, b"unauthorized"),
            _DummyResponse(200, b"\xba\xad\xf0\r"),
        ]
    )

    adm_calls: list[str | None] = []
    refresh_calls: list[None] = []
    on_401_calls: list[bool] = []

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        adm_calls.append(username)
        return "adm-old"

    async def _refresh_override() -> str:
        refresh_calls.append(None)
        return "adm-new"

    original_on_401 = AsyncTTLPolicy.async_on_401

    async def _spy_on_401(self: AsyncTTLPolicy, adaptive_downshift: bool = True) -> Any:
        on_401_calls.append(adaptive_downshift)
        return await original_on_401(self, adaptive_downshift=adaptive_downshift)

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.AsyncTTLPolicy.async_on_401",
        _spy_on_401,
    )

    async def _exercise() -> tuple[str, Any]:
        await cache.set(DATA_AAS_TOKEN, "aas-original")
        result = await async_nova_request(
            "testScope",
            "deadbeef",
            username="user@example.com",
            cache=cache,
            session=session,
            refresh_override=_refresh_override,
        )
        final_aas = await cache.get(DATA_AAS_TOKEN)
        return result, final_aas

    result, final_aas = await _exercise()

    assert result == "baadf00d"
    assert final_aas == "aas-original"
    assert adm_calls == ["user@example.com"]
    assert len(refresh_calls) == 1
    assert len(on_401_calls) == 1
    assert len(session.calls) == EXPECTED_RETRY_COUNT
    second_headers = session.calls[1]["kwargs"].get("headers", {})
    assert second_headers.get("Authorization") == "Bearer adm-new"


class _CoordinatedSession:
    """Session stub orchestrating overlapping ADM refresh behavior."""

    def __init__(self, allow_refresh: asyncio.Event) -> None:
        self._allow_refresh = allow_refresh
        self._initial_calls = 0
        self.calls: list[dict[str, Any]] = []

    def post(self, *_args: object, **kwargs: Any) -> _DummyResponse:
        headers = kwargs.get("headers", {})
        auth = headers.get("Authorization")
        status: int
        body: bytes

        if auth == "Bearer initial-token":
            self._initial_calls += 1
            # With the pre-request gate, the second request waits before sending,
            # so we trigger allow_refresh on the FIRST call (not after MAX_INITIAL_CALLS).
            # This simulates the improved behavior where only one request triggers refresh.
            self._allow_refresh.set()
            status, body = 401, b"unauthorized"
        elif auth == "Bearer refreshed-token":
            status, body = 200, b"ok"
        else:  # pragma: no cover - defensive guard for unexpected headers
            raise AssertionError(f"Unexpected Authorization header: {auth!r}")

        self.calls.append(
            {
                "auth": auth,
                "status": status,
                "headers": dict(headers),
            }
        )
        return _DummyResponse(status, body)


async def test_async_nova_request_reuses_cached_token_after_recent_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Overlapping 401 retries reuse the freshly cached ADM token."""

    cache = _StubCache()
    username = "user@example.com"
    namespace = "entry-id"
    bare_token_key = f"adm_token_{username}"
    namespaced_token_key = f"{namespace}:{bare_token_key}"

    async def _exercise() -> tuple[list[str], list[dict[str, Any]], int]:
        allow_refresh = asyncio.Event()
        session = _CoordinatedSession(allow_refresh)

        await cache.set(bare_token_key, "initial-token")
        await cache.set(namespaced_token_key, "initial-token")

        refresh_calls = 0

        async def _fake_get_adm_token(
            user: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            assert user == username
            cached = await cache.get(bare_token_key)
            if isinstance(cached, str) and cached:
                return cached
            return "initial-token"

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )

        async def _refresh_override() -> str:
            nonlocal refresh_calls
            refresh_calls += 1
            await allow_refresh.wait()
            token = "refreshed-token"
            await cache.set(bare_token_key, token)
            await cache.set(namespaced_token_key, token)
            return token

        tasks = [
            asyncio.create_task(
                async_nova_request(
                    "scope",
                    "00",
                    username=username,
                    cache=cache,
                    session=session,
                    namespace=namespace,
                    refresh_override=_refresh_override,
                )
            )
            for _ in range(2)
        ]

        results = await asyncio.gather(*tasks)
        return results, session.calls, refresh_calls

    results, calls, refreshes = await _exercise()

    assert results == ["6f6b", "6f6b"]
    # With the pre-request gate, only ONE refresh should occur.
    # The second request waits for the first to complete, then uses the cached token.
    assert refreshes == 1

    statuses = [call["status"] for call in calls]
    # With pre-request gate: first request gets 401 and refreshes,
    # second request waits and then succeeds with refreshed token.
    # So we expect 1 unauthorized (from first request) and 2 successes.
    assert statuses.count(UNAUTHORIZED_STATUS) == 1
    assert statuses.count(SUCCESS_STATUS) == 2
    successful_auths = [
        call["auth"] for call in calls if call["status"] == SUCCESS_STATUS
    ]
    assert successful_auths == ["Bearer refreshed-token", "Bearer refreshed-token"]


async def test_device_list_namespace_override_does_not_double_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Namespace-aware overrides must not prefix keys twice when 401 triggers a refresh."""

    cache = _StubCache()
    namespace = "entry-double"
    username = "user@example.com"

    get_keys: list[str] = []
    set_keys: list[str] = []

    async def _cache_get_override(key: str) -> Any:
        get_keys.append(key)
        return await cache.get(key)

    async def _cache_set_override(key: str, value: Any) -> None:
        set_keys.append(key)
        await cache.set(key, value)

    session = _DummySession(
        [
            _DummyResponse(401, b"unauthorized"),
            _DummyResponse(200, b"\xde\xad\xbe\xef"),
        ]
    )

    async def _fake_initial_token(
        user: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        resolved = (user or username).lower()
        await cache.set(f"adm_token_{resolved}", "initial-token")
        return "initial-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_initial_token,
    )

    async def _refresh_override() -> str:
        token = "refreshed-token"
        await cache.set(f"adm_token_{username}", token)
        await cache.set(f"{namespace}:adm_token_{username}", token)
        return token

    async def _exercise() -> str:
        return await async_request_device_list(
            username,
            session=session,
            cache=cache,
            cache_get=_cache_get_override,
            cache_set=_cache_set_override,
            refresh_override=_refresh_override,
            namespace=namespace,
        )

    result_hex = await _exercise()

    assert result_hex == "deadbeef"
    double_prefixed = [
        key
        for key in [*get_keys, *set_keys]
        if key.startswith(f"{namespace}:{namespace}:")
    ]
    assert not double_prefixed
    assert f"{namespace}:adm_token_issued_at_{username}" in set_keys


async def test_async_ttl_policy_refresh_preserves_existing_startup_probe() -> None:  # noqa: PLR0915
    """401 refresh clears stale token keys without resetting startup probe counters."""

    async def _run() -> None:  # noqa: PLR0915
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-refresh")
        try:
            logger = logging.getLogger("test_async_ttl_policy_refresh")
            username = "user@example.com"
            namespace = "entry-refresh"
            bare_token_key = f"adm_token_{username}"
            namespaced_token_key = f"{namespace}:{bare_token_key}"
            issued_bare_key = f"adm_token_issued_at_{username}"
            issued_ns_key = f"{namespace}:{issued_bare_key}"
            probe_bare_key = f"adm_probe_startup_left_{username}"

            await cache.set(bare_token_key, "stale-cache-token")
            await cache.set(namespaced_token_key, "stale-ns-token")
            await cache.set(issued_bare_key, 1.0)
            await cache.set(issued_ns_key, 2.0)
            await cache.set(probe_bare_key, 1)

            minted_tokens = ["fresh-token"]
            header: dict[str, str] = {}

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                cached = await cache.get(bare_token_key)
                if isinstance(cached, str) and cached:
                    return cached
                if not minted_tokens:
                    raise AssertionError("Expected to mint a fresh ADM token")
                token = minted_tokens.pop(0)
                await cache.set(bare_token_key, token)
                return token

            policy = AsyncTTLPolicy(
                username=username,
                logger=logger,
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda bearer: header.__setitem__("value", bearer),
                ns_prefix=namespace,
            )

            issued_at = time.time() - 900
            await cache.set(policy.k_issued, issued_at)
            await cache.set(issued_bare_key, issued_at - 30)
            await cache.set(policy.k_startleft, 1)
            await cache.set(probe_bare_key, 1)

            assert await cache.get(policy.k_startleft) == 1
            assert await cache.get(probe_bare_key) == 1

            result = await policy.async_on_401()

            assert result == "fresh-token"
            assert not minted_tokens
            assert header["value"] == "Bearer fresh-token"
            assert await cache.get(bare_token_key) == "fresh-token"
            assert await cache.get(namespaced_token_key) == "fresh-token"

            updated_issued_ns = await cache.get(policy.k_issued)
            updated_issued_bare = await cache.get(issued_bare_key)
            assert isinstance(updated_issued_ns, (int, float))
            assert isinstance(updated_issued_bare, (int, float))
            assert updated_issued_ns >= issued_at
            assert updated_issued_bare >= issued_at

            assert await cache.get(policy.k_startleft) == 1
            assert await cache.get(probe_bare_key) == 1
        finally:
            await cache.close()

    await _run()


async def test_async_ttl_policy_clears_namespaced_aas_token_on_invalid_refresh() -> (
    None
):
    """Invalid AAS tokens remove both namespaced and bare cache keys."""

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-invalid-aas")
        try:
            namespace = "entry-invalid-aas"

            await cache.set(DATA_AAS_TOKEN, "seed-bare")
            await cache.set(f"{namespace}:{DATA_AAS_TOKEN}", "seed-ns")

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                raise InvalidAasTokenError("expired")

            policy = AsyncTTLPolicy(
                username="user@example.com",
                logger=logging.getLogger("test_async_ttl_invalid_aas"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            with pytest.raises(NovaAuthError):
                await policy._do_refresh_async(time.time())

            assert await cache.get(DATA_AAS_TOKEN) is None
            assert await cache.get(f"{namespace}:{DATA_AAS_TOKEN}") is None
        finally:
            await cache.close()

    await _run()


async def test_async_nova_request_fetches_token_when_not_supplied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nova should resolve an ADM token when `token` kwarg is omitted."""

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\x10\x20")])

    calls: list[dict[str, Any]] = []

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        calls.append(
            {
                "username": username,
                "cache": cache,
                "retries": retries,
                "backoff": backoff,
            }
        )
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="User@Example.COM",
            cache=cache,
            session=session,
        )

    result = await _exercise()

    assert result == "1020"
    assert calls and calls[0]["username"] == "user@example.com"
    assert session.calls
    headers = session.calls[0]["kwargs"].get("headers", {})
    assert headers.get("Authorization") == "Bearer resolved-token"


async def test_async_nova_request_returns_hex_only_on_http_200(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-None return is produced exclusively by an HTTP 200.

    The return contract IS the acceptance signal: ``async_nova_request`` returns
    the hex body only when Google answers 200, and raises on every other outcome
    (see the rejection/connection regressions below). That is what lets
    ``api.async_play_sound`` derive "command accepted" — and therefore "keep the
    cancel key" — purely from "the submitter returned", with no out-of-band
    dispatch signal to keep in sync. See IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY.
    """

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\x10\x20")])

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    result = await _exercise()

    assert result == "1020"  # acceptance signal: a value came back from a 200
    assert len(session.calls) == 1


async def test_async_nova_request_raises_before_wire_on_pre_dispatch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-dispatch failure raises and never returns a value.

    An invalid hex payload is rejected after token resolution but before the
    POST loop, so nothing is sent and the function raises rather than returns.
    The caller therefore never sees a result for an unsent command and keeps a
    previous play's cancel key intact.
    """

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\x10\x20")])

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "zz",  # invalid hex -> ValueError before the POST loop (pre-dispatch)
            username="user@example.com",
            cache=cache,
            session=session,
        )

    with pytest.raises(ValueError, match="Invalid hex payload"):
        await _exercise()

    assert session.calls == []  # nothing was sent


async def test_async_nova_request_raises_on_connection_setup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connect-phase failure raises NovaError flagged as *not* dispatched.

    A connect-phase timeout (``ConnectionTimeoutError``) raises the moment the
    ``async with session.post(...)`` context is *entered*, after token and
    payload resolution but before a single byte reaches the wire. The retry
    handler classifies it as pre-dispatch and re-raises a ``NovaError`` with
    ``dispatched is False``, so ``api.async_play_sound`` returns ``(False,
    None)`` and the coordinator keeps the still-valid cancel key of a previous
    ring intact (it can never have started a new ring).
    """

    class _ConnFailingResponse:
        """Context manager that raises on entry, mimicking a connect failure."""

        async def __aenter__(self) -> Any:
            raise aiohttp.ConnectionTimeoutError

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _ConnFailingSession:
        """Session stub whose every POST fails during connection setup."""

        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def post(self, *_args: object, **_kwargs: object) -> _ConnFailingResponse:
            # post() merely builds the context manager; the failure surfaces on
            # __aenter__, exactly like aiohttp's real connection acquisition.
            self.calls.append({"args": _args, "kwargs": _kwargs})
            return _ConnFailingResponse()

    cache = _StubCache()
    session = _ConnFailingSession()

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    # Collapse the retry backoff so the test stays fast across NOVA_MAX_RETRIES.
    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    # Connection errors are retried, then surface as NovaError once exhausted.
    with pytest.raises(NovaError) as exc_info:
        await _exercise()

    # Pre-dispatch: the request never reached the wire, so the cancel key must
    # be dropped (no new ring could have started).
    assert exc_info.value.dispatched is False
    assert session.calls  # post() *was* attempted (and retried), only entry failed


async def test_async_nova_request_network_retry_uses_tiered_log_level(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The network-exception retry path logs the first attempt at INFO, not WARNING.

    A single transient network blip is not actionable, so attempt 1 must be INFO
    to avoid log noise, while repeated failures (attempt 2+) escalate to WARNING.
    This mirrors the tiered severity already used by the HTTP-status retry path in
    the same function. The previous code logged WARNING from the very first
    attempt, which spammed the log on momentary connectivity hiccups that the
    built-in retry recovered from on its own.
    """

    class _ConnFailingResponse:
        """Context manager that raises on entry, mimicking a connect failure."""

        async def __aenter__(self) -> Any:
            raise aiohttp.ConnectionTimeoutError

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _ConnFailingSession:
        """Session stub whose every POST fails during connection setup."""

        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def post(self, *_args: object, **_kwargs: object) -> _ConnFailingResponse:
            self.calls.append({"args": _args, "kwargs": _kwargs})
            return _ConnFailingResponse()

    cache = _StubCache()
    session = _ConnFailingSession()

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    # Collapse the retry backoff so the test stays fast across NOVA_MAX_RETRIES.
    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    caplog.set_level(
        logging.INFO, logger="custom_components.googlefindmy.NovaApi.nova_request"
    )

    # Exhaust the retries: every attempt fails pre-connect, so the loop logs the
    # first retry, then escalates, then raises once retries are spent.
    with pytest.raises(NovaError):
        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    retry_records = [r for r in caplog.records if "Retrying in" in r.getMessage()]
    first_attempt = [r for r in retry_records if "(Attempt 1/" in r.getMessage()]
    later_attempts = [r for r in retry_records if "(Attempt 2/" in r.getMessage()]

    # Mutation sentinel: the first retry is INFO. If the tiering is removed and
    # the code logs WARNING from attempt 1 again, this assertion turns red.
    assert len(first_attempt) == 1
    assert first_attempt[0].levelno == logging.INFO
    # Escalation still works: a repeated failure surfaces at WARNING.
    assert later_attempts
    assert all(r.levelno == logging.WARNING for r in later_attempts)


async def test_async_nova_request_marks_read_phase_failure_dispatched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A post-send read failure raises NovaError flagged as *dispatched*.

    The ``async with session.post(...)`` context is entered (the server
    responded with headers), but reading the body raises ``ServerDisconnectedError``.
    The request therefore reached the wire and may have started a ring, so the
    retry handler re-raises a ``NovaError`` with ``dispatched is True`` and
    ``api.async_play_sound`` keeps the cancel key for a later Stop.
    """

    class _ReadFailingResponse:
        """Response whose body read fails after the context is entered."""

        def __init__(self) -> None:
            self.status = 200
            self.headers: dict[str, str] = {}

        async def read(self) -> bytes:
            raise aiohttp.ServerDisconnectedError("peer closed during read")

        async def __aenter__(self) -> _ReadFailingResponse:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _ReadFailingSession:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def post(self, *_args: object, **_kwargs: object) -> _ReadFailingResponse:
            self.calls.append({"args": _args, "kwargs": _kwargs})
            return _ReadFailingResponse()

    cache = _StubCache()
    session = _ReadFailingSession()

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    # Collapse the retry backoff so the test stays fast across NOVA_MAX_RETRIES.
    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    # Read-phase errors are retried, then surface as a *dispatched* NovaError.
    with pytest.raises(NovaError) as exc_info:
        await _exercise()

    assert exc_info.value.dispatched is True
    assert session.calls  # the request reached the wire on every attempt


async def test_async_nova_request_latches_dispatch_across_mixed_retry_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dispatch latches across the whole retry sequence, not the last attempt.

    Regression for the Codex iter-6 finding: when an *early* attempt reaches the
    wire and fails post-send (``ServerDisconnectedError`` during read), a ring
    may already be active. If a *later* retry then fails pre-connect
    (``ConnectionTimeoutError``), deriving ``dispatched`` from the final
    exception alone would wrongly report ``False`` and drop the cancel key,
    leaving the device ringing. The retry handler must therefore latch
    ``dispatched is True`` the moment any attempt reaches the wire and keep it
    set regardless of how a subsequent attempt fails.
    """

    class _ReadFailingCtx:
        """Post-send failure: context enters, body read raises."""

        def __init__(self) -> None:
            self.status = 200
            self.headers: dict[str, str] = {}

        async def read(self) -> bytes:
            raise aiohttp.ServerDisconnectedError("peer closed during read")

        async def __aenter__(self) -> _ReadFailingCtx:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _ConnFailingCtx:
        """Pre-connect failure: context raises on entry."""

        async def __aenter__(self) -> Any:
            raise aiohttp.ConnectionTimeoutError

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _MixedSession:
        """First POST fails post-send, every later POST fails pre-connect."""

        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def post(self, *_args: object, **_kwargs: object) -> Any:
            self.calls.append({"args": _args, "kwargs": _kwargs})
            # Attempt 1 reaches the wire (read failure); the rest never connect.
            return _ReadFailingCtx() if len(self.calls) == 1 else _ConnFailingCtx()

    cache = _StubCache()
    session = _MixedSession()

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    with pytest.raises(NovaError) as exc_info:
        await _exercise()

    # The *final* attempt failed pre-connect, but an earlier attempt reached the
    # wire: dispatch must remain latched so the cancel key is preserved.
    assert exc_info.value.dispatched is True
    assert len(session.calls) > 1  # the mixed sequence actually retried


async def test_async_nova_request_latches_dispatch_across_http_status_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dispatch latches across a sequence that *ends* on an HTTP status.

    Regression for the Codex iter-8 finding: an early attempt reaches the wire
    and fails post-send (``ServerDisconnectedError`` during read), latching
    dispatch — a ring may already be active. If the retries are then exhausted
    by repeated HTTP 503s, the sequence raises ``NovaHTTPError``. That exit must
    still report ``dispatched is True`` so ``api.async_play_sound`` keeps the
    cancel key; otherwise a later Stop cannot target the ringing device. The
    retry-loop choke point stamps the latch onto *every* error leaving the loop,
    HTTP-status exits included — not only the wrapped network failure (which the
    sibling test above already covers).
    """

    class _ReadFailingCtx:
        """Post-send failure: context enters, body read raises."""

        def __init__(self) -> None:
            self.status = 200
            self.headers: dict[str, str] = {}

        async def read(self) -> bytes:
            raise aiohttp.ServerDisconnectedError("peer closed during read")

        async def __aenter__(self) -> _ReadFailingCtx:
            return self

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _WireThenHttpSession:
        """Attempt 1 fails post-send; every later attempt returns HTTP 503."""

        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def post(self, *_args: object, **_kwargs: object) -> Any:
            self.calls.append({"args": _args, "kwargs": _kwargs})
            if len(self.calls) == 1:
                return _ReadFailingCtx()
            return _DummyResponse(503, b"")

    cache = _StubCache()
    session = _WireThenHttpSession()

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    with pytest.raises(NovaHTTPError) as exc_info:
        await _exercise()

    # The sequence ended on an HTTP 503, but an earlier attempt reached the wire:
    # dispatch must remain latched so the cancel key is preserved.
    assert exc_info.value.dispatched is True
    assert len(session.calls) > 1  # the wire-reaching attempt actually retried


async def test_async_nova_request_latches_dispatch_on_pure_status_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pure server-status read (no read exception) latches dispatch.

    Regression for the Codex finding (AP8): a 5xx/429 status reaches the server
    even though the body read itself never fails, so the request may already
    have started a side effect (a device ring). If a *later* retry then fails
    pre-connect, the wrapped ``NovaError`` must still report ``dispatched is
    True`` so ``api.async_play_sound`` keeps the cancel key. Before the fix the
    latch flipped only inside the network-exception handler, so a pure status
    read left it ``False`` and the ring became unstoppable.
    """

    class _ConnFailingCtx:
        """Pre-connect failure: context raises on entry."""

        async def __aenter__(self) -> Any:
            raise aiohttp.ConnectionTimeoutError

        async def __aexit__(self, *_exc: object) -> None:
            return None

    class _StatusThenConnSession:
        """Attempt 1 reads HTTP 503 (no read error); later attempts never connect."""

        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def post(self, *_args: object, **_kwargs: object) -> Any:
            self.calls.append({"args": _args, "kwargs": _kwargs})
            if len(self.calls) == 1:
                return _DummyResponse(503, b"")
            return _ConnFailingCtx()

    cache = _StubCache()
    session = _StatusThenConnSession()

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    with pytest.raises(NovaError) as exc_info:
        await _exercise()

    # The 503 status read on attempt 1 latched dispatch; the final pre-connect
    # failure must not clear it.
    assert exc_info.value.dispatched is True
    assert len(session.calls) > 1  # the status read actually triggered a retry


async def test_async_nova_request_pure_4xx_does_not_latch_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A permanent 4xx rejection must NOT latch dispatch (S2 boundary).

    The server refuses a 403 *before* any side effect, so the cancel key must be
    dropped — otherwise it could overwrite the still-valid key of a parallel,
    possibly still-ringing earlier play on the same device. This pins the
    deliberate asymmetry of the AP8 fix: only transient 5xx latch; 4xx never do.
    """

    cache = _StubCache()
    session = _DummySession([_DummyResponse(403, b"forbidden")])

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    with pytest.raises(NovaAuthError) as exc_info:
        await _exercise()

    # A clean 4xx rejection never reached a side effect: the key must drop.
    assert exc_info.value.dispatched is False


@pytest.mark.parametrize("status", [501, 505, 508])
async def test_async_nova_request_non_retryable_5xx_does_not_latch_dispatch(
    monkeypatch: pytest.MonkeyPatch, status: int
) -> None:
    """A non-retryable 5xx (501/505/508) must NOT latch dispatch.

    Regression for the Codex follow-up on the AP8 latch: the first cut flipped
    the latch for *every* ``status >= 500``, but 501 Not Implemented, 505 HTTP
    Version Not Supported and 508 Loop Detected are documented as permanent
    config rejections — the server refuses them *before* executing the command,
    so no device ring can have started. Latching them would let an undispatched
    failure overwrite the still-valid cancel key of a parallel, possibly still
    ringing earlier play on the same device. The latch is now scoped to
    ``HTTP_DISPATCH_LATCH_ELIGIBLE`` (transient 5xx 500/502/503/504 only), so
    these raise ``NovaHTTPError`` with ``dispatched is False``.
    """

    cache = _StubCache()
    session = _DummySession([_DummyResponse(status, b"nope")])

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    with pytest.raises(NovaHTTPError) as exc_info:
        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    # Non-retryable 5xx is refused before any side effect: the key must drop.
    assert exc_info.value.dispatched is False
    assert len(session.calls) == 1  # single attempt, no retry


async def test_async_nova_request_pure_429_does_not_latch_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pure 429 rate-limit sequence must NOT latch dispatch.

    Regression for the Codex follow-up on the AP8 latch: 429 Too Many Requests
    is rejected at the gate and never processed, so a Play Sound request that
    only ever sees 429 cannot have started a device ring. Latching it would make
    ``api.async_play_sound`` keep a cancel UUID for a request the server refused;
    if the user presses Play while an older ring is still active, that bogus UUID
    overwrites the valid cancel key and the default Stop Sound targets the wrong
    request. 429 stays retry-eligible but is excluded from
    ``HTTP_DISPATCH_LATCH_ELIGIBLE``, so the wrapped error reports
    ``dispatched is False``.
    """

    cache = _StubCache()
    # NOVA_MAX_RETRIES == 6 -> 7 attempts all rate-limited.
    session = _DummySession([_DummyResponse(429, b"slow down") for _ in range(7)])

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    with pytest.raises(NovaRateLimitError) as exc_info:
        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    # Pure rate-limit: never processed, so the cancel key must drop.
    assert exc_info.value.dispatched is False
    assert len(session.calls) == 7  # exhausted the retry budget


async def test_async_nova_request_latch_survives_later_429(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dispatch-ambiguous 5xx keeps the latch even when later attempts see 429.

    The latch accumulates monotonically across the retry sequence: once an early
    503 makes the command dispatch-ambiguous (the backend may have begun a ring),
    a subsequent 429 must not clear that signal. This pins Codex's caveat that
    429 should be excluded from the latch *unless* an earlier attempt was already
    post-dispatch ambiguous.
    """

    cache = _StubCache()
    # Attempt 1 = 503 (latches), the rest = 429 (must not clear the latch).
    session = _DummySession(
        [_DummyResponse(503, b"")] + [_DummyResponse(429, b"slow") for _ in range(6)]
    )

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)

    with pytest.raises(NovaError) as exc_info:
        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
        )

    # The early 503 latched dispatch; the trailing 429s must not clear it.
    assert exc_info.value.dispatched is True
    assert len(session.calls) == 7


async def test_async_nova_request_uses_central_total_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SSOT regression: the HTTP request's ``total`` budget must come from the
    central ``NOVA_REQUEST_TOTAL_TIMEOUT_S`` constant, not a scattered literal.

    The outer poll guard (``POLL_DEVICE_OUTER_TIMEOUT_S``) is budgeted against
    this exact value, so the two must never drift apart (Codex finding: the outer
    guard has to cover the preceding HTTP phase). Pinning the wired-through total
    here keeps the coupling honest.
    """

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\x10\x20")])

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    await async_nova_request(
        "testScope",
        "00",
        username="user@example.com",
        cache=cache,
        session=session,
    )

    assert session.calls
    timeout = session.calls[0]["kwargs"].get("timeout")
    assert timeout is not None, "nova_request must pass an explicit ClientTimeout"
    assert timeout.total == NOVA_REQUEST_TOTAL_TIMEOUT_S


async def test_async_nova_request_uses_registered_cache_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Provider fallback supplies cache when async_nova_request cache arg is omitted."""

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\x01\x02")])

    calls: list[dict[str, Any]] = []

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        calls.append(
            {
                "username": username,
                "cache": cache,
                "retries": retries,
                "backoff": backoff,
            }
        )
        return "provider-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> str:
        await cache.set(username_string, "user@example.com")
        register_cache_provider(lambda: cache)
        try:
            return await async_nova_request(
                "testScope",
                "00",
                session=session,
            )
        finally:
            unregister_cache_provider()

    result = await _exercise()

    assert result == "0102"
    assert calls and calls[0]["cache"] is cache
    assert session.calls
    headers = session.calls[0]["kwargs"].get("headers", {})
    assert headers.get("Authorization") == "Bearer provider-token"


async def test_async_nova_request_requires_cache_when_provider_missing() -> None:
    """Missing cache and provider should raise before issuing the request."""

    async def _exercise() -> None:
        with pytest.raises(ValueError):
            await async_nova_request("testScope", "00", username="user@example.com")

    await _exercise()


async def test_async_nova_request_invokes_adm_exchange_even_with_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Providing a token kwarg must still route through async_get_adm_token_api."""

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\xaa\xbb")])

    calls: list[dict[str, Any]] = []

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        stored = await cache.get(DATA_AAS_TOKEN)
        calls.append(
            {
                "username": username,
                "cache": cache,
                "retries": retries,
                "backoff": backoff,
                "stored_aas": stored,
            }
        )
        return "adm-from-override"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "beef",
            username="User@Example.COM",
            token="aas_et/FLOW",
            cache=cache,
            session=session,
        )

    result = await _exercise()

    assert result == "aabb"
    assert calls and calls[0]["username"] == "user@example.com"
    assert calls[0]["stored_aas"] == "aas_et/FLOW"
    assert session.calls
    headers = session.calls[0]["kwargs"].get("headers", {})
    assert headers.get("Authorization") == "Bearer adm-from-override"


async def test_async_nova_request_skips_seeding_without_username(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not seed non-AAS override tokens when username kwarg is omitted."""

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\x01\x02")])

    calls: list[Any] = []
    final_state: dict[str, Any] = {}

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        calls.append(await cache.get(DATA_AAS_TOKEN))
        return "adm-fallback"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> str:
        await cache.set(username_string, "user@example.com")
        result = await async_nova_request(
            "testScope",
            "c0de",
            token="fcm-registration-token",
            cache=cache,
            session=session,
        )
        final_state["seeded"] = await cache.get(DATA_AAS_TOKEN)
        return result

    result = await _exercise()

    assert result == "0102"
    assert calls == [None]
    assert final_state["seeded"] is None
    assert session.calls
    headers = session.calls[0]["kwargs"].get("headers", {})
    assert headers.get("Authorization") == "Bearer adm-fallback"


async def test_async_nova_request_preserves_existing_aas_when_username_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-seeded AAS token must survive flow token usage without username kwarg."""

    cache = _StubCache()
    session = _DummySession([_DummyResponse(200, b"\xfa\xce")])

    calls: list[dict[str, Any]] = []

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        calls.append(
            {
                "username": username,
                "cache": cache,
                "retries": retries,
                "backoff": backoff,
            }
        )
        return "adm-preseed"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> tuple[str, Any]:
        await cache.set(username_string, "user@example.com")
        await cache.set(DATA_AAS_TOKEN, "cached-aas-token")
        result = await async_nova_request(
            "testScope",
            "face",
            token="fcm-registration-token",
            cache=cache,
            session=session,
        )
        final_aas = await cache.get(DATA_AAS_TOKEN)
        return result, final_aas

    result, final_aas = await _exercise()

    assert result == "face"
    assert calls and calls[0]["username"] == "user@example.com"
    assert calls[0]["cache"] is cache
    assert calls[0]["retries"] == EXPECTED_RETRY_COUNT
    assert calls[0]["backoff"] == 1.0
    assert final_aas == "cached-aas-token"
    assert session.calls
    headers = session.calls[0]["kwargs"].get("headers", {})
    assert headers.get("Authorization") == "Bearer adm-preseed"


async def test_async_nova_request_converts_flow_token_with_ephemeral_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Config-flow style caches must convert AAS tokens before Nova POST."""

    cache = _EphemeralCache(oauth_token=None, email="User@Example.COM")
    session = _DummySession([_DummyResponse(200, b"\x99\x33")])

    calls: list[str] = []

    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        calls.append(await cache.get(DATA_AAS_TOKEN))
        return "adm-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _exercise() -> str:
        return await async_nova_request(
            "testScope",
            "cafe",
            username="user@example.com",
            token="aas_et/CONFIG_FLOW",
            cache=cache,
            session=session,
        )

    result = await _exercise()

    assert result == "9933"
    assert calls == ["aas_et/CONFIG_FLOW"]
    assert session.calls
    headers = session.calls[0]["kwargs"].get("headers", {})
    assert headers.get("Authorization") == "Bearer adm-token"


async def test_async_ttl_policy_invalidate_aas_token_clears_both_bare_and_namespaced() -> (
    None
):
    """async_invalidate_aas_token must clear AAS token from both bare and namespaced keys.

    This ensures that when persistent 401 errors indicate a potentially invalid
    AAS token (even without explicit BadAuthentication), subsequent poll cycles
    can generate a fresh OAuth -> AAS -> ADM token chain.
    """

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-invalidate-aas")
        try:
            namespace = "entry-invalidate-aas"
            username = "user@example.com"

            # Seed both bare and namespaced AAS tokens
            await cache.set(DATA_AAS_TOKEN, "stale-bare-aas")
            await cache.set(f"{namespace}:{DATA_AAS_TOKEN}", "stale-ns-aas")

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger("test_invalidate_aas"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # Verify tokens exist before invalidation
            assert await cache.get(DATA_AAS_TOKEN) == "stale-bare-aas"
            assert await cache.get(f"{namespace}:{DATA_AAS_TOKEN}") == "stale-ns-aas"

            # Call the new invalidation method
            await policy.async_invalidate_aas_token()

            # Both keys must be cleared
            assert await cache.get(DATA_AAS_TOKEN) is None
            assert await cache.get(f"{namespace}:{DATA_AAS_TOKEN}") is None
        finally:
            await cache.close()

    await _run()


async def test_async_nova_request_invalidates_aas_token_after_exhausted_401_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persistent 401 errors after token refresh must raise a permanent error.

    With the current retry sequence:
    - Step 1: First 401 → refresh ADM → 6s → retry
    - Step 2: Second 401 → 61s → refresh ADM (AAS retained) → 6s → retry
    - Step 3: Third 401 → 501s → retry
    - Step 4: Fourth 401 → permanent error

    The AAS token is NOT invalidated during the retry sequence. If the AAS
    were truly rejected, Step 1 would have raised NovaAuthPermanentError via
    InvalidAasTokenError. Persistent 401s indicate propagation delay, not
    AAS invalidity.
    """

    cache = _StubCache()
    # Need 4 responses: 4 consecutive 401s to exhaust all retry steps
    session = _DummySession(
        [
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
        ]
    )

    aas_invalidation_calls: list[str] = []

    async def _exercise() -> None:
        # Seed the AAS token
        await cache.set(DATA_AAS_TOKEN, "stale-aas-token")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        async def _refresh() -> str:
            return "refreshed-adm"

        # Mock asyncio.sleep to skip delays
        async def _instant_sleep(_: float) -> None:
            pass

        # Spy on async_invalidate_aas_token to verify it's NOT called
        original_invalidate = AsyncTTLPolicy.async_invalidate_aas_token

        async def _spy_invalidate(self: AsyncTTLPolicy) -> None:
            aas_invalidation_calls.append(self.username)
            await original_invalidate(self)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _instant_sleep)
        monkeypatch.setattr(
            AsyncTTLPolicy,
            "async_invalidate_aas_token",
            _spy_invalidate,
        )

        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            token="initial-token",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

    with pytest.raises(NovaAuthError) as err:
        await _exercise()

    # After all retries exhausted, error is permanent (requires re-auth)
    assert err.value.status == 401
    assert err.value.is_permanent is True

    # AAS invalidation should NOT happen - persistent 401s are treated as
    # propagation delay, not AAS invalidity
    assert len(aas_invalidation_calls) == 0


async def test_async_nova_request_aas_token_cleared_enables_fresh_chain_on_next_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AAS token is retained across poll cycles since it is not invalidated on 401.

    The current retry sequence does NOT invalidate the AAS token during 401
    retries.  If the AAS were truly rejected, gpsoauth would raise
    InvalidAasTokenError during the ADM refresh step.  Persistent 401s are
    treated as propagation delay.

    This test verifies:
    1. First poll cycle: persistent 401s -> AAS token is preserved
    2. Second poll cycle: succeeds (simulating eventual propagation)
    """

    cache = _StubCache()
    poll_cycle = 0
    aas_states_before_request: list[tuple[int, str | None]] = []

    async def _exercise() -> tuple[str | None, str | None]:
        nonlocal poll_cycle

        # Seed initial AAS token
        await cache.set(DATA_AAS_TOKEN, "stale-aas")
        await cache.set(username_string, "user@example.com")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return f"adm-cycle-{poll_cycle}"

        async def _refresh() -> str:
            return f"refreshed-adm-cycle-{poll_cycle}"

        async def _instant_sleep(_: float) -> None:
            pass

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _instant_sleep)

        # First poll cycle: all 401s -> AAS token is NOT invalidated
        poll_cycle = 1
        aas_states_before_request.append((poll_cycle, await cache.get(DATA_AAS_TOKEN)))
        session1 = _DummySession(
            [
                _DummyResponse(401, b"Unauthorized"),
                _DummyResponse(401, b"Unauthorized"),
                _DummyResponse(401, b"Unauthorized"),
                _DummyResponse(401, b"Unauthorized"),
            ]
        )

        try:
            await async_nova_request(
                "testScope",
                "00",
                username="user@example.com",
                cache=cache,
                session=session1,
                refresh_override=_refresh,
            )
        except NovaAuthError:
            pass  # Expected

        # AAS token is preserved (not invalidated) after first cycle
        aas_after_first = await cache.get(DATA_AAS_TOKEN)

        # Second poll cycle: success
        poll_cycle = 2
        session2 = _DummySession([_DummyResponse(200, b"\xca\xfe")])

        second_result = await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session2,
            refresh_override=_refresh,
        )

        return aas_after_first, second_result

    aas_after_first, second_result = await _exercise()

    # AAS should be preserved after first failed cycle (not invalidated)
    assert aas_after_first == "stale-aas"

    # Second cycle should succeed
    assert second_result == "cafe"

    # Verify AAS state progression:
    # Cycle 1: AAS = "stale-aas" (before request, and preserved after)
    assert aas_states_before_request[0] == (1, "stale-aas")


async def test_async_nova_request_preserves_aas_token_on_successful_401_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AAS token must be preserved when 401 recovery succeeds within retry window.

    If the first 401 triggers a token refresh and the retry succeeds, the AAS
    token should NOT be invalidated. This prevents unnecessary token regeneration
    when the issue was transient (e.g., backend propagation delay).
    """

    cache = _StubCache()
    # First request: 401, second request after refresh: success
    session = _DummySession(
        [
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(200, b"\xbe\xef"),
        ]
    )

    aas_invalidation_calls: list[str] = []

    async def _exercise() -> tuple[str, str | None]:
        await cache.set(DATA_AAS_TOKEN, "original-aas")
        await cache.set(username_string, "user@example.com")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        async def _refresh() -> str:
            return "refreshed-adm"

        # Spy on async_invalidate_aas_token to verify it's NOT called
        original_invalidate = AsyncTTLPolicy.async_invalidate_aas_token

        async def _spy_invalidate(self: AsyncTTLPolicy) -> None:
            aas_invalidation_calls.append(self.username)
            await original_invalidate(self)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr(
            AsyncTTLPolicy,
            "async_invalidate_aas_token",
            _spy_invalidate,
        )

        # Don't pass token kwarg to avoid overwriting AAS in cache
        result = await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

        final_aas = await cache.get(DATA_AAS_TOKEN)
        return result, final_aas

    result, final_aas = await _exercise()

    # Request should succeed
    assert result == "beef"

    # AAS token should be preserved (not invalidated)
    assert final_aas == "original-aas"

    # async_invalidate_aas_token should NOT have been called
    assert len(aas_invalidation_calls) == 0


async def test_async_nova_request_no_double_aas_invalidation_on_repeated_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AAS invalidation must NOT happen during the 401 retry sequence.

    When 401s persist through all retries, async_invalidate_aas_token should
    not be called at all.  The production code treats persistent 401s as
    propagation delay rather than AAS invalidity.
    """

    cache = _StubCache()
    session = _DummySession(
        [
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
        ]
    )

    invalidation_call_count = 0

    async def _exercise() -> None:
        nonlocal invalidation_call_count

        await cache.set(DATA_AAS_TOKEN, "stale-aas")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        async def _refresh() -> str:
            return "refreshed-adm"

        async def _instant_sleep(_: float) -> None:
            pass

        original_invalidate = AsyncTTLPolicy.async_invalidate_aas_token

        async def _counting_invalidate(self: AsyncTTLPolicy) -> None:
            nonlocal invalidation_call_count
            invalidation_call_count += 1
            await original_invalidate(self)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _instant_sleep)
        monkeypatch.setattr(
            AsyncTTLPolicy,
            "async_invalidate_aas_token",
            _counting_invalidate,
        )

        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            token="initial-token",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

    with pytest.raises(NovaAuthError):
        await _exercise()

    # No AAS invalidation should occur during 401 retries
    assert invalidation_call_count == 0


async def test_async_nova_request_loop_prevention_across_multiple_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AAS token is preserved across multiple consecutive failed poll cycles.

    The production code no longer invalidates the AAS token during 401 retries.
    Persistent 401s are treated as propagation delay.  The AAS token remains
    intact so that subsequent cycles can retry with the same token chain.
    """

    cache = _StubCache()
    cycle_count = 0
    aas_invalidations: list[int] = []
    aas_states_at_cycle_start: list[tuple[int, str | None]] = []

    async def _exercise() -> str:
        nonlocal cycle_count

        await cache.set(DATA_AAS_TOKEN, "initial-stale-aas")
        await cache.set(username_string, "user@example.com")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return f"adm-cycle-{cycle_count}"

        async def _refresh() -> str:
            return f"refreshed-adm-cycle-{cycle_count}"

        async def _instant_sleep(_: float) -> None:
            pass

        original_invalidate = AsyncTTLPolicy.async_invalidate_aas_token

        async def _tracking_invalidate(self: AsyncTTLPolicy) -> None:
            aas_invalidations.append(cycle_count)
            await original_invalidate(self)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _instant_sleep)
        monkeypatch.setattr(
            AsyncTTLPolicy,
            "async_invalidate_aas_token",
            _tracking_invalidate,
        )

        # Simulate 3 failed cycles, then 1 successful cycle
        for i in range(1, 4):
            cycle_count = i
            # Record AAS state at the start of each cycle
            current_aas = await cache.get(DATA_AAS_TOKEN)
            aas_states_at_cycle_start.append((i, current_aas))

            session = _DummySession(
                [
                    _DummyResponse(401, b"Unauthorized"),
                    _DummyResponse(401, b"Unauthorized"),
                    _DummyResponse(401, b"Unauthorized"),
                    _DummyResponse(401, b"Unauthorized"),
                ]
            )
            try:
                await async_nova_request(
                    "testScope",
                    "00",
                    username="user@example.com",
                    cache=cache,
                    session=session,
                    refresh_override=_refresh,
                )
            except NovaAuthError:
                pass  # Expected for cycles 1-3

        # Record final AAS state before successful cycle
        cycle_count = 4
        final_aas_before_success = await cache.get(DATA_AAS_TOKEN)
        aas_states_at_cycle_start.append((4, final_aas_before_success))

        session_success = _DummySession([_DummyResponse(200, b"\xde\xad")])

        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session_success,
            refresh_override=_refresh,
        )

    result = await _exercise()

    # Final cycle should succeed
    assert result == "dead"

    # AAS should NOT be invalidated during 401 retries
    assert aas_invalidations == []

    # AAS token is preserved across all cycles (never invalidated)
    assert aas_states_at_cycle_start[0] == (1, "initial-stale-aas")
    assert aas_states_at_cycle_start[1][1] == "initial-stale-aas"
    assert aas_states_at_cycle_start[2][1] == "initial-stale-aas"
    assert aas_states_at_cycle_start[3][1] == "initial-stale-aas"


async def test_async_nova_request_adm_only_failure_recovers_on_second_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ADM refresh alone should fix most 401 errors without invalidating AAS.

    This test verifies the new retry sequence:
    - Step 1: First 401 → refresh ADM only → 6s propagation → retry
    - Success on second request proves ADM-only refresh was sufficient

    The AAS token should NOT be invalidated in this scenario.
    """

    cache = _StubCache()
    # First request: 401, second request after ADM refresh: success
    session = _DummySession(
        [
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(200, b"\xca\xfe\xba\xbe"),
        ]
    )

    aas_invalidation_calls: list[str] = []
    adm_refresh_calls: list[str] = []
    sleep_delays: list[float] = []

    async def _exercise() -> tuple[str, str | None]:
        await cache.set(DATA_AAS_TOKEN, "valid-aas")
        await cache.set(username_string, "user@example.com")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        async def _refresh() -> str:
            adm_refresh_calls.append("refresh")
            return "refreshed-adm"

        async def _tracking_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        original_invalidate = AsyncTTLPolicy.async_invalidate_aas_token

        async def _spy_invalidate(self: AsyncTTLPolicy) -> None:
            aas_invalidation_calls.append(self.username)
            await original_invalidate(self)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _tracking_sleep)
        monkeypatch.setattr(
            AsyncTTLPolicy,
            "async_invalidate_aas_token",
            _spy_invalidate,
        )

        result = await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

        final_aas = await cache.get(DATA_AAS_TOKEN)
        return result, final_aas

    result, final_aas = await _exercise()

    # Request should succeed
    assert result == "cafebabe"

    # ADM refresh should have been called once
    assert len(adm_refresh_calls) == 1

    # AAS token should NOT be invalidated (ADM refresh was sufficient)
    assert len(aas_invalidation_calls) == 0
    assert final_aas == "valid-aas"

    # Should have waited 6s for propagation delay
    assert 6.0 in sleep_delays


async def test_async_nova_request_aas_adm_failure_requires_full_chain_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ADM-only refresh fails, Step 2 retries ADM with AAS retained.

    This test verifies the current retry sequence:
    - Step 1: First 401 → refresh ADM only → 6s → retry
    - Step 2: Second 401 → 61s cooldown → refresh ADM (AAS retained) → 6s → retry
    - Success on third request proves the retry was sufficient

    The AAS token is NOT invalidated; persistent 401s are treated as
    propagation delay.
    """

    cache = _StubCache()
    # Two 401s (ADM-only fails), then success after second ADM refresh
    session = _DummySession(
        [
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(200, b"\xde\xad\xbe\xef"),
        ]
    )

    aas_invalidation_calls: list[str] = []
    adm_refresh_calls: list[str] = []
    sleep_delays: list[float] = []

    async def _exercise() -> tuple[str, str | None]:
        await cache.set(DATA_AAS_TOKEN, "stale-aas")
        await cache.set(username_string, "user@example.com")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        async def _refresh() -> str:
            adm_refresh_calls.append("refresh")
            return "refreshed-adm"

        async def _tracking_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        original_invalidate = AsyncTTLPolicy.async_invalidate_aas_token

        async def _spy_invalidate(self: AsyncTTLPolicy) -> None:
            aas_invalidation_calls.append(self.username)
            await original_invalidate(self)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _tracking_sleep)
        monkeypatch.setattr(
            AsyncTTLPolicy,
            "async_invalidate_aas_token",
            _spy_invalidate,
        )

        result = await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

        final_aas = await cache.get(DATA_AAS_TOKEN)
        return result, final_aas

    result, final_aas = await _exercise()

    # Request should succeed
    assert result == "deadbeef"

    # ADM refresh is called once in Step 1; in Step 2, async_on_401 sees the
    # recent refresh (stampede guard) and returns the cached token without
    # calling the refresh function again.
    assert len(adm_refresh_calls) == 1

    # AAS token should NOT be invalidated (AAS retained in step 2)
    assert len(aas_invalidation_calls) == 0

    # AAS should be preserved
    assert final_aas == "stale-aas"

    # Verify the delay sequence: 6s (step 1) + 61s (step 2 cooldown) + 6s (step 2 propagation)
    assert sleep_delays == [6.0, 61.0, 6.0]


async def test_async_nova_request_full_retry_sequence_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When all 4 retry steps fail, a permanent auth error must be raised.

    This test verifies the complete retry sequence:
    - Step 1: First 401 → refresh ADM → 6s → retry
    - Step 2: Second 401 → 61s → refresh ADM (AAS retained) → 6s → retry
    - Step 3: Third 401 → 501s long cooldown → retry
    - Step 4: Fourth 401 → permanent error (re-auth required)

    Total wait: 6s + 61s + 6s + 501s = 574s (~9.5 min)
    """

    cache = _StubCache()
    # Four consecutive 401s to exhaust all retries
    session = _DummySession(
        [
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
        ]
    )

    aas_invalidation_calls: list[str] = []
    adm_refresh_calls: list[str] = []
    sleep_delays: list[float] = []

    async def _exercise() -> None:
        await cache.set(DATA_AAS_TOKEN, "stale-aas")
        await cache.set(username_string, "user@example.com")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        async def _refresh() -> str:
            adm_refresh_calls.append("refresh")
            return "refreshed-adm"

        async def _tracking_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        original_invalidate = AsyncTTLPolicy.async_invalidate_aas_token

        async def _spy_invalidate(self: AsyncTTLPolicy) -> None:
            aas_invalidation_calls.append(self.username)
            await original_invalidate(self)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _tracking_sleep)
        monkeypatch.setattr(
            AsyncTTLPolicy,
            "async_invalidate_aas_token",
            _spy_invalidate,
        )

        await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

    with pytest.raises(NovaAuthError) as err:
        await _exercise()

    # Should be a permanent error requiring re-authentication
    assert err.value.status == 401
    assert err.value.is_permanent is True
    assert "re-authentication required" in str(err.value).lower()

    # ADM refresh is called once in Step 1; in Step 2, async_on_401 sees the
    # recent refresh (stampede guard) and returns the cached token without
    # calling the refresh function again.
    assert len(adm_refresh_calls) == 1

    # AAS invalidation should NOT have been called (AAS retained)
    assert len(aas_invalidation_calls) == 0

    # Verify the complete delay sequence: 6s + 61s + 6s + 501s
    assert sleep_delays == [6.0, 61.0, 6.0, 501.0]


async def test_async_nova_request_recovers_on_long_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery on step 3 (after 501s long cooldown) should succeed.

    This test verifies that even if ADM-only and AAS+ADM refresh both fail,
    success after the long cooldown period should work.

    Sequence: 401 → ADM → 401 → AAS+ADM → 401 → 501s → success
    """

    cache = _StubCache()
    # Three 401s, then success after long cooldown
    session = _DummySession(
        [
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(401, b"Unauthorized"),
            _DummyResponse(200, b"\xfe\xed\xfa\xce"),
        ]
    )

    sleep_delays: list[float] = []

    async def _exercise() -> str:
        await cache.set(DATA_AAS_TOKEN, "stale-aas")
        await cache.set(username_string, "user@example.com")

        async def _fake_get_adm_token(
            username: str | None = None,
            *,
            retries: int = 2,
            backoff: float = 1.0,
            cache: Any,
        ) -> str:
            return "initial-adm"

        async def _refresh() -> str:
            return "refreshed-adm"

        async def _tracking_sleep(delay: float) -> None:
            sleep_delays.append(delay)

        monkeypatch.setattr(
            "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
            _fake_get_adm_token,
        )
        monkeypatch.setattr("asyncio.sleep", _tracking_sleep)

        return await async_nova_request(
            "testScope",
            "00",
            username="user@example.com",
            cache=cache,
            session=session,
            refresh_override=_refresh,
        )

    result = await _exercise()

    # Should succeed after long cooldown
    assert result == "feedface"

    # Verify delays: 6s + 61s + 6s + 501s
    assert sleep_delays == [6.0, 61.0, 6.0, 501.0]


async def test_async_ttl_policy_aas_ttl_learning_records_observed_lifetime() -> None:
    """AAS TTL learning should record the observed token lifetime when invalidated.

    This test verifies that when an AAS token is invalidated:
    1. The observed lifetime is calculated from the issued timestamp
    2. The best TTL is updated with a 5% safety margin
    """

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-aas-ttl")
        try:
            namespace = "entry-aas-ttl"
            username = "user@example.com"

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger("test_aas_ttl_learning"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # Simulate AAS token issued 24 hours ago
            issued_time = time.time() - (24 * 3600)  # 24 hours ago
            await cache.set(policy.k_aas_issued, issued_time)
            await cache.set(DATA_AAS_TOKEN, "old-aas")

            # Invalidate the AAS token
            await policy.async_invalidate_aas_token()

            # Verify AAS token is cleared
            assert await cache.get(DATA_AAS_TOKEN) is None

            # Verify best TTL was learned (24 hours * 0.95 = ~82080 seconds)
            best_ttl = await cache.get(policy.k_aas_bestttl)
            assert best_ttl is not None
            expected_ttl = 24 * 3600 * 0.95
            assert abs(float(best_ttl) - expected_ttl) < 60  # Allow 1 min tolerance

        finally:
            await cache.close()

    await _run()


async def test_async_ttl_policy_aas_proactive_refresh_triggers_near_expiry() -> None:
    """AAS proactive refresh should NOT trigger even when approaching learned TTL.

    The production code no longer proactively invalidates the AAS token.
    In CLI mode the OAuth cookie is single-use and consumed, so destroying
    the AAS would be fatal.  Even in HA mode, premature invalidation is
    wasteful.  Instead, gpsoauth validates the AAS on the next ADM refresh.

    async_check_aas_proactive_refresh now always returns False and logs
    that validation will happen on the next ADM refresh.
    """

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-aas-proactive")
        try:
            namespace = "entry-aas-proactive"
            username = "user@example.com"

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger("test_aas_proactive"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # Set best TTL to 1 hour (3600 seconds)
            await cache.set(policy.k_aas_bestttl, 3600.0)

            # AAS token issued 57 minutes ago (past the threshold + max jitter)
            issued_time = time.time() - (57 * 60)  # 57 minutes ago
            await cache.set(policy.k_aas_issued, issued_time)
            await cache.set(DATA_AAS_TOKEN, "old-aas")

            # Check proactive refresh - should NOT trigger
            result = await policy.async_check_aas_proactive_refresh()

            # Should NOT have triggered proactive refresh
            assert result is False

            # AAS token should be preserved (not invalidated)
            assert await cache.get(DATA_AAS_TOKEN) == "old-aas"

        finally:
            await cache.close()

    await _run()


async def test_async_ttl_policy_aas_proactive_refresh_skips_when_fresh() -> None:
    """AAS proactive refresh should NOT trigger when token is still fresh.

    When the AAS token is well within its TTL, proactive refresh should skip.
    """

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-aas-fresh")
        try:
            namespace = "entry-aas-fresh"
            username = "user@example.com"

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger("test_aas_fresh"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # Set best TTL to 24 hours
            await cache.set(policy.k_aas_bestttl, 24 * 3600.0)

            # AAS token issued 1 hour ago (well within 24 hour TTL)
            issued_time = time.time() - 3600  # 1 hour ago
            await cache.set(policy.k_aas_issued, issued_time)
            await cache.set(DATA_AAS_TOKEN, "fresh-aas")

            # Check proactive refresh - should NOT trigger
            result = await policy.async_check_aas_proactive_refresh()

            # Should NOT have triggered proactive refresh
            assert result is False

            # AAS token should still exist
            assert await cache.get(DATA_AAS_TOKEN) == "fresh-aas"

        finally:
            await cache.close()

    await _run()


async def test_async_ttl_policy_ignores_very_short_ttl_in_recalibration() -> None:
    """Very short observed TTLs should be ignored to avoid race condition artifacts.

    When a token expires very quickly (< MIN_TTL_FOR_LEARNING_SEC), it typically
    indicates a transient server issue (rate-limit, propagation race) rather than
    the actual token lifetime. The TTL policy should ignore these observations
    to prevent permanent corruption of the learned TTL.
    """

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-short-ttl")
        try:
            namespace = "entry-short-ttl"
            username = "user@example.com"

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger("test_short_ttl"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # Set a known best TTL of 2 hours (7200 seconds)
            original_ttl = 7200.0
            await cache.set(policy.k_bestttl, original_ttl)

            # Simulate a token that was issued only 30 seconds ago (well below MIN_TTL)
            # MIN_TTL_FOR_LEARNING_SEC is 300 seconds (5 minutes)
            issued_time = time.time() - 30  # 30 seconds ago
            await cache.set(policy.k_issued, issued_time)

            # Trigger async_on_401 with adaptive downshift enabled
            # The observed TTL (30s) is far below MIN_TTL_FOR_LEARNING_SEC (300s)
            # so it should be ignored
            await policy.async_on_401(adaptive_downshift=True)

            # The best TTL should remain unchanged (not corrupted by the short TTL)
            best_ttl_after = await cache.get(policy.k_bestttl)
            assert best_ttl_after == original_ttl, (
                f"Expected TTL to remain {original_ttl}, but got {best_ttl_after}"
            )

        finally:
            await cache.close()

    await _run()


async def test_async_ttl_policy_accepts_legitimate_short_ttl_above_threshold() -> None:
    """TTLs above the minimum threshold should still trigger recalibration.

    When a token expires after MIN_TTL_FOR_LEARNING_SEC but significantly shorter
    than the current best TTL, recalibration should still occur.
    """

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-legit-short")
        try:
            namespace = "entry-legit-short"
            username = "user@example.com"

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger("test_legit_short"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # Set a known best TTL of 2 hours (7200 seconds)
            original_ttl = 7200.0
            await cache.set(policy.k_bestttl, original_ttl)

            # Simulate a token that was issued 10 minutes ago (600s > MIN_TTL of 300s)
            # This is above the threshold but significantly shorter than 2h
            # 600 + 120 (TTL_MARGIN_SEC) = 720 < 0.9 * 7200 = 6480, so recalibration triggers
            observed_ttl = 600.0
            issued_time = time.time() - observed_ttl
            await cache.set(policy.k_issued, issued_time)

            # Trigger async_on_401 with adaptive downshift enabled
            await policy.async_on_401(adaptive_downshift=True)

            # The best TTL should be updated to approximately observed * 0.95
            # Use approximate comparison due to time elapsed between setting issued_time and now
            expected_new_ttl = observed_ttl * 0.95  # ~570s
            best_ttl_after = await cache.get(policy.k_bestttl)
            assert best_ttl_after is not None
            assert abs(best_ttl_after - expected_new_ttl) < 1.0, (
                f"Expected TTL to be recalibrated to ~{expected_new_ttl}, "
                f"but got {best_ttl_after}"
            )

        finally:
            await cache.close()

    await _run()


async def test_async_ttl_policy_recalibration_logs_info_not_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The async recalibration path logs at INFO, not WARNING (severity downgrade).

    A downward TTL recalibration after an unplanned 401 is correct, self-healing
    behavior (a 6h probe re-explores the full lifetime upward), so the event is
    not user-actionable and must not raise a WARNING.

    Mutation sentinel: if the emitter is reverted to ``self.log.warning(...)``,
    the level assertion turns red.
    """

    logger_name = "test_async_recal_info"

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-async-recal-info")
        try:
            namespace = "entry-async-recal-info"
            username = "user@example.com"

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger(logger_name),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # best=7200s, observed=600s: above MIN_TTL (300s) yet markedly shorter
            # (600 + 120 margin < 0.9 * 7200), so the recalibration branch fires.
            original_ttl = 7200.0
            await cache.set(policy.k_bestttl, original_ttl)
            observed_ttl = 600.0
            await cache.set(policy.k_issued, time.time() - observed_ttl)

            with caplog.at_level(logging.INFO, logger=logger_name):
                await policy.async_on_401(adaptive_downshift=True)

            recalibrations = [
                r for r in caplog.records if "Unexpected short TTL" in r.getMessage()
            ]
            assert len(recalibrations) == 1
            assert recalibrations[0].levelno == logging.INFO
            assert not any(
                r.levelno == logging.WARNING
                and "Unexpected short TTL" in r.getMessage()
                for r in caplog.records
            )

            # Behavior preserved: best TTL is still recalibrated to ~observed * 0.95.
            best_ttl_after = await cache.get(policy.k_bestttl)
            assert best_ttl_after is not None
            assert abs(best_ttl_after - observed_ttl * 0.95) < 1.0

        finally:
            await cache.close()

    await _run()


def test_sync_ttl_policy_recalibration_logs_info_not_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The sync ``TTLPolicy.on_401`` recalibration path logs at INFO, not WARNING.

    Twin of the async coverage above. The downward recalibration is benign and
    self-healing, so it must not raise a WARNING.

    Mutation sentinel: reverting the emitter to ``self.log.warning(...)`` turns
    the level assertion red.
    """

    logger_name = "test_sync_recal_info"
    store: dict[str, Any] = {}

    def _get(key: str) -> Any:
        return store.get(key)

    def _set(key: str, value: Any) -> None:
        if value is None:
            store.pop(key, None)
        else:
            store[key] = value

    policy = TTLPolicy(
        username="user@example.com",
        logger=logging.getLogger(logger_name),
        get_value=_get,
        set_value=_set,
        refresh_fn=lambda: "fresh-token",
        set_auth_header_fn=lambda _: None,
        ns_prefix="entry-sync-recal-info",
    )

    # Mirror the async scenario: best=7200s, observed=600s triggers recalibration.
    original_ttl = 7200.0
    store[policy.k_bestttl] = original_ttl
    observed_ttl = 600.0
    store[policy.k_issued] = time.time() - observed_ttl

    with caplog.at_level(logging.INFO, logger=logger_name):
        policy.on_401(adaptive_downshift=True)

    recalibrations = [
        r for r in caplog.records if "Unexpected short TTL" in r.getMessage()
    ]
    assert len(recalibrations) == 1
    assert recalibrations[0].levelno == logging.INFO
    assert not any(
        r.levelno == logging.WARNING and "Unexpected short TTL" in r.getMessage()
        for r in caplog.records
    )

    # Behavior preserved: best TTL is still recalibrated to ~observed * 0.95.
    # (_do_refresh clears only the token/issued keys, never k_bestttl.)
    assert abs(float(store[policy.k_bestttl]) - observed_ttl * 0.95) < 1.0


async def test_both_adm_and_aas_tokens_have_min_ttl_protection() -> None:
    """CRITICAL: Both ADM and AAS tokens must have MIN_TTL protection.

    This test documents a past bug where only ADM tokens had MIN_TTL protection,
    leaving AAS tokens vulnerable to TTL corruption from transient server errors.

    The token chain is: OAuth -> AAS (days/weeks) -> ADM (hours)
    Both need protection against falsely learning very short TTLs.

    ADM: MIN_TTL_FOR_LEARNING_SEC = 300 (5 minutes)
    AAS: AAS_MIN_TTL_FOR_LEARNING_SEC = 3600 (1 hour)
    """

    async def _run() -> None:
        hass = _FakeHass()
        cache = await TokenCache.create(hass, "entry-both-tokens")
        try:
            namespace = "entry-both-tokens"
            username = "user@example.com"

            async def _cache_get(key: str) -> Any:
                return await cache.get(key)

            async def _cache_set(key: str, value: Any) -> None:
                await cache.set(key, value)

            async def _refresh() -> str:
                return "fresh-token"

            policy = AsyncTTLPolicy(
                username=username,
                logger=logging.getLogger("test_both_tokens"),
                get_value=_cache_get,
                set_value=_cache_set,
                refresh_fn=_refresh,
                set_auth_header_fn=lambda _: None,
                ns_prefix=namespace,
            )

            # --- Test 1: ADM token MIN_TTL protection ---
            # ADM tokens typically live 1-4 hours; MIN_TTL is 5 minutes
            adm_original_ttl = 7200.0  # 2 hours
            await cache.set(policy.k_bestttl, adm_original_ttl)

            # Simulate very short ADM lifetime (30 seconds - below MIN_TTL of 300s)
            adm_short_ttl = 30.0
            issued_time = time.time() - adm_short_ttl
            await cache.set(policy.k_issued, issued_time)

            await policy.async_on_401(adaptive_downshift=True)

            # ADM TTL must remain unchanged (protected by MIN_TTL_FOR_LEARNING_SEC)
            adm_ttl_after = await cache.get(policy.k_bestttl)
            assert adm_ttl_after == adm_original_ttl, (
                f"ADM TTL was corrupted! Expected {adm_original_ttl}, got {adm_ttl_after}. "
                "MIN_TTL_FOR_LEARNING_SEC protection is missing!"
            )

            # --- Test 2: AAS token MIN_TTL protection ---
            # AAS tokens typically live days/weeks; MIN_TTL is 1 hour
            aas_original_ttl = 7 * 24 * 3600.0  # 7 days
            await cache.set(policy.k_aas_bestttl, aas_original_ttl)

            # Simulate very short AAS lifetime (30 minutes - below MIN_TTL of 1 hour)
            aas_short_ttl = 30 * 60.0  # 30 minutes
            aas_issued_time = time.time() - aas_short_ttl
            await cache.set(policy.k_aas_issued, aas_issued_time)

            await policy.async_invalidate_aas_token()

            # AAS TTL must remain unchanged (protected by AAS_MIN_TTL_FOR_LEARNING_SEC)
            aas_ttl_after = await cache.get(policy.k_aas_bestttl)
            assert aas_ttl_after == aas_original_ttl, (
                f"AAS TTL was corrupted! Expected {aas_original_ttl}, got {aas_ttl_after}. "
                "AAS_MIN_TTL_FOR_LEARNING_SEC protection is missing!"
            )

            # --- Verify both constants exist and have sensible values ---
            assert hasattr(policy, "MIN_TTL_FOR_LEARNING_SEC"), (
                "ADM MIN_TTL constant missing from AsyncTTLPolicy!"
            )
            assert hasattr(policy, "AAS_MIN_TTL_FOR_LEARNING_SEC"), (
                "AAS MIN_TTL constant missing from AsyncTTLPolicy!"
            )
            assert policy.MIN_TTL_FOR_LEARNING_SEC == 300, (
                f"ADM MIN_TTL should be 300s (5min), got {policy.MIN_TTL_FOR_LEARNING_SEC}"
            )
            assert policy.AAS_MIN_TTL_FOR_LEARNING_SEC == 3600, (
                f"AAS MIN_TTL should be 3600s (1h), got {policy.AAS_MIN_TTL_FOR_LEARNING_SEC}"
            )
            # AAS threshold must be higher than ADM (AAS tokens live longer)
            assert (
                policy.AAS_MIN_TTL_FOR_LEARNING_SEC > policy.MIN_TTL_FOR_LEARNING_SEC
            ), "AAS MIN_TTL must be > ADM MIN_TTL since AAS tokens live longer!"

        finally:
            await cache.close()

    await _run()


class TestIsCredentialRejection:
    """The shared status criterion every NovaAuthError handler reads.

    The type covers every non-retryable 4xx, so only these rows may read as
    "your sign-in expired"; every other client rejection is the server
    refusing the REQUEST, not the credentials.
    """

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (401, True),
            (403, True),
            (400, False),
            (404, False),
            (405, False),
            (409, False),
            (422, False),
        ],
    )
    def test_the_status_alone_decides_for_a_plain_error(
        self, status: int, expected: bool
    ) -> None:
        assert is_credential_rejection(NovaAuthError(status, "detail")) is expected

    def test_permanence_outranks_a_non_credential_status(self) -> None:
        """is_permanent means "re-authentication is definitively required"."""
        assert is_credential_rejection(NovaAuthError(404, "perm", is_permanent=True))

    def test_the_permanent_subclass_stays_a_credential_rejection(self) -> None:
        assert is_credential_rejection(NovaAuthPermanentError(404, "aas rejected"))

    def test_an_unreadable_status_keeps_the_conservative_verdict(self) -> None:
        """A test double or a future subclass reads as auth, not as client error.

        delattr, not ``= None``: the annotation says int, and removing the
        instance attribute is exactly what makes getattr fall back.
        """
        err = NovaAuthError(404, "double without a status")
        delattr(err, "status")
        assert is_credential_rejection(err)


class TestTheDocumentedExtentStaysTrue:
    """The AGENTS.md paragraph counts call sites and handlers; nothing enforced it.

    `custom_components/googlefindmy/AGENTS.md` states the extent of the
    status-based classification in prose ("six call sites in four files",
    "ten try blocks catch NovaAuthError", "eight carry a broad except
    Exception"). That paragraph calls itself "the only thing standing in
    [the defect's] way", which makes an unenforced number the load-bearing
    part of the contract: a seventh handler, or a refactor that drops one,
    leaves the prose quietly wrong while every test stays green.

    `tests/AGENTS.md` already establishes the remedy for exactly this shape --
    a tuple that "must still equal the AST-derived set" rather than a
    hand-maintained copy. These rows apply it to the numbers above. They are
    deliberately AST-derived and not grep-derived: a comment mentioning the
    predicate must not count as a call site.

    The class has since grown past that one paragraph, because the same shape
    kept recurring: a load-bearing detail stated only in prose. It now also
    derives the guard count in `LocationRequestNotAcceptedError`'s own
    docstring (five broad handlers with a guard in front, a sixth deliberately
    without) and checks that every test name cited in the component's
    `AGENTS.md` files still resolves to a test. That last one overlaps with
    `TestTheDocumentedRejectionGuardStaysTrue` in
    `tests/test_coordinator_semantic_mappings.py`, which already guards one
    named list and, unlike this class, verifies its stated COUNT; the row here
    says what it adds instead of pretending the ground was bare. Numbers and
    names fail the same way -- silently, with a green suite -- and they are
    pinned here for the same reason.

    When the extent legitimately changes, update BOTH this test and the
    paragraph in the same commit. A failure here is not a bug in the code; it
    is the contract telling you it went stale.
    """

    _ROOT = Path(__file__).resolve().parents[1] / "custom_components" / "googlefindmy"

    @staticmethod
    def _handler_names(handler: ast.ExceptHandler) -> set[str]:
        node = handler.type
        if node is None:
            return {"Exception"}
        parts = node.elts if isinstance(node, ast.Tuple) else [node]
        out: set[str] = set()
        for part in parts:
            if isinstance(part, ast.Name):
                out.add(part.id)
            elif isinstance(part, ast.Attribute):
                out.add(part.attr)
        return out

    def _modules(self) -> list[tuple[Path, ast.Module]]:
        return [
            (path, ast.parse(path.read_text(encoding="utf-8")))
            for path in sorted(self._ROOT.rglob("*.py"))
        ]

    def test_the_predicate_has_the_documented_six_call_sites_in_four_files(
        self,
    ) -> None:
        per_file: dict[str, int] = {}
        for path, tree in self._modules():
            calls = [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "is_credential_rejection"
            ] + [
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "is_credential_rejection"
            ]
            if calls:
                per_file[path.relative_to(self._ROOT).as_posix()] = len(calls)

        assert sum(per_file.values()) == 6, per_file
        assert len(per_file) == 4, per_file
        assert per_file.get("api.py") == 3, per_file

    def test_ten_try_blocks_catch_the_auth_error_and_eight_have_a_broad_handler(
        self,
    ) -> None:
        """The count that gates the open follow-up (a dedicated client-error class).

        Splitting NovaAuthError is only safe once every catcher is known: the
        eight blocks with a broad `except Exception` would swallow a new class
        silently, the two sound handlers would let it escape. Both numbers are
        the reason that follow-up is not a one-liner, so both are pinned.
        """

        catching: list[str] = []
        with_broad: list[str] = []
        for path, tree in self._modules():
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Try, ast.TryStar)):
                    continue
                handlers = [self._handler_names(h) for h in node.handlers]
                if not any("NovaAuthError" in names for names in handlers):
                    continue
                where = f"{path.relative_to(self._ROOT).as_posix()}:{node.lineno}"
                catching.append(where)
                if any(
                    "Exception" in names or "BaseException" in names
                    for names in handlers
                ):
                    with_broad.append(where)

        assert len(catching) == 10, catching
        assert len(with_broad) == 8, with_broad
        assert len(catching) - len(with_broad) == 2, (catching, with_broad)

    # --------------------------------------------------------------- #
    # The second extent this file pins: the guards in front of the     #
    # broad handlers on the LocationRequestNotAcceptedError path.      #
    # --------------------------------------------------------------- #

    def _guarded_broad_blocks(self) -> list[tuple[str, bool, bool]]:
        """Every ``try`` that catches the signal AND has a broad handler.

        Returns ``(location, guard_comes_first, guard_is_a_bare_reraise)``.
        """

        found: list[tuple[str, bool, bool]] = []
        for path, tree in self._modules():
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Try, ast.TryStar)):
                    continue
                handlers = [self._handler_names(h) for h in node.handlers]
                broad = [
                    i
                    for i, names in enumerate(handlers)
                    if "Exception" in names or "BaseException" in names
                ]
                guard = [
                    i
                    for i, names in enumerate(handlers)
                    if "LocationRequestNotAcceptedError" in names
                ]
                if not broad or not guard:
                    continue
                body = node.handlers[guard[0]].body
                bare = (
                    len(body) == 1
                    and isinstance(body[0], ast.Raise)
                    and body[0].exc is None
                )
                where = f"{path.relative_to(self._ROOT).as_posix()}:{node.lineno}"
                found.append((where, guard[0] < broad[0], bare))
        return sorted(found)

    def test_every_broad_handler_that_guards_the_signal_puts_the_guard_first(
        self,
    ) -> None:
        """The class docstring counts five guards; nothing derived that five.

        `LocationRequestNotAcceptedError` is an `Exception`, so every broad
        `except Exception` on its path catches it as well and turns the raise
        back into the empty result the type was introduced to replace. Its
        docstring names five such blocks and asserts that all five carry a
        guard placed BEFORE the broad handler. That was a hand-counted number
        in prose, and the failure it guards against is the silent kind: moving
        a guard behind its broad neighbour leaves every existing test green,
        because the observable outcome is identical to the pre-change one.

        What is pinned is ORDER, MEMBERSHIP and SHAPE -- not the handler body
        beyond telling a bare re-raise from a dedicated branch. Three of the
        five re-raise bare; the two coordinator blocks handle the signal in a
        branch, which is the documented design, so requiring a bare `raise`
        everywhere would be wrong. That three is derived here rather than
        stated: an earlier revision of this docstring wrote "two", which is
        the failure mode this whole class exists to catch, committed inside
        the fix for it.

        "Bare" is measured as SHAPE, not meaning: one statement, a `raise` with
        no expression. Adding a log line ahead of an otherwise bare re-raise
        therefore turns this row red even though the handler still only
        re-raises. That direction is loud rather than silent, and the fix is
        one word in the sentence above, so it is left strict on purpose.

        One limit, stated so this is not read as more than it is: the row
        cannot see a NEW broad handler introduced on the path WITHOUT a guard
        -- such a block has no `except LocationRequestNotAcceptedError` and
        therefore never enters this set. The seam tests in
        `tests/test_location_request_not_accepted.py` cover that direction
        behaviourally; this row covers the direction they cannot, which is a
        guard that still exists but no longer runs first.
        """

        blocks = self._guarded_broad_blocks()
        assert len(blocks) == 5, blocks
        assert all(first for _, first, _ in blocks), blocks
        assert Counter(where.split(":")[0] for where, _, _ in blocks) == Counter(
            {
                "NovaApi/ExecuteAction/LocateTracker/location_request.py": 2,
                "api.py": 1,
                "coordinator/locate.py": 1,
                "coordinator/polling.py": 1,
            }
        ), blocks
        assert Counter(
            where.split(":")[0] for where, _, bare in blocks if bare
        ) == Counter(
            {
                "NovaApi/ExecuteAction/LocateTracker/location_request.py": 2,
                "api.py": 1,
            }
        ), blocks

    def test_the_sync_helper_is_the_broad_handler_deliberately_left_unguarded(
        self,
    ) -> None:
        """The sixth broad handler is a contract, not an oversight -- pin it.

        `api._run_sync_helper` flattens every exception to the caller's
        default, so `api.get_device_location` hands back `{}` for this signal
        too. The class docstring says so explicitly and calls it documented
        rather than missed. Adding a guard there would be a behaviour change at
        a public sync entry point AND would make that paragraph false, so this
        row fails first and sends whoever does it to the prose.
        """

        api = self._ROOT / "api.py"
        tree = ast.parse(api.read_text(encoding="utf-8"))
        helper = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef) and node.name == "_run_sync_helper"
        )
        broad = [
            handler
            for node in ast.walk(helper)
            if isinstance(node, (ast.Try, ast.TryStar))
            for handler in node.handlers
            if "Exception" in self._handler_names(handler)
        ]
        assert broad, "the sync helper no longer has a broad handler"
        guards = [
            handler
            for node in ast.walk(helper)
            if isinstance(node, (ast.Try, ast.TryStar))
            for handler in node.handlers
            if "LocationRequestNotAcceptedError" in self._handler_names(handler)
        ]
        assert guards == [], "the sync helper grew a guard; update the class docstring"

    def test_every_test_name_the_component_contracts_cite_exists(self) -> None:
        """Contract prose pins behaviour by NAME; most of those names were loose.

        `AGENTS.md` closes paragraphs with lists of test names. A rename or a
        deletion leaves such a list pointing at nothing, and the reader cannot
        tell a stale name from a test that merely lives elsewhere -- the prose
        reads the same either way.

        Part of this was already guarded and saying otherwise would repeat the
        defect: `TestTheDocumentedRejectionGuardStaysTrue` in
        `tests/test_coordinator_semantic_mappings.py` reads the sentence
        "Eight tests pin this:" out of the same file and checks BOTH its number
        against the list it prints AND that every listed name exists. That row
        can do something this one cannot -- verify a stated count -- so it
        stays; do not fold the two together. What was NOT covered, and is what
        this row adds: the second list ("Three tests pin the current state"),
        which that sentence pattern does not match; scattered one-off citations
        outside any list; class names, including the class you are reading, on
        which a whole paragraph rests; and the other AGENTS.md files under this
        component, which no row read at all.

        Module paths are excluded by CONTEXT, not by name: a citation preceded
        by `tests/` or followed by `.py` is a file reference. The obvious
        alternative -- drop any name that also exists as a file stem under
        `tests/` -- was tried first and is a loophole, because four names in
        this tree are BOTH a module stem and a test function. A stale citation
        of one of those would be skipped in silence, and adding a new module
        could retroactively blind an existing citation. The cost of the context
        rule is that a citation of a FILE goes unchecked; renaming a test module
        leaves such a reference dangling and no row here notices.

        Scope is every `AGENTS.md` under the component. `tests/AGENTS.md` is deliberately NOT included: it cites
        at least one name as a FORMER name on purpose, so folding it in needs a
        way to declare a citation historical, which is a change of its own.

        A name written into contract prose before the test exists fails this
        row. That is the intended direction: the contract may not promise
        coverage that is not there yet.
        """

        pattern = (
            r"(?<!tests/)\b(?:test_[A-Za-z0-9_]+|Test[A-Z][A-Za-z0-9_]*)\b(?!\.py)"
        )
        cited: dict[str, str] = {}
        for contract in sorted(self._ROOT.rglob("AGENTS.md")):
            where = contract.relative_to(self._ROOT).as_posix()
            for name in re.findall(pattern, contract.read_text(encoding="utf-8")):
                cited.setdefault(name, where)

        defined: set[str] = set()
        for path in Path(__file__).resolve().parent.rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(
                    node, (ast.FunctionDef, ast.AsyncFunctionDef)
                ) and node.name.startswith("test_"):
                    defined.add(node.name)
                elif isinstance(node, ast.ClassDef):
                    defined.add(node.name)

        assert cited, "the contracts cite no test names any more; check the pattern"
        assert [name for name in cited if name not in defined] == [], {
            name: where for name, where in sorted(cited.items()) if name not in defined
        }


# ---------------------------------------------------------------------------
# Dispatch observer: the point ``poll_age_s`` is stamped from
# (``coordinator/helpers/locate_clock.py``). It must fire immediately before
# each POST and never for a request that stops earlier.
# ---------------------------------------------------------------------------


def _seed_adm_token(
    monkeypatch: pytest.MonkeyPatch, error: BaseException | None = None
) -> None:
    async def _fake_get_adm_token(
        username: str | None = None,
        *,
        retries: int = 2,
        backoff: float = 1.0,
        cache: Any,
    ) -> str:
        if error is not None:
            raise error
        return "resolved-token"

    monkeypatch.setattr(
        "custom_components.googlefindmy.NovaApi.nova_request.async_get_adm_token_api",
        _fake_get_adm_token,
    )

    async def _instant_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _instant_sleep)


async def _nova(session: Any) -> str:
    return await async_nova_request(
        "testScope",
        "00",
        username="user@example.com",
        cache=_StubCache(),
        session=session,
    )


async def test_dispatch_observer_fires_once_per_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retried request fires the observer once per POST, each time before it.

    The observer records how many POSTs had gone out when it ran: ``[0, 1]``
    means it ran immediately before the first and before the second.
    """
    _seed_adm_token(monkeypatch)
    session = _DummySession([_DummyResponse(503, b""), _DummyResponse(200, b"\x01")])
    seen: list[int] = []
    with observe_request_dispatch(lambda: seen.append(len(session.calls))):
        assert await _nova(session) == "01"
    assert len(session.calls) == 2
    assert seen == [0, 1]


async def test_dispatch_observer_does_not_fire_without_a_post(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A token fetch that fails stops the request before any POST: no call."""
    _seed_adm_token(monkeypatch, error=RuntimeError("token fetch failed"))
    session = _DummySession([_DummyResponse(200, b"\x01")])
    seen: list[int] = []
    with (
        observe_request_dispatch(lambda: seen.append(1)),
        pytest.raises(Exception),
    ):
        await _nova(session)
    assert session.calls == []
    assert seen == []


async def test_a_failing_dispatch_observer_does_not_cost_the_request(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The observer only feeds a measurement; the request still succeeds.

    The DEBUG record names the error type but not its message.
    """
    _seed_adm_token(monkeypatch)
    session = _DummySession([_DummyResponse(200, b"\x01")])

    def _broken() -> None:
        raise ValueError("secret-detail")

    caplog.set_level(
        logging.DEBUG, logger="custom_components.googlefindmy.NovaApi.nova_request"
    )
    with observe_request_dispatch(_broken):
        assert await _nova(session) == "01"
    assert len(session.calls) == 1
    assert "Request dispatch observer failed: ValueError" in caplog.text
    assert "secret-detail" not in caplog.text


async def test_the_dispatch_observer_is_reset_after_its_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A request after the block must not reach the observer set for it."""
    _seed_adm_token(monkeypatch)
    session = _DummySession(
        [_DummyResponse(200, b"\x01"), _DummyResponse(200, b"\x02")]
    )
    seen: list[int] = []
    with observe_request_dispatch(lambda: seen.append(1)):
        await _nova(session)
    await _nova(session)
    assert len(session.calls) == 2
    assert seen == [1]
