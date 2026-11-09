# tests/test_token_retrieval.py
"""Regression tests for the AAS-token error classifier in token_retrieval.

These tests pin the FIX 5 matcher hardening: HTTP-generic tokens
("unauthorized"/"forbidden"/status codes) must no longer promote an arbitrary
transport/network exception string to :class:`InvalidAasTokenError`, while the
gpsoauth-specific ``Error`` vocabulary (``badauthentication``) keeps doing so.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy.Auth import token_retrieval
from custom_components.googlefindmy.Auth.token_retrieval import (
    InvalidAasTokenError,
    _coerce_android_id,
    _extract_android_id_from_credentials,
    _is_invalid_aas_error_text,
)
from custom_components.googlefindmy.exceptions import MissingTokenCacheError

# ---------------------------------------------------------------------------
# _is_invalid_aas_error_text: HTTP-generic gating
# ---------------------------------------------------------------------------


def test_http_generic_forbidden_gated_off_by_default() -> None:
    """A bare "403 forbidden" string is NOT an invalid-AAS signal by default."""
    assert (
        _is_invalid_aas_error_text(
            "server returned 403 forbidden", allow_http_generic=False
        )
        is False
    )


def test_http_generic_forbidden_honoured_when_opted_in() -> None:
    """The structured gpsoauth Error field opts in and keeps the old behavior."""
    assert (
        _is_invalid_aas_error_text(
            "server returned 403 forbidden", allow_http_generic=True
        )
        is True
    )


def test_http_generic_unauthorized_gated() -> None:
    """ "unauthorized" mirrors "forbidden": gated off by default, on when opted in."""
    assert (
        _is_invalid_aas_error_text("401 unauthorized", allow_http_generic=False)
        is False
    )
    assert (
        _is_invalid_aas_error_text("401 unauthorized", allow_http_generic=True) is True
    )


def test_gpsoauth_badauthentication_always_matches() -> None:
    """gpsoauth-specific tokens stay active regardless of the HTTP-generic gate."""
    assert _is_invalid_aas_error_text("BadAuthentication") is True
    assert (
        _is_invalid_aas_error_text("BadAuthentication", allow_http_generic=True) is True
    )


def test_gpsoauth_invalid_credential_vocabulary_always_matches() -> None:
    """ "invalid" + credential vocabulary is gpsoauth-specific, never gated."""
    assert _is_invalid_aas_error_text("invalid credential") is True
    assert _is_invalid_aas_error_text("needsbrowser") is True


# ---------------------------------------------------------------------------
# _perform_oauth_sync: the generic ``except Exception`` call site
# ---------------------------------------------------------------------------


def _install_fake_gpsoauth(monkeypatch: pytest.MonkeyPatch, perform_oauth: Any) -> None:
    """Patch the module-local gpsoauth accessor with a fake providing perform_oauth."""
    fake = SimpleNamespace(perform_oauth=perform_oauth)
    monkeypatch.setattr(token_retrieval, "_gpsoauth", lambda: fake)


def test_generic_network_exception_does_not_discard_aas_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generic transport failure whose text contains "403 Forbidden" must NOT
    be reclassified as InvalidAasTokenError (which would discard the AAS token);
    it stays a plain RuntimeError so the caller treats it as retryable."""

    def _boom(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise OSError("WAF blocked request: HTTP 403 Forbidden (getaddrinfo)")

    _install_fake_gpsoauth(monkeypatch, _boom)

    with pytest.raises(RuntimeError) as excinfo:
        token_retrieval._perform_oauth_sync(
            "user@example.com",
            "aas_et/fake",
            "android_device_manager",
            False,
            android_id=0x1234,
        )
    assert not isinstance(excinfo.value, InvalidAasTokenError)


def test_genuine_badauthentication_still_discards_aas_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A structured gpsoauth Error=BadAuthentication response must still raise
    InvalidAasTokenError so the stale AAS token is discarded (no regression)."""

    def _bad_auth(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"Error": "BadAuthentication"}

    _install_fake_gpsoauth(monkeypatch, _bad_auth)

    with pytest.raises(InvalidAasTokenError):
        token_retrieval._perform_oauth_sync(
            "user@example.com",
            "aas_et/fake",
            "android_device_manager",
            False,
            android_id=0x1234,
        )


def test_generic_exception_with_gpsoauth_vocabulary_still_discards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even via the generic except-Exception path, a gpsoauth-specific token in
    the exception text still promotes to InvalidAasTokenError."""

    def _boom(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("gpsoauth rejected: BadAuthentication")

    _install_fake_gpsoauth(monkeypatch, _boom)

    with pytest.raises(InvalidAasTokenError):
        token_retrieval._perform_oauth_sync(
            "user@example.com",
            "aas_et/fake",
            "android_device_manager",
            False,
            android_id=0x1234,
        )


# ---------------------------------------------------------------------------
# _perform_oauth_sync: typed gpsoauth.AuthError structural channel
# ---------------------------------------------------------------------------


class _MockAuthError(Exception):
    """Stand-in for ``gpsoauth.exceptions.AuthError`` (gpsoauth is absent in CI)."""


def test_typed_gpsoauth_autherror_http_only_discards_aas_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A typed gpsoauth AuthError with HTTP-generic-only text must discard the
    AAS token. gpsoauth may raise its typed AuthError instead of returning an
    Error dict; classifying it by type (mirroring the AAS exchange path) must
    promote it to InvalidAasTokenError even though the text matcher gates the
    HTTP-generic tokens off (``allow_http_generic=False``)."""

    monkeypatch.setattr(
        token_retrieval,
        "gpsoauth_exceptions",
        SimpleNamespace(AuthError=_MockAuthError),
    )

    def _typed_auth_error(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise _MockAuthError("HTTP 403 Forbidden")

    _install_fake_gpsoauth(monkeypatch, _typed_auth_error)

    with pytest.raises(InvalidAasTokenError):
        token_retrieval._perform_oauth_sync(
            "user@example.com",
            "aas_et/fake",
            "android_device_manager",
            False,
            android_id=0x1234,
        )


def test_typed_channel_absent_http_only_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the gpsoauth exceptions module is unavailable (dependency absent),
    the typed channel is skipped and an HTTP-generic-only exception text keeps
    the safe FIX-5 default: it stays a retryable RuntimeError and does not
    masquerade as an invalid AAS token."""

    monkeypatch.setattr(token_retrieval, "gpsoauth_exceptions", None)

    def _http_only(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("HTTP 403 Forbidden")

    _install_fake_gpsoauth(monkeypatch, _http_only)

    with pytest.raises(RuntimeError) as excinfo:
        token_retrieval._perform_oauth_sync(
            "user@example.com",
            "aas_et/fake",
            "android_device_manager",
            False,
            android_id=0x1234,
        )
    assert not isinstance(excinfo.value, InvalidAasTokenError)


# ---------------------------------------------------------------------------
# _is_invalid_aas_error_text: "invalid" without credential vocabulary
# ---------------------------------------------------------------------------


def test_invalid_without_credential_vocabulary_is_not_a_match() -> None:
    """ "invalid" alone (no token/auth/credential word) must NOT be treated as an
    invalid-AAS signal, even when the HTTP-generic gate is opened."""
    assert _is_invalid_aas_error_text("invalid request payload") is False
    assert (
        _is_invalid_aas_error_text("invalid request payload", allow_http_generic=True)
        is False
    )


# ---------------------------------------------------------------------------
# _extract_android_id_from_credentials
# ---------------------------------------------------------------------------


def test_extract_android_id_returns_int_from_gcm_block() -> None:
    """A numeric android_id in the FCM gcm block is returned unchanged."""
    assert (
        _extract_android_id_from_credentials({"gcm": {"android_id": 0xABCDEF}})
        == 0xABCDEF
    )


def test_extract_android_id_non_numeric_string_returns_none() -> None:
    """A non-numeric string android_id is rejected (returns None)."""
    assert (
        _extract_android_id_from_credentials({"gcm": {"android_id": "not-a-number"}})
        is None
    )


def test_extract_android_id_unsupported_type_returns_none() -> None:
    """An android_id of an unsupported type (e.g. float) is rejected."""
    assert _extract_android_id_from_credentials({"gcm": {"android_id": 3.14}}) is None


# ---------------------------------------------------------------------------
# _coerce_android_id
# ---------------------------------------------------------------------------


def test_coerce_android_id_parses_hex_string() -> None:
    """A "0x"-prefixed hex string cached value is coerced to its integer form."""
    assert _coerce_android_id("0x1000", "cache") == 0x1000


def test_coerce_android_id_non_numeric_string_returns_none() -> None:
    """A non-numeric cached string yields None instead of raising."""
    assert _coerce_android_id("bogus", "cache") is None


def test_coerce_android_id_unsupported_type_returns_none() -> None:
    """A cached value of an unsupported type (e.g. float) yields None."""
    assert _coerce_android_id(3.14, "cache") is None


# ---------------------------------------------------------------------------
# _resolve_android_id: defensive cache-failure branches
# ---------------------------------------------------------------------------


class _AllRaisingCache:
    """Async cache whose every get/set raises, to exercise defensive branches."""

    async def get(self, name: str) -> Any:
        raise RuntimeError(f"cache get failed: {name}")

    async def set(self, name: str, value: Any) -> None:
        raise RuntimeError(f"cache set failed: {name}")


class _FcmIntThenSetRaisesCache:
    """Returns a valid FCM android_id on read but fails every write."""

    def __init__(self, android_id: int) -> None:
        self._android_id = android_id

    async def get(self, name: str) -> Any:
        if name == "fcm_credentials":
            return {"gcm": {"android_id": self._android_id}}
        return None

    async def set(self, name: str, value: Any) -> None:
        raise RuntimeError(f"cache set failed: {name}")


@pytest.mark.asyncio
async def test_resolve_android_id_survives_all_cache_failures() -> None:
    """When every cache read/write fails, a fresh android_id is still generated
    and returned; the defensive branches must swallow the cache errors."""
    cache = _AllRaisingCache()
    result = await token_retrieval._resolve_android_id(
        cache=cache, username="user@example.com"
    )
    assert isinstance(result, int)
    # Generated fallback lies in the documented range (see _resolve_android_id).
    assert 0x1000000000000000 <= result < 0x1000000000000000 + 0xF000000000000000


@pytest.mark.asyncio
async def test_resolve_android_id_returns_fcm_id_even_if_persist_fails() -> None:
    """The FCM-derived android_id is returned even when persisting it raises."""
    cache = _FcmIntThenSetRaisesCache(0xABCDEF)
    result = await token_retrieval._resolve_android_id(
        cache=cache, username="user@example.com"
    )
    assert result == 0xABCDEF


# ---------------------------------------------------------------------------
# _perform_oauth_sync: empty response and legacy "Auth" fallback
# ---------------------------------------------------------------------------


def test_empty_gpsoauth_response_raises_runtime_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty gpsoauth response surfaces as a retryable RuntimeError (not an
    InvalidAasTokenError, so the AAS token is preserved)."""

    def _empty(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {}

    _install_fake_gpsoauth(monkeypatch, _empty)

    with pytest.raises(RuntimeError) as excinfo:
        token_retrieval._perform_oauth_sync(
            "user@example.com",
            "aas_et/fake",
            "android_device_manager",
            False,
            android_id=0x1234,
        )
    assert not isinstance(excinfo.value, InvalidAasTokenError)
    assert "No response" in str(excinfo.value)


def test_legacy_auth_field_used_when_token_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When gpsoauth omits "Token" but returns the legacy "Auth" field, the
    legacy value is used as the OAuth token."""

    def _legacy(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"Auth": "ya29.legacy-token"}

    _install_fake_gpsoauth(monkeypatch, _legacy)

    result = token_retrieval._perform_oauth_sync(
        "user@example.com",
        "aas_et/fake",
        "android_device_manager",
        False,
        android_id=0x1234,
    )
    assert result == "ya29.legacy-token"


# ---------------------------------------------------------------------------
# request_token (sync entry point): guards and happy path
# ---------------------------------------------------------------------------


class _SimpleAsyncCache:
    """Minimal async cache backed by a plain dict (reads/writes never raise)."""

    def __init__(self, initial: dict[str, Any] | None = None) -> None:
        self._data: dict[str, Any] = dict(initial or {})

    async def get(self, name: str) -> Any:
        return self._data.get(name)

    async def set(self, name: str, value: Any) -> None:
        self._data[name] = value


def test_request_token_without_cache_raises_missing_cache() -> None:
    """Called with no running loop and no cache, request_token must raise
    MissingTokenCacheError (the multi-account isolation guard)."""
    with pytest.raises(MissingTokenCacheError):
        token_retrieval.request_token("user@example.com", "android_device_manager")


@pytest.mark.asyncio
async def test_request_token_inside_event_loop_is_rejected() -> None:
    """request_token is blocking and must refuse to run inside a live event loop,
    directing callers to the async variant instead."""
    with pytest.raises(RuntimeError, match="event loop is running"):
        token_retrieval.request_token("user@example.com", "android_device_manager")


def test_request_token_happy_path_with_injected_aas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With an injected AAS token and a cached android_id, request_token resolves
    the OAuth token via the blocking gpsoauth exchange."""

    def _ok(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"Token": "ya29.sync-ok"}

    _install_fake_gpsoauth(monkeypatch, _ok)
    cache = _SimpleAsyncCache({"android_id_user@example.com": 0x99})

    result = token_retrieval.request_token(
        "user@example.com",
        "android_device_manager",
        aas_token="aas_et/fake",
        cache=cache,
    )
    assert result == "ya29.sync-ok"


# ---------------------------------------------------------------------------
# async_request_token: cache guard and default AAS provider fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_request_token_without_cache_raises() -> None:
    """async_request_token requires a cache for multi-account isolation."""
    with pytest.raises(MissingTokenCacheError):
        await token_retrieval.async_request_token(
            "user@example.com", "android_device_manager", cache=None
        )


@pytest.mark.asyncio
async def test_async_request_token_uses_default_aas_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With neither an injected token nor a provider, async_request_token falls
    back to the default async_get_aas_token provider."""

    async def _fake_get_aas(*, cache: Any) -> str:
        return "aas_et/default"

    monkeypatch.setattr(token_retrieval, "async_get_aas_token", _fake_get_aas)

    def _ok(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"Token": "ya29.async-default"}

    _install_fake_gpsoauth(monkeypatch, _ok)
    cache = _SimpleAsyncCache({"android_id_user@example.com": 0x5})

    result = await token_retrieval.async_request_token(
        "user@example.com",
        "android_device_manager",
        cache=cache,
    )
    assert result == "ya29.async-default"


def test_structured_error_text_is_classified_before_it_reaches_the_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gpsoauth `Error` field is server text. The exception message is
    logged one hop later (`adm_token_retrieval` logs it at ERROR), so it must
    carry the classified kind, never the field's wording (AGENTS.md R-1)."""
    leaked = "BadAuthentication for user@example.com token aas_et/SECRET"

    def _bad_auth(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {"Error": leaked}

    _install_fake_gpsoauth(monkeypatch, _bad_auth)

    with pytest.raises(InvalidAasTokenError) as excinfo:
        token_retrieval._perform_oauth_sync(
            "user@example.com",
            "aas_et/fake",
            "android_device_manager",
            False,
            android_id=0x1234,
        )
    message = str(excinfo.value)
    assert "SECRET" not in message
    assert "user@example.com" not in message
    assert f"UNRECOGNIZED ({len(leaked)} chars)" in message


_CALL_ARGS = ("user@example.com", "aas_et/fake", "android_device_manager", False)


@pytest.mark.parametrize(
    ("typed", "text"),
    [
        (True, "HTTP 403 Forbidden for user@example.com token aas_et/SECRET"),
        (False, "gpsoauth rejected: BadAuthentication aas_et/SECRET user@example.com"),
    ],
    ids=["typed-autherror", "vocabulary-in-generic-text"],
)
def test_producer_error_text_is_described_not_quoted(
    monkeypatch: pytest.MonkeyPatch, typed: bool, text: str
) -> None:
    """Both InvalidAasTokenError paths that start from an exception (typed
    AuthError, generic text with gpsoauth vocabulary) carry the markers and
    the length of the producer text, never the text (AGENTS.md R-1)."""
    if typed:
        monkeypatch.setattr(
            token_retrieval,
            "gpsoauth_exceptions",
            SimpleNamespace(AuthError=_MockAuthError),
        )

    def _boom(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise (_MockAuthError if typed else RuntimeError)(text)

    _install_fake_gpsoauth(monkeypatch, _boom)

    with pytest.raises(InvalidAasTokenError) as excinfo:
        token_retrieval._perform_oauth_sync(*_CALL_ARGS, android_id=0x1234)
    message = str(excinfo.value)
    assert "SECRET" not in message
    assert "user@example.com" not in message
    assert f"({len(text)} chars)" in message
    assert "markers=[" in message


def test_transport_error_text_is_typed_not_quoted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retryable fall-through names the producer exception type and the
    text length; the text itself (which may carry a token) stays out."""
    text = "connection reset by peer while sending aas_et/SECRET"

    def _boom(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise ConnectionError(text)

    _install_fake_gpsoauth(monkeypatch, _boom)

    with pytest.raises(RuntimeError) as excinfo:
        token_retrieval._perform_oauth_sync(*_CALL_ARGS, android_id=0x1234)
    assert not isinstance(excinfo.value, InvalidAasTokenError)
    message = str(excinfo.value)
    assert "SECRET" not in message
    assert f"ConnectionError ({len(text)} chars)" in message
