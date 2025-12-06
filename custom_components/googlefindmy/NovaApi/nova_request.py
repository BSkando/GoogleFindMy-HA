# custom_components/googlefindmy/NovaApi/nova_request.py
#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
"""
Nova API request helpers (async-first) for Google Find My Device.

This module exposes `async_nova_request(...)`, the non-blocking API consumed by
the Home Assistant integration.

Features
--------
- Optional reuse of the Home Assistant-managed aiohttp ClientSession to avoid
  per-call pools (`register_hass()` / `unregister_hass()`).
- Token TTL learning policy to proactively refresh ADM tokens while preventing
  thundering herds.
- Robust retry logic (401 -> refresh & retry; 5xx/429 -> exponential backoff).
- **Entry-scoped, multi-account ready**: when a `TokenCache` is supplied via
  `async_nova_request(..., cache=...)`, *all* username lookups, token retrieval
  (initial and refresh), and TTL metadata reads/writes are performed strictly
  against that cache. Optionally, a `namespace` (e.g., entry_id) can be used to
  prefix cache keys, preventing collisions when multiple entries share a cache.
"""

from __future__ import annotations

import asyncio
import binascii
import contextvars
import logging
import random
import re
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from importlib import import_module
from typing import TYPE_CHECKING, Any, cast

import aiohttp

from custom_components.googlefindmy.Auth.adm_token_retrieval import (
    async_get_adm_token as async_get_adm_token_api,
)
from custom_components.googlefindmy.Auth.token_cache import (
    TokenCache,  # NEW: for entry-scoped cache access in async path
)
from custom_components.googlefindmy.Auth.token_retrieval import InvalidAasTokenError
from custom_components.googlefindmy.Auth.username_provider import (
    async_get_username,
    username_string,
)

from ..const import DATA_AAS_TOKEN, NOVA_API_USER_AGENT

if TYPE_CHECKING:
    from bs4 import BeautifulSoup as _BeautifulSoupType
    from homeassistant.core import HomeAssistant
else:
    _BeautifulSoupType = Any
    HomeAssistant = Any

_beautiful_soup_factory: Callable[[str, str], _BeautifulSoupType] | None
try:
    from bs4 import BeautifulSoup as _bs_factory
except ImportError:  # pragma: no cover - optional dependency, covered via fallback branch
    _beautiful_soup_factory = None
    _BS4_AVAILABLE = False
else:
    _beautiful_soup_factory = cast(
        "Callable[[str, str], _BeautifulSoupType]", _bs_factory
    )
    _BS4_AVAILABLE = True

_LOGGER = logging.getLogger(__name__)

if not _BS4_AVAILABLE:
    _LOGGER.debug(
        "BeautifulSoup4 not installed, error response beautification disabled."
    )

# --- Retry constants ---
NOVA_MAX_RETRIES = 6
NOVA_INITIAL_BACKOFF_S = 4.0
NOVA_BACKOFF_FACTOR = 2.0
NOVA_MAX_RETRY_AFTER_S = 60.0

HTTP_OK = 200
HTTP_UNAUTHORIZED = 401
HTTP_TOO_MANY_REQUESTS = 429
HTTP_SERVER_ERROR_MIN = 500
HTTP_SERVER_ERROR_MAX = 600
RECENT_REFRESH_WINDOW_S = 2.0

MAX_PAYLOAD_BYTES = 512 * 1024  # 512 KiB

# --- Retry helpers ---


def _compute_delay(attempt: int, retry_after: str | None) -> float:
    """Return a retry delay honoring Retry-After headers with jittered exponential backoff fallback."""

    delay: float | None = None
    if retry_after:
        try:
            delay = float(retry_after)
        except (TypeError, ValueError):
            try:
                retry_dt = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                retry_dt = None
            if retry_dt is not None:
                if retry_dt.tzinfo is None:
                    retry_dt = retry_dt.replace(tzinfo=UTC)
                now = datetime.now(UTC)
                delay = max(0.0, (retry_dt - now).total_seconds())

    if delay is None:
        exponent = max(0, attempt - 1)
        backoff = (NOVA_BACKOFF_FACTOR**exponent) * NOVA_INITIAL_BACKOFF_S
        next_backoff = backoff * NOVA_BACKOFF_FACTOR
        delay = random.uniform(backoff, next_backoff)

    return min(delay, NOVA_MAX_RETRY_AFTER_S)


# --- PII Redaction ---

_RE_BEARER = re.compile(r"Bearer\s+[A-Za-z0-9\-\._~\+\/]+=*", re.I)
_RE_EMAIL = re.compile(r"([A-Za-z0-9._%+-])([A-Za-z0-9._%+-]*)(@[^,\s]+)")
_RE_HEX16 = re.compile(r"\b[0-9a-fA-F]{16,}\b")


def _redact(s: str) -> str:
    """Redact sensitive information from a string for safe logging."""
    s = _RE_BEARER.sub("Bearer <redacted>", s)
    s = _RE_EMAIL.sub(r"\1***\3", s)
    s = _RE_HEX16.sub("<hex-redacted>", s)
    return s


_ERROR_SNIPPET_MAX = 512


def _beautify_text(resp_text: str) -> str:
    """Return a human-readable snippet of a response body for logging purposes."""

    if not resp_text:
        return ""

    if _BS4_AVAILABLE and _beautiful_soup_factory is not None:
        try:
            text_raw = _beautiful_soup_factory(resp_text, "html.parser").get_text(
                separator=" ", strip=True
            )
            text = str(text_raw)
        except Exception as err:  # pragma: no cover - defensive logging path
            _LOGGER.debug(
                "Failed to parse error response body via BeautifulSoup: %s", err
            )
        else:
            if text:
                return text[:_ERROR_SNIPPET_MAX]

    return resp_text[:_ERROR_SNIPPET_MAX]


# --- Custom Exceptions ---
class NovaError(Exception):
    """Base exception for Nova API errors."""


class NovaAuthError(NovaError):
    """Raised on 4xx client errors after retries."""

    def __init__(self, status: int, detail: str | None = None):
        super().__init__(f"HTTP Client Error {status}: {detail or ''}".strip())
        self.status = status
        self.detail = detail


class NovaRateLimitError(NovaError):
    """Raised on 429 rate-limiting errors after retries."""

    def __init__(self, detail: str | None = None):
        super().__init__(f"Rate limited by upstream API: {detail or ''}".strip())
        self.detail = detail


class NovaHTTPError(NovaError):
    """Raised for 5xx server errors after retries."""

    def __init__(self, status: int, detail: str | None = None):
        super().__init__(f"HTTP Server Error {status}: {detail or ''}".strip())
        self.status = status
        self.detail = detail


# ------------------------ Optional Home Assistant hooks ------------------------
# These hooks allow the integration to supply a shared aiohttp ClientSession.
_STATE: dict[str, Any] = {
    "hass": None,
    "async_refresh_lock": None,
    "cache_provider": None,
}


def register_hass(hass: HomeAssistant) -> None:
    """Register a Home Assistant instance to provide a shared ClientSession."""

    _STATE["hass"] = hass


def unregister_hass() -> None:
    """Unregister the Home Assistant instance reference."""

    _STATE["hass"] = None


# Cache provider hook for multi-entry support using contextvars for async safety
_CACHE_PROVIDER: contextvars.ContextVar[Callable[[], Any] | None] = contextvars.ContextVar(
    "_cache_provider", default=None
)

def register_cache_provider(provider: Callable[[], Any]) -> None:
    """Register a callable that returns the entry-specific cache (context-local).

    Args:
        provider: Callable that returns a TokenCache instance.

    This allows multi-entry scenarios where each API instance uses its own cache
    instead of relying on the global cache facade. Uses contextvars to ensure
    concurrent async requests don't interfere with each other.
    """
    _STATE["cache_provider"] = provider
    _CACHE_PROVIDER.set(provider)

def unregister_cache_provider() -> None:
    """Unregister the cache provider for the current context."""
    _STATE["cache_provider"] = None
    _CACHE_PROVIDER.set(None)


def resolve_cache_from_provider() -> TokenCache | None:
    """Return the cache supplied by the registered provider, if any."""

    provider: Callable[[], TokenCache | None] | None = _CACHE_PROVIDER.get()
    if provider is None:
        provider = cast(Callable[[], TokenCache | None] | None, _STATE.get("cache_provider"))
    if provider is None:
        return None
    try:
        cache = provider()
        if cache is not None and getattr(cache, "_closed", False):
            return None
        return cache
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOGGER.warning("Cache provider callable failed: %s", exc)
        return None

def _get_cache_provider() -> TokenCache | None:
    """Get the registered cache provider for the current context or None.

    Returns None if no provider is registered or if the provider's cache is closed.
    """
    return resolve_cache_from_provider()

# --- Refresh Locks ---
_async_refresh_lock: asyncio.Lock | None = None


def _get_async_refresh_lock() -> asyncio.Lock:
    """Lazily initialize and return the async refresh lock."""

    if _STATE["async_refresh_lock"] is None:
        _STATE["async_refresh_lock"] = asyncio.Lock()
    return cast(asyncio.Lock, _STATE["async_refresh_lock"])


async def _get_initial_token_async(
    username: str,
    logger: logging.Logger,
    *,
    ns_prefix: str = "",
    cache: TokenCache | None,
) -> str:
    """Return an ADM token for *username* from the entry-scoped cache or API."""

    resolved_cache = cache or resolve_cache_from_provider()
    if resolved_cache is None:
        raise ValueError("TokenCache instance is required for Nova requests.")

    normalized_user = (username or "").strip().lower()
    if not normalized_user:
        raise ValueError("Username is empty/invalid; cannot retrieve ADM token.")

    prefix = (ns_prefix or "").strip()
    if prefix and not prefix.endswith(":"):
        prefix += ":"

    cache_key = f"{prefix}adm_token_{normalized_user}"

    cached = await resolved_cache.get(cache_key)
    if isinstance(cached, str) and cached:
        return cached

    if prefix:
        fallback_key = f"adm_token_{normalized_user}"
        fallback = await resolved_cache.get(fallback_key)
        if isinstance(fallback, str) and fallback:
            try:
                await resolved_cache.set(cache_key, fallback)
            except Exception as err:  # pragma: no cover - defensive logging
                logger.debug(
                    "Failed to mirror ADM token to namespaced key",
                    extra={"cache_key": cache_key},
                    exc_info=err,
                )
            return fallback

    token = await async_get_adm_token_api(normalized_user, cache=resolved_cache)

    if prefix:
        try:
            await resolved_cache.set(cache_key, token)
        except Exception as err:  # pragma: no cover - defensive logging
            logger.debug(
                "Failed to persist ADM token to namespaced key",
                extra={"cache_key": cache_key},
                exc_info=err,
            )

    return token


# ------------------------ TTL policy (shared core) ------------------------
class TTLPolicy:
    """Token TTL/probe policy (synchronous I/O).

    Encapsulates probe arming, jitter and proactive refresh. The policy records
    the best-known TTL and times token issuance, allowing pre-emptive refreshes.
    """

    TTL_MARGIN_SEC, JITTER_SEC = 120, 90
    PROBE_INTERVAL_SEC, PROBE_INTERVAL_JITTER_PCT = 6 * 3600, 0.1

    def __init__(  # noqa: PLR0913
        self,
        username: str,
        logger: logging.Logger,
        get_value: Callable[[str], float | int | str | None],
        set_value: Callable[[str, object | None], None],
        refresh_fn: Callable[[], str | None],
        set_auth_header_fn: Callable[[str], None],
        ns_prefix: str = "",
    ) -> None:  # noqa: PLR0913
        """
        Args:
            username: Account name this policy applies to.
            logger: Logger to use.
            get_value/set_value: Cache I/O functions (sync).
            refresh_fn: Returns a fresh token (string) or None.
            set_auth_header_fn: Receives the full 'Authorization' header value.
            ns_prefix: Optional key namespace (e.g., config entry_id) to avoid collisions.
        """
        self.username = username
        self.log = logger
        self._get, self._set, self._refresh, self._set_auth = (
            get_value,
            set_value,
            refresh_fn,
            set_auth_header_fn,
        )
        self._ns = (ns_prefix or "").strip()
        if self._ns and not self._ns.endswith(":"):
            self._ns += ":"

    # Cache keys for this username (optionally namespaced)
    @property
    def k_issued(self) -> str:
        return f"{self._ns}adm_token_issued_at_{self.username}"

    @property
    def k_bestttl(self) -> str:
        return f"{self._ns}adm_best_ttl_sec_{self.username}"

    @property
    def k_startleft(self) -> str:
        return f"{self._ns}adm_probe_startup_left_{self.username}"

    @property
    def k_probenext(self) -> str:
        return f"{self._ns}adm_probe_next_at_{self.username}"

    @property
    def k_armed(self) -> str:
        return f"{self._ns}adm_probe_armed_{self.username}"

    def _key_variants(self, base: str) -> set[str]:
        """Return the set of cache key variants for a given base name."""
        if self._ns:
            return {f"{self._ns}{base}", base}
        return {base}

    @staticmethod
    def _jitter_sec(base: float, jitter_abs: float) -> float:
        """Apply symmetric ±jitter_abs (seconds); never return negative."""
        try:
            return max(0.0, base + random.uniform(-jitter_abs, +jitter_abs))
        except Exception:
            return max(0.0, base)

    @staticmethod
    def _jitter_pct(base: float, pct: float) -> float:
        """Apply symmetric ±pct jitter; never return negative."""
        try:
            return max(0.0, base + random.uniform(-pct * base, +pct * base))
        except Exception:
            return max(0.0, base)

    @property
    def probe_interval_jitter_pct(self) -> float:
        """Expose jitter percentage for subclasses without duplication."""
        return self.PROBE_INTERVAL_JITTER_PCT

    def _arm_probe_if_due(self, now: float) -> bool:
        """Arm a probe if startup probes remain or the (jittered) schedule is due."""
        startup_left = self._get(self.k_startleft)
        probenext = self._get(self.k_probenext)

        do_arm = False
        if startup_left and int(startup_left) > 0:
            do_arm = True
        elif probenext is None:
            self._set(
                self.k_probenext,
                now
                + self._jitter_pct(
                    self.PROBE_INTERVAL_SEC, self.probe_interval_jitter_pct
                ),
            )
        elif now >= float(probenext):
            do_arm = True
            self._set(
                self.k_probenext,
                now
                + self._jitter_pct(
                    self.PROBE_INTERVAL_SEC, self.probe_interval_jitter_pct
                ),
            )

        if do_arm:
            self._set(self.k_armed, 1)
            return True
        return bool(self._get(self.k_armed))

    def _do_refresh(self, now: float) -> str | None:
        """Refresh token, update header and issuance timestamp, bootstrap startup probes if missing."""
        bases_to_clear = (
            f"adm_token_{self.username}",
            f"adm_token_issued_at_{self.username}",
        )
        for base in bases_to_clear:
            for key in self._key_variants(base):
                try:
                    self._set(key, None)
                except Exception:
                    pass
        tok = self._refresh()
        if tok:
            token_base = f"adm_token_{self.username}"
            for key in self._key_variants(token_base):
                try:
                    self._set(key, tok)
                except Exception:
                    pass

            self._set_auth("Bearer " + tok)
            issued_base = f"adm_token_issued_at_{self.username}"
            for key in self._key_variants(issued_base):
                try:
                    self._set(key, now)
                except Exception:
                    pass
            if self._get(self.k_startleft) is None:
                probe_base = f"adm_probe_startup_left_{self.username}"
                for key in self._key_variants(probe_base):
                    try:
                        if self._get(key) is None:
                            self._set(key, 3)
                    except Exception:
                        pass
        return tok

    def pre_request(self) -> None:
        """Arm probes and (if not armed) proactively refresh near the measured TTL."""
        now = time.time()
        issued_at = self._get(self.k_issued)
        best_ttl = self._get(self.k_bestttl)

        # Arm probe if needed; returns True if probe is armed.
        armed = self._arm_probe_if_due(now)

        # Proactive refresh only if NOT armed and we have a measured TTL
        if (not armed) and issued_at and best_ttl:
            try:
                age = now - float(issued_at)
                threshold = max(0.0, float(best_ttl) - self.TTL_MARGIN_SEC)
                threshold = self._jitter_sec(threshold, self.JITTER_SEC)
                if age >= threshold:
                    self.log.info(
                        "ADM token reached measured threshold – proactively refreshing..."
                    )
                    self._do_refresh(now)
            except (ValueError, TypeError) as e:
                self.log.debug("Threshold check failed: %s", e)

    async def async_pre_request(self) -> None:
        """Async wrapper for compatibility with AsyncTTLPolicy."""

        self.pre_request()

    def on_401(self, adaptive_downshift: bool = True) -> str | None:
        """Handle 401: measure TTL, adapt quickly on unexpected short TTLs, then refresh."""
        now = time.time()
        issued = self._get(self.k_issued)
        if issued is None:
            self.log.warning(
                "Got 401 – issued timestamp missing; attempting token refresh."
            )
            return self._do_refresh(now)

        age_sec = now - float(issued)
        age_min = age_sec / 60.0
        planned_probe = bool(self._get(self.k_armed))

        if planned_probe:
            self.log.info("Got 401 (forced probe) – measured TTL: %.1f min.", age_min)
            self._set(self.k_bestttl, age_sec)  # always accept probe (up or down)
            self._set(self.k_armed, 0)  # coalesce multiple 401 in same probe window
            left = self._get(self.k_startleft)
            if left and int(left) > 0:
                try:
                    self._set(self.k_startleft, int(left) - 1)
                except (ValueError, TypeError):
                    self._set(self.k_startleft, 0)
        else:
            self.log.warning(
                "Got 401 – token expired after %.1f min (unplanned).", age_min
            )
            if adaptive_downshift:
                best = self._get(self.k_bestttl)
                try:
                    # If clearly shorter than our current model (>10% shorter), recalibrate immediately.
                    if best and (age_sec + self.TTL_MARGIN_SEC) < 0.9 * float(best):
                        self.log.warning(
                            "Unexpected short TTL – recalibrating best known TTL with safety buffer."
                        )
                        safe_calibration = age_sec * 0.95
                        self._set(self.k_bestttl, safe_calibration)
                except (ValueError, TypeError) as e:
                    self.log.debug("Recalibration check failed: %s", e)

        # Always refresh after a 401 to resume normal operation.
        return self._do_refresh(now)

    async def async_on_401(
        self, adaptive_downshift: bool = True
    ) -> str | None:  # noqa: PLR0915
        """Async wrapper delegating to the synchronous policy."""

        return self.on_401(adaptive_downshift=adaptive_downshift)


class AsyncTTLPolicy(TTLPolicy):
    """Native async version of the TTL policy (no blocking calls)."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        username: str,
        logger: logging.Logger,
        get_value: Callable[[str], Awaitable[Any]],
        set_value: Callable[[str, Any], Awaitable[None]],
        refresh_fn: Callable[[], Awaitable[str | None]],
        set_auth_header_fn: Callable[[str], None],
        ns_prefix: str = "",
    ) -> None:  # noqa: PLR0913
        super().__init__(
            username=username,
            logger=logger,
            get_value=lambda _: None,
            set_value=lambda _key, _value: None,
            refresh_fn=lambda: None,
            set_auth_header_fn=set_auth_header_fn,
            ns_prefix=ns_prefix,
        )
        self._aget: Callable[[str], Awaitable[Any]] = get_value
        self._aset: Callable[[str, Any], Awaitable[None]] = set_value
        self._arefresh: Callable[[], Awaitable[str | None]] = refresh_fn

    async def _arm_probe_if_due_async(self, now: float) -> bool:
        startup_left = await self._aget(self.k_startleft)
        probenext = await self._aget(self.k_probenext)
        do_arm = bool(startup_left and int(startup_left) > 0)
        if not do_arm:
            if probenext is None:
                await self._aset(
                    self.k_probenext,
                    now
                    + self._jitter_pct(
                        self.PROBE_INTERVAL_SEC, self.PROBE_INTERVAL_JITTER_PCT
                    ),
                )
            elif now >= float(probenext):
                do_arm = True
                await self._aset(
                    self.k_probenext,
                    now
                    + self._jitter_pct(
                        self.PROBE_INTERVAL_SEC, self.PROBE_INTERVAL_JITTER_PCT
                    ),
                )
        if do_arm:
            await self._aset(self.k_armed, 1)
            return True
        return bool(await self._aget(self.k_armed))

    async def _do_refresh_async(self, now: float) -> str | None:  # noqa: PLR0912
        bases_to_clear = (
            f"adm_token_{self.username}",
            f"adm_token_issued_at_{self.username}",
        )
        for base in bases_to_clear:
            for key in self._key_variants(base):
                try:
                    await self._aset(key, None)
                except Exception:
                    pass
        try:
            tok = await self._arefresh()  # async callable
        except InvalidAasTokenError as err:
            self.log.error(
                "ADM token refresh failed because the cached AAS token was rejected; re-authentication is required."
            )
            for key in self._key_variants(DATA_AAS_TOKEN):
                try:
                    await self._aset(key, None)
                except Exception:
                    pass
            raise NovaAuthError(401, "AAS token invalid during ADM refresh") from err
        if tok:
            token_base = f"adm_token_{self.username}"
            for key in self._key_variants(token_base):
                try:
                    await self._aset(key, tok)
                except Exception:
                    pass

            self._set_auth("Bearer " + tok)
            issued_base = f"adm_token_issued_at_{self.username}"
            for key in self._key_variants(issued_base):
                try:
                    await self._aset(key, now)
                except Exception:
                    pass
            if await self._aget(self.k_startleft) is None:
                probe_base = f"adm_probe_startup_left_{self.username}"
                for key in self._key_variants(probe_base):
                    try:
                        if await self._aget(key) is None:
                            await self._aset(key, 3)
                    except Exception:
                        pass
        return tok

    async def async_pre_request(self) -> None:
        now = time.time()
        issued_at = await self._aget(self.k_issued)
        best_ttl = await self._aget(self.k_bestttl)
        armed = await self._arm_probe_if_due_async(now)
        if (not armed) and issued_at and best_ttl:
            try:
                age = now - float(issued_at)
                threshold = max(0.0, float(best_ttl) - self.TTL_MARGIN_SEC)
                threshold = self._jitter_sec(threshold, self.JITTER_SEC)
                if age >= threshold:
                    self.log.info(
                        "ADM token reached measured threshold – proactively refreshing (async)…"
                    )
                    await self._do_refresh_async(now)
            except (ValueError, TypeError) as e:
                self.log.debug("Threshold check failed (async): %s", e)

    async def async_on_401(self, adaptive_downshift: bool = True) -> str | None:  # noqa: PLR0912,PLR0915
        """Async 401 handling with stampede guard and async cache I/O."""
        lock = _get_async_refresh_lock()
        async with lock:
            now = time.time()

            # Skip duplicate refresh if someone just refreshed recently.
            issued_raw = await self._aget(self.k_issued)
            issued: float | None
            try:
                issued = float(issued_raw) if issued_raw is not None else None
            except (TypeError, ValueError):
                self.log.debug(
                    "Cached issued timestamp is invalid (value=%r); forcing refresh.",
                    issued_raw,
                )
                issued = None

            if issued is not None and (now - issued) < RECENT_REFRESH_WINDOW_S:
                self.log.debug(
                    "Another task already refreshed the token; skipping duplicate refresh."
                )
                token_value: str | None = None
                token_base = f"adm_token_{self.username}"
                for key in self._key_variants(token_base):
                    try:
                        candidate = await self._aget(key)
                    except Exception as err:  # noqa: BLE001 - defensive cache read
                        self.log.debug(
                            "Failed to read cached token from key '%s': %s", key, err
                        )
                        continue
                    if isinstance(candidate, bytes):
                        candidate = candidate.decode()
                    if isinstance(candidate, str) and candidate:
                        token_value = candidate
                        break

                if token_value is not None:
                    self._set_auth("Bearer " + token_value)
                    return token_value

                self.log.debug(
                    "Recent refresh detected but no cached ADM token available; forcing refresh."
                )
                issued = None  # Ensure we fall through to refresh logic below.

            if issued is None:
                self.log.info(
                    "Got 401 – issued timestamp missing; attempting token refresh (async)."
                )
                return await self._do_refresh_async(now)

            age_sec = now - float(issued)
            age_min = age_sec / 60.0
            planned_probe = bool(await self._aget(self.k_armed))

            if planned_probe:
                self.log.info(
                    "Got 401 (forced probe) – measured TTL: %.1f min.", age_min
                )
                await self._aset(self.k_bestttl, age_sec)
                await self._aset(self.k_armed, 0)
                left = await self._aget(self.k_startleft)
                if left and int(left) > 0:
                    try:
                        await self._aset(self.k_startleft, int(left) - 1)
                    except (ValueError, TypeError):
                        await self._aset(self.k_startleft, 0)
            else:
                self.log.info(
                    "Got 401 – token expired after %.1f min (unplanned).", age_min
                )
                if adaptive_downshift:
                    best = await self._aget(self.k_bestttl)
                    try:
                        if best and (age_sec + self.TTL_MARGIN_SEC) < 0.9 * float(best):
                            self.log.warning(
                                "Unexpected short TTL – recalibrating best known TTL with safety buffer (async)."
                            )
                            safe_calibration = age_sec * 0.95
                            await self._aset(self.k_bestttl, safe_calibration)
                    except (ValueError, TypeError) as e:
                        self.log.debug("Recalibration check failed (async): %s", e)
            return await self._do_refresh_async(now)


async def async_nova_request(  # noqa: PLR0913,PLR0912,PLR0915
    api_scope: str,
    hex_payload: str,
    *,
    username: str | None = None,
    session: aiohttp.ClientSession | None = None,
    # Optional: initial token for config-flow isolation (bypass cache lookups)
    token: str | None = None,
    # Optional overrides for flow-local validation (avoid global cache conflicts)
    cache_get: Callable[[str], Awaitable[Any]] | None = None,
    cache_set: Callable[[str, Any], Awaitable[None]] | None = None,
    refresh_override: Callable[[], Awaitable[str | None]] | None = None,
    # Optional: entry-specific namespace for cache keys (e.g., entry_id)
    namespace: str | None = None,
    # Entry-scoped TokenCache for strict multi-account separation
    cache: TokenCache | None = None,
) -> str:
    """
    Asynchronous Nova API request for Home Assistant (entry-scoped capable).

    This is the preferred method for all communication with the Nova API from
    within Home Assistant, as it is non-blocking and integrates with HA's
    shared aiohttp session.

    Entry-scope & cache behavior:
        - All username lookups, token retrieval, and TTL metadata reads/writes use
          the provided entry-scoped cache exclusively.
        - `namespace` (e.g., config entry_id) is appended as a prefix to cache keys
          to avoid collisions when multiple entries share a backing store.
        - If flow-local `cache_get`/`cache_set` are provided, they override the
          default helpers but are expected to operate on the same entry scope.

    Args:
        api_scope: Nova API scope suffix (appended to the base URL).
        hex_payload: Hex string body.
        username: Optional username. If omitted, resolved from the entry-scoped cache.
        session: Optional aiohttp session to reuse.
        token: Optional, direct ADM token to bypass cache lookups (for config flow).
        cache_get/cache_set: Optional async functions for TTL metadata (flow-local overrides).
        refresh_override: Optional async function returning a fresh token. Use
            this to perform a *real* AAS→ADM refresh isolated from globals
            (e.g. via `async_get_adm_token_isolated(...)`).
        namespace: Optional key namespace (e.g., config entry_id) to avoid cache collisions.
        cache: Optional entry-scoped TokenCache for strict multi-account separation.

    Returns:
        Hex-encoded response body.

    Raises:
        ValueError: if the hex_payload is invalid or username is unavailable.
        NovaAuthError: on 4xx client errors.
        NovaRateLimitError: on 429 errors after all retries.
        NovaHTTPError: on 5xx server errors after all retries.
        NovaError: on other unrecoverable errors like network issues after retries.
    """
    url = f"https://android.googleapis.com/nova/{api_scope}"

    ns_raw = (namespace or "").strip()
    ns_prefix = f"{ns_raw}:" if ns_raw and not ns_raw.endswith(":") else ns_raw

    resolved_cache = cache or resolve_cache_from_provider()
    if resolved_cache is None:
        raise ValueError("TokenCache instance is required for Nova requests.")

    async def _cache_get(key: str) -> Any:
        if cache_get is not None:
            return await cache_get(key)
        return await resolved_cache.get(key)

    async def _cache_set(key: str, value: Any) -> None:
        if cache_set is not None:
            await cache_set(key, value)
            return
        await resolved_cache.set(key, value)

    user: str | None
    if isinstance(username, str) and username.strip():
        user = username.strip()
    else:
        val = await resolved_cache.get(username_string)
        user = str(val).strip() if isinstance(val, str) and val else None
        if not user:
            fetched = await async_get_username(cache=resolved_cache)
            user = fetched.strip() if isinstance(fetched, str) and fetched else None
    if user is None:
        raise ValueError("Username is not available for async_nova_request.")

    if (
        isinstance(token, str)
        and token
        and isinstance(username, str)
        and username.strip()
    ):
        try:
            await resolved_cache.set(DATA_AAS_TOKEN, token)
            if ns_prefix:
                await resolved_cache.set(f"{ns_prefix}{DATA_AAS_TOKEN}", token)
        except Exception as err:  # noqa: BLE001 - defensive caching
            _LOGGER.debug(
                "Failed to seed provided flow token into cache", exc_info=err
            )

    initial_token = await _get_initial_token_async(
        user, _LOGGER, ns_prefix=(namespace or ""), cache=resolved_cache
    )

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Authorization": f"Bearer {initial_token}",
        "Accept-Language": "en-US",
        "User-Agent": NOVA_API_USER_AGENT,
    }
    try:
        payload = binascii.unhexlify(hex_payload)
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise ValueError(f"Nova payload too large: {len(payload)} bytes")
    except (binascii.Error, ValueError) as e:
        raise ValueError("Invalid hex payload for Nova request") from e

    # Select cache/refresh providers (entry-scoped TokenCache)
    if refresh_override is not None:
        rf_fn: Callable[[], Awaitable[str | None]] = refresh_override
    else:
        rf_fn = lambda: async_get_adm_token_api(user, cache=resolved_cache)  # noqa: E731

    policy = AsyncTTLPolicy(
        username=user,
        logger=_LOGGER,
        get_value=_cache_get,
        set_value=_cache_set,
        refresh_fn=rf_fn,
        set_auth_header_fn=lambda bearer: headers.update({"Authorization": bearer}),
        ns_prefix=(namespace or ""),
    )
    await policy.async_pre_request()

    ephemeral_session = False
    if session is None:
        hass_ref = _STATE.get("hass")
        if hass_ref:
            # Use the HA-managed shared session for best performance and resource management.
            async_get_clientsession = import_module(
                "homeassistant.helpers.aiohttp_client"
            ).async_get_clientsession

            session = async_get_clientsession(hass_ref)
        else:
            # Fallback for environments without a shared session (e.g., standalone scripts).
            session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=16, enable_cleanup_closed=True)
            )
            ephemeral_session = True

    try:
        refreshed_once = False
        retries_used = 0
        while True:
            attempt = retries_used + 1
            try:
                timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=30)
                async with session.post(
                    url,
                    headers=headers,
                    data=payload,
                    timeout=timeout,
                    allow_redirects=False,
                ) as response:
                    content = await response.read()
                    status = response.status
                    _LOGGER.debug(
                        "Nova API async request to %s: status=%d", api_scope, status
                    )

                    if status == HTTP_OK:
                        return content.hex()

                    text_snippet = _redact(
                        _beautify_text(content.decode(errors="ignore"))
                    )

                    if status == HTTP_UNAUTHORIZED:
                        lvl = logging.INFO if not refreshed_once else logging.WARNING
                        _LOGGER.log(
                            lvl,
                            "Nova API async request to %s: 401 Unauthorized. Refreshing token.",
                            api_scope,
                        )
                        await policy.async_on_401()
                        if not refreshed_once:
                            refreshed_once = True
                            # Allow time for the refreshed token to propagate across Google backends
                            # before retrying the request.
                            await asyncio.sleep(6.0)
                            continue  # Free retry

                        raise NovaAuthError(status, "Unauthorized after token refresh")

                    if status in (408, HTTP_TOO_MANY_REQUESTS) or (
                        HTTP_SERVER_ERROR_MIN <= status < HTTP_SERVER_ERROR_MAX
                    ):
                        if retries_used < NOVA_MAX_RETRIES:
                            delay = _compute_delay(
                                attempt, response.headers.get("Retry-After")
                            )
                            _LOGGER.info(
                                "Nova API async request to %s failed with status %d. Retrying in %.2f seconds (attempt %d/%d)...",
                                api_scope,
                                status,
                                delay,
                                retries_used + 1,
                                NOVA_MAX_RETRIES,
                            )
                            retries_used += 1
                            await asyncio.sleep(delay)
                            continue
                        else:
                            _LOGGER.error(
                                "Nova API async request to %s failed after %d attempts with status %d.",
                                api_scope,
                                retries_used + 1,
                                status,
                            )
                    if status == HTTP_TOO_MANY_REQUESTS:
                        raise NovaRateLimitError(
                            f"Nova API rate limited after {NOVA_MAX_RETRIES} attempts."
                        )
                    raise NovaHTTPError(
                        status,
                        f"Nova API failed after {NOVA_MAX_RETRIES} attempts.",
                    )

                raise NovaAuthError(status, text_snippet)

            except asyncio.CancelledError:
                raise
            except (TimeoutError, aiohttp.ClientError) as e:
                if retries_used < NOVA_MAX_RETRIES:
                    delay = _compute_delay(attempt, None)
                    _LOGGER.info(
                        "Nova API async request to %s failed with %s. Retrying in %.2f seconds (attempt %d/%d)...",
                        api_scope,
                        type(e).__name__,
                        delay,
                        retries_used + 1,
                        NOVA_MAX_RETRIES,
                    )
                    retries_used += 1
                    await asyncio.sleep(delay)
                    continue
                else:
                    _LOGGER.error(
                        "Nova API async request to %s failed after %d attempts with %s.",
                        api_scope,
                        retries_used + 1,
                        type(e).__name__,
                    )
                    raise NovaError(
                        f"Nova API request failed after retries: {e}"
                    ) from e

    finally:
        if ephemeral_session and session:
            await session.close()
