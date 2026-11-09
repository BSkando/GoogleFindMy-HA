# custom_components/googlefindmy/api.py
"""API wrapper for Google Find My Device (async-first, HA-friendly).

This module encapsulates all network interactions with Google's Find My Device
backend and exposes a small, HA-oriented API surface:

- Device enumeration (lightweight list w/ capability hints).
- Per-device location retrieval.
- Action endpoints (play/stop sound) using the shared FCM receiver.

Token/Auth handling (Step 5.1-D):
- **401/403 (auth failures)** raised by Nova helpers are mapped to
  `homeassistant.exceptions.ConfigEntryAuthFailed` so the *coordinator* can
  trigger HA’s re-auth UX and Repairs issue workflow. One documented
  asymmetry: the per-device location path converts only a PERMANENT auth
  failure (and an HTTP 401/403). A plain non-permanent 403 is re-raised as
  `NovaAuthError` on purpose, because the polling coordinator counts
  consecutive transient auth failures and escalates at its own threshold
  rather than on the first occurrence.
  Every handler branches on the STATUS, via
  `NovaApi.nova_request.is_credential_rejection`: permanent first, then 401/403,
  an unreadable status keeps the conservative verdict. That matters because the
  transport raises `NovaAuthError` for every non-retryable 4xx (400, 404, 405,
  409, 422 included), so a type check reads "device not found" as "your sign-in
  expired". A non-credential rejection therefore leaves the device list as
  `UpdateFailed`, and the location request passes the error on to its callers,
  whose own branches skip the device without touching the re-auth machinery. It
  is deliberately NOT collapsed to an empty result. That began as protection
  against a second defect, now fixed: both callers used to read a non-raising
  return as proof that the credentials work and cleared the auth state before
  they checked for emptiness, so a permanently rejected tracker masked a real
  401 on another tracker. Those resets sit behind the empty guard since
  (`PLAN_GFMY_AUTH_RESET_POSITIVE_PROOF`); passing the error on stays right
  because a rejection is a failure and an empty dict cannot say so. `_classify_nova_auth_error` is the sound-path adapter over the
  same predicate. Still open, stated so it is not mistaken for coverage: the
  transport keeps raising a type named "auth" for all of them, so a future
  handler that reads the type instead of the predicate repeats the defect.
- **gpsoauth/ADM failures** (e.g., "BadAuthentication", "Missing 'Token' in gpsoauth")
  are normalized to `ConfigEntryAuthFailed` as well, even if they bubble up as a
  `RuntimeError`/`ValueError` rather than a `NovaAuthError`.
- Other server/network problems are treated as *transient*:
  - For device list: re-raised as `UpdateFailed` to keep coordinator semantics.
  - For per-device location and actions: logged and return {} / False to keep the
    polling cycle resilient (do not abort the sequential loop on a single error).
"""

from __future__ import annotations

import asyncio
import logging
import time
import warnings
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, cast, runtime_checkable

from aiohttp import ClientError, ClientSession
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed

from ._reauth_reason import ReauthReasonCode
from .Auth.token_cache import TokenCache
from .Auth.username_provider import username_string
from .const import (
    CONF_OAUTH_TOKEN,  # used by the ephemeral flow cache
    CONTRIBUTOR_MODE_HIGH_TRAFFIC,
    CONTRIBUTOR_MODE_IN_ALL_AREAS,
    DEFAULT_CONTRIBUTOR_MODE,
    DOMAIN,
    PlaySoundResult,
    SoundDispatchOutcome,
)
from .NovaApi import nova_request
from .NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
    DecryptionError,
    OwnerKeyLookupTransientError,
    any_real_location_record,
)
from .NovaApi.ExecuteAction.LocateTracker.location_request import (
    LocationRequestNotAcceptedError,
    get_location_data_for_device,
)
from .NovaApi.ExecuteAction.PlaySound.start_sound_request import (
    async_submit_start_sound_request,
)
from .NovaApi.ExecuteAction.PlaySound.stop_sound_request import (
    async_submit_stop_sound_request,
)
from .NovaApi.ListDevices.nbe_list_devices import async_request_device_list
from .NovaApi.nova_request import (
    NovaAuthError,
    NovaAuthPermanentError,
    NovaError,
    NovaHTTPError,
    NovaLogicError,
    NovaProtobufDecodeError,
    NovaRateLimitError,
    is_credential_rejection,
)
from .NovaApi.util import generate_random_uuid
from .ProtoDecoders.decoder import (
    _select_best_location as _decoder_select_best_location,
)
from .ProtoDecoders.decoder import (
    get_canonic_ids as _decoder_get_canonic_ids,
)
from .ProtoDecoders.decoder import (
    get_devices_with_location,
    parse_device_list_protobuf,
)
from .SpotApi.spot_request import SpotAuthPermanentError

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Internal logging helpers / guards
# ---------------------------------------------------------------------

# We log the "multiple config entries active" guard at INFO only once to avoid spam.
_GUARD_LOGGED_ONCE = False

# Limit error messages to avoid leaking long payloads by accident (defensive).
_MAX_ERR_CHARS = 300


def _short_err(e: Exception | str) -> str:
    """Return a truncated error string suitable for logs (privacy-conscious)."""
    msg = str(e)
    if len(msg) > _MAX_ERR_CHARS:
        return msg[: _MAX_ERR_CHARS - 3] + "..."
    return msg


def _classify_nova_auth_error(err: NovaAuthError) -> SoundDispatchOutcome:
    """Name who refused when the transport raised ``NovaAuthError``.

    The criterion lives in ``nova_request.is_credential_rejection``; this is
    only its sound-path adapter. That predicate carries the full reasoning:
    the type is wider than its name, permanence outranks the status, and an
    unreadable status keeps the conservative verdict.

    ``REJECTED_AUTH`` is for a refusal "on credentials: HTTP 401 or 403", so a
    missing device or a malformed request belongs in ``REJECTED_SERVER``, which
    names the non-credential client rejections alongside the 5xx. Do NOT
    re-derive the status test here: a second copy is exactly how the two sound
    handlers and the four other handlers drifted apart in the first place.
    """
    if is_credential_rejection(err):
        return SoundDispatchOutcome.REJECTED_AUTH
    return SoundDispatchOutcome.REJECTED_SERVER


def _play_result_after_failure(
    err: NovaError, request_uuid: str, outcome: SoundDispatchOutcome
) -> PlaySoundResult:
    """Build the Play Sound result for a non-accepted command.

    Two independent facts are carried, and they must not be confused. ``outcome``
    names WHO refused: the server, the network, or this integration. ``cancel_key``
    answers only "may the device be ringing", which the transport latches onto the
    error (``NovaError.dispatched``) at its retry-loop choke point. Before this
    type existed, the second fact was the only one that survived the boundary, and
    the coordinator had to reconstruct the first from it, which is why a server
    rejection and a dead network both ended up as a push transport problem.

    A raised ``NovaError`` is never an acceptance (the submitter returns a tuple
    only on HTTP 200). The cancel key is preserved ONLY when the failure latched
    dispatch (``err.dispatched``): some attempt in the retry sequence reached the
    wire, so the device may already be ringing and a later Stop needs the key.
    Pure rejections (401/403/5xx/429
    with no wire-reaching attempt) and provable pre-dispatch failures inherit
    ``dispatched is False`` and drop the key, so they can never overwrite a
    previous, possibly still-ringing play's valid cancel key. Centralizing the
    decision here keeps every non-acceptance exit on one rule instead of
    re-deriving it per ``except`` handler. See ``NovaError.dispatched``,
    IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY and IRR-CA-SOUND-FAILURE-CLASS.
    """
    return PlaySoundResult(outcome, cancel_key=request_uuid if err.dispatched else None)


# Backward-compatible export for tests and legacy call sites.
get_canonic_ids = _decoder_get_canonic_ids


def _is_multi_entry_guard_message(msg: str) -> bool:
    """Detect the 'multiple entries' guard by message content (signature-free)."""
    m = msg or ""
    return ("Multiple config entries active" in m) or ("entry.runtime_data" in m)


def _maybe_log_guard_once(
    context: str, *, email: str | None = None, entry_id: str | None = None
) -> None:
    """Log the multi-entry guard once at INFO; subsequent occurrences at DEBUG."""
    global _GUARD_LOGGED_ONCE
    extra = []
    if email:
        extra.append(f"email={email}")
    if entry_id:
        extra.append(f"entry_id={entry_id}")
    suffix = f" ({', '.join(extra)})" if extra else ""

    if not _GUARD_LOGGED_ONCE:
        _LOGGER.info(
            "Auth guard: multiple config entries detected; deferring validation to setup%s",
            suffix,
        )
        _GUARD_LOGGED_ONCE = True
    else:
        _LOGGER.debug("Auth guard (suppressed duplicate): %s%s", context, suffix)


# ----------------------------- Minimal protocols -----------------------------
@runtime_checkable
class FcmReceiverProtocol(Protocol):
    """Minimal protocol for the shared FCM receiver used by this module.

    Implementations must provide a `get_fcm_token()` method that returns a string
    token or None when not yet initialized.
    """

    def get_fcm_token(self, entry_id: str | None = None) -> str | None: ...


@runtime_checkable
class CacheProtocol(Protocol):
    """Entry-scoped cache protocol (TokenCache instance).

    The API expects a minimal async get/set key-value store used for:
      - username lookup,
      - token TTL metadata and ephemeral flags during flows,
      - optional stats persistence hooks (coordinator handles most stats).
    """

    async def async_get_cached_value(self, key: str) -> Any: ...
    async def async_set_cached_value(self, key: str, value: Any) -> None: ...


@runtime_checkable
class GoogleFindMyAPIProtocol(Protocol):  # pylint: disable=unnecessary-ellipsis
    """Protocol defining the public interface for Google Find My Device API.

    This abstraction layer enables:
    - Consistent API contract for the coordinator and other consumers
    - Easy mocking in tests without depending on concrete implementation
    - Future extensibility for alternative backend implementations

    All methods that interact with Google's servers are available in both
    sync and async variants where applicable.
    """

    async def close(self) -> None:
        """Release resources held by the API instance."""
        ...

    def set_contributor_mode(
        self, mode: str | None, *, switch_epoch: int | None = None
    ) -> None:
        """Update the contributor mode used for Nova requests."""
        ...

    async def async_get_basic_device_list(
        self,
        *,
        contributor_mode: str | None = None,
        switch_epoch: int | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch the device list from Google Find My Device (async).

        Returns a list of device dictionaries with basic info and capabilities.
        """
        ...

    def get_basic_device_list(self) -> list[dict[str, Any]]:
        """Fetch the device list from Google Find My Device (sync wrapper)."""
        ...

    def get_devices(self) -> list[dict[str, Any]]:
        """Legacy alias for get_basic_device_list()."""
        ...

    async def async_get_device_location(
        self,
        device_id: str,
        device_name: str,
        *,
        contributor_mode: str | None = None,
        switch_epoch: int | None = None,
    ) -> dict[str, Any]:
        """Request location for a specific device (async).

        Returns location data on success and an empty dict on a transient
        failure. `ConfigEntryAuthFailed` is raised only where re-auth must
        start at once: an HTTP 401/403 (`NovaHTTPError`), or a PERMANENT Nova
        auth failure. Every other `NovaAuthError` is re-raised unchanged --
        including a plain, non-permanent credential rejection such as a 403,
        which the polling coordinator counts against its own threshold instead
        of prompting on the first occurrence. Callers must therefore classify
        a `NovaAuthError` with `nova_request.is_credential_rejection` rather
        than assume the type already means "credentials rejected".
        """
        ...

    def get_device_location(self, device_id: str, device_name: str) -> dict[str, Any]:
        """Request location for a specific device (sync wrapper)."""
        ...

    def locate_device(self, device_id: str) -> dict[str, Any]:
        """Legacy alias for get_device_location()."""
        ...

    def is_push_ready(self) -> bool:
        """Check if push infrastructure is available for actions."""
        ...

    def push_ready(self) -> bool:
        """Alias for is_push_ready()."""
        ...

    def can_play_sound(self, device_id: str) -> bool | None:
        """Check if a device supports the play sound action."""
        ...

    def play_sound(self, device_id: str) -> bool:
        """Play a sound on the device (sync wrapper)."""
        ...

    def stop_sound(self, device_id: str, request_uuid: str | None = None) -> bool:
        """Stop playing sound on the device (sync wrapper)."""
        ...

    async def async_play_sound(self, device_id: str) -> PlaySoundResult:
        """Play a sound on the device (async).

        Returns the classification and the cancel key; see PlaySoundResult.
        """
        ...

    async def async_stop_sound(
        self, device_id: str, request_uuid: str | None = None
    ) -> SoundDispatchOutcome:
        """Stop playing sound on the device (async).

        Returns the classification; see SoundDispatchOutcome.
        """
        ...


# Module-local FCM provider getter; installed by the integration at setup time.
_FCM_ReceiverGetter: (
    Callable[[str | None], FcmReceiverProtocol]
    | Callable[[], FcmReceiverProtocol]
    | None
) = None


def register_fcm_receiver_provider(
    getter: Callable[[str | None], FcmReceiverProtocol]
    | Callable[[], FcmReceiverProtocol],
) -> None:
    """Register a getter that returns the shared FCM receiver (HA-managed).

    The provider accepts an optional entry ID and returns the current receiver
    instance for that scope. We keep this indirection to avoid importing heavy modules
    at import time and to stay resilient to reloads (the callable resolves the live
    object on access).
    We keep this indirection to avoid importing heavy modules at import time and
    to stay resilient to reloads (the callable resolves the live object on access).

    Args:
        getter: A callable that returns the singleton FcmReceiverProtocol instance.
    """
    global _FCM_ReceiverGetter
    _FCM_ReceiverGetter = getter


def unregister_fcm_receiver_provider() -> None:
    """Unregister the FCM receiver provider (called on unload/reload)."""
    global _FCM_ReceiverGetter
    _FCM_ReceiverGetter = None


# ----------------------------- Small helpers --------------------------------
def _infer_can_ring_slot(device: dict[str, Any]) -> bool | None:
    """Normalize a 'can ring' capability from various shapes; return None if unknown.

    We try multiple layouts because upstream protobuf decoders may evolve:
    - device["can_ring"] -> bool
    - device["canRing"] -> bool
    - device["capabilities"] -> list[str] / dict[str,bool]
    Returns:
        True/False when we could infer a verdict, otherwise None.
    """
    try:
        if "can_ring" in device:
            return bool(device.get("can_ring"))
        if "canRing" in device:
            return bool(device.get("canRing"))

        caps = device.get("capabilities")
        if isinstance(caps, (list, set, tuple)):
            lowered = {str(x).lower() for x in caps}
            return ("ring" in lowered) or ("play_sound" in lowered)
        if isinstance(caps, dict):
            lowered_map = {str(k).lower(): v for k, v in caps.items()}
            return bool(lowered_map.get("ring")) or bool(lowered_map.get("play_sound"))
    except Exception:
        return None
    return None


def _build_can_ring_index(
    parsed_device_list: Any,
    *,
    cache: TokenCache | None = None,
) -> dict[str, bool]:
    """Build a mapping canonical_id -> can_ring (where determinable).

    Args:
        parsed_device_list: The parsed protobuf message from the device list response.
        cache: Optional entry-scoped TokenCache for decrypting location payloads.

    Returns:
        A dictionary mapping canonical device IDs to a boolean indicating if they can ring.
    """
    index: dict[str, bool] = {}
    try:
        # Capability probe only: stay diagnostically silent so this pass does not consume
        # the canonicless visibility transition or pop the count guard (CQS). The main poll
        # below emits the diagnostics exactly once per poll.
        devices = get_devices_with_location(
            parsed_device_list, cache=cache, emit_canonicless_diagnostics=False
        )
    except Exception:
        devices = []

    for d in devices or []:
        cid = d.get("canonicalId") or d.get("id") or d.get("device_id")
        if not cid:
            continue
        verdict = _infer_can_ring_slot(d)
        if isinstance(verdict, bool):
            index[str(cid)] = verdict
    return index


# ---------------------- Ephemeral flow cache for Config Flow -----------------
class _EphemeralCache:
    """Tiny in-memory cache used only for short-lived validation in flows.

    It implements the CacheProtocol subset that the API needs. Values are kept
    in-memory only and never persisted to disk.
    """

    def __init__(
        self,
        *,
        oauth_token: str | None,
        email: str | None,
        fcm_credentials: dict[str, Any] | None = None,
        aas_token: str | None = None,
        secrets_bundle: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the ephemeral cache with credentials.

        Args:
            oauth_token: The OAuth token.
            email: The user's Google email address.
            fcm_credentials: Optional FCM credentials dict (contains android_id).
            aas_token: Optional AAS token (if already generated by GoogleFindMyTools).
        """
        self._data: dict[str, Any] = {}
        if isinstance(email, str) and email:
            self._data[username_string] = email
        if isinstance(oauth_token, str) and oauth_token:
            self._data[CONF_OAUTH_TOKEN] = oauth_token
        if isinstance(fcm_credentials, dict) and fcm_credentials:
            self._data["fcm_credentials"] = fcm_credentials
        if isinstance(aas_token, str) and aas_token:
            self._data["aas_token"] = aas_token
        if isinstance(secrets_bundle, dict):
            fcm_creds = secrets_bundle.get("fcm_credentials")
            if isinstance(fcm_creds, dict):
                self._data.setdefault("fcm_credentials", fcm_creds)
                _LOGGER.debug(
                    "_EphemeralCache: injected fcm_credentials for validation probe."
                )
            else:
                _LOGGER.debug(
                    "_EphemeralCache: secrets bundle provided without fcm_credentials;"
                    " validation may fall back to static android id."
                )

    async def get(self, name: str) -> Any:
        """Get a value from the in-memory cache (TokenCache interface)."""

        return self._data.get(name)

    async def set(self, name: str, value: Any) -> None:
        """Set a value in the in-memory cache (TokenCache interface)."""

        if value is None:
            self._data.pop(name, None)
        else:
            self._data[name] = value

    async def all(self) -> dict[str, Any]:
        """Return all cached values (TokenCache interface)."""

        return dict(self._data)

    async def get_or_set(
        self, name: str, generator: Callable[[], Awaitable[Any] | Any]
    ) -> Any:
        """Return existing value or compute/store it (TokenCache interface)."""

        if (existing := self._data.get(name)) is not None:
            return existing

        new_value = generator()
        if asyncio.iscoroutine(new_value):
            new_value = await new_value

        await self.set(name, new_value)
        return new_value

    async def async_get_cached_value(self, key: str) -> Any:
        """Get a value from the in-memory cache (CacheProtocol interface).

        Args:
            key: The key of the value to retrieve.

        Returns:
            The cached value, or None if not found.
        """
        return await self.get(key)

    async def async_set_cached_value(self, key: str, value: Any) -> None:
        """Set a value in the in-memory cache (CacheProtocol interface).

        Args:
            key: The key of the value to set.
            value: The value to store. If None, the key is removed.
        """
        await self.set(key, value)


# ----------------------------- API class ------------------------------------
_PREVIOUS_GOOGLEFINDMYAPI = globals().get("GoogleFindMyAPI")


class GoogleFindMyAPI:
    """Async-first API wrapper for Google Find My Device.

    This class provides a high-level interface to the underlying Google Find My
    Device services. It handles authentication, data parsing, and action execution
    (like locating a device or playing a sound) in an asynchronous manner suitable
    for Home Assistant.

    Notes:
        - For runtime use, credentials/metadata come from the entry-scoped cache (TokenCache).
        - For short-lived Config/Options flows, minimal credentials may be provided directly.
        - A HA-managed aiohttp session can be reused for all network calls.
        - Push actions depend on the shared FCM receiver provider.
    """

    def __init__(
        self,
        cache: CacheProtocol | None = None,
        *,
        session: ClientSession | None = None,
        oauth_token: str | None = None,
        google_email: str | None = None,
        secrets_bundle: dict[str, Any] | None = None,
        contributor_mode: str | None = None,
        contributor_mode_switch_epoch: int | None = None,
    ) -> None:
        """Initialize the API wrapper.

        Preferred:
            Pass a TokenCache-like object via `cache`.

        Flow-friendly:
            If `cache` is not provided, you may pass `oauth_token` and/or
            `google_email`. The API will construct an ephemeral in-memory cache
            that satisfies the lookups it performs (primarily the username).

        Args:
            cache: Entry-scoped TokenCache instance (recommended for runtime).
            session: HA-managed aiohttp ClientSession to reuse for network calls.
            oauth_token: Optional OAuth token (flow validation only).
            google_email: Optional Google account e-mail (flow validation only).
            contributor_mode: Preferred contributor mode ("high_traffic" or "in_all_areas").
            contributor_mode_switch_epoch: Epoch timestamp when the contributor mode last changed.
        """
        if cache is None and (oauth_token or google_email):
            cache = _EphemeralCache(
                oauth_token=oauth_token,
                email=google_email,
                secrets_bundle=secrets_bundle,
            )
        if cache is None:
            # Runtime misuse: the coordinator should always pass a cache; flows should
            # at least pass email/token. Fail early to surface programming errors.
            raise TypeError(
                "GoogleFindMyAPI requires either `cache=` or minimal flow credentials "
                "(`oauth_token`/`google_email`)."
            )

        self._cache: CacheProtocol = cache
        self._session = session
        self._sync_loop: asyncio.AbstractEventLoop | None = None

        self._contributor_mode = self._normalize_contributor_mode(contributor_mode)
        if contributor_mode_switch_epoch is None or contributor_mode_switch_epoch <= 0:
            contributor_mode_switch_epoch = int(time.time())
        self._contributor_mode_switch_epoch = int(contributor_mode_switch_epoch)

        # Capability cache to avoid repeated network calls in capability checks.
        # Key: canonical device id, Value: can_ring (bool)
        self._device_capabilities: dict[str, bool] = {}

    async def close(self) -> None:
        """Cleanup local references."""

        # [FIX] Do not close the shared Home Assistant session.
        self._session = None
        _LOGGER.debug("API session unreferenced (shared session preserved)")

    @staticmethod
    def _normalize_contributor_mode(mode: str | None) -> str:
        """Normalize a contributor mode value."""

        if isinstance(mode, str):
            normalized = mode.strip().lower()
            if normalized in (
                CONTRIBUTOR_MODE_HIGH_TRAFFIC,
                CONTRIBUTOR_MODE_IN_ALL_AREAS,
            ):
                return normalized
        return DEFAULT_CONTRIBUTOR_MODE

    def set_contributor_mode(
        self, mode: str | None, *, switch_epoch: int | None = None
    ) -> None:
        """Update the contributor mode used for Nova requests."""

        self._contributor_mode = self._normalize_contributor_mode(mode)
        if switch_epoch is None or switch_epoch <= 0:
            switch_epoch = int(time.time())
        self._contributor_mode_switch_epoch = int(switch_epoch)

    def _sync_call_guard(self, log_message: str) -> bool:
        """Return True if a sync wrapper should abort due to an active loop."""

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False

        _LOGGER.error(log_message)
        return True

    def _resolve_sync_loop(self) -> asyncio.AbstractEventLoop:
        """Return the event loop the sync helpers should execute on."""

        if self._session is not None:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                session_loop = cast(
                    asyncio.AbstractEventLoop | None,
                    getattr(self._session, "loop", None),
                )
            if session_loop is None:
                raise RuntimeError(
                    "Unable to determine the event loop for the provided session"
                )
            if session_loop.is_closed():
                raise RuntimeError(
                    "The event loop bound to the provided session is closed"
                )
            return session_loop

        loop = self._sync_loop
        if loop is None or loop.is_closed():
            loop = asyncio.new_event_loop()
            self._sync_loop = loop
        return loop

    def _run_sync_helper(
        self,
        coro_factory: Callable[[], Awaitable[Any]],
        *,
        guard_message: str,
        context: str,
        default: Any,
    ) -> Any:
        """Execute an async helper from sync context, respecting session loops."""

        if self._sync_call_guard(guard_message):
            return default

        try:
            loop = self._resolve_sync_loop()
        except RuntimeError as err:
            _LOGGER.error("Failed to %s (sync setup): %s", context, _short_err(err))
            return default

        if loop.is_running():
            _LOGGER.error(
                "Failed to %s (sync setup): target event loop is already running",
                context,
            )
            return default

        try:
            return loop.run_until_complete(coro_factory())
        except Exception as err:  # noqa: BLE001 - surface all sync failures uniformly
            _LOGGER.error("Failed to %s (sync): %s", context, _short_err(err))
            return default

    # ------------------------ Namespace helper (entry-scope) ------------------------
    def _namespace(self) -> str | None:
        """Return an entry-scoped namespace for downstream Nova helpers.

        Prefer an explicit `entry_id` attribute on the cache; fall back to a generic
        `namespace` attribute if present. Returns None when no scope is available.
        """
        try:
            ns = getattr(self._cache, "entry_id", None) or getattr(
                self._cache, "namespace", None
            )
            if isinstance(ns, str) and ns.strip():
                return ns.strip()
        except Exception:
            pass
        return None

    def _decoder_token_cache(self) -> TokenCache | None:
        """Return the concrete TokenCache instance when available.

        The decoder helpers expect the full TokenCache implementation to resolve
        owner keys and usernames. During config flows the API uses an ephemeral
        cache, so we only forward the cache when it is the real TokenCache.
        """

        try:
            if isinstance(self._cache, TokenCache):
                return self._cache
        except Exception:
            return None
        return None

    # ------------------------ Internal processing helpers ------------------------
    def _process_device_list_response(self, result_hex: str) -> list[dict[str, Any]]:
        """Parse protobuf, update capability cache, and build basic device list.

        Args:
            result_hex: The hexadecimal string of the protobuf response.

        Returns:
            A list of dictionaries, each representing a device with its basic info.
        """
        parsed = parse_device_list_protobuf(result_hex)
        token_cache = self._decoder_token_cache()
        cap_index = _build_can_ring_index(
            parsed,
            cache=token_cache,
        )
        if cap_index:
            self._device_capabilities.update(cap_index)

        devices_by_id: OrderedDict[str, dict[str, Any]] = OrderedDict()
        # Main poll: the single pass per poll that emits the canonicless diagnostics and
        # updates the cross-poll visibility/count guards (CQS, see decoder gate).
        device_rows = get_devices_with_location(
            parsed, cache=token_cache, emit_canonicless_diagnostics=True
        )
        if device_rows:
            for device in device_rows:
                canonical_id = device.get("id")
                if not isinstance(canonical_id, str) or not canonical_id:
                    continue

                normalized = dict(device)
                last_seen = normalized.get("last_seen")
                if isinstance(last_seen, dict):
                    seconds = last_seen.get("seconds")
                    nanos = last_seen.get("nanos", 0)
                    if isinstance(seconds, (int, float)):
                        normalized["last_seen"] = (
                            float(seconds) + float(nanos or 0) / 1e9
                        )
                elif hasattr(last_seen, "seconds"):
                    seconds = getattr(last_seen, "seconds", None)
                    nanos = getattr(last_seen, "nanos", 0)
                    if isinstance(seconds, (int, float)):
                        normalized["last_seen"] = (
                            float(seconds) + float(nanos or 0) / 1e9
                        )

                accuracy = normalized.get("accuracy")
                if accuracy is None:
                    accuracy_meters = normalized.get("accuracy_meters")
                    if isinstance(accuracy_meters, (int, float)):
                        normalized["accuracy"] = float(accuracy_meters)

                for key in ("latitude", "longitude"):
                    val = normalized.get(key)
                    if isinstance(val, bool):
                        continue
                    if isinstance(val, int):
                        normalized[key] = float(val) / 1e7
                    elif isinstance(val, float) and abs(val) > 180:
                        normalized[key] = val / 1e7

                can_ring_hint = self._device_capabilities.get(canonical_id)
                if can_ring_hint is not None:
                    normalized["can_ring"] = bool(can_ring_hint)

                devices_by_id[canonical_id] = normalized
        else:
            for device_name, canonic_id in get_canonic_ids(parsed):
                canonical_id = str(canonic_id)
                if not canonical_id:
                    continue

                can_ring_hint = self._device_capabilities.get(canonical_id)
                item: dict[str, Any] = {
                    "name": device_name,
                    "id": canonical_id,
                    "device_id": canonical_id,
                }
                if can_ring_hint is not None:
                    item["can_ring"] = bool(can_ring_hint)
                devices_by_id.setdefault(canonical_id, item)

        return list(devices_by_id.values())

    def _extend_with_empty_location_fields(
        self, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Augment basic device entries with common location fields set to None.

        Args:
            items: A list of basic device dictionaries.

        Returns:
            A new list of device dictionaries with added placeholder location fields.
        """
        extended: list[dict[str, Any]] = []
        for base in items:
            dev = {
                **base,
                "latitude": None,
                "longitude": None,
                "altitude": None,
                "accuracy": None,
                "last_seen": None,
                "status": "No location data (requires individual request)",
                "is_own_report": None,
                "semantic_name": None,
                "battery_level": None,
            }
            extended.append(dev)
        return extended

    def _select_best_location(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Pick the most relevant location record with sensible tie-breaking.

        Primary ordering is driven by ``last_seen``. When multiple records share the
        most recent timestamp, prefer ones reported by the device owner
        (``is_own_report``). If ownership is also tied, fall back to the most precise
        location (lowest accuracy value). Ultimately, list order wins when all
        ranking metrics are identical.

        Args:
            records: A list of location data dictionaries for a device.

        Returns:
            The single best location record dictionary.
        """
        if not records:
            return {}

        best_record, _ = _decoder_select_best_location(records)
        if best_record is not None:
            return best_record
        return records[0]

    # ------------------------ FCM helper (via provider) --------------------------
    def _get_fcm_token_for_action(self) -> str | None:
        """Return a valid FCM token for action requests via the shared receiver.

        Notes:
            - Uses the provider installed by the integration (HA-managed singleton).
            - Returns None if the provider is missing or a token cannot be obtained.

        Returns:
            The FCM token as a string, or None if unavailable.
        """
        if _FCM_ReceiverGetter is None:
            _LOGGER.error("Cannot obtain FCM token: no provider registered.")
            return None
        entry_id: str | None
        try:
            raw_entry_id = getattr(self._cache, "entry_id", None)
        except Exception:
            raw_entry_id = None
        if isinstance(raw_entry_id, str) and raw_entry_id.strip():
            entry_id = raw_entry_id.strip()
        else:
            entry_id = self._namespace()
        receiver: FcmReceiverProtocol | None
        try:
            receiver = cast(
                Callable[[str | None], FcmReceiverProtocol], _FCM_ReceiverGetter
            )(entry_id)
        except TypeError:
            _LOGGER.debug(
                "FCM receiver provider does not accept entry context; retrying without entry_id."
            )
            try:
                receiver = cast(
                    Callable[[], FcmReceiverProtocol], _FCM_ReceiverGetter
                )()
            except Exception as err:
                _LOGGER.error(
                    "Cannot obtain FCM token: provider callable failed (legacy path)",
                    exc_info=err,
                )
                return None
        except Exception as err:
            _LOGGER.error(
                "Cannot obtain FCM token: provider callable failed",
                exc_info=err,
            )
            return None
        if receiver is None:
            _LOGGER.error("Cannot obtain FCM token: provider returned None.")
            return None
        try:
            if entry_id is not None:
                token = receiver.get_fcm_token(entry_id)
            else:
                token = receiver.get_fcm_token()
        except TypeError:
            _LOGGER.debug(
                "FCM token provider does not accept entry-scoped lookups; falling back to legacy call."
            )
            try:
                token = receiver.get_fcm_token()
            except Exception as err:
                _LOGGER.error(
                    "Cannot obtain FCM token from shared receiver (legacy fallback failed)",
                    exc_info=err,
                )
                return None
        except Exception as err:
            _LOGGER.error("Cannot obtain FCM token from shared receiver", exc_info=err)
            return None
        if not token or not isinstance(token, str) or len(token) < 10:
            _LOGGER.error("FCM token not available or invalid (via shared receiver).")
            return None
        return token

    def _peek_fcm_token_quietly(self) -> str | None:
        """Best-effort token probe for readiness checks (no ERROR-level log spam).

        Returns:
            Token string when obtainable; otherwise None. All failures are logged at DEBUG.
        """
        if _FCM_ReceiverGetter is None:
            _LOGGER.debug("FCM readiness probe: no provider registered.")
            return None
        entry_id: str | None
        try:
            raw_entry_id = getattr(self._cache, "entry_id", None)
        except Exception:
            raw_entry_id = None
        if isinstance(raw_entry_id, str) and raw_entry_id.strip():
            entry_id = raw_entry_id.strip()
        else:
            entry_id = self._namespace()
        receiver: FcmReceiverProtocol | None
        try:
            receiver = cast(
                Callable[[str | None], FcmReceiverProtocol], _FCM_ReceiverGetter
            )(entry_id)
        except TypeError:
            _LOGGER.debug(
                "FCM readiness probe: provider does not accept entry context; retrying without entry_id."
            )
            try:
                receiver = cast(
                    Callable[[], FcmReceiverProtocol], _FCM_ReceiverGetter
                )()
            except Exception as err:
                _LOGGER.debug(
                    "FCM readiness probe: provider callable failed (legacy path)",
                    exc_info=err,
                )
                return None
        except Exception as err:
            _LOGGER.debug(
                "FCM readiness probe: provider callable failed",
                exc_info=err,
            )
            return None
        if receiver is None:
            _LOGGER.debug("FCM readiness probe: provider returned None.")
            return None
        try:
            if entry_id is not None:
                token = receiver.get_fcm_token(entry_id)
            else:
                token = receiver.get_fcm_token()
        except TypeError:
            _LOGGER.debug(
                "FCM readiness probe: receiver does not accept entry_id; retrying without scoped parameter."
            )
            try:
                token = receiver.get_fcm_token()
            except Exception as err:
                _LOGGER.debug(
                    "FCM readiness probe: legacy get_fcm_token call failed",
                    exc_info=err,
                )
                return None
        except Exception as err:
            _LOGGER.debug(
                "FCM readiness probe: get_fcm_token failed",
                exc_info=err,
            )
            return None
        if not token or not isinstance(token, str) or len(token) < 10:
            _LOGGER.debug("FCM readiness probe: token missing or too short.")
            return None
        return token

    # ----------------------------- Device enumeration ----------------------------
    async def async_get_basic_device_list(
        self,
        username: str | None = None,
        *,
        # Flow/local validation overrides (passed through to Nova):
        token: str | None = None,
        cache_get: Callable[[str], Awaitable[Any]] | None = None,
        cache_set: Callable[[str, Any], Awaitable[None]] | None = None,
        refresh_override: Callable[[], Awaitable[str | None]] | None = None,
    ) -> list[dict[str, Any]]:
        """Async variant of the lightweight device list used by HA flows/coordinator.

        This method fetches a list of devices associated with the Google account,
        including their names, IDs, and ringing capability.

        Args:
            username: The Google account email. If None, it will be retrieved from the cache.
            token: Optional auth token override to use for this call only.
            cache_get: Optional async getter for TTL/aux metadata (flow-local).
            cache_set: Optional async setter for TTL/aux metadata (flow-local).
            refresh_override: Optional async callable to refresh/obtain a token for this call.

        Returns:
            A list of minimal device dicts (id, name, optional can_ring).

        Raises:
            ConfigEntryAuthFailed: If authentication fails.
            UpdateFailed: If the API is rate-limited, returns a server error, or a network/other error occurs.
        """
        # Pass cache explicitly for multi-account isolation (no global registration)
        try:
            if not username:
                try:
                    username = await self._cache.async_get_cached_value(username_string)
                except Exception:
                    username = None

            # Prefer the HA-managed session if available.
            sess = self._session

            # Provide defaults for TTL metadata I/O if the caller didn't override.
            cg = cache_get or self._cache.async_get_cached_value
            cs = cache_set or self._cache.async_set_cached_value

            # Forward flow/local knobs to the Nova ListDevices helper. If the installed
            # helper is older and does not support these kwargs, gracefully fall back.
            try:
                result_hex = await async_request_device_list(
                    username,
                    session=sess,
                    cache=cast("TokenCache | None", self._cache),
                    token=token,
                    cache_get=cg,
                    cache_set=cs,
                    refresh_override=refresh_override,
                    namespace=self._namespace(),
                )
            except TypeError:
                # Older helper signature (no pass-through); best-effort fallback matrix.
                legacy_request = cast(Any, async_request_device_list)
                if sess is not None:
                    try:
                        result_hex = await legacy_request(username, session=sess)
                    except TypeError:
                        result_hex = await legacy_request(username)
                else:
                    result_hex = await legacy_request(username)

            payload = self._process_device_list_response(result_hex)
            _LOGGER.debug("nbe_list_devices: count=%d", len(payload))
            return payload

        except asyncio.CancelledError:
            raise

        except NovaRateLimitError as err:
            _LOGGER.warning("Device list temporarily rate-limited: %s", _short_err(err))
            raise UpdateFailed(
                _short_err(err),
                translation_domain=DOMAIN,
                translation_key="device_list_rate_limited",
            ) from err

        except NovaHTTPError as err:
            # Map 401/403 explicitly to ConfigEntryAuthFailed
            if getattr(err, "status", None) in (401, 403):
                _LOGGER.error(
                    "Authentication failed (HTTP %s) while listing devices: %s",
                    err.status,
                    _short_err(err),
                )
                exc = ConfigEntryAuthFailed(_short_err(err))
                exc.reauth_code = ReauthReasonCode.HTTP_401_AFTER_REFRESH
                raise exc from err
            _LOGGER.warning(
                "Device list temporarily unavailable (server error %s): %s",
                err.status,
                _short_err(err),
            )
            raise UpdateFailed(
                _short_err(err),
                translation_domain=DOMAIN,
                translation_key="device_list_server_error",
                translation_placeholders={"status": str(err.status)},
            ) from err

        except NovaAuthError as err:
            # Branch on the STATUS, never on the type. The transport raises this
            # class for every non-retryable 4xx, so a deleted device or a
            # malformed request arrived here as "your sign-in expired" and
            # produced an immediate re-auth prompt with no threshold in front of
            # it. A non-credential rejection is a server-side refusal and takes
            # the same exit as a 5xx one branch up: UpdateFailed,
            # ApiStatus.ERROR, no reauth reason, no Repairs issue.
            if not is_credential_rejection(err):
                _LOGGER.warning(
                    "Device list rejected by the server (HTTP %s): %s",
                    getattr(err, "status", "unknown"),
                    _short_err(err),
                )
                raise UpdateFailed(
                    _short_err(err),
                    translation_domain=DOMAIN,
                    translation_key="device_list_rejected",
                    translation_placeholders={
                        "status": str(getattr(err, "status", "unknown"))
                    },
                ) from err

            # Mirror the location path's base handler: a permanent credential
            # failure (NovaAuthPermanentError subclass, or a base NovaAuthError
            # flagged is_permanent=True after token refresh) must record the
            # permanent reason, not the generic/transient one, so diagnostics
            # point triage at the right credential layer. Control flow is
            # unchanged (both cases raise ConfigEntryAuthFailed); only the
            # reason code and log wording differ by permanence.
            exc = ConfigEntryAuthFailed(_short_err(err))
            if err.is_permanent:
                _LOGGER.error(
                    "Permanent authentication failure while listing devices: %s",
                    _short_err(err),
                )
                exc.reauth_code = ReauthReasonCode.NOVA_AUTH_PERMANENT
            else:
                _LOGGER.error(
                    "Authentication failed while listing devices: %s",
                    _short_err(err),
                )
                exc.reauth_code = ReauthReasonCode.NOVA_AUTH_FAILED
            raise exc from err

        except NovaProtobufDecodeError as err:
            # Protobuf decode failures indicate corrupted response or protocol mismatch
            _LOGGER.error("Failed to decode device list response: %s", _short_err(err))
            raise UpdateFailed(
                _short_err(err),
                translation_domain=DOMAIN,
                translation_key="device_list_decode_failed",
            ) from err

        except NovaLogicError as err:
            # Logic errors from Google (e.g., invalid device ID, permission denied)
            _LOGGER.error(
                "Nova API Logic Error while listing devices: Code %s - %s",
                err.code,
                err.message or "Unknown",
            )
            raise UpdateFailed(
                _short_err(err),
                translation_domain=DOMAIN,
                translation_key="device_list_logic_error",
            ) from err

        # Normalize gpsoauth/ADM "BadAuthentication" style failures to ConfigEntryAuthFailed
        except (RuntimeError, ValueError) as err:
            msg = str(err)
            if (
                "BadAuthentication" in msg
                or "Missing 'Token' in gpsoauth" in msg
                or "Bad Authentication" in msg
            ):
                _LOGGER.error("Authentication failed (gpsoauth): %s", _short_err(msg))
                exc = ConfigEntryAuthFailed(_short_err(msg))
                exc.reauth_code = ReauthReasonCode.BADAUTH_GPSOAUTH
                raise exc from err

            # TokenCache closed indicates the integration is in an invalid state
            # (e.g., after a failed reload or during shutdown). Trigger re-auth
            # to force a clean re-initialization of the entry.
            if "TokenCache is closed" in msg:
                _LOGGER.error(
                    "TokenCache is closed; integration state is invalid. "
                    "Triggering re-authentication to reinitialize."
                )
                exc = ConfigEntryAuthFailed(
                    "Integration state invalid (cache closed); please re-authenticate"
                )
                exc.reauth_code = ReauthReasonCode.TOKENCACHE_CLOSED
                raise exc from err

            # Detect and tame the multi-entry guard (INFO once, DEBUG thereafter)
            if _is_multi_entry_guard_message(msg):
                # Try to enrich with context if available from cache (best-effort)
                try:
                    email = await self._cache.async_get_cached_value(username_string)
                except Exception:
                    email = None
                entry_id = getattr(self._cache, "entry_id", None)
                _maybe_log_guard_once("device_list", email=email, entry_id=entry_id)

                # Still raise UpdateFailed so the coordinator/flow can keep semantics,
                # and the flow can recognize the guard by message content.
                raise UpdateFailed(
                    _short_err(msg),
                    translation_domain=DOMAIN,
                    translation_key="multi_entry_guard",
                ) from err

            _LOGGER.warning(
                "Failed to get basic device list (runtime/value): %s", _short_err(err)
            )
            raise UpdateFailed(
                _short_err(err),
                translation_domain=DOMAIN,
                translation_key="device_list_failed",
            ) from err

        except ClientError as err:
            # Minimal-invasive change: do not degrade to empty success; signal transient failure.
            _LOGGER.warning(
                "Failed to get basic device list (async, network): %s", _short_err(err)
            )
            raise UpdateFailed(
                f"Network error fetching device list: {_short_err(err)}",
                translation_domain=DOMAIN,
                translation_key="device_list_network_error",
            ) from err

        except Exception as err:
            # Do not mask unexpected errors as an empty list; let the coordinator keep last good data.
            _LOGGER.error(
                "Failed to get basic device list (async): %s", _short_err(err)
            )
            raise UpdateFailed(
                f"Unexpected error fetching device list: {_short_err(err)}",
                translation_domain=DOMAIN,
                translation_key="device_list_unexpected_error",
            ) from err

    def get_basic_device_list(self) -> list[dict[str, Any]]:
        """Thin sync wrapper around async_get_basic_device_list for non-HA contexts.

        Guard:
            If called inside a running event loop (e.g., HA), logs and returns [].

        Returns:
            A list of device dictionaries.
        """
        result = self._run_sync_helper(
            self.async_get_basic_device_list,
            guard_message=(
                "get_basic_device_list() called inside an active event loop; use async_get_basic_device_list()."
            ),
            context="get basic device list",
            default=[],
        )
        return cast(list[dict[str, Any]], result)

    def get_devices(self) -> list[dict[str, Any]]:
        """Return devices with basic info only; no up-front location fetch (sync wrapper).

        Returns:
            A list of device dictionaries augmented with empty location fields.
        """
        base = self.get_basic_device_list()
        if base:
            _LOGGER.info("API v3.0: Returning %d devices (basic)", len(base))
        return self._extend_with_empty_location_fields(base)

    # --------------------------------- Location ----------------------------------
    async def async_get_device_location(
        self, device_id: str, device_name: str
    ) -> dict[str, Any]:
        """Async, HA-compatible location request for a single device.

        This function requests the location for a specific device and selects the most
        relevant location record from the response.

        **Auth mapping (5.1-D):**
            - `ConfigEntryAuthFailed` is raised ONLY where re-auth must start
              immediately, without a threshold in front of it: a `NovaHTTPError`
              with status 401/403, or a PERMANENT Nova auth failure (the
              `NovaAuthPermanentError` subclass, or a base `NovaAuthError`
              flagged `is_permanent=True` after the refresh sequence).
            - A plain, NON-permanent credential rejection -- in practice a 403,
              since a 401 that survives the refresh arrives flagged permanent --
              is re-raised as `NovaAuthError`, NOT converted. That is deliberate:
              the polling coordinator counts consecutive transient auth failures
              and escalates only at its own threshold, so converting here would
              prompt the user on the first occurrence. This asymmetry is the one
              difference to the device-list handler, which converts every
              credential rejection because it has no such counter behind it.
            - Any OTHER `NovaAuthError` is the server refusing the request, not
              the credentials -- a device removed from the account, a malformed
              body -- and is re-raised unchanged. It is deliberately NOT turned
              into an empty result: a return WITH CONTENT is what both callers
              read as "the credentials worked", and they clear the account auth
              state on it. An empty return no longer reaches those resets -- they
              sit behind the empty guard -- but collapsing a rejection into `{}`
              would still be wrong, because a failure that presents as an idle
              device is indistinguishable from a healthy idle device. Only a
              transient/5xx failure returns `{}`.
            - Because the previous two bullets BOTH arrive as `NovaAuthError`,
              the type alone never tells a caller which one it holds. Every
              caller must ask `nova_request.is_credential_rejection`.
            - Rate limit / other server issues are treated as transient and return `{}`.

        Args:
            device_id: The canonical ID of the device.
            device_name: The human-readable name of the device for logging.

        Returns:
            A dictionary containing the best available location data for the
            device, or an empty dictionary on a transient failure (5xx, rate
            limit, timeout).

            A return WITH CONTENT means the credentials were accepted, and both
            callers rely on that: they clear the account auth state on it. An
            EMPTY return means only that nothing raised. Callers must not read it
            as proof -- four pre-accept failures still arrive this way, and so
            does every healthy idle BLE tag. Since
            `PLAN_GFMY_AUTH_RESET_POSITIVE_PROOF` both callers check for
            emptiness first, and in the poll loop an empty result counts merely
            as "this request was not refused", which is enough to break a streak
            of credential rejections and deliberately not enough to clear the
            auth state.

            That reliance is only as sound as the layer below. It holds exactly
            to the extent that a request which never got past this integration's
            ACCEPT POINT raises rather than returning empty -- see
            `LocationRequestNotAcceptedError`
            below. Those raise sites now exist: a 5xx, a 429, a network error and
            a failed FCM registration leave `location_request.py` as that
            exception, not through this exit. What still arrives here as an empty
            list is what the layer below vouches for as ACCEPTED, which is not
            the same as healthy, and is deliberately NOT a closed list: the idle
            BLE tag; the post-accept branches of the locate flow (the
            unexpected-device mismatch, and any error the outer surfacing handler
            catches once the accept line has been passed); and every failure the
            FCM callback absorbs on its own, which is a fully-catching layer of
            its own after that line -- an unreadable push, a decode failure, a
            push for another device. Reading an empty result as "healthy" was the
            defect; reading it as "one of exactly three things" would be the next
            one.

        Raises:
            ConfigEntryAuthFailed: Re-auth must start at once -- an HTTP 401/403
                (`NovaHTTPError`), or a permanent Nova auth failure. The
                coordinator starts re-authentication.
            NovaAuthError: Everything else the transport raised under that name,
                re-raised unchanged. Two distinct cases share this exit: a
                NON-permanent credential rejection (403), which the polling
                coordinator counts toward its own threshold, and a server-side
                refusal of this request (a non-retryable 4xx such as 400/404/422)
                that says nothing about the credentials. Callers MUST tell them
                apart with `nova_request.is_credential_rejection`; the type does
                not, and reading the type is the defect this contract exists to
                prevent.
            LocationRequestNotAcceptedError: The locate request never got past this
                integration's accept point (which a 429 and a 5xx also miss, having
                reached the server and been answered). Re-raised unchanged so the
                accepted-versus-
                failed outcome survives this boundary instead of being flattened
                into `{}` and reconstructed downstream once the evidence is gone.
                It claims a POSITION in the request flow, never a cause, so it is
                NOT an auth verdict: a request that was not accepted says nothing
                about the credentials, and this exit must not be folded into the
                auth mapping above.
        """

        # Register cache provider for multi-entry support
        def _cache_provider() -> CacheProtocol | None:
            return self._cache

        nova_request.register_cache_provider(_cache_provider)

        try:
            _LOGGER.debug(
                "API v3.0 Async: Requesting location for %s (%s)",
                device_name,
                device_id,
            )
            # Prefer new signature with entry namespace; fall back gracefully.
            try:
                records = await get_location_data_for_device(
                    device_id,
                    device_name,
                    session=self._session,
                    namespace=self._namespace(),
                    cache=cast("TokenCache | None", self._cache),
                    contributor_mode=self._contributor_mode,
                    last_mode_switch=self._contributor_mode_switch_epoch,
                )
            except TypeError:
                try:
                    records = await get_location_data_for_device(
                        device_id,
                        device_name,
                        session=self._session,
                        cache=cast("TokenCache | None", self._cache),
                        contributor_mode=self._contributor_mode,
                        last_mode_switch=self._contributor_mode_switch_epoch,
                    )
                except TypeError:
                    try:
                        records = await get_location_data_for_device(
                            device_id,
                            device_name,
                            session=self._session,
                            cache=cast("TokenCache | None", self._cache),
                        )
                    except TypeError:
                        legacy_location = cast(Any, get_location_data_for_device)
                        records = await legacy_location(
                            device_id, device_name, session=self._session
                        )
            best = self._select_best_location(records)
            if best:
                _LOGGER.debug(
                    "API v3.0 Async: Selected location record for %s (have %d total)",
                    device_name,
                    len(records),
                )
                # _select_best_location ranks by newest last_seen and may return a
                # report-less SEMANTIC/metadata row, hiding a sibling coordinate
                # report that decrypted successfully. Carry the FULL-list decrypt
                # proof as an internal hint so the poll loop's reauth-budget gate
                # is not fooled by the collapsed view (consumers pop it before
                # caching, like _report_hint). See any_real_location_record.
                best["_decrypt_proven"] = any_real_location_record(records)
                return best
            _LOGGER.debug("API v3.0 Async: No location data for %s", device_name)
            return {}

        except SpotAuthPermanentError:
            raise

        except NovaAuthPermanentError as err:
            # Permanent auth failure (AAS token invalid) - immediate reauth required
            # This path also runs on every poll cycle, so records at INFO and above
            # name the device by id and the display name goes to a DEBUG companion
            # (AGENTS.md section 5 (b)).
            _LOGGER.error(
                "Permanent authentication failure for device %s: %s. Re-authentication required.",
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            exc = ConfigEntryAuthFailed(_short_err(err))
            exc.reauth_code = ReauthReasonCode.NOVA_AUTH_PERMANENT
            raise exc from err

        except NovaAuthError as err:
            # Branch on the STATUS, never on the type. Same criterion as the
            # device-list handler and the two sound handlers; see
            # nova_request.is_credential_rejection.
            if not is_credential_rejection(err):
                # The server refused the REQUEST, not the credentials: a device
                # removed from the account, a malformed body. Pass it through so
                # each caller can take its own non-auth exit; both of them have a
                # branch for exactly this status.
                #
                # Do NOT collapse this to `return {}`. That was the first attempt
                # and it traded one defect for another. The first reason is now
                # history and is kept as such: a non-raising return used to be
                # what both callers read as positive proof that the sign-in
                # works. coordinator/polling.py cleared the auth state and reset
                # the transient-auth counter, and coordinator/locate.py cleared
                # the auth state, each BEFORE looking at whether the result was
                # empty, so a permanently rejected tracker wiped a real 401 from
                # another tracker in every cycle. Both resets now sit BEHIND the
                # empty guard (`PLAN_GFMY_AUTH_RESET_POSITIVE_PROOF`), so an
                # empty return cannot reach them any more.
                #
                # The other half of the reason outlives that fix and is why this
                # still raises: a client rejection is a failure, and `{}` is the
                # shape of a healthy idle tag. Handing the caller that shape
                # hands it a result it cannot classify -- no `cycle_failed`,
                # nothing for the post-loop guard to count -- and ending exactly
                # that ambiguity is what the raise sites exist for.
                #
                # The 5xx branch below still returns {}. It no longer clears
                # anything, and measured it is not reached at all any more:
                # `get_location_data_for_device` converts the 5xx and the 429
                # into `LocationRequestNotAcceptedError` itself
                # (`PLAN_GFMY_EMPTY_RESULT_DISTINGUISHABLE`), and its broad
                # handler re-raises only before the accept line and returns []
                # after it -- so no `NovaHTTPError` leaves that layer. The sync
                # wrapper is no exception: it calls this very method. The branch
                # stays as the guard for the day that conversion moves or goes.
                #
                # DEBUG, not WARNING: both callers already log a WARNING naming
                # the device, so a WARNING here would print the same event twice
                # per device per poll cycle. This line exists for the sync
                # wrapper, which has no handler behind it.
                _LOGGER.debug(
                    "Client error (HTTP %s) while getting location for %s (%s): %s",
                    getattr(err, "status", "unknown"),
                    device_name,
                    device_id,
                    _short_err(err),
                )
                raise

            # Transient auth failure - may self-heal in subsequent poll cycles.
            # Re-raise so coordinator can track consecutive failures before triggering reauth.
            if err.is_permanent:
                _LOGGER.error(
                    "Permanent authentication failure for device %s: %s",
                    device_id,
                    _short_err(err),
                )
                _LOGGER.debug(
                    "Location lookup failure for device %s concerns %s",
                    device_id,
                    device_name,
                )
                exc = ConfigEntryAuthFailed(_short_err(err))
                exc.reauth_code = ReauthReasonCode.NOVA_AUTH_PERMANENT
                raise exc from err

            _LOGGER.warning(
                "Transient authentication error for device %s: %s. May resolve in next poll cycle.",
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            # Re-raise to let coordinator track consecutive failures
            raise

        except NovaHTTPError as err:
            # Map 401/403 to ConfigEntryAuthFailed; other HTTP errors are transient here.
            if getattr(err, "status", None) in (401, 403):
                _LOGGER.error(
                    "Authentication failed (HTTP %s) while getting location for device %s: %s",
                    err.status,
                    device_id,
                    _short_err(err),
                )
                _LOGGER.debug(
                    "Location lookup failure for device %s concerns %s",
                    device_id,
                    device_name,
                )
                exc = ConfigEntryAuthFailed(_short_err(err))
                exc.reauth_code = ReauthReasonCode.HTTP_401_AFTER_REFRESH
                raise exc from err
            _LOGGER.warning(
                "Server error (%s) while getting location for device %s: %s",
                err.status,
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            return {}

        except NovaRateLimitError as err:
            _LOGGER.warning(
                "Location request rate-limited for device %s: %s",
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            return {}

        except NovaProtobufDecodeError as err:
            # Protobuf decode failures indicate corrupted response or protocol mismatch
            _LOGGER.error(
                "Failed to decode location response for device %s: %s",
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            return {}

        except NovaLogicError as err:
            # Logic errors from Google (e.g., invalid device ID, permission denied)
            _LOGGER.error(
                "Nova API Logic Error for device %s: Code %s - %s",
                device_id,
                err.code,
                err.message or "Unknown",
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            return {}

        except ClientError as err:
            _LOGGER.error(
                "Network error while getting async location for device %s: %s",
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            return {}

        except DecryptionError:
            # Audit finding A1: DecryptionError is a RuntimeError subclass, so the
            # broad `except RuntimeError` / `except Exception` below would silently
            # swallow an auth-fatal stale-shared-key failure into an empty result.
            # Re-raise it so the coordinator can count it and escalate to a reauth
            # flow (or per-tracker repair). This is the layer that must stay
            # transparent for the location_request fix to have any effect.
            raise

        except OwnerKeyLookupTransientError:
            # A transient owner-key lookup miss (base Exception, NOT a
            # DecryptionError and NOT a RuntimeError): the transient must reach the
            # coordinator sink for a DEBUG skip without a counter touch. The broad
            # `except Exception` below would otherwise turn it into an empty result,
            # hiding the transient from the coordinator. Analog to the A1
            # `except DecryptionError: raise` guard above.
            raise

        except LocationRequestNotAcceptedError:
            # The locate request never got past this integration's accept point (no FCM
            # token, a failed FCM registration, a 429, a 5xx, a network error, an
            # unclassified Nova failure, or a surfacing error before the accept
            # line). This layer must stay transparent for it, exactly like the two
            # guards above: the broad `except Exception` below would turn it back
            # into `return {}`, and an empty dict is precisely the evidence loss
            # the signal exists to prevent. That one handler is the whole reason
            # this branch exists -- `except RuntimeError` in between never sees the
            # type, whose base is `Exception` and deliberately NOT `RuntimeError`
            # (see the class docstring). Placement is therefore load-bearing
            # against the broad handler specifically: being LAST would make this
            # branch unreachable, while sitting anywhere ahead of it suffices.
            #
            # Note what this pass-through does NOT do: it does not classify. The
            # signal names a POSITION in the request flow, never a cause, so the
            # auth mapping documented above is untouched by it -- an unreachable
            # server says nothing about the credentials.
            #
            # This receiver was installed one step BEFORE the seven raise sites in
            # `location_request.py` existed. The order is deliberate, though not
            # for the reason it is tempting to state: with NO receiver anywhere,
            # a sender-first step would have been INERT rather than dangerous,
            # because the broad `except Exception` below flattens the signal to
            # `{}` before any coordinator sees it. The danger is the half-wired
            # state -- this pass-through present and the coordinator's not: the
            # signal then reaches the poll loop's broad per-device handler, which
            # sets both `cycle_failed` and `last_exception`, and a single 5xx on a
            # single tracker would mark every entity of the account unavailable.
            raise

        except RuntimeError as err:
            # Startup safety net: during cold boot, the FCM provider may not yet be registered.
            # Downgrade this expected transient to DEBUG and retry on the next cycle.
            if "FCM receiver provider has not been registered" in str(err):
                _LOGGER.debug(
                    "Startup race: FCM provider not ready for %s (%s). Will retry on next cycle.",
                    device_name,
                    device_id,
                )
                return {}
            _LOGGER.error(
                "Runtime error while getting async location for device %s: %s",
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            return {}

        except Exception as err:
            _LOGGER.error(
                "Failed to get async location for device %s: %s",
                device_id,
                _short_err(err),
            )
            _LOGGER.debug(
                "Location lookup failure for device %s concerns %s",
                device_id,
                device_name,
            )
            return {}

    def get_device_location(self, device_id: str, device_name: str) -> dict[str, Any]:
        """Thin sync wrapper around async_get_device_location for non-HA contexts.

        Args:
            device_id: The canonical ID of the device.
            device_name: The human-readable name of the device.

        Returns:
            A dictionary containing location data, or an empty dictionary on failure.
        """
        result = self._run_sync_helper(
            lambda: self.async_get_device_location(device_id, device_name),
            guard_message=(
                "get_device_location() called inside an active event loop; use async_get_device_location()."
            ),
            context=f"get location for {device_name} ({device_id})",
            default={},
        )
        return cast(dict[str, Any], result)

    def locate_device(self, device_id: str) -> dict[str, Any]:
        """Compatibility sync entrypoint for location (uses sync wrapper).

        Args:
            device_id: The canonical ID of the device.

        Returns:
            A dictionary containing location data.
        """
        return self.get_device_location(device_id, device_id)

    # ------------------------ Play/Stop Sound / Push readiness -------------------
    def is_push_ready(self) -> bool:
        """Return True if the push transport (FCM) appears initialized and ready.

        Heuristics (no I/O, no blocking):
          1) Use receiver-level readiness flags when available (is_ready/ready).
          2) Inspect push client state on the receiver (pc.run_state == STARTED and pc.do_listen).

        This keeps API- and coordinator-level gating consistent while avoiding tight
        coupling to specific FCM client classes and enums.
        """
        # No provider registered?
        if _FCM_ReceiverGetter is None:
            return False

        # Resolve live receiver (may change across reloads)
        entry_id: str | None
        try:
            raw_entry_id = getattr(self._cache, "entry_id", None)
        except Exception:
            raw_entry_id = None
        if isinstance(raw_entry_id, str) and raw_entry_id.strip():
            entry_id = raw_entry_id.strip()
        else:
            entry_id = self._namespace()

        try:
            receiver = cast(
                Callable[[str | None], FcmReceiverProtocol], _FCM_ReceiverGetter
            )(entry_id)
        except TypeError:
            try:
                receiver = cast(
                    Callable[[], FcmReceiverProtocol], _FCM_ReceiverGetter
                )()
            except Exception:
                return False
        except Exception:
            return False
        if receiver is None:
            return False

        # 1) Receiver-level booleans
        for attr in ("is_ready", "ready"):
            val = getattr(receiver, attr, None)
            if isinstance(val, bool):
                return val

        # 2) Push client heuristic: tolerate enum or string for run_state
        pc = getattr(receiver, "pc", None)
        if pc is not None:
            state = getattr(pc, "run_state", None)
            state_name = getattr(state, "name", state)  # enum.name or raw
            if state_name == "STARTED" and bool(getattr(pc, "do_listen", False)):
                return True

        return False

    @property
    def push_ready(self) -> bool:
        """Back-compat property variant of is_push_ready()."""
        return self.is_push_ready()

    def can_play_sound(self, device_id: str) -> bool | None:
        """Return a verdict whether 'Play Sound' is supported for this device.

        Strategy:
            - If push is not ready -> False.
            - Check internal capability cache first (no network).
            - If capability is unknown -> return None (let the caller decide optimistically).

        Args:
            device_id: The canonical ID of the device.

        Returns:
            True if the device can play sound, False if not, or None if unknown.
        """
        if not self.is_push_ready():
            return False
        if device_id in self._device_capabilities:
            return bool(self._device_capabilities[device_id])
        return None

    # ---------- Play/Stop Sound (sync wrappers; for CLI/non-HA) ----------
    def play_sound(self, device_id: str) -> bool:
        """Thin sync wrapper around async_play_sound for non-HA contexts.

        The classification is deliberately dropped here: this entry point exists
        for CLI use and has no consumer that could act on it. HA callers use
        ``async_play_sound`` and read ``PlaySoundResult.outcome``.

        Args:
            device_id: The canonical ID of the device.

        Returns:
            True if Nova accepted the command (HTTP 200), False otherwise.
        """
        result = self._run_sync_helper(
            lambda: self.async_play_sound(device_id),
            guard_message=(
                "play_sound() called inside an active event loop; use async_play_sound()."
            ),
            context=f"play sound on {device_id}",
            default=PlaySoundResult(SoundDispatchOutcome.INTERNAL_ERROR),
        )
        return cast(PlaySoundResult, result).accepted

    def stop_sound(self, device_id: str, request_uuid: str | None = None) -> bool:
        """Thin sync wrapper around async_stop_sound for non-HA contexts.

        The classification is deliberately dropped here, for the same reason as in
        ``play_sound``: this entry point is for CLI use. HA callers use
        ``async_stop_sound`` and read the ``SoundDispatchOutcome``.

        Args:
            device_id: The canonical ID of the device.

        Returns:
            True if Nova accepted the command (HTTP 200), False otherwise.
        """
        result = self._run_sync_helper(
            lambda: self.async_stop_sound(device_id, request_uuid),
            guard_message=(
                "stop_sound() called inside an active event loop; use async_stop_sound()."
            ),
            context=f"stop sound on {device_id}",
            default=SoundDispatchOutcome.INTERNAL_ERROR,
        )
        return cast(SoundDispatchOutcome, result) is SoundDispatchOutcome.ACCEPTED

    # ---------- Play/Stop Sound (async; HA-first) ----------
    async def async_play_sound(self, device_id: str) -> PlaySoundResult:
        """Send a 'Play Sound' command to a device (async path for HA).

        Auth mapping note:
            A credential rejection here is logged and returned as REJECTED_AUTH
            (service call context), since re-auth is primarily driven by the
            coordinator’s data update path. "Credential rejection" means the
            STATUS says so (401/403) or the error is flagged permanent, not
            merely that the transport raised ``NovaAuthError`` -- that type also
            carries 400, 404 and the other non-retryable 4xx, which return
            REJECTED_SERVER. See ``_classify_nova_auth_error``.

        Args:
            device_id: The canonical ID of the device.

        Returns:
            A `PlaySoundResult` carrying two independent facts. `outcome` names who
            refused (see `SoundDispatchOutcome`); only `TRANSPORT_FAILED` describes
            a broken push transport, and only that value may make a caller arm a
            cooldown. `cancel_key` captures the client-generated cancel key and
            answers one question only: may the device be ringing. Never derive the
            cause from the key -- that out-of-band inference is what this type
            removes. The UUID is generated locally *before*
            dispatch. It is returned (non-None) in two cases where the device may
            be ringing and a later Stop needs the key:
              1. Acceptance — the server answered 200, `outcome` is `ACCEPTED`.
              2. Post-dispatch ambiguity — a network failure occurred at or after
                 the request reached the wire (server disconnect, read timeout,
                 payload error), so the play may have started even though no 200
                 was read. `outcome` is not `ACCEPTED`, but the key is preserved.
            Every failure that provably never reached the wire — no FCM token,
            missing cache, username/payload/token resolution, connection setup
            (DNS/connect refused/connect timeout) — and every explicit server
            rejection (401/403/4xx/5xx/429) returns a null `cancel_key` so it cannot
            overwrite a still-valid cancel key of a previous, possibly still-ringing
            play. The sole exception is a rejection/HTTP-status exit whose retry
            sequence latched dispatch on an *earlier* wire-reaching attempt: there
            the key is preserved because that earlier attempt may already be
            ringing. The pre-/post-dispatch split is classified in-band on the
            raised NovaError (`NovaError.dispatched`), stamped uniformly at the
            transport's retry-loop choke point for every exit. See
            IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY. On the Stop side the key is not
            dropped at all (IRR-CA-STOP-KEEPS-CANCEL-KEY): neither our own key
            nor a foreign key passed in by a service call evicts our handle.

            `ACCEPTED` means Nova accepted the submission (HTTP 200). This is
            NOT a confirmation that the device received or executed the command: no
            ExecuteActionResponse schema exists and no FCM callback is
            registered for sound, so nothing on this path can observe the ring.
            See IRR-CA-NO-RING-CONFIRMATION in
            docs/PLAY_SOUND_ARCHITECTURE.md.
        """
        # Pass cache explicitly for multi-account isolation
        token = self._get_fcm_token_for_action()
        if not token:
            # PRE-dispatch: no transport was used at all, so there is no cancel key
            # to keep and neither the server nor the network may be blamed.
            # NOT_SENT keeps the caller from arming a push cooldown for a missing
            # local token.
            return PlaySoundResult(SoundDispatchOutcome.NOT_SENT)
        # Generate the cancel key locally *before* dispatch so it is in scope on
        # every path. Acceptance is still derived structurally from the submitter's
        # return contract: it returns a result tuple exclusively on an HTTP 200 and
        # re-raises on every non-acceptance. So `success` (the True/False bool) is
        # driven purely by "did the submitter return a tuple", with no out-of-band
        # dispatch signal to keep in sync. On the failure branch we additionally
        # preserve the key when the wrapped NovaError says the request reached the
        # wire (err.dispatched): the play may be ringing even without a 200, so a
        # later Stop needs the key. Explicit rejections (401/403/5xx) and provable
        # pre-dispatch failures keep dropping the key, so they can never overwrite a
        # previous, possibly still-ringing play's valid cancel key.
        request_uuid = generate_random_uuid()

        try:
            _LOGGER.info("Submitting Play Sound (async) for %s", device_id)
            # Delegate payload build + transport to the submitter; provide HA session.
            # NOTE: If Nova later requires an explicit username for action endpoints,
            # extend submitter signatures to accept and forward it consistently.
            result = await async_submit_start_sound_request(
                device_id,
                token,
                session=self._session,
                namespace=self._namespace(),
                cache=cast("TokenCache | None", self._cache),
                request_uuid=request_uuid,
            )
            if result is None:
                _LOGGER.error(
                    "Play Sound (async) submission failed for %s: "
                    "empty response from server (no error details available)",
                    device_id,
                )
                # Defensive: the submitter returns a tuple on every accepted (200)
                # command and re-raises otherwise, so None is not an expected
                # outcome. That is a broken contract on our side, not an outage:
                # INTERNAL_ERROR, never TRANSPORT_FAILED. Treat the unconfirmed
                # result conservatively — drop the key rather than risk overwriting
                # a previous play's valid one.
                return PlaySoundResult(SoundDispatchOutcome.INTERNAL_ERROR)

            response_hex, _response_uuid = result
            _LOGGER.info("Play Sound (async) submitted successfully for %s", device_id)
            # Byte count only: the body is never parsed
            # (docs/PLAY_SOUND_ARCHITECTURE.md) and raw API payloads are never
            # logged (AGENTS.md section 5).
            _LOGGER.debug(
                "Play Sound Nova response for %s (uuid=%s): %d bytes",
                device_id,
                request_uuid[:8] if request_uuid else "none",
                len(response_hex) // 2 if response_hex else 0,
            )
            return PlaySoundResult(
                SoundDispatchOutcome.ACCEPTED, cancel_key=request_uuid
            )

        except NovaAuthError as err:
            # The server answered and refused: never a transport failure, so an
            # expired sign-in is not hidden behind a self-clearing 90-second
            # cooldown. WHICH refusal it was is decided by the status, not by
            # the exception type; see _classify_nova_auth_error.
            outcome = _classify_nova_auth_error(err)
            if outcome is SoundDispatchOutcome.REJECTED_AUTH:
                _LOGGER.error(
                    "Authentication failed while playing sound on %s: %s",
                    device_id,
                    _short_err(err),
                )
            else:
                _LOGGER.warning(
                    "Client error (HTTP %s) while playing sound on %s: %s",
                    getattr(err, "status", "unknown"),
                    device_id,
                    _short_err(err),
                )
            # A raised error is never an acceptance (the submitter returns a tuple
            # only on a 200). Keep the cancel key only if an earlier attempt
            # latched dispatch (err.dispatched): a pure rejection with no
            # wire-reaching attempt drops it, so it can never overwrite a
            # previous, possibly still-ringing play's valid key.
            return _play_result_after_failure(err, request_uuid, outcome)

        except NovaHTTPError as err:
            # Non-acceptance (401/403/5xx/other): drop the key UNLESS an earlier
            # attempt latched dispatch — a prior attempt that reached the server
            # (a post-send network failure, or a 5xx/429 status read) may already
            # be ringing. err.dispatched carries that sticky sequence latch
            # (stamped at the transport's retry-loop choke point). Either way the
            # server answered, so the transport worked and the outcome names the
            # server, never the network.
            if getattr(err, "status", None) in (401, 403):
                _LOGGER.error(
                    "Authentication failed (HTTP %s) while playing sound on %s: %s",
                    err.status,
                    device_id,
                    _short_err(err),
                )
                return _play_result_after_failure(
                    err, request_uuid, SoundDispatchOutcome.REJECTED_AUTH
                )
            _LOGGER.warning(
                "Server error (%s) while playing sound on %s: %s",
                err.status,
                device_id,
                _short_err(err),
            )
            return _play_result_after_failure(
                err, request_uuid, SoundDispatchOutcome.REJECTED_SERVER
            )

        except NovaRateLimitError as err:
            _LOGGER.warning(
                "Play Sound rate-limited for %s: %s", device_id, _short_err(err)
            )
            # Same rule as the other non-acceptances: a rate-limited final attempt
            # never rang, but if a *prior* attempt reached the wire the latch
            # (err.dispatched) preserves the key. The server answered; only the
            # pace was wrong, so this is neither an outage nor a credential
            # problem and gets its own value.
            return _play_result_after_failure(
                err, request_uuid, SoundDispatchOutcome.REJECTED_RATE_LIMIT
            )

        except NovaProtobufDecodeError as err:
            # A 200 whose body we could not decode. The transport delivered; the
            # payload broke on our side of the contract, so it is our bug, not an
            # outage. Caught BEFORE `except NovaError` on purpose: that handler
            # would otherwise classify it as TRANSPORT_FAILED and arm a cooldown
            # for a decoding defect.
            _LOGGER.error(
                "Undecodable Play Sound response for %s: %s",
                device_id,
                _short_err(err),
                exc_info=True,
            )
            return _play_result_after_failure(
                err, request_uuid, SoundDispatchOutcome.INTERNAL_ERROR
            )

        except NovaLogicError as err:
            # HTTP 200 with an error code in the payload: the server answered and
            # refused (unknown device, permission denied at API level). Same
            # ordering rationale as above -- `except NovaError` would blame the
            # network for a decision the server made.
            _LOGGER.warning(
                "Play Sound refused by Nova for %s: %s", device_id, _short_err(err)
            )
            return _play_result_after_failure(
                err, request_uuid, SoundDispatchOutcome.REJECTED_SERVER
            )

        except NovaError as err:
            # A network failure wrapped by async_nova_request after its retries,
            # or any other NovaError leaving the transport. The retry-loop choke
            # point stamps the sticky dispatch latch onto it (err.dispatched): if
            # any attempt may have been processed by the server (a post-send
            # network failure, or a 5xx/429 status read), the play may already be
            # ringing, so keep the cancel key; a provable pre-connect failure
            # never rang, so drop it. The two subclasses that are NOT transport
            # failures (NovaLogicError, NovaProtobufDecodeError) are caught above,
            # so what reaches this handler really is the transport giving up.
            if err.dispatched:
                _LOGGER.warning(
                    "Play Sound for %s failed after reaching the server (%s); "
                    "keeping the cancel key in case the device is ringing.",
                    device_id,
                    _short_err(err),
                )
            else:
                _LOGGER.error(
                    "Network error while playing sound on %s before dispatch: %s",
                    device_id,
                    _short_err(err),
                )
            # The one outcome that justifies arming a push cooldown.
            return _play_result_after_failure(
                err, request_uuid, SoundDispatchOutcome.TRANSPORT_FAILED
            )

        except ClientError as err:
            # Defensive: async_nova_request wraps aiohttp errors into NovaError
            # (handled above), so a raw ClientError here is a pre-dispatch failure
            # raised before the POST loop (e.g. token/cache resolution). It never
            # reached the wire, so the device cannot be ringing: drop the key.
            _LOGGER.error(
                "Network error while playing sound on %s before dispatch: %s",
                device_id,
                _short_err(err),
            )
            return PlaySoundResult(SoundDispatchOutcome.TRANSPORT_FAILED)

        except Exception as err:
            # A bug on our side, not an outage. It gets a traceback and its own
            # class, because reporting it as a transport failure is what put the
            # integration into a self-inflicted 90-second cooldown.
            _LOGGER.error(
                "Failed to play sound (async) on %s: %s",
                device_id,
                _short_err(err),
                exc_info=True,
            )
            return PlaySoundResult(SoundDispatchOutcome.INTERNAL_ERROR)

    async def async_stop_sound(
        self, device_id: str, request_uuid: str | None = None
    ) -> SoundDispatchOutcome:
        """Send a 'Stop Sound' command to a device (async path for HA).

        Auth mapping note:
            A credential rejection here is logged and returned as REJECTED_AUTH
            (service call context), since re-auth is primarily driven by the
            coordinator’s data update path. "Credential rejection" means the
            STATUS says so (401/403) or the error is flagged permanent, not
            merely that the transport raised ``NovaAuthError`` -- that type also
            carries 400, 404 and the other non-retryable 4xx, which return
            REJECTED_SERVER. See ``_classify_nova_auth_error``.

        Args:
            device_id: The canonical ID of the device.
            request_uuid: Optional UUID of the Play Sound request to cancel.
                If omitted, the request is submitted WITHOUT a cancel key
                (the protobuf leaves ``requestUuid`` empty). Google then has
                nothing to correlate the stop with, so the ring may keep
                playing. The caller is responsible for surfacing that
                limitation; see StopSoundOutcome.UNCORRELATED.

        Returns:
            A `SoundDispatchOutcome` naming who refused, on the same contract as
            `async_play_sound`. Only `TRANSPORT_FAILED` describes a broken push
            transport and may make a caller arm a cooldown; a server rejection, a
            rate limit, a missing local token and a bug of our own each keep their
            own value.

            `ACCEPTED` means Nova accepted the submission (HTTP 200). It is NOT a
            confirmation that the device received or executed the command, and
            in particular not that the ring stopped: no ExecuteActionResponse
            schema exists and no FCM callback is registered for sound, so
            nothing on this path can observe the ring. See
            IRR-CA-NO-RING-CONFIRMATION in docs/PLAY_SOUND_ARCHITECTURE.md.
        """
        # Pass cache explicitly for multi-account isolation
        token = self._get_fcm_token_for_action()
        if not token:
            # PRE-dispatch: no transport was used, so neither the server nor the
            # network may be blamed for the missing local token.
            return SoundDispatchOutcome.NOT_SENT
        # Idempotent guard, not a second normalisation policy: the coordinator
        # funnel already maps blank to None. This entry point is public and
        # documented "for non-HA contexts", so a blank string can still arrive
        # here -- and a blank key is dropped from the proto3 payload entirely.
        # Without this, the log below would claim a cancel key that never
        # reaches the wire, which is the very class of unbacked success claim
        # this change set removes.
        request_uuid = (request_uuid or "").strip() or None
        try:
            if request_uuid:
                _LOGGER.info(
                    "Submitting Stop Sound (async) for %s (UUID: %s)",
                    device_id,
                    request_uuid[:8],
                )
            else:
                _LOGGER.warning(
                    "Submitting Stop Sound (async) for %s without a cancel key; "
                    "the server cannot correlate it with a running ring",
                    device_id,
                )
            result_hex = await async_submit_stop_sound_request(
                device_id,
                token,
                session=self._session,
                namespace=self._namespace(),
                cache=cast("TokenCache | None", self._cache),
                request_uuid=request_uuid,
            )
            # NOTE: a non-empty Nova reply means "the POST was accepted", not
            # "the device stopped". No ExecuteActionResponse schema exists, so
            # the body is never parsed. Deliberate architecture boundary, see
            # docs/PLAY_SOUND_ARCHITECTURE.md and IRR-CA-NO-RING-CONFIRMATION.
            submitted = result_hex is not None
            if submitted and request_uuid:
                _LOGGER.info(
                    "Stop Sound (async) submitted for %s (cancel key present)",
                    device_id,
                )
            elif submitted:
                _LOGGER.warning(
                    "Stop Sound (async) submitted for %s without a cancel key; "
                    "the server cannot correlate it with a running ring, so the "
                    "device may keep ringing",
                    device_id,
                )
            if submitted:
                # Byte count only: the body is never parsed (see NOTE above)
                # and raw API payloads are never logged (AGENTS.md section 5).
                _LOGGER.debug(
                    "Stop Sound Nova response for %s: %d bytes",
                    device_id,
                    len(result_hex) // 2 if result_hex else 0,
                )
            else:
                _LOGGER.error(
                    "Stop Sound (async) submission failed for %s: "
                    "empty response from server (no error details available)",
                    device_id,
                )
            # The submitter returns a hex body on acceptance and re-raises
            # otherwise, so an empty reply breaks its own contract: our bug, not
            # an outage.
            if submitted:
                return SoundDispatchOutcome.ACCEPTED
            return SoundDispatchOutcome.INTERNAL_ERROR

        except NovaAuthError as err:
            # The server answered and refused. The transport worked, so no
            # cooldown may be armed for this. Same rule as on the play path:
            # the status names the refusal, not the exception type.
            outcome = _classify_nova_auth_error(err)
            if outcome is SoundDispatchOutcome.REJECTED_AUTH:
                _LOGGER.error(
                    "Authentication failed while stopping sound on %s: %s",
                    device_id,
                    _short_err(err),
                )
            else:
                _LOGGER.warning(
                    "Client error (HTTP %s) while stopping sound on %s: %s",
                    getattr(err, "status", "unknown"),
                    device_id,
                    _short_err(err),
                )
            return outcome

        except NovaHTTPError as err:
            if getattr(err, "status", None) in (401, 403):
                _LOGGER.error(
                    "Authentication failed (HTTP %s) while stopping sound on %s: %s",
                    err.status,
                    device_id,
                    _short_err(err),
                )
                return SoundDispatchOutcome.REJECTED_AUTH
            _LOGGER.warning(
                "Server error (%s) while stopping sound on %s: %s",
                err.status,
                device_id,
                _short_err(err),
            )
            return SoundDispatchOutcome.REJECTED_SERVER

        except NovaRateLimitError as err:
            _LOGGER.warning(
                "Stop Sound rate-limited for %s: %s", device_id, _short_err(err)
            )
            # The server answered; only the pace was wrong.
            return SoundDispatchOutcome.REJECTED_RATE_LIMIT

        except NovaProtobufDecodeError as err:
            # Caught before `except NovaError` for the same reason as on the play
            # path: an undecodable body is our defect, not a dead network.
            _LOGGER.error(
                "Undecodable Stop Sound response for %s: %s",
                device_id,
                _short_err(err),
                exc_info=True,
            )
            return SoundDispatchOutcome.INTERNAL_ERROR

        except NovaLogicError as err:
            _LOGGER.warning(
                "Stop Sound refused by Nova for %s: %s", device_id, _short_err(err)
            )
            return SoundDispatchOutcome.REJECTED_SERVER

        except NovaError as err:
            # A network failure wrapped by async_nova_request after its retries,
            # or any other NovaError leaving the transport. Before the sound
            # contract existed this fell through to `except Exception` and was
            # reported as a plain False, indistinguishable from a server saying
            # no. It is the one outcome that justifies arming a push cooldown.
            _LOGGER.error(
                "Network error while stopping sound on %s: %s",
                device_id,
                _short_err(err),
            )
            return SoundDispatchOutcome.TRANSPORT_FAILED

        except ClientError as err:
            _LOGGER.error(
                "Network error while stopping sound on %s: %s",
                device_id,
                _short_err(err),
            )
            return SoundDispatchOutcome.TRANSPORT_FAILED

        except Exception as err:
            # Our own bug: traceback, own class, and never a cooldown.
            _LOGGER.error(
                "Failed to stop sound (async) on %s: %s",
                device_id,
                _short_err(err),
                exc_info=True,
            )
            return SoundDispatchOutcome.INTERNAL_ERROR


if (
    isinstance(_PREVIOUS_GOOGLEFINDMYAPI, type)
    and _PREVIOUS_GOOGLEFINDMYAPI is not GoogleFindMyAPI
):
    for _attr, _value in vars(GoogleFindMyAPI).items():
        if _attr in {"__dict__", "__weakref__", "__annotations__"}:
            continue
        setattr(_PREVIOUS_GOOGLEFINDMYAPI, _attr, _value)
    GoogleFindMyAPI = cast("type[GoogleFindMyAPI]", _PREVIOUS_GOOGLEFINDMYAPI)  # type: ignore[misc]
