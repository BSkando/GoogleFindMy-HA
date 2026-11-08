# custom_components/googlefindmy/SpotApi/GetEidInfoForE2eeDevices/get_owner_key.py
#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
"""Owner key retrieval and normalization for Google Find My Device.

This module provides an asynchronous API to obtain the per-user *owner key*
that is required to decrypt location payloads.

Multi-account design (entry-scoped):
- Callers **must** provide the entry-scoped TokenCache. All reads/writes are
  performed strictly against that cache; global facades have been removed.
- Keys are stored per user under ``owner_key_<username>``. A legacy non-scoped
  ``owner_key`` is migrated *within the same cache scope* (if present).
- Username resolution uses the same entry-scoped cache.

Normalization & validation:
- Owner keys are normalized to **hex strings** in storage.
- Decoded keys must be exactly 32 bytes (256 bit).
- Potentially blocking calls (network/crypto/CPU-bound) are offloaded to an executor.

Injection points (optional):
- ``eid_info_getter``: async callable that returns EID info (for tests/flows).
- ``shared_key_getter``: async callable that returns the shared key bytes.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import re
from binascii import Error as BinasciiError
from binascii import unhexlify
from collections.abc import Awaitable, Callable
from typing import Any, NamedTuple

from cryptography.exceptions import InvalidTag
from homeassistant.exceptions import ConfigEntryAuthFailed

from custom_components.googlefindmy._reauth_reason import ReauthReasonCode
from custom_components.googlefindmy.Auth.aas_token_retrieval import (
    _mask_email_for_logs,
)
from custom_components.googlefindmy.Auth.token_cache import TokenCache
from custom_components.googlefindmy.Auth.username_provider import (
    async_get_username,
    username_string,
)
from custom_components.googlefindmy.KeyBackup.cloud_key_decryptor import (
    decrypt_owner_key,
)
from custom_components.googlefindmy.KeyBackup.shared_key_retrieval import (
    async_get_shared_key,
)
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
    SpotApiEmptyResponseError,
    async_get_eid_info,
)
from custom_components.googlefindmy.SpotApi.spot_request import SpotAuthPermanentError
from custom_components.googlefindmy.typing_utils import (
    run_in_executor as _run_in_executor,
)

_LOGGER = logging.getLogger(__name__)

# Cache key base for owner keys. We migrate from legacy "owner_key" to per-user keys.
_OWNER_KEY_CACHE_PREFIX = "owner_key"
_OWNER_KEY_LENGTH = 32


class OwnerKeyInfo(NamedTuple):
    """Owner key material with version metadata."""

    key: bytes
    version: int | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _user_cache_key(username: str) -> str:
    """Return the per-user cache key for the owner key hex string."""
    return f"{_OWNER_KEY_CACHE_PREFIX}_{username}"


def _try_hex(s: str) -> bytes:
    """Try to decode a string as hexadecimal.

    Accepts optional "0x" prefix and ignores whitespace. Pads odd-length strings.
    Raises:
        BinasciiError: if the string contains non-hex characters.
    """
    t = (s or "").strip().lower()
    if t.startswith("0x"):
        t = t[2:]
    # Allow whitespace in user-pasted values
    t = re.sub(r"\s+", "", t)
    if not re.fullmatch(r"[0-9a-f]*", t):
        raise BinasciiError("String contains non-hexadecimal characters.")
    if len(t) % 2:
        t = "0" + t  # Prepend a zero to make it even-length
    return bytes(unhexlify(t))


def _try_base64_like(s: str) -> bytes:
    """Try to decode a string as base64/base64url/PEM-like content.

    - Removes PEM-style headers/footers.
    - Strips whitespace.
    - Adds missing padding for base64/base64url.

    Returns:
        bytes: decoded bytes
    Raises:
        ValueError | TypeError: if decoding fails for both urlsafe and standard base64.
    """
    v = re.sub(r"-{5}BEGIN[^-]+-{5}|-{5}END[^-]+-{5}", "", s or "")
    v = re.sub(r"\s+", "", v)
    pad = (-len(v)) % 4
    v_padded = v + ("=" * pad)
    try:
        decoded = base64.urlsafe_b64decode(v_padded)
    except (ValueError, TypeError):
        decoded = base64.b64decode(v_padded)
    return bytes(decoded)


# ---------------------------------------------------------------------------
# Core retrieval (entry-scoped when a TokenCache is supplied)
# ---------------------------------------------------------------------------


async def _retrieve_owner_key(
    username: str,
    *,
    cache: TokenCache,
    eid_info_getter: Callable[[], Awaitable[Any]] | None = None,
    shared_key_getter: Callable[[], Awaitable[bytes]] | None = None,
) -> dict[str, Any]:
    """Retrieve and decrypt the owner key for `username`, returning cache payload.

    Steps:
        1. Call SPOT GetEidInfoForE2eeDevices (may raise SpotApiEmptyResponseError).
        2. Obtain shared key (bytes).
        3. Decrypt the encrypted owner key to raw bytes.
        4. Return a mapping containing lowercase hex key string and owner key version.

    The retrieval of EID info and shared key can be provided via async callables;
    when omitted, synchronous helpers are executed in a thread pool.

    Raises:
        SpotApiEmptyResponseError: If SPOT returns an empty/trailers-only body.
        RuntimeError: If required fields/keys are missing or invalid.
    """
    if eid_info_getter is not None:
        eid_info = await eid_info_getter()
    else:
        try:
            eid_info = await async_get_eid_info(cache=cache)
        except SpotAuthPermanentError as exc:
            reauth_exc = ConfigEntryAuthFailed(
                "Owner key retrieval failed: Google session invalid; re-authentication required."
            )
            reauth_exc.reauth_code = ReauthReasonCode.OWNER_KEY_PERMANENT
            raise reauth_exc from exc
        except SpotApiEmptyResponseError:
            _LOGGER.error(
                "Owner key retrieval failed: SPOT returned an empty/trailers-only "
                "response for GetEidInfoForE2eeDevices; the exact gRPC status is "
                "logged for diagnosis."
            )
            raise

    if shared_key_getter is not None:
        shared_key: Any = await shared_key_getter()
    else:
        shared_key = await async_get_shared_key(cache=cache, username=username)

    if not isinstance(shared_key, (bytes, bytearray)) or not shared_key:
        raise RuntimeError("Shared key is missing or empty; cannot decrypt owner key")

    metadata = getattr(eid_info, "encryptedOwnerKeyAndMetadata", None)
    if metadata is None:
        # R9b: log WHICH eid_info fields the server populated (names only, never
        # values/bytes) so a partial response is diagnosable beyond "Missing X".
        _LOGGER.debug(
            "eid_info field shape on missing metadata: populated=%s",
            [
                name
                for name in ("encryptedOwnerKeyAndMetadata",)
                if getattr(eid_info, name, None)
            ],
        )
        raise RuntimeError("Missing 'encryptedOwnerKeyAndMetadata' in eid_info")

    encrypted_owner_key = getattr(metadata, "encryptedOwnerKey", b"")
    if (
        not isinstance(encrypted_owner_key, (bytes, bytearray))
        or len(encrypted_owner_key) == 0
    ):
        # R9b: list the metadata field NAMES the server populated (never values /
        # decrypted bytes) so the missing-owner-key shape is visible at DEBUG.
        _LOGGER.debug(
            "eid_info.encryptedOwnerKeyAndMetadata field shape on missing "
            "encryptedOwnerKey: populated=%s",
            [
                name
                for name in (
                    "encryptedOwnerKey",
                    "ownerKeyVersion",
                    "securityDomain",
                )
                if getattr(metadata, name, None)
            ],
        )
        raise RuntimeError(
            "Missing or empty 'encryptedOwnerKey' in eid_info.encryptedOwnerKeyAndMetadata"
        )

    try:
        owner_key: Any = await _run_in_executor(
            decrypt_owner_key, shared_key, encrypted_owner_key
        )
    except InvalidTag:
        # Propagate InvalidTag unchanged. The caller (decrypt_locations) maps it to
        # SharedKeyMismatchError to distinguish "shared key wrong/stale" from
        # "shared key missing" (RuntimeError). Wrapping it into RuntimeError here
        # would erase that discriminator and collapse both root causes into one
        # opaque message, which is exactly what hid the real cause from users.
        raise
    owner_key_version = getattr(metadata, "ownerKeyVersion", None)

    if not isinstance(owner_key, (bytes, bytearray)) or len(owner_key) == 0:
        # R9b: log the decrypted owner-key shape (type + length only, NEVER the
        # bytes/value) so an empty/short result is diagnosable.
        _LOGGER.debug(
            "Decrypted owner_key shape on empty/invalid result: type=%s, len=%s",
            type(owner_key).__name__,
            len(owner_key) if isinstance(owner_key, (bytes, bytearray)) else "n/a",
        )
        raise RuntimeError("Decrypted owner_key is empty or invalid type")

    _LOGGER.info(
        "Retrieved owner key (version=%s, len=%s) for user=%s",
        owner_key_version,
        len(owner_key),
        _mask_email_for_logs(username),
    )

    return {
        "key": bytes(owner_key).hex(),
        "version": owner_key_version if isinstance(owner_key_version, int) else None,
    }


async def _get_or_generate_user_owner_key_hex(
    username: str,
    *,
    cache: TokenCache,
    eid_info_getter: Callable[[], Awaitable[Any]] | None = None,
    shared_key_getter: Callable[[], Awaitable[bytes]] | None = None,
    force_refresh: bool = False,
) -> Any:
    """Return the per-user owner key payload, migrating and generating if needed.

    Cache behavior:
        - All operations are performed against the provided entry-scoped cache.
        - Legacy `owner_key` (non-scoped) is migrated *within the same cache scope*.
        - Cached values may be a string (legacy) or mapping with ``{"key": str, "version": int | None}``.
        - When ``force_refresh`` is True, skip cached values and fetch a fresh key from the server.
    """
    user_key_name = _user_cache_key(username)

    # 1) Per-user cache hit?
    if cache is None:
        raise ValueError("TokenCache instance is required for multi-account safety.")

    # Skip cache lookups entirely when a forced refresh is requested.
    if force_refresh:
        fresh = await _retrieve_owner_key(
            username,
            cache=cache,
            eid_info_getter=eid_info_getter,
            shared_key_getter=shared_key_getter,
        )
        await cache.set(user_key_name, fresh)
        return fresh

    user_key = await cache.get(user_key_name)
    if isinstance(user_key, dict) and user_key.get("key"):
        return user_key

    if isinstance(user_key, str) and user_key:
        return user_key

    # 2) Legacy migration (within the same cache scope only)
    legacy = await cache.get(_OWNER_KEY_CACHE_PREFIX)
    if isinstance(legacy, dict) and legacy.get("key"):
        await cache.set(user_key_name, legacy)
        _LOGGER.debug(
            "Migrated legacy 'owner_key' (entry-scoped) to user-scoped cache for %s",
            _mask_email_for_logs(username),
        )
        return legacy

    if isinstance(legacy, str) and legacy:
        await cache.set(user_key_name, legacy)
        _LOGGER.debug(
            "Migrated legacy 'owner_key' (entry-scoped) to user-scoped cache for %s",
            _mask_email_for_logs(username),
        )
        return legacy

    # 3) Generate fresh value and cache under the user-specific key.
    return await cache.get_or_set(
        user_key_name,
        lambda: _retrieve_owner_key(
            username,
            cache=cache,
            eid_info_getter=eid_info_getter,
            shared_key_getter=shared_key_getter,
        ),
    )


# ---------------------------------------------------------------------------
# Public API (async-first, entry-scoped capable)
# ---------------------------------------------------------------------------


async def async_get_owner_key(  # noqa: PLR0912,PLR0915
    *,
    cache: TokenCache,
    username: str | None = None,
    eid_info_getter: Callable[[], Awaitable[Any]] | None = None,
    shared_key_getter: Callable[[], Awaitable[bytes]] | None = None,
    force_refresh: bool = False,
) -> OwnerKeyInfo:
    """Return the binary owner key (32 bytes) for the current user (entry-scoped capable).

    Behavior:
        - Uses a per-user cache key and migrates from legacy `owner_key` if found
          within the same cache scope.
        - Accepts and normalizes the cached key to a hex string.
        - Can decode base64/base64url/PEM-like content once and self-heal to hex.
        - Enforces a strict 32-byte key length.
        - When ``force_refresh`` is True, bypasses cached values and retrieves the owner key directly.
        - If `cache` is provided, username resolution will prefer the same cache.

    Returns:
        OwnerKeyInfo: the owner key (exactly 32 bytes) and version metadata if available.

    Raises:
        RuntimeError: if the key is missing/invalid or has incorrect length.
    """
    if cache is None:
        raise ValueError("TokenCache instance is required for multi-account safety.")

    # Resolve username (prefer entry-scoped cache).
    user: str | None
    if username is not None:
        user = username if isinstance(username, str) and username else None
    else:
        cached_username = await cache.get(username_string)
        user = (
            cached_username
            if isinstance(cached_username, str) and cached_username
            else None
        )
        if user is None:
            resolved_username = await async_get_username(cache=cache)
            user = (
                resolved_username
                if isinstance(resolved_username, str) and resolved_username
                else None
            )

    if user is None:
        raise RuntimeError("Username is not configured; cannot retrieve owner key.")

    try:
        raw_value = await _get_or_generate_user_owner_key_hex(
            user,
            cache=cache,
            eid_info_getter=eid_info_getter,
            shared_key_getter=shared_key_getter,
            force_refresh=force_refresh,
        )
    except SpotApiEmptyResponseError as exc:
        # E3-W: ground the wording in the observed state only; the
        # ConfigEntryAuthFailed type (and thus the reauth behavior) is kept
        # UNCHANGED deliberately, because whether an empty/trailers-only response
        # is transient or auth is not yet proven. The captured gRPC status (R9a)
        # makes the next real incident diagnosable before any reclassification.
        reauth_exc = ConfigEntryAuthFailed(
            "Owner key retrieval failed: SPOT returned an empty/trailers-only "
            "response; the exact gRPC status is logged for diagnosis."
        )
        reauth_exc.reauth_code = ReauthReasonCode.OWNER_KEY_EMPTY_RESPONSE
        raise reauth_exc from exc

    cache_version: int | None = None
    cache_key_value: str | None = None
    if isinstance(raw_value, dict):
        version_value = raw_value.get("version")
        if isinstance(version_value, int):
            cache_version = version_value
        elif isinstance(version_value, str) and version_value.isdigit():
            cache_version = int(version_value)

        key_candidate = raw_value.get("key")
        cache_key_value = key_candidate if isinstance(key_candidate, str) else None
    elif isinstance(raw_value, str):
        cache_key_value = raw_value

    if cache_key_value is None:
        await cache.set(_user_cache_key(user), None)
        await cache.set(_OWNER_KEY_CACHE_PREFIX, None)
        raise RuntimeError("Owner key is missing or invalid; please re-authenticate.")

    decoded_from_non_hex = False
    key_bytes: bytes
    try:
        key_bytes = _try_hex(cache_key_value)
    except (BinasciiError, TypeError):
        # 2) Fallback: try base64/base64url/PEM-like
        try:
            key_bytes = _try_base64_like(cache_key_value)
            decoded_from_non_hex = True
        except Exception as exc:
            _LOGGER.error(
                "Owner key for user '%s' is not valid hex or base64/base64url. "
                "Please store the key as a 64-char hex string (32 bytes). Error: %s",
                user,
                exc,
            )
            # Clear per-user & legacy cache to prevent repeated failures on the same invalid data
            await cache.set(_user_cache_key(user), None)
            await cache.set(_OWNER_KEY_CACHE_PREFIX, None)
            raise RuntimeError(
                "Invalid owner_key format (expect 32-byte key in hex or base64)."
            ) from exc
    normalized_value = {"key": key_bytes.hex(), "version": cache_version}
    if decoded_from_non_hex:
        # Self-heal: normalize the cache to hex for future consistency
        _LOGGER.info(
            "Successfully decoded owner key from a non-hex format; normalizing cache to hex."
        )

    if decoded_from_non_hex or not isinstance(raw_value, dict):
        await cache.set(_user_cache_key(user), normalized_value)

    # 3) Final validation: the owner key must be exactly 32 bytes long
    if len(key_bytes) != _OWNER_KEY_LENGTH:
        _LOGGER.error(
            "Owner key for user '%s' has an invalid length: %d bytes (expected 32). "
            "Clear credentials and re-authenticate if this persists.",
            user,
            len(key_bytes),
        )
        await cache.set(_user_cache_key(user), None)
        await cache.set(_OWNER_KEY_CACHE_PREFIX, None)
        raise RuntimeError("Owner key must be exactly 32 bytes long.")

    return OwnerKeyInfo(key=key_bytes, version=cache_version)


# ---------------------------------------------------------------------------
# Legacy sync facade (CLI/offline only)
# ---------------------------------------------------------------------------


def get_owner_key(*, cache: TokenCache) -> OwnerKeyInfo:
    """Synchronous facade for CLI/offline usage; not allowed in the HA event loop.

    Raises:
        RuntimeError: If called from within a running event loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(async_get_owner_key(cache=cache))
    raise RuntimeError(
        "Sync get_owner_key() called from the event loop. "
        "Use `await async_get_owner_key()` instead."
    )
