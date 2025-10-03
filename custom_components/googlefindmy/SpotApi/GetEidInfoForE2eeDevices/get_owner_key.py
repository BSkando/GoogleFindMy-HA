#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
from binascii import unhexlify, Error as BinasciiError
import logging

from custom_components.googlefindmy.Auth.token_cache import (
    get_cached_value_or_set,
    get_cached_value,
    set_cached_value,
)
from custom_components.googlefindmy.Auth.username_provider import get_username
from custom_components.googlefindmy.KeyBackup.cloud_key_decryptor import decrypt_owner_key
from custom_components.googlefindmy.KeyBackup.shared_key_retrieval import get_shared_key
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
    get_eid_info,
    SpotApiEmptyResponseError,
)

_LOGGER = logging.getLogger(__name__)

# Cache key base for owner keys. We migrate from legacy "owner_key" to per-user keys.
_OWNER_KEY_CACHE_PREFIX = "owner_key"


def _user_cache_key(username: str) -> str:
    """Compute per-user cache key for the owner key."""
    return f"{_OWNER_KEY_CACHE_PREFIX}_{username}"


def _retrieve_owner_key(username: str) -> str:
    """
    Retrieve and decrypt the owner key for the given account, and return it as hex string.

    Steps:
    - Call GetEidInfoForE2eeDevices (may raise SpotApiEmptyResponseError if trailers-only/empty).
    - Fetch shared_key (must be non-empty bytes).
    - Decrypt encrypted owner key.
    - Return hex string; do not cache here (caller manages cache).
    """
    try:
        eid_info = get_eid_info()
    except SpotApiEmptyResponseError as exc:
        # Clear, actionable message; actual token invalidation/retry happens in spot_request.
        _LOGGER.error(
            "Owner key retrieval failed: SPOT returned empty/trailers-only body "
            "for GetEidInfoForE2eeDevices (likely auth/session issue). Please re-authenticate."
        )
        raise

    shared_key = get_shared_key()
    if not isinstance(shared_key, (bytes, bytearray)) or not shared_key:
        raise RuntimeError("Shared key is missing or empty; cannot decrypt owner key")

    # Guards for presence and non-empty encrypted owner key
    metadata = getattr(eid_info, "encryptedOwnerKeyAndMetadata", None)
    if metadata is None:
        raise RuntimeError("Missing 'encryptedOwnerKeyAndMetadata' in eid_info")

    encrypted_owner_key = getattr(metadata, "encryptedOwnerKey", b"")
    if not isinstance(encrypted_owner_key, (bytes, bytearray)) or len(encrypted_owner_key) == 0:
        raise RuntimeError("Missing or empty 'encryptedOwnerKey' in eid_info.encryptedOwnerKeyAndMetadata")

    owner_key = decrypt_owner_key(shared_key, encrypted_owner_key)
    owner_key_version = getattr(metadata, "ownerKeyVersion", None)

    if not isinstance(owner_key, (bytes, bytearray)) or len(owner_key) == 0:
        raise RuntimeError("Decrypted owner_key is empty or invalid type")

    _LOGGER.info("Retrieved owner key (version=%s, len=%s) for user=%s",
                 owner_key_version, len(owner_key), username)

    return owner_key.hex()


def _get_or_generate_user_owner_key_hex(username: str) -> str:
    """
    Get user-scoped owner key from cache; migrate from legacy key if present;
    otherwise generate and cache.
    """
    user_key = get_cached_value(_user_cache_key(username))
    if user_key:
        return user_key

    # Legacy migration path: move global 'owner_key' to user-scoped cache if present.
    legacy = get_cached_value(_OWNER_KEY_CACHE_PREFIX)
    if legacy:
        set_cached_value(_user_cache_key(username), legacy)
        _LOGGER.debug("Migrated legacy 'owner_key' to user-scoped cache for %s", username)
        return legacy

    # Generate fresh value and cache under the user-specific key.
    return get_cached_value_or_set(_user_cache_key(username), lambda: _retrieve_owner_key(username))


def get_owner_key() -> bytes:
    """
    Public entry: returns the binary owner key (bytes) for the current user.
    - Uses per-user cache key, with migration from legacy 'owner_key'.
    - On corrupt hex in cache: logs and clears cache, then re-raises to let upper layers retry.
    """
    username = get_username()
    value = _get_or_generate_user_owner_key_hex(username)
    try:
        return unhexlify(value)
    except BinasciiError as exc:
        _LOGGER.error("Invalid hex value for owner key in cache (user=%s). Clearing cached entry.", username)
        # Clear both user-scoped and legacy entries to force regeneration next call.
        set_cached_value(_user_cache_key(username), None)
        set_cached_value(_OWNER_KEY_CACHE_PREFIX, None)
        raise RuntimeError("Invalid hex value for 'owner_key' from cache") from exc


if __name__ == '__main__':
    print(get_owner_key())
