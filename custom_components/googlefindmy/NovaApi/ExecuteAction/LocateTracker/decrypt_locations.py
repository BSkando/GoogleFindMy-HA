# custom_components/googlefindmy/NovaApi/ExecuteAction/LocateTracker/decrypt_locations.py
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
from __future__ import annotations

# ruff: noqa: PLR0911, PLR0912, PLR0915
import asyncio
import datetime
import hashlib
import logging
import math
import time
from collections.abc import Sequence
from importlib import import_module
from itertools import zip_longest
from typing import TYPE_CHECKING, Any, cast

from cryptography.exceptions import InvalidTag

from custom_components.googlefindmy import get_proto_decoder
from custom_components.googlefindmy.Auth.adm_token_retrieval import (
    is_non_retryable_auth_kind,
)
from custom_components.googlefindmy.Auth.username_provider import username_string
from custom_components.googlefindmy.const import MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S
from custom_components.googlefindmy.FMDNCrypto._lazy_crypto import get_aesgcm_class
from custom_components.googlefindmy.FMDNCrypto.curve_profile import SECP160R1, SECP256R1
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    EIK_LENGTH,
    MEASURED_COMPONENT_ROWS,
    derive_component_key,
)
from custom_components.googlefindmy.FMDNCrypto.foreign_report_errors import (
    ForeignReportAuthError,
    ForeignReportStructureError,
    UnsupportedCurveError,
)
from custom_components.googlefindmy.FMDNCrypto.foreign_tracker_cryptor import (
    ForeignDecryptResult,
    decrypt_foreign_report,
)
from custom_components.googlefindmy.FMDNCrypto.mcu_utils import (
    flip_bits,
    is_mcu_tracker,
)
from custom_components.googlefindmy.KeyBackup.cloud_key_decryptor import (
    EIK_GCM_TOTAL_LEN,
    decrypt_aes_gcm,
    decrypt_eik,
)
from custom_components.googlefindmy.KeyBackup.shared_key_retrieval import (
    SharedKeyUnavailableError,
    async_get_shared_key,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypted_location import (
    WrappedLocation,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.foreign_reading_tracker import (
    FOREIGN_READING_TRACKER,
    DeviceKey,
    foreign_device_key,
)
from custom_components.googlefindmy.ProtoDecoders.decoder import (
    parse_device_update_protobuf,
    trackable_components_of,
)
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
    SpotApiEmptyResponseError,
    async_get_eid_info,
)
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_owner_key import (
    OwnerKeyInfo,
    async_get_owner_key,
)
from custom_components.googlefindmy.SpotApi.spot_request import SpotError
from google.protobuf.message import DecodeError

if TYPE_CHECKING:
    from custom_components.googlefindmy.Auth.token_cache import TokenCache
    from custom_components.googlefindmy.ProtoDecoders.DeviceUpdate_pb2 import (
        DeviceRegistration as DeviceRegistrationMessage,
    )
    from custom_components.googlefindmy.ProtoDecoders.DeviceUpdate_pb2 import (
        DeviceUpdate as DeviceUpdateMessage,
    )
else:
    DeviceRegistrationMessage = Any
    DeviceUpdateMessage = Any

DeviceRegistration = DeviceRegistrationMessage
DeviceUpdateProto = DeviceUpdateMessage

_LAT_MIN = -90.0
_LAT_MAX = 90.0
_LON_MIN = -180.0
_LON_MAX = 180.0
_MICROSECONDS_THRESHOLD = 1e15
_MILLISECONDS_THRESHOLD = 1e12
_MIN_VALID_EPOCH_S = 946684800.0  # 2000-01-01

# Acceptable future drift for timestamps to accommodate timezone offsets and
# clock skew while still rejecting obviously invalid data is centralized in
# MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S (const.py) so downstream components stay
# aligned on the same acceptance window.
_LOGGER = logging.getLogger(__name__)
# Soft limit to avoid pathological payloads; large batches are unusual and heavy.
_MAX_REPORTS: int = 500

# Strict length of Ephemeral Identity Key (bytes). Paper and ecosystem practice expect 32 bytes.
_EIK_LEN: int = 32
# Heuristic threshold suggesting encryptedUserSecrets holds structured data rather than a raw key blob.
# Moto Tag payloads can legitimately exceed the smaller legacy cutoff, so tolerate larger blobs before
# raising a diagnostic warning.
_STRUCTURED_PAYLOAD_LEN_THRESHOLD: int = 256

# -------------------------------------------------------------------------
# EIK Cache (Performance Optimization)
# -------------------------------------------------------------------------
# Cache decrypted **FMDN Ephemeral Identity Keys (EIK)** to avoid expensive
# AES-GCM operations on every location request.
#
# IMPORTANT TERMINOLOGY:
# - EIK (Ephemeral Identity Key) = FMDN master secret for location decryption
#   and owner-specific operations (recovery_key, ringing_key, tracking_key are
#   derived from this). This is what we cache here.
# - IRK (Identity Resolving Key) = BLE-specific key for resolving resolvable
#   private addresses (Bluetooth Core Spec). NOT cached here and NOT used by
#   this integration.
#
# The EIK never changes unless the device is re-paired or the owner key
# version is rotated.
#
# Cache key: SHA-256 hash of (encrypted_identity_key + owner_key_version + flip_state)
# Cache value: Decrypted EIK bytes (32 bytes)
#
# Thread-safety: Access occurs only on the HA event loop (single-threaded),
# so no explicit locking is required.
# -------------------------------------------------------------------------
_eik_cache: dict[str, bytes] = {}
_eik_cache_stats = {"hits": 0, "misses": 0}


def _get_eik_cache_key(
    encrypted_identity_key: bytes, owner_key_version: int, flip_state: bool
) -> str:
    """Generate a stable cache key for the EIK.

    Args:
        encrypted_identity_key: The encrypted EIK blob.
        owner_key_version: The owner key version from device registration.
        flip_state: Whether the bit-flip quirk is applied.

    Returns:
        A hex string cache key (SHA-256 hash).
    """
    # Combine encrypted key, version, and flip state to ensure cache invalidation
    # on key rotation or quirk detection changes
    combined = (
        encrypted_identity_key
        + owner_key_version.to_bytes(4, "big")
        + (b"\x01" if flip_state else b"\x00")
    )
    return hashlib.sha256(combined).hexdigest()


def clear_eik_cache() -> None:
    """Clear the entire EIK cache (e.g., on integration reload or E2EE reset)."""
    _eik_cache.clear()
    _LOGGER.debug("EIK cache cleared (all entries)")


def invalidate_eik_cache_for_key(
    encrypted_identity_key: bytes, owner_key_version: int
) -> int:
    """Invalidate all cached EIK entries for a specific encrypted identity key.

    Called when every key-relevant encrypted report in a poll cycle fails
    MAC/InvalidTag authentication (key-neutral foreign reports do not count), which suggests the cached decrypted EIK is stale (e.g., after
    key rotation, re-pairing, or E2EE reset). Whether the failure is actually
    persistent is decided by the stateful callers (coordinator poll verdict / FCM
    callback), not here: this helper only drops the cache so the next poll
    re-derives the key.

    Returns:
        The number of entries removed.
    """
    removed = 0
    for do_flip in (False, True):
        flipped_blob = flip_bits(encrypted_identity_key, do_flip)
        cache_key = _get_eik_cache_key(flipped_blob, owner_key_version, do_flip)
        if cache_key in _eik_cache:
            del _eik_cache[cache_key]
            removed += 1
    if removed:
        _LOGGER.info(
            "Invalidated %d EIK cache entries (version=%s) after authentication "
            "failures this cycle; the next poll re-derives the key",
            removed,
            owner_key_version,
        )
    return removed


def get_eik_cache_stats() -> dict[str, int]:
    """Return current cache statistics (for diagnostics)."""
    return {
        "hits": _eik_cache_stats["hits"],
        "misses": _eik_cache_stats["misses"],
        "size": len(_eik_cache),
    }


# -------------------------------------------------------------------------
# AAD Envelope Unwrap (matches eid_resolver pattern)
# -------------------------------------------------------------------------
_AESGCM_NONCE_LEN = 12


def _try_unwrap_aesgcm_envelope(
    envelope: bytes, wrapping_key: bytes, device_id: str | None
) -> bytes | None:
    """Try AES-GCM envelope unwrap with AAD (device_id as registry_id).

    Some trackers use an AES-GCM envelope format where the encrypted identity
    key includes Additional Authenticated Data (AAD). The standard decrypt_eik
    (without AAD) fails with InvalidTag for these. This mirrors the fallback in
    eid_resolver._unwrap_aesgcm_envelope.
    """
    if device_id is None or len(envelope) <= _AESGCM_NONCE_LEN:
        return None
    try:
        AESGCM = get_aesgcm_class()
        nonce = envelope[:_AESGCM_NONCE_LEN]
        ciphertext = envelope[_AESGCM_NONCE_LEN:]
        return AESGCM(wrapping_key).decrypt(nonce, ciphertext, device_id.encode())
    except Exception:  # noqa: BLE001
        return None


def _extract_canonic_id(device_update_protobuf: Any) -> str | None:
    """Extract the first canonical ID from a DeviceUpdate protobuf."""
    try:
        canonic_ids = device_update_protobuf.deviceMetadata.identifierInformation.canonicIds.canonicId
        for cid in canonic_ids:
            val = getattr(cid, "id", None)
            if isinstance(val, str) and val:
                return val
    except Exception:  # noqa: BLE001
        pass
    return None


def _get_common_pb2() -> Any:
    """Return the Common_pb2 module lazily."""

    return get_proto_decoder("Common_pb2")


def _get_device_update_pb2() -> Any:
    """Return the DeviceUpdate_pb2 module lazily."""

    return get_proto_decoder("DeviceUpdate_pb2")


# ---- Exceptions (specific, compatible via RuntimeError) -----------------------
class DecryptionError(RuntimeError):
    """Raised when decryption fails for reasons other than stale owner key."""


class StaleOwnerKeyError(DecryptionError):
    """Raised when the tracker was encrypted with an older owner key version.

    Per-tracker condition: a single tracker still encrypts reports with an
    owner-key version that is no longer current. An account-wide re-auth does NOT
    fix it; the tracker must be removed and re-paired. Kept distinct from the
    account-wide shared-key failures below so the coordinator can escalate each
    differently (per-tracker repair issue vs. ConfigEntryAuthFailed).
    """


class SharedKeyMismatchError(DecryptionError):
    """Raised when the shared key cannot decrypt the owner key (AES-GCM InvalidTag).

    Account-wide: the bundle's shared key is stale or incompatible with the
    server-side owner key. Only a fresh secrets.json obtained via the interactive
    browser flow can fix this; force_refresh of the owner key cannot, because the
    shared key itself is the wrong one.
    """


class SharedKeyMissingError(DecryptionError):
    """Raised when the shared key is absent or empty (incomplete bundle import).

    Account-wide: the secrets.json did not provide a usable shared key. A complete
    re-import / fresh secrets.json is required.
    """


class OwnerKeyInvalidError(DecryptionError):
    """Raised when the LOCALLY stored owner key is structurally invalid.

    Account-wide credential defect, NOT a transient miss: the owner key was
    obtained from the local cache/credentials but is missing, malformed (not a
    valid hex/base64 key), or has the wrong length. Retrying can never repair such
    a deterministic local defect, so it must surface as a reauth-worthy
    ``DecryptionError`` (a fresh secrets.json is required), exactly like the
    shared-key failures above.

    Kept a DISTINCT ``DecryptionError`` subclass (NOT ``OwnerKeyLookupTransientError``)
    so the failure class name is diagnosable in logs and the coordinator routes it
    to the account-wide reauth path instead of swallowing it as a transient lookup
    miss. Contrast ``OwnerKeyLookupTransientError`` (base ``Exception``), which is
    reserved for partial/trailers-only server responses, transport failures and
    otherwise unclassified misses where the credentials are presumed valid.
    """


class OwnReportIdentityMismatchError(DecryptionError):
    """Raised when a device's OWN server reports all fail authentication.

    Device-local condition: the account keys are healthy (the identity key was
    derived and siblings decrypt), but THIS device's own reports on the server were
    encrypted with an identity key the local cache no longer matches -- typically a
    phone powered off for days whose server-side reports are stale, or a re-paired
    device. A fresh secrets.json (account-wide re-auth) does NOT fix it, so when a
    sibling proves the shared key is healthy the coordinator downgrades this to a
    per-device warning instead of prompting for re-authentication.

    Kept a DISTINCT subclass so the coordinator's sibling-success downgrade applies
    ONLY here. Every OTHER ``DecryptionError`` -- identity-key derivation failures
    (account-wide owner/shared key, e.g. "Identity key decryption failed.") and the
    explicit shared-key subclasses -- stays on the account-wide reauth path, so a
    new raise-site fails safe (escalates) rather than being silently suppressed.
    """


class OwnerKeyLookupTransientError(Exception):
    """Raised when the owner-key lookup did not complete for a transient reason.

    Base class is ``Exception`` deliberately, NOT ``DecryptionError`` and NOT
    ``RuntimeError``: a partial/trailers-only server response, a network/gRPC
    transport failure or any otherwise unclassified owner-key retrieval miss is a
    TRANSIENT condition, not a credential defect. Because it is not a
    ``DecryptionError`` it is never caught by the coordinator's
    ``except DecryptionError`` blocks and never reaches the account-wide reauth
    verdict (Option B); because it is not a ``RuntimeError`` it is not swallowed by
    a broad ``except RuntimeError`` (e.g. ``api.py``). It carries NO "stale" /
    "re-authenticate" claim: the credentials are presumed valid and the right
    recovery is an ordinary retry/skip, never an account re-authentication.
    """


class _OwnerKeyRederiveRequired(OwnerKeyLookupTransientError):
    """Internal signal that a locally structurally-defective owner key should
    trigger a single-shot ``force_refresh`` re-derive.

    A LOCAL owner-key STRUCTURE defect (the stored owner key is missing/invalid,
    malformed, or the wrong length) is re-derivable from the (valid) shared key,
    so the caller should re-derive once via ``async_get_owner_key(force_refresh=
    True)`` BEFORE deciding anything -- never escalate the first defect straight
    to reauth. Subclasses the transient base ``OwnerKeyLookupTransientError``
    deliberately: should this internal signal ever leak past the caller's
    orchestration, it fails safe as a benign transient miss (skipped by the
    poll/locate paths, never caught by ``except DecryptionError``, never reaching
    the account-wide reauth verdict). It is NOT a ``DecryptionError`` and makes no
    "re-authenticate" claim.
    """


# Single-shot WARNING dedup for the R4 "persistent structure defect after a
# successful re-derive" notice. Keyed on the entry-scope identity (here ``id(
# cache)`` -- the closest available per-entry handle in this function; this keys
# per cache OBJECT, so the dedup only suppresses cross-poll spam, not distinct
# entries). The R4 contract asserts exactly one WARNING per key.
_warned_rederive_persistent_defect: set[int] = set()


def _classify_owner_key_failure(exc: Exception, *, context: str) -> Exception:
    """Map a low-level owner-key lookup failure to a specific error class.

    ``async_get_owner_key`` collapses distinct root causes into either an
    ``InvalidTag`` (wrong/stale shared key) or a ``RuntimeError`` (missing/empty
    shared key, a partial server-field response, or a generic failure). This helper
    restores the discriminator as a typed class so the failure class name appears
    in logs and the coordinator can pick the correct recovery path. The caller
    preserves the original error via ``raise ... from exc``.

    Deterministic, field-specific matching (first match wins):
      (auth) a non-retryable structured ``error_kind`` (badauthentication /
          auth_error / invalid_grant) -> ``OwnerKeyInvalidError`` (reauth-worthy
          ``DecryptionError``). This is the ONLY reauth-worthy branch and is keyed
          on the structured ``error_kind`` attribute, NEVER on message substrings,
          so a transient text that merely contains "401" (e.g. "retry after 4012
          ms") stays transient. See AP3.
      (a) ``InvalidTag`` -> ``SharedKeyMismatchError`` (genuine credential defect).
      (b) the shared key is genuinely absent/empty -> ``SharedKeyMissingError``
          (genuine credential defect). Recognised primarily by the TYPED
          ``SharedKeyUnavailableError`` raised at the retrieval source (robust to
          message wording, e.g. the non-interactive "shared key not available"
          case), and as a fallback by the legacy local "shared key ... missing or
          empty" message of the defensive empty-return guard in
          ``get_owner_key``.
      (b2) a local owner-key STRUCTURE defect (the stored owner key is missing,
          malformed, or the wrong length) -> ``_OwnerKeyRederiveRequired`` (an
          INTERNAL re-derive signal, NOT reauth). The defect is re-derivable from
          the (valid) shared key, so the caller force-refreshes the owner key once
          before deciding anything. Fail-safe transient subclass; never reauth.
      (c) a partial server-field response (``encryptedOwnerKeyAndMetadata`` /
          ``encryptedOwnerKey`` / "Decrypted owner_key is empty") ->
          ``OwnerKeyLookupTransientError`` (transient server condition).
      (d) everything else (default, inverted) -> ``OwnerKeyLookupTransientError``
          (no "stale" / "re-authenticate" guess).

    Order / reachability: the (auth) ``error_kind`` check runs first and is the
    only path that yields a reauth-worthy ``OwnerKeyInvalidError``. It is
    reachable via a bare ``RuntimeError`` carrying ``error_kind`` propagated from
    the ADM/owner-key getters; ``error_kind``-tagged errors carry none of the
    (a)-(c) wordings, so placing it first does not steal those branches.
    ``InvalidAasTokenError`` does NOT reach this branch (the lower layers
    pre-convert it). The (b2) anchor is distinct from the (c) server-field tokens,
    so a deterministic local structure defect is routed to the re-derive signal
    before the transient fallthrough can hide it.

    Scope of (b2): it covers only STRUCTURAL defects of an owner key that was
    already obtained (missing/invalid value after retrieval, malformed encoding,
    wrong length). Precondition failures raised BEFORE retrieval -- e.g.
    ``get_owner_key.py``'s "Username is not configured" -- deliberately stay on the
    transient default (d): an unset username can be a cold-start race (the username
    cache is not populated yet), and escalating it to reauth would reintroduce the
    cry-wolf prompt this classifier exists to avoid. They are NOT structural
    owner-key defects, so they are intentionally not folded into (b2).
    """
    if is_non_retryable_auth_kind(exc):
        return OwnerKeyInvalidError(
            f"Owner key lookup hit a non-retryable auth failure during {context} "
            "(structured error_kind). The stored credentials are rejected; "
            "re-authentication is required."
        )
    if isinstance(exc, InvalidTag):
        return SharedKeyMismatchError(
            f"Owner key decryption failed (InvalidTag) during {context}: the shared "
            "key is stale or incompatible with this account. A fresh secrets.json "
            "is required (re-authentication)."
        )
    text = str(exc).lower()
    if isinstance(exc, SharedKeyUnavailableError) or (
        "shared key" in text and "missing or empty" in text
    ):
        return SharedKeyMissingError(
            f"Shared key is missing or empty during {context} (incomplete bundle). "
            "Re-import a complete secrets.json."
        )
    if (
        "owner key is missing or invalid" in text
        or "invalid owner_key format" in text
        or "owner key must be exactly" in text
    ):
        return _OwnerKeyRederiveRequired(
            f"The locally stored owner key is structurally defective during "
            f"{context} (missing, malformed, or wrong length). It is re-derivable "
            "from the shared key; a single-shot owner-key re-derive (force_refresh) "
            "is required. This is transient, not a credential rejection."
        )
    if (
        "encryptedownerkeyandmetadata" in text
        or "encryptedownerkey" in text
        or "decrypted owner_key is empty" in text
    ):
        return OwnerKeyLookupTransientError(
            "Owner key retrieval did not complete (transient): the server response "
            "was incomplete. The crypto keys are presumed valid; this will be "
            "retried."
        )
    return OwnerKeyLookupTransientError(
        "Owner key retrieval did not complete (transient): the lookup failed for "
        "an unclassified reason. The crypto keys are presumed valid; this will be "
        "retried."
    )


async def _unwrap_encrypted_identity_key(
    identity_key: bytes, *, cache: TokenCache, device_id: str | None = None
) -> bytes | None:
    """Unwrap a 60-byte encrypted identity key into a 32-byte EIK if possible."""

    if len(identity_key) != EIK_GCM_TOTAL_LEN:
        return None

    try:
        owner_key_info = await async_get_owner_key(cache=cache)
    except Exception as exc:  # pragma: no cover - defensive diagnostic path
        _LOGGER.debug("[DECRYPT] Owner key lookup failed while unwrapping EIK: %s", exc)
        return None

    try:
        decrypted_eik = await asyncio.to_thread(
            decrypt_eik, owner_key_info.key, identity_key
        )
    except Exception as exc:  # pragma: no cover - defensive diagnostic path
        _LOGGER.debug("[DECRYPT] EIK unwrap failed in thread: %s", exc)
        # Try AAD envelope fallback before giving up
        aad_result = _try_unwrap_aesgcm_envelope(
            identity_key, owner_key_info.key, device_id
        )
        if aad_result is not None and len(aad_result) == _EIK_LEN:
            _LOGGER.debug(
                "[DECRYPT] Successfully unwrapped 60-byte EIK via AAD envelope (early path)."
            )
            return bytes(aad_result)
        return None

    if len(decrypted_eik) != _EIK_LEN:
        _LOGGER.debug(
            "[DECRYPT] Unwrapped EIK length %s does not match expected %s bytes",
            len(decrypted_eik),
            _EIK_LEN,
        )
        return None

    _LOGGER.debug(
        "[DECRYPT] Successfully unwrapped 60-byte EIK to 32 bytes (early path)."
    )
    return bytes(decrypted_eik)


def _status_name_safe(code: Any) -> str:
    """Safely get the string representation of an enum, with a robust fallback."""
    try:
        common_pb2 = _get_common_pb2()
        status_name: str = common_pb2.Status.Name(int(code))
        return status_name
    except Exception:
        try:
            return str(int(code))
        except Exception:
            return str(code)


def create_google_maps_link(latitude: float, longitude: float) -> str | None:
    """Return a Google Maps link for valid coordinates, otherwise None.

    Contract:
    - Returns a valid URL string, or None if coordinates are invalid.
    - Avoids mixing error strings with URLs at call sites.

    Note: Keep this for developer diagnostics (debug level only elsewhere).
    """
    try:
        lat_f = float(latitude)
        lon_f = float(longitude)
    except (TypeError, ValueError):
        _LOGGER.debug(
            "Invalid coordinate types for Maps link; skipping link generation"
        )
        return None

    if not (_LAT_MIN <= lat_f <= _LAT_MAX and _LON_MIN <= lon_f <= _LON_MAX):
        _LOGGER.debug(
            "Out-of-bounds coordinates for Maps link; skipping link generation"
        )
        return None

    return f"https://www.google.com/maps/search/?api=1&query={lat_f},{lon_f}"


async def async_retrieve_identity_key(
    device_registration: DeviceRegistration,
    *,
    cache: TokenCache,
    device_id: str | None = None,
    _retry: bool = True,
) -> list[bytes]:  # noqa: PLR0912, PLR0915
    """Retrieve the device Ephemeral Identity Key (EIK) asynchronously.

    Flow (async-first, HA-friendly):
    - Check cache for previously decrypted EIKs (performance optimization).
    - Try both MCU bit-flip states to derive candidate keys.
    - Obtain owner key + shared key (async).
    - Decrypt each candidate EIK (CPU-bound → offload to thread).
    - On InvalidTag: try AAD envelope unwrap (matches eid_resolver pattern).
    - Cache decrypted EIKs for future requests.
    - Strictly validate length to avoid silent misuse downstream.

    Performance:
    - Decrypted EIKs are cached using a SHA-256 hash of (encrypted_key + owner_key_version + flip_state).
    - Cache hits avoid expensive AES-GCM decryption (90%+ CPU reduction on repeated polls).
    - Cache automatically invalidates when owner_key_version changes.

    Args:
        device_registration: Tracker registration metadata containing the encrypted EIK.
        cache: Entry-scoped TokenCache used for owner key and username resolution.
        device_id: Optional canonical device ID used as AAD for envelope unwrap.

    Raises:
        StaleOwnerKeyError: if tracker is encrypted with an older owner key.
        DecryptionError: for generic decryption failures.
        SpotApiEmptyResponseError: propagated if EID info trailers-only response indicates auth/session issue.
        RuntimeError: if the TokenCache is missing (multi-account safety guard).
    """
    encrypted_user_secrets = device_registration.encryptedUserSecrets

    raw_encrypted_identity_key = encrypted_user_secrets.encryptedIdentityKey

    if cache is None:
        raise RuntimeError(
            "TokenCache instance is required to retrieve the tracker identity key."
        )

    owner_key_version = getattr(encrypted_user_secrets, "ownerKeyVersion", 0)
    # Set when the single-shot structural-defect re-derive below already forced a
    # fresh owner key. It makes the later blind-refresh path a no-op so the owner
    # key is force-refreshed at most once per call (R1: exactly one force_refresh).
    did_force_rederive = False
    try:
        owner_key_info: OwnerKeyInfo = await async_get_owner_key(cache=cache)
    except SpotError as exc:
        # Typed transport/gRPC failure: always transient, never "stale". Caught
        # before the generic block so a network/timeout/status error never lands
        # in the speculative reauth path (Q5 invariant).
        _LOGGER.debug(
            "Owner-key lookup phase=%s hit a transient SpotError: %s",
            "initial lookup",
            type(exc).__name__,
        )
        raise OwnerKeyLookupTransientError(
            "Owner key retrieval did not complete (transient): the SPOT request "
            "failed at the transport/gRPC layer. The crypto keys are presumed "
            "valid; this will be retried."
        ) from exc
    except (InvalidTag, RuntimeError) as exc:
        _LOGGER.debug("Owner-key lookup phase=%s failed", "initial lookup")
        classified = _classify_owner_key_failure(exc, context="initial lookup")
        if not isinstance(classified, _OwnerKeyRederiveRequired):
            raise classified from exc
        # single-shot re-derive: a structural owner-key defect is re-derivable from
        # the (valid) shared key. Mirror the force_refresh pattern below; LINEAR,
        # not recursive -> structurally single-shot (the re-derive's own failures
        # are converted here, never re-entered into the re-derive branch).
        _LOGGER.debug(
            "Owner-key structural defect on initial lookup; re-deriving once "
            "(force_refresh)."
        )
        did_force_rederive = True
        try:
            owner_key_info = await async_get_owner_key(cache=cache, force_refresh=True)
            # Re-arm the persistent-defect WARNING guard: a successful re-derive
            # means the structural defect is resolved, so a future recurrence must
            # warn again instead of being suppressed by a lifetime-accumulating
            # guard (re-arm-on-recovery, mirrors the dedup-guard lesson from the
            # earlier device-drop work).
            _warned_rederive_persistent_defect.discard(id(cache))
        except SpotError as exc2:
            _LOGGER.debug(
                "Owner-key re-derive phase=%s hit a transient SpotError: %s",
                "forced re-derive",
                type(exc2).__name__,
            )
            raise OwnerKeyLookupTransientError(
                "Owner key retrieval did not complete (transient): the forced "
                "re-derive failed at the transport/gRPC layer. The crypto keys are "
                "presumed valid; this will be retried."
            ) from exc2
        except (InvalidTag, RuntimeError) as exc2:
            reclassified = _classify_owner_key_failure(exc2, context="forced re-derive")
            if isinstance(reclassified, _OwnerKeyRederiveRequired):
                # R4 default: still structurally defective AFTER a successful-HTTP
                # re-derive -> transient + WARNING-once (no reauth without an
                # InvalidTag shared-key rejection). Dedup keyed on id(cache) (the
                # per-entry handle available here) so polls do not spam the log.
                rederive_warn_key = id(cache)
                if rederive_warn_key not in _warned_rederive_persistent_defect:
                    _warned_rederive_persistent_defect.add(rederive_warn_key)
                    _LOGGER.warning(
                        "Owner key remains structurally defective after a forced "
                        "re-derive; treating as transient (no re-authentication). "
                        "The lookup will be retried on the next poll."
                    )
                raise OwnerKeyLookupTransientError(
                    "Owner key retrieval did not complete (transient): the owner "
                    "key is still structurally defective after a re-derive. The "
                    "crypto keys are presumed valid; this will be retried."
                ) from exc2
            raise reclassified from exc2

    # --- Proactive Owner Key Version Mismatch Check ---
    # If the tracker requires a newer owner key version than what we have cached,
    # force-refresh the owner key BEFORE attempting decryption to avoid an
    # unnecessary AES-GCM InvalidTag failure followed by a reactive retry.
    if (
        owner_key_version
        and owner_key_info.version is not None
        and owner_key_version > owner_key_info.version
    ):
        _LOGGER.info(
            "Owner Key Version mismatch detected: Tracker requires V%s, "
            "Cache has V%s. Refreshing...",
            owner_key_version,
            owner_key_info.version,
        )
        try:
            owner_key_info = await async_get_owner_key(cache=cache, force_refresh=True)
        except SpotError as exc:
            # Typed transport/gRPC failure during the forced refresh: transient,
            # never "stale" (caught before the generic block, Q5 invariant).
            _LOGGER.debug(
                "Owner-key lookup phase=%s hit a transient SpotError: %s",
                "forced refresh",
                type(exc).__name__,
            )
            raise OwnerKeyLookupTransientError(
                "Owner key retrieval did not complete (transient): the SPOT "
                "request failed at the transport/gRPC layer during refresh. The "
                "crypto keys are presumed valid; this will be retried."
            ) from exc
        except (InvalidTag, RuntimeError) as exc:
            _LOGGER.debug("Owner-key lookup phase=%s failed", "forced refresh")
            raise _classify_owner_key_failure(exc, context="forced refresh") from exc

    # Build key sources list (matches eid_resolver pattern: try owner + shared)
    key_sources: list[tuple[str, bytes]] = [("owner", owner_key_info.key)]
    try:
        _cached_user = await cache.get(username_string)
        shared_key = await async_get_shared_key(
            cache=cache,
            username=_cached_user if isinstance(_cached_user, str) else None,
        )
        if shared_key is not None:
            key_sources.append(("shared", shared_key))
    except Exception:  # noqa: BLE001 - best-effort
        pass

    candidates: list[bytes] = []
    decrypt_errors: list[Exception] = []

    for _key_source, wrapping_key in key_sources:
        for do_flip in (False, True):
            flipped_blob = flip_bits(raw_encrypted_identity_key, do_flip)

            # --- EIK Cache Lookup (Performance Optimization) ---
            eik_cache_key = _get_eik_cache_key(flipped_blob, owner_key_version, do_flip)
            cached_eik = _eik_cache.get(eik_cache_key)
            if cached_eik is not None:
                _eik_cache_stats["hits"] += 1
                _LOGGER.debug(
                    "EIK cache hit (version=%s, flip=%s)",
                    owner_key_version,
                    do_flip,
                )
                if cached_eik not in candidates:
                    candidates.append(cached_eik)
                continue

            _eik_cache_stats["misses"] += 1
            _LOGGER.debug(
                "EIK cache miss (version=%s, flip=%s, source=%s), decrypting...",
                owner_key_version,
                do_flip,
                _key_source,
            )

            try:
                # CPU-heavy → do not block the event loop
                eik_bytes = await asyncio.to_thread(
                    decrypt_eik, wrapping_key, flipped_blob
                )
                if (
                    not isinstance(eik_bytes, (bytes, bytearray))
                    or len(eik_bytes) != _EIK_LEN
                ):
                    raise DecryptionError(
                        f"Ephemeral identity key invalid (expected {_EIK_LEN} bytes)."
                    )

                key_bytes = bytes(eik_bytes)

                # Cache the decrypted EIK for future requests
                _eik_cache[eik_cache_key] = key_bytes
                _LOGGER.debug(
                    "EIK cached (version=%s, flip=%s, cache_size=%d)",
                    owner_key_version,
                    do_flip,
                    len(_eik_cache),
                )

                if key_bytes not in candidates:
                    candidates.append(key_bytes)
            except InvalidTag:
                # Fallback: try AESGCM envelope with AAD (matches eid_resolver)
                envelope_result = _try_unwrap_aesgcm_envelope(
                    flipped_blob, wrapping_key, device_id=device_id
                )
                if envelope_result is not None and len(envelope_result) == _EIK_LEN:
                    key_bytes = bytes(envelope_result)
                    _eik_cache[eik_cache_key] = key_bytes
                    _LOGGER.info(
                        "EIK decrypted via AAD envelope fallback "
                        "(version=%s, flip=%s, source=%s)",
                        owner_key_version,
                        do_flip,
                        _key_source,
                    )
                    if key_bytes not in candidates:
                        candidates.append(key_bytes)
                    continue
                decrypt_errors.append(InvalidTag())
            except Exception as exc:  # Capture and continue to try other states
                decrypt_errors.append(exc)

    if candidates and not decrypt_errors:
        return candidates

    current_owner_key_version = None
    try:
        e2ee_data = await async_get_eid_info(cache=cache)
        current_owner_key_version = (
            e2ee_data.encryptedOwnerKeyAndMetadata.ownerKeyVersion
        )
        _LOGGER.debug(
            "E2EE metadata: current ownerKeyVersion=%s", current_owner_key_version
        )
    except SpotApiEmptyResponseError:
        _LOGGER.error(
            "Failed to decrypt identity key due to empty trailers-only EID info response "
            "(authentication/session). Please re-authenticate and retry."
        )
        raise
    except Exception as meta_exc:  # best-effort diagnostics
        # Downgraded to debug: this metadata fetch is a non-actionable,
        # best-effort diagnostic aid. Its failure (e.g. transient network
        # timeout or SSL teardown) is swallowed and the regular decrypt
        # error path continues unaffected, so it must not surface as a
        # user-facing WARNING.
        _LOGGER.debug("Failed to retrieve E2EE metadata for diagnostics: %s", meta_exc)

    old_ver = getattr(encrypted_user_secrets, "ownerKeyVersion", None)
    last_exc = decrypt_errors[-1] if decrypt_errors else None
    if (
        current_owner_key_version is not None
        and old_ver is not None
        and old_ver < current_owner_key_version
    ):
        if _retry:
            username = None
            cache_key: str | None = None
            try:
                username = await cache.get(username_string)
            except Exception as cache_exc:
                _LOGGER.debug(
                    "Failed to resolve username from cache before clearing owner key: %s",
                    cache_exc,
                )

            if isinstance(username, str) and username:
                cache_key = f"owner_key_{username}"

            _LOGGER.info(
                "Owner key version mismatch (tracker=%s, current=%s); %s and retrying once.",
                old_ver,
                current_owner_key_version,
                "clearing cached owner key"
                if cache_key
                else "retrying with fresh owner key",
            )

            if cache_key:
                try:
                    # TokenCache clears entries when the value is set to None
                    await cache.set(cache_key, None)
                except Exception as cache_exc:
                    _LOGGER.debug("Failed to clear cached owner key: %s", cache_exc)

            return await async_retrieve_identity_key(
                device_registration,
                cache=cache,
                device_id=device_id,
                _retry=False,
            )

        _LOGGER.error(
            "Owner key version mismatch: tracker=%s, current=%s. "
            "This typically occurs after resetting E2EE data for this tracker. "
            "Re-pair this single tracker in the Find My Device app; no account "
            "re-authentication and no new secrets.json are required, and other "
            "devices are unaffected.",
            old_ver,
            current_owner_key_version,
        )
        raise StaleOwnerKeyError(
            "Tracker was encrypted with a stale owner key version; re-pair this "
            "single tracker (no account re-authentication or new secrets.json needed)."
        ) from last_exc

    # Blind refresh: version check unavailable but key is clearly wrong.
    # This mirrors the version-mismatch retry above but works when SPOT gRPC
    # fails to provide current_owner_key_version (common in standalone CLI).
    if (
        not candidates
        and current_owner_key_version is None
        and _retry
        and not did_force_rederive
    ):
        _LOGGER.info(
            "EIK decryption failed and SPOT version check unavailable; "
            "attempting blind owner key refresh."
        )
        try:
            username = await cache.get(username_string)
            if isinstance(username, str) and username:
                await cache.set(f"owner_key_{username}", None)
                await cache.set(f"shared_key_{username}", None)
            # Also clear the bare key (written by generic secrets loop).
            # In HA mode this triggers RuntimeError → ConfigEntryAuthFailed
            # → reauth flow, where the user provides a fresh secrets bundle.
            await cache.set("shared_key", None)
        except Exception as cache_exc:  # noqa: BLE001
            _LOGGER.debug("Failed to clear cached keys: %s", cache_exc)
        try:
            return await async_retrieve_identity_key(
                device_registration,
                cache=cache,
                device_id=device_id,
                _retry=False,
            )
        except Exception as refresh_exc:  # noqa: BLE001
            _LOGGER.warning("Blind owner key refresh also failed: %s", refresh_exc)

    if candidates:
        return candidates

    _LOGGER.error(
        "Failed to decrypt identity key (owner key version %s vs. current %s). "
        "If you recently reset E2EE data, re-authenticate or recreate keys. "
        "If the issue persists, clear the integration secrets to force a fresh key derivation.",
        old_ver,
        current_owner_key_version,
    )
    raise DecryptionError("Identity key decryption failed.") from last_exc


def retrieve_identity_key(
    device_registration: DeviceRegistration,
    *,
    cache: TokenCache | None = None,
) -> list[bytes]:
    """Legacy synchronous facade removed in favor of the async API."""

    raise RuntimeError(
        "Legacy sync retrieve_identity_key() has been removed. "
        "Use await async_retrieve_identity_key(..., cache=...) instead."
    )


def _parse_epoch_seconds(value: Any, now_s: float) -> float | None:  # noqa: PLR0911
    """Robustly parse a Unix epoch timestamp (float) from various inputs.

    Handles int, float, str, bytes, and protobuf Time objects.
    Sanitizes strings, checks for finiteness, and applies a plausibility window.

    Returns:
        The timestamp as a float, or None if invalid or implausible.
    """
    v: float
    # bool is an int subclass; reject it explicitly for type parity with the
    # numeric normalizers (normalize_pair_date_value /
    # normalize_creation_timestamp_value), which both guard with
    # ``not isinstance(raw, bool)`` before numeric handling. Without this a
    # bool would fall through to ``float(raw)`` and be treated as a number
    # instead of a type-foreign value (H5 parity).
    if isinstance(value, bool):
        return None
    # Protobuf Timestamp: seconds (+ optional nanos)
    if hasattr(value, "seconds"):
        try:
            secs = float(getattr(value, "seconds"))
            nanos = float(getattr(value, "nanos", 0.0))
            v = secs + nanos / 1e9
        except (TypeError, ValueError):
            return None
    else:
        raw = value
        # Bytes -> UTF-8
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8", "strict")
            except Exception:
                return None
        # Sanitize strings (Whitespace, BOM, Non-breaking space)
        if isinstance(raw, str):
            raw = raw.strip().replace("\ufeff", "").replace("\u00a0", "")
        try:
            v = float(raw)
        except (TypeError, ValueError):
            return None

    # Unit heuristic (ms/μs)
    if v > _MICROSECONDS_THRESHOLD:  # microseconds
        v /= 1e6
    elif v > _MILLISECONDS_THRESHOLD:  # milliseconds
        v /= 1e3

    # Finite and plausibility check
    if not math.isfinite(v):
        return None
    # Plausibility: >= 2000-01-01 and <= now + realistic drift window
    if v < _MIN_VALID_EPOCH_S:
        return None
    if v > (now_s + MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S):
        return None
    return v


def normalize_pair_date_value(raw: Any, *, now_wall: float) -> int | None:
    """Normalize pairDate values that may include millisecond or microsecond units."""

    if raw is None:
        return None

    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        if not math.isfinite(raw):
            return None

        value = float(raw)
        if value > _MICROSECONDS_THRESHOLD:
            value /= 1e6
        elif value > _MILLISECONDS_THRESHOLD:
            value /= 1e3

        if value < _MIN_VALID_EPOCH_S:
            return None
        if value > (now_wall + MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S):
            return None
        return int(value)

    ts = _parse_epoch_seconds(raw, now_wall)
    if ts is None:
        return None
    return int(ts)


def normalize_creation_timestamp_value(raw: Any, *, now_wall: float) -> int | None:
    """Normalize encryptedUserSecrets.creationDate inputs."""

    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        if not math.isfinite(raw):
            return None

        value = float(raw)
        if value > _MICROSECONDS_THRESHOLD:
            value /= 1e6
        elif value > _MILLISECONDS_THRESHOLD:
            value /= 1e3

        if value < _MIN_VALID_EPOCH_S:
            return None
        if value > (now_wall + MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S):
            return None
        return int(value)

    ts = _parse_epoch_seconds(raw, now_wall)
    if ts is None:
        return None
    return int(ts)


async def _offload_decrypt_aes(identity_key: bytes, encrypted_location: bytes) -> bytes:
    """Offload AES-GCM decryption; derive key hash cheaply on event loop."""
    identity_key_hash = hashlib.sha256(identity_key).digest()  # cheap hash → OK on loop
    return await asyncio.to_thread(
        decrypt_aes_gcm, identity_key_hash, encrypted_location
    )


def _component_keys(
    device_registration: object, identity_keys: Sequence[bytes]
) -> list[bytes]:
    """Return the per-component keys of a multi-component device, else nothing.

    Finder reports on a component EID are encrypted to that EID (``docs/FMDN.md``
    S3.4.1). These keys are only tried: they are neither identity keys nor
    candidates, so nothing may promote or persist them.
    """
    if (trackable_components_of(device_registration) or 0) <= 1:
        return []
    return [
        derive_component_key(key, component)
        for key in identity_keys
        if len(key) == EIK_LENGTH
        for component in MEASURED_COMPONENT_ROWS
    ]


async def _offload_decrypt_foreign(
    identity_keys: Sequence[bytes],
    encrypted_location: bytes,
    public_key_random: bytes,
    time_offset: int,
    preferred_reading_id: str | None,
) -> ForeignDecryptResult:
    """Offload ECC-based decryption of a foreign report over all key candidates.

    Every key and every reading of the curve selected from
    ``len(public_key_random)`` is tried in the worker thread; the preferred
    reading goes first. Failures raise the ``ForeignReportError`` family.
    """
    return await asyncio.to_thread(
        decrypt_foreign_report,
        tuple(identity_keys),
        encrypted_location,
        public_key_random,
        time_offset,
        preferred_reading_id=preferred_reading_id,
    )


# ----------------------------- Validation helpers -----------------------------
def _ensure_bytes(value: object) -> bytes | None:
    """Return a ``bytes`` instance when the value can be losslessly coerced."""

    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return None


def _is_valid_latlon(lat: float, lon: float) -> bool:
    """Validate latitude/longitude are finite and within geographic bounds.

    POPETS'25 notes integer-scaled coordinates (±90/±180 after scaling by 1e7).
    We validate after scaling here and fail fast on out-of-range/NaN/Inf.
    """
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except (TypeError, ValueError):
        return False
    if not (math.isfinite(lat_f) and math.isfinite(lon_f)):
        return False
    if not (_LAT_MIN <= lat_f <= _LAT_MAX and _LON_MIN <= lon_f <= _LON_MAX):
        return False
    return True


def _infer_report_hint(status_value: Any) -> str | None:
    """Infer a throttling hint from the protobuf Status.

    Strategy:
    1) Prefer **explicit enum comparisons** (robust across locales).
    2) Fall back to **name substring checks** if enums are unavailable
       in the environment/build (defensive coding for older protobufs).

    Hints:
        - "high_traffic"  → aggregated server-side reports typically throttled more aggressively.
        - "in_all_areas"  → crowdsourced reports available broadly; back off for longer.
        - None            → unknown/irrelevant; coordinator applies no type-specific cooldown.
    """
    # --- Explicit enum mapping (robust path) -----------------------
    common_pb2 = _get_common_pb2()
    try:
        if int(status_value) == getattr(common_pb2.Status, "CROWDSOURCED"):
            return "in_all_areas"
    except Exception:
        pass
    try:
        if int(status_value) == getattr(common_pb2.Status, "AGGREGATED"):
            return "high_traffic"
    except Exception:
        pass

    # --- Conservative fallback based on enum name -------------------
    try:
        name = common_pb2.Status.Name(int(status_value)).lower()
    except Exception:
        return None

    if "high" in name and "traffic" in name:
        return "high_traffic"
    if "in" in name and "all" in name and "areas" in name:
        return "in_all_areas"
    return None


def is_real_location_record(record: dict[str, Any] | None) -> bool:
    """Return True only for an authenticated, decrypted *coordinate* report.

    Clearing the shared decrypt-failure (reauth) budget requires positive proof
    that the account-wide shared key still decrypts. Decrypted coordinates are
    that proof: they are produced exclusively by the authenticated crypto path
    (own/foreign AES-GCM decrypt, then lat/lon validation below). Requiring a real
    coordinate pair is therefore an allowlist for genuine reports and rejects every
    truthy-but-unauthenticated record shape, including:

    - ``metadata_only=True`` sentinel rows -- only secrets-bundle key *material*,
      emitted when no encrypted report decrypts (e.g. a phone without a fresh fix).
    - ``SEMANTIC`` rows -- appended with ``decrypted_location=b""`` and ``continue``d
      before the crypto path, so they carry a server-provided ``semantic_name`` and
      a ``last_seen`` timestamp but ``latitude``/``longitude`` are ``None``.

    Neither shape carries coordinates, so neither may clear the budget; a denylist
    on a single known sentinel (``metadata_only``) would miss the others. ``is not
    None`` (not truthiness) preserves legitimate ``0.0`` coordinates. The metadata
    merges below only ever write key-material/date keys, never ``latitude``/
    ``longitude``, so they cannot make a sentinel/SEMANTIC row look authenticated.
    """
    if not record:
        return False
    return record.get("latitude") is not None and record.get("longitude") is not None


def any_real_location_record(records: list[dict[str, Any]] | None) -> bool:
    """Return True if ANY record in a response authenticates a coordinate report.

    The decrypt proof -- positive evidence that the account-wide shared key still
    decrypts -- is a property of the WHOLE response, not of any single record. A
    display selector that ranks by newest ``last_seen`` (see
    ``api._select_best_location``) can hand back a report-less SEMANTIC/metadata
    row even when a sibling coordinate report decrypted successfully in the same
    response. Evaluating only that collapsed record would discard the proof and
    let a later transient failure trip a spurious reauth. Both automatic paths
    (poll and background push) must therefore run the FULL candidate list through
    this one predicate so a hidden success is never lost. See
    ``is_real_location_record`` for the per-record allowlist.
    """
    if not records:
        return False
    return any(is_real_location_record(record) for record in records)


# ----------------------------- Main decryptor ---------------------------------
async def async_decrypt_location_response_locations(  # noqa: PLR0912, PLR0915
    device_update_protobuf: DeviceUpdateProto,
    *,
    cache: TokenCache,
    action_feedback: bool = False,
) -> list[dict[str, Any]]:
    """Decrypt and normalize location reports into HA-friendly dicts (async).

    Guarantees:
    - Event loop remains responsive: CPU-heavy crypto is offloaded via asyncio.to_thread().
    - Fail-fast: malformed coordinates are dropped at the decryption boundary,
      preventing bad data from leaking into higher layers (HA Platinum quality).
    - Robust against partial/invalid reports (log and continue).
    - No prints or process termination; errors bubble or are logged with context.

    Raises:
        DecryptionError: if the device has its own encrypted reports and every
            one of them fails authentication (no own-report success). This is an
            account-wide stale-key signal; foreign/crowdsourced failures and
            report-less devices never raise (they return ``[]``/metadata-only).

    Args:
        device_update_protobuf: Raw protobuf payload containing encrypted locations.
        cache: Entry-scoped TokenCache forwarded to key/EID helpers.
        action_feedback: True when the caller recognised the payload as the
            acknowledgement of a Play/Stop Sound (its ``requestUuid`` is one of
            our own Play Sound keys). Such a push carries key material but no
            location report by design, so "No locations found" is expected and
            logged at DEBUG instead of INFO. Decryption itself is unchanged:
            the metadata-only record is still returned.

    POPETS'25 reference (Böttger et al., 2025):
      - Integer-scaled coordinates and validation: §4
      - "High Traffic" vs. "In All Areas" throttling semantics: §4–5
    """
    common_pb2 = _get_common_pb2()
    device_update_pb2 = _get_device_update_pb2()
    # Defensive guards on required metadata
    try:
        device_registration: DeviceRegistration = (
            device_update_protobuf.deviceMetadata.information.deviceRegistration
        )
    except Exception as exc:
        _LOGGER.error("Device registration metadata missing or invalid: %s", exc)
        raise

    # Extract canonical device ID for AAD envelope unwrap (matches eid_resolver)
    canonic_id = _extract_canonic_id(device_update_protobuf)
    # One token per call: a poll and an FCM push each count as one poll for
    # the foreign-reading feedback threshold (three reports in two polls).
    poll_id = object()
    # Per config entry, so two entries never share foreign-reading state; no
    # tracking without a canonical ID. The key is case-normalized (CX-7).
    device_key: DeviceKey | None = (
        foreign_device_key(getattr(cache, "entry_id", None), canonic_id)
        if canonic_id is not None
        else None
    )

    encrypted_user_secrets = device_registration.encryptedUserSecrets
    raw_encrypted_identity_key: bytes = b""
    early_unwrapped_identity_key: bytes | None = None
    try:
        raw_encrypted_identity_key = getattr(
            encrypted_user_secrets, "encryptedIdentityKey", b""
        )
        serialized_length = None
        secrets_blob: bytes | None = None
        try:
            secrets_blob = encrypted_user_secrets.SerializeToString()
            serialized_length = len(secrets_blob)
        except Exception as serialize_exc:  # pragma: no cover - diagnostics only
            _LOGGER.debug(
                "Failed to serialize the encrypted key container for length check: %s",
                serialize_exc,
            )

        if (
            raw_encrypted_identity_key
            and len(raw_encrypted_identity_key) == EIK_GCM_TOTAL_LEN
        ):
            early_unwrapped_identity_key = await _unwrap_encrypted_identity_key(
                raw_encrypted_identity_key, cache=cache, device_id=canonic_id
            )

        # Structure only: field names, types and lengths. Neither the
        # registration message (its text format prints the key escaped) nor
        # the key bytes themselves are logged (AGENTS.md section 5). Guarded:
        # the field walk runs per device on the event loop.
        if _LOGGER.isEnabledFor(logging.DEBUG):
            _LOGGER.debug(
                "[DIAG-SECRETS] Structure Analysis:\n"
                "  - DeviceReg Fields: %s\n"
                "  - Secrets Container Type: %s\n"
                "  - Secrets Serialized Length: %s bytes\n"
                "  - EncryptedIdentityKey Length: %s bytes\n"
                "  - EncryptedIdentityKey Type: %s",
                [field.name for field, _value in device_registration.ListFields()],
                type(encrypted_user_secrets),
                serialized_length if serialized_length is not None else "Unknown",
                len(raw_encrypted_identity_key)
                if raw_encrypted_identity_key
                else "None",
                type(raw_encrypted_identity_key),
            )

        if (
            serialized_length is not None
            and serialized_length > _STRUCTURED_PAYLOAD_LEN_THRESHOLD
        ):
            _LOGGER.warning(
                "[DIAG-ALERT] encryptedUserSecrets serialized length is %d bytes (> %d)."
                " This suggests a wrapped/structured payload instead of a raw key.",
                serialized_length,
                _STRUCTURED_PAYLOAD_LEN_THRESHOLD,
            )

        if (
            early_unwrapped_identity_key is None
            and raw_encrypted_identity_key
            and len(raw_encrypted_identity_key) != _EIK_LEN
        ):
            _LOGGER.debug(
                "[DIAG] Key length is %d (expected %d for raw EIK);"
                " likely GCM-wrapped (normal for Moto Tag / Chipolo).",
                len(raw_encrypted_identity_key),
                _EIK_LEN,
            )

        if (
            early_unwrapped_identity_key is None
            and raw_encrypted_identity_key
            and len(raw_encrypted_identity_key) > _EIK_LEN
        ):
            _LOGGER.debug(
                "[DIAG] Key length %d bytes exceeds expected raw EIK length (%d);"
                " probable GCM-wrapped key (will attempt unwrap).",
                len(raw_encrypted_identity_key),
                _EIK_LEN,
            )

        if secrets_blob is not None and raw_encrypted_identity_key:
            if raw_encrypted_identity_key in secrets_blob:
                offset = secrets_blob.find(raw_encrypted_identity_key)
                prefix_start = max(0, offset - 10)
                prefix_bytes = secrets_blob[prefix_start:offset]
                suffix_start = offset + len(raw_encrypted_identity_key)
                suffix_bytes = secrets_blob[suffix_start : suffix_start + 10]
                # Offset and lengths only; the surrounding bytes are part of
                # the secrets blob and are never logged (AGENTS.md section 5).
                _LOGGER.debug(
                    "[DIAG-SECRETS-BYTE-SCAN] Cloud key located inside encryptedUserSecrets at offset %d."
                    " Prefix (%d bytes) | Suffix (%d bytes)",
                    offset,
                    len(prefix_bytes),
                    len(suffix_bytes),
                )
            else:
                _LOGGER.debug(
                    "[DIAG-SECRETS-BYTE-SCAN] Cloud key NOT found inside encryptedUserSecrets blob."
                    " This suggests the blob holds a distinct container or wrapped value."
                )
    except Exception as exc:  # pragma: no cover - diagnostics only
        _LOGGER.warning(
            "[DIAG-ERROR] Failed to inspect the encrypted key container: %s", exc
        )
        raw_encrypted_identity_key = b""

    raw_encrypted_identity_key = bytes(raw_encrypted_identity_key)
    raw_owner_key_version = getattr(encrypted_user_secrets, "ownerKeyVersion", None)

    # Bug 6 fix: Always generate the full candidate set (MCU flip variants,
    # owner + shared keys) so that foreign/crowdsourced FMDN reports can try
    # all identity-key candidates.  Prepend the early-unwrapped key when
    # available — it is the most likely correct key and avoids extra work on
    # the happy path.
    try:
        retrieved_candidates = await async_retrieve_identity_key(
            device_registration, cache=cache, device_id=canonic_id
        )
    except SpotApiEmptyResponseError as exc:
        # This handler is only reachable from the secondary async_get_eid_info()
        # diagnostic call inside async_retrieve_identity_key (line ~511), NOT
        # from the primary owner-key path (which converts to ConfigEntryAuthFailed,
        # caught by the generic except Exception below).
        # Before Bug 6, this call was skipped entirely when early_unwrapped
        # was available, so re-raising unconditionally would regress devices
        # that previously worked fine.  Fall back to the early key when we
        # have one; re-raise only when there is no fallback so the
        # coordinator can trigger ConfigEntryAuthFailed / reauth.
        if early_unwrapped_identity_key is not None:
            _LOGGER.warning(
                "Identity-key retrieval returned SpotApiEmptyResponseError "
                "(%s); falling back to early-unwrapped key.",
                exc,
            )
            retrieved_candidates = []
        else:
            raise
    except Exception as exc:
        if early_unwrapped_identity_key is not None:
            _LOGGER.debug(
                "async_retrieve_identity_key failed (%s), using early-unwrapped key only.",
                exc,
            )
            retrieved_candidates = []
        else:
            raise

    if early_unwrapped_identity_key is not None:
        early_bytes = bytes(early_unwrapped_identity_key)
        if early_bytes not in retrieved_candidates:
            identity_key_candidates = [early_bytes] + retrieved_candidates
        else:
            identity_key_candidates = retrieved_candidates
    else:
        identity_key_candidates = retrieved_candidates
    identity_key = identity_key_candidates[0] if identity_key_candidates else None
    identity_key_bytes = bytes(identity_key) if identity_key is not None else None
    identity_key_candidate_bytes = [
        bytes(candidate) for candidate in identity_key_candidates
    ]

    # Handle Moto Tag / Chipolo-style 60-byte EIK wrappers by unwrapping to 32 bytes.
    if identity_key_bytes and len(identity_key_bytes) == EIK_GCM_TOTAL_LEN and cache:
        try:
            owner_key_info = await async_get_owner_key(cache=cache)
            decrypted_identity_key = await asyncio.to_thread(
                decrypt_eik, owner_key_info.key, identity_key_bytes
            )
            if len(decrypted_identity_key) == _EIK_LEN:
                _LOGGER.debug("[DECRYPT] Successfully unwrapped 60-byte EIK.")
                identity_key_bytes = bytes(decrypted_identity_key)
                identity_key_candidate_bytes = [identity_key_bytes]
                identity_key = identity_key_bytes
        except Exception as exc:  # pragma: no cover - diagnostics only
            _LOGGER.warning(
                "[DECRYPT] Failed to unwrap 60-byte EIK: %s; retrying with raw key.",
                type(exc).__name__,
            )

    if identity_key is None:
        raise DecryptionError("Identity key derivation returned no candidates.")

    try:
        locations_proto = device_update_protobuf.deviceMetadata.information.locationInformation.reports.recentLocationAndNetworkLocations
    except Exception as exc:
        _LOGGER.error("Location information missing or invalid: %s", exc)
        raise

    is_mcu = is_mcu_tracker(device_registration)

    now_wall = time.time()
    metadata: dict[str, Any] = {}
    metadata_update: dict[str, Any] = {}
    device_registration_metadata: dict[str, Any] = {}
    encrypted_user_secrets_metadata: dict[str, Any] = {}
    device_type_information_metadata: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # PERSISTENCE DATA EXTRACTION (Reboot Bug fix)
    # Preserve pairing and secrets metadata as soon as the protobuf is
    # parsed so the coordinator can persist the refreshed crypto material
    # to the device registry. Without this step, HA restarts lose the
    # decrypted identity key and creation date (Moto Tag / Chipolo lock-on
    # fails).
    # ------------------------------------------------------------------
    if device_registration:
        pair_date_raw = getattr(device_registration, "pairDate", None)
        if pair_date_raw is not None:
            metadata_update.setdefault("pair_date", pair_date_raw)
            metadata_update.setdefault("pairDate", pair_date_raw)

        # FIX: Extract manufacturer, model, and fast_pair_model_id for EID resolver
        manufacturer_raw = getattr(device_registration, "manufacturer", None)
        if manufacturer_raw and isinstance(manufacturer_raw, str):
            metadata_update.setdefault("manufacturer", manufacturer_raw)

        model_raw = getattr(device_registration, "model", None)
        if model_raw and isinstance(model_raw, str):
            metadata_update.setdefault("model", model_raw)

        fast_pair_model_id_raw = getattr(device_registration, "fastPairModelId", None)
        if fast_pair_model_id_raw and isinstance(fast_pair_model_id_raw, str):
            metadata_update.setdefault("fast_pair_model_id", fast_pair_model_id_raw)
            metadata_update.setdefault("fastPairModelId", fast_pair_model_id_raw)

    if encrypted_user_secrets:
        creation = getattr(encrypted_user_secrets, "creationDate", None)
        if creation is not None:
            creation_seconds = getattr(creation, "seconds", None)
            if creation_seconds is not None:
                metadata_update.setdefault("secrets_creation_date", creation_seconds)
                metadata_update.setdefault("secretsCreationDate", creation_seconds)
                metadata_update.setdefault("creationDate", creation_seconds)
                metadata_update.setdefault("creation_date", creation_seconds)

    device_type_information = getattr(
        device_update_protobuf, "deviceTypeInformation", None
    )

    pair_date_sources = [
        getattr(device_registration, "pairDate", None),
        getattr(device_update_protobuf, "pairDate", None),
        getattr(device_type_information, "pairDate", None),
    ]

    pair_date: int | None = None
    for candidate in pair_date_sources:
        pair_date = normalize_pair_date_value(candidate, now_wall=now_wall)
        if pair_date is not None:
            break

    if pair_date is not None:
        metadata_update["pair_date"] = pair_date
        metadata_update["pairDate"] = pair_date
        device_registration_metadata["pairDate"] = pair_date
        if device_type_information is not None:
            device_type_information_metadata["pairDate"] = pair_date

    creation_date_sources = [
        getattr(encrypted_user_secrets, "creationDate", None),
        getattr(
            getattr(device_update_protobuf, "encryptedUserSecrets", None),
            "creationDate",
            None,
        ),
        getattr(
            getattr(device_type_information, "encryptedUserSecrets", None),
            "creationDate",
            None,
        ),
    ]

    secrets_creation_date: int | None = None
    for candidate in creation_date_sources:
        secrets_creation_date = normalize_creation_timestamp_value(
            candidate, now_wall=now_wall
        )
        if secrets_creation_date is not None:
            break

    if secrets_creation_date is not None:
        metadata_update["secrets_creation_date"] = secrets_creation_date
        metadata_update["secretsCreationDate"] = secrets_creation_date
        metadata_update["creationDate"] = secrets_creation_date
        metadata_update["creation_date"] = secrets_creation_date
        encrypted_user_secrets_metadata["creationDate"] = secrets_creation_date
        encrypted_user_secrets_metadata["creation_date"] = secrets_creation_date
        if device_type_information is not None:
            device_type_information_metadata["creationDate"] = secrets_creation_date

    if device_registration_metadata:
        metadata["device_registration"] = device_registration_metadata
        metadata.setdefault("deviceRegistration", device_registration_metadata)

    if encrypted_user_secrets_metadata:
        metadata["encrypted_user_secrets"] = encrypted_user_secrets_metadata
        metadata.setdefault("encryptedUserSecrets", encrypted_user_secrets_metadata)

    if device_type_information_metadata:
        metadata["device_type_information"] = device_type_information_metadata
        metadata.setdefault("deviceTypeInformation", device_type_information_metadata)

    # Store identity keys as bytes (coordinator normalizes via _normalize_identity_key)
    if identity_key_bytes is not None and len(identity_key_bytes) == _EIK_LEN:
        metadata_update["identity_key"] = identity_key_bytes
        metadata_update["identityKey"] = identity_key_bytes
    if identity_key_candidate_bytes:
        metadata.setdefault("identity_key_candidates", identity_key_candidate_bytes)
        metadata.setdefault("identityKeyCandidates", identity_key_candidate_bytes)
    if raw_encrypted_identity_key:
        metadata_update.setdefault("encrypted_identity_key", raw_encrypted_identity_key)
        metadata_update.setdefault("encryptedIdentityKey", raw_encrypted_identity_key)
    if raw_owner_key_version is not None:
        metadata.setdefault("owner_key_version", raw_owner_key_version)
        metadata.setdefault("ownerKeyVersion", raw_owner_key_version)

    if metadata_update:
        metadata.update(metadata_update)

    # Assemble reports (preserve semantics; own report is appended if present)
    recent_location = locations_proto.recentLocation
    recent_location_time = locations_proto.recentLocationTimestamp
    network_locations: list[Any] = list(locations_proto.networkLocations)
    network_locations_time: list[Any] = list(locations_proto.networkLocationTimestamps)

    len_diff = len(network_locations) - len(network_locations_time)
    if len_diff > 0:
        network_locations_time.extend([None] * len_diff)
    elif len_diff < 0:
        network_locations_time = network_locations_time[: len(network_locations)]

    if locations_proto.HasField("recentLocation"):
        network_locations.append(recent_location)
        network_locations_time.append(recent_location_time)

    # Optional hard cap (defense-in-depth against pathological inputs)
    if len(network_locations) > _MAX_REPORTS:
        _LOGGER.warning(
            "Truncating reports: %s → %s", len(network_locations), _MAX_REPORTS
        )
        network_locations = network_locations[:_MAX_REPORTS]
        network_locations_time = network_locations_time[:_MAX_REPORTS]

    wrapped: list[WrappedLocation] = []
    _auth_failures = 0  # Track MAC / InvalidTag failures across all reports
    _encrypted_report_count = 0  # Count non-SEMANTIC reports attempted
    # Diff-Review #1: Own-report-only failure tracking. Unlike the mixed
    # counters above (which include foreign/crowdsourced reports that legitimately
    # fail when decrypted with another account's key), these track ONLY the
    # device's own reports (public_key_random == b""). Exhausting every own report
    # is the one account-wide auth signal that warrants reauth escalation.
    _own_encrypted_report_count = 0  # Own (Owner-key) reports attempted
    _own_auth_failures = 0  # Own-report MAC / InvalidTag failures
    _own_report_success = False  # At least one own report authenticated OK
    # P-256 crowdsourced reports that no key and no reading authenticated. Kept
    # apart from _auth_failures: the readings are provisional, so a miss says
    # nothing about the cached EIK and must not invalidate it (Z11).
    _p256_auth_failures = 0
    # The part of them counted in _auth_failures: misses on a device for which
    # a reading has already decrypted a report.
    _p256_counted_failures = 0
    # Foreign reports whose outcome says nothing about the cached EIK: P-256
    # misses, an Sx length without a curve, a malformed payload. They leave the
    # denominator of the invalidation below, so an auth failure beside them
    # still counts as "every key-relevant report failed" (CX-5).
    _key_neutral_report_count = 0

    # FIX #155: Prepare all identity key candidates for multi-candidate retry.
    # async_retrieve_identity_key can return multiple candidates (MCU bit-flip
    # variants, owner+shared key sources). The primary candidate may work for
    # own-reports (AES-GCM) but fail for foreign/crowdsourced reports (ECDH +
    # AES-EAX with different key derivation). Trying all candidates prevents
    # silent loss of all crowdsourced location data.
    all_identity_keys: list[bytes] = [
        bytes(k)
        for k in (identity_key_candidates or [])
        if k is not None and len(k) == _EIK_LEN
    ]
    # Track the active key; promote alternate candidates on success
    active_identity_key: bytes | None = (
        all_identity_keys[0] if all_identity_keys else identity_key
    )

    if len(network_locations) != len(network_locations_time):
        _LOGGER.debug(
            "Mismatched report arrays: locations=%s timestamps=%s (dropping unmatched entries)",
            len(network_locations),
            len(network_locations_time),
        )
    for loc, time_ts in zip_longest(
        network_locations, network_locations_time, fillvalue=None
    ):
        if loc is None or time_ts is None:
            continue
        # Defaults for diagnostic logging in except handlers
        public_key_random = b""
        time_offset = 0
        try:
            ts = _parse_epoch_seconds(time_ts, now_wall)
            if ts is None:
                _LOGGER.warning(
                    "Dropping one location report due to invalid or missing timestamp (raw=%r).",
                    time_ts,
                )
                continue

            if loc.status == common_pb2.Status.SEMANTIC:
                wrapped.append(
                    WrappedLocation(
                        decrypted_location=b"",
                        time=ts,
                        accuracy=0,  # Internal placeholder, not for display
                        status=loc.status,
                        is_own_report=False,  # SEMANTIC is not an Owner-Report
                        is_network_report=True,  # SEMANTIC hits are network-side, never own AES-GCM
                        name=loc.semanticLocation.locationName,
                    )
                )
                continue

            _encrypted_report_count += 1
            enc = loc.geoLocation.encryptedReport
            encrypted_location: bytes = enc.encryptedLocation
            public_key_random = enc.publicKeyRandom

            if public_key_random == b"":  # Own report
                _own_encrypted_report_count += 1
                if active_identity_key is None:
                    # Unreachable in practice: a None identity_key already raised
                    # DecryptionError at function entry, and active_identity_key
                    # falls back to the non-None identity_key. Kept as a defensive
                    # guard. Intentionally a ValueError (config error, not an auth
                    # signal) so it is NOT counted as an own auth failure and does
                    # not trigger stale-key reauth escalation.
                    raise ValueError(
                        "No identity key available for own-report decryption"
                    )
                decrypted_location_raw = await _offload_decrypt_aes(
                    active_identity_key, encrypted_location
                )
                # Authentication passed: the cached identity key still matches the
                # server's own reports. One success proves the key is healthy, so
                # foreign-report failures must not trigger account-wide escalation.
                _own_report_success = True
            else:
                # FIX #155: Foreign/crowdsourced reports use ECDH + AES-EAX
                # with a different key derivation path than own reports.
                # decrypt_foreign_report tries every identity key candidate and
                # every reading of the curve before giving up; failures are
                # classified by type in the handlers below.
                time_offset = 0 if is_mcu else loc.geoLocation.deviceTimeOffset
                foreign_keys = all_identity_keys or (
                    [active_identity_key] if active_identity_key else []
                )
                decrypted_location_raw = None
                # With no candidate at all, decrypt_foreign_report would reject
                # the empty key list as a generic ValueError; skip the attempt
                # and let the payload check below drop the report.
                if foreign_keys:
                    foreign_result = await _offload_decrypt_foreign(
                        [
                            *foreign_keys,
                            *_component_keys(device_registration, foreign_keys),
                        ],
                        encrypted_location,
                        public_key_random,
                        time_offset,
                        FOREIGN_READING_TRACKER.preferred(device_key)
                        if device_key is not None
                        else None,
                    )
                    decrypted_location_raw = foreign_result.plaintext
                    # An index past the identity keys is a per-component key,
                    # which stands for the identity key and is never promoted.
                    candidate_key = (
                        foreign_keys[foreign_result.key_index]
                        if foreign_result.key_index < len(foreign_keys)
                        else None
                    )
                    # Promote successful alternate candidate for remaining reports
                    # and sync identity_key_bytes so payload construction uses
                    # the key that actually decrypted.
                    if candidate_key not in (None, active_identity_key):
                        active_identity_key = candidate_key
                        identity_key_bytes = bytes(candidate_key)
                        # Sync metadata_update and metadata so the
                        # promoted key persists to cache/payload
                        # instead of the original stale value.
                        metadata_update["identity_key"] = identity_key_bytes
                        metadata_update["identityKey"] = identity_key_bytes
                        metadata["identity_key"] = identity_key_bytes
                        metadata["identityKey"] = identity_key_bytes
                        # Reorder candidates so the promoted key is
                        # tried first for remaining reports.
                        all_identity_keys.remove(candidate_key)
                        all_identity_keys.insert(0, candidate_key)
                        # Keep payload-facing list in sync so
                        # persisted candidates reflect the promotion.
                        identity_key_candidate_bytes = list(all_identity_keys)
                        _LOGGER.info(
                            "Foreign decryption succeeded with alternate "
                            "identity key candidate (index=%d)",
                            foreign_result.key_index,
                        )
                    if device_key is not None:
                        FOREIGN_READING_TRACKER.note_success(
                            device_key,
                            foreign_result.reading,
                            len(public_key_random),
                        )

            decrypted_location = _ensure_bytes(decrypted_location_raw)
            if decrypted_location is None:
                _LOGGER.warning(
                    "Decrypted location payload is not bytes (type=%s); dropping one report",
                    type(decrypted_location_raw).__name__,
                )
                continue

            wrapped.append(
                WrappedLocation(
                    decrypted_location=decrypted_location,
                    time=ts,
                    accuracy=loc.geoLocation.accuracy,
                    status=loc.status,
                    is_own_report=enc.isOwnReport,
                    # Cryptographic provenance from the decrypt path: an empty
                    # publicKeyRandom is an own AES-GCM report; a non-empty one is a
                    # foreign/crowdsourced ECDH report. This is authoritative even
                    # when the server flags a crowdsourced report as isOwnReport=True.
                    is_network_report=public_key_random != b"",
                    name="",
                )
            )
        except UnsupportedCurveError as unsupported_exc:
            # The Sx length maps to no curve; no key can change that. The
            # tracker WARNING needs a canonical ID; without one: DEBUG only.
            _key_neutral_report_count += 1
            if device_key is not None:
                FOREIGN_READING_TRACKER.note_unsupported(
                    device_key, unsupported_exc.sx_len
                )
            _LOGGER.debug(
                "Skipping one foreign report: %s (time_offset=%s)",
                unsupported_exc,
                time_offset,
            )
        except ForeignReportAuthError as foreign_auth_exc:
            if foreign_auth_exc.curve_name == SECP160R1.name and (
                canonic_id is not None
                and FOREIGN_READING_TRACKER.locked_curve(canonic_id) == SECP256R1.name
            ):
                # The resolver locked this device to a P-256 EID. A 20-byte Sx
                # then comes from a finder that read only part of it
                # (MODERN_P256_X20_TRUNC_*); no key can authenticate such a
                # report, so it says nothing about the cached EIK.
                _key_neutral_report_count += 1
                _LOGGER.debug(
                    "Skipping one secp160r1 foreign report of a device locked "
                    "to a P-256 EID (time_offset=%s): %s",
                    time_offset,
                    foreign_auth_exc,
                )
            elif foreign_auth_exc.curve_name == SECP160R1.name:
                # Same meaning as InvalidTag: the report did not authenticate
                # with the cached key candidates.
                _auth_failures += 1
                _LOGGER.debug(
                    "Decryption auth failed for foreign report "
                    "(time_offset=%s, key_len=%s, candidates=%d): %s",
                    time_offset,
                    len(active_identity_key) if active_identity_key else 0,
                    len(all_identity_keys),
                    foreign_auth_exc,
                )
            else:
                _p256_auth_failures += 1
                if (
                    device_key is not None
                    and FOREIGN_READING_TRACKER.preferred(device_key) is not None
                ):
                    # A reading has decrypted this device before, so a miss
                    # with every key and reading is the stale-key signal a
                    # SECP160R1 failure gives.
                    _auth_failures += 1
                    _p256_counted_failures += 1
                else:
                    # Z11: while no reading has worked for the device, a miss
                    # says nothing about the cached key.
                    _key_neutral_report_count += 1
                if device_key is not None:
                    FOREIGN_READING_TRACKER.note_all_failed(
                        device_key,
                        foreign_auth_exc.curve_name,
                        len(public_key_random),
                        foreign_auth_exc.readings_tried,
                        poll_id,
                    )
                # Per report DEBUG only; the thresholded WARNING comes from
                # the foreign-reading tracker, which needs a canonical ID.
                # Without one this report stays at DEBUG.
                _LOGGER.debug(
                    "No provisional reading authenticated one foreign report "
                    "(time_offset=%s, candidates=%d): %s",
                    time_offset,
                    len(all_identity_keys),
                    foreign_auth_exc,
                )
        except ForeignReportStructureError as structure_exc:
            # Short payload or an Sx that is not on the curve: unusable
            # independently of the key, so it is no authentication signal.
            _key_neutral_report_count += 1
            _LOGGER.debug(
                "Skipping one malformed foreign report: %s (time_offset=%s)",
                structure_exc,
                time_offset,
            )
        except InvalidTag:
            # InvalidTag means AES-GCM authentication failed during decryption.
            _auth_failures += 1
            if public_key_random == b"":  # Own-report auth failure
                _own_auth_failures += 1
            # Per-report detail diagnostics only: a single report failing to
            # authenticate is not actionable on its own (a stale own report,
            # an offline device, or a foreign/crowdsourced report keyed to
            # another account all land here). The user-facing verdict and any
            # reauth decision belong to the stateful, sibling-aware callers
            # (coordinator poll verdict / FCM callback), so this stays DEBUG and
            # carries no reauth advice.
            _LOGGER.debug(
                "Decryption auth failed (InvalidTag) for %s report "
                "(time_offset=%s, key_len=%s, candidates=%d): "
                "report could not be authenticated.",
                "own" if public_key_random == b"" else "foreign",
                time_offset if public_key_random != b"" else "N/A",
                len(active_identity_key) if active_identity_key else 0,
                len(all_identity_keys),
            )
        except ValueError as ve:
            # Plain ValueError: bad lengths or data outside the foreign-report
            # taxonomy. Authentication failures are typed (InvalidTag above,
            # ForeignReportAuthError above) and never classified by text.
            _LOGGER.warning(
                "Failed to process one location report (malformed data): %s",
                ve,
            )
        except (AttributeError, KeyError, TypeError) as expected_exc:
            # Expected errors from malformed protobuf data - log at warning level
            _LOGGER.warning(
                "Failed to process one location report (malformed data): %s",
                expected_exc,
            )
        except Exception as unexpected_exc:
            # Unexpected errors indicate bugs or API changes - log with stack trace
            _LOGGER.error(
                "Unexpected error processing location report: %s",
                unexpected_exc,
                exc_info=True,
            )

    if _p256_auth_failures:
        _LOGGER.debug(
            "%d crowdsourced P-256 report(s) failed with every provisional "
            "reading; %d of them counted toward identity key cache "
            "invalidation (a reading had decrypted the device before).",
            _p256_auth_failures,
            _p256_counted_failures,
        )

    # FIX: When ALL encrypted reports fail authentication, the cached EIK is
    # likely stale (e.g., after key rotation, re-pairing, or E2EE reset).
    # Invalidate the cache so the next attempt forces a fresh key derivation.
    # Key-neutral foreign reports are left out of the denominator (CX-5).
    if (
        _auth_failures > 0
        and _encrypted_report_count > 0
        and _auth_failures >= _encrypted_report_count - _key_neutral_report_count
        and raw_encrypted_identity_key
    ):
        owner_ver = getattr(encrypted_user_secrets, "ownerKeyVersion", 0)
        removed = invalidate_eik_cache_for_key(raw_encrypted_identity_key, owner_ver)
        # This aggregate notice stays DEBUG and carries no reauth advice: this
        # function is stateless per call (it sees a single poll cycle, often with
        # just one report) and cannot judge whether the failure is persistent. The
        # cache invalidation above is logged once at INFO; the sibling-aware,
        # cross-cycle reauth verdict belongs to the coordinator
        # (PollingOperations) and the FCM callback, which remain the user-facing
        # owners of any WARNING/ERROR. The own-report mismatch is still surfaced
        # via OwnReportIdentityMismatchError below, so the escalation machinery is
        # untouched.
        if removed:
            _LOGGER.debug(
                "All %d key-relevant encrypted location reports failed authentication; "
                "invalidated %d cached identity key(s) to force re-derivation "
                "on the next poll.",
                _auth_failures,
                removed,
            )
        else:
            _LOGGER.debug(
                "All %d key-relevant encrypted location reports failed authentication "
                "but no cached keys were found to invalidate.",
                _auth_failures,
            )

    # Convert to structured payloads for HA entities (with fail-fast validation).
    # Built BEFORE the own-report-mismatch escalation decision below so that
    # decision can use the SAME validated real-coordinate predicate the callers use
    # (`is_real_location_record`), instead of a premature "decrypted to bytes" flag.
    structured: list[dict[str, Any]] = []
    for loc in wrapped:
        try:
            report_hint = _infer_report_hint(loc.status)  # may be None (conservative)

            try:
                setattr(loc, "pair_date", metadata_update.get("pair_date"))
                setattr(
                    loc,
                    "secrets_creation_date",
                    metadata_update.get("secrets_creation_date"),
                )
                setattr(loc, "identity_key", identity_key_bytes)
                _LOGGER.debug("Injected fresh metadata into location object.")
            except Exception as metadata_exc:  # pragma: no cover - diagnostic safety
                _LOGGER.debug("Failed to inject metadata: %s", metadata_exc)

            if loc.status == common_pb2.Status.SEMANTIC:
                payload: dict[str, Any] = {
                    "latitude": None,
                    "longitude": None,
                    "altitude": None,
                    "accuracy": None,  # No coordinates means no meaningful accuracy
                    "last_seen": loc.time,
                    "status": _status_name_safe(loc.status),
                    "status_code": int(loc.status),
                    "is_own_report": False,
                    "is_network_report": loc.is_network_report,
                    "semantic_name": loc.name,
                    "encrypted_identity_key": raw_encrypted_identity_key,
                    "owner_key_version": raw_owner_key_version,
                    "identity_key": identity_key_bytes,
                    "identity_key_candidates": identity_key_candidate_bytes
                    if identity_key_candidate_bytes
                    else None,
                }
                # Internal hint helps the coordinator schedule throttling-aware cooldowns.
                if report_hint:
                    payload["_report_hint"] = report_hint
            else:
                proto_loc = device_update_pb2.Location()
                try:
                    # Protobuf parsing is relatively cheap → inline
                    proto_loc.ParseFromString(loc.decrypted_location)
                except DecodeError as de:
                    _LOGGER.warning(
                        "Failed to parse Location protobuf; dropping one report: %s", de
                    )
                    continue

                # --- Fail-fast coordinate validation (POPETS'25 §4) -----------------
                # The protocol uses integer-scaled lat/lon (1e7). We validate *after* scaling.
                latitude = proto_loc.latitude / 1e7
                longitude = proto_loc.longitude / 1e7
                if not _is_valid_latlon(latitude, longitude):
                    # Keep the message non-sensitive: do not print raw coordinates.
                    _LOGGER.warning(
                        "Dropping invalid/out-of-bounds coordinates from one report"
                    )
                    continue
                # ---------------------------------------------------------------------

                altitude = proto_loc.altitude

                if _LOGGER.isEnabledFor(logging.DEBUG):
                    _LOGGER.debug(
                        "Parsed valid coordinates (altitude present: %s)",
                        altitude is not None,
                    )
                    maps_link = create_google_maps_link(latitude, longitude)
                    if maps_link:
                        _LOGGER.debug("Google Maps Link: %s", maps_link)

                status_name = _status_name_safe(loc.status)
                payload = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "altitude": altitude,
                    "accuracy": loc.accuracy,
                    "last_seen": loc.time,
                    "status": status_name,
                    "status_code": int(loc.status),
                    "is_own_report": loc.is_own_report,
                    "is_network_report": loc.is_network_report,
                    "semantic_name": None,
                    "encrypted_identity_key": raw_encrypted_identity_key,
                    "owner_key_version": raw_owner_key_version,
                    "identity_key": identity_key_bytes,
                    "identity_key_candidates": identity_key_candidate_bytes,
                }
                if report_hint:
                    payload["_report_hint"] = report_hint

            # [FIX: UNIVERSAL METADATA MERGE]
            # Apply this to ALL payloads, not just semantic ones.
            # This fixes the "Anchor=None" bug for standard GPS updates.
            if metadata_update:
                if (
                    "secretsCreationDate" in metadata_update
                    and "secrets_creation_date" not in metadata_update
                ):
                    metadata_update["secrets_creation_date"] = metadata_update[
                        "secretsCreationDate"
                    ]
                payload.update(metadata_update)

            # Safety net: ensure identity key propagates even if metadata lacks it
            if "identity_key" not in payload and identity_key_bytes:
                payload["identity_key"] = identity_key_bytes

            if metadata:
                payload.update(metadata)

            # Log with timezone-awareness if HA util is available (debug only)
            if _LOGGER.isEnabledFor(logging.DEBUG):
                try:
                    dt_util = import_module("homeassistant.util.dt")

                    ts_local = dt_util.as_local(
                        datetime.datetime.fromtimestamp(loc.time, tz=datetime.UTC)
                    )
                    _LOGGER.debug(
                        "Time (local): %s | Status: %s | Own: %s",
                        ts_local,
                        loc.status,
                        loc.is_own_report,
                    )
                except Exception:
                    _LOGGER.debug(
                        "Time (epoch): %s | Status: %s | Own: %s",
                        loc.time,
                        loc.status,
                        loc.is_own_report,
                    )

            structured.append(payload)
        except Exception as one_exc:
            _LOGGER.debug(
                "Failed to convert one WrappedLocation to structured payload: %s",
                one_exc,
            )

    # Diff-Review #1 / Codex (PR #1153): the device HAS its own encrypted reports
    # and EVERY one failed authentication (no own-report success), so THIS
    # device's server-side own reports predate the cached identity key (they were
    # encrypted under a previous key) -- distinct from
    # foreign/crowdsourced failures (which legitimately fail with other accounts'
    # keys and must NOT escalate). Raise the dedicated OwnReportIdentityMismatchError
    # so the escalation machinery (coordinator poll/locate handlers and the FCM push
    # handler, all gated by per-cycle counting + cooldown) surfaces a reauth repair
    # instead of silently returning empty every cycle -- UNLESS this device already
    # produced a usable foreign/crowdsourced (network) coordinate in this very call.
    # That judgement is made HERE, on the VALIDATED `structured` output, using the
    # exact `is_real_location_record` predicate the callers use to clear the reauth
    # budget: a foreign report that merely decrypted to bytes but then failed
    # protobuf parsing / lat-lon validation (or a SEMANTIC network hit with no
    # coordinate) is NOT a usable fix and must not suppress the escalation. A fresh
    # secrets.json cannot fix stale own reports of an offline/re-paired device
    # anyway, and the cache invalidation above still forces own-key re-derivation on
    # the next poll; when a real network fix exists, suppressing the raise lets that
    # good position flow through the normal return path below. (A sibling device
    # decrypting in the same cycle is handled one layer up, in the sibling-gated
    # coordinator/FCM callers.)
    if (
        _own_encrypted_report_count > 0
        and _own_auth_failures >= _own_encrypted_report_count
        and not _own_report_success
    ):
        # Detect the network fix by CRYPTOGRAPHIC PROVENANCE (decrypted via the
        # foreign/ECDH path), NOT by the server-supplied ``is_own_report`` flag. This
        # integration's own crowdsourced uploader stamps valid network reports with
        # ``isOwnReport=True`` while carrying a non-empty publicKeyRandom
        # (fmdn_finder/location_uploader.py), so keying off ``not is_own_report``
        # would misclassify such a real fix as an own report, fire the raise, and
        # discard the good coordinate. ``is_network_report`` is set from the actual
        # decrypt branch and is immune to that flag.
        has_network_fix = any(
            record.get("is_network_report") and is_real_location_record(record)
            for record in structured
        )
        if not has_network_fix:
            raise OwnReportIdentityMismatchError(
                "All own-report decryptions failed: the server-side own reports "
                "predate the current identity key (they were encrypted under a "
                "previous key, so the current key cannot read them). This clears "
                "once the device uploads a fresh report."
            )

    if not wrapped:
        if action_feedback:
            # Expected for a Play/Stop Sound acknowledgement; at INFO it read
            # like a failed locate after every ring and every stop.
            _LOGGER.debug(
                "[DecryptLocations] Play/Stop Sound acknowledgement push: no "
                "location reports expected."
            )
            _LOGGER.debug("[DecryptLocations] No locations found.")
        else:
            _LOGGER.info("[DecryptLocations] No locations found.")
        # FIX: Merge metadata_update into the returned payload even when no locations.
        # This ensures encrypted_identity_key, secrets_creation_date, owner_key_version,
        # identity_key, etc. are returned for devices (like phones) that have these
        # fields but no location reports yet.
        if metadata or metadata_update:
            metadata_only: dict[str, Any] = {}
            if metadata:
                metadata_only.update(metadata)
            if metadata_update:
                metadata_only.update(metadata_update)
            metadata_only["metadata_only"] = True
            return [metadata_only]
        return []

    return structured


def decrypt_location_response_locations(
    device_update_protobuf: DeviceUpdateProto,
    *,
    cache: TokenCache,
) -> list[dict[str, Any]]:
    """Synchronous legacy facade.

    IMPORTANT:
    - MUST NOT be called from inside Home Assistant's running event loop.
    - Prefer: `await async_decrypt_location_response_locations(...)`.

    Implementation:
    - Runs the async implementation via `asyncio.run` only if no loop is running
      in this thread. Otherwise, raises a clear RuntimeError.
    """
    try:
        asyncio.get_running_loop()  # raises RuntimeError if no loop in this thread
    except RuntimeError:
        # No running loop in this thread → safe to use asyncio.run
        return asyncio.run(
            async_decrypt_location_response_locations(
                device_update_protobuf, cache=cache
            )
        )
    else:
        # A loop is running in this thread → don't deadlock
        raise RuntimeError(
            "Sync decrypt_location_response_locations() used inside a running event loop. "
            "Use `await async_decrypt_location_response_locations(...)` instead."
        )


if __name__ == "__main__":  # Developer self-check only; not used by Home Assistant
    res = parse_device_update_protobuf("")
    try:
        decrypt_location_response_locations(res, cache=cast("TokenCache", None))
    except Exception as exc:
        print(f"Self-check encountered exception (expected outside HA runtime): {exc}")
