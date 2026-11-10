# custom_components/googlefindmy/SpotApi/GetEidInfoForE2eeDevices/get_eid_info_request.py
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
"""
GetEidInfoForE2eeDevices request helpers (async-first).

This module exposes:
- `async_get_eid_info()`: primary non-blocking API used by the integration.
- `get_eid_info()`: guarded sync wrapper for CLI/testing only. It fails fast
  if invoked from the Home Assistant event loop.

Behavior
--------
- Builds the protobuf request (ownerKeyVersion = -1 to request the latest key).
- Calls the SPOT endpoint via the Spot API request helper.
- Raises `SpotApiEmptyResponseError` on trailers-only/empty bodies for clear
  and actionable error handling upstream.
- Parses the protobuf response defensively and logs concise diagnostics.

Notes
-----
- The async path prefers `async_spot_request()` if available. If the legacy
  synchronous `spot_request()` is the only option, it is executed safely in
  a worker thread to avoid blocking the event loop.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, cast

from custom_components.googlefindmy.ProtoDecoders import Common_pb2, DeviceUpdate_pb2
from custom_components.googlefindmy.SpotApi import spot_request as spot_request_module
from custom_components.googlefindmy.SpotApi.spot_request import (
    SpotAuthPermanentError,
    SpotError,
    SpotTrailersOnlyError,
)
from custom_components.googlefindmy.unknown_field_tree import log_unknown_fields
from google.protobuf.message import DecodeError  # parse-time error type for protobufs

if TYPE_CHECKING:
    from custom_components.googlefindmy.Auth.token_cache import TokenCache

_LOGGER = logging.getLogger(__name__)


class SpotApiEmptyResponseError(RuntimeError):
    """Raised when a SPOT API call returns an empty body where one was expected."""


def _classify_body(body: bytes) -> str:
    """Name the content class of an undecodable response body for the log.

    Decided on the first non-whitespace byte: an HTML error page (`<`), a JSON
    error (`{` or `[`), nothing at all, or binary. This is the diagnostic the
    warning used to smuggle in as a 16-byte hex prefix; the class carries it
    without the payload. Takes the whole body so that no slice of it appears
    in the log call (`tests/test_guard_logging_payloads.py`).
    """
    stripped = body.lstrip()
    if not stripped:
        return "empty"
    if stripped[:1] == b"<":
        return "html"
    if stripped[:1] in (b"{", b"["):
        return "json"
    return "binary"


def _build_request_bytes() -> bytes:
    """Build and serialize the GetEidInfoForE2eeDevices protobuf request.

    Returns:
        Serialized request bytes suitable for the Spot API.
    """
    req = Common_pb2.GetEidInfoForE2eeDevicesRequest()
    # API convention: -1 means "latest available owner key"
    req.ownerKeyVersion = -1
    req.hasOwnerKeyVersion = True
    return req.SerializeToString()


async def _spot_call_async(scope: str, payload: bytes, *, cache: TokenCache) -> bytes:
    """Call the Spot API asynchronously.

    Prefer an async helper if available; otherwise run the sync helper in an executor.

    Args:
        scope: Spot API method name (e.g., "GetEidInfoForE2eeDevices").
        payload: Serialized protobuf request.

    Returns:
        Raw response bytes.

    Raises:
        SpotApiEmptyResponseError: if trailers-only/invalid body is indicated.
        RuntimeError: on other underlying request errors.
    """
    # Try native async helper first
    async_helper = getattr(spot_request_module, "async_spot_request", None)

    if callable(async_helper):
        try:
            response_bytes = await async_helper(scope, payload, cache=cache)
            return cast(bytes, response_bytes)
        except SpotTrailersOnlyError as e:
            # Map to integration-level error expected by callers
            raise SpotApiEmptyResponseError(
                "Empty gRPC body (trailers-only) for GetEidInfoForE2eeDevices"
            ) from e
        except TypeError as exc:
            raise RuntimeError(
                "async_spot_request() implementation is outdated; missing cache= support."
            ) from exc

    loop = asyncio.get_running_loop()

    def _call_sync() -> bytes:
        return spot_request_module.spot_request(scope, payload)

    try:
        response_bytes = await loop.run_in_executor(None, _call_sync)
    except SpotAuthPermanentError:
        # Re-raise auth errors so they propagate to trigger reauth flow
        raise
    except SpotTrailersOnlyError as e:
        raise SpotApiEmptyResponseError(
            "Empty gRPC body (trailers-only) for GetEidInfoForE2eeDevices"
        ) from e
    except SpotError:
        # Type-preserving re-raise (R8): a typed SpotError (network/gRPC status)
        # must NOT be flattened into a bare RuntimeError, or the typed-transient
        # discriminator is erased through this defensive path (Q5 invariant).
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError("Synchronous SPOT request helper failed.") from exc

    if isinstance(response_bytes, bytearray):
        return bytes(response_bytes)
    if not isinstance(response_bytes, bytes):
        raise RuntimeError(
            "Synchronous SPOT request helper returned a non-bytes response."
        )

    return response_bytes


async def async_get_eid_info(
    *, cache: TokenCache
) -> DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse:
    """Fetch and parse EID info for E2EE devices (async, preferred).

    Args:
        cache: Entry-scoped TokenCache used for SPOT authentication and token reuse.

    Returns:
        Parsed `GetEidInfoForE2eeDevicesResponse` protobuf message.

    Raises:
        SpotApiEmptyResponseError: if the response body is empty (e.g., trailers-only).
        DecodeError: if the protobuf payload cannot be parsed.
        RuntimeError: for lower-level Spot API request failures or missing helpers.
    """
    serialized_request = _build_request_bytes()
    response_bytes = await _spot_call_async(
        "GetEidInfoForE2eeDevices", serialized_request, cache=cache
    )

    # Defensive checks + diagnostics for trailers-only / empty payloads (sync-path fallback)
    if not response_bytes:
        _LOGGER.warning(
            "GetEidInfoForE2eeDevices: empty/none response (len=0, kind=empty). "
            "This often indicates an authentication issue (trailers-only with grpc-status!=0). "
            "If this persists after a token refresh, please re-authenticate your Google account."
        )
        raise SpotApiEmptyResponseError(
            "Empty gRPC body (possibly trailers-only) for GetEidInfoForE2eeDevices"
        )

    eid_info = DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse()
    try:
        eid_info.ParseFromString(response_bytes)
    except DecodeError:
        # Minimal, high-signal context: length and content class, never the
        # bytes themselves (AGENTS.md section 5: raw API payloads are not logged).
        _LOGGER.warning(
            "GetEidInfoForE2eeDevices: protobuf DecodeError (len=%s, kind=%s). "
            "This may indicate a truncated/corrupted gRPC response or a server-side format change. "
            "If this persists, try re-authenticating.",
            len(response_bytes),
            _classify_body(response_bytes),
        )
        raise

    # Account-wide response: the kind alone keys the comparison.
    log_unknown_fields("eid_response", "", "account", [eid_info])
    return eid_info


def get_eid_info() -> DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse:
    """Synchronous helper for CLI/testing only (NOT for use inside HA's event loop).

    Raises:
        RuntimeError: if called from a running event loop.
        SpotApiEmptyResponseError, DecodeError: see `async_get_eid_info`.
    """
    # Fail-fast if used in an event loop
    try:
        loop = asyncio.get_running_loop()
        if loop.is_running():
            raise RuntimeError(
                "Sync get_eid_info() called from within the event loop. "
                "Use `await async_get_eid_info()` instead."
            )
    except RuntimeError:
        # No running loop -> OK for CLI usage
        pass

    serialized_request = _build_request_bytes()
    response_bytes = spot_request_module.spot_request(
        "GetEidInfoForE2eeDevices", serialized_request
    )

    if not response_bytes:
        _LOGGER.warning(
            "GetEidInfoForE2eeDevices: empty/none response (len=0, kind=empty). "
            "This often indicates an authentication issue (trailers-only with grpc-status!=0). "
            "If this persists after a token refresh, please re-authenticate your Google account."
        )
        raise SpotApiEmptyResponseError(
            "Empty gRPC body (possibly trailers-only) for GetEidInfoForE2eeDevices"
        )

    eid_info = DeviceUpdate_pb2.GetEidInfoForE2eeDevicesResponse()
    try:
        eid_info.ParseFromString(response_bytes)
    except DecodeError:
        _LOGGER.warning(
            "GetEidInfoForE2eeDevices: protobuf DecodeError (len=%s, kind=%s). "
            "This may indicate a truncated/corrupted gRPC response or a server-side format change. "
            "If this persists, try re-authenticating.",
            len(response_bytes),
            _classify_body(response_bytes),
        )
        raise

    return eid_info


if __name__ == "__main__":
    # CLI/dev convenience: print whether owner-key metadata is present
    info = get_eid_info()
    print(getattr(info, "encryptedOwnerKeyAndMetadata", None))
