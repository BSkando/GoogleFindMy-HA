#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

import logging
import httpx

from custom_components.googlefindmy.Auth.spot_token_retrieval import get_spot_token
from custom_components.googlefindmy.Auth.username_provider import get_username
from custom_components.googlefindmy.SpotApi.grpc_parser import GrpcParser
from custom_components.googlefindmy.Auth.adm_token_retrieval import get_adm_token
from custom_components.googlefindmy.Auth.token_cache import set_cached_value, get_cached_value, get_all_cached_values

_LOGGER = logging.getLogger(__name__)


def _beautify_text(resp):
    """Best-effort body-to-text for diagnostics (HTML/JSON error pages)."""
    try:
        from bs4 import BeautifulSoup  # lazy import, optional
        return BeautifulSoup(resp.text, "html.parser").get_text()
    except Exception:
        return (resp.content or b"")[:256]


def _pick_auth_token(prefer_adm: bool = False):
    """
    Select a valid auth token. Prefer SPOT unless prefer_adm=True.
    Returns (token, kind, token_owner_username).

    NOTE (auth routing):
    - We first try SPOT for the current user (unless prefer_adm=True).
    - If that fails or prefer_adm=True, we try ADM for *the same* user.
    - As a last resort, we may fall back to any cached ADM token from other users.
      In that case, the returned username will be the token owner, so that any
      subsequent invalidation targets the correct account.
    """
    original_username = get_username()

    # Try SPOT first unless explicitly preferring ADM
    if not prefer_adm:
        try:
            tok = get_spot_token(original_username)
            return tok, "spot", original_username
        except Exception as e:
            _LOGGER.debug("Failed to get SPOT token for %s: %s; falling back to ADM", original_username, e)

    # Try ADM for the same user first (deterministic)
    tok = get_cached_value(f"adm_token_{original_username}")
    if not tok:
        try:
            tok = get_adm_token(original_username)
        except Exception:
            tok = None
    if tok:
        return tok, "adm", original_username

    # Fallback: any cached ADM token (multi-account) — last resort
    for key, value in (get_all_cached_values() or {}).items():
        if key.startswith("adm_token_") and "@" in key and value:
            fallback_username = key.replace("adm_token_", "")
            _LOGGER.debug("Using ADM token from cache for %s (fallback)", fallback_username)
            return value, "adm", fallback_username

    # No token available for any route
    raise RuntimeError("No valid SPOT/ADM token available")


def _invalidate_token(kind: str, username: str):
    """Invalidate cached tokens to force re-auth on next call (scoped to the *token owner's* username)."""
    if kind == "adm":
        set_cached_value(f"adm_token_{username}", None)
    elif kind == "spot":
        # IMPORTANT: also drop the cached SPOT access token itself
        set_cached_value(f"spot_token_{username}", None)
        # Drop AAS so that the SPOT flow regenerates from its root credentials if needed
        set_cached_value("aas_token", None)


def spot_request(api_scope: str, payload: bytes) -> bytes:
    """
    Perform a SPOT gRPC unary request over HTTP/2.

    ### Responsibilities of this function
    - Enforce HTTP/2 + TE: trailers (required by gRPC).
    - Send framed request (5-byte gRPC prefix).
    - Handle three server patterns:
      (1) 200 + data frame(s)  -> extract and return the uncompressed payload.
      (2) 200 + trailers-only  -> no DATA frames; read grpc-status/message and log appropriately.
      (3) Non-200 HTTP         -> log diagnostics and raise.
    - Keep return type stable for callers: bytes or empty bytes on trailers-only/invalid 200 bodies.
    - On persistent AuthN/AuthZ failure (gRPC 16/7) after a retry, raise to avoid silent failure.
    """
    url = "https://spot-pa.googleapis.com/google.internal.spot.v1.SpotService/" + api_scope

    # Ensure HTTP/2 support is available (httpx[http2] -> h2)
    try:
        import h2  # noqa: F401
    except Exception as e:
        raise RuntimeError(
            "HTTP/2 support is required for SPOT gRPC. Please install the HTTP/2 extra: pip install 'httpx[http2]'"
        ) from e

    # Build framed payload early
    grpc_body = GrpcParser.construct_grpc(payload)

    # HTTP/2 + TE: trailers are required by gRPC; httpx provides HTTP/2 support.
    attempts = 0
    prefer_adm = False  # If first try with SPOT hits AuthN/AuthZ error, switch to ADM on retry.

    # --- Networking: reuse a single HTTP/2 client for both attempts (perf + connection reuse) ---
    with httpx.Client(http2=True, timeout=30.0) as client:
        while attempts < 2:
            token, kind, token_user = _pick_auth_token(prefer_adm=prefer_adm)

            headers = {
                "User-Agent": "com.google.android.gms/244433022 grpc-java-cronet/1.69.0-SNAPSHOT",
                "Content-Type": "application/grpc",
                "Te": "trailers",  # required by gRPC over HTTP/2
                "Authorization": "Bearer " + token,
                "Grpc-Accept-Encoding": "gzip",
            }

            # --- HTTP/2 request (gRPC requires trailers over HTTP/2) ---
            resp = client.post(url, headers=headers, content=grpc_body)

            status = resp.status_code
            ctype = resp.headers.get("Content-Type")
            clen = len(resp.content or b"")
            _LOGGER.debug("SPOT %s: HTTP %s, ctype=%s, len=%d", api_scope, status, ctype, clen)

            # --- (1) Happy path: 200 + valid gRPC message frame ---
            if status == 200 and clen >= 5 and resp.content[0] in (0, 1):
                return GrpcParser.extract_grpc_payload(resp.content)

            # --- (2) Trailer-only / invalid-body handling (HTTP 200 without a usable frame) ---
            grpc_status = resp.headers.get("grpc-status")
            grpc_msg = resp.headers.get("grpc-message")

            if status == 200:
                # 2a) Explicit gRPC status in trailers (no data frames)
                if grpc_status and grpc_status != "0":
                    code_name = {"16": "UNAUTHENTICATED", "7": "PERMISSION_DENIED"}.get(grpc_status, "NON_OK")

                    if grpc_status in ("16", "7"):
                        # AuthN/AuthZ failures: log ERROR, invalidate token, and retry once.
                        _LOGGER.error(
                            "SPOT %s trailers-only error: grpc-status=%s (%s), msg=%s",
                            api_scope, grpc_status, code_name, grpc_msg
                        )
                        if attempts == 0:
                            _invalidate_token(kind, token_user)
                            attempts += 1
                            # Switch to ADM on retry if the first attempt used SPOT.
                            prefer_adm = (kind == "spot")
                            continue
                        # Second consecutive AuthN/AuthZ failure → raise to avoid silent failure.
                        raise RuntimeError(f"Spot API authentication failed after retry ({code_name})")

                    # Other non-OK gRPC statuses: warn and keep bytes contract.
                    _LOGGER.warning(
                        "SPOT %s trailers-only non-OK: grpc-status=%s (%s), msg=%s",
                        api_scope, grpc_status, code_name, grpc_msg
                    )
                    return b""

                # 2b) No grpc-status, but body is empty or not a valid frame: ambiguous trailers-only/protocol quirk.
                if (ctype or "").startswith("application/grpc") and clen == 0:
                    # For critical RPCs, an empty body prevents decryption -> log as ERROR.
                    critical_methods = {"GetEidInfoForE2eeDevices"}
                    if api_scope in critical_methods:
                        _LOGGER.error(
                            "SPOT %s: HTTP 200 with empty gRPC body (likely trailers-only or missing response). "
                            "This will prevent E2EE key retrieval and decryption.",
                            api_scope,
                        )
                    else:
                        _LOGGER.warning(
                            "SPOT %s: HTTP 200 with empty gRPC body (possible trailers-only OK or missing response).",
                            api_scope,
                        )
                    return b""

                # 2c) 200 but no usable frame; log a small snippet for diagnostics and keep bytes contract.
                snippet = (resp.content or b"")[:128]
                _LOGGER.debug("SPOT %s invalid 200 body (no frame). Snippet=%r", api_scope, snippet)
                return b""

            # --- (3) Non-200 HTTP responses (retry once on common auth HTTP codes) ---
            if status in (401, 403) and attempts == 0:
                _LOGGER.debug("SPOT %s: %s, invalidating %s token for %s and retrying",
                              api_scope, status, kind, token_user)
                _invalidate_token(kind, token_user)
                attempts += 1
                # If the first attempt used SPOT and got 401/403, prefer ADM on retry.
                prefer_adm = (kind == "spot")
                continue

            # Other HTTP errors: include a brief body for debugging and raise.
            pretty = _beautify_text(resp)
            _LOGGER.debug("SPOT %s HTTP error body: %r", api_scope, pretty)
            raise RuntimeError(f"Spot API HTTP {status} for {api_scope}")

    raise RuntimeError("Spot request failed after retries")
