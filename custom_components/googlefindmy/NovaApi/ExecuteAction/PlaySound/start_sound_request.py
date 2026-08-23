# custom_components/googlefindmy/NovaApi/ExecuteAction/PlaySound/start_sound_request.py
#
#  GoogleFindMyTools - Tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger.
#
from __future__ import annotations

import asyncio
from typing import Optional

import aiohttp
from aiohttp import ClientSession

from custom_components.googlefindmy.NovaApi.ExecuteAction.PlaySound.sound_request import (
    create_sound_request,
)
from custom_components.googlefindmy.NovaApi.nova_request import (
    async_nova_request,
    NovaAuthError,
    NovaRateLimitError,
    NovaHTTPError,
)
from custom_components.googlefindmy.NovaApi.scopes import NOVA_ACTION_API_SCOPE
from custom_components.googlefindmy.NovaApi.util import generate_random_uuid
from custom_components.googlefindmy.example_data_provider import get_example_data


def start_sound_request(canonic_device_id: str, gcm_registration_id: str) -> tuple[str, str]:
    """Build the hex payload for a 'Play Sound' action (pure builder).

    This function performs no network I/O. It exists for backwards
    compatibility with code paths that submit the payload themselves.

    Args:
        canonic_device_id: The canonical ID of the target device.
        gcm_registration_id: The FCM registration token for push notifications.

    Returns:
        A tuple of (hex_payload, request_uuid) where:
        - hex_payload: Hex-encoded protobuf payload for Nova transport
        - request_uuid: The UUID used for this request (needed to cancel it later)
    """
    request_uuid = generate_random_uuid()
    hex_payload = create_sound_request(True, canonic_device_id, gcm_registration_id, request_uuid)
    return (hex_payload, request_uuid)


async def async_submit_start_sound_request(
    canonic_device_id: str,
    gcm_registration_id: str,
    *,
    session: Optional[ClientSession] = None,
    cache: Optional[any] = None,
) -> Optional[tuple[str, str]]:
    """Submit a 'Play Sound' action using the shared async Nova client.

    This function handles the network request and robustly catches common API
    and network errors, returning None in those cases to prevent crashes.

    Args:
        canonic_device_id: The canonical ID of the target device.
        gcm_registration_id: The FCM registration token for push notifications.
        session: (Deprecated) The aiohttp ClientSession. No longer used as the
                 nova client handles session management internally.

    Returns:
        A tuple of (response_hex, request_uuid) on success, or None on any handled error.
        The request_uuid should be stored and used when calling Stop Sound to properly
        cancel this specific Play Sound request.
    """
    hex_payload, request_uuid = start_sound_request(canonic_device_id, gcm_registration_id)
    try:
        # The async Nova client manages session reuse internally; do not pass session through.
        response_hex = await async_nova_request(NOVA_ACTION_API_SCOPE, hex_payload, cache=cache)
        return (response_hex, request_uuid) if response_hex is not None else None
    except asyncio.CancelledError:
        raise
    except NovaRateLimitError as e:
        # transient; caller should treat as soft-fail
        return None
    except NovaHTTPError as e:
        # transient server-side; caller should treat as soft-fail
        return None
    except NovaAuthError as e:
        # auth required; caller may trigger re-auth UX
        return None
    except aiohttp.ClientError:
        # local/network problem
        return None


if __name__ == "__main__":
    # CLI helper (non-HA): obtain a token synchronously and submit via asyncio once.
    async def _main():
        """Run a test execution of the start sound request for development."""
        from custom_components.googlefindmy.Auth.fcm_receiver import FcmReceiver  # sync-only CLI variant

        sample_canonic_device_id = get_example_data("sample_canonic_device_id")
        fcm_token = FcmReceiver().register_for_location_updates(lambda x: None)

        await async_submit_start_sound_request(sample_canonic_device_id, fcm_token)

    asyncio.run(_main())
