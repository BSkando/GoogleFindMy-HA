# custom_components/googlefindmy/NovaApi/ExecuteAction/PlaySound/stop_sound_request.py
#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#
"""Handles sending a 'Stop Sound' command for a Google Find My Device."""
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
from custom_components.googlefindmy.example_data_provider import get_example_data


def stop_sound_request(canonic_device_id: str, gcm_registration_id: str, request_uuid: Optional[str] = None) -> str:
    """Build the hex payload for a 'Stop Sound' action (pure builder).

    This function performs no network I/O. It creates the serialized protobuf
    message required to stop a sound on a device.

    Args:
        canonic_device_id: The canonical ID of the target device.
        gcm_registration_id: The FCM registration token for push notifications.
        request_uuid: Optional UUID to cancel a specific Play Sound request.
                     If not provided, a new UUID is generated (may not properly cancel the sound).

    Returns:
        Hex-encoded protobuf payload for Nova transport.
    """
    return create_sound_request(False, canonic_device_id, gcm_registration_id, request_uuid)


async def async_submit_stop_sound_request(
    canonic_device_id: str,
    gcm_registration_id: str,
    *,
    request_uuid: Optional[str] = None,
    session: Optional[ClientSession] = None,
    cache: Optional[any] = None,
) -> Optional[str]:
    """Submit a 'Stop Sound' action using the shared async Nova client.

    This function handles the network request and robustly catches common API
    and network errors, returning None in those cases to prevent crashes.

    Args:
        canonic_device_id: The canonical ID of the target device.
        gcm_registration_id: The FCM registration token for push notifications.
        request_uuid: Optional UUID to cancel a specific Play Sound request.
                     If not provided, a new UUID is generated (may not properly cancel the sound).
        session: (Deprecated) The aiohttp ClientSession. No longer used as the
                 nova client handles session management internally.

    Returns:
        A hex string of the response payload on success (can be empty),
        or None on any handled error (e.g., auth, rate-limit, server error,
        or network issues).
    """
    hex_payload = stop_sound_request(canonic_device_id, gcm_registration_id, request_uuid)
    try:
        # The async Nova client manages session reuse internally; do not pass session through.
        return await async_nova_request(NOVA_ACTION_API_SCOPE, hex_payload, cache=cache)
    except asyncio.CancelledError:
        raise
    except NovaRateLimitError:
        # transient; caller should treat as soft-fail
        return None
    except NovaHTTPError:
        # transient server-side; caller should treat as soft-fail
        return None
    except NovaAuthError:
        # auth required; caller may trigger re-auth UX
        return None
    except aiohttp.ClientError:
        # local/network problem
        return None


if __name__ == "__main__":
    # This block serves as a CLI helper for standalone testing and development.
    # It obtains an FCM token synchronously and then runs the async submission
    # function in a new event loop.
    async def _main():
        """Run a test execution of the stop sound request for development."""
        from custom_components.googlefindmy.Auth.fcm_receiver import FcmReceiver  # sync-only CLI variant

        sample_canonic_device_id = get_example_data("sample_canonic_device_id")
        fcm_token = FcmReceiver().register_for_location_updates(lambda x: None)

        await async_submit_stop_sound_request(sample_canonic_device_id, fcm_token)

    asyncio.run(_main())
