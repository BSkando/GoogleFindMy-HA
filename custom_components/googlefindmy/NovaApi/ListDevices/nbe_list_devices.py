#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.

from __future__ import annotations

import asyncio
import binascii
import logging
from typing import Optional

from aiohttp import ClientSession

from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.location_request import (
    get_location_data_for_device,
)
from custom_components.googlefindmy.NovaApi.nova_request import async_nova_request
from custom_components.googlefindmy.NovaApi.scopes import NOVA_LIST_DEVICES_API_SCOPE
from custom_components.googlefindmy.NovaApi.util import generate_random_uuid
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2
from custom_components.googlefindmy.ProtoDecoders.decoder import (
    parse_device_list_protobuf,
    get_canonic_ids,
)
from custom_components.googlefindmy.SpotApi.CreateBleDevice.create_ble_device import (
    register_esp32,
)
from custom_components.googlefindmy.SpotApi.UploadPrecomputedPublicKeyIds.upload_precomputed_public_key_ids import (
    refresh_custom_trackers,
)

_LOGGER = logging.getLogger(__name__)


def create_device_list_request() -> str:
    """Build the protobuf request and return it as a hex string (transport payload)."""
    wrapper = DeviceUpdate_pb2.DevicesListRequest()

    # Query for Spot devices only (keeps payload lean).
    wrapper.deviceListRequestPayload.type = DeviceUpdate_pb2.DeviceType.SPOT_DEVICE

    # Assign a random UUID as request id to help server-side correlation.
    wrapper.deviceListRequestPayload.id = generate_random_uuid()

    # Serialize to bytes and hex-encode for Nova transport.
    binary_payload = wrapper.SerializeToString()
    hex_payload = binascii.hexlify(binary_payload).decode("utf-8")
    return hex_payload


async def async_request_device_list(
    username: Optional[str] = None,
    *,
    session: Optional[ClientSession] = None,
) -> str:
    """Asynchronously request the device list via Nova.

    Priority of HTTP session (HA best practice):
    1) Explicit `session` argument (tests/special cases),
    2) Registered provider from nova_request (uses HA's async_get_clientsession),
    3) Short-lived fallback session managed by nova_request (DEBUG only).

    Returns:
        Hex-encoded Nova response payload.

    Raises:
        RuntimeError / aiohttp.ClientError on transport failures.
    """
    hex_payload = create_device_list_request()
    # Delegate HTTP to Nova client (handles session provider & timeouts).
    return await async_nova_request(
        NOVA_LIST_DEVICES_API_SCOPE,
        hex_payload,
        username=username,
        session=session,
    )


def request_device_list() -> str:
    """Synchronous convenience wrapper for CLI/legacy callers.

    NOTE:
    - This wrapper spins a private event loop via `asyncio.run(...)`.
    - Do NOT call from inside an active event loop (will raise RuntimeError).
    - In Home Assistant, prefer `async_request_device_list(...)` and await it.
    """
    try:
        return asyncio.run(async_request_device_list())
    except RuntimeError as err:
        # This indicates incorrect usage (called from within a running loop).
        _LOGGER.error(
            "request_device_list() must not be called inside an active event loop. "
            "Use async_request_device_list(...) instead. Error: %s",
            err,
        )
        raise


# ------------------------------ CLI helper ---------------------------------
async def _async_cli_main() -> None:
    """Asynchronous main function for the CLI experience (single event loop)."""
    print("Loading...")
    result_hex = await async_request_device_list()

    device_list = parse_device_list_protobuf(result_hex)

    # Maintain side-effect helpers for Spot custom trackers.
    refresh_custom_trackers(device_list)
    canonic_ids = get_canonic_ids(device_list)

    print("")
    print("-" * 50)
    print("Welcome to GoogleFindMyTools!")
    print("-" * 50)
    print("")
    print("The following trackers are available:")

    for idx, (device_name, canonic_id) in enumerate(canonic_ids, start=1):
        print(f"{idx}. {device_name}: {canonic_id}")

    selected_value = input(
        "\nIf you want to see locations of a tracker, type the number of the tracker and press 'Enter'.\n"
        "If you want to register a new ESP32- or Zephyr-based tracker, type 'r' and press 'Enter': "
    )

    if selected_value == "r":
        print("Loading...")
        # Run potential blocking/IO work in a worker thread to avoid blocking the loop.
        await asyncio.to_thread(register_esp32)
    else:
        selected_idx = int(selected_value) - 1
        selected_device_name = canonic_ids[selected_idx][0]
        selected_canonic_id = canonic_ids[selected_idx][1]

        print("Fetching location...")
        await get_location_data_for_device(selected_canonic_id, selected_device_name)


if __name__ == "__main__":
    try:
        asyncio.run(_async_cli_main())
    except KeyboardInterrupt:
        print("\nExiting.")
