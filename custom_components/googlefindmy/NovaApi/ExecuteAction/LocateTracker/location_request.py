#
#  GoogleFindMyTools - A set of tools to interact with the Google Find My API
#  Copyright © 2024 Leon Böttger. All rights reserved.
#

import asyncio
import time
import logging
import traceback

# Import FcmReceiver lazily to avoid protobuf conflicts
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import decrypt_location_response_locations
from custom_components.googlefindmy.NovaApi.ExecuteAction.nbe_execute_action import create_action_request, serialize_action_request
from custom_components.googlefindmy.NovaApi.nova_request import async_nova_request
from custom_components.googlefindmy.NovaApi.scopes import NOVA_ACTION_API_SCOPE
from custom_components.googlefindmy.NovaApi.util import generate_random_uuid
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2
from custom_components.googlefindmy.ProtoDecoders.decoder import parse_device_update_protobuf
from custom_components.googlefindmy.example_data_provider import get_example_data

def create_location_request(canonic_device_id, fcm_registration_id, request_uuid):

    action_request = create_action_request(canonic_device_id, fcm_registration_id, request_uuid=request_uuid)

    # Use a current timestamp; server treats this as an arbitrary marker.
    action_request.action.locateTracker.lastHighTrafficEnablingTime.seconds = int(time.time())
    action_request.action.locateTracker.contributorType = DeviceUpdate_pb2.SpotContributorType.FMDN_ALL_LOCATIONS

    # Convert to hex string
    hex_payload = serialize_action_request(action_request)

    return hex_payload


async def get_location_data_for_device(canonic_device_id, name):
    """Get location data for device - HA-compatible async version."""
    
    logger = logging.getLogger(__name__)
    logger.info(f"GoogleFindMyTools: Requesting location data for {name}...")

    fcm_receiver = None
    registered = False

    # --- FIX: Replace polling/racy dict with asyncio.Event to avoid race conditions ---
    fcm_response_event = asyncio.Event()
    fcm_response_data = {"data": None}  # stable container shared with callback

    try:
        # Generate request UUID
        request_uuid = generate_random_uuid()

        # Set up FCM receiver with callback (following original pattern)
        def location_callback(response_canonic_id, hex_response):
            nonlocal fcm_response_data  # ensure we update the outer variable (no rebinding bugs)
            try:
                logger.info(f"FCM callback triggered for {name}, processing response...")
                logger.debug(f"FCM response length: {len(hex_response)} chars")
                
                # Import functions inside callback for thread safety / avoiding import races with protobuf in HA
                try:
                    from custom_components.googlefindmy.ProtoDecoders.decoder import parse_device_update_protobuf
                    from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
                        decrypt_location_response_locations,
                        DecryptionError,
                        StaleOwnerKeyError,
                    )
                    from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
                        SpotApiEmptyResponseError,
                    )
                except ImportError as import_error:
                    logger.error(f"Failed to import decoder functions in callback for {name}: {import_error}")
                    # NEW: wake the waiter immediately on critical import failure
                    fcm_response_data["data"] = []
                    fcm_response_event.set()
                    return
                
                # Parse the hex response
                try:
                    device_update = parse_device_update_protobuf(hex_response)
                except Exception as parse_exc:
                    logger.error(f"Failed to parse device update in callback for {name}: {parse_exc}")
                    # NEW: wake the waiter immediately on parsing failure
                    fcm_response_data["data"] = []
                    fcm_response_event.set()
                    return
                
                # Validate canonic_id matches what we requested
                if response_canonic_id != canonic_device_id:
                    logger.warning(f"FCM callback received data for device {response_canonic_id}, but we requested {canonic_device_id}. Ignoring.")
                    return

                # Decrypt the location data
                try:
                    location_data = decrypt_location_response_locations(device_update)
                except StaleOwnerKeyError as stale_exc:
                    logger.error(f"Decryption failed due to stale owner key for {name}: {stale_exc}")
                    fcm_response_data["data"] = []
                    fcm_response_event.set()
                    return
                except DecryptionError as dec_exc:
                    logger.error(f"Decryption failed for {name}: {dec_exc}")
                    fcm_response_data["data"] = []
                    fcm_response_event.set()
                    return
                except SpotApiEmptyResponseError as auth_exc:
                    logger.error(f"E2EE metadata unavailable (trailers-only/auth) for {name}: {auth_exc}")
                    fcm_response_data["data"] = []
                    fcm_response_event.set()
                    return
                except Exception as dec_generic:
                    logger.error(f"Unexpected error while decrypting location for {name}: {dec_generic}")
                    fcm_response_data["data"] = []
                    fcm_response_event.set()
                    return

                if location_data:
                    logger.info(f"Successfully decrypted {len(location_data)} location records for {name}")
                    # Add canonic_id for later reference and validation
                    location_data[0]["canonic_id"] = response_canonic_id
                    fcm_response_data["data"] = location_data
                    fcm_response_event.set()
                    logger.info(f"Successfully processed location data for {name}")
                else:
                    logger.warning(f"No location data found after decryption for {name}")
                    fcm_response_data["data"] = []
                    fcm_response_event.set()

            except Exception as callback_error:
                logger.error(f"Error processing FCM callback for {name}: {callback_error}")
                logger.debug(f"FCM callback traceback: {traceback.format_exc()}")
                fcm_response_data["data"] = []
                fcm_response_event.set()

        # Get HA-compatible FCM receiver and register for updates
        try:
            from custom_components.googlefindmy.Auth.fcm_receiver_ha import FcmReceiverHA
            fcm_receiver = FcmReceiverHA()
            
            logger.debug(f"Initializing FCM receiver for {name}...")
            # Initialize async
            if not await fcm_receiver.async_initialize():
                logger.error(f"Failed to initialize FCM receiver for {name}")
                return []
            
            logger.debug(f"FCM receiver initialized, registering for location updates for {name}...")
            # Register for location updates with device-specific callback
            fcm_token = await fcm_receiver.async_register_for_location_updates(canonic_device_id, location_callback)
            if not fcm_token:
                logger.error(f"Failed to get FCM token for {name}")
                return []
            registered = True
            
            logger.debug(f"FCM token obtained for {name}: {fcm_token[:20]}...")
                
        except Exception as fcm_error:
            logger.error(f"FCM setup failed for {name}: {fcm_error}")
            logger.debug(f"FCM setup traceback: {traceback.format_exc()}")
            return []

        # Create location request payload
        hex_payload = create_location_request(canonic_device_id, fcm_token, request_uuid)

        # Send location request to Google API
        logger.info(f"Sending location request to Google API for {name}...")
        nova_result = await async_nova_request(NOVA_ACTION_API_SCOPE, hex_payload)

        # NOTE: For this RPC the server often returns HTTP 200 with empty body (FCM delivers the data).
        # Treat None as "accepted" and proceed to wait for FCM; do not bail out early.
        if nova_result is None:
            logger.info(f"Location request accepted by Google for {name}; awaiting FCM data...")
        else:
            logger.info(f"Location request accepted by Google for {name} (response length: {len(nova_result)} chars)")
        
        # --- FIX: Wait efficiently for the callback to signal completion via Event (no polling, no rebinding) ---
        timeout = 60  # seconds
        logger.info(f"Waiting for location response for {name}...")
        try:
            await asyncio.wait_for(fcm_response_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"No location response received for {name} (timeout: {timeout}s)")
            return []

        data = fcm_response_data.get("data") or []
        if data and data[0].get("canonic_id") == canonic_device_id:
            logger.info(f"Successfully received location data for {name}")
            return data
        if not data:
            logger.warning(f"No location data found after decryption for {name}")
        else:
            logger.warning(f"Received location data for unexpected device in {name} flow; ignoring.")
        return []

    except asyncio.CancelledError:
        logger.info(f"Location request cancelled for {name}")
        raise
    except Exception as e:
        logger.error(f"Error requesting location for {name}: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []
    finally:
        # Clean up - unregister callback first, then stop receiver if no more callbacks
        try:
            if fcm_receiver and registered:
                await fcm_receiver.async_unregister_for_location_updates(canonic_device_id)
            
            # Only stop the receiver if no other callbacks are registered
            if fcm_receiver and len(getattr(fcm_receiver, "location_update_callbacks", {})) == 0:
                await fcm_receiver.async_stop()
                logger.debug(f"Stopped FCM receiver after unregistering last callback for {name}")
            elif fcm_receiver:
                logger.debug(f"FCM receiver kept running - {len(getattr(fcm_receiver, 'location_update_callbacks', {}))} callbacks still registered")
                
        except Exception as cleanup_error:
            logger.warning(f"Error during FCM cleanup for {name}: {cleanup_error}")

if __name__ == '__main__':
    get_location_data_for_device(get_example_data("sample_canonic_device_id"), "Test")
