"""Home Assistant compatible FCM receiver for Google Find My Device."""
import asyncio
import base64
import binascii
import logging
from typing import Optional, Callable, Dict, Any

from custom_components.googlefindmy.Auth.token_cache import (
    set_cached_value, 
    get_cached_value, 
    async_set_cached_value, 
    async_get_cached_value,
    async_load_cache_from_file
)

_LOGGER = logging.getLogger(__name__)


class FcmReceiverHA:
    """FCM Receiver that works with Home Assistant's async architecture."""
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(FcmReceiverHA, cls).__new__(cls, *args, **kwargs)
        return cls._instance
    
    def __init__(self):
        """Initialize the FCM receiver for Home Assistant."""
        if hasattr(self, '_initialized') and self._initialized:
            return
        self._initialized = True
        
        self.credentials = None
        self.location_update_callbacks: Dict[str, Callable] = {}
        self.coordinators = []  # List of coordinators that can receive background updates
        self.pc = None
        self._listening = False
        self._listen_task = None
        
        # Firebase project configuration for Google Find My Device
        self.project_id = "google.com:api-project-289722593072"
        self.app_id = "1:289722593072:android:3cfcf5bc359f0308"
        self.api_key = "AIzaSyD_gko3P392v6how2H7UpdeXQ0v2HLettc"
        self.message_sender_id = "289722593072"
        
        # Note: credentials will be loaded asynchronously in async_initialize
        
    async def async_initialize(self):
        """Async initialization that works with Home Assistant."""
        try:
            # Load cached credentials asynchronously to avoid blocking I/O
            await async_load_cache_from_file()
            self.credentials = await async_get_cached_value('fcm_credentials')
            
            # Parse JSON string if credentials were saved as JSON
            if isinstance(self.credentials, str):
                import json
                try:
                    self.credentials = json.loads(self.credentials)
                    _LOGGER.debug("Parsed FCM credentials from JSON string")
                except json.JSONDecodeError as e:
                    _LOGGER.error(f"Failed to parse FCM credentials JSON: {e}")
                    return False
            
            # Import FCM libraries
            from custom_components.googlefindmy.Auth.firebase_messaging import FcmRegisterConfig, FcmPushClient
            
            fcm_config = FcmRegisterConfig(
                project_id=self.project_id,
                app_id=self.app_id,
                api_key=self.api_key,
                messaging_sender_id=self.message_sender_id,
                bundle_id="com.google.android.apps.adm",
            )
            
            # Create push client with callbacks
            self.pc = FcmPushClient(
                self._on_notification, 
                fcm_config, 
                self.credentials, 
                self._on_credentials_updated
            )
            
            _LOGGER.info("FCM receiver initialized successfully")
            return True
            
        except Exception as e:
            _LOGGER.error(f"Failed to initialize FCM receiver: {e}")
            return False
    
    async def async_register_for_location_updates(self, device_id: str, callback: Callable) -> Optional[str]:
        """Register for location updates asynchronously."""
        try:
            # Add callback to dict
            self.location_update_callbacks[device_id] = callback
            _LOGGER.debug(f"Registered FCM callback for device: {device_id}")
            
            # If not listening, start listening
            if not self._listening:
                await self._start_listening()
            
            # Return FCM token if available
            if self.credentials and 'fcm' in self.credentials and 'registration' in self.credentials['fcm']:
                token = self.credentials['fcm']['registration']['token']
                _LOGGER.info(f"FCM token available: {token[:20]}...")
                return token
            else:
                _LOGGER.warning("FCM credentials not available")
                return None
                
        except Exception as e:
            _LOGGER.error(f"Failed to register for location updates: {e}")
            return None
    
    async def async_unregister_for_location_updates(self, device_id: str) -> None:
        """Unregister a device from location updates."""
        try:
            if device_id in self.location_update_callbacks:
                del self.location_update_callbacks[device_id]
                _LOGGER.debug(f"Unregistered FCM callback for device: {device_id}")
            else:
                _LOGGER.debug(f"No FCM callback found to unregister for device: {device_id}")
        except Exception as e:
            _LOGGER.error(f"Failed to unregister location updates for {device_id}: {e}")
    
    def register_coordinator(self, coordinator) -> None:
        """Register a coordinator to receive background location updates."""
        if coordinator not in self.coordinators:
            self.coordinators.append(coordinator)
            _LOGGER.debug(f"Registered coordinator for background FCM updates")
    
    def unregister_coordinator(self, coordinator) -> None:
        """Unregister a coordinator from background FCM updates."""
        if coordinator in self.coordinators:
            self.coordinators.remove(coordinator)
            _LOGGER.debug(f"Unregistered coordinator from background FCM updates")
    
    async def _start_listening(self):
        """Start listening for FCM messages."""
        try:
            if not self.pc:
                await self.async_initialize()
            
            if self.pc:
                # Register with FCM
                await self._register_for_fcm()
                
                # Start listening in background task
                self._listen_task = asyncio.create_task(self._listen_for_messages())
                self._listening = True
                _LOGGER.info("Started listening for FCM notifications")
            else:
                _LOGGER.error("Failed to create FCM push client")
                
        except Exception as e:
            _LOGGER.error(f"Failed to start FCM listening: {e}")
    
    async def _register_for_fcm(self):
        """Register with FCM to get token."""
        if not self.pc:
            return
            
        fcm_token = None
        retries = 0
        
        while fcm_token is None and retries < 3:
            try:
                fcm_token = await self.pc.checkin_or_register()
                if fcm_token:
                    _LOGGER.info(f"FCM registration successful, token: {fcm_token[:20]}...")
                else:
                    _LOGGER.warning(f"FCM registration attempt {retries + 1} failed")
                    retries += 1
                    await asyncio.sleep(5)
            except Exception as e:
                _LOGGER.error(f"FCM registration error: {e}")
                retries += 1
                await asyncio.sleep(5)
    
    async def _listen_for_messages(self):
        """Listen for FCM messages in background."""
        try:
            if self.pc:
                await self.pc.start()
                _LOGGER.info("FCM message listener started")
        except Exception as e:
            _LOGGER.error(f"FCM listen error: {e}")
            self._listening = False
    
    def _on_notification(self, obj: Dict[str, Any], notification, data_message):
        """Handle incoming FCM notification."""
        try:
            # Check if the payload is present
            if 'data' in obj and 'com.google.android.apps.adm.FCM_PAYLOAD' in obj['data']:
                # Decode the base64 string with padding fix
                base64_string = obj['data']['com.google.android.apps.adm.FCM_PAYLOAD']
                
                # Add proper Base64 padding if missing
                missing_padding = len(base64_string) % 4
                if missing_padding:
                    base64_string += '=' * (4 - missing_padding)
                
                try:
                    decoded_bytes = base64.b64decode(base64_string)
                except Exception as decode_error:
                    _LOGGER.error(f"FCM Base64 decode failed in _on_notification: {decode_error}")
                    _LOGGER.debug(f"Problematic Base64 string (length={len(base64_string)}): {base64_string[:50]}...")
                    return
                
                # Convert to hex string
                hex_string = binascii.hexlify(decoded_bytes).decode('utf-8')
                
                _LOGGER.info(f"Received FCM location response: {len(hex_string)} chars")
                
                # Extract canonic_id from response to find the right callback
                canonic_id = None
                try:
                    canonic_id = self._extract_canonic_id_from_response(hex_string)
                except Exception as extract_error:
                    _LOGGER.error(f"Failed to extract canonic_id from FCM response: {extract_error}")
                    return
                
                if canonic_id and canonic_id in self.location_update_callbacks:
                    callback = self.location_update_callbacks[canonic_id]
                    try:
                        # Run callback in executor to avoid blocking the event loop
                        asyncio.create_task(self._run_callback_async(callback, canonic_id, hex_string))
                    except Exception as e:
                        _LOGGER.error(f"Error scheduling FCM callback for device {canonic_id}: {e}")
                elif canonic_id:
                    # Check if this is a tracked device from any coordinator
                    handled_by_coordinator = False
                    for coordinator in self.coordinators:
                        if hasattr(coordinator, 'tracked_devices') and canonic_id in coordinator.tracked_devices:
                            _LOGGER.info(f"Processing background FCM update for tracked device {coordinator._device_names.get(canonic_id, canonic_id[:8])}")
                            # Process background update
                            asyncio.create_task(self._process_background_update(coordinator, canonic_id, hex_string))
                            handled_by_coordinator = True
                            break
                    
                    if not handled_by_coordinator:
                        # Check if we have any active callbacks
                        registered_count = len(self.location_update_callbacks)
                        if registered_count > 0:
                            registered_devices = list(self.location_update_callbacks.keys())
                            _LOGGER.debug(f"Received FCM response for untracked device {canonic_id[:8]}... "
                                        f"Currently waiting for: {[d[:8]+'...' for d in registered_devices]}")
                        else:
                            _LOGGER.debug(f"Received FCM response for untracked device {canonic_id[:8]}... "
                                        f"(not in any coordinator's tracked devices)")
                else:
                    _LOGGER.debug("Could not extract canonic_id from FCM response")
            else:
                _LOGGER.debug("FCM notification without location payload")
                
        except Exception as e:
            _LOGGER.error(f"Error processing FCM notification: {e}")
    
    def _extract_canonic_id_from_response(self, hex_response: str) -> Optional[str]:
        """Extract canonic_id from FCM response to identify which device sent it."""
        try:
            # Import with fallback for different module loading contexts
            try:
                from custom_components.googlefindmy.ProtoDecoders.decoder import parse_device_update_protobuf
            except ImportError:
                from .ProtoDecoders.decoder import parse_device_update_protobuf
            
            device_update = parse_device_update_protobuf(hex_response)
            
            if (device_update.HasField("deviceMetadata") and 
                device_update.deviceMetadata.identifierInformation.canonicIds.canonicId):
                return device_update.deviceMetadata.identifierInformation.canonicIds.canonicId[0].id
        except Exception as e:
            _LOGGER.debug(f"Failed to extract canonic_id from FCM response: {e}")
        return None

    async def _run_callback_async(self, callback, canonic_id: str, hex_string: str):
        """Run callback in executor to avoid blocking the event loop."""
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            # Run the potentially blocking callback in a thread executor with canonic_id
            await loop.run_in_executor(None, callback, canonic_id, hex_string)
        except Exception as e:
            _LOGGER.error(f"Error in async FCM callback for device {canonic_id}: {e}")
    
    async def _process_background_update(self, coordinator, canonic_id: str, hex_string: str):
        """Process background FCM update for a tracked device."""
        try:
            import asyncio
            import time
            
            # Run the location processing in executor to avoid blocking
            location_data = await asyncio.get_event_loop().run_in_executor(
                None, self._decode_background_location, hex_string
            )
            
            if location_data:
                device_name = coordinator._device_names.get(canonic_id, canonic_id[:8])

                # Apply Google Home device filtering
                semantic_name = location_data.get('semantic_name')
                if semantic_name and hasattr(coordinator, 'google_home_filter'):
                    should_filter, replacement_location = coordinator.google_home_filter.should_filter_detection(canonic_id, semantic_name)
                    if should_filter:
                        _LOGGER.debug(f"FCM: Filtering out Google Home spam detection for {device_name}")
                        return  # Skip processing this update
                    elif replacement_location:
                        _LOGGER.info(f"FCM: Google Home filter: Device {device_name} detected at '{semantic_name}', using '{replacement_location}'")
                        location_data = location_data.copy()
                        location_data['semantic_name'] = replacement_location

                # Check if this is actually new location data (avoid duplicates)
                current_last_seen = location_data.get('last_seen')
                existing_data = coordinator._device_location_data.get(canonic_id, {})
                existing_last_seen = existing_data.get('last_seen')

                if current_last_seen != existing_last_seen:
                    # Store in coordinator's location cache only if last_seen changed
                    coordinator._device_location_data[canonic_id] = location_data.copy()
                    coordinator._device_location_data[canonic_id]["last_updated"] = time.time()

                    _LOGGER.info(f"Stored NEW background location update for {device_name} (last_seen: {current_last_seen})")

                    # Increment background update counter
                    coordinator.increment_stat("background_updates")

                    # Check if it's a crowd-sourced update (not own report)
                    is_own_report = location_data.get('is_own_report')
                    _LOGGER.info(f"FCM location saved for {device_name}: is_own_report={is_own_report}")
                    if is_own_report is False:
                        coordinator.increment_stat("crowd_sourced_updates")
                        _LOGGER.info(f"Crowd-sourced update detected for {device_name} via FCM")

                    # Trigger coordinator update to refresh entities
                    await coordinator.async_request_refresh()
                else:
                    _LOGGER.debug(f"Skipping duplicate background location update for {device_name} (same last_seen: {current_last_seen})")
                    coordinator.increment_stat("skipped_duplicates")
            else:
                _LOGGER.debug(f"No location data in background update for device {canonic_id}")
                
        except Exception as e:
            _LOGGER.error(f"Error processing background update for device {canonic_id}: {e}")
    
    def _decode_background_location(self, hex_string: str) -> dict:
        """Decode location data from hex string (runs in executor)."""
        try:
            # Import with robust fallback for different module loading contexts
            try:
                from custom_components.googlefindmy.ProtoDecoders.decoder import parse_device_update_protobuf
                from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import decrypt_location_response_locations
            except ImportError:
                try:
                    # Try relative import from Auth directory
                    from ..ProtoDecoders.decoder import parse_device_update_protobuf
                    from ..NovaApi.ExecuteAction.LocateTracker.decrypt_locations import decrypt_location_response_locations
                except ImportError:
                    # Last resort - try from current working directory
                    import sys
                    import os
                    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
                    from ProtoDecoders.decoder import parse_device_update_protobuf
                    from NovaApi.ExecuteAction.LocateTracker.decrypt_locations import decrypt_location_response_locations
            
            # Parse and decrypt
            device_update = parse_device_update_protobuf(hex_string)
            location_data = decrypt_location_response_locations(device_update)
            
            if location_data and len(location_data) > 0:
                return location_data[0]
            return {}
            
        except Exception as e:
            _LOGGER.error(f"Failed to decode background location data: {e}")
            return {}
    
    def _on_credentials_updated(self, creds):
        """Handle credential updates."""
        # PATCH: Normalize credentials if provided as a JSON string to ensure consistent dict type for downstream consumers and cache.
        creds_to_store = creds
        if isinstance(creds_to_store, str):
            try:
                import json
                creds_to_store = json.loads(creds_to_store)
            except Exception:
                _LOGGER.debug("Received FCM credentials as non-JSON string; storing raw value (read-path is hardened).")
        self.credentials = creds_to_store
        # Schedule async update to avoid blocking I/O in callback
        asyncio.create_task(self._async_save_credentials())
        _LOGGER.info("FCM credentials updated")
    
    async def _async_save_credentials(self):
        """Save credentials asynchronously."""
        try:
            await async_set_cached_value('fcm_credentials', self.credentials)
        except Exception as e:
            _LOGGER.error(f"Failed to save FCM credentials: {e}")
    
    async def async_stop(self):
        """Stop listening for FCM messages."""
        try:
            if self._listen_task:
                self._listen_task.cancel()
                try:
                    await self._listen_task
                except asyncio.CancelledError:
                    pass
                
            if self.pc:
                try:
                    # Check if the push client was properly started before trying to stop
                    if (hasattr(self.pc, 'stop') and callable(getattr(self.pc, 'stop')) and
                        hasattr(self.pc, 'stopping_lock') and self.pc.stopping_lock is not None):
                        await self.pc.stop()
                    else:
                        _LOGGER.debug("FCM push client not fully initialized, skipping stop")
                        # Just set the client to None to clean up
                        self.pc = None
                except TypeError as type_error:
                    if "asynchronous context manager protocol" in str(type_error):
                        _LOGGER.debug(f"FCM push client stop method has context manager issue, skipping: {type_error}")
                    else:
                        _LOGGER.warning(f"Type error stopping FCM push client: {type_error}")
                except Exception as pc_error:
                    _LOGGER.debug(f"Error stopping FCM push client: {pc_error}")
                
            self._listening = False
            _LOGGER.info("FCM receiver stopped")
            
        except Exception as e:
            _LOGGER.error(f"Error stopping FCM receiver: {e}")
    
    def get_fcm_token(self) -> Optional[str]:
        """Get current FCM token if available."""
        if self.credentials and 'fcm' in self.credentials and 'registration' in self.credentials['fcm']:
            return self.credentials['fcm']['registration']['token']
        return None
