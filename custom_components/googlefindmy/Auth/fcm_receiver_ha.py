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
        self._background_tasks = set()  # Track background tasks for cleanup
        self._stopping = False  # Flag to prevent recursive cleanup
        
        # Firebase project configuration for Google Find My Device
        self.project_id = "google.com:api-project-289722593072"
        self.app_id = "1:289722593072:android:3cfcf5bc359f0308"
        self.api_key = "AIzaSyD_gko3P392v6how2H7UpdeXQ0v2HLettc"
        self.message_sender_id = "289722593072"
        
        # Note: credentials will be loaded asynchronously in async_initialize
        
    async def async_initialize(self):
        """Async initialization that works with Home Assistant."""
        # Skip if already initialized with valid credentials and push client
        if self.credentials and self.pc:
            return True

        try:
            # Load cached credentials asynchronously to avoid blocking I/O
            await async_load_cache_from_file()
            self.credentials = await async_get_cached_value('fcm_credentials')
            
            # Parse JSON string if credentials were saved as JSON
            if isinstance(self.credentials, str):
                import json
                try:
                    self.credentials = json.loads(self.credentials)
                    # FCM credentials parsed (reduced logging)
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
            
            # FCM receiver initialized (reduced logging)
            return True
            
        except Exception as e:
            _LOGGER.error(f"Failed to initialize FCM receiver: {e}")
            return False
    
    async def async_register_for_location_updates(self, device_id: str, callback: Callable) -> Optional[str]:
        """Register for location updates asynchronously."""
        try:
            # Add callback to dict
            self.location_update_callbacks[device_id] = callback
            # FCM callback registered (reduced logging)
            
            # If not listening, start listening
            if not self._listening:
                await self._start_listening()
            
            # Return FCM token if available
            if self.credentials and 'fcm' in self.credentials and 'registration' in self.credentials['fcm']:
                token = self.credentials['fcm']['registration']['token']
                # FCM token available (reduced logging)
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
                # FCM callback unregistered (reduced logging)
            else:
                # No FCM callback found (reduced logging)
                pass
        except Exception as e:
            _LOGGER.error(f"Failed to unregister location updates for {device_id}: {e}")
    
    def register_coordinator(self, coordinator) -> None:
        """Register a coordinator to receive background location updates."""
        if coordinator not in self.coordinators:
            self.coordinators.append(coordinator)
            # Coordinator registered for FCM updates (reduced logging)
    
    def unregister_coordinator(self, coordinator) -> None:
        """Unregister a coordinator from background location updates."""
        if coordinator in self.coordinators:
            self.coordinators.remove(coordinator)
            # Coordinator unregistered from FCM (reduced logging)
            
            # If no coordinators left, clean up to prevent memory leaks
            if not self.coordinators and not self.location_update_callbacks:
                asyncio.create_task(self._cleanup_if_empty())
    
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
                # Started FCM listening (reduced logging)
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
                # FCM message listener started (reduced logging)
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
                
                # FCM location response received (reduced logging)
                
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
                        task = asyncio.create_task(self._run_callback_async(callback, canonic_id, hex_string))
                        self._background_tasks.add(task)
                        task.add_done_callback(self._background_tasks.discard)
                    except Exception as e:
                        _LOGGER.error(f"Error scheduling FCM callback for device {canonic_id}: {e}")
                elif canonic_id:
                    # Check if this is a tracked device from any coordinator
                    handled_by_coordinator = False
                    for coordinator in self.coordinators:
                        if hasattr(coordinator, 'tracked_devices') and canonic_id in coordinator.tracked_devices:
                            # Processing background FCM update (reduced logging)
                            # Process background update
                            task = asyncio.create_task(self._process_background_update(coordinator, canonic_id, hex_string))
                            self._background_tasks.add(task)
                            task.add_done_callback(self._background_tasks.discard)
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

                # Apply Google Home filtering to background updates
                filtered_location_data = await self._apply_google_home_filtering(location_data, coordinator, device_name)

                # Store in coordinator's location cache
                coordinator._device_location_data[canonic_id] = filtered_location_data
                coordinator._device_location_data[canonic_id]["last_updated"] = time.time()

                semantic_value = filtered_location_data.get('semantic_name')
                _LOGGER.debug(f"FCM stored background location update for {device_name}: semantic='{semantic_value}' (type: {type(semantic_value)})")

                # Trigger coordinator update to refresh entities
                await coordinator.async_request_refresh()
            else:
                _LOGGER.debug(f"No location data in background update for device {canonic_id}")
                
        except Exception as e:
            _LOGGER.error(f"Error processing background update for device {canonic_id}: {e}")

    async def _apply_google_home_filtering(self, location_data: dict, coordinator, device_name: str) -> dict:
        """Apply Google Home filtering logic to location data."""
        try:
            from ..const import DOMAIN

            # Get configuration settings
            config_data = coordinator.hass.data.get(DOMAIN, {}).get("config_data", {})
            filter_google_home = config_data.get("filter_google_home_devices", False)
            google_home_keywords = config_data.get("google_home_device_keywords", ["Speaker", "Hub", "Display", "Chromecast", "Google Home", "Nest"])

            semantic = location_data.get('semantic_name')

            if filter_google_home and semantic:
                is_google_home_location = any(keyword.strip().lower() in semantic.lower() for keyword in google_home_keywords if keyword.strip())
                if is_google_home_location:
                    _LOGGER.debug(f"FCM background update: Google Home location detected for {device_name}: '{semantic}'")

                    # Replace with home zone (same logic as coordinator)
                    home_zone = coordinator.hass.states.get("zone.home")
                    if home_zone:
                        semantic = home_zone.attributes.get("friendly_name", "Home")
                        _LOGGER.info(f"FCM: Replaced with home zone: {semantic}")
                    else:
                        # FCM: No home zone found (reduced logging)
                        pass

            # Create filtered location data
            filtered_data = location_data.copy()
            filtered_data['semantic_name'] = semantic
            return filtered_data

        except Exception as e:
            _LOGGER.error(f"Error applying Google Home filtering to background update: {e}")
            return location_data.copy()

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
        self.credentials = creds
        # Schedule async update to avoid blocking I/O in callback
        task = asyncio.create_task(self._async_save_credentials())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        _LOGGER.debug("FCM credentials updated")
    
    async def _async_save_credentials(self):
        """Save credentials asynchronously."""
        try:
            await async_set_cached_value('fcm_credentials', self.credentials)
        except Exception as e:
            _LOGGER.error(f"Failed to save FCM credentials: {e}")
    
    async def async_stop(self):
        """Stop listening for FCM messages."""
        if self._stopping:
            return  # Already stopping, prevent recursion
        self._stopping = True
        try:
            # Cancel all background tasks first
            for task in list(self._background_tasks):
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete with timeout
            if self._background_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*self._background_tasks, return_exceptions=True),
                        timeout=5.0
                    )
                except asyncio.TimeoutError:
                    _LOGGER.warning("Some FCM background tasks didn't complete within timeout")
            
            self._background_tasks.clear()
            
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
            
            # Clear coordinator references to prevent memory leaks
            self.coordinators.clear()
            self.location_update_callbacks.clear()
            
            _LOGGER.debug("FCM receiver stopped")

        except Exception as e:
            _LOGGER.error(f"Error stopping FCM receiver: {e}")
        finally:
            self._stopping = False
    
    def get_fcm_token(self) -> Optional[str]:
        """Get current FCM token if available."""
        if self.credentials and 'fcm' in self.credentials and 'registration' in self.credentials['fcm']:
            return self.credentials['fcm']['registration']['token']
        return None
    
    async def _cleanup_if_empty(self) -> None:
        """Clean up FCM receiver if no callbacks or coordinators are registered."""
        try:
            if self._stopping:
                return  # Already stopping, prevent recursion
            if not self.coordinators and not self.location_update_callbacks:
                _LOGGER.debug("No coordinators or callbacks remaining, stopping FCM receiver to prevent memory leak")
                await self.async_stop()
        except Exception as e:
            _LOGGER.error(f"Error during FCM receiver cleanup: {e}")