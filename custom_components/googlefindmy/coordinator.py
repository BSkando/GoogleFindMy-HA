"""Data coordinator for Google Find My Device."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
import time

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN, UPDATE_INTERVAL
from .api import GoogleFindMyAPI
from .location_recorder import LocationRecorder
from .google_home_filter import GoogleHomeFilter

_LOGGER = logging.getLogger(__name__)


class GoogleFindMyCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Google Find My Device data."""

    def __init__(self, hass: HomeAssistant, oauth_token: str = None, google_email: str = None, secrets_data: dict = None, tracked_devices: list = None, location_poll_interval: int = 300, device_poll_delay: int = 5, min_poll_interval: int = 60, config_data: dict = None) -> None:
        """Initialize."""
        if secrets_data:
            self.api = GoogleFindMyAPI(secrets_data=secrets_data)
        else:
            self.api = GoogleFindMyAPI(oauth_token=oauth_token, google_email=google_email)
        
        self.tracked_devices = tracked_devices or []
        self.location_poll_interval = max(location_poll_interval, min_poll_interval)  # Enforce minimum interval
        self.device_poll_delay = device_poll_delay
        self.min_poll_interval = min_poll_interval  # Minimum 1 minute between polls
        
        # Location data cache
        self._device_location_data = {}  # Store latest location data for each device
        self._last_location_poll_time = 0 # Start at 0 to have immediate poll on first startup
        self._device_names = {}  # Map device IDs to names for easier lookup
        self._startup_complete = False  # Flag to track if initial setup is done
        
        # Initialize recorder-based location history
        self.location_recorder = LocationRecorder(hass)

        # Initialize Google Home device filter
        self.google_home_filter = GoogleHomeFilter(hass, config_data or {})
        
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )
        
        # Defer FCM receiver registration to avoid blocking startup
        self._fcm_registered = False



    async def _async_update_data(self):
        """Update data via library."""
        try:
            # Get basic device list - always use async to avoid blocking the event loop
            all_devices = await self.hass.async_add_executor_job(self.api.get_basic_device_list)
            
            # Filter to only tracked devices
            if self.tracked_devices:
                devices = [dev for dev in all_devices if dev["id"] in self.tracked_devices]
            else:
                devices = all_devices
            
            # Update device names mapping
            for device in devices:
                self._device_names[device["id"]] = device["name"]
            
            current_time = time.time()
            
            # Check if it's time for a location poll
            time_since_last_poll = current_time - self._last_location_poll_time
            should_poll_location = (time_since_last_poll >= self.location_poll_interval)
            
            # Skip location polling during first refresh to speed up startup
            if self._last_location_poll_time == 0:
                should_poll_location = False
                # Set timestamp so next update cycle will poll normally
                self._last_location_poll_time = current_time
            
            # Register with FCM receiver after first refresh to enable background updates
            if not self._fcm_registered:
                try:
                    from .Auth.fcm_receiver_ha import FcmReceiverHA
                    fcm_receiver = FcmReceiverHA()
                    fcm_receiver.register_coordinator(self)
                    self._fcm_registered = True
                except Exception as e:
                    _LOGGER.warning(f"Failed to register with FCM receiver: {e}")
            
            if should_poll_location and devices:
                
                # Poll all devices with delays to avoid rate limiting
                for i, device in enumerate(devices):
                    device_id = device["id"]
                    device_name = device["name"]
                    
                    # Add delay between requests to avoid overwhelming the API
                    if i > 0:
                        await asyncio.sleep(self.device_poll_delay)
                    
                    try:
                        _LOGGER.debug(f"Requesting location for {device_name}")
                        location_data = await self.api.async_get_device_location(device_id, device_name)
                        
                        if location_data:
                            lat = location_data.get('latitude')
                            lon = location_data.get('longitude')
                            accuracy = location_data.get('accuracy')
                            semantic = location_data.get('semantic_name')

                            # Apply Google Home device filtering
                            if semantic:
                                should_filter, replacement_location = self.google_home_filter.should_filter_detection(device_id, semantic)
                                if should_filter:
                                    _LOGGER.debug(f"Filtering out Google Home spam detection for {device_name}")
                                    continue  # Skip this device for this poll cycle
                                elif replacement_location:
                                    _LOGGER.info(f"Google Home filter: Device {device_name} detected at '{semantic}', using '{replacement_location}'")
                                    semantic = replacement_location
                                    # Update the semantic_name in location_data for consistency
                                    location_data = location_data.copy()
                                    location_data['semantic_name'] = replacement_location
                            
                            # Get accuracy threshold from config
                            config_data = self.hass.data.get(DOMAIN, {}).get("config_data", {})
                            min_accuracy_threshold = config_data.get("min_accuracy_threshold", 100)
                            
                            # Validate coordinates and accuracy threshold
                            if lat is not None and lon is not None or semantic:
                                if accuracy is not None and accuracy > min_accuracy_threshold:
                                    _LOGGER.debug(f"Filtering out location for {device_name}: accuracy {accuracy}m exceeds threshold {min_accuracy_threshold}m")
                                else:
                                    # Location received successfully
                                    
                                    # Store current location and get best from recorder history
                                    self._device_location_data[device_id] = location_data.copy()
                                    self._device_location_data[device_id]["last_updated"] = current_time
                                    
                                    
                                    # Get recorder history and combine with current data for better location selection
                                    try:
                                        # Try both possible entity ID formats
                                        entity_id_by_unique = f"device_tracker.{DOMAIN}_{device_id}"
                                        entity_id_by_name = f"device_tracker.{device_name.lower().replace(' ', '_')}"
                                        
                                        # Try unique ID format first
                                        historical_locations = await self.location_recorder.get_location_history(entity_id_by_unique, hours=24)
                                        
                                        # If no history found, try name-based format
                                        if not historical_locations:
                                            _LOGGER.debug(f"No history for {entity_id_by_unique}, trying {entity_id_by_name}")
                                            historical_locations = await self.location_recorder.get_location_history(entity_id_by_name, hours=24)
                                        
                                        # Add current Google API location to historical data
                                        current_location_entry = {
                                            'timestamp': location_data.get('last_seen', current_time),
                                            'latitude': location_data.get('latitude'),
                                            'longitude': location_data.get('longitude'),
                                            'accuracy': location_data.get('accuracy'),
                                            'semantic_name' : location_data.get('semantic_name'),
                                            'is_own_report': location_data.get('is_own_report', False),
                                            'altitude': location_data.get('altitude')
                                        }
                                        historical_locations.insert(0, current_location_entry)
                                        
                                        # Select best location from all data (current + 24hrs of history)
                                        best_location = self.location_recorder.get_best_location(historical_locations)
                                        
                                        if best_location:
                                            # Use the best location from combined dataset
                                            self._device_location_data[device_id].update({
                                                'latitude': best_location.get('latitude'),
                                                'longitude': best_location.get('longitude'),
                                                'accuracy': best_location.get('accuracy'),
                                                'altitude': best_location.get('altitude'),
                                                'semantic_name' : best_location.get('semantic_name'),
                                                'is_own_report': best_location.get('is_own_report')
                                            })
                                    
                                    except Exception as e:
                                        _LOGGER.debug(f"Recorder history lookup failed for {device_name}, using current data: {e}")
                            else:
                                _LOGGER.warning(f"Invalid coordinates/semantic location for {device_name}: lat={lat}, lon={lon}, semantic_name={semantic}")
                        else:
                            _LOGGER.warning(f"No location data returned for {device_name}")
                            
                    except Exception as e:
                        _LOGGER.error(f"Failed to get location for {device_name}: {e}")
                
                # Update polling state
                self._last_location_poll_time = current_time
            
            # Build device data with cached location information
            device_data = []
            for device in devices:
                # Start with last known good position if available
                if device["id"] in self._device_location_data:
                    # Use cached location as base (preserves last known good position)
                    device_info = self._device_location_data[device["id"]].copy()
                    device_info.update({
                        "name": device["name"],
                        "id": device["id"], 
                        "device_id": device["id"],
                        "status": "Using last known position"
                    })
                else:
                    # No cached data, create new entry
                    device_info = {
                        "name": device["name"],
                        "id": device["id"], 
                        "device_id": device["id"],
                        "latitude": None,
                        "longitude": None,
                        "altitude": None,
                        "accuracy": None,
                        "last_seen": None,
                        "status": "Waiting for location poll",
                        "is_own_report": None,
                        "semantic_name": None,
                        "battery_level": None
                    }
                
                # Apply fresh cached location data if available (COPY to avoid contamination)
                if device["id"] in self._device_location_data and "last_updated" in self._device_location_data[device["id"]]:
                    cached_location = self._device_location_data[device["id"]].copy()
                    device_info.update(cached_location)
                    # Applied cached location data
                    
                    # Add status based on data age
                    last_updated = cached_location.get("last_updated", 0)
                    data_age = current_time - last_updated
                    if data_age < self.location_poll_interval:
                        device_info["status"] = "Location data current"
                    elif data_age < self.location_poll_interval * 2:
                        device_info["status"] = "Location data aging"
                    else:
                        device_info["status"] = "Location data stale"
                # Remove excessive "no cached data" logging
                
                device_data.append(device_info)
                
            return device_data
            
        except Exception as exception:
            raise UpdateFailed(exception) from exception

    async def async_locate_device(self, device_id: str) -> dict:
        """Locate a device."""
        return await self.hass.async_add_executor_job(
            self.api.locate_device, device_id
        )

    async def async_play_sound(self, device_id: str) -> bool:
        """Play sound on a device."""
        return await self.hass.async_add_executor_job(
            self.api.play_sound, device_id
        )