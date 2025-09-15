"""Data coordinator for Google Find My Device."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
import time
from math import radians, cos, sin, asin, sqrt

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN, UPDATE_INTERVAL
from .api import GoogleFindMyAPI
from .location_recorder import LocationRecorder

_LOGGER = logging.getLogger(__name__)

def haversine(lon1, lat1, lon2, lat2):
    """Calculate the great circle distance between two points on the earth."""
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Radius of earth in meters
    return c * r


class GoogleFindMyCoordinator(DataUpdateCoordinator):
    """Class to manage fetching Google Find My Device data."""

    def __init__(self, hass: HomeAssistant, oauth_token: str = None, google_email: str = None, secrets_data: dict = None, tracked_devices: list = None, location_poll_interval: int = 300, device_poll_delay: int = 5, min_poll_interval: int = 60) -> None:
        """Initialize."""
        if secrets_data:
            self.api = GoogleFindMyAPI(secrets_data=secrets_data)
        else:
            self.api = GoogleFindMyAPI(oauth_token=oauth_token, google_email=google_email)
        
        self.tracked_devices = tracked_devices or []
        self.location_poll_interval = max(location_poll_interval, min_poll_interval)  # Enforce minimum interval
        self.device_poll_delay = device_poll_delay
        self.min_poll_interval = min_poll_interval  # Minimum 1 minute between polls
        
        # Location data cache with size limits
        self._device_location_data = {}  # Store latest location data for each device
        self._last_location_poll_time = 0 # Start at 0 to have immediate poll on first startup
        self._device_names = {}  # Map device IDs to names for easier lookup
        self._startup_complete = False  # Flag to track if initial setup is done
        
        # Zone cache for Google Home filtering (refresh every hour)
        self._cached_zones = None
        self._zones_cache_time = 0
        
        # Initialize recorder-based location history
        self.location_recorder = LocationRecorder(hass)
        
        # Pre-compile Google Home keywords for efficient filtering
        self._google_home_keywords_lower = None
        self._last_config_check = 0
        
        # Track if we're being shutdown to prevent memory leaks
        self._is_shutdown = False
        
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )
        
        # Defer FCM receiver registration to avoid blocking startup
        self._fcm_registered = False

    def _update_google_home_keywords(self, config_data: dict, current_time: float) -> None:
        """Update Google Home keywords cache if config changed."""
        # Only check config every 5 minutes to reduce overhead
        if current_time - self._last_config_check < 300:
            return
            
        self._last_config_check = current_time
        raw_keywords = config_data.get("google_home_device_keywords", ["Speaker", "Hub", "Display", "Chromecast", "Google Home", "Nest"])
        # Pre-process and limit keywords to prevent memory issues
        self._google_home_keywords_lower = [kw.strip().lower() for kw in raw_keywords[:20] if kw.strip()]

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
            if not self._fcm_registered and not self._is_shutdown:
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
                        # Requesting location for device (reduced logging)
                        location_data = await self.api.async_get_device_location(device_id, device_name)
                        
                        if location_data:
                            lat = location_data.get('latitude')
                            lon = location_data.get('longitude')
                            accuracy = location_data.get('accuracy')
                            semantic = location_data.get('semantic_name')

                            # Debug logging to understand what data we get
                            # Location data received (reduced logging)
                            
                            # Get configuration settings
                            config_data = self.hass.data.get(DOMAIN, {}).get("config_data", {})
                            min_accuracy_threshold = config_data.get("min_accuracy_threshold", 100)
                            filter_google_home = config_data.get("filter_google_home_devices", False)
                            
                            # Update keywords cache if needed
                            self._update_google_home_keywords(config_data, current_time)
                            
                            # Handle Google Home semantic locations if filtering is enabled
                            google_home_detected = False
                            if filter_google_home and semantic and self._google_home_keywords_lower:
                                # Efficient keyword matching using pre-compiled lowercase keywords
                                semantic_lower = semantic.lower()
                                is_google_home_location = any(keyword in semantic_lower for keyword in self._google_home_keywords_lower)
                                        
                                # Google Home location check (reduced logging)
                                if is_google_home_location:
                                    google_home_detected = True
                                    _LOGGER.info(f"Google Home location detected for {device_name}: '{semantic}'")

                                    # Try to determine which HA zone we're in based on GPS coordinates
                                    zone_name = None
                                    if lat is not None and lon is not None:
                                        # Get all zones from Home Assistant
                                        enable_zone_caching = config_data.get("enable_zone_caching", True)
                                        zone_cache_duration = config_data.get("zone_cache_duration", 3600)
                                        
                                        if enable_zone_caching:
                                            # Use zone caching to reduce memory usage
                                            if not self._cached_zones or (current_time - self._zones_cache_time) > zone_cache_duration:
                                                # Clear old cache first to prevent memory accumulation
                                                self._cached_zones = None
                                                # Get fresh zones and limit to reasonable number
                                                all_zones = self.hass.states.async_all("zone")
                                                self._cached_zones = all_zones[:50] if len(all_zones) > 50 else all_zones
                                                self._zones_cache_time = current_time
                                            zones = self._cached_zones
                                        else:
                                            # No caching - get zones fresh each time but still limit
                                            all_zones = self.hass.states.async_all("zone")
                                            zones = all_zones[:50] if len(all_zones) > 50 else all_zones
                                        
                                        # Process zones (already limited to 50 max)
                                        for zone in zones:

                                            zone_lat = zone.attributes.get("latitude")
                                            zone_lon = zone.attributes.get("longitude")
                                            zone_radius = zone.attributes.get("radius", 100)

                                            if zone_lat and zone_lon:
                                                # Calculate distance to zone center
                                                distance = haversine(lon, lat, zone_lon, zone_lat)
                                                if distance <= zone_radius:
                                                    zone_name = zone.attributes.get("friendly_name", zone.entity_id.split('.')[1])
                                                    # Device found in zone (reduced logging)
                                                    break

                                    # Replace semantic location with zone name or use default
                                    if zone_name:
                                        semantic = zone_name
                                        _LOGGER.info(f"Replaced Google Home location with zone: {zone_name}")
                                    elif lat is None or lon is None:
                                        # No GPS coordinates to determine zone, use the user's home zone
                                        # Get the actual home zone (zone.home)
                                        home_zone = self.hass.states.get("zone.home")
                                        if home_zone:
                                            semantic = home_zone.attributes.get("friendly_name", "Home")
                                            # No GPS, using home zone (reduced logging)
                                        else:
                                            # Can't determine zone without GPS, keep original semantic
                                            semantic = semantic  # Keep original semantic
                                    else:
                                        # Has GPS but not in any defined zone, clear the semantic location
                                        semantic = None
                                        # Google Home location filtered (reduced logging)

                            # Process location data if we have coordinates or semantic location
                            if lat is not None and lon is not None or semantic:
                                # Check if this is first poll for this device (no cached data)
                                is_first_poll = device_id not in self._device_location_data

                                # Check if current GPS data should be filtered out due to poor accuracy
                                # Always accept if: first poll, semantic-only (no GPS), or good accuracy
                                should_accept = (is_first_poll or
                                               semantic and (lat is None or lon is None) or
                                               accuracy is None or
                                               accuracy <= min_accuracy_threshold)

                                if not should_accept:
                                    _LOGGER.warning(f"Poor accuracy for {device_name}: {accuracy}m exceeds {min_accuracy_threshold}m threshold")
                                else:
                                    if is_first_poll and accuracy is not None and accuracy > min_accuracy_threshold:
                                        _LOGGER.info(f"Accepting poor accuracy for {device_name} on first poll: {accuracy}m")
                                    elif semantic and (lat is None or lon is None):
                                        _LOGGER.debug(f"Accepting semantic location for {device_name}: {semantic}")
                                    # Location received successfully
                                    # Check if we should skip this update to avoid duplicate recorder entries
                                    skip_update = False
                                    if google_home_detected and device_id in self._device_location_data:
                                        # Check if we're already in the same zone
                                        current_stored_semantic = self._device_location_data[device_id].get('semantic_name')
                                        if current_stored_semantic == semantic:
                                            # Already in the same zone, skip update to avoid recorder spam
                                            # Skipping update, already in zone (reduced logging)
                                            skip_update = True

                                    if not skip_update:
                                        # Store current location data with filtered semantic location
                                        filtered_location_data = location_data.copy()
                                        filtered_location_data['semantic_name'] = semantic  # Use filtered semantic location
                                        filtered_location_data["last_updated"] = current_time
                                        self._device_location_data[device_id] = filtered_location_data

                                        # Limit cache size per device to prevent memory leaks
                                        if len(self._device_location_data) > 50:  # Reduced from 100 to 50
                                            # Remove oldest entry based on last_updated timestamp
                                            oldest_key = min(self._device_location_data.keys(),
                                                           key=lambda k: self._device_location_data[k].get('last_updated', 0))
                                            # Cache cleanup (reduced logging)
                                            del self._device_location_data[oldest_key]

                                        # Location data stored (reduced logging)

                                        # Get recorder history and combine with current data for better location selection
                                        try:
                                            # Try both possible entity ID formats
                                            entity_id_by_unique = f"device_tracker.{DOMAIN}_{device_id}"
                                            entity_id_by_name = f"device_tracker.{device_name.lower().replace(' ', '_')}"
                                        
                                            # Try unique ID format first
                                            historical_locations = await self.location_recorder.get_location_history(entity_id_by_unique, hours=24)

                                            # If no history found, try name-based format
                                            if not historical_locations:
                                                # No history, trying alternate entity (reduced logging)
                                                historical_locations = await self.location_recorder.get_location_history(entity_id_by_name, hours=24)

                                            # Add current Google API location to historical data
                                            current_location_entry = {
                                                'timestamp': location_data.get('last_seen', current_time),
                                                'latitude': location_data.get('latitude'),
                                                'longitude': location_data.get('longitude'),
                                                'accuracy': location_data.get('accuracy'),
                                                'semantic_name' : semantic,  # Use processed semantic location (zone name if replaced)
                                                'is_own_report': location_data.get('is_own_report', False),
                                                'altitude': location_data.get('altitude')
                                            }
                                            historical_locations.insert(0, current_location_entry)

                                            # Limit historical locations to prevent memory growth
                                            if len(historical_locations) > 50:  # Reduced from 100 to 50
                                                historical_locations = historical_locations[:50]
                                                # Limited historical locations (reduced logging)

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
                                            # Recorder lookup failed, using current (reduced logging)
                                            pass
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
    
    async def async_shutdown(self) -> None:
        """Clean up coordinator resources."""
        _LOGGER.info("Coordinator shutting down, cleaning up resources")
        
        self._is_shutdown = True
        
        # Clear caches to free memory
        self._device_location_data.clear()
        self._device_names.clear()
        self._cached_zones = None
        self._google_home_keywords_lower = None
            
        _LOGGER.info("Coordinator cleanup completed")