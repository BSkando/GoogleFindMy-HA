"""Data coordinator for Google Find My Device."""
from __future__ import annotations

import asyncio
import logging
import gc
from datetime import datetime, timedelta
import time
from math import radians, cos, sin, asin, sqrt

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

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

class MemoryGuardian:
    """Memory monitoring and safety system to prevent HA crashes."""
    
    def __init__(self):
        self.enabled = PSUTIL_AVAILABLE
        if self.enabled:
            self.process = psutil.Process()
        else:
            self.process = None
            _LOGGER.warning("psutil not available - memory monitoring disabled")
        self.initial_memory = None
        self.memory_warning_threshold = 85  # %
        self.memory_critical_threshold = 95  # %
        self.memory_growth_threshold = 500 * 1024 * 1024  # 500MB growth
        self.memory_checks = 0
        self.last_warning_time = 0
        self.emergency_shutdown_triggered = False
        
    def check_memory_safety(self) -> tuple[bool, str]:
        """Check memory usage and return (is_safe, status_message)."""
        if not self.enabled:
            return True, "Memory monitoring disabled (psutil not available)"

        try:
            # Get current memory info
            memory_info = self.process.memory_info()
            current_rss = memory_info.rss

            # Set initial memory baseline on first check
            if self.initial_memory is None:
                self.initial_memory = current_rss
                return True, f"Memory baseline set: {current_rss / 1024 / 1024:.1f}MB"

            # Calculate memory growth
            memory_growth = current_rss - self.initial_memory
            current_mb = current_rss / 1024 / 1024
            growth_mb = memory_growth / 1024 / 1024

            # Get system memory percentage
            system_memory = psutil.virtual_memory()
            memory_percent = system_memory.percent

            self.memory_checks += 1
            current_time = time.time()

            # Critical memory usage - emergency shutdown (be more conservative)
            if memory_percent > self.memory_critical_threshold or memory_growth > self.memory_growth_threshold:
                self.emergency_shutdown_triggered = True
                return False, f"CRITICAL: Memory usage {memory_percent:.1f}% or growth {growth_mb:.1f}MB - EMERGENCY SHUTDOWN"

            # Warning level
            if memory_percent > self.memory_warning_threshold and (current_time - self.last_warning_time) > 300:  # 5 min between warnings
                self.last_warning_time = current_time
                _LOGGER.warning(f"GoogleFindMy memory warning: System {memory_percent:.1f}%, Process {current_mb:.1f}MB (+{growth_mb:.1f}MB)")
                return True, f"WARNING: High memory usage {memory_percent:.1f}%"

            # Normal operation - log every 5 checks instead of 10 to see more activity
            if self.memory_checks % 5 == 0:
                return True, f"Memory OK: {current_mb:.1f}MB (+{growth_mb:.1f}MB), System: {memory_percent:.1f}%"

            return True, "Memory OK"

        except Exception as e:
            _LOGGER.warning(f"Memory check failed: {e} - continuing with polling")
            # Don't stop polling if memory check fails - just log the error
            return True, "Memory check failed - monitoring disabled"
    
    def force_garbage_collection(self):
        """Force garbage collection and memory cleanup."""
        try:
            collected = gc.collect()
            _LOGGER.debug(f"Garbage collection freed {collected} objects")
        except Exception as e:
            _LOGGER.error(f"Garbage collection failed: {e}")


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
        
        # Track if we're being shutdown to prevent memory leaks
        self._is_shutdown = False
        self._fcm_receiver = None
        
        # Initialize memory guardian for safety monitoring
        self._memory_guardian = MemoryGuardian()
        self._emergency_mode = False
        
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
            # Memory safety check - emergency shutdown if critical
            is_safe, memory_status = self._memory_guardian.check_memory_safety()
            if not is_safe:
                _LOGGER.critical(f"GoogleFindMy EMERGENCY SHUTDOWN: {memory_status}")
                self._emergency_mode = True
                # Trigger coordinator cleanup
                await self.async_shutdown()
                # Stop all operations
                raise UpdateFailed("Emergency memory shutdown to prevent HA crash")
            
            # Log memory status occasionally (only warnings/baselines)
            if "baseline" in memory_status or "WARNING" in memory_status:
                _LOGGER.debug(f"GoogleFindMy Memory Status: {memory_status}")
            
            # Skip operations if in emergency mode
            if self._emergency_mode:
                _LOGGER.warning("GoogleFindMy in emergency mode, skipping operations")
                return []
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
                    self._fcm_receiver = FcmReceiverHA()
                    self._fcm_receiver.register_coordinator(self)
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
                            google_home_keywords = config_data.get("google_home_device_keywords", ["Speaker", "Hub", "Display", "Chromecast", "Google Home", "Nest"])

                            # Configuration check (reduced logging)
                            
                            # Handle Google Home semantic locations if filtering is enabled
                            google_home_detected = False
                            if filter_google_home and semantic:
                                # Checking semantic location (reduced logging)
                                # Cache lowercased semantic and keywords to prevent repeated string operations
                                semantic_lower = semantic.lower()
                                # Limit keyword checking to prevent memory issues
                                max_keywords = 20
                                keywords_checked = 0
                                is_google_home_location = False
                                
                                for keyword in google_home_keywords:
                                    keywords_checked += 1
                                    if keywords_checked > max_keywords:
                                        break
                                    keyword_clean = keyword.strip()
                                    if keyword_clean and keyword_clean.lower() in semantic_lower:
                                        is_google_home_location = True
                                        break
                                        
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
                                                if self._cached_zones:
                                                    del self._cached_zones
                                                self._cached_zones = self.hass.states.async_all("zone")
                                                self._zones_cache_time = current_time
                                                # Zone cache refreshed (reduced logging)
                                            zones = self._cached_zones
                                        else:
                                            # No caching - get zones fresh each time
                                            zones = self.hass.states.async_all("zone")
                                        
                                        # Limit zone processing to prevent memory issues
                                        zone_count = 0
                                        max_zones = 50  # Limit zone checking to prevent memory leak
                                        
                                        for zone in zones:
                                            zone_count += 1
                                            if zone_count > max_zones:
                                                _LOGGER.warning(f"Zone processing limit reached ({max_zones}), stopping search")
                                                break
                                                
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
                                # Check if current GPS data should be filtered out due to poor accuracy
                                if accuracy is not None and accuracy > min_accuracy_threshold:
                                    _LOGGER.warning(f"Poor accuracy for {device_name}: {accuracy}m exceeds {min_accuracy_threshold}m threshold")
                                else:
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
                                            
                                            # Force garbage collection of removed data
                                            import gc
                                            gc.collect()

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
                                                # Force cleanup of truncated data
                                                gc.collect()

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
                
                # Force garbage collection after intensive polling operations
                self._memory_guardian.force_garbage_collection()
            
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
        
        # Unregister from FCM receiver
        if self._fcm_receiver and self._fcm_registered:
            try:
                self._fcm_receiver.unregister_coordinator(self)
                # Unregistered from FCM receiver (reduced logging)
            except Exception as e:
                _LOGGER.warning(f"Error unregistering from FCM receiver: {e}")
        
        # Clear caches to free memory
        if self._device_location_data:
            self._device_location_data.clear()
            
        if self._device_names:
            self._device_names.clear()
            
        if self._cached_zones:
            del self._cached_zones
            self._cached_zones = None
            
        # Force garbage collection
        import gc
        gc.collect()
        
        _LOGGER.info("Coordinator cleanup completed")