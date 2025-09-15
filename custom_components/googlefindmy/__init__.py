"""Google Find My Device integration for Home Assistant.

Version: 2.0 - Location extraction from device list
"""
from __future__ import annotations

import logging
import time
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import config_validation as cv
import voluptuous as vol

from .const import DOMAIN, CONF_OAUTH_TOKEN, SERVICE_LOCATE_DEVICE, SERVICE_PLAY_SOUND, SERVICE_LOCATE_EXTERNAL
from .coordinator import GoogleFindMyCoordinator
from .Auth.token_cache import async_load_cache_from_file

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.DEVICE_TRACKER, Platform.BUTTON]


async def _async_save_secrets_data(secrets_data: dict) -> None:
    """Save complete secrets data to persistent cache asynchronously."""
    from .Auth.token_cache import async_set_cached_value
    from .Auth.username_provider import username_string
    import json
    
    # Create enhanced data similar to API initialization
    enhanced_data = secrets_data.copy()
    
    # Extract and add username
    google_email = secrets_data.get('username', secrets_data.get('Email'))
    if google_email:
        enhanced_data[username_string] = google_email
    
    # Save all the secrets data to persistent cache
    for key, value in enhanced_data.items():
        try:
            if isinstance(value, (str, int, float)):
                await async_set_cached_value(key, str(value))
            elif key == 'fcm_credentials':
                # Save FCM credentials as JSON
                await async_set_cached_value(key, json.dumps(value))
            else:
                # Convert other complex values to JSON string for storage
                await async_set_cached_value(key, json.dumps(value))
        except Exception as e:
            _LOGGER.warning(f"Failed to save {key} to persistent cache: {e}")


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Google Find My Device from a config entry."""
    # Preload the cache to avoid blocking I/O later
    try:
        await async_load_cache_from_file()
        _LOGGER.debug("Cache preloaded successfully")
    except Exception as e:
        _LOGGER.warning(f"Failed to preload cache: {e}")
    
    auth_method = entry.data.get("auth_method", "individual_tokens")
    tracked_devices = entry.data.get("tracked_devices", [])
    location_poll_interval = entry.data.get("location_poll_interval", 300)
    device_poll_delay = entry.data.get("device_poll_delay", 5)
    min_accuracy_threshold = entry.data.get("min_accuracy_threshold", 100)
    movement_threshold = entry.data.get("movement_threshold", 50)
    
    if auth_method == "secrets_json":
        secrets_data = entry.data.get("secrets_data")
        if not secrets_data:
            _LOGGER.error("Secrets data not found in config entry")
            raise ConfigEntryNotReady("Secrets data not found")
        coordinator = GoogleFindMyCoordinator(
            hass,
            secrets_data=secrets_data,
            tracked_devices=tracked_devices,
            location_poll_interval=location_poll_interval,
            device_poll_delay=device_poll_delay,
            config_data=entry.data
        )
    else:
        oauth_token = entry.data.get(CONF_OAUTH_TOKEN)
        google_email = entry.data.get("google_email")
        
        if not oauth_token:
            _LOGGER.error("OAuth token not found in config entry")
            raise ConfigEntryNotReady("OAuth token not found")
        
        if not google_email:
            _LOGGER.error("Google email not found in config entry")
            raise ConfigEntryNotReady("Google email not found")

        coordinator = GoogleFindMyCoordinator(
            hass,
            oauth_token=oauth_token,
            google_email=google_email,
            tracked_devices=tracked_devices,
            location_poll_interval=location_poll_interval,
            device_poll_delay=device_poll_delay,
            config_data=entry.data
        )
    
    try:
        await coordinator.async_config_entry_first_refresh()
    except Exception as err:
        _LOGGER.error("Failed to initialize Google Find My Device: %s", err)
        raise ConfigEntryNotReady from err

    # Save complete secrets data to persistent cache asynchronously
    if auth_method == "secrets_json" and secrets_data:
        try:
            await _async_save_secrets_data(secrets_data)
            _LOGGER.debug("Saved complete secrets data to persistent cache")
        except Exception as e:
            _LOGGER.warning(f"Failed to save secrets data to persistent cache: {e}")

    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator
    
    # Store config data for device tracker to use
    hass.data[DOMAIN]["config_data"] = {
        "min_accuracy_threshold": min_accuracy_threshold,
        "movement_threshold": movement_threshold
    }

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Register services
    await _async_register_services(hass, coordinator)

    # Listen for config entry updates to reload settings
    entry.async_on_unload(entry.add_update_listener(async_update_entry))

    return True


async def async_update_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Update a config entry."""
    # Get the coordinator
    coordinator = hass.data[DOMAIN][entry.entry_id]
    
    # Update coordinator settings from new config entry data
    coordinator.tracked_devices = entry.data.get("tracked_devices", [])
    coordinator.location_poll_interval = entry.data.get("location_poll_interval", 300)
    coordinator.device_poll_delay = entry.data.get("device_poll_delay", 5)

    # Update Google Home filter configuration
    if hasattr(coordinator, 'google_home_filter'):
        coordinator.google_home_filter.update_config(entry.data)

    # Update config data for device tracker
    hass.data[DOMAIN]["config_data"] = {
        "min_accuracy_threshold": entry.data.get("min_accuracy_threshold", 100),
        "movement_threshold": entry.data.get("movement_threshold", 50)
    }
    
    # Reset polling state to apply changes immediately  
    coordinator._last_location_poll_time = 0
    coordinator._device_location_data = {}
    
    _LOGGER.info(f"Updated configuration: {len(coordinator.tracked_devices)} tracked devices, {coordinator.location_poll_interval}s poll interval")
    
    # Trigger immediate refresh
    await coordinator.async_refresh()


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        hass.data[DOMAIN].pop(entry.entry_id)

    return unload_ok


async def _async_register_services(hass: HomeAssistant, coordinator: GoogleFindMyCoordinator) -> None:
    """Register services for the integration."""
    
    async def async_locate_device_service(call: ServiceCall) -> None:
        """Handle locate device service call."""
        device_id = call.data["device_id"]
        try:
            await coordinator.async_locate_device(device_id)
        except Exception as err:
            _LOGGER.error("Failed to locate device %s: %s", device_id, err)

    async def async_play_sound_service(call: ServiceCall) -> None:
        """Handle play sound service call."""
        device_id = call.data["device_id"]
        try:
            await coordinator.async_play_sound(device_id)
        except Exception as err:
            _LOGGER.error("Failed to play sound on device %s: %s", device_id, err)

    async def async_locate_external_service(call: ServiceCall) -> None:
        """Handle external locate device service call using original GoogleFindMyTools."""
        device_id = call.data.get("device_id")
        device_name = call.data.get("device_name", device_id)
        
        try:
            import subprocess
            import json
            import tempfile
            import os
            
            _LOGGER.info(f"External location request for device: {device_name} ({device_id})")
            
            # Create a temporary Python script that uses the existing GoogleFindMyTools
            script_content = f'''
import sys
import os
sys.path.append("/config/custom_components/googlefindmy")

# Location request test code removed - not needed in production
'''
            
            # Write script to temp file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
                f.write(script_content)
                script_path = f.name
            
            try:
                # Run the script and capture output
                result = await hass.async_add_executor_job(
                    subprocess.run,
                    ["python3", script_path],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                # Parse the output for location data
                if result.returncode == 0:
                    for line in result.stdout.split('\\n'):
                        if line.startswith('LOCATION_RESULT:'):
                            location_data = json.loads(line[16:])  # Remove 'LOCATION_RESULT: '
                            if location_data and len(location_data) > 0:
                                loc = location_data[0]
                                if loc.get('latitude') and loc.get('longitude'):
                                    _LOGGER.info(f"External location found for {device_name}: lat={loc['latitude']}, lon={loc['longitude']}")
                                    
                                    # Update the coordinator's location cache
                                    coordinator._device_location_data[device_id] = loc
                                    coordinator._device_location_data[device_id]["last_updated"] = time.time()
                                    
                                    # Trigger a coordinator update to refresh device tracker entities
                                    await coordinator.async_request_refresh()
                                else:
                                    _LOGGER.warning(f"External location request for {device_name} returned no coordinates")
                            else:
                                _LOGGER.warning(f"External location request for {device_name} returned empty data")
                        elif line.startswith('LOCATION_ERROR:'):
                            error = line[15:]  # Remove 'LOCATION_ERROR: '
                            _LOGGER.error(f"External location request failed for {device_name}: {error}")
                else:
                    _LOGGER.error(f"External location script failed for {device_name}: {result.stderr}")
            finally:
                # Clean up temp file
                try:
                    os.unlink(script_path)
                except:
                    pass
                    
        except Exception as err:
            _LOGGER.error("Failed to get external location for device %s: %s", device_name, err)

    # Register services
    hass.services.async_register(
        DOMAIN,
        SERVICE_LOCATE_DEVICE,
        async_locate_device_service,
        schema=vol.Schema({
            vol.Required("device_id"): cv.string,
        }),
    )

    hass.services.async_register(
        DOMAIN,
        SERVICE_PLAY_SOUND,
        async_play_sound_service,
        schema=vol.Schema({
            vol.Required("device_id"): cv.string,
        }),
    )
    
    hass.services.async_register(
        DOMAIN,
        SERVICE_LOCATE_EXTERNAL,
        async_locate_external_service,
        schema=vol.Schema({
            vol.Required("device_id"): cv.string,
            vol.Optional("device_name"): cv.string,
        }),
    )