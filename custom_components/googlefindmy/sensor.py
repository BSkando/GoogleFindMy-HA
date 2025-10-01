"""Sensor entities for Google Find My Device integration."""
import logging
from typing import Any

from homeassistant.components.sensor import (
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Google Find My Device sensor entities."""
    coordinator = hass.data[DOMAIN][entry.entry_id]

    entities = []

    # Add global statistics sensors (for the integration itself) if enabled
    if entry.data.get("enable_stats_entities", True):
        entities.extend([
            GoogleFindMyStatsSensor(coordinator, "skipped_duplicates", "Skipped Duplicates"),
            GoogleFindMyStatsSensor(coordinator, "background_updates", "Background Updates"),
            GoogleFindMyStatsSensor(coordinator, "crowd_sourced_updates", "Crowd-sourced Updates"),
        ])

    # Add per-device last_seen sensors if we have device data
    if coordinator.data:
        for device in coordinator.data:
            entities.append(GoogleFindMyLastSeenSensor(coordinator, device))

    async_add_entities(entities)


class GoogleFindMyStatsSensor(CoordinatorEntity, SensorEntity):
    """Sensor for Google Find My Device statistics."""

    def __init__(self, coordinator, stat_key: str, stat_name: str):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._stat_key = stat_key
        self._stat_name = stat_name
        self._attr_name = f"Google Find My {stat_name}"
        self._attr_unique_id = f"{DOMAIN}_{stat_key}"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_native_unit_of_measurement = "updates"

    @property
    def state(self):
        """Return the state of the sensor."""
        value = self.coordinator.stats.get(self._stat_key, 0)
        _LOGGER.debug(f"Sensor {self._stat_name} returning value {value}")
        return value

    @property
    def icon(self):
        """Return the icon for the sensor."""
        if "duplicate" in self._stat_key:
            return "mdi:cancel"
        elif "background" in self._stat_key:
            return "mdi:cloud-download"
        elif "crowd" in self._stat_key:
            return "mdi:account-group"
        return "mdi:counter"

    @property
    def device_info(self) -> dict[str, Any]:
        """Return device info for integration device."""
        return {
            "identifiers": {(DOMAIN, "integration")},
            "name": "Google Find My Integration",
            "manufacturer": "BSkando",
            "model": "Find My Device Integration",
            "configuration_url": "https://github.com/BSkando/GoogleFindMy-HA",
        }



class GoogleFindMyLastSeenSensor(CoordinatorEntity, SensorEntity):
    """Sensor showing last_seen timestamp for each device."""

    def __init__(self, coordinator, device: dict[str, Any]):
        """Initialize the sensor."""
        super().__init__(coordinator)
        self._device = device
        self._device_id = device["id"]
        self._device_name = device["name"]
        self._attr_name = "Last Seen"
        self._attr_unique_id = f"{DOMAIN}_{self._device_id}_last_seen"
        self._attr_device_class = "timestamp"
        self._attr_has_entity_name = True

    @property
    def state(self):
        """Return the last_seen timestamp."""
        device_data = self.coordinator._device_location_data.get(self._device_id, {})
        last_seen = device_data.get('last_seen')
        if last_seen:
            import datetime
            return datetime.datetime.fromtimestamp(last_seen).isoformat()
        return None

    @property
    def icon(self):
        """Return the icon for the sensor."""
        return "mdi:clock-outline"

    @property
    def device_info(self) -> dict[str, Any]:
        """Return device info."""
        # Get Home Assistant base URL using proper HA methods
        from homeassistant.helpers.network import get_url

        try:
            # Try to get the best available URL, preferring external access
            base_url = get_url(self.hass, prefer_external=True, allow_cloud=True, allow_external=True, allow_internal=True)
        except Exception:
            base_url = "http://homeassistant.local:8123"

        # Generate auth token for map access
        auth_token = self._get_map_token()

        return {
            "identifiers": {(DOMAIN, self._device["id"])},
            "name": self._device["name"],
            "manufacturer": "Google",
            "model": "Find My Device",
            "configuration_url": f"{base_url}/api/googlefindmy/map/{self._device['id']}?token={auth_token}",
            "hw_version": self._device["id"],
        }

    def _get_map_token(self) -> str:
        """Generate a simple token for map authentication.

        Weekly-rotating token when enabled; otherwise a static token.
        """
        import hashlib
        import time
        from .const import DEFAULT_MAP_VIEW_TOKEN_EXPIRATION

        # Check if token expiration is enabled - prefer options over data
        config_entry = getattr(self.coordinator, "config_entry", None)
        if config_entry:
            token_expiration_enabled = config_entry.options.get(
                "map_view_token_expiration",
                config_entry.data.get("map_view_token_expiration", DEFAULT_MAP_VIEW_TOKEN_EXPIRATION)
            )
        else:
            token_expiration_enabled = DEFAULT_MAP_VIEW_TOKEN_EXPIRATION

        ha_uuid = str(self.hass.data.get("core.uuid", "ha"))

        if token_expiration_enabled:
            # Weekly-rolling token (7-day bucket)
            week = str(int(time.time() // 604800))
            token_src = f"{ha_uuid}:{week}"
        else:
            # Static token (no rotation)
            token_src = f"{ha_uuid}:static"

        return hashlib.md5(token_src.encode()).hexdigest()[:16]