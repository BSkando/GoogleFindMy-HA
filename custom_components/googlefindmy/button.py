"""Button platform for Google Find My Device."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import GoogleFindMyCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Google Find My Device button entities."""
    coordinator: GoogleFindMyCoordinator = hass.data[DOMAIN][config_entry.entry_id]

    entities = []
    if coordinator.data:
        for device in coordinator.data:
            entities.append(GoogleFindMyPlaySoundButton(coordinator, device))

    async_add_entities(entities, True)


class GoogleFindMyPlaySoundButton(CoordinatorEntity, ButtonEntity):
    """Representation of a Google Find My Device play sound button."""

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
    ) -> None:
        """Initialize the button."""
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{DOMAIN}_{device['id']}_play_sound"
        self._attr_name = f"{device['name']} Play Sound"
        self._attr_icon = "mdi:volume-high"
        self._attr_has_entity_name = True

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

    async def async_press(self) -> None:
        """Handle the button press."""
        device_id = self._device["id"]
        device_name = self._device["name"]

        _LOGGER.debug(f"Play sound button pressed for {device_name} ({device_id})")
        
        try:
            result = await self.coordinator.async_play_sound(device_id)
            if result:
                _LOGGER.info(f"Successfully played sound on {device_name}")
            else:
                _LOGGER.warning(f"Failed to play sound on {device_name}")
        except Exception as err:
            _LOGGER.error(f"Error playing sound on {device_name}: {err}")