"""Google Home device filtering utilities."""
from __future__ import annotations

import logging
import time
from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.components.zone import DOMAIN as ZONE_DOMAIN

from .const import GOOGLE_HOME_SPAM_THRESHOLD_MINUTES

_LOGGER = logging.getLogger(__name__)


class GoogleHomeFilter:
    """Handles Google Home device filtering and spam prevention."""

    def __init__(self, hass: HomeAssistant, config_data: dict[str, Any]) -> None:
        """Initialize the Google Home filter."""
        self.hass = hass
        self._enabled = config_data.get("google_home_filter_enabled", True)
        from .const import DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS
        keywords_string = config_data.get("google_home_filter_keywords", DEFAULT_GOOGLE_HOME_FILTER_KEYWORDS)
        self._keywords = self._parse_keywords(keywords_string)
        self._spam_tracking = {}  # Track last detection times per device
        self._spam_threshold = GOOGLE_HOME_SPAM_THRESHOLD_MINUTES * 60  # Convert to seconds


    def _parse_keywords(self, keywords_string: str) -> list[str]:
        """Parse comma-separated keywords into a list."""
        if not keywords_string:
            return []

        keywords = [keyword.strip().lower() for keyword in keywords_string.split(",")]
        return [keyword for keyword in keywords if keyword]  # Remove empty strings

    def is_google_home_device(self, location_name: str) -> bool:
        """Check if location name matches Google Home device keywords."""
        if not self._enabled or not self._keywords:
            return False

        location_lower = location_name.lower()
        return any(keyword in location_lower for keyword in self._keywords)

    def get_home_zone_name(self) -> str | None:
        """Get the Home zone name from Home Assistant."""
        try:
            zone_states = self.hass.states.async_all(ZONE_DOMAIN)

            for state in zone_states:
                # Home zone typically has entity_id "zone.home"
                if state.entity_id == "zone.home":
                    return state.attributes.get("friendly_name", "Home")

            # Fallback: look for any zone with "home" in the name
            for state in zone_states:
                if "home" in state.entity_id.lower() or "home" in str(state.attributes.get("friendly_name", "")).lower():
                    return state.attributes.get("friendly_name", "Home")

            # Default fallback
            return "Home"

        except Exception as e:
            _LOGGER.warning("Failed to get Home zone name: %s", e)
            return "Home"

    def is_device_at_home(self, device_id: str) -> bool:
        """Check if device is currently in the Home zone."""
        try:
            # Look for device tracker entity
            entity_id = f"device_tracker.{device_id.lower().replace(' ', '_')}"
            state = self.hass.states.get(entity_id)

            if state is None:
                # Try with domain prefix
                entity_id = f"device_tracker.googlefindmy_{device_id}"
                state = self.hass.states.get(entity_id)

            if state is None:
                _LOGGER.debug("Could not find device tracker entity for %s", device_id)
                return False

            # Check if state is "home" (case insensitive)
            current_zone = str(state.state).lower()
            return current_zone == "home"

        except Exception as e:
            _LOGGER.debug("Error checking if device %s is at home: %s", device_id, e)
            return False

    def should_filter_detection(self, device_id: str, location_name: str) -> tuple[bool, str | None]:
        """
        Determine if detection should be filtered and what location to use.

        Returns:
            tuple: (should_filter, replacement_location)
            - should_filter: True if detection should be discarded
            - replacement_location: New location name if substitution needed, None otherwise
        """
        if not self._enabled:
            return False, None

        # Check if this is a Google Home device detection OR already a Home zone detection
        is_google_home = self.is_google_home_device(location_name)
        is_home_zone = location_name.lower() in ["home", self.get_home_zone_name().lower() if self.get_home_zone_name() else ""]

        if not is_google_home and not is_home_zone:
            return False, None  # Not a Google Home device or Home zone, don't filter

        # Check if device is already at Home
        if self.is_device_at_home(device_id):
            # Device already at Home - check spam prevention
            if self._should_prevent_spam(device_id):
                _LOGGER.debug("Filtering out spam detection for %s at %s", device_id, location_name)
                return True, None  # Filter out to prevent spam
            else:
                # Allow this detection but don't substitute location
                self._update_spam_tracking(device_id)
                return False, None
        else:
            # Device NOT at Home - substitute with Home zone
            home_zone = self.get_home_zone_name()
            _LOGGER.info("Device %s detected at Google Home device '%s', substituting with '%s'",
                        device_id, location_name, home_zone)
            self._update_spam_tracking(device_id)
            return False, home_zone

    def _should_prevent_spam(self, device_id: str) -> bool:
        """Check if we should prevent spam for this device."""
        last_detection = self._spam_tracking.get(device_id)

        if last_detection is None:
            return False  # First detection, don't prevent

        current_time = time.time()
        time_since_last = current_time - last_detection

        return time_since_last < self._spam_threshold

    def _update_spam_tracking(self, device_id: str) -> None:
        """Update the spam tracking timestamp for a device."""
        self._spam_tracking[device_id] = time.time()

    def reset_spam_tracking(self, device_id: str) -> None:
        """Reset spam tracking when device leaves Home zone."""
        if device_id in self._spam_tracking:
            del self._spam_tracking[device_id]
            _LOGGER.debug("Reset spam tracking for %s (left Home zone)", device_id)

    def update_config(self, config_data: dict[str, Any]) -> None:
        """Update filter configuration."""
        self._enabled = config_data.get("google_home_filter_enabled", True)
        self._keywords = self._parse_keywords(config_data.get("google_home_filter_keywords", ""))
        _LOGGER.info("Updated Google Home filter config: enabled=%s, keywords=%s",
                    self._enabled, self._keywords)