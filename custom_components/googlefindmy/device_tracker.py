"""Device tracker platform for Google Find My Device."""
from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any

from homeassistant.components.device_tracker import SourceType, TrackerEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    ATTR_GPS_ACCURACY,  # use HA core constant
    ATTR_LATITUDE,
    ATTR_LONGITUDE,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.network import get_url
from homeassistant.helpers.restore_state import RestoreEntity
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DEFAULT_MAP_VIEW_TOKEN_EXPIRATION, DOMAIN
from .coordinator import GoogleFindMyCoordinator

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Google Find My Device tracker entities."""
    coordinator: GoogleFindMyCoordinator = hass.data[DOMAIN][config_entry.entry_id]

    # Explicit typing for readability and IDE support
    entities: list[GoogleFindMyDeviceTracker] = []
    known_ids: set[str] = set()

    # --- Startup: create entities from existing data or tracked IDs (skeletons) ---
    if coordinator.data:
        for device in coordinator.data:
            if device.get("id") and device.get("name"):
                known_ids.add(device["id"])
                entities.append(GoogleFindMyDeviceTracker(coordinator, device))
            else:
                _LOGGER.warning("Skipping device due to missing 'id' or 'name': %s", device)
    else:
        # No live data yet (early startup). Create skeleton entities from configured tracked IDs.
        tracked_ids: list[str] = getattr(coordinator, "tracked_devices", []) or []
        name_map: dict[str, str] = getattr(coordinator, "_device_names", {})  # noqa: SLF001
        for dev_id in tracked_ids:
            name = name_map.get(dev_id) or f"Find My - {dev_id}"
            known_ids.add(dev_id)
            entities.append(GoogleFindMyDeviceTracker(coordinator, {"id": dev_id, "name": name}))
        if tracked_ids:
            _LOGGER.debug(
                "Created %d skeleton device_tracker entities for restore (no live data yet)",
                len(tracked_ids),
            )

    if entities:
        async_add_entities(entities, True)

    # --- Dynamic add: attach a coordinator listener to add *new* trackers later ---
    @callback
    def _sync_entities_from_coordinator() -> None:
        """Add new tracker entities when the coordinator learns about new devices."""
        if not coordinator.data:
            return

        to_add: list[GoogleFindMyDeviceTracker] = []
        for device in coordinator.data:
            dev_id = device.get("id")
            name = device.get("name")
            if not dev_id or not name:
                continue
            if dev_id in known_ids:
                continue
            known_ids.add(dev_id)
            to_add.append(GoogleFindMyDeviceTracker(coordinator, device))

        if to_add:
            _LOGGER.info("Adding %d newly discovered Find My tracker(s)", len(to_add))
            async_add_entities(to_add, True)

    # Register listener and clean it up on unload
    unsub = coordinator.async_add_listener(_sync_entities_from_coordinator)
    config_entry.async_on_unload(unsub)

    # Run once in case data arrived between setup and listener registration
    _sync_entities_from_coordinator()


class GoogleFindMyDeviceTracker(CoordinatorEntity, TrackerEntity, RestoreEntity):
    """Representation of a Google Find My Device tracker."""

    _attr_has_entity_name = False
    _attr_source_type = SourceType.GPS
    _attr_entity_category = None  # Ensure device trackers are not diagnostic

    def __init__(
        self,
        coordinator: GoogleFindMyCoordinator,
        device: dict[str, Any],
    ) -> None:
        """Initialize the tracker."""
        super().__init__(coordinator)
        self._device = device
        self._attr_unique_id = f"{DOMAIN}_{device['id']}"
        self._attr_name = device["name"]
        # Track last good accuracy location for database writes
        self._last_good_accuracy_data: dict[str, Any] | None = None

    async def async_added_to_hass(self) -> None:
        """Restore last known location via HA's persistent state store.

        Seed our internal/coordinator cache so the entity has coordinates
        immediately after a restart, until fresh data arrives.
        """
        await super().async_added_to_hass()

        try:
            last_state = await self.async_get_last_state()
        except Exception as err:  # noqa: BLE001
            _LOGGER.warning("Failed to get last state for %s: %s", self.entity_id, err)
            return

        if not last_state:
            return

        # Standard device_tracker attributes (with safe fallbacks)
        lat = last_state.attributes.get(ATTR_LATITUDE, last_state.attributes.get("latitude"))
        lon = last_state.attributes.get(ATTR_LONGITUDE, last_state.attributes.get("longitude"))
        acc = last_state.attributes.get(ATTR_GPS_ACCURACY, last_state.attributes.get("gps_accuracy"))

        restored: dict[str, Any] = {}
        try:
            if lat is not None and lon is not None:
                restored["latitude"] = float(lat)
                restored["longitude"] = float(lon)
            if acc is not None:
                restored["accuracy"] = int(acc)
        except (TypeError, ValueError) as ex:
            _LOGGER.debug("Invalid restored coordinates for %s: %s", self.entity_id, ex)
            restored = {}

        if restored:
            self._last_good_accuracy_data = {**restored}

            # Prime coordinator cache used elsewhere (best-effort).
            dev_id = self._device["id"]
            try:
                if hasattr(self.coordinator, "prime_device_location_cache"):
                    # Expected: prime_device_location_cache(device_id: str, data: dict[str, Any]) -> None
                    self.coordinator.prime_device_location_cache(dev_id, restored)  # type: ignore[attr-defined]
                else:
                    mapping = getattr(self.coordinator, "_device_location_data", None)  # noqa: SLF001
                    if isinstance(mapping, dict):
                        slot = mapping.get(dev_id, {})
                        slot.update(restored)
                        mapping[dev_id] = slot
                    else:
                        setattr(self.coordinator, "_device_location_data", {dev_id: restored})  # noqa: SLF001
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Failed to seed coordinator cache for %s: %s", self.entity_id, err)

            self.async_write_ha_state()

    @property
    def device_info(self) -> DeviceInfo:
        """Return device info.

        NOTE (prep for later path refactor):
        Build the *path* separately so we can switch to returning a relative
        configuration_url in a later step without touching other code.
        """
        # Generate auth token and build path first
        auth_token = self._get_map_token()
        path = self._build_map_path(self._device["id"], auth_token, redirect=False)

        # Today: still return absolute URL; redirect endpoint handles origin correctly
        try:
            base_url = get_url(
                self.hass,
                prefer_external=True,   # prefer URL that also works from remote/cloud
                allow_cloud=True,
                allow_external=True,
                allow_internal=True,
            )
        except Exception as e:  # noqa: BLE001
            _LOGGER.warning("Could not determine Home Assistant URL, using fallback: %s", e)
            base_url = "http://homeassistant.local:8123"

        return DeviceInfo(
            identifiers={(DOMAIN, self._device["id"])},
            name=self._device["name"],
            manufacturer="Google",
            model="Find My Device",
            configuration_url=f"{base_url}{path}",  # later: just `path`
            hw_version=self._device["id"],  # Show device ID as hardware version for easy copying
        )

    def _build_map_path(self, device_id: str, token: str, *, redirect: bool = False) -> str:
        """Return the map URL *path* (no scheme/host)."""
        if redirect:
            return f"/api/googlefindmy/redirect_map/{device_id}?token={token}"
        return f"/api/googlefindmy/map/{device_id}?token={token}"

    @property
    def _current_device_data(self) -> dict[str, Any] | None:
        """Get current device data from coordinator's location cache."""
        dev_id = self._device["id"]
        if hasattr(self.coordinator, "get_device_location_data"):
            # Expected: get_device_location_data(device_id: str) -> dict[str, Any] | None
            try:
                return self.coordinator.get_device_location_data(dev_id)  # type: ignore[attr-defined]
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Coordinator public API failed for %s: %s", dev_id, err)
        return getattr(self.coordinator, "_device_location_data", {}).get(dev_id)  # noqa: SLF001

    @property
    def _data_to_persist(self) -> dict[str, Any] | None:
        """Return data used for persistent state (lat/lon/accuracy)."""
        return self._last_good_accuracy_data or self._current_device_data

    @property
    def available(self) -> bool:
        """Return True if entity has valid location data.

        If coordinator has no data yet, but we restored a valid location,
        expose the entity as available to avoid 'unavailable' after reboot.
        """
        device_data = self._current_device_data
        if device_data:
            if (
                device_data.get("latitude") is not None
                and device_data.get("longitude") is not None
            ) or device_data.get("semantic_name") is not None:
                return True
        # Fallback to restored data (seeded in async_added_to_hass)
        return self._last_good_accuracy_data is not None

    @property
    def latitude(self) -> float | None:
        """Return latitude value of the device."""
        if data := self._data_to_persist:
            return data.get("latitude")
        return None

    @property
    def longitude(self) -> float | None:
        """Return longitude value of the device."""
        if data := self._data_to_persist:
            return data.get("longitude")
        return None

    @property
    def location_accuracy(self) -> int | None:
        """Return accuracy of location."""
        if data := self._data_to_persist:
            return data.get("accuracy")
        return None

    @property
    def location_name(self) -> str | None:
        """Return the location name (zone or semantic location)."""
        if device_data := self._current_device_data:
            return device_data.get("semantic_name")
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attributes: dict[str, Any] = {}
        if device_data := self._current_device_data:
            if last_seen_ts := device_data.get("last_seen"):
                attributes["last_seen"] = datetime.fromtimestamp(last_seen_ts).isoformat()
            if altitude := device_data.get("altitude"):
                attributes["altitude"] = altitude
            if status := device_data.get("status"):
                attributes["device_status"] = status
            if (is_own := device_data.get("is_own_report")) is not None:
                attributes["is_own_report"] = is_own
            if semantic_name := device_data.get("semantic_name"):
                attributes["semantic_location"] = semantic_name
        return attributes

    def _get_map_token(self) -> str:
        """Generate a simple token for map authentication.

        Options-first: prefer config_entry.options over data; fallback to default.
        """
        config_entry = getattr(self.coordinator, "config_entry", None)
        if config_entry:
            token_expiration_enabled = config_entry.options.get(
                "map_view_token_expiration",
                config_entry.data.get("map_view_token_expiration", DEFAULT_MAP_VIEW_TOKEN_EXPIRATION),
            )
        else:
            token_expiration_enabled = DEFAULT_MAP_VIEW_TOKEN_EXPIRATION

        ha_uuid = str(self.hass.data.get("core.uuid", "ha"))

        if token_expiration_enabled:
            # Weekly-rolling token (7-day bucket)
            week = str(int(time.time() // 604800))
            secret = f"{ha_uuid}:{week}"
        else:
            # Static token (no rotation)
            secret = f"{ha_uuid}:static"

        return hashlib.md5(secret.encode()).hexdigest()[:16]

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        # Update display name if the coordinator learned a better one
        try:
            name_map: dict[str, str] = getattr(self.coordinator, "_device_names", {})  # noqa: SLF001
            new_name = name_map.get(self._device["id"])
            if new_name and new_name != self._attr_name:
                self._attr_name = new_name
        except Exception:  # noqa: BLE001
            pass

        config_entry = getattr(self.coordinator, "config_entry", None)
        if config_entry:
            min_accuracy_threshold = config_entry.options.get("min_accuracy_threshold", 0)
        else:
            # Legacy fallback (kept for backward compatibility)
            cfg = self.hass.data.get(DOMAIN, {}).get("config_data", {})
            min_accuracy_threshold = cfg.get("min_accuracy_threshold", 0)

        device_data = self._current_device_data
        if not device_data:
            self.async_write_ha_state()
            return

        accuracy = device_data.get("accuracy")
        lat = device_data.get("latitude")
        lon = device_data.get("longitude")

        # Update last good data if accuracy filtering is off or the new data is good enough
        is_good = (
            min_accuracy_threshold <= 0
            or (accuracy is not None and lat is not None and lon is not None and accuracy <= min_accuracy_threshold)
        )

        if is_good:
            self._last_good_accuracy_data = device_data.copy()
            if min_accuracy_threshold > 0 and accuracy is not None:
                _LOGGER.debug(
                    "Updated last good accuracy data for %s: accuracy=%sm (threshold=%sm)",
                    self.name,
                    accuracy,
                    min_accuracy_threshold,
                )
        elif accuracy is not None:
            _LOGGER.info(
                "Keeping previous good data for %s: current accuracy=%sm > threshold=%sm",
                self.name,
                accuracy,
                min_accuracy_threshold,
            )

        self.async_write_ha_state()
