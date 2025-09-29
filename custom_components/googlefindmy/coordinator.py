"""Data coordinator for Google Find My Device."""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
import time

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN, UPDATE_INTERVAL
from .api import GoogleFindMyAPI
from .storage import LastKnownStore

_LOGGER = logging.getLogger(__name__)


class GoogleFindMyCoordinator(DataUpdateCoordinator):
    """Manage fetching and caching Google Find My Device data."""

    def __init__(self, hass: HomeAssistant, secrets_data: dict, tracked_devices: list = None, location_poll_interval: int = 300, device_poll_delay: int = 5, min_poll_interval: int = 120, min_accuracy_threshold: int = 100) -> None:
        """Initialize the coordinator."""
        self.api = GoogleFindMyAPI(secrets_data=secrets_data)

        self.tracked_devices = tracked_devices or []
        self.location_poll_interval = location_poll_interval  # Configured polling interval
        self.device_poll_delay = device_poll_delay
        self.min_poll_interval = min_poll_interval  # Minimum time between polls
        self._min_accuracy_threshold = min_accuracy_threshold  # Accuracy filter threshold
        
        # Sequential polling state
        self._device_location_data = {}  # Latest location data per device (live session cache)
        self._current_device_index = 0
        self._last_location_poll_time = 0
        self._device_names = {}  # Device ID -> name

        # Persistence: simple JSON store for last known valid positions
        self._store = LastKnownStore(hass)
        self.last_known_locations: dict[str, dict] = {}

        # Statistics (loaded from cache or start at zero)
        self.stats = {
            "skipped_duplicates": 0,
            "background_updates": 0,
            "crowd_sourced_updates": 0,
        }
        _LOGGER.debug(f"Initialized stats: {self.stats}")

        # Pre-load stats from cache
        hass.async_create_task(self._async_load_stats())

        # Polling state
        self._is_polling = False
        self._startup_complete = False

        # On startup: load persisted last-known positions and expose them in the live cache
        hass.async_create_task(self._async_load_last_known())

        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )

    async def _async_update_data(self):
        """Fetch and prepare data for entities."""
        try:
            # Get basic device list first
            all_devices = await self.hass.async_add_executor_job(self.api.get_basic_device_list)
            
            # Filter by configured list (if provided)
            if self.tracked_devices:
                devices = [dev for dev in all_devices if dev["id"] in self.tracked_devices]
            else:
                devices = all_devices
            
            # Update ID -> name map
            for device in devices:
                self._device_names[device["id"]] = device["name"]
            
            current_time = time.time()
            
            # Determine whether a new location poll should run
            time_since_last_poll = current_time - self._last_location_poll_time

            # On first update after startup: set a baseline without immediate polling
            if not self._startup_complete:
                self._startup_complete = True
                self._last_location_poll_time = current_time
                _LOGGER.debug("First startup - setting poll baseline, will poll on normal schedule")
                should_poll_location = False
                time_since_last_poll = 0
            else:
                should_poll_location = (time_since_last_poll >= self.location_poll_interval)

            _LOGGER.debug(
                "Poll check: time_since_last_poll=%.1fs, interval=%ss, should_poll=%s, devices=%d",
                time_since_last_poll, self.location_poll_interval, should_poll_location, len(devices) if devices else 0
            )

            if should_poll_location and devices:
                # Mark polling running and notify listeners
                self._is_polling = True
                _LOGGER.debug("Started polling cycle")
                self.async_set_updated_data(self.data)

                # Poll all devices sequentially with timeouts
                _LOGGER.info("Starting sequential poll of %d devices", len(devices))

                for i, device_to_poll in enumerate(devices):
                    device_id = device_to_poll["id"]
                    device_name = device_to_poll["name"]

                    _LOGGER.info(
                        "Sequential poll: requesting location for device %s (%d/%d)",
                        device_name, i + 1, len(devices)
                    )

                    try:
                        # Call async location function with a hard timeout
                        location_data = await asyncio.wait_for(
                            self.api.async_get_device_location(device_id, device_name),
                            timeout=30.0
                        )

                        # Log age, but do not reject by age alone
                        if location_data:
                            last_seen = location_data.get('last_seen', 0)
                            if last_seen > 0:
                                location_age_hours = (current_time - last_seen) / 3600
                                if location_age_hours > 24:
                                    _LOGGER.info(
                                        "Using old location data for %s (age: %.1fh)",
                                        device_name, location_age_hours
                                    )
                                elif location_age_hours > 1:
                                    _LOGGER.debug(
                                        "Using location data for %s (age: %.1fh)",
                                        device_name, location_age_hours
                                    )

                        if location_data:
                            # Validate location fields
                            lat = location_data.get('latitude')
                            lon = location_data.get('longitude')
                            acc = location_data.get('accuracy')
                            last_seen = location_data.get('last_seen', 0)

                            # Refined coordinate handling:
                            # - both None => debug (no coordinates yet)
                            # - one None  => warning (incomplete)
                            # - both present but out of range => warning (invalid range)
                            # - both present and valid => process as before
                            if lat is not None and lon is not None:
                                if -90 <= lat <= 90 and -180 <= lon <= 180:
                                    # Skip duplicates when last_seen unchanged
                                    existing_data = self._device_location_data.get(device_id, {})
                                    existing_last_seen = existing_data.get('last_seen')

                                    if last_seen != existing_last_seen:
                                        # New data
                                        if last_seen > 0:
                                            location_age_hours = (current_time - last_seen) / 3600
                                            _LOGGER.info(
                                                "Got valid location for %s: lat=%s, lon=%s, accuracy=%sm, age=%.1fh",
                                                device_name, lat, lon, acc, location_age_hours
                                            )
                                        else:
                                            _LOGGER.info(
                                                "Got valid location for %s: lat=%s, lon=%s, accuracy=%sm",
                                                device_name, lat, lon, acc
                                            )

                                        self._device_location_data[device_id] = location_data
                                        self._device_location_data[device_id]["last_updated"] = current_time

                                        # Stats
                                        self.increment_stat("background_updates")

                                        # Persist last-known position asynchronously (non-blocking)
                                        self.last_known_locations[device_id] = {
                                            "latitude": lat,
                                            "longitude": lon,
                                            "accuracy": acc,
                                            "last_seen": last_seen,
                                            "last_updated": self._device_location_data[device_id].get("last_updated", current_time),
                                            "status": self._device_location_data[device_id].get("status"),
                                            "is_own_report": location_data.get("is_own_report"),
                                            "semantic_name": location_data.get("semantic_name"),
                                        }
                                        self.hass.async_create_task(self._store.async_save(self.last_known_locations))
                                    else:
                                        _LOGGER.debug(
                                            "Skipping duplicate location data for %s (same last_seen: %s)",
                                            device_name, last_seen
                                        )
                                        self.increment_stat("skipped_duplicates")
                                else:
                                    _LOGGER.warning(
                                        "Invalid coordinate range for %s: lat=%s, lon=%s",
                                        device_name, lat, lon
                                    )
                            else:
                                if lat is None and lon is None:
                                    _LOGGER.debug(
                                        "No coordinates for %s: lat=None, lon=None (preserving previous data)",
                                        device_name
                                    )
                                else:
                                    _LOGGER.warning(
                                        "Incomplete coordinates for %s: lat=%s, lon=%s",
                                        device_name, lat, lon
                                    )
                        else:
                            _LOGGER.warning("No location data returned for %s", device_name)

                    except asyncio.TimeoutError:
                        _LOGGER.warning("Location request timed out for %s after 30 seconds", device_name)
                    except Exception as e:
                        _LOGGER.error("Failed to get location for %s: %s", device_name, e)
                        # Keep previous data on error

                    # Delay before next device (not after last)
                    if i < len(devices) - 1:
                        _LOGGER.debug("Waiting %ds before next device poll", self.device_poll_delay)
                        await asyncio.sleep(self.device_poll_delay)

                # Clear polling flag and notify listeners
                self._is_polling = False
                _LOGGER.debug("Completed polling cycle for %d devices", len(devices))
                self.async_set_updated_data(self.data)

                # Update poll timestamp
                self._last_location_poll_time = current_time
            
            # Build list for entities, merging cached location data
            device_data = []
            for device in devices:
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
                
                # Prefer live session cache
                if device["id"] in self._device_location_data:
                    location_data = self._device_location_data[device["id"]]
                    device_info.update(location_data)
                    _LOGGER.debug(
                        "Applied cached location for %s: lat=%s, lon=%s, acc=%s",
                        device_info['name'],
                        device_info.get('latitude'),
                        device_info.get('longitude'),
                        device_info.get('accuracy'),
                    )

                    # Derive simple status from age
                    last_updated = location_data.get("last_updated", 0)
                    data_age = current_time - last_updated
                    if data_age < self.location_poll_interval:
                        device_info["status"] = "Location data current"
                    elif data_age < self.location_poll_interval * 2:
                        device_info["status"] = "Location data aging"
                    else:
                        device_info["status"] = "Location data stale"
                else:
                    # Fallback: try HA registry/state history when no live cached data
                    from homeassistant.helpers import entity_registry as er
                    ent_reg = er.async_get(self.hass)
                    unique_id = f"{DOMAIN}_{device['id']}"
                    entity_entry = ent_reg.async_get_entity_id("device_tracker", DOMAIN, unique_id)

                    if entity_entry:
                        entity_id = entity_entry
                        _LOGGER.debug(
                            "Found entity_id '%s' for device '%s' from registry",
                            entity_id, device_info['name']
                        )
                    else:
                        # Guess entity_id from name as last resort
                        entity_id = f"device_tracker.{device_info['name'].lower().replace(' ', '_').replace(\"'\", '')}"
                        _LOGGER.debug(
                            "No registry entry found, trying '%s' for device '%s'",
                            entity_id, device_info['name']
                        )

                    try:
                        state = self.hass.states.get(entity_id)
                        _LOGGER.debug("State for %s: %s", entity_id, state.state if state else 'None')

                        # If missing in current state, query recorder database for most recent coordinates
                        if not state:
                            _LOGGER.debug("Entity %s not found in current state, querying database", entity_id)
                            try:
                                import sqlite3
                                db_path = self.hass.config.path("home-assistant_v2.db")

                                # Try multiple name formats
                                entity_patterns = [
                                    entity_id,
                                    f"device_tracker.{device_info['name'].lower().replace(' ', '_').replace(\"'\", '')}",
                                    f"device_tracker.{device_info['name'].lower().replace(' ', '_')}",
                                ]

                                conn = sqlite3.connect(db_path)
                                cursor = conn.cursor()

                                for pattern in entity_patterns:
                                    _LOGGER.debug("Trying pattern '%s'", pattern)
                                    query = """
                                    SELECT s.state, sa.shared_attrs
                                    FROM states s
                                    JOIN state_attributes sa ON s.attributes_id = sa.attributes_id
                                    WHERE s.entity_id = ?
                                    AND sa.shared_attrs LIKE '%latitude%'
                                    AND sa.shared_attrs LIKE '%longitude%'
                                    ORDER BY s.last_updated_ts DESC
                                    LIMIT 1
                                    """
                                    cursor.execute(query, (pattern,))
                                    result = cursor.fetchone()

                                    if result:
                                        import json
                                        attrs = json.loads(result[1])
                                        lat = attrs.get('latitude')
                                        lon = attrs.get('longitude')
                                        acc = attrs.get('gps_accuracy')

                                        if lat is not None and lon is not None:
                                            _LOGGER.debug("Found DB data for %s: lat=%s, lon=%s", pattern, lat, lon)

                                            class FakeState:
                                                def __init__(self, state_val, attrs):
                                                    self.state = state_val
                                                    self.attributes = attrs

                                            state = FakeState(result[0], attrs)
                                            entity_id = pattern
                                            break

                                conn.close()
                            except Exception as e:
                                _LOGGER.error("Database query failed for %s: %s", device_info['name'], e, exc_info=True)

                        if state:
                            # Copy coordinates from attributes even if state is 'unavailable'
                            lat = state.attributes.get('latitude')
                            lon = state.attributes.get('longitude')
                            acc = state.attributes.get('gps_accuracy')
                            _LOGGER.debug("Attributes for %s: lat=%s, lon=%s, acc=%s", entity_id, lat, lon, acc)

                            if lat is not None and lon is not None:
                                device_info.update({
                                    "latitude": lat,
                                    "longitude": lon,
                                    "accuracy": acc,
                                    "status": "Using historical data"
                                })
                                _LOGGER.info("Using historical location for %s: lat=%s, lon=%s", device_info['name'], lat, lon)
                            else:
                                _LOGGER.debug("No historical location data for %s (id=%s)", device_info['name'], device['id'])
                        else:
                            _LOGGER.debug("No state found for %s (id=%s)", device_info['name'], device['id'])
                    except Exception as e:
                        _LOGGER.debug("Error retrieving historical data for %s: %s", device_info['name'], e)
                        _LOGGER.debug("No cached location data for %s (id=%s)", device_info['name'], device['id'])
                
                device_data.append(device_info)
                
            _LOGGER.debug(
                "Sequential polling: processed %d devices, next poll in %ds",
                len(devices),
                max(0, int(self.location_poll_interval - time_since_last_poll))
            )

            # Final debug of the return payload
            for device in device_data:
                _LOGGER.debug(
                    "Returning device %s: lat=%s, lon=%s, semantic=%s",
                    device['name'],
                    device.get('latitude'),
                    device.get('longitude'),
                    device.get('semantic_name'),
                )

            return device_data
            
        except Exception as exception:
            raise UpdateFailed(exception) from exception

    async def _async_load_stats(self):
        """Load statistics from the persistent cache."""
        try:
            from .Auth.token_cache import async_get_cached_value
            cached_stats = await async_get_cached_value("integration_stats")
            if cached_stats and isinstance(cached_stats, dict):
                for key in self.stats.keys():
                    if key in cached_stats:
                        self.stats[key] = cached_stats[key]
                _LOGGER.debug("Loaded statistics from cache: %s", self.stats)
        except Exception as e:
            _LOGGER.debug("Failed to load statistics from cache: %s", e)

    async def _async_save_stats(self):
        """Persist statistics to the cache (fire-and-forget)."""
        try:
            from .Auth.token_cache import async_set_cached_value
            await async_set_cached_value("integration_stats", self.stats.copy())
        except Exception as e:
            _LOGGER.debug("Failed to save statistics to cache: %s", e)

    def increment_stat(self, stat_name: str):
        """Increment a statistic and schedule a background save."""
        if stat_name in self.stats:
            old_value = self.stats[stat_name]
            self.stats[stat_name] += 1
            _LOGGER.debug("Incremented %s from %s to %s", stat_name, old_value, self.stats[stat_name])
            self.hass.async_create_task(self._async_save_stats())
        else:
            _LOGGER.warning("Tried to increment unknown stat %s, available: %s", stat_name, list(self.stats.keys()))

    async def async_locate_device(self, device_id: str) -> dict:
        """Trigger a one-off locate for a device."""
        return await self.hass.async_add_executor_job(
            self.api.locate_device, device_id
        )

    async def async_play_sound(self, device_id: str) -> bool:
        """Trigger a play-sound action on a device."""
        return await self.hass.async_add_executor_job(
            self.api.play_sound, device_id
        )

    async def _async_load_last_known(self) -> None:
        """Load persisted last-known positions and seed the live cache."""
        try:
            loaded = await self._store.async_load()
            if not loaded:
                _LOGGER.debug("No last-known locations found in storage")
                return
            self.last_known_locations = loaded

            # Seed live cache for devices missing any location (so UI can render immediately)
            now_ts = time.time()
            applied = 0
            for dev_id, data in loaded.items():
                if dev_id not in self._device_location_data:
                    merged = dict(data)
                    merged.setdefault("status", "Loaded last known location")
                    merged.setdefault("last_updated", now_ts)
                    merged["source"] = "store"
                    self._device_location_data[dev_id] = merged
                    applied += 1
            if applied:
                _LOGGER.debug("Applied %d last-known location(s) from storage to cache", applied)
                # Notify listeners if the coordinator is already wired up
                try:
                    self.async_set_updated_data(self.data)
                except Exception:
                    # Avoid crashing during early startup stages
                    pass
        except Exception as err:
            _LOGGER.debug("Failed to load last-known locations: %s", err)
