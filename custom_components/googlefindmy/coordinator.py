"""Data coordinator for Google Find My Device."""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
import time
from typing import Any, Dict, List, Optional

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.helpers import entity_registry as er
from homeassistant.components.recorder import (
    get_instance as get_recorder,
    history as recorder_history,
)

from .const import DOMAIN, UPDATE_INTERVAL
from .api import GoogleFindMyAPI

_LOGGER = logging.getLogger(__name__)


# -------------------------------------------------------------------------
# Synchronous history helper (runs in Recorder executor)
# -------------------------------------------------------------------------
def _sync_get_last_gps_from_history(
    hass: HomeAssistant, entity_id: str
) -> Optional[Dict[str, Any]]:
    """Fetch last state change with GPS coordinates via Recorder History (sync).

    IMPORTANT:
    - Must run in a worker (Recorder executor). Do not call asyncio APIs here.
    - Keep queries minimal to avoid heavy DB load.
    """
    try:
        # Minimal query: the last single change for this entity_id
        changes = recorder_history.get_last_state_changes(hass, 1, entity_id)
        samples = changes.get(entity_id, [])
        if not samples:
            return None

        last_state = samples[-1]
        attrs = getattr(last_state, "attributes", {}) or {}
        lat = attrs.get("latitude")
        lon = attrs.get("longitude")
        if lat is None or lon is None:
            return None

        return {
            "latitude": lat,
            "longitude": lon,
            "accuracy": attrs.get("gps_accuracy"),
            "last_seen": int(last_state.last_updated.timestamp()),
            "status": "Using historical data",
        }
    except Exception as err:
        _LOGGER.debug("History lookup failed for %s: %s", entity_id, err)
        return None


class GoogleFindMyCoordinator(DataUpdateCoordinator[List[Dict[str, Any]]]):
    """Coordinator that manages polling and cache for Google Find My Device."""

    def __init__(
        self,
        hass: HomeAssistant,
        secrets_data: dict,
        tracked_devices: Optional[List[str]] = None,
        location_poll_interval: int = 300,
        device_poll_delay: int = 5,
        min_poll_interval: int = 120,
        min_accuracy_threshold: int = 100,
        allow_history_fallback: bool = False,  # explicit, default OFF
    ) -> None:
        """Initialize the coordinator."""
        self.hass = hass
        self.api = GoogleFindMyAPI(secrets_data=secrets_data)

        # Configuration
        self.tracked_devices = tracked_devices or []
        self.location_poll_interval = int(location_poll_interval)
        self.device_poll_delay = int(device_poll_delay)
        self.min_poll_interval = int(min_poll_interval)  # hard lower bound between cycles
        self._min_accuracy_threshold = int(min_accuracy_threshold)  # quality filter (meters)
        self.allow_history_fallback = bool(allow_history_fallback)

        # Internal cache & bookkeeping
        # Cache of latest location payloads per device_id (values are dicts)
        self._device_location_data: Dict[str, Dict[str, Any]] = {}
        self._device_names: Dict[str, str] = {}  # device_id -> human name

        # Polling state (decoupled background task)
        self._poll_lock = asyncio.Lock()
        self._is_polling = False
        self._startup_complete = False
        self._last_poll_mono: float = 0.0  # monotonic timestamp for scheduling

        # Statistics (extend as needed)
        self.stats: Dict[str, int] = {
            "skipped_duplicates": 0,
            "background_updates": 0,
            "crowd_sourced_updates": 0,
            "history_fallback_used": 0,
            "timeouts": 0,
            "invalid_coords": 0,
            "low_quality_dropped": 0,
        }
        _LOGGER.debug("Initialized stats: %s", self.stats)

        # Load persistent statistics asynchronously
        hass.async_create_task(self._async_load_stats())

        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL),
        )

    # -------------------------------------------------------------------------
    # Coordinator refresh path (must be quick and non-blocking)
    # -------------------------------------------------------------------------
    async def _async_update_data(self) -> List[Dict[str, Any]]:
        """Provide cached device data; trigger background poll if due.

        This method should not perform long-running work. It prepares
        current data from cache and fires a background poll cycle when due.
        """
        try:
            # 1) Always fetch the lightweight device list via executor (API is sync)
            all_devices = await self.hass.async_add_executor_job(
                self.api.get_basic_device_list
            )

            # 2) Filter to tracked devices (if configured)
            if self.tracked_devices:
                devices = [d for d in all_devices if d["id"] in self.tracked_devices]
            else:
                devices = all_devices

            # 3) Update device name map (for logging/UX)
            for dev in devices:
                self._device_names[dev["id"]] = dev.get("name", dev["id"])

            # 4) Scheduling: decide whether to trigger a poll cycle (monotonic clock)
            now_mono = time.monotonic()
            effective_interval = max(self.location_poll_interval, self.min_poll_interval)

            if not self._startup_complete:
                # First run: set baseline, do not poll immediately
                self._startup_complete = True
                self._last_poll_mono = now_mono
                _LOGGER.debug(
                    "First startup - set poll baseline; next poll follows normal schedule"
                )
            else:
                due = (now_mono - self._last_poll_mono) >= effective_interval
                if due and not self._is_polling and devices:
                    _LOGGER.debug(
                        "Scheduling background polling cycle (devices=%d, interval=%ds)",
                        len(devices),
                        effective_interval,
                    )
                    self.hass.async_create_task(self._async_start_poll_cycle(devices))
                else:
                    _LOGGER.debug(
                        "Poll not due (elapsed=%.1fs/%ss) or already running=%s",
                        now_mono - self._last_poll_mono,
                        effective_interval,
                        self._is_polling,
                    )

            # 5) Build data snapshot from cache and (optionally) minimal fallbacks
            snapshot = await self._async_build_device_snapshot_with_fallbacks(devices)
            _LOGGER.debug(
                "Returning %d device entries; next poll in ~%ds",
                len(snapshot),
                int(
                    max(0, effective_interval - (time.monotonic() - self._last_poll_mono))
                ),
            )
            return snapshot

        except Exception as exc:  # keep UpdateFailed semantics
            raise UpdateFailed(exc) from exc

    # -------------------------------------------------------------------------
    # Background polling
    # -------------------------------------------------------------------------
    async def _async_start_poll_cycle(self, devices: List[Dict[str, Any]]) -> None:
        """Run a full sequential polling cycle in a background task.

        This runs with a lock to avoid overlapping cycles, updates the
        internal cache, and pushes snapshots at start and end.
        """
        if not devices:
            return

        async with self._poll_lock:
            if self._is_polling:
                return

            self._is_polling = True
            # Push a snapshot from cache to signal "polling" state to listeners
            start_snapshot = self._build_snapshot_from_cache(devices, wall_now=time.time())
            self.async_set_updated_data(start_snapshot)
            _LOGGER.info("Starting sequential poll of %d devices", len(devices))

            try:
                for idx, dev in enumerate(devices):
                    dev_id = dev["id"]
                    dev_name = dev.get("name", dev_id)
                    _LOGGER.info(
                        "Sequential poll: requesting location for %s (%d/%d)",
                        dev_name,
                        idx + 1,
                        len(devices),
                    )

                    try:
                        # Protect API awaitable with timeout
                        location = await asyncio.wait_for(
                            self.api.async_get_device_location(dev_id, dev_name),
                            timeout=30.0,
                        )

                        if not location:
                            _LOGGER.warning("No location data returned for %s", dev_name)
                            continue

                        lat = location.get("latitude")
                        lon = location.get("longitude")
                        acc = location.get("accuracy")
                        last_seen = location.get("last_seen", 0)

                        # Semantic location without coordinates: keep previous coordinates
                        if (lat is None or lon is None) and location.get(
                            "semantic_name"
                        ):
                            prev = self._device_location_data.get(dev_id, {})
                            if prev:
                                # Preserve previous coordinates but reflect semantic origin
                                location["latitude"] = prev.get("latitude")
                                location["longitude"] = prev.get("longitude")
                                location["accuracy"] = prev.get("accuracy")
                                location["status"] = (
                                    "Semantic location; preserving previous coordinates"
                                )

                        # Validate coordinates
                        lat = location.get("latitude")
                        lon = location.get("longitude")
                        if lat is None or lon is None:
                            _LOGGER.warning(
                                "Invalid coordinates for %s: lat=%s, lon=%s",
                                dev_name,
                                lat,
                                lon,
                            )
                            self.increment_stat("invalid_coords")
                            continue

                        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                            _LOGGER.warning(
                                "Coordinates out of range for %s: lat=%s, lon=%s",
                                dev_name,
                                lat,
                                lon,
                            )
                            self.increment_stat("invalid_coords")
                            continue

                        # Accuracy quality filter
                        if (
                            isinstance(self._min_accuracy_threshold, int)
                            and self._min_accuracy_threshold > 0
                            and isinstance(acc, (int, float))
                            and acc > self._min_accuracy_threshold
                        ):
                            _LOGGER.debug(
                                "Dropping low-quality fix for %s (accuracy=%sm > %sm)",
                                dev_name,
                                acc,
                                self._min_accuracy_threshold,
                            )
                            self.increment_stat("low_quality_dropped")
                            continue

                        # Dedupe by last_seen
                        existing = self._device_location_data.get(dev_id, {})
                        existing_last_seen = existing.get("last_seen")
                        if existing_last_seen == last_seen and last_seen:
                            _LOGGER.debug(
                                "Skipping duplicate location for %s (last_seen=%s)",
                                dev_name,
                                last_seen,
                            )
                            self.increment_stat("skipped_duplicates")
                            continue

                        # Log age information (informational only)
                        wall_now = time.time()
                        if last_seen:
                            age_hours = max(0.0, (wall_now - float(last_seen)) / 3600.0)
                            if age_hours > 24:
                                _LOGGER.info(
                                    "Using old location data for %s (age=%.1fh)",
                                    dev_name,
                                    age_hours,
                                )
                            elif age_hours > 1:
                                _LOGGER.debug(
                                    "Using location data for %s (age=%.1fh)",
                                    dev_name,
                                    age_hours,
                                )

                        # Commit to cache
                        location["last_updated"] = wall_now  # wall-clock for user info
                        self._device_location_data[dev_id] = location
                        self.increment_stat("background_updates")

                    except asyncio.TimeoutError:
                        _LOGGER.warning(
                            "Location request timed out for %s after 30 seconds",
                            dev_name,
                        )
                        self.increment_stat("timeouts")
                    except Exception as err:
                        _LOGGER.error("Failed to get location for %s: %s", dev_name, err)

                    # Inter-device delay (except after the last one)
                    if idx < len(devices) - 1 and self.device_poll_delay > 0:
                        await asyncio.sleep(self.device_poll_delay)

                _LOGGER.debug("Completed polling cycle for %d devices", len(devices))
            finally:
                # Update scheduling baseline and clear flag, then push end snapshot
                self._last_poll_mono = time.monotonic()
                self._is_polling = False
                end_snapshot = self._build_snapshot_from_cache(
                    devices, wall_now=time.time()
                )
                self.async_set_updated_data(end_snapshot)

    # -------------------------------------------------------------------------
    # Snapshot builders
    # -------------------------------------------------------------------------
    def _build_snapshot_from_cache(
        self, devices: List[Dict[str, Any]], wall_now: float
    ) -> List[Dict[str, Any]]:
        """Build a lightweight snapshot using only the in-memory cache.

        This never touches HA state or the database; it is safe in background tasks.
        """
        snapshot: List[Dict[str, Any]] = []
        for dev in devices:
            dev_id = dev["id"]
            dev_name = dev.get("name", dev_id)
            info: Dict[str, Any] = {
                "name": dev_name,
                "id": dev_id,
                "device_id": dev_id,
                "latitude": None,
                "longitude": None,
                "altitude": None,
                "accuracy": None,
                "last_seen": None,
                "status": "Waiting for location poll",
                "is_own_report": None,
                "semantic_name": None,
                "battery_level": None,
            }
            cached = self._device_location_data.get(dev_id)
            if cached:
                info.update(cached)
                last_updated_ts = cached.get("last_updated", 0)
                age = max(0.0, wall_now - float(last_updated_ts))
                if age < self.location_poll_interval:
                    info["status"] = "Location data current"
                elif age < self.location_poll_interval * 2:
                    info["status"] = "Location data aging"
                else:
                    info["status"] = "Location data stale"
            snapshot.append(info)
        return snapshot

    async def _async_build_device_snapshot_with_fallbacks(
        self, devices: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Build a snapshot using cache, HA state and (optionally) loud history fallback."""
        snapshot: List[Dict[str, Any]] = []
        wall_now = time.time()
        ent_reg = er.async_get(self.hass)

        for dev in devices:
            dev_id = dev["id"]
            dev_name = dev.get("name", dev_id)

            info: Dict[str, Any] = {
                "name": dev_name,
                "id": dev_id,
                "device_id": dev_id,
                "latitude": None,
                "longitude": None,
                "altitude": None,
                "accuracy": None,
                "last_seen": None,
                "status": "Waiting for location poll",
                "is_own_report": None,
                "semantic_name": None,
                "battery_level": None,
            }

            # Prefer cached result
            cached = self._device_location_data.get(dev_id)
            if cached:
                info.update(cached)
                last_updated_ts = cached.get("last_updated", 0)
                age = max(0.0, wall_now - float(last_updated_ts))
                if age < self.location_poll_interval:
                    info["status"] = "Location data current"
                elif age < self.location_poll_interval * 2:
                    info["status"] = "Location data aging"
                else:
                    info["status"] = "Location data stale"
                snapshot.append(info)
                continue

            # No cache -> Registry + State (cheap, non-blocking)
            unique_id = f"{DOMAIN}_{dev_id}"
            entity_id = ent_reg.async_get_entity_id("device_tracker", DOMAIN, unique_id)
            if not entity_id:
                _LOGGER.warning(
                    "No entity registry entry for device '%s' (unique_id=%s); "
                    "skipping any fallback.",
                    dev_name,
                    unique_id,
                )
                snapshot.append(info)
                continue

            state = self.hass.states.get(entity_id)
            if state:
                lat = state.attributes.get("latitude")
                lon = state.attributes.get("longitude")
                acc = state.attributes.get("gps_accuracy")
                if lat is not None and lon is not None:
                    info.update(
                        {
                            "latitude": lat,
                            "longitude": lon,
                            "accuracy": acc,
                            "last_seen": int(state.last_updated.timestamp()),
                            "status": "Using current state",
                        }
                    )
                    snapshot.append(info)
                    continue

            # Optional loud history fallback
            if self.allow_history_fallback:
                _LOGGER.warning(
                    "No live state for %s (entity_id=%s); attempting history fallback via Recorder.",
                    dev_name,
                    entity_id,
                )
                rec = get_recorder(self.hass)
                result = await rec.async_add_executor_job(
                    _sync_get_last_gps_from_history, self.hass, entity_id
                )
                if result:
                    info.update(result)
                    self.increment_stat("history_fallback_used")
                else:
                    _LOGGER.warning(
                        "No historical GPS data found for %s (entity_id=%s). "
                        "Entity may be excluded from Recorder.",
                        dev_name,
                        entity_id,
                    )

            snapshot.append(info)

        return snapshot

    # -------------------------------------------------------------------------
    # Stats persistence helpers
    # -------------------------------------------------------------------------
    async def _async_load_stats(self) -> None:
        """Load statistics from cache."""
        try:
            from .Auth.token_cache import async_get_cached_value

            cached = await async_get_cached_value("integration_stats")
            if cached and isinstance(cached, dict):
                for key in self.stats.keys():
                    if key in cached:
                        self.stats[key] = cached[key]
                _LOGGER.debug("Loaded statistics from cache: %s", self.stats)
        except Exception as err:
            _LOGGER.debug("Failed to load statistics from cache: %s", err)

    async def _async_save_stats(self) -> None:
        """Persist statistics to cache."""
        try:
            from .Auth.token_cache import async_set_cached_value

            await async_set_cached_value("integration_stats", self.stats.copy())
        except Exception as err:
            _LOGGER.debug("Failed to save statistics from cache: %s", err)

    def increment_stat(self, stat_name: str) -> None:
        """Increment a statistic counter and schedule async persistence."""
        if stat_name in self.stats:
            before = self.stats[stat_name]
            self.stats[stat_name] = before + 1
            _LOGGER.debug(
                "Incremented %s from %s to %s", stat_name, before, self.stats[stat_name]
            )
            self.hass.async_create_task(self._async_save_stats())
        else:
            _LOGGER.warning(
                "Tried to increment unknown stat '%s'; available=%s",
                stat_name,
                list(self.stats.keys()),
            )

    # -------------------------------------------------------------------------
    # Passthrough API helpers (unchanged)
    # -------------------------------------------------------------------------
    async def async_locate_device(self, device_id: str) -> Dict[str, Any]:
        """Locate a device (executes blocking client code in executor)."""
        return await self.hass.async_add_executor_job(self.api.locate_device, device_id)

    async def async_play_sound(self, device_id: str) -> bool:
        """Play sound on a device (executes blocking client code in executor)."""
        return await self.hass.async_add_executor_job(self.api.play_sound, device_id)
