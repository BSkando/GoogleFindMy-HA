# tests/test_duplicate_device_entities.py
"""Regression test ensuring duplicate device IDs are deduplicated at setup."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

from custom_components.googlefindmy.const import (
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
)
from tests.helpers.config_entries_stub import make_config_entry


def test_duplicate_devices_seed_only_once(
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> None:
    """Duplicate IDs in the coordinator snapshot create a single entity per platform."""

    del (
        deterministic_config_subentry_id
    )  # fixture side effects patch ensure_config_subentry_id

    device_tracker = importlib.import_module(
        "custom_components.googlefindmy.device_tracker"
    )
    sensor = importlib.import_module("custom_components.googlefindmy.sensor")

    class _StubCoordinator(device_tracker.GoogleFindMyCoordinator):
        def __init__(self, devices: Iterable[dict[str, Any]]) -> None:
            self._snapshot = list(devices)
            self._listeners: list[Callable[[], None]] = []
            self.hass = SimpleNamespace()
            self.config_entry = make_config_entry(entry_id="entry-id")
            self.stats: dict[str, int] = {}
            self._device_names: dict[str, str] = {}
            self._device_location_data: dict[str, Any] = {}
            self._device_caps: dict[str, Any] = {}
            self._present_last_seen: dict[str, float] = {}

        def async_add_listener(
            self, listener: Callable[[], None]
        ) -> Callable[[], None]:
            self._listeners.append(listener)
            return lambda: None

        def stable_subentry_identifier(
            self, *, key: str | None = None, feature: str | None = None
        ) -> str:
            assert key is not None
            return f"{key}-identifier"

        def get_subentry_metadata(
            self, *, key: str | None = None, feature: str | None = None
        ) -> Any:
            if key is not None:
                resolved = key
            elif feature in {"button", "device_tracker", "sensor", "switch"}:
                resolved = TRACKER_SUBENTRY_KEY
            elif feature == "binary_sensor":
                resolved = SERVICE_SUBENTRY_KEY
            else:
                resolved = TRACKER_SUBENTRY_KEY
            return SimpleNamespace(key=resolved)

        def get_subentry_snapshot(
            self, key: str | None = None, *, feature: str | None = None
        ) -> list[dict[str, Any]]:
            return list(self._snapshot)

    class _StubConfigEntry:
        def __init__(self, coordinator: _StubCoordinator) -> None:
            self.runtime_data = coordinator
            self.entry_id = "entry-id"
            self.data: dict[str, Any] = {}
            self.options: dict[str, Any] = {}
            self._unsub: list[Callable[[], None]] = []

        def async_on_unload(self, callback: Callable[[], None]) -> None:
            self._unsub.append(callback)

    devices = [
        {"id": "dup-device", "name": "Pixel"},
        {"id": "dup-device", "name": "Pixel Again"},
    ]
    coordinator = _StubCoordinator(devices)
    entry = _StubConfigEntry(coordinator)

    tracker_added: list[list[Any]] = []
    sensor_added: list[list[Any]] = []

    def _capture_tracker(entities, update_before_add: bool = False):
        tracker_added.append(list(entities))
        assert update_before_add is True

    def _capture_sensor(entities, update_before_add: bool = False):
        sensor_added.append(list(entities))
        assert update_before_add is True

    async def _run_setup() -> None:
        await device_tracker.async_setup_entry(
            SimpleNamespace(), entry, _capture_tracker
        )
        await sensor.async_setup_entry(SimpleNamespace(), entry, _capture_sensor)

    asyncio.run(_run_setup())

    assert len(tracker_added) == 1
    # Should have 2 entities: main tracker + last location (but only for one device due to deduplication)
    assert len(tracker_added[0]) == 2
    assert tracker_added[0][0].device_id == "dup-device"
    assert tracker_added[0][1].device_id == "dup-device"
    # First is main tracker, second is last location
    assert not tracker_added[0][0].unique_id.endswith(":last_location")
    assert tracker_added[0][1].unique_id.endswith(":last_location")

    assert len(sensor_added) == 2

    semantic_entities = [
        entity
        for batch in sensor_added
        for entity in batch
        if isinstance(entity, sensor.GoogleFindMySemanticLabelSensor)
    ]
    last_seen_entities = [
        entity
        for batch in sensor_added
        for entity in batch
        if isinstance(entity, sensor.GoogleFindMyLastSeenSensor)
    ]

    assert len(semantic_entities) == 1
    assert len(last_seen_entities) == 1
    assert last_seen_entities[0].device_id == "dup-device"
