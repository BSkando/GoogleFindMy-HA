# tests/test_last_seen_sensor_no_map_attributes.py
"""Regression: the last_seen TIMESTAMP sensor must not advertise map coordinates.

Home Assistant auto-plots any entity that carries both ``latitude`` and
``longitude`` state attributes on the map, regardless of its domain or
``device_class``. ``GoogleFindMyLastSeenSensor`` is a TIMESTAMP diagnostic sensor
and previously leaked these two keys through the shared ``_as_ha_attributes``
helper, producing a spurious third map marker per device (next to the real
``device_tracker`` and the ``last_location`` tracker).

The fix strips ``latitude``/``longitude`` at the sensor consumer (not at the
shared helper, which the ``device_tracker`` legitimately relies on). These tests
encode the HA auto-map rule as an assertion and guard the helper's integrity so a
future change cannot regress the tracker.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable, Iterable
from types import SimpleNamespace
from typing import Any

from custom_components.googlefindmy.const import (
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
)
from tests.helpers.config_entries_stub import make_config_entry

# A decoder row with a real position. ``_as_ha_attributes`` turns this into an
# attribute dict that includes ``latitude``/``longitude`` (the leak under test)
# plus the diagnostic keys that must be preserved.
_POSITION_ROW: dict[str, Any] = {
    "id": "dev-1",
    "device_id": "dev-1",
    "name": "Pixel",
    "latitude": 52.5200,
    "longitude": 13.4050,
    "accuracy": 12.0,
    "last_seen": 1_700_000_000,
    "source_label": "Owner",
    "source_rank": 1,
}


async def _build_last_seen_sensor(
    row: dict[str, Any] | None,
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> Any:
    """Construct a real ``GoogleFindMyLastSeenSensor`` via the platform setup path.

    Mirrors ``tests/test_duplicate_device_entities.py``: a stub coordinator that
    subclasses the real coordinator, importlib module loading, and
    ``sensor.async_setup_entry`` to obtain a genuine entity instance. The stub
    serves ``row`` from ``get_device_location_data_for_subentry`` so the property
    under test sees a controlled position.

    Authored as a coroutine and awaited directly (never ``asyncio.run()``): HA
    tests run inside pytest-asyncio's managed loop (``asyncio_mode = "auto"``);
    a private loop would break the managed-loop fixtures (see ``tests/AGENTS.md``
    "Async tests").
    """

    del (
        deterministic_config_subentry_id
    )  # side effect: patches ensure_config_subentry_id

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
            # Row returned to the sensor property under test (settable per case).
            self._row: dict[str, Any] | None = row

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

        def get_device_location_data_for_subentry(
            self, subentry_key: str, device_id: str
        ) -> dict[str, Any] | None:
            return self._row

    class _StubConfigEntry:
        def __init__(self, coordinator: _StubCoordinator) -> None:
            self.runtime_data = coordinator
            self.entry_id = "entry-id"
            self.data: dict[str, Any] = {}
            self.options: dict[str, Any] = {}
            self._unsub: list[Callable[[], None]] = []

        def async_on_unload(self, callback: Callable[[], None]) -> None:
            self._unsub.append(callback)

    coordinator = _StubCoordinator([{"id": "dev-1", "name": "Pixel"}])
    entry = _StubConfigEntry(coordinator)

    sensor_added: list[list[Any]] = []

    def _capture_sensor(entities, update_before_add: bool = False):
        sensor_added.append(list(entities))

    await sensor.async_setup_entry(SimpleNamespace(), entry, _capture_sensor)

    last_seen_entities = [
        entity
        for batch in sensor_added
        for entity in batch
        if isinstance(entity, sensor.GoogleFindMyLastSeenSensor)
    ]
    assert len(last_seen_entities) == 1
    return last_seen_entities[0]


async def test_last_seen_sensor_strips_map_coordinates(
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> None:
    """A position row must not yield latitude/longitude on the TIMESTAMP sensor."""

    entity = await _build_last_seen_sensor(
        _POSITION_ROW, deterministic_config_subentry_id
    )
    attrs = entity.extra_state_attributes

    assert attrs is not None
    # The HA auto-map rule: neither key may be present on a non-tracker entity.
    assert "latitude" not in attrs
    assert "longitude" not in attrs


async def test_last_seen_sensor_keeps_diagnostic_attributes(
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> None:
    """Stripping coordinates must not drop the diagnostic attributes."""

    entity = await _build_last_seen_sensor(
        _POSITION_ROW, deterministic_config_subentry_id
    )
    attrs = entity.extra_state_attributes

    assert attrs is not None
    # Diagnostic keys produced by _as_ha_attributes survive the strip.
    assert "accuracy_m" in attrs
    assert "last_seen" in attrs


async def test_last_seen_sensor_none_row_yields_none(
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> None:
    """No location row → no attributes (unchanged None-guard behavior)."""

    entity = await _build_last_seen_sensor(None, deterministic_config_subentry_id)
    assert entity.extra_state_attributes is None


def test_shared_helper_still_emits_coordinates_for_tracker() -> None:
    """Helper-integrity gegentest: the shared helper must keep latitude/longitude.

    The fix lives at the sensor consumer, not at ``_as_ha_attributes``. The
    device_tracker relies on the helper emitting coordinates, so a regression that
    moved the strip into the helper would break the legitimate tracker marker.
    """

    main = importlib.import_module("custom_components.googlefindmy.coordinator.main")
    helper_attrs = main._as_ha_attributes(_POSITION_ROW)

    assert helper_attrs is not None
    assert helper_attrs["latitude"] == 52.5200
    assert helper_attrs["longitude"] == 13.4050
