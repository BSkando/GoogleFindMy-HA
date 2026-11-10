# tests/test_cloud_battery_removed.py
"""The cloud device list carries no battery level, so no entity shows one.

``battery_level`` was a placeholder that the Google device list never filled
(measured three times on 2026-11-10, ``docs/BLE_BATTERY_SENSOR.md`` section
6.4). Battery state comes from BLE only. Cache rows written by older versions
may still carry the key and must load without error and without the attribute.
"""

from __future__ import annotations

from custom_components.googlefindmy.coordinator.helpers.cache import (
    DEFAULT_SNAPSHOT_FIELDS,
    build_base_snapshot_entry,
    merge_cache_row,
)
from custom_components.googlefindmy.coordinator.main import (
    _PERSISTED_METADATA_KEYS,
    _as_ha_attributes,
)


def _row(**extra: object) -> dict[str, object]:
    row: dict[str, object] = {
        "name": "Tag",
        "id": "dev-1",
        "device_id": "dev-1",
        "latitude": 1.5,
        "longitude": 2.5,
        "accuracy": 20.0,
        "last_seen": 1_700_000_000,
        "status": "Location data current",
    }
    row.update(extra)
    return row


def test_ha_attributes_carry_no_cloud_battery() -> None:
    """A value in the row is not passed through as an entity attribute."""
    attributes = _as_ha_attributes(_row(battery_level=42))
    assert attributes is not None
    assert "battery_level" not in attributes


def test_legacy_cache_row_with_battery_level_loads() -> None:
    """A cache row in the old format merges and renders without the attribute."""
    legacy = _row(battery_level=42)
    merged = merge_cache_row(existing=legacy, incoming=_row(last_seen=1_700_000_100))
    attributes = _as_ha_attributes(merged)
    assert attributes is not None
    assert "battery_level" not in attributes
    assert attributes["last_seen_utc"]


def test_snapshot_and_persisted_keys_drop_the_placeholder() -> None:
    """New snapshots and persisted metadata no longer name the placeholder."""
    assert "battery_level" not in DEFAULT_SNAPSHOT_FIELDS
    assert "battery_level" not in build_base_snapshot_entry({"id": "dev-1"})
    assert "battery_level" not in _PERSISTED_METADATA_KEYS
