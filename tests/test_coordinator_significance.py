# tests/test_coordinator_significance.py
from __future__ import annotations

import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pytest

from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from custom_components.googlefindmy.coordinator.cache import (
    MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S,
)
from tests.helpers.config_entries_stub import make_config_entry

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory


def _make_coordinator(existing: dict[str, Any]) -> GoogleFindMyCoordinator:
    """Create a coordinator instance with preloaded cache data for testing."""

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._device_location_data = {"device-1": dict(existing)}
    coordinator._device_names = {}
    coordinator._device_update_history = {}
    coordinator.increment_stat = lambda *_args, **_kwargs: None
    coordinator._apply_report_type_cooldown = lambda *_args, **_kwargs: None
    coordinator._is_on_hass_loop = lambda: True
    coordinator._run_on_hass_loop = lambda *_args, **_kwargs: None
    return coordinator


def _stat_recorder() -> tuple[dict[str, int], Callable[[str], None]]:
    """Return a stat counter map and increment callback for assertions."""

    counts: dict[str, int] = {}

    def _increment(stat_name: str) -> None:
        counts[stat_name] = counts.get(stat_name, 0) + 1

    return counts, _increment


def _weighted_coordinates(
    existing: dict[str, Any], incoming: dict[str, Any]
) -> tuple[float, float]:
    """Return the weighted fusion result used by the coordinator."""

    w_old = 1 / (max(1.0, float(existing["accuracy"])) ** 2)
    w_new = 1 / (max(1.0, float(incoming["accuracy"])) ** 2)
    total = w_old + w_new

    fused_lat = (existing["latitude"] * w_old + incoming["latitude"] * w_new) / total
    fused_lon = (existing["longitude"] * w_old + incoming["longitude"] * w_new) / total
    return fused_lat, fused_lon


def test_stale_timestamp_is_rejected_before_merge() -> None:
    """Out-of-order payloads must not regress cached coordinates."""

    existing = {
        "latitude": 37.4219999,
        "longitude": -122.0840575,
        "accuracy": 25.0,
        "last_seen": 1_700_000_000,
        "status": "coordinate",
    }
    coordinator = _make_coordinator(existing)
    stat_counts, increment = _stat_recorder()
    coordinator.increment_stat = increment

    stale = {
        "latitude": 37.4224,
        "longitude": -122.085,
        "accuracy": 30.0,
        "last_seen": existing["last_seen"] - 120,
        "status": "coordinate",
    }

    coordinator.update_device_cache("device-1", stale)

    cached = coordinator._device_location_data["device-1"]
    assert cached["latitude"] == pytest.approx(existing["latitude"])
    assert cached["longitude"] == pytest.approx(existing["longitude"])
    assert cached["last_seen"] == pytest.approx(existing["last_seen"])
    assert stat_counts == {
        # Accuracy class of the incoming fix. Counted before the fusion and
        # before the gate (#216), so the distribution sees every fix - including
        # the ones later dropped, which are the interesting half.
        "accuracy_bucket_10_50": 1,
        "invalid_ts_drop_count": 1,
        "drop_reason_invalid_ts": 1,
        "invalid_ts_drop_benign": 1,
    }


def test_same_timestamp_with_new_altitude_is_significant() -> None:
    """Altitude-only updates fuse coordinates but refresh height."""

    existing = {
        "latitude": 37.4219999,
        "longitude": -122.0840575,
        "accuracy": 25.0,
        "last_seen": 1_700_000_000,
        "altitude": None,
    }
    coordinator = _make_coordinator(existing)

    new_data = {**existing, "altitude": 120.0}

    coordinator.update_device_cache("device-1", new_data)

    cached = coordinator._device_location_data["device-1"]
    assert cached["altitude"] == pytest.approx(120.0)
    assert cached["last_seen"] == pytest.approx(existing["last_seen"])
    assert cached["status"] == "Fused (Weighted)"


def test_same_timestamp_with_altitude_delta_is_significant() -> None:
    """Altitude changes fuse while keeping timestamps stable."""

    existing = {
        "latitude": 37.4219999,
        "longitude": -122.0840575,
        "accuracy": 25.0,
        "last_seen": 1_700_000_000,
        "altitude": 110.0,
    }
    coordinator = _make_coordinator(existing)

    new_data = {**existing, "altitude": 126.5}

    coordinator.update_device_cache("device-1", new_data)

    cached = coordinator._device_location_data["device-1"]
    assert cached["altitude"] == pytest.approx(126.5)
    assert cached["last_seen"] == pytest.approx(existing["last_seen"])
    assert cached["status"] == "Fused (Weighted)"


def test_accuracy_gain_is_significant_even_when_stationary() -> None:
    """Accuracy-only gains fuse coordinates without suppressing movement."""

    existing = {
        "latitude": 52.52,
        "longitude": 13.405,
        "accuracy": 150.0,
        "last_seen": 1_700_000_000.0,
    }
    coordinator = _make_coordinator(existing)
    stat_counts, increment = _stat_recorder()
    coordinator.increment_stat = increment

    new_data = {
        **existing,
        "latitude": existing["latitude"] + 0.00005,
        "longitude": existing["longitude"] + 0.00005,
        "accuracy": 100.0,  # ~33 % improvement triggers accuracy gate
        "last_seen": existing["last_seen"] + 15,
    }

    coordinator.update_device_cache("device-1", new_data)

    cached = coordinator._device_location_data["device-1"]
    fused_lat, fused_lon = _weighted_coordinates(existing, new_data)

    assert cached["latitude"] == pytest.approx(fused_lat)
    assert cached["longitude"] == pytest.approx(fused_lon)
    assert cached["accuracy"] == pytest.approx(100.0)
    assert cached["last_seen"] == pytest.approx(new_data["last_seen"])
    assert cached["status"] == "Fused (Weighted)"
    assert stat_counts == {
        "background_updates": 1,
        "fused_updates": 1,
        # Accuracy class of the incoming fix. Counted before the fusion and
        # before the gate (#216), so the distribution sees every fix - including
        # the ones later dropped, which are the interesting half.
        "accuracy_bucket_50_200": 1,
    }


def test_stationary_update_fuses_overlapping_coordinates() -> None:
    """Sub-threshold movement fuses coordinates instead of clamping."""

    existing = {
        "latitude": 40.7128,
        "longitude": -74.006,
        "accuracy": 150.0,
        "last_seen": 1_700_000_000.0,
        "status": "coordinate",
        "source_label": "semantic/unknown",
    }
    coordinator = _make_coordinator(existing)
    stat_counts, increment = _stat_recorder()
    coordinator.increment_stat = increment

    new_payload = {
        "latitude": existing["latitude"] + 0.0003,  # ~33 m delta, below accuracy
        "longitude": existing["longitude"] + 0.0003,
        "accuracy": 115.0,
        "last_seen": existing["last_seen"] + 30,
        "status": existing["status"],
    }

    coordinator.update_device_cache("device-1", new_payload)

    cached = coordinator._device_location_data["device-1"]
    fused_lat, fused_lon = _weighted_coordinates(existing, new_payload)

    assert cached["latitude"] == pytest.approx(fused_lat)
    assert cached["longitude"] == pytest.approx(fused_lon)
    assert cached["accuracy"] == pytest.approx(115.0)
    assert cached["last_seen"] == pytest.approx(new_payload["last_seen"])
    assert cached["status"] == "Fused (Weighted)"
    assert stat_counts == {
        "background_updates": 1,
        "fused_updates": 1,
        # Accuracy class of the incoming fix. Counted before the fusion and
        # before the gate (#216), so the distribution sees every fix - including
        # the ones later dropped, which are the interesting half.
        "accuracy_bucket_50_200": 1,
    }


def test_stationary_metadata_change_fuses_coordinates() -> None:
    """Low-movement updates fuse coordinates while refreshing metadata."""

    existing = {
        "latitude": 40.7128,
        "longitude": -74.006,
        "accuracy": 150.0,
        "last_seen": 1_700_000_000.0,
        "status": "coordinate",
        "battery_level": 0.9,
        "source_label": "semantic/unknown",
    }
    coordinator = _make_coordinator(existing)
    stat_counts, increment = _stat_recorder()
    coordinator.increment_stat = increment

    new_payload = {
        "latitude": existing["latitude"] + 0.0003,  # ~33 m delta, below accuracy
        "longitude": existing["longitude"] + 0.0003,
        "accuracy": 115.0,
        "last_seen": existing["last_seen"] + 45,
        "battery_level": 0.55,
        "status": "low_battery",
    }

    coordinator.update_device_cache("device-1", new_payload)

    cached = coordinator._device_location_data["device-1"]
    fused_lat, fused_lon = _weighted_coordinates(existing, new_payload)

    assert cached["latitude"] == pytest.approx(fused_lat)
    assert cached["longitude"] == pytest.approx(fused_lon)
    assert cached["accuracy"] == pytest.approx(115.0)
    assert cached["last_seen"] == pytest.approx(new_payload["last_seen"])
    assert cached["battery_level"] == pytest.approx(new_payload["battery_level"])
    assert cached["status"] == "Fused (Weighted)"
    assert stat_counts == {
        "background_updates": 1,
        "fused_updates": 1,
        # Accuracy class of the incoming fix. Counted before the fusion and
        # before the gate (#216), so the distribution sees every fix - including
        # the ones later dropped, which are the interesting half.
        "accuracy_bucket_50_200": 1,
    }


def test_identity_key_delta_triggers_resolver_refresh() -> None:
    """FCM-provided identity key updates must refresh the resolver cache."""

    existing = {
        "latitude": 40.0,
        "longitude": -75.0,
        "accuracy": 20.0,
        "last_seen": 1_700_000_000.0,
        "encrypted_identity_key": b"old",
        "owner_key_version": 1,
    }
    coordinator = _make_coordinator(existing)

    refresh_calls: list[str] = []
    coordinator._schedule_eid_resolver_refresh = lambda: refresh_calls.append("refresh")

    incoming = {
        "latitude": existing["latitude"] + 0.0002,
        "longitude": existing["longitude"] + 0.0002,
        "accuracy": 10.0,
        "last_seen": existing["last_seen"] + 60,
        "encrypted_identity_key": b"new",
        "owner_key_version": 2,
    }

    coordinator.update_device_cache("device-1", incoming)

    cached = coordinator._device_location_data["device-1"]
    assert cached["encrypted_identity_key"] == b"new"
    assert cached["owner_key_version"] == 2
    assert refresh_calls == ["refresh"]


def test_identity_key_stability_skips_resolver_refresh() -> None:
    """Repeated identity key payloads should not retrigger resolver work."""

    existing = {
        "latitude": 41.0,
        "longitude": -76.0,
        "accuracy": 15.0,
        "last_seen": 1_700_000_000.0,
        "encrypted_identity_key": b"stable",
        "owner_key_version": 3,
    }
    coordinator = _make_coordinator(existing)

    refresh_calls: list[str] = []
    coordinator._schedule_eid_resolver_refresh = lambda: refresh_calls.append("refresh")

    incoming = {
        "latitude": existing["latitude"],
        "longitude": existing["longitude"],
        "accuracy": 12.0,
        "last_seen": existing["last_seen"] + 90,
        "encrypted_identity_key": b"stable",
        "owner_key_version": 3,
    }

    coordinator.update_device_cache("device-1", incoming)

    assert refresh_calls == []


def test_update_cache_keeps_coordinates_when_semantic_refresh_arrives() -> None:
    """Semantic-only updates must not drop cached coordinates for a device."""

    existing = {
        "name": "Tracker",
        "latitude": 48.137154,
        "longitude": 11.576124,
        "accuracy": 30.0,
        "altitude": 520.0,
        "last_seen": 1_700_000_000.0,
        "last_seen_utc": "2023-11-14T22:13:20Z",
        "semantic_name": "Warehouse",
        "status": "coordinate",
    }
    coordinator = _make_coordinator(existing)

    incoming = {
        "name": "Tracker",
        "device_id": "device-1",
        "id": "device-1",
        "semantic_name": "Service Center",
        "status": "semantic_only",
        "last_seen": existing["last_seen"],
    }

    coordinator.update_device_cache("device-1", incoming)

    cached = coordinator._device_location_data["device-1"]
    assert cached["semantic_name"] == "Service Center"
    assert cached["latitude"] == pytest.approx(existing["latitude"])
    assert cached["longitude"] == pytest.approx(existing["longitude"])
    assert cached["last_seen"] == pytest.approx(existing["last_seen"])


def test_update_cache_preserves_last_seen_when_timestamp_missing() -> None:
    """Payloads without last_seen must inherit the cached timestamp markers."""

    existing = {
        "name": "Tracker",
        "latitude": 34.052235,
        "longitude": -118.243683,
        "accuracy": 12.0,
        "last_seen": 1_700_100_000.0,
        "last_seen_utc": "2023-11-16T02:53:20Z",
        "semantic_name": "Studio",
        "status": "coordinate",
    }
    coordinator = _make_coordinator(existing)

    incoming = {
        "name": "Tracker",
        "device_id": "device-1",
        "id": "device-1",
        "semantic_name": "Set",
        "status": "semantic_only",
        # last_seen omitted on purpose
    }

    coordinator.update_device_cache("device-1", incoming)

    cached = coordinator._device_location_data["device-1"]
    assert cached["semantic_name"] == "Set"
    assert cached["last_seen"] == pytest.approx(existing["last_seen"])
    assert cached["last_seen_utc"] == existing["last_seen_utc"]


# --------------------------------------------- a report is fused only once
#
# A report delivered again with the same ``last_seen`` used to be averaged with
# the position the cache already held, and the cache already held the average of
# that very report. Every delivery halved the remaining distance to the raw fix,
# so the published position crept without a new location and every step was a
# fresh state write. Unless a test says otherwise (no timestamp, or a fix far
# away for the gate), the cached row starts with an OLDER fix that overlaps the
# report, so the first delivery fuses and any further fusion of the same report
# would move the position again - which is what makes the defect visible at all.

_PRIOR_FIX: dict[str, Any] = {
    "latitude": 40.7128,
    "longitude": -74.006,
    "accuracy": 50.0,
    "last_seen": 1_700_000_000.0,
    "status": "coordinate",
}


def _report(**overrides: Any) -> dict[str, Any]:
    """One report one minute after the prior fix, about 33 m north of it."""

    report: dict[str, Any] = {
        "latitude": 40.7131,
        "longitude": -74.006,
        "accuracy": 50.0,
        "last_seen": 1_700_000_060.0,
        "status": "coordinate",
    }
    report.update(overrides)
    return report


def _fusing_coordinator(
    existing: dict[str, Any] | None = None,
) -> tuple[GoogleFindMyCoordinator, dict[str, int]]:
    coordinator = _make_coordinator(_PRIOR_FIX if existing is None else existing)
    counts, increment = _stat_recorder()
    coordinator.increment_stat = increment
    return coordinator, counts


def _position(coordinator: GoogleFindMyCoordinator) -> tuple[Any, Any, Any]:
    cached = coordinator._device_location_data["device-1"]
    return cached["latitude"], cached["longitude"], cached["accuracy"]


def test_same_report_three_times_is_fused_once() -> None:
    """The second and third delivery of one report leave the cache as it was."""

    coordinator, counts = _fusing_coordinator()

    coordinator.update_device_cache("device-1", _report())
    after_first = _position(coordinator)
    coordinator.update_device_cache("device-1", _report())
    coordinator.update_device_cache("device-1", _report())

    assert _position(coordinator) == after_first
    assert after_first[0] == pytest.approx(40.71295, abs=1e-9)
    assert counts.get("fused_updates") == 1


def test_two_reports_in_one_second_are_each_fused_once() -> None:
    """Two different reports sharing a timestamp are two reports, not one."""

    coordinator, counts = _fusing_coordinator()
    report_a = _report()
    report_b = _report(latitude=40.7128, longitude=-74.0056)

    coordinator.update_device_cache("device-1", dict(report_a))
    coordinator.update_device_cache("device-1", dict(report_b))
    after_second = _position(coordinator)
    coordinator.update_device_cache("device-1", dict(report_a))
    coordinator.update_device_cache("device-1", dict(report_b))

    assert counts.get("fused_updates") == 2
    assert _position(coordinator) == after_second


def test_repeated_report_does_not_evict_other_report_of_same_second() -> None:
    """Re-deliveries of one report must not push the other one out of memory."""

    coordinator, counts = _fusing_coordinator()
    report_a = _report()
    report_b = _report(latitude=40.7128, longitude=-74.0056)

    coordinator.update_device_cache("device-1", dict(report_a))
    coordinator.update_device_cache("device-1", dict(report_b))
    for _ in range(8):
        coordinator.update_device_cache("device-1", dict(report_a))
    coordinator.update_device_cache("device-1", dict(report_b))

    assert counts.get("fused_updates") == 2


def test_same_position_one_second_later_is_fused() -> None:
    """A new timestamp is a new location, even at the same coordinates."""

    coordinator, counts = _fusing_coordinator()

    coordinator.update_device_cache("device-1", _report())
    coordinator.update_device_cache("device-1", _report(last_seen=1_700_000_061.0))

    cached = coordinator._device_location_data["device-1"]
    assert cached["status"] == "Fused (Weighted)"
    assert cached["last_seen"] == 1_700_000_061
    assert counts.get("fused_updates") == 2


def test_new_accuracy_in_same_second_is_fused() -> None:
    """A different radius for the same second and position is a new measurement."""

    coordinator, counts = _fusing_coordinator()

    coordinator.update_device_cache("device-1", _report())
    coordinator.update_device_cache("device-1", _report(accuracy=30.0))

    assert counts.get("fused_updates") == 2


def test_repeated_report_without_last_seen_is_fused_once() -> None:
    """Two identical reports without a timestamp cannot be told apart.

    The prior fix carries neither ``last_seen`` nor ``source_rank``, so the
    merge accepts every delivery and a re-fusion would show as movement.
    """

    coordinator, counts = _fusing_coordinator(
        {"latitude": 40.7128, "longitude": -74.006, "accuracy": 50.0}
    )
    report = _report()
    del report["last_seen"]

    coordinator.update_device_cache("device-1", dict(report))
    after_first = _position(coordinator)
    coordinator.update_device_cache("device-1", dict(report))
    coordinator.update_device_cache("device-1", dict(report))

    assert after_first[0] == pytest.approx(40.71295, abs=1e-9)
    assert _position(coordinator) == after_first
    assert counts.get("fused_updates") == 1


@pytest.mark.parametrize("with_last_seen", [True, False], ids=["stamped", "stampless"])
def test_gate_rejected_report_stays_rejected_on_repeat(with_last_seen: bool) -> None:
    """Recognising a repeat must never become a way around the accuracy gate."""

    now = time.time()
    precise = {
        "latitude": 49.866,
        "longitude": 10.839,
        "accuracy": 20.0,
        "last_seen": now - 300.0,
        "location_type": "sensor",
        "accuracy_estimated": False,
    }
    coordinator, counts = _fusing_coordinator(precise)
    coordinator.config_entry = make_config_entry(entry_id="entry-id")
    coarse: dict[str, Any] = {
        "latitude": 49.916,
        "longitude": 10.839,
        "accuracy": 1600.0,
        "status": "coordinate",
    }
    if with_last_seen:
        coarse["last_seen"] = now

    coordinator.update_device_cache("device-1", dict(coarse))
    coordinator.update_device_cache("device-1", dict(coarse))

    cached = coordinator._device_location_data["device-1"]
    # Exact: a relative tolerance at epoch scale would hide minutes.
    assert cached["last_seen"] == now - 300.0
    assert (cached["latitude"], cached["longitude"]) == (49.866, 10.839)
    if not with_last_seen:
        assert counts.get("accuracy_gate_rejects") == 2


def test_duplicate_keeps_cached_geometry_and_commits_other_fields() -> None:
    """A repeat changes nothing about the position, but other news still lands."""

    coordinator, counts = _fusing_coordinator()

    coordinator.update_device_cache("device-1", _report())
    after_first = _position(coordinator)
    coordinator.update_device_cache("device-1", _report(battery_level=0.4))

    cached = coordinator._device_location_data["device-1"]
    assert cached["battery_level"] == pytest.approx(0.4)
    assert _position(coordinator) == after_first
    assert cached["status"] == "Fused (Weighted)"
    assert counts.get("fused_updates") == 1


def test_duplicate_lets_new_altitude_through() -> None:
    """Height is not part of the fusion, so a repeat may still refresh it."""

    coordinator, counts = _fusing_coordinator()

    coordinator.update_device_cache("device-1", _report(altitude=110.0))
    after_first = _position(coordinator)
    coordinator.update_device_cache("device-1", _report(altitude=126.5))

    cached = coordinator._device_location_data["device-1"]
    assert cached["altitude"] == pytest.approx(126.5)
    assert _position(coordinator)[:2] == after_first[:2]
    assert counts.get("fused_updates") == 1


def test_duplicate_at_trusted_anchor_keeps_anchor_height() -> None:
    """A repeat must not undo the height a trusted anchor snapped to."""

    coordinator, _counts = _fusing_coordinator(
        _PRIOR_FIX | {"location_type": "trusted", "altitude": 100.0}
    )

    coordinator.update_device_cache("device-1", _report(altitude=200.0))
    coordinator.update_device_cache("device-1", _report(altitude=200.0))

    cached = coordinator._device_location_data["device-1"]
    assert cached["status"] == "Stationary (at Anchor)"
    assert cached["altitude"] == 100.0


def test_report_dropped_as_future_is_fused_once_it_is_due(
    freezer: FrozenDateTimeFactory,
) -> None:
    """Only a committed report is remembered.

    A report stamped beyond the accepted clock drift is dropped by the
    significance check after the fusion ran. Remembering it there would refuse
    the same report once its stamp is no longer in the future.
    """

    now = time.time()
    coordinator, counts = _fusing_coordinator(_PRIOR_FIX | {"last_seen": now - 600.0})
    early = _report(last_seen=now + MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S + 60.0)

    coordinator.update_device_cache("device-1", dict(early))
    assert coordinator._device_location_data["device-1"]["last_seen"] == now - 600.0
    freezer.tick(MAX_ACCEPTED_LOCATION_FUTURE_DRIFT_S + 120.0)
    coordinator.update_device_cache("device-1", dict(early))

    cached = coordinator._device_location_data["device-1"]
    assert cached["status"] == "Fused (Weighted)"
    assert cached["last_seen"] == int(early["last_seen"])
    assert _position(coordinator)[0] == pytest.approx(40.71295, abs=1e-9)
