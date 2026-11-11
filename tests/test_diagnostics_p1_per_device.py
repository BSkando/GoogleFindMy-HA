# tests/test_diagnostics_p1_per_device.py
"""P1 diagnostics expansion: HA core version, drop counters, per-device telemetry.

These tests pin the additive, privacy-hardened P1 diagnostics surface:

* **P1-1** -- ``integration["ha_core_version"]`` carries the Home Assistant core
  version string so a maintainer can correlate core-specific regressions.
* **P1-2** -- the three canonicless-drop counters (``canonicless_drop_total`` /
  ``_benign`` / ``_warn``) surface under ``coordinator["stats"]``.
* **P1-3** -- ``coordinator["devices"]`` is an anonymous per-device telemetry list:
  an opaque position index over ``sorted(device_ids)`` plus exactly seven fields
  (``device_class``, ``last_poll_age_s``, ``last_fix_age_s``,
  ``last_accuracy_bucket``, ``is_own_report``, ``has_key``). No names, canonical
  IDs, coordinates, or clear-text keys may appear (POPETS'25 hardening).

The per-device list is produced by ``coordinator.build_per_device_diagnostics()``
and the pure mapping helpers ``_accuracy_bucket`` / ``_round_age`` /
``_device_class`` on the coordinator module, exercised directly here for the
boundary tables.
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy import diagnostics
from custom_components.googlefindmy.coordinator import main as coordinator_main
from custom_components.googlefindmy.coordinator.main import GoogleFindMyCoordinator
from tests.helpers import drain_loop
from tests.helpers.config_entries_stub import make_config_entry

# DEVICE_TYPE_PHONE == 20, DEVICE_TYPE_UNKNOWN == 0 (SpotDeviceType enum).
_DEVICE_TYPE_PHONE = 20
_DEVICE_TYPE_TRACKER = 1  # DEVICE_TYPE_BEACON (any non-phone, non-zero known int)

_EXPECTED_DEVICE_KEYS = {
    "index",
    "device_class",
    "last_poll_age_s",
    "last_fix_age_s",
    "last_accuracy_bucket",
    "is_own_report",
    "has_key",
    # Accuracy gate (#216): what was discarded, without its coordinates. A
    # diagnostics dump is routinely pasted into a public issue.
    "coarse_fix_accuracy_bucket",
    "coarse_fix_age_s",
    # Automatic polling switch (#242): state, time of the last change, sent
    # requests split by origin, age of the last request, and the position the
    # device has in the ``uwt_transitions`` events of the same dump.
    "automatic_polling",
    "automatic_polling_changed_at",
    "locate_requests_auto",
    "locate_requests_manual",
    "last_locate_request_age_s",
    "uwt_device_index",
}


def _run(coro: Any) -> Any:
    """Execute an async coroutine within an isolated, drained event loop."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        drain_loop(loop)


class _PerDeviceCoordinator(GoogleFindMyCoordinator):
    """Coordinator stub exposing the data/slot surface the P1-3 helper consumes.

    Subclasses the production coordinator so ``build_per_device_diagnostics`` is
    the real method under test, but bypasses ``__init__`` (which needs the HA
    runtime). ``data`` mirrors the published snapshot (a list of row dicts keyed
    by ``device_id``); ``_device_location_data`` mirrors the per-device slot that
    preserves ``device_type`` and ``encrypted_identity_key``;
    ``_present_last_seen`` mirrors the monotonic poll-age map.
    """

    def __init__(  # noqa: D107 - documented at class level
        self,
        *,
        data: list[dict[str, Any]] | None = None,
        slots: dict[str, dict[str, Any]] | None = None,
        present_last_seen: dict[str, float] | None = None,
        stats: dict[str, int] | None = None,
    ) -> None:
        self.data = data
        self._device_location_data: dict[str, dict[str, Any]] = slots or {}
        self._present_last_seen: dict[str, float] = present_last_seen or {}
        # Minimal surface consumed by the rest of async_get_config_entry_diagnostics.
        self._device_names: dict[str, str] = {}
        self._last_poll_mono: float | None = None
        self.stats: dict[str, int] = stats or {}
        self.performance_metrics: dict[str, float] = {}
        self.recent_errors: list[object] = []
        self._enabled_poll_device_ids: set[str] = set()
        self._present_device_ids: set[str] = set()


def _patch_registries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralize loader and registry lookups so the dump builds in isolation."""

    async def _fake_get_integration(_hass: Any, _domain: str) -> SimpleNamespace:
        return SimpleNamespace(name="Test Integration", version="1.2.3")

    monkeypatch.setattr(diagnostics, "async_get_integration", _fake_get_integration)
    monkeypatch.setattr(
        diagnostics.dr, "async_get", lambda _hass: SimpleNamespace(devices={})
    )
    monkeypatch.setattr(
        diagnostics.er, "async_get", lambda _hass: SimpleNamespace(entities={})
    )


def _make_entry_and_hass(
    coordinator: _PerDeviceCoordinator,
) -> tuple[Any, SimpleNamespace]:
    """Build a canonical config entry plus a minimal hass referencing it."""
    entry = make_config_entry(
        entry_id="entry-p1",
        data={},
        options={},
        runtime_data=SimpleNamespace(coordinator=coordinator),
    )
    hass = SimpleNamespace(data={diagnostics.DOMAIN: {}})
    return entry, hass


# ---------------------------------------------------------------------------
# P1-1: HA core version
# ---------------------------------------------------------------------------


def test_ha_core_version_present_in_integration_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A1: ``integration["ha_core_version"]`` equals the mocked core version string.

    The integration reads ``homeassistant.const.__version__`` via ``sys.modules``
    at call time; the test suite swaps that module for a lightweight stub, so the
    version is mocked on the active ``sys.modules`` object rather than on the real
    package (which the stub shadows).
    """
    import sys

    ha_const = sys.modules["homeassistant.const"]
    monkeypatch.setattr(ha_const, "__version__", "2099.9.9", raising=False)
    coordinator = _PerDeviceCoordinator(data=[])
    entry, hass = _make_entry_and_hass(coordinator)
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))

    integration = payload["integration"]
    assert integration["ha_core_version"] == "2099.9.9"
    assert isinstance(integration["ha_core_version"], str)


# ---------------------------------------------------------------------------
# P1-2: canonicless drop counters surfaced under coordinator["stats"]
# ---------------------------------------------------------------------------


def test_drop_counters_surface_in_stats_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A3: the three drop counters appear under ``coordinator["stats"]`` as ints."""
    coordinator = _PerDeviceCoordinator(
        data=[],
        stats={
            "canonicless_drop_total": 5,
            "canonicless_drop_benign": 3,
            "canonicless_drop_warn": 2,
        },
    )
    entry, hass = _make_entry_and_hass(coordinator)
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))

    stats = payload["coordinator"]["stats"]
    assert stats["canonicless_drop_total"] == 5
    assert stats["canonicless_drop_benign"] == 3
    assert stats["canonicless_drop_warn"] == 2
    for key in (
        "canonicless_drop_total",
        "canonicless_drop_benign",
        "canonicless_drop_warn",
    ):
        assert isinstance(stats[key], int)


# ---------------------------------------------------------------------------
# P1-3: per-device telemetry list -- pure mapping helpers (boundary tables)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("accuracy", "expected"),
    [
        (0, "<10"),
        (9.9, "<10"),
        (10, "10-50"),
        (49.9, "10-50"),
        (50, "50-200"),
        (199.9, "50-200"),
        # The former single ">200" class was split (#216): it covered everything
        # from the accuracy gate's lower bound to infinity and so hid exactly the
        # range the gate acts on.
        (200, "200-500"),
        (499.9, "200-500"),
        (500, "500-2000"),
        (1600, "500-2000"),  # the radius reported in BSkando#216
        (2000, ">2000"),
        (50000, ">2000"),
        (None, None),
        (-1, None),
    ],
)
def test_accuracy_bucket_boundaries(accuracy: Any, expected: str | None) -> None:
    """A5: half-open accuracy intervals with None/negative collapsing to null."""
    assert coordinator_main._accuracy_bucket(accuracy) == expected


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (0, 0),
        (3725, 3720),
        (3727, 3730),
        (4, 0),
        (5, 0),
        (6, 10),
    ],
)
def test_round_age_to_ten_seconds(age: float, expected: int) -> None:
    """A5: ages are rounded via ``round(x/10)*10``."""
    assert coordinator_main._round_age(age) == expected


@pytest.mark.parametrize(
    ("device_type", "expected"),
    [
        (20, "phone"),
        (None, "other"),
        (0, "other"),
        (1, "bt_tracker"),
        (19, "bt_tracker"),
        (24, "bt_tracker"),
    ],
)
def test_device_class_mapping(device_type: Any, expected: str) -> None:
    """V3: the abrupt three-class mapping over the device_type int."""
    assert coordinator_main._device_class(device_type) == expected


# ---------------------------------------------------------------------------
# P1-3: per-device telemetry list -- coordinator helper
# ---------------------------------------------------------------------------


def test_per_device_empty_data_is_empty_list() -> None:
    """A4: empty/None ``self.data`` yields ``[]`` (no crash, no None)."""
    assert _PerDeviceCoordinator(data=None).build_per_device_diagnostics() == []  # type: ignore[attr-defined]
    assert _PerDeviceCoordinator(data=[]).build_per_device_diagnostics() == []  # type: ignore[attr-defined]


def test_per_device_schema_exact_keys_and_contiguous_index() -> None:
    """A4/A6: each entry has exactly the seven fields plus a 0..n-1 index.

    Two device IDs sorted as ``dev-a`` < ``dev-b`` deterministically map to index
    0 and 1; no extra keys appear.
    """
    coordinator = _PerDeviceCoordinator(
        data=[
            {"device_id": "dev-b", "is_own_report": False},
            {"device_id": "dev-a", "is_own_report": True},
        ],
        slots={
            "dev-a": {"device_type": 20, "encrypted_identity_key": b"k"},
            "dev-b": {"device_type": 1},
        },
    )
    entries = coordinator.build_per_device_diagnostics()  # type: ignore[attr-defined]

    assert [e["index"] for e in entries] == [0, 1]
    for entry in entries:
        assert set(entry.keys()) == _EXPECTED_DEVICE_KEYS
    # sorted(device_ids) -> dev-a (index 0) is the phone with a key.
    assert entries[0]["device_class"] == "phone"
    assert entries[0]["has_key"] is True
    assert entries[0]["is_own_report"] is True
    assert entries[1]["device_class"] == "bt_tracker"
    assert entries[1]["has_key"] is False


def test_per_device_age_disambiguation() -> None:
    """A4b: fix age (epoch ``last_seen``) and poll age (monotonic) are independent.

    A device with an OLD wall-clock fix but a FRESH monotonic poll must yield a
    large ``last_fix_age_s`` and a small ``last_poll_age_s``.
    """
    now_epoch = time.time()
    now_mono = time.monotonic()
    coordinator = _PerDeviceCoordinator(
        data=[
            {
                "device_id": "dev-old-fix",
                "last_seen": now_epoch - 3600.0,
                "is_own_report": False,
            }
        ],
        present_last_seen={"dev-old-fix": now_mono - 5.0},
    )
    entries = coordinator.build_per_device_diagnostics()  # type: ignore[attr-defined]
    entry = entries[0]
    assert entry["last_fix_age_s"] is not None
    assert entry["last_fix_age_s"] >= 3000
    assert entry["last_poll_age_s"] is not None
    assert entry["last_poll_age_s"] <= 60


def test_per_device_missing_timestamps_yield_null() -> None:
    """A4b: missing ``last_seen`` -> null fix age; missing slot -> null poll age."""
    coordinator = _PerDeviceCoordinator(
        data=[{"device_id": "dev-x", "is_own_report": None}],
        present_last_seen={},
    )
    entry = coordinator.build_per_device_diagnostics()[0]  # type: ignore[attr-defined]
    assert entry["last_fix_age_s"] is None
    assert entry["last_poll_age_s"] is None
    # V2/F3: is_own_report key stays present, value may be None.
    assert entry["is_own_report"] is None


def test_per_device_never_polled_slot_missing_defaults() -> None:
    """V2/R-P3: a never-polled device with no slot defaults to other/has_key=False."""
    coordinator = _PerDeviceCoordinator(
        data=[{"device_id": "ghost"}],
        slots={},
    )
    entry = coordinator.build_per_device_diagnostics()[0]  # type: ignore[attr-defined]
    assert entry["device_class"] == "other"
    assert entry["has_key"] is False
    assert entry["is_own_report"] is None


@pytest.mark.parametrize("accuracy_field", ["accuracy", "accuracy_m"])
def test_per_device_accuracy_bucketed_not_raw(accuracy_field: str) -> None:
    """A5/A8: a raw accuracy float is bucketed, never echoed verbatim.

    The runtime ``self.data`` snapshot rows carry the radius under
    ``accuracy``; ``accuracy_m`` is only produced later by
    ``_as_ha_attributes()`` for HA entity attributes. The diagnostics must read
    the snapshot field, so both the real ``accuracy`` path and a defensive
    ``accuracy_m`` fallback have to bucket correctly.
    """
    coordinator = _PerDeviceCoordinator(
        data=[{"device_id": "dev-acc", accuracy_field: 137.4, "is_own_report": False}],
    )
    entry = coordinator.build_per_device_diagnostics()[0]  # type: ignore[attr-defined]
    assert entry["last_accuracy_bucket"] == "50-200"
    assert 137.4 not in entry.values()


def test_per_device_skips_non_dict_rows() -> None:
    """Defensive: a non-dict entry in ``self.data`` is skipped, not crashed on."""
    coordinator = _PerDeviceCoordinator(
        data=["junk", {"device_id": "dev-a", "is_own_report": True}, None],  # type: ignore[list-item]
    )
    entries = coordinator.build_per_device_diagnostics()
    assert len(entries) == 1
    assert entries[0]["index"] == 0


# ---------------------------------------------------------------------------
# P1-1 / P1-3: resilience fallbacks
# ---------------------------------------------------------------------------


def test_ha_core_version_survives_loader_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A1/R-P3: if the manifest loader fails, ``ha_core_version`` is still present."""
    import sys

    ha_const = sys.modules["homeassistant.const"]
    monkeypatch.setattr(ha_const, "__version__", "2099.9.9", raising=False)

    async def _boom(_hass: Any, _domain: str) -> SimpleNamespace:
        raise RuntimeError("loader unavailable")

    monkeypatch.setattr(diagnostics, "async_get_integration", _boom)
    monkeypatch.setattr(
        diagnostics.dr, "async_get", lambda _hass: SimpleNamespace(devices={})
    )
    monkeypatch.setattr(
        diagnostics.er, "async_get", lambda _hass: SimpleNamespace(entities={})
    )
    coordinator = _PerDeviceCoordinator(data=[])
    entry, hass = _make_entry_and_hass(coordinator)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))
    assert payload["integration"]["ha_core_version"] == "2099.9.9"


def test_devices_block_empty_when_builder_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A coordinator without the P1-3 builder yields ``coordinator["devices"] == []``."""

    class _NoBuilderCoordinator:
        def __init__(self) -> None:
            self._device_names: dict[str, str] = {}
            self._device_location_data: dict[str, Any] = {}
            self._last_poll_mono: float | None = None
            self.stats: dict[str, int] = {}
            self.performance_metrics: dict[str, float] = {}
            self.recent_errors: list[object] = []
            self._enabled_poll_device_ids: set[str] = set()
            self._present_device_ids: set[str] = set()

    coordinator = _NoBuilderCoordinator()
    entry = make_config_entry(
        entry_id="entry-nb",
        runtime_data=SimpleNamespace(coordinator=coordinator),
    )
    hass = SimpleNamespace(data={diagnostics.DOMAIN: {}})
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))
    assert payload["coordinator"]["devices"] == []


def test_devices_block_empty_when_builder_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A builder that raises degrades to ``coordinator["devices"] == []`` (defensive)."""

    class _RaisingCoordinator(_PerDeviceCoordinator):
        def build_per_device_diagnostics(self) -> list[dict[str, Any]]:
            raise RuntimeError("boom")

    coordinator = _RaisingCoordinator(data=[])
    entry, hass = _make_entry_and_hass(coordinator)
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))
    assert payload["coordinator"]["devices"] == []


# ---------------------------------------------------------------------------
# P1-3: per-device list inside the dump -- privacy net (A7/A8)
# ---------------------------------------------------------------------------


def test_per_device_list_in_dump_introduces_no_redaction_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A7: the new list survives ``async_redact_data`` without TO_REDACT keys."""
    coordinator = _PerDeviceCoordinator(
        data=[
            {
                "device_id": "dev-secret",
                "last_seen": time.time() - 30.0,
                "accuracy_m": 12.0,
                "is_own_report": True,
            }
        ],
        slots={"dev-secret": {"device_type": 20, "encrypted_identity_key": b"k"}},
    )
    entry, hass = _make_entry_and_hass(coordinator)
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))

    devices = payload["coordinator"]["devices"]
    assert isinstance(devices, list)
    assert len(devices) == 1
    record = devices[0]
    # No entry key is a redaction-pending key, and nothing was redacted.
    for key in record:
        assert key not in diagnostics.TO_REDACT
        assert record[key] != diagnostics.REDACTED
    assert set(record.keys()) == _EXPECTED_DEVICE_KEYS


def test_per_device_block_has_no_identifying_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A8: a recursive scan of the per-device block finds no forbidden artifacts."""
    coordinator = _PerDeviceCoordinator(
        data=[
            {
                "device_id": "dev-secret",
                "device_name": "Garage Tracker",
                "latitude": 52.5,
                "longitude": 13.4,
                "last_seen": time.time() - 30.0,
                "accuracy_m": 12.0,
                "is_own_report": True,
            }
        ],
        slots={"dev-secret": {"device_type": 20, "encrypted_identity_key": b"secret"}},
    )
    entry, hass = _make_entry_and_hass(coordinator)
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))
    devices = payload["coordinator"]["devices"]

    forbidden_substrings = ("garage", "tracker", "dev-secret", "secret")

    def _scan(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                assert "name" not in key.lower()
                _scan(value)
        elif isinstance(node, list):
            for item in node:
                _scan(item)
        elif isinstance(node, str):
            lowered = node.lower()
            for needle in forbidden_substrings:
                assert needle not in lowered
        elif isinstance(node, (bytes, bytearray)):
            raise AssertionError("raw bytes leaked into the per-device block")
        else:
            # numeric / bool / None are safe.
            assert isinstance(node, (int, float, bool)) or node is None

    _scan(devices)
    # Forbidden float coordinates must not appear anywhere in the block.
    flat = repr(devices)
    assert "52.5" not in flat
    assert "13.4" not in flat
