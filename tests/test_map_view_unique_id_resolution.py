# tests/test_map_view_unique_id_resolution.py
"""Tests for resolving map view tracker entities by exact unique_id match."""

from __future__ import annotations

import importlib
import importlib.util
import sys
from collections import OrderedDict
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy.const import (
    DOMAIN,
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
)
from tests.helpers import install_homeassistant_core_callback_stub


class _StubCoordinator:
    """Coordinator stub that exposes a devices snapshot."""

    def __init__(self, devices: list[dict[str, Any]]) -> None:
        self.data = devices

    def stable_subentry_identifier(
        self, *, key: str | None = None, feature: str | None = None
    ) -> str:
        assert key is not None, "Map view should request subentry identifiers by key"
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
        return list(self.data)

    def is_device_visible_in_subentry(self, subentry_key: str, device_id: str) -> bool:
        return True

    def attach_subentry_manager(self, manager: Any, *, is_reload: bool = False) -> None:
        self.subentry_manager = manager
        self._is_reload = is_reload


class _StubEntry:
    """Config entry stub carrying runtime data."""

    def __init__(self, entry_id: str, coordinator: _StubCoordinator) -> None:
        self.entry_id = entry_id
        self.data: dict[str, Any] = {}
        self.options: dict[str, Any] = {}
        self.runtime_data = SimpleNamespace(coordinator=coordinator)


class _StubHass:
    """Minimal Home Assistant stub for the map view handler."""

    def __init__(self) -> None:
        self.data: dict[str, Any] = {}

    async def async_add_executor_job(self, func: Any, *args: Any) -> Any:
        """Execute the provided callable synchronously during tests."""

        return func(*args)


class _StubEntityEntry:
    """Entity registry entry stub used for lookup assertions."""

    def __init__(
        self,
        *,
        entity_id: str,
        unique_id: str,
        config_entry_id: str,
    ) -> None:
        self.entity_id = entity_id
        self.unique_id = unique_id
        self.config_entry_id = config_entry_id
        self.platform = DOMAIN


class _StubEntityRegistry:
    """Entity registry stub that emulates HA lookups."""

    def __init__(self, entries: list[_StubEntityEntry]) -> None:
        ordered = OrderedDict((entry.entity_id, entry) for entry in entries)
        self.entities: OrderedDict[str, _StubEntityEntry] = ordered

    def async_get_entity_id(
        self, domain: str, platform: str, unique_id: str
    ) -> str | None:
        for entry in self.entities.values():
            if (
                entry.entity_id.startswith(f"{domain}.")
                and entry.platform == platform
                and entry.unique_id == unique_id
            ):
                return entry.entity_id
        return None

    def async_get(self, entity_id: str) -> _StubEntityEntry | None:
        return self.entities.get(entity_id)


def _load_real_safe_accuracy() -> Any:
    """Load the canonical ``safe_accuracy`` from geo.py by file path.

    Loading the module directly (rather than importing it through the package)
    keeps this lightweight loader free of the heavy integration import chain
    while still exercising the real accuracy policy that production re-exports as
    ``coordinator.safe_accuracy``. geo.py imports only ``math``/``typing`` and has
    no relative imports, so it loads in isolation.
    """

    geo_path = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "googlefindmy"
        / "coordinator"
        / "helpers"
        / "geo.py"
    )
    spec = importlib.util.spec_from_file_location("googlefindmy_test_geo", geo_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load geo module for testing")
    geo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(geo)
    return geo.safe_accuracy


def _load_real_is_valid_accuracy() -> Any:
    """Load the canonical ``is_valid_accuracy`` from geo.py by file path.

    Mirrors ``_load_real_safe_accuracy`` so the stub exposes the same accuracy
    re-exports that production publishes on ``coordinator.is_valid_accuracy``.
    geo.py has no relative imports, so it loads in isolation without dragging in
    the heavy integration import chain.
    """

    geo_path = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "googlefindmy"
        / "coordinator"
        / "helpers"
        / "geo.py"
    )
    spec = importlib.util.spec_from_file_location("googlefindmy_test_geo", geo_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load geo module for testing")
    geo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(geo)
    return geo.is_valid_accuracy


def _load_real_parse_last_seen_timestamp() -> Any:
    """Load the canonical ``parse_last_seen_timestamp`` from subentry.py by path.

    Mirrors ``_load_real_safe_accuracy`` so the stub exposes the same timestamp
    parser that production re-exports on ``coordinator.parse_last_seen_timestamp``.
    subentry.py imports only ``math``/``datetime``/``typing`` (and defines its own
    ``normalize_epoch_seconds`` helper), so it loads in isolation without dragging
    in the heavy integration import chain.
    """

    subentry_path = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "googlefindmy"
        / "coordinator"
        / "helpers"
        / "subentry.py"
    )
    spec = importlib.util.spec_from_file_location(
        "googlefindmy_test_subentry", subentry_path
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load subentry module for testing")
    subentry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(subentry)
    return subentry.parse_last_seen_timestamp


#: Read once per test session. Several callers of ``_load_map_view_module`` run
#: inside an ``async def`` test, so re-reading the 160 KiB of vendored assets on
#: every module load would put exactly the blocking read this change removes
#: back into a running event loop, just in the test tree. Stated plainly: this
#: leaves at most one such read per session (whichever caller comes first)
#: instead of one per module load.
_LEAFLET_ASSETS_CACHE: dict[str, str] = {}


def _leaflet_assets_once(map_view_module: ModuleType) -> dict[str, str]:
    """Return the vendored Leaflet assets, reading them at most once."""

    if not _LEAFLET_ASSETS_CACHE:
        _LEAFLET_ASSETS_CACHE.update(map_view_module._read_leaflet_assets())
    return _LEAFLET_ASSETS_CACHE


def _load_map_view_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Load the map_view module with stubbed Home Assistant dependencies."""

    http_module = ModuleType("homeassistant.components.http")

    class _HttpViewStub:
        def __init__(self, hass: Any | None = None) -> None:
            self.hass = hass

    http_module.HomeAssistantView = _HttpViewStub
    monkeypatch.setitem(sys.modules, "homeassistant.components.http", http_module)

    helpers_http_module = ModuleType("homeassistant.helpers.http")
    helpers_http_module.HomeAssistantView = _HttpViewStub
    helpers_http_module.request_handler_factory = lambda hass, view, handler: handler
    monkeypatch.setitem(sys.modules, "homeassistant.helpers.http", helpers_http_module)

    core_module = install_homeassistant_core_callback_stub(monkeypatch)

    class _HomeAssistantStub:  # pragma: no cover - structural stub
        pass

    monkeypatch.setattr(
        core_module,
        "HomeAssistant",
        _HomeAssistantStub,
        raising=False,
    )

    helpers_module = ModuleType("homeassistant.helpers.entity_registry")
    helpers_module.async_get = lambda _hass: None
    monkeypatch.setitem(
        sys.modules, "homeassistant.helpers.entity_registry", helpers_module
    )

    helpers_pkg = ModuleType("homeassistant.helpers")
    helpers_pkg.__path__ = []
    helpers_pkg.entity_registry = helpers_module
    helpers_pkg.http = helpers_http_module
    monkeypatch.setitem(sys.modules, "homeassistant.helpers", helpers_pkg)

    homeassistant_pkg = ModuleType("homeassistant")
    homeassistant_pkg.__path__ = []
    homeassistant_pkg.helpers = helpers_pkg
    monkeypatch.setitem(sys.modules, "homeassistant", homeassistant_pkg)

    dt_module = ModuleType("homeassistant.util.dt")
    dt_module.utcnow = lambda: datetime.now(UTC)
    dt_module.as_local = lambda value: value
    dt_module.UTC = UTC
    dt_module.as_utc = lambda value: (
        value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    )

    def _parse_datetime(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed

    dt_module.parse_datetime = _parse_datetime
    monkeypatch.setitem(sys.modules, "homeassistant.util.dt", dt_module)

    util_module = ModuleType("homeassistant.util")
    util_module.dt = dt_module
    monkeypatch.setitem(sys.modules, "homeassistant.util", util_module)

    custom_components_pkg = ModuleType("custom_components")
    custom_components_pkg.__path__ = [
        str(Path(__file__).resolve().parents[1] / "custom_components")
    ]
    monkeypatch.setitem(sys.modules, "custom_components", custom_components_pkg)

    googlefindmy_pkg = ModuleType("custom_components.googlefindmy")
    googlefindmy_pkg.__path__ = [
        str(Path(__file__).resolve().parents[1] / "custom_components" / "googlefindmy")
    ]
    monkeypatch.setitem(sys.modules, "custom_components.googlefindmy", googlefindmy_pkg)

    coordinator_module = ModuleType("custom_components.googlefindmy.coordinator")
    coordinator_module.GoogleFindMyCoordinator = _StubCoordinator
    # Expose safe_accuracy as a direct attribute, mirroring the production
    # re-export in coordinator/__init__.py. map_view resolves it via
    # ``from .coordinator import safe_accuracy``; without this attribute the
    # plain ModuleType stub (no __path__) would make that import fail.
    coordinator_module.safe_accuracy = _load_real_safe_accuracy()
    # Mirror the production re-export of ``is_valid_accuracy`` (see
    # coordinator/__init__.py) so the map can flag estimated points without the
    # plain ModuleType stub (no __path__) breaking the lazy import.
    coordinator_module.is_valid_accuracy = _load_real_is_valid_accuracy()
    # Mirror the production re-export of ``parse_last_seen_timestamp`` (see
    # coordinator/__init__.py) so the map history can normalize recorded
    # last_seen/last_seen_utc attributes without the plain ModuleType stub (no
    # __path__) breaking the lazy import.
    coordinator_module.parse_last_seen_timestamp = (
        _load_real_parse_last_seen_timestamp()
    )
    monkeypatch.setitem(
        sys.modules, "custom_components.googlefindmy.coordinator", coordinator_module
    )
    # Force every coordinator lookup through the stub. If a previous test already
    # imported the real ``coordinator.helpers.geo`` it would linger in sys.modules
    # and let a nested ``from .coordinator.helpers.geo import ...`` resolve from
    # that cache, masking the very import-order fragility this loader guards
    # against. Dropping the cached submodules makes the stub the only source.
    for cached in (
        "custom_components.googlefindmy.coordinator.helpers.geo",
        "custom_components.googlefindmy.coordinator.helpers",
    ):
        monkeypatch.delitem(sys.modules, cached, raising=False)

    module_name = "custom_components.googlefindmy.map_view"
    spec = importlib.util.spec_from_file_location(
        module_name,
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "googlefindmy"
        / "map_view.py",
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise RuntimeError("Failed to load map_view module for testing")
    map_view = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, map_view)
    spec.loader.exec_module(map_view)
    # A freshly executed module starts with an empty Leaflet cache, and
    # ``_leaflet_asset`` deliberately refuses to read through on a miss (that
    # read is the blocking event-loop call the cache exists to avoid). In
    # production the request handler fills it via
    # ``_async_prime_leaflet_cache``; the loader stands in for that step so the
    # direct ``_generate_map_html`` callers below keep working.
    map_view._LEAFLET_CACHE.update(_leaflet_assets_once(map_view))
    return map_view


async def test_map_view_prefers_exact_unique_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Tracker selection must match explicit unique_id formats before fallback."""

    map_view = _load_map_view_module(monkeypatch)

    device_id = "device-abc"
    coordinator = _StubCoordinator(
        devices=[
            {"id": device_id, "name": "Primary Device"},
            {"id": "device-abc-shadow", "name": "Shadow Device"},
        ]
    )
    entry = _StubEntry("entry-123", coordinator)

    identifier = coordinator.stable_subentry_identifier(key=TRACKER_SUBENTRY_KEY)
    target_unique_id = f"{entry.entry_id}:{identifier}:{device_id}"
    overlapping_unique_id = f"{entry.entry_id}:{identifier}:{device_id}-shadow"

    registry = _StubEntityRegistry(
        [
            _StubEntityEntry(
                entity_id="device_tracker.googlefindmy_shadow",
                unique_id=overlapping_unique_id,
                config_entry_id=entry.entry_id,
            ),
            _StubEntityEntry(
                entity_id="device_tracker.googlefindmy_primary",
                unique_id=target_unique_id,
                config_entry_id=entry.entry_id,
            ),
        ]
    )

    monkeypatch.setattr(
        map_view, "GoogleFindMyCoordinator", _StubCoordinator, raising=False
    )
    monkeypatch.setattr(
        map_view,
        "_resolve_entry_by_token",
        lambda _hass, token: (entry, {token}) if token == "valid" else (None, None),
        raising=False,
    )
    monkeypatch.setattr(
        map_view.er,
        "async_get",
        lambda _hass: registry,
        raising=False,
    )

    history_calls: list[list[str]] = []

    def _stub_history(
        _hass: Any, _start: Any, _end: Any, entity_ids: list[str]
    ) -> dict[str, Any]:
        history_calls.append(list(entity_ids))
        return {}

    history_module = ModuleType("homeassistant.components.recorder.history")
    history_module.get_significant_states = _stub_history
    monkeypatch.setitem(
        sys.modules,
        "homeassistant.components.recorder.history",
        history_module,
    )

    hass = _StubHass()
    view = map_view.GoogleFindMyMapView(hass)

    request = SimpleNamespace(query={"token": "valid"})
    response = await view.get(request, device_id)

    assert response.status == 200
    assert history_calls == [["device_tracker.googlefindmy_primary"]]


async def test_map_view_uses_iso_last_seen_for_timeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ISO last_seen strings must drive ordering and de-duplication."""

    map_view = _load_map_view_module(monkeypatch)

    device_id = "device-iso"
    coordinator = _StubCoordinator(devices=[{"id": device_id, "name": "ISO Device"}])
    entry = _StubEntry("entry-iso", coordinator)

    identifier = coordinator.stable_subentry_identifier(key=TRACKER_SUBENTRY_KEY)
    registry = _StubEntityRegistry(
        [
            _StubEntityEntry(
                entity_id="device_tracker.googlefindmy_primary",
                unique_id=f"{entry.entry_id}:{identifier}:{device_id}",
                config_entry_id=entry.entry_id,
            )
        ]
    )

    monkeypatch.setattr(
        map_view, "GoogleFindMyCoordinator", _StubCoordinator, raising=False
    )
    monkeypatch.setattr(
        map_view,
        "_resolve_entry_by_token",
        lambda _hass, token: (entry, {token}) if token == "valid" else (None, None),
        raising=False,
    )
    monkeypatch.setattr(
        map_view.er,
        "async_get",
        lambda _hass: registry,
        raising=False,
    )

    iso_old = "2024-01-01T00:00:00Z"
    iso_new = "2024-01-02T00:00:00Z"
    history_states = [
        SimpleNamespace(
            attributes={
                "latitude": "10.0",
                "longitude": "20.0",
                "last_seen": iso_old,
                "gps_accuracy": 5,
            },
            last_updated=datetime(2024, 7, 1, tzinfo=UTC),
            state="one",
        ),
        SimpleNamespace(
            attributes={
                "latitude": "11.0",
                "longitude": "21.0",
                "last_seen": iso_new,
                "gps_accuracy": 10,
            },
            last_updated=datetime(2024, 5, 1, tzinfo=UTC),
            state="two",
        ),
        SimpleNamespace(
            attributes={
                "latitude": "11.5",
                "longitude": "21.5",
                "last_seen": iso_new,
                "gps_accuracy": 15,
            },
            last_updated=datetime(2024, 8, 1, tzinfo=UTC),
            state="duplicate",
        ),
    ]

    def _stub_history(
        _hass: Any, _start: Any, _end: Any, entity_ids: list[str]
    ) -> dict[str, Any]:
        assert entity_ids == ["device_tracker.googlefindmy_primary"]
        return {"device_tracker.googlefindmy_primary": history_states}

    history_module = ModuleType("homeassistant.components.recorder.history")
    history_module.get_significant_states = _stub_history
    monkeypatch.setitem(
        sys.modules,
        "homeassistant.components.recorder.history",
        history_module,
    )

    captured_locations: list[dict[str, Any]] = []

    def _capture_html(
        self: Any,
        _device_name: str,
        locations: list[dict[str, Any]],
        *_args: Any,
        **_kwargs: Any,
    ) -> str:
        captured_locations.extend(locations)
        return "ok"

    monkeypatch.setattr(
        map_view.GoogleFindMyMapView,
        "_generate_map_html",
        _capture_html,
        raising=False,
    )

    hass = _StubHass()
    view = map_view.GoogleFindMyMapView(hass)

    request = SimpleNamespace(query={"token": "valid"})
    response = await view.get(request, device_id)

    assert response.status == 200
    assert len(captured_locations) == 2

    iso_old_ts = datetime.fromisoformat(iso_old.replace("Z", "+00:00")).timestamp()
    iso_new_ts = datetime.fromisoformat(iso_new.replace("Z", "+00:00")).timestamp()

    assert captured_locations[0]["last_seen"] == pytest.approx(iso_old_ts)
    assert captured_locations[1]["last_seen"] == pytest.approx(iso_new_ts)
    assert captured_locations[0]["last_seen"] < captured_locations[1]["last_seen"]
    assert captured_locations[0]["last_seen"] != pytest.approx(
        history_states[0].last_updated.timestamp()
    )


def test_map_view_html_uses_iso_conversion(monkeypatch: pytest.MonkeyPatch) -> None:
    """Embedded scripts must convert date inputs via toISOString and local formatters."""

    map_view = _load_map_view_module(monkeypatch)
    hass = _StubHass()
    view = map_view.GoogleFindMyMapView(hass)

    start = datetime(2024, 1, 1, tzinfo=UTC)
    end = datetime(2024, 1, 2, tzinfo=UTC)
    locations = [
        {
            "lat": 10.0,
            "lon": 20.0,
            "accuracy": 5.0,
            "timestamp": "2024-01-01T00:00:00+00:00",
            "last_seen": start.timestamp(),
            "entity_id": "device_tracker.sample",
            "state": "home",
            "is_own_report": True,
            "semantic_location": "Sample",
        }
    ]

    html_with_locations = view._generate_map_html(
        "Device", locations, "device-1", start, end, 0
    )
    html_empty = view._generate_map_html("Device", [], "device-1", start, end, 0)

    # Date inputs are still converted via toISOString, now inside the shared
    # setDateParam helper behind an isNaN validity guard, so a cleared
    # datetime-local field no longer throws a RangeError and its stale query
    # parameter is deleted rather than preserved (see
    # test_map_view_bug_hardening BUG 3).
    assert "parsed.toISOString()" in html_with_locations
    assert "parsed.toISOString()" in html_empty
    assert "isNaN(parsed.getTime())" in html_with_locations
    assert "setDateParam(url, 'start')" in html_with_locations
    assert "setDateParam(url, 'end')" in html_with_locations
    assert "getFullYear()" in html_with_locations
    assert "getFullYear()" in html_empty


def test_map_view_resolves_safe_accuracy_under_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The accuracy resolver must work under the lightweight coordinator stub.

    Regression for the Codex #186 finding: ``_resolve_safe_accuracy`` previously
    imported ``.coordinator.helpers.geo``, which requires the coordinator to be a
    real package. Under the plain ModuleType stub installed by this loader the
    nested import aborted with "coordinator is not a package", so any history
    render exercising the accuracy filter would crash. Resolving the re-exported
    ``coordinator.safe_accuracy`` keeps the map renderable under the stub.
    """

    map_view = _load_map_view_module(monkeypatch)

    safe_accuracy = map_view._resolve_safe_accuracy()

    # Canonical policy: valid measurements pass through unchanged, while the 0.0
    # Android error code and missing values map to the conservative fallback.
    assert safe_accuracy(50.0) == 50.0
    assert safe_accuracy(0.0) == 200.0
    assert safe_accuracy(None) == 200.0

    # Second call must hit the cache instead of re-importing (same object).
    assert map_view._resolve_safe_accuracy() is safe_accuracy


def test_map_view_resolves_is_valid_accuracy_under_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The validity predicate resolver must work under the coordinator stub.

    The stub re-exports ``is_valid_accuracy`` exactly like production
    (coordinator/__init__.py), so ``_resolve_is_valid_accuracy`` returns the real
    predicate and classifies raw gps_accuracy values with the canonical policy.
    """

    map_view = _load_map_view_module(monkeypatch)

    is_valid_accuracy = map_view._resolve_is_valid_accuracy()

    # Canonical policy: real measurements are valid; the 0.0 error code, missing
    # values, and sub-millimeter noise fall back to the conservative radius.
    assert is_valid_accuracy(50.0) is True
    assert is_valid_accuracy(0.0) is False
    assert is_valid_accuracy(None) is False

    # Second call must hit the cache instead of re-importing (same object).
    assert map_view._resolve_is_valid_accuracy() is is_valid_accuracy


def test_resolve_is_valid_accuracy_falls_back_when_symbol_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A coordinator stub without ``is_valid_accuracy`` must not crash the map.

    Older or partial coordinator stubs may re-export ``safe_accuracy`` but not
    ``is_valid_accuracy``. The lazy resolver must catch the resulting ImportError
    and fall back to a predicate that mirrors the geo.py validity policy
    (None -> False; float-cast; finite and >= 0.001m). Without the fallback the
    history render aborts with ImportError (Codex #1124 finding 3).
    """

    map_view = _load_map_view_module(monkeypatch)

    # Simulate a coordinator stub that lacks the re-export.
    coordinator_stub = sys.modules["custom_components.googlefindmy.coordinator"]
    monkeypatch.delattr(coordinator_stub, "is_valid_accuracy", raising=False)
    monkeypatch.setattr(map_view, "_IS_VALID_ACCURACY", None, raising=False)

    is_valid_accuracy = map_view._resolve_is_valid_accuracy()

    # Fallback predicate must reproduce the canonical geo.py semantics.
    assert is_valid_accuracy(50.0) is True
    assert is_valid_accuracy(0.0) is False
    assert is_valid_accuracy(0.0005) is False  # below MIN_VALID_ACCURACY (0.001m)
    assert is_valid_accuracy(0.001) is True
    assert is_valid_accuracy(-1.0) is False
    assert is_valid_accuracy(float("nan")) is False
    assert is_valid_accuracy(float("inf")) is False
    assert is_valid_accuracy(None) is False
    assert is_valid_accuracy("75") is True  # float-cast accepts numeric strings
    assert is_valid_accuracy("abc") is False

    # Second call must hit the cache instead of re-resolving (same object).
    assert map_view._resolve_is_valid_accuracy() is is_valid_accuracy


def test_map_view_resolves_parse_last_seen_timestamp_under_stub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The timestamp parser resolver must work under the coordinator stub.

    The stub re-exports ``parse_last_seen_timestamp`` exactly like production
    (coordinator/__init__.py), so ``_resolve_parse_last_seen_timestamp`` returns
    the real parser. It normalizes numeric epochs and ISO 8601 strings (incl. the
    ``last_seen_utc`` mirror) back to epoch seconds, and returns None for
    unparseable input so the map history can fall through to ``last_updated``.
    """

    map_view = _load_map_view_module(monkeypatch)

    parse_last_seen_timestamp = map_view._resolve_parse_last_seen_timestamp()

    # ISO 8601 (the recorded last_seen/last_seen_utc shape) -> epoch seconds.
    assert parse_last_seen_timestamp("2024-01-01T00:00:00Z") == pytest.approx(
        datetime(2024, 1, 1, tzinfo=UTC).timestamp()
    )
    # Numeric epoch passes through as float.
    assert parse_last_seen_timestamp(1704067200.0) == pytest.approx(1704067200.0)
    # Unparseable input yields None (caller keeps the last_updated default).
    assert parse_last_seen_timestamp("not-a-timestamp") is None
    assert parse_last_seen_timestamp(None) is None

    # Second call must hit the cache instead of re-importing (same object).
    assert map_view._resolve_parse_last_seen_timestamp() is parse_last_seen_timestamp
