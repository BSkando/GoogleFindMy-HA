# tests/test_coordinator_main_basics.py
"""Branch-coverage tests for ``coordinator.main`` pure helpers and static methods.

Phase 3 AP-F: target the pure helpers and ``GoogleFindMyCoordinator`` static
methods exposed by ``coordinator/main.py``. Async lifecycle methods
(``async_setup``, ``async_shutdown``, ``async_update_data``) stay out of
scope; they land in Phase 4 with HA-loop test methodology.
"""

from __future__ import annotations

import math
import sys
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from custom_components.googlefindmy.coordinator import main as main_module
from custom_components.googlefindmy.coordinator.main import (
    DeviceIdentity,
    GoogleFindMyCoordinator,
    SemanticLabelRecord,
    _as_ha_attributes,
    _get_api_class,
    _normalize_epoch_seconds,
    _parse_last_seen_timestamp,
    _RecorderHistoryProxy,
    _resolve_last_seen_from_attributes,
    _sync_get_last_gps_from_history,
    _update_preserve_metadata,
    format_epoch_utc,
    get_recorder,
    normalize_epoch_seconds,
)
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.main_coordinator_stub import MainCoordinatorStub

# ---------------------------------------------------------------------------
# Module-level pure helpers
# ---------------------------------------------------------------------------


class TestUpdatePreserveMetadata:
    """``_update_preserve_metadata`` keeps persisted metadata sticky."""

    def test_non_metadata_keys_use_dict_update_semantics(self) -> None:
        target = {"battery_level": 70, "name": "Old"}
        source = {"name": "New", "status": "Online"}

        _update_preserve_metadata(target, source)

        assert target == {"battery_level": 70, "name": "New", "status": "Online"}

    def test_metadata_key_with_none_value_does_not_clobber_existing(self) -> None:
        target = {"pair_date": 80}
        source = {"pair_date": None}

        _update_preserve_metadata(target, source)

        assert target == {"pair_date": 80}

    def test_metadata_key_with_real_value_overwrites(self) -> None:
        target = {"pair_date": 80}
        source = {"pair_date": 90}

        _update_preserve_metadata(target, source)

        assert target == {"pair_date": 90}

    def test_metadata_key_added_when_target_missing(self) -> None:
        target: dict[str, Any] = {}
        source = {"identity_key": b"\xaa\xbb"}

        _update_preserve_metadata(target, source)

        assert target == {"identity_key": b"\xaa\xbb"}

    def test_metadata_none_does_not_add_key(self) -> None:
        target: dict[str, Any] = {}
        source = {"identity_key": None, "pair_date": None}

        _update_preserve_metadata(target, source)

        assert target == {}


class TestEpochHelpers:
    """``normalize_epoch_seconds`` and ``format_epoch_utc`` are tolerant aliases."""

    def test_normalize_epoch_seconds_public_and_alias_agree(self) -> None:
        public_result = normalize_epoch_seconds(1_700_000_000)
        alias_result = _normalize_epoch_seconds(1_700_000_000)

        assert public_result == 1_700_000_000
        assert alias_result == 1_700_000_000

    def test_normalize_epoch_seconds_accepts_milliseconds(self) -> None:
        # 1.7e12 looks like ms-since-epoch; the helper normalises to seconds.
        result = normalize_epoch_seconds(1_700_000_000_000)

        assert result == 1_700_000_000

    def test_normalize_epoch_seconds_returns_none_for_invalid(self) -> None:
        assert normalize_epoch_seconds("not a number") is None
        assert normalize_epoch_seconds(None) is None

    def test_format_epoch_utc_returns_iso8601(self) -> None:
        # 2023-11-14T22:13:20Z = 1700000000
        result = format_epoch_utc(1_700_000_000)

        assert isinstance(result, str)
        assert result.startswith("2023-11-14T22:13:20")

    def test_format_epoch_utc_none_in_none_out(self) -> None:
        assert format_epoch_utc(None) is None


class TestLastSeenHelpers:
    """``_parse_last_seen_timestamp`` and ``_resolve_last_seen_from_attributes``."""

    def test_parse_last_seen_accepts_epoch_int(self) -> None:
        assert _parse_last_seen_timestamp(1_700_000_000) == 1_700_000_000.0

    def test_parse_last_seen_returns_none_for_unparseable(self) -> None:
        assert _parse_last_seen_timestamp("not a timestamp") is None

    def test_resolve_returns_fallback_when_attributes_empty(self) -> None:
        result = _resolve_last_seen_from_attributes(None, 42.0)

        assert result == 42.0

    def test_resolve_returns_fallback_when_attributes_lack_last_seen(self) -> None:
        result = _resolve_last_seen_from_attributes({"other": "key"}, 99.0)

        assert result == 99.0

    def test_resolve_prefers_last_seen_attribute(self) -> None:
        attrs = {"last_seen": 1_700_000_000}

        result = _resolve_last_seen_from_attributes(attrs, 0.0)

        assert result == 1_700_000_000.0

    def test_resolve_falls_back_to_last_seen_utc(self) -> None:
        attrs = {"last_seen": None, "last_seen_utc": 1_700_000_000}

        result = _resolve_last_seen_from_attributes(attrs, 0.0)

        assert result == 1_700_000_000.0


class TestAsHaAttributes:
    """``_as_ha_attributes`` curates HA-recorder-friendly attribute dicts."""

    def test_empty_row_returns_none(self) -> None:
        assert _as_ha_attributes(None) is None
        assert _as_ha_attributes({}) is None

    def test_full_row_includes_lat_lon_and_optional_fields(self) -> None:
        row = {
            "name": "Phone",
            "device_id": "abc",
            "status": "Online",
            "semantic_name": "Home",
            "battery_level": 90,
            "latitude": 1.0,
            "longitude": 2.0,
            "accuracy": 5.0,
            "altitude": 100.0,
            "last_seen": 1_700_000_000,
        }

        result = _as_ha_attributes(row)

        assert result is not None
        assert result["device_name"] == "Phone"
        assert result["device_id"] == "abc"
        assert result["latitude"] == 1.0
        assert result["longitude"] == 2.0
        assert result["accuracy_m"] == 5.0
        assert result["altitude_m"] == 100.0
        assert result["last_seen"].startswith("2023-11-14T22:13:20")

    def test_partial_row_drops_lat_lon_when_one_missing(self) -> None:
        row = {"name": "Phone", "latitude": 1.0, "longitude": None}

        result = _as_ha_attributes(row)

        assert result is not None
        assert "latitude" not in result
        assert "longitude" not in result

    def test_non_finite_lat_lon_are_dropped(self) -> None:
        row = {"latitude": math.nan, "longitude": math.inf}

        result = _as_ha_attributes(row)

        assert result is not None
        assert "latitude" not in result
        assert "longitude" not in result

    def test_id_falls_back_to_device_id_field(self) -> None:
        row = {"id": "fallback-id"}

        result = _as_ha_attributes(row)

        assert result is not None
        assert result["device_id"] == "fallback-id"


# ---------------------------------------------------------------------------
# _RecorderHistoryProxy & get_recorder
# ---------------------------------------------------------------------------


class TestRecorderHistoryProxy:
    """``_RecorderHistoryProxy`` is a lazy passthrough to the HA history module."""

    def test_load_caches_module_after_first_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # ``_RecorderHistoryProxy._load`` executes
        # ``from homeassistant.components.recorder import history as history_module``.
        # That statement resolves through ``sys.modules``, so the test must
        # replace the module entries there rather than ``setattr`` on the
        # parent package (which only retargets the attribute lookup, not the
        # cached import path used by ``from ... import history``).
        fake_history = ModuleType("homeassistant.components.recorder.history")
        fake_recorder = ModuleType("homeassistant.components.recorder")
        fake_recorder.history = fake_history  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "homeassistant.components.recorder", fake_recorder
        )
        monkeypatch.setitem(
            sys.modules,
            "homeassistant.components.recorder.history",
            fake_history,
        )

        proxy = _RecorderHistoryProxy()
        first = proxy._load()
        second = proxy._load()

        assert first is fake_history
        assert first is second

    def test_get_recorder_imports_lazily(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # ``get_recorder`` runs
        # ``from homeassistant.components.recorder import get_instance as ...``;
        # the lookup goes through ``sys.modules["homeassistant.components.recorder"]``.
        # ``monkeypatch.setattr("homeassistant.components.recorder", ...)`` only
        # rebinds the ``recorder`` attribute on the parent package and leaves
        # the conftest stub in ``sys.modules`` in place, so we replace the
        # cached module instead.
        sentinel = object()
        fake_recorder = ModuleType("homeassistant.components.recorder")
        fake_recorder.get_instance = lambda hass: sentinel  # type: ignore[attr-defined]
        monkeypatch.setitem(
            sys.modules, "homeassistant.components.recorder", fake_recorder
        )

        result = get_recorder(MagicMock())

        assert result is sentinel


class TestSyncGetLastGpsFromHistory:
    """``_sync_get_last_gps_from_history`` reads the most recent GPS state."""

    def test_no_samples_returns_none(self) -> None:
        history = MagicMock()
        history.get_last_state_changes = MagicMock(return_value={"entity": []})

        with pytest.MonkeyPatch.context() as m:
            m.setattr(main_module, "recorder_history", history)
            result = _sync_get_last_gps_from_history(MagicMock(), "entity")

        assert result is None

    def test_missing_lat_lon_returns_none(self) -> None:
        sample = SimpleNamespace(
            attributes={"gps_accuracy": 10},
            last_updated=SimpleNamespace(timestamp=lambda: 1_700_000_000.0),
        )
        history = MagicMock()
        history.get_last_state_changes = MagicMock(return_value={"entity": [sample]})

        with pytest.MonkeyPatch.context() as m:
            m.setattr(main_module, "recorder_history", history)
            result = _sync_get_last_gps_from_history(MagicMock(), "entity")

        assert result is None

    def test_happy_path_returns_dict(self) -> None:
        sample = SimpleNamespace(
            attributes={
                "latitude": 1.0,
                "longitude": 2.0,
                "gps_accuracy": 5,
                "last_seen": 1_700_000_000,
            },
            last_updated=SimpleNamespace(timestamp=lambda: 1_700_000_000.0),
        )
        history = MagicMock()
        history.get_last_state_changes = MagicMock(return_value={"entity": [sample]})

        with pytest.MonkeyPatch.context() as m:
            m.setattr(main_module, "recorder_history", history)
            result = _sync_get_last_gps_from_history(MagicMock(), "entity")

        assert result is not None
        assert result["latitude"] == 1.0
        assert result["longitude"] == 2.0
        assert result["accuracy"] == 5
        assert result["last_seen"] == 1_700_000_000.0
        assert result["status"] == "Using historical data"

    def test_history_exception_returns_none(self) -> None:
        history = MagicMock()
        history.get_last_state_changes = MagicMock(side_effect=RuntimeError("boom"))

        with pytest.MonkeyPatch.context() as m:
            m.setattr(main_module, "recorder_history", history)
            result = _sync_get_last_gps_from_history(MagicMock(), "entity")

        assert result is None


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


class TestSemanticLabelRecordCopy:
    """``SemanticLabelRecord.copy`` returns an independent device set."""

    def test_copy_clones_device_set(self) -> None:
        record = SemanticLabelRecord(
            label="Home", first_seen=1.0, last_seen=2.0, devices={"a", "b"}
        )

        snapshot = record.copy()
        snapshot.devices.add("c")

        assert record.devices == {"a", "b"}
        assert snapshot.devices == {"a", "b", "c"}

    def test_copy_preserves_scalar_fields(self) -> None:
        record = SemanticLabelRecord(label="Work", first_seen=10.0, last_seen=11.0)

        snapshot = record.copy()

        assert snapshot.label == "Work"
        assert snapshot.first_seen == 10.0
        assert snapshot.last_seen == 11.0


class TestDeviceIdentityDataclass:
    """``DeviceIdentity`` is a slotted dataclass with sensible defaults."""

    def test_required_fields_only(self) -> None:
        identity = DeviceIdentity(
            registry_id="reg-1",
            canonical_id="canon-1",
            identity_key=None,
        )

        assert identity.registry_id == "reg-1"
        assert identity.canonical_id == "canon-1"
        assert identity.identity_key is None
        assert identity.encrypted_identity_key is None
        assert identity.owner_key_version is None
        assert identity.device_type is None

    def test_optional_fields_round_trip(self) -> None:
        identity = DeviceIdentity(
            registry_id="reg-2",
            canonical_id="canon-2",
            identity_key=b"\x01",
            owner_key_version=7,
            device_type=3,
            manufacturer="Acme",
            model="ABC-123",
        )

        assert identity.identity_key == b"\x01"
        assert identity.owner_key_version == 7
        assert identity.manufacturer == "Acme"
        assert identity.model == "ABC-123"


# ---------------------------------------------------------------------------
# _get_api_class
# ---------------------------------------------------------------------------


class TestGetApiClass:
    """``_get_api_class`` honours module- and package-level monkeypatches."""

    def test_no_patch_returns_original(self) -> None:
        original = main_module._OriginalGoogleFindMyAPI

        assert _get_api_class() is original

    def test_module_level_patch_takes_priority(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sentinel = type("PatchedAPI", (), {})

        monkeypatch.setattr(main_module, "GoogleFindMyAPI", sentinel, raising=True)

        assert _get_api_class() is sentinel

    def test_package_level_patch_used_when_module_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        coordinator_pkg = __import__(
            "custom_components.googlefindmy.coordinator", fromlist=["GoogleFindMyAPI"]
        )
        sentinel = type("PackagePatchedAPI", (), {})

        monkeypatch.setattr(coordinator_pkg, "GoogleFindMyAPI", sentinel, raising=False)

        assert _get_api_class() is sentinel


# ---------------------------------------------------------------------------
# Coordinator static methods (no stub needed)
# ---------------------------------------------------------------------------


class TestSanitizeContributorMode:
    """``_sanitize_contributor_mode`` normalises optional strings."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("high_traffic", "high_traffic"),
            ("HIGH_TRAFFIC", "high_traffic"),
            ("  in_all_areas  ", "in_all_areas"),
            ("unsupported", "in_all_areas"),
            (None, "in_all_areas"),
            (123, "in_all_areas"),
        ],
    )
    def test_normalisation(self, raw: Any, expected: str) -> None:
        # DEFAULT_CONTRIBUTOR_MODE is "in_all_areas"; unsupported / non-string
        # inputs fall back to the default.
        assert GoogleFindMyCoordinator._sanitize_contributor_mode(raw) == expected


class TestCoerceFloat:
    """``_coerce_float`` rejects non-numeric and non-finite values."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (1, 1.0),
            (1.5, 1.5),
            ("2.5", 2.5),
            (None, None),
            ("not a number", None),
        ],
    )
    def test_round_trip(self, raw: Any, expected: float | None) -> None:
        assert GoogleFindMyCoordinator._coerce_float(raw) == expected


class TestNormalizeIdentityKey:
    """``_normalize_identity_key`` accepts bytes, bytearray and hex strings."""

    def test_bytes_pass_through(self) -> None:
        assert (
            GoogleFindMyCoordinator._normalize_identity_key(b"\xaa\xbb") == b"\xaa\xbb"
        )

    def test_bytearray_converts_to_bytes(self) -> None:
        result = GoogleFindMyCoordinator._normalize_identity_key(bytearray(b"\xcc"))

        assert isinstance(result, bytes)
        assert result == b"\xcc"

    def test_hex_string_decodes(self) -> None:
        assert GoogleFindMyCoordinator._normalize_identity_key("aabb") == b"\xaa\xbb"

    def test_invalid_hex_returns_none(self) -> None:
        assert GoogleFindMyCoordinator._normalize_identity_key("not hex") is None

    def test_non_string_non_bytes_returns_none(self) -> None:
        assert GoogleFindMyCoordinator._normalize_identity_key(123) is None
        assert GoogleFindMyCoordinator._normalize_identity_key(None) is None


class TestNormalizeIdentityKeyCandidates:
    """``_normalize_identity_key_candidates`` flattens scalar or iterable input."""

    def test_none_returns_empty(self) -> None:
        assert GoogleFindMyCoordinator._normalize_identity_key_candidates(None) == []

    def test_single_bytes_wrapped(self) -> None:
        result = GoogleFindMyCoordinator._normalize_identity_key_candidates(b"\x01")

        assert result == [b"\x01"]

    def test_iterable_deduplicates(self) -> None:
        result = GoogleFindMyCoordinator._normalize_identity_key_candidates(
            [b"\x01", b"\x01", b"\x02"]
        )

        assert result == [b"\x01", b"\x02"]

    def test_iterable_with_invalid_entries_skipped(self) -> None:
        result = GoogleFindMyCoordinator._normalize_identity_key_candidates(
            [b"\x01", "not hex", 123, b"\x02"]
        )

        assert result == [b"\x01", b"\x02"]

    def test_unsupported_input_returns_empty(self) -> None:
        assert GoogleFindMyCoordinator._normalize_identity_key_candidates(42) == []


class TestNormalizeOptionalString:
    """``_normalize_optional_string`` strips and returns None for empty input."""

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            (" hello ", "hello"),
            ("", None),
            ("   ", None),
            (None, None),
            (123, None),
        ],
    )
    def test_round_trip(self, raw: Any, expected: str | None) -> None:
        assert GoogleFindMyCoordinator._normalize_optional_string(raw) == expected


class TestNormalizeEncryptedBlob:
    """``_normalize_encrypted_blob`` mirrors ``_normalize_identity_key`` semantics."""

    def test_bytes_pass_through(self) -> None:
        assert GoogleFindMyCoordinator._normalize_encrypted_blob(b"\xaa") == b"\xaa"

    def test_hex_string_decodes(self) -> None:
        assert GoogleFindMyCoordinator._normalize_encrypted_blob("aa") == b"\xaa"

    def test_invalid_hex_returns_none(self) -> None:
        assert GoogleFindMyCoordinator._normalize_encrypted_blob("not hex") is None

    def test_non_string_non_bytes_returns_none(self) -> None:
        assert GoogleFindMyCoordinator._normalize_encrypted_blob(123) is None


# ---------------------------------------------------------------------------
# Stub-based tests (instance methods reading attribute state)
# ---------------------------------------------------------------------------


@pytest.fixture
def coord() -> MainCoordinatorStub:
    """Return a default :class:`MainCoordinatorStub` bound to a config entry."""

    entry = make_config_entry(entry_id="entry-abc")
    return MainCoordinatorStub(config_entry=entry)


class TestCacheProperty:
    """``cache`` is a read-only property returning the bound cache."""

    def test_returns_seeded_cache(self, coord: MainCoordinatorStub) -> None:
        assert coord.cache is coord._cache


class TestRecentReconfigure:
    """``mark_recent_reconfigure`` and ``recent_reconfigure_at`` are paired."""

    def test_initially_none(self, coord: MainCoordinatorStub) -> None:
        assert coord.recent_reconfigure_at is None

    def test_mark_uses_supplied_timestamp(self, coord: MainCoordinatorStub) -> None:
        coord.mark_recent_reconfigure(when=42.0)

        assert coord.recent_reconfigure_at == 42.0

    def test_mark_uses_time_time_when_omitted(
        self, coord: MainCoordinatorStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(main_module.time, "time", lambda: 99.0)

        coord.mark_recent_reconfigure()

        assert coord.recent_reconfigure_at == 99.0


class TestSafeUpdateMetric:
    """``safe_update_metric`` coerces floats and swallows errors."""

    def test_writes_metric(self, coord: MainCoordinatorStub) -> None:
        coord.safe_update_metric("latency_ms", 12.5)

        assert coord.performance_metrics["latency_ms"] == 12.5

    def test_string_numeric_coerced(self, coord: MainCoordinatorStub) -> None:
        coord.safe_update_metric("count", "3")  # type: ignore[arg-type]

        assert coord.performance_metrics["count"] == 3.0

    def test_invalid_value_swallowed(self, coord: MainCoordinatorStub) -> None:
        coord.safe_update_metric("bad", "not a number")  # type: ignore[arg-type]

        assert "bad" not in coord.performance_metrics


class TestGetIgnoredSet:
    """``_get_ignored_set`` reads from entry options (preferred) or attribute."""

    def test_empty_when_entry_options_missing(self, coord: MainCoordinatorStub) -> None:
        coord.config_entry = make_config_entry(entry_id="e", options={})

        assert coord._get_ignored_set() == set()

    def test_reads_mapping_from_options(self) -> None:
        entry = make_config_entry(
            entry_id="e",
            options={"ignored_devices": {"dev-1": {}, "dev-2": {}}},
        )
        coord = MainCoordinatorStub(config_entry=entry)

        assert coord._get_ignored_set() == {"dev-1", "dev-2"}

    def test_falls_back_to_attribute(self, coord: MainCoordinatorStub) -> None:
        coord.config_entry = None
        coord.ignored_devices = ["a", "b", 3]  # type: ignore[list-item]

        assert coord._get_ignored_set() == {"a", "b"}

    def test_attribute_not_list_returns_empty(self, coord: MainCoordinatorStub) -> None:
        coord.config_entry = None
        coord.ignored_devices = "not a list"  # type: ignore[assignment]

        assert coord._get_ignored_set() == set()


class TestIsIgnored:
    """``is_ignored`` delegates to ``_get_ignored_set``."""

    def test_true_when_in_set(self, coord: MainCoordinatorStub) -> None:
        coord._get_ignored_set = MagicMock(return_value={"dev-1"})  # type: ignore[method-assign]

        assert coord.is_ignored("dev-1") is True

    def test_false_when_not_in_set(self, coord: MainCoordinatorStub) -> None:
        coord._get_ignored_set = MagicMock(return_value={"dev-1"})  # type: ignore[method-assign]

        assert coord.is_ignored("dev-2") is False


class TestFindSemanticMatch:
    """``_find_semantic_match`` is case- and 'Near '-prefix-insensitive."""

    def test_exact_match(self, coord: MainCoordinatorStub) -> None:
        mapping = {"Home": {"latitude": 1.0}}

        result = coord._find_semantic_match("home", mapping)

        assert result == {"latitude": 1.0}

    def test_strips_near_prefix(self, coord: MainCoordinatorStub) -> None:
        mapping = {"Home": {"latitude": 1.0}}

        result = coord._find_semantic_match("Near Home", mapping)

        assert result == {"latitude": 1.0}

    def test_returns_none_when_missing(self, coord: MainCoordinatorStub) -> None:
        result = coord._find_semantic_match("Unknown", {"Home": {"latitude": 1.0}})

        assert result is None


class TestApplySemanticMapping:
    """``_apply_semantic_mapping`` rewrites payload coordinates when matched."""

    def test_returns_false_when_no_semantic_name(
        self, coord: MainCoordinatorStub
    ) -> None:
        assert coord._apply_semantic_mapping({}) is False

    def test_returns_false_for_blank_string(self, coord: MainCoordinatorStub) -> None:
        assert coord._apply_semantic_mapping({"semantic_name": "   "}) is False

    def test_returns_false_for_replayed_payload(
        self, coord: MainCoordinatorStub
    ) -> None:
        payload = {"semantic_name": "Home", "is_replayed": True}

        assert coord._apply_semantic_mapping(payload) is False

    def test_returns_false_without_entry(self) -> None:
        coord = MainCoordinatorStub(config_entry=None)

        assert coord._apply_semantic_mapping({"semantic_name": "Home"}) is False

    def test_returns_false_when_no_mappings_configured(
        self, coord: MainCoordinatorStub
    ) -> None:
        # Default entry has empty options + empty data → no mappings
        assert coord._apply_semantic_mapping({"semantic_name": "Home"}) is False

    def test_rewrites_payload_on_match(self) -> None:
        entry = make_config_entry(
            entry_id="e",
            options={
                "semantic_locations": {
                    "Home": {"latitude": 50.0, "longitude": 10.0, "accuracy": 25.0}
                }
            },
        )
        coord = MainCoordinatorStub(config_entry=entry)
        payload: dict[str, Any] = {"semantic_name": "Home"}

        result = coord._apply_semantic_mapping(payload)

        assert result is True
        assert payload["latitude"] == 50.0
        assert payload["longitude"] == 10.0
        assert payload["accuracy"] == 25.0
        assert payload["location_type"] == "trusted"

    def test_skips_mapping_with_invalid_coords(self) -> None:
        entry = make_config_entry(
            entry_id="e",
            options={
                "semantic_locations": {"Home": {"latitude": "bad", "longitude": 1.0}}
            },
        )
        coord = MainCoordinatorStub(config_entry=entry)

        assert coord._apply_semantic_mapping({"semantic_name": "Home"}) is False

    def test_an_emptied_options_mapping_does_not_revive_the_data_copy(self) -> None:
        """Deleting the last semantic location must actually stop the rewrite.

        The options flow writes ``semantic_locations`` on every add, edit and
        delete, so ``{}`` is a deliberate "no mappings". Reading the key by
        truthiness sent that case on to ``entry.data``, which the soft
        migration copies but never clears -- the deleted mapping kept
        relabelling coordinates as ``trusted``, across restarts.
        """
        entry = make_config_entry(
            entry_id="e",
            options={"semantic_locations": {}},
            data={
                "semantic_locations": {
                    "Home": {"latitude": 1.0, "longitude": 2.0, "accuracy": 50.0}
                }
            },
        )
        coord = MainCoordinatorStub(config_entry=entry)
        payload: dict[str, Any] = {"semantic_name": "Home"}

        assert coord._apply_semantic_mapping(payload) is False
        assert "latitude" not in payload
        assert "location_type" not in payload

    def test_an_absent_options_key_still_reads_the_data_copy(self) -> None:
        """The migration path stays intact: absence is not emptiness.

        Guards the other side of the presence check above. An entry that has
        not been through ``_async_soft_migrate_data_to_options`` yet carries
        the mapping only in ``data``, and must still get its rewrite.
        """
        entry = make_config_entry(
            entry_id="e",
            options={},
            data={
                "semantic_locations": {
                    "Home": {"latitude": 1.0, "longitude": 2.0, "accuracy": 50.0}
                }
            },
        )
        coord = MainCoordinatorStub(config_entry=entry)
        payload: dict[str, Any] = {"semantic_name": "Home"}

        assert coord._apply_semantic_mapping(payload) is True
        assert payload["latitude"] == 1.0
        assert payload["location_type"] == "trusted"


class TestSemanticLabelTracking:
    """``_record_semantic_label`` + ``get_observed_semantic_labels``."""

    def test_record_ignores_non_string(self, coord: MainCoordinatorStub) -> None:
        coord._record_semantic_label({"semantic_name": 123})

        assert coord.get_observed_semantic_labels() == []

    def test_record_ignores_empty_string(self, coord: MainCoordinatorStub) -> None:
        coord._record_semantic_label({"semantic_name": "   "})

        assert coord.get_observed_semantic_labels() == []

    def test_record_creates_entry(
        self, coord: MainCoordinatorStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(main_module.time, "time", lambda: 100.0)

        coord._record_semantic_label({"semantic_name": "Home"}, device_id="dev-1")

        snapshot = coord.get_observed_semantic_labels()
        assert len(snapshot) == 1
        assert snapshot[0].label == "Home"
        assert snapshot[0].first_seen == 100.0
        assert snapshot[0].devices == {"dev-1"}

    def test_record_updates_existing(
        self, coord: MainCoordinatorStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        ticks = iter([100.0, 200.0])
        monkeypatch.setattr(main_module.time, "time", lambda: next(ticks))

        coord._record_semantic_label({"semantic_name": "Home"}, device_id="dev-1")
        coord._record_semantic_label({"semantic_name": "Home"}, device_id="dev-2")

        snapshot = coord.get_observed_semantic_labels()
        assert len(snapshot) == 1
        assert snapshot[0].first_seen == 100.0
        assert snapshot[0].last_seen == 200.0
        assert snapshot[0].devices == {"dev-1", "dev-2"}

    def test_snapshot_is_independent_of_cache(
        self, coord: MainCoordinatorStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(main_module.time, "time", lambda: 1.0)

        coord._record_semantic_label({"semantic_name": "Home"}, device_id="dev-1")
        snapshot = coord.get_observed_semantic_labels()
        snapshot[0].devices.add("dev-X")

        # Cache untouched
        live = coord.get_observed_semantic_labels()
        assert live[0].devices == {"dev-1"}

    def test_empty_cache_returns_empty_list(self, coord: MainCoordinatorStub) -> None:
        assert coord.get_observed_semantic_labels() == []


class TestShouldPreservePreciseHomeCoordinates:
    """``_should_preserve_precise_home_coordinates`` guards against semantic drift."""

    def test_no_prev_location_returns_false(self, coord: MainCoordinatorStub) -> None:
        result = coord._should_preserve_precise_home_coordinates(None, {})

        assert result is False

    def test_invalid_prev_location_returns_false(
        self, coord: MainCoordinatorStub
    ) -> None:
        prev = {"latitude": "bad", "longitude": 0.0, "accuracy": 1.0}
        proposed = {"latitude": 0.0, "longitude": 0.0, "radius": 100.0}

        assert coord._should_preserve_precise_home_coordinates(prev, proposed) is False

    def test_non_finite_values_return_false(self, coord: MainCoordinatorStub) -> None:
        prev = {"latitude": math.nan, "longitude": 0.0, "accuracy": 1.0}
        proposed = {"latitude": 0.0, "longitude": 0.0, "radius": 100.0}

        assert coord._should_preserve_precise_home_coordinates(prev, proposed) is False

    def test_prev_accuracy_worse_than_radius_returns_false(
        self, coord: MainCoordinatorStub
    ) -> None:
        prev = {"latitude": 0.0, "longitude": 0.0, "accuracy": 200.0}
        proposed = {"latitude": 0.0, "longitude": 0.0, "radius": 100.0}

        assert coord._should_preserve_precise_home_coordinates(prev, proposed) is False

    def test_zero_radius_returns_false(self, coord: MainCoordinatorStub) -> None:
        prev = {"latitude": 0.0, "longitude": 0.0, "accuracy": 1.0}
        proposed = {"latitude": 0.0, "longitude": 0.0, "radius": 0.0}

        assert coord._should_preserve_precise_home_coordinates(prev, proposed) is False

    def test_within_radius_returns_true(self, coord: MainCoordinatorStub) -> None:
        coord._haversine_distance = MagicMock(return_value=50.0)  # type: ignore[method-assign]
        prev = {"latitude": 0.0, "longitude": 0.0, "accuracy": 1.0}
        proposed = {"latitude": 0.0, "longitude": 0.0, "radius": 100.0}

        assert coord._should_preserve_precise_home_coordinates(prev, proposed) is True

    def test_outside_radius_returns_false(self, coord: MainCoordinatorStub) -> None:
        coord._haversine_distance = MagicMock(return_value=500.0)  # type: ignore[method-assign]
        prev = {"latitude": 0.0, "longitude": 0.0, "accuracy": 1.0}
        proposed = {"latitude": 0.0, "longitude": 0.0, "radius": 100.0}

        assert coord._should_preserve_precise_home_coordinates(prev, proposed) is False


class TestAuthErrorActive:
    """``auth_error_active`` reads the private auth-failure flag."""

    def test_initially_false(self, coord: MainCoordinatorStub) -> None:
        assert coord.auth_error_active is False

    def test_reflects_private_flag(self, coord: MainCoordinatorStub) -> None:
        coord._auth_error_active = True

        assert coord.auth_error_active is True
