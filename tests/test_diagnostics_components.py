# tests/test_diagnostics_components.py
"""Hashed flags of component series: right mask, diagnostics only, no sensor."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy import diagnostics
from custom_components.googlefindmy.const import DATA_EID_RESOLVER, DOMAIN
from custom_components.googlefindmy.eid_resolver import (
    FMDN_FRAME_TYPE,
    MODERN_FRAME_TYPE,
    EIDMatch,
    GoogleFindMyEIDResolver,
    ObservationClock,
    UwtSource,
)
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    MEASURED_COMPONENT_ROWS,
    EidVariant,
    compute_flags_xor_mask,
    derive_component_key,
    generate_eid_variant,
)
from tests.test_eid_resolver_components import (
    COUNTER,
    EIK,
    LEGACY,
    NOW,
    _eid,
    _identity,
    _resolve,
    _resolver_with,
)

# Mask of series 2 at COUNTER, from the separate implementation.
MASK_SERIES_2 = 102
_HEADER = b"\xaa\xfe"


def _framed(component: int, decoded: int, frame: int = FMDN_FRAME_TYPE) -> bytes:
    mask = compute_flags_xor_mask(derive_component_key(EIK, component), COUNTER)
    return bytes([frame]) + _eid(component) + bytes([decoded ^ mask])


def _entry(resolver: GoogleFindMyEIDResolver) -> dict[int, dict[str, Any]]:
    return resolver.component_flags_snapshot("entry").get("can-reg-1", {})


def test_known_mask_of_a_series() -> None:
    assert (
        compute_flags_xor_mask(derive_component_key(EIK, 2), COUNTER) == MASK_SERIES_2
    )
    resolver = _resolver_with([_identity()])
    assert resolver._lookup_metadata[_eid(2)]["flags_xor_mask"] == MASK_SERIES_2


@pytest.mark.parametrize("component", MEASURED_COMPONENT_ROWS)
def test_flags_decode_with_the_mask_of_the_series(component: int) -> None:
    resolver = _resolver_with([_identity()])
    assert _resolve(resolver, _framed(component, 0x02))
    entry = _entry(resolver)[component]
    assert entry["decoded"] == 0x02
    assert entry["frame"] == FMDN_FRAME_TYPE
    assert entry["observations"] == 1
    assert entry["observed_at_wall"] == pytest.approx(NOW, abs=2)


def test_series_never_fill_the_sensors() -> None:
    resolver = _resolver_with([_identity()])
    for component in MEASURED_COMPONENT_ROWS:
        assert _resolve(resolver, _framed(component, 0x02))
    assert resolver.get_ble_battery_state("can-reg-1") is None
    assert not resolver._uwt_track
    assert not resolver._uwt_events


def test_single_component_device_still_fills_the_sensors() -> None:
    resolver = _resolver_with([_identity(trackable=None)])
    eid = generate_eid_variant(EIK, COUNTER, LEGACY)
    payload = (
        bytes([FMDN_FRAME_TYPE]) + eid + bytes([compute_flags_xor_mask(EIK, COUNTER)])
    )
    assert _resolve(resolver, payload)
    state = resolver.get_ble_battery_state("can-reg-1")
    assert state is not None
    assert state.decoded_flags == 0
    assert resolver.component_flags_snapshot("entry") == {}


def test_eik_series_of_a_multi_component_device_stays_on_the_old_path() -> None:
    resolver = _resolver_with([_identity()])
    eid = generate_eid_variant(EIK, COUNTER, LEGACY)
    payload = (
        bytes([FMDN_FRAME_TYPE]) + eid + bytes([compute_flags_xor_mask(EIK, COUNTER)])
    )
    assert _resolve(resolver, payload)
    assert resolver.get_ble_battery_state("can-reg-1") is not None


@pytest.mark.parametrize(
    ("frame", "decoded", "mode"),
    [
        (MODERN_FRAME_TYPE, 0x00, True),
        (MODERN_FRAME_TYPE, 0x01, True),
        (FMDN_FRAME_TYPE, 0x00, False),
        (FMDN_FRAME_TYPE, 0x01, False),
    ],
)
def test_mode_of_a_series_comes_from_the_frame_type(
    frame: int, decoded: int, mode: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """The meaning of the flags of a series is unknown, the frame type is clear."""
    resolver = _resolver_with([_identity()])
    with caplog.at_level("INFO"):
        _resolve(resolver, _framed(2, decoded, frame))
    sighting = resolver.last_confirmed_sighting("reg-1")
    assert (sighting.uwt_mode, sighting.uwt_source) == (mode, UwtSource.FRAME_TYPE)
    assert "CONFLICT" not in caplog.text
    assert _entry(resolver)[2]["decoded"] == decoded


def test_guessed_flags_byte_of_a_series_is_not_recorded() -> None:
    resolver = _resolver_with([_identity()])
    match = EIDMatch("reg-1", "entry", "can-reg-1", 0, False, 2)
    metadata = {"component": 2, "flags_xor_mask": 0}
    reading = resolver._update_ble_battery(
        b"\x40" + b"\x01" * 20 + b"\x02", 0x40, metadata, [match]
    )
    assert reading.origin == "guessed"
    assert resolver.component_flags_snapshot("entry") == {}


def test_reading_without_a_canonical_id_uses_the_device_id() -> None:
    resolver = _resolver_with([_identity()])
    match = EIDMatch("reg-1", "entry", "", 0, False, 2)
    resolver._record_component_flags(
        [match],
        2,
        decoded=2,
        frame=0x40,
        observation=ObservationClock(wall=1.0, monotonic=1.0),
    )
    assert resolver.component_flags_snapshot("entry")["reg-1"][2]["decoded"] == 2


def test_bare_eid_has_no_flags_and_no_mode() -> None:
    resolver = _resolver_with([_identity()])
    assert _resolve(resolver, _eid(2))
    assert 2 not in _entry(resolver)
    assert resolver.last_confirmed_sighting("reg-1").uwt_mode is None


def test_older_observation_does_not_overwrite_the_reading() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _framed(2, 0x02), at=NOW + 10)
    _resolve(resolver, _framed(2, 0x04), at=NOW + 5)
    assert _entry(resolver)[2]["decoded"] == 0x02
    assert _entry(resolver)[2]["observations"] == 2


def test_invalid_component_does_not_reach_the_diagnostics_or_the_sensor() -> None:
    resolver = _resolver_with([_identity()])
    match = EIDMatch("reg-1", "entry", "can-reg-1", 0, False)
    metadata = {"component": True, "flags_xor_mask": 0}
    resolver._update_ble_battery(
        b"\x40" + b"\x01" * 20 + b"\x02", 0x40, metadata, [match]
    )
    assert resolver.component_flags_snapshot("entry") == {}
    assert resolver.get_ble_battery_state("can-reg-1") is None


def test_recording_ignores_a_component_that_is_no_int() -> None:
    resolver = _resolver_with([_identity()])
    match = EIDMatch("reg-1", "entry", "can-reg-1", 0, False)
    resolver._record_component_flags(
        [match],
        True,
        decoded=2,
        frame=0x40,
        observation=ObservationClock(wall=1.0, monotonic=1.0),
    )
    assert resolver.component_flags_snapshot("entry") == {}


def test_bare_series_does_not_use_up_the_probe_of_the_device(
    caplog: pytest.LogCaptureFixture,
) -> None:
    resolver = _resolver_with([_identity()])
    with caplog.at_level("INFO"):
        _resolve(resolver, _eid(2))
        assert "FMDN_FLAGS_PROBE" not in caplog.text
        eik_eid = generate_eid_variant(EIK, COUNTER, LEGACY)
        _resolve(resolver, eik_eid, at=NOW + 1)
    assert "FMDN_FLAGS_PROBE" in caplog.text


def test_purge_keeps_a_reading_keyed_by_the_registry_id() -> None:
    resolver = _resolver_with([_identity()])
    resolver._record_component_flags(
        [EIDMatch("reg-1", "entry", "", 0, False, 2)],
        2,
        decoded=2,
        frame=0x40,
        observation=ObservationClock(wall=1.0, monotonic=1.0),
    )
    resolver.hass = SimpleNamespace(data={DOMAIN: {"entries": {"entry": object()}}})
    resolver._cached_identities = [_identity()]
    resolver._purge_uwt_state()
    assert resolver.component_flags_snapshot("entry")["reg-1"][2]["decoded"] == 2


def test_snapshot_is_limited_to_the_entry_and_a_copy() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _framed(2, 0x02))
    assert resolver.component_flags_snapshot("other") == {}
    resolver.component_flags_snapshot("entry")["can-reg-1"][2]["decoded"] = 99
    assert _entry(resolver)[2]["decoded"] == 0x02


def test_purge_drops_flags_of_a_removed_device() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _framed(2, 0x02))
    resolver.hass = SimpleNamespace(data={DOMAIN: {"entries": {"entry": object()}}})
    resolver._cached_identities = [_identity()]
    resolver._purge_uwt_state()
    assert _entry(resolver)
    resolver._cached_identities = []
    resolver._purge_uwt_state()
    assert not _entry(resolver)


def test_stop_clears_the_readings() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _framed(2, 0x02))
    resolver._unsub_alignment = None
    resolver._unsub_interval = None
    resolver._load_task = None
    resolver.stop()
    assert resolver.component_flags_snapshot("entry") == {}


def test_no_mask_or_key_in_the_log(caplog: pytest.LogCaptureFixture) -> None:
    resolver = _resolver_with([_identity()])
    with caplog.at_level("DEBUG"):
        _resolve(resolver, _framed(2, 0x02))
    assert "xor_mask" not in caplog.text
    assert derive_component_key(EIK, 2).hex() not in caplog.text


def _hass_with(resolver: object) -> SimpleNamespace:
    return SimpleNamespace(data={DOMAIN: {DATA_EID_RESOLVER: resolver}})


def test_diagnostics_block_uses_positions_and_raw_values() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _framed(2, 0x02))
    _resolve(resolver, _framed(254, 0x06), at=NOW + 1)
    block = diagnostics._component_flags_block(_hass_with(resolver), "entry")
    text = json.dumps(block)
    assert "can-reg-1" not in text
    assert "reg-1" not in text
    assert derive_component_key(EIK, 2).hex() not in text
    assert [
        (row["device_index"], row["component"], row["decoded"]) for row in block
    ] == [
        (0, 2, 2),
        (0, 254, 6),
    ]


@pytest.mark.parametrize("resolver", [None, object()])
def test_diagnostics_block_without_a_resolver_is_empty(resolver: object) -> None:
    assert diagnostics._component_flags_block(_hass_with(resolver), "entry") == []


def test_diagnostics_block_survives_a_failing_reader() -> None:
    def boom(_entry_id: str) -> None:
        raise RuntimeError("secret detail")

    resolver = SimpleNamespace(component_flags_snapshot=boom)
    assert diagnostics._component_flags_block(_hass_with(resolver), "entry") == []


def test_variant_of_the_series_is_legacy() -> None:
    resolver = _resolver_with([_identity()])
    assert resolver._lookup_metadata[_eid(2)]["variant"] == (
        EidVariant.LEGACY_SECP160R1_X20_BE.value
    )
