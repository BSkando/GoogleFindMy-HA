# tests/test_eid_resolver_mcu_static.py
"""Static counter-0 EID of DIY microcontroller trackers.

Trackers registered with GoogleFindMyTools (Fast Pair model ``003200``, see
``is_mcu_tracker``) run ESP32/Zephyr firmware that computes one EID at
counter 0 and advertises it unchanged. The resolver must precompute that EID
(and its hashed-flags mask) for these trackers only, so their advertisements
resolve and the FMDN battery level decodes.
"""

from __future__ import annotations

import hashlib
from unittest.mock import patch

from custom_components.googlefindmy import eid_resolver as resolver_module
from custom_components.googlefindmy.coordinator import DeviceIdentity
from custom_components.googlefindmy.eid_resolver import (
    MCU_STATIC_TIME_BASIS,
    EIDMatch,
    GoogleFindMyEIDResolver,
)
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    EidVariant,
    compute_flags_xor_mask,
    generate_eid_variant,
)
from custom_components.googlefindmy.SpotApi.CreateBleDevice.config import (
    mcu_fast_pair_model_id,
)

# Synthetic key, no relation to any real tracker.
EIK = hashlib.sha256(b"googlefindmy-test-mcu-static-eik").digest()
NOW = 1_780_000_000
PAIR_DATE = NOW - 30 * 24 * 60 * 60  # Registered a month ago.
LEGACY = EidVariant.LEGACY_SECP160R1_X20_BE
CANONICAL_ID = "can-mcu"

# FMDN hashed flags: battery level in bits 1-2, UWT mode in bit 0.
BATTERY_LOW = 2
DECODED_FLAGS = (BATTERY_LOW << 1) | 0x01


def _identity(*, fast_pair_model_id: str | None) -> DeviceIdentity:
    return DeviceIdentity(
        registry_id="reg-mcu",
        canonical_id=CANONICAL_ID,
        identity_key=EIK,
        config_entry_id="entry",
        pair_date=PAIR_DATE,
        device_type=1,
        fast_pair_model_id=fast_pair_model_id,
    )


def _new_resolver() -> GoogleFindMyEIDResolver:
    resolver = GoogleFindMyEIDResolver.__new__(GoogleFindMyEIDResolver)
    resolver._ensure_cache_defaults()
    return resolver


def _build(
    resolver: GoogleFindMyEIDResolver, identity: DeviceIdentity, *, now: int
) -> None:
    items = resolver._collect_work_items([identity], now_unix=now)
    lookup, metadata, _invalid = resolver._build_lookup_sync(
        items, now, resolver._build_rotation_params()
    )
    resolver._lookup = lookup
    resolver._lookup_metadata = metadata


def _resolve(
    resolver: GoogleFindMyEIDResolver, payload: bytes, *, at: int
) -> list[EIDMatch]:
    monotonic = float(at - NOW + 50_000)
    with (
        patch("time.time", return_value=float(at)),
        patch("time.monotonic", return_value=monotonic),
    ):
        return resolver.resolve_eid_all(payload, observed_at=monotonic)


def _static_eid() -> bytes:
    return generate_eid_variant(EIK, 0, LEGACY)


def _flags_mask() -> int:
    curve_len, curve_order, derivation = resolver_module._VARIANT_CURVE_PARAMS[LEGACY]
    return compute_flags_xor_mask(
        EIK,
        0,
        curve_byte_len=curve_len,
        curve_order=curve_order,
        derivation=derivation,
    )


def _advertisement() -> bytes:
    """FEAA service data as the firmware sends it: frame, EID, hashed flags."""
    return bytes([0x41]) + _static_eid() + bytes([DECODED_FLAGS ^ _flags_mask()])


def test_mcu_tracker_static_eid_is_in_lookup() -> None:
    resolver = _new_resolver()
    _build(resolver, _identity(fast_pair_model_id=mcu_fast_pair_model_id), now=NOW)

    metadata = resolver._lookup_metadata[_static_eid()]
    assert metadata["rotation_timestamp"] == 0
    assert metadata["timestamp_basis"] == MCU_STATIC_TIME_BASIS
    assert metadata["flags_xor_mask"] == _flags_mask()


def test_mcu_tracker_static_eid_resolves_and_decodes_battery() -> None:
    resolver = _new_resolver()
    _build(resolver, _identity(fast_pair_model_id=mcu_fast_pair_model_id), now=NOW)

    matches = _resolve(resolver, _advertisement(), at=NOW)

    assert [match.device_id for match in matches] == ["reg-mcu"]
    state = resolver.get_ble_battery_state(CANONICAL_ID)
    assert state is not None
    assert state.battery_level == BATTERY_LOW
    assert state.battery_pct == resolver_module.FMDN_BATTERY_PCT[BATTERY_LOW]
    assert state.uwt_mode is True
    assert state.decoded_flags == DECODED_FLAGS


def test_mcu_tracker_still_resolves_after_lock_moves_on() -> None:
    """The lock a counter-0 hit creates walks forward; the window must stay."""
    resolver = _new_resolver()
    identity = _identity(fast_pair_model_id=mcu_fast_pair_model_id)
    _build(resolver, identity, now=NOW)
    assert _resolve(resolver, _advertisement(), at=NOW)
    assert resolver._locks["reg-mcu"].rotation_timestamp == 0

    later = NOW + 6 * 60 * 60
    _build(resolver, identity, now=later)

    assert [m.device_id for m in _resolve(resolver, _advertisement(), at=later)] == [
        "reg-mcu"
    ]
    assert ("reg-mcu", MCU_STATIC_TIME_BASIS) not in resolver._known_offsets


def test_non_mcu_tracker_gets_no_static_eid() -> None:
    for fast_pair_model_id in (None, "123456"):
        resolver = _new_resolver()
        _build(resolver, _identity(fast_pair_model_id=fast_pair_model_id), now=NOW)

        assert _static_eid() not in resolver._lookup
        assert all(
            meta.get("timestamp_basis") != MCU_STATIC_TIME_BASIS
            for meta in resolver._lookup_metadata.values()
        )
        assert _resolve(resolver, _advertisement(), at=NOW) == []
