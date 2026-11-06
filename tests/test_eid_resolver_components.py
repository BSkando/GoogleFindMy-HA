# tests/test_eid_resolver_components.py
"""EID series of devices with several FMDN components in the resolver.

Single-component devices must stay byte-identical; the digest constants come
from the base commit (see ``_golden``), computed by the same code.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any
from unittest.mock import patch

import pytest

from custom_components.googlefindmy import eid_resolver as resolver_module
from custom_components.googlefindmy.coordinator import DeviceIdentity
from custom_components.googlefindmy.eid_resolver import (
    EIDGenerationLock,
    EIDMatch,
    GoogleFindMyEIDResolver,
    VariantSpec,
    WindowCandidate,
    identity_key_fingerprint,
)
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    MEASURED_COMPONENT_ROWS,
    EidVariant,
    derive_component_key,
    generate_eid_variant,
)

EIK = bytes(range(32))
NOW = 1_780_000_000
COUNTER = 150528  # Vielfaches von 1024, siehe tests/test_eid_generator_components.py
PAIR_DATE = NOW - COUNTER
LEGACY = EidVariant.LEGACY_SECP160R1_X20_BE

# Known answers from the separate implementation, same counter and key.
KNOWN_EIDS = {
    0: "006d60601e1bf24827e5a72a89a837866d2a2de1",
    2: "841b27e816bc4edbf23ea6db754d8a3b61bebe32",
    255: "79b8497c94159e0121d470fc6df88911de63ac77",
}

# Measured on commit d2ea140c, /mnt/nas backup golden_erzeugen.py.
GOLDEN_NO_LOCK = "7cbf36f0a862017a1e4c532f357aabbc6e37d8f18094c13399b8305fbe48e7ec"
GOLDEN_LOCKED = "5460395d2f234f1c8d7259acc791fe19f0b25f45fc27b678c1498e40bf0beb7b"


def _new_resolver() -> GoogleFindMyEIDResolver:
    resolver = GoogleFindMyEIDResolver.__new__(GoogleFindMyEIDResolver)
    resolver._ensure_cache_defaults()
    return resolver


def _identity(
    *,
    trackable: int | None = 3,
    key: bytes = EIK,
    registry_id: str = "reg-1",
    pair_date: int = PAIR_DATE,
) -> DeviceIdentity:
    return DeviceIdentity(
        registry_id=registry_id,
        canonical_id=f"can-{registry_id}",
        identity_key=key,
        config_entry_id="entry",
        pair_date=pair_date,
        trackable_components=trackable,
    )


def _build_into(
    resolver: GoogleFindMyEIDResolver, identities: list[DeviceIdentity]
) -> None:
    items = resolver._collect_work_items(identities, now_unix=NOW)
    lookup, metadata, _invalid = resolver._build_lookup_sync(
        items, NOW, resolver._build_rotation_params()
    )
    resolver._lookup = lookup
    resolver._lookup_metadata = metadata


def _resolver_with(identities: list[DeviceIdentity]) -> GoogleFindMyEIDResolver:
    resolver = _new_resolver()
    _build_into(resolver, identities)
    return resolver


def _eid(component: int, counter: int = COUNTER, key: bytes = EIK) -> bytes:
    return generate_eid_variant(derive_component_key(key, component), counter, LEGACY)


def _resolve(
    resolver: GoogleFindMyEIDResolver, payload: bytes, *, at: int = NOW
) -> list[EIDMatch]:
    with (
        patch("time.time", return_value=float(at)),
        patch("time.monotonic", return_value=float(at - NOW + 50_000)),
    ):
        return resolver.resolve_eid_all(payload, observed_at=float(at - NOW + 50_000))


def _golden(trackable: dict[str, Any], *, locked: bool) -> tuple[str, int]:
    """Digest over lookup, metadata and signature; same code as on the base commit."""
    resolver = _new_resolver()
    identities = [
        DeviceIdentity(
            registry_id=f"reg-{i}",
            canonical_id=f"can-{i}",
            identity_key=bytes([i]) * 32,
            config_entry_id="entry",
            pair_date=1_750_000_000 + i * 1024,
            **trackable,
        )
        for i in (1, 2)
    ]
    if locked:
        age = NOW - 1_750_000_000
        resolver._locks["reg-1"] = EIDGenerationLock(
            device_id="reg-1",
            canonical_id="can-1",
            variant="legacy_secp160r1_x20_be",
            advertisement_reversed=False,
            eid_length=20,
            rotation_timestamp=age - age % 1024,
            time_basis="pair_date",
            created_at=NOW - 2048,
            drift_offset=0,
            last_seen_at=NOW - 1024,
        )
    items = resolver._collect_work_items(identities, now_unix=NOW)
    params = resolver._build_rotation_params()
    lookup, meta, invalid = resolver._build_lookup_sync(items, NOW, params)
    canon = sorted(
        (
            eid.hex(),
            sorted(repr(tuple(m)[:5]) for m in matches),
            sorted(
                (
                    k,
                    sorted(v)
                    if isinstance(v, set | frozenset)
                    else (v.hex() if isinstance(v, bytes) else v),
                )
                for k, v in meta[eid].items()
            ),
        )
        for eid, matches in lookup.items()
    )
    signature = resolver._build_signature(items, now_unix=NOW, params=params)
    digest = json.dumps([canon, invalid, signature], default=str).encode()
    assert all(
        match.component is None for matches in lookup.values() for match in matches
    )
    assert all("component" not in entry for entry in meta.values())
    return hashlib.sha256(digest).hexdigest(), len(lookup)


@pytest.mark.parametrize(
    "trackable", [{}, {"trackable_components": None}, {"trackable_components": 1}]
)
@pytest.mark.parametrize(
    ("locked", "expected", "size"),
    [(False, GOLDEN_NO_LOCK, 420), (True, GOLDEN_LOCKED, 240)],
)
def test_single_component_devices_stay_byte_identical(
    trackable: dict[str, Any], locked: bool, expected: str, size: int
) -> None:
    assert _golden(trackable, locked=locked) == (expected, size)


@pytest.mark.parametrize("component", MEASURED_COMPONENT_ROWS)
def test_every_series_resolves_to_the_same_device(component: int) -> None:
    resolver = _resolver_with([_identity()])
    matches = _resolve(resolver, _eid(component))
    assert len(matches) == 1
    assert matches[0].device_id == "reg-1"
    assert matches[0].canonical_id == "can-reg-1"
    assert matches[0].component == component
    assert matches[0].is_reversed is False


@pytest.mark.parametrize(("component", "eid"), sorted(KNOWN_EIDS.items()))
def test_lookup_contains_the_known_answers(component: int, eid: str) -> None:
    resolver = _resolver_with([_identity()])
    assert bytes.fromhex(eid) in resolver._lookup
    assert resolver._lookup_metadata[bytes.fromhex(eid)]["component"] == component


def test_series_are_not_reversed_and_use_only_the_legacy_variant() -> None:
    resolver = _resolver_with([_identity()])
    assert _eid(2)[::-1] not in resolver._lookup
    for entry in resolver._lookup_metadata.values():
        if "component" in entry:
            assert entry["variant"] == LEGACY.value


def test_eik_series_stays_for_a_multi_component_device() -> None:
    resolver = _resolver_with([_identity()])
    eik_eid = generate_eid_variant(EIK, COUNTER, LEGACY)
    assert resolver._lookup[eik_eid][0].component is None
    assert "component" not in resolver._lookup_metadata[eik_eid]


@pytest.mark.parametrize("trackable", [None, 0, 1])
def test_no_series_without_a_count_above_one(trackable: int | None) -> None:
    resolver = _resolver_with([_identity(trackable=trackable)])
    assert _eid(2) not in resolver._lookup


def test_no_series_for_a_key_of_another_length() -> None:
    resolver = _new_resolver()
    item = resolver._prepare_work_item(_identity(key=b"\x01" * 31), now_unix=NOW)
    assert item is not None
    assert item.component_keys == ()


def test_fingerprint_stays_with_the_eik() -> None:
    resolver = _resolver_with([_identity()])
    entry = resolver._lookup_metadata[_eid(254)]
    assert entry["key_fingerprint"] == identity_key_fingerprint(EIK)


def test_series_count_stays_bounded() -> None:
    resolver = _new_resolver()
    single = resolver._collect_work_items([_identity(trackable=1)], now_unix=NOW)
    multi = resolver._collect_work_items([_identity()], now_unix=NOW)
    windows, _ = resolver._compute_time_windows(
        multi[0], now_unix=NOW, params=resolver._build_rotation_params()
    )
    extra = sum(
        len(resolver._compute_variants(multi[0], window))
        - len(resolver._compute_variants(single[0], window))
        for window in windows
    )
    assert extra == len(MEASURED_COMPONENT_ROWS) * sum(
        len(window.windows) for window in windows
    )


def test_locked_device_keeps_the_series() -> None:
    resolver = _new_resolver()
    resolver._locks["reg-1"] = EIDGenerationLock(
        device_id="reg-1",
        canonical_id="can-reg-1",
        variant=LEGACY.value,
        advertisement_reversed=False,
        eid_length=20,
        rotation_timestamp=COUNTER,
        time_basis="pair_date",
        created_at=NOW - 2048,
        last_seen_at=NOW - 1024,
    )
    _build_into(resolver, [_identity()])
    assert resolver._lookup[_eid(1)][0].component == 1


def test_switching_series_keeps_lock_and_offsets() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _eid(2))
    lock = resolver._locks["reg-1"]
    state = (lock.drift_offset, lock.rotation_timestamp, dict(resolver._known_offsets))
    with patch.object(GoogleFindMyEIDResolver, "_schedule_lock_save") as save:
        _resolve(resolver, _eid(254), at=NOW + 5)
    assert resolver._locks["reg-1"] is lock
    assert (
        lock.drift_offset,
        lock.rotation_timestamp,
        dict(resolver._known_offsets),
    ) == state
    assert lock.variant == LEGACY.value
    assert save.call_count <= 1


def test_sighting_carries_the_component_of_the_received_series() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _eid(2))
    sighting = resolver.last_confirmed_sighting("reg-1")
    assert sighting is not None
    assert sighting.component == 2
    assert sighting.window_counter == COUNTER
    assert sighting.key_fingerprint == identity_key_fingerprint(EIK)
    _resolve(resolver, _eid(254), at=NOW + 5)
    assert resolver.last_confirmed_sighting("reg-1").component == 254


def test_series_with_an_older_counter_does_not_replace_within_a_period() -> None:
    """The clocks of the forms differ by 10 to 14 s around a rotation."""
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _eid(2))
    _resolve(resolver, _eid(254, COUNTER - 1024), at=NOW + 10)
    sighting = resolver.last_confirmed_sighting("reg-1")
    assert (sighting.component, sighting.window_counter) == (2, COUNTER)


@pytest.mark.parametrize("bad", [True, -1, 256, "2", 2.0, None])
def test_invalid_component_in_metadata_records_no_sighting(bad: object) -> None:
    resolver = _new_resolver()
    resolver._note_confirmed_sighting(
        "reg-1",
        {
            "rotation_timestamp": COUNTER,
            "variant": LEGACY.value,
            "component": bad,
            "key_fingerprint": identity_key_fingerprint(EIK),
        },
        observed_at=NOW,
    )
    assert resolver.last_confirmed_sighting("reg-1") is None


def test_single_component_sighting_has_no_component() -> None:
    resolver = _resolver_with([_identity(trackable=None)])
    _resolve(resolver, generate_eid_variant(EIK, COUNTER, LEGACY))
    assert resolver.last_confirmed_sighting("reg-1").component is None


def test_signature_changes_with_the_series_and_not_for_single_devices() -> None:
    resolver = _new_resolver()
    params = resolver._build_rotation_params()

    def signature(trackable: int | None) -> str:
        return resolver._build_signature(
            resolver._collect_work_items(
                [_identity(trackable=trackable)], now_unix=NOW
            ),
            now_unix=NOW,
            params=params,
        )

    assert signature(1) == signature(None)
    assert signature(3) != signature(1)


def test_signature_folds_which_series_are_built(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = _new_resolver()
    params = resolver._build_rotation_params()

    def signature() -> str:
        return resolver._build_signature(
            resolver._collect_work_items([_identity()], now_unix=NOW),
            now_unix=NOW,
            params=params,
        )

    before = signature()
    monkeypatch.setattr(resolver_module, "MEASURED_COMPONENT_ROWS", (0, 1, 2, 254, 253))
    assert signature() != before


def test_shared_device_entry_without_a_value_has_no_series() -> None:
    resolver = _resolver_with(
        [_identity(), _identity(trackable=None, registry_id="reg-2")]
    )
    assert [match.device_id for match in resolver._lookup[_eid(2)]] == ["reg-1"]
    eik_eid = generate_eid_variant(EIK, COUNTER, LEGACY)
    assert {match.device_id for match in resolver._lookup[eik_eid]} == {
        "reg-1",
        "reg-2",
    }


def test_several_candidate_keys_each_get_their_series() -> None:
    other = bytes(reversed(EIK))
    resolver = _resolver_with([_identity(), _identity(key=other)])
    assert resolver._lookup_metadata[_eid(2)][
        "key_fingerprint"
    ] == identity_key_fingerprint(EIK)
    assert resolver._lookup_metadata[_eid(2, key=other)][
        "key_fingerprint"
    ] == identity_key_fingerprint(other)


def test_heuristic_never_sees_a_component_key() -> None:
    resolver = _resolver_with([_identity()])
    resolver._cached_identities = [_identity()]
    seen: list[bytes] = []
    with patch.object(
        resolver_module,
        "generate_heuristic_eid",
        side_effect=lambda key, *_a, **_k: seen.append(key) or [],
    ):
        assert _resolve(resolver, b"\x77" * 20) == []
    assert seen
    assert set(seen) <= {EIK}
    for component in MEASURED_COMPONENT_ROWS:
        assert derive_component_key(EIK, component) not in seen


def test_no_key_material_in_repr_or_debug_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    resolver = _new_resolver()
    with caplog.at_level(logging.DEBUG):
        item = resolver._prepare_work_item(_identity(), now_unix=NOW)
        assert item is not None
        _build_into(resolver, [_identity()])
        _resolve(resolver, _eid(2))
    window = WindowCandidate(
        timestamp=COUNTER, semantic_offset=0, time_basis="pair_date", candidate_value=0
    )
    spec = VariantSpec(
        key_bytes=item.component_keys[0][1], variant=LEGACY, window=window
    )
    secrets = [EIK, *(derive_component_key(EIK, c) for c in MEASURED_COMPONENT_ROWS)]
    text = repr(item) + repr(spec) + caplog.text
    for secret in secrets:
        assert secret.hex() not in text
        assert repr(secret) not in text


def test_component_hit_is_logged_once_per_series(
    caplog: pytest.LogCaptureFixture,
) -> None:
    resolver = _resolver_with([_identity()])
    with caplog.at_level(logging.INFO):
        _resolve(resolver, _eid(2))
        _resolve(resolver, _eid(2), at=NOW + 1)
        _resolve(resolver, _eid(254), at=NOW + 2)
    lines = [
        r.getMessage() for r in caplog.records if "FMDN_COMPONENT_HIT" in r.getMessage()
    ]
    assert len(lines) == 2
    assert "device=reg-1 canonical=can-reg-1 component=2" in lines[0]
    assert "component=254" in lines[1]


def test_single_component_hit_is_not_logged(caplog: pytest.LogCaptureFixture) -> None:
    resolver = _resolver_with([_identity(trackable=None)])
    with caplog.at_level(logging.INFO):
        _resolve(resolver, generate_eid_variant(EIK, COUNTER, LEGACY))
    assert "FMDN_COMPONENT_HIT" not in caplog.text


def test_stop_forgets_the_logged_series() -> None:
    resolver = _resolver_with([_identity()])
    _resolve(resolver, _eid(2))
    assert resolver._component_hits
    resolver._unsub_alignment = None
    resolver._unsub_interval = None
    resolver._load_task = None
    resolver.stop()
    assert not resolver._component_hits
