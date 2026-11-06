# tests/test_decrypt_locations_components.py
"""Foreign reports on component EIDs decrypt without promoting the series key."""

from __future__ import annotations

import pytest

from custom_components.googlefindmy.FMDNCrypto import foreign_tracker_cryptor as cryptor
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    MEASURED_COMPONENT_ROWS,
    EidVariant,
    derive_component_key,
    generate_eid_variant,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker import (
    decrypt_locations,
)
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2
from tests.test_decrypt_locations import _add_report, _valid_location_bytes

EIK = bytes(range(32))
COUNTER = 5 * 1024
BASE_NOW = 1_700_200_000.0
LEGACY = EidVariant.LEGACY_SECP160R1_X20_BE


def _capabilities(trackable: int) -> bytes:
    return DeviceUpdate_pb2.DeviceCapabilities(
        trackableComponents=trackable
    ).SerializeToString()


def _update(eid: bytes, capabilities: bytes | None) -> DeviceUpdate_pb2.DeviceUpdate:
    update = DeviceUpdate_pb2.DeviceUpdate()
    registration = update.deviceMetadata.information.deviceRegistration
    registration.SetInParent()
    if capabilities is not None:
        registration.capabilities = capabilities
    encrypted, sx = cryptor.encrypt(_valid_location_bytes(), b"\x03" * 32, eid)
    _add_report(
        update,
        public_key_random=sx,
        encrypted_location=encrypted,
        is_own_report=False,
        base_now=BASE_NOW,
    )
    reports = update.deviceMetadata.information.locationInformation.reports
    reports.recentLocationAndNetworkLocations.networkLocations[
        -1
    ].geoLocation.deviceTimeOffset = COUNTER
    return update


@pytest.fixture(autouse=True)
def _environment(monkeypatch: pytest.MonkeyPatch) -> None:
    async def identity_keys(*_args: object, **_kwargs: object) -> list[bytes]:
        return [EIK]

    monkeypatch.setattr(decrypt_locations.time, "time", lambda: BASE_NOW)
    monkeypatch.setattr(decrypt_locations, "is_mcu_tracker", lambda *_a: False)
    monkeypatch.setattr(decrypt_locations, "async_retrieve_identity_key", identity_keys)


async def _records(update: DeviceUpdate_pb2.DeviceUpdate) -> list[dict[str, object]]:
    result = await decrypt_locations.async_decrypt_location_response_locations(
        update, cache=object()
    )
    return [r for r in result if decrypt_locations.is_real_location_record(r)]


@pytest.mark.asyncio
@pytest.mark.parametrize("component", MEASURED_COMPONENT_ROWS)
async def test_report_on_a_series_decrypts_and_the_eik_stays(component: int) -> None:
    key = derive_component_key(EIK, component)
    eid = generate_eid_variant(key, COUNTER, LEGACY)
    records = await _records(_update(eid, _capabilities(3)))
    assert len(records) == 1
    assert records[0]["identity_key"] == EIK
    assert records[0]["identity_key_candidates"] == [EIK]


@pytest.mark.asyncio
async def test_series_of_a_second_candidate_promotes_that_candidate(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    stale = b"\x51" * 32

    async def identity_keys(*_args: object, **_kwargs: object) -> list[bytes]:
        return [stale, EIK]

    monkeypatch.setattr(decrypt_locations, "async_retrieve_identity_key", identity_keys)
    eid = generate_eid_variant(derive_component_key(EIK, 254), COUNTER, LEGACY)
    with caplog.at_level("INFO"):
        records = await _records(_update(eid, _capabilities(3)))
    assert len(records) == 1
    assert records[0]["identity_key"] == EIK
    assert "alternate identity key candidate (index=1)" in caplog.text


def test_keys_of_another_length_get_no_series() -> None:
    registration = DeviceUpdate_pb2.DeviceRegistration(capabilities=_capabilities(3))
    assert decrypt_locations._component_keys(registration, [b"\x01" * 16]) == []
    pairs = decrypt_locations._component_keys(registration, [EIK])
    assert [identity for identity, _key in pairs] == [EIK] * 5
    assert [key for _identity, key in pairs] == [
        derive_component_key(EIK, c) for c in MEASURED_COMPONENT_ROWS
    ]


@pytest.mark.asyncio
async def test_series_key_does_not_reach_any_output(
    caplog: pytest.LogCaptureFixture,
) -> None:
    key = derive_component_key(EIK, 2)
    eid = generate_eid_variant(key, COUNTER, LEGACY)
    with caplog.at_level("DEBUG"):
        records = await _records(_update(eid, _capabilities(3)))
    assert key not in repr(records).encode()
    assert key.hex() not in repr(records)
    assert key.hex() not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("capabilities", [None, _capabilities(1), b"\xff\xfe"])
async def test_without_a_count_above_one_only_the_eik_is_tried(
    capabilities: bytes | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    tried: list[tuple[bytes, ...]] = []
    real = decrypt_locations._offload_decrypt_foreign

    async def spy(keys: tuple[bytes, ...], *args: object) -> object:
        tried.append(tuple(keys))
        return await real(keys, *args)

    monkeypatch.setattr(decrypt_locations, "_offload_decrypt_foreign", spy)
    eid = generate_eid_variant(EIK, COUNTER, LEGACY)
    assert len(await _records(_update(eid, capabilities))) == 1
    assert tried == [(EIK,)]


@pytest.mark.asyncio
async def test_report_on_an_unknown_series_still_fails_and_counts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A series outside the measured set behaves like any undecryptable report."""
    eid = generate_eid_variant(derive_component_key(EIK, 7), COUNTER, LEGACY)
    assert await _records(_update(eid, _capabilities(3))) == []


@pytest.mark.asyncio
async def test_own_reports_stay_on_their_path(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[bytes] = []

    async def own(identity_key: bytes, _encrypted: bytes) -> bytes:
        calls.append(identity_key)
        return _valid_location_bytes()

    monkeypatch.setattr(decrypt_locations, "_offload_decrypt_aes", own)
    update = DeviceUpdate_pb2.DeviceUpdate()
    update.deviceMetadata.information.deviceRegistration.capabilities = _capabilities(3)
    _add_report(
        update,
        public_key_random=b"",
        encrypted_location=b"own",
        is_own_report=True,
        base_now=BASE_NOW,
    )
    assert len(await _records(update)) == 1
    assert calls == [EIK]
