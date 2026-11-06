# tests/test_bermuda_listener_components.py
"""The finder reports exactly the EID of the component series it received."""

from __future__ import annotations

import logging
from dataclasses import replace
from unittest.mock import MagicMock, patch

import pytest

from custom_components.googlefindmy.const import DOMAIN
from custom_components.googlefindmy.eid_resolver import (
    ConfirmedSighting,
    identity_key_fingerprint,
)
from custom_components.googlefindmy.fmdn_finder import bermuda_listener
from custom_components.googlefindmy.FMDNCrypto import foreign_tracker_cryptor as cryptor
from custom_components.googlefindmy.FMDNCrypto.eid_generator import (
    MEASURED_COMPONENT_ROWS,
    EidVariant,
    derive_component_key,
    generate_eid_variant,
)
from custom_components.googlefindmy.FMDNCrypto.foreign_report_errors import (
    ForeignReportAuthError,
)

EIK = bytes(range(32))
COUNTER = 150528
NOW = 1_780_000_000
LEGACY = EidVariant.LEGACY_SECP160R1_X20_BE
KNOWN_EIDS = {
    0: "006d60601e1bf24827e5a72a89a837866d2a2de1",
    2: "841b27e816bc4edbf23ea6db754d8a3b61bebe32",
    255: "79b8497c94159e0121d470fc6df88911de63ac77",
}


def _sighting(**changes: object) -> ConfirmedSighting:
    sighting = ConfirmedSighting(
        variant=LEGACY,
        window_counter=COUNTER,
        observed_at=NOW - 30,
        uwt_mode=False,
        key_fingerprint=identity_key_fingerprint(EIK),
        component=2,
    )
    return replace(sighting, **changes)  # type: ignore[arg-type]


async def _upload(sighting: ConfirmedSighting | None) -> object:
    resolver = MagicMock()
    resolver.last_confirmed_sighting.side_effect = lambda _registry_id: sighting
    hass = MagicMock()
    hass.data = {DOMAIN: {"eid_resolver": resolver}}
    coordinator = MagicMock()
    coordinator.get_active_device_identities.return_value = [
        MagicMock(canonical_id="entry:dev-1", identity_key=EIK, registry_id="reg-1")
    ]
    with patch.object(bermuda_listener.time, "time", return_value=NOW):
        return await bermuda_listener._async_get_device_eid(hass, coordinator, "dev-1")


@pytest.mark.parametrize(("component", "eid"), sorted(KNOWN_EIDS.items()))
async def test_report_carries_the_eid_of_the_series(component: int, eid: str) -> None:
    upload = await _upload(_sighting(component=component))
    assert upload is not None
    assert upload.eid.hex() == eid
    assert upload.observed_at == NOW - 30
    assert upload.uwt_mode is False


@pytest.mark.parametrize("component", MEASURED_COMPONENT_ROWS)
async def test_report_can_be_read_with_the_series_key(component: int) -> None:
    upload = await _upload(_sighting(component=component))
    key = derive_component_key(EIK, component)
    assert upload.eid == generate_eid_variant(key, COUNTER, LEGACY)
    encrypted, sx = cryptor.encrypt(b"location", b"\x07" * 32, upload.eid)
    result = cryptor.decrypt_foreign_report([key], encrypted, sx, COUNTER)
    assert result.plaintext == b"location"
    with pytest.raises(ForeignReportAuthError):
        cryptor.decrypt_foreign_report([EIK], encrypted, sx, COUNTER)


async def test_single_component_sighting_stays_with_the_eik() -> None:
    upload = await _upload(_sighting(component=None))
    assert upload.eid == generate_eid_variant(EIK, COUNTER, LEGACY)


async def test_foreign_fingerprint_reports_nothing() -> None:
    fingerprint = identity_key_fingerprint(b"\x09" * 32)
    assert await _upload(_sighting(key_fingerprint=fingerprint)) is None


async def test_unknown_fingerprint_reports_nothing() -> None:
    assert await _upload(_sighting(key_fingerprint=None)) is None


async def test_component_with_another_variant_reports_nothing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG):
        upload = await _upload(_sighting(variant=EidVariant.SPEC_P256_X32_BE))
    assert upload is None
    assert derive_component_key(EIK, 2).hex() not in caplog.text


async def test_old_sighting_reports_nothing() -> None:
    assert await _upload(_sighting(observed_at=NOW - 10_000)) is None


async def test_unknown_mode_travels_with_the_report() -> None:
    upload = await _upload(_sighting(uwt_mode=None))
    assert upload.uwt_mode is None


async def test_debug_line_names_the_component_without_key_material(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG):
        await _upload(_sighting())
    assert "component=2" in caplog.text
    assert derive_component_key(EIK, 2).hex() not in caplog.text
    assert EIK.hex() not in caplog.text
