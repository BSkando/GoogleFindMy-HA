# tests/test_identity_components.py
"""The component count reaches ``DeviceIdentity``; a missing value changes nothing."""

from __future__ import annotations

from dataclasses import fields
from typing import Any

import pytest

import custom_components.googlefindmy.coordinator as coordinator_module
from custom_components.googlefindmy.coordinator.cache import _METADATA_KEYS
from custom_components.googlefindmy.coordinator.helpers import (
    normalize_component_count,
)
from custom_components.googlefindmy.coordinator.main import (
    _PERSISTED_METADATA_KEYS,
    DeviceIdentity,
    _update_preserve_metadata,
)
from custom_components.googlefindmy.eid_resolver import GoogleFindMyEIDResolver
from custom_components.googlefindmy.ProtoDecoders.decoder import _DEVICE_STUB_KEYS
from tests.helpers.config_entries_stub import make_config_entry


class _StubDevice:
    def __init__(self) -> None:
        self.identifier = "device-1"
        self.id = "registry-1"
        self.custom_fields: dict[str, object] = {}
        self.disabled_by: str | None = None


class _StubRegistry:
    def async_entries_for_config_entry(self, _entry_id: str) -> list[_StubDevice]:
        return [_StubDevice()]


class _StubHass:
    def __init__(self) -> None:
        self.data: dict[str, object] = {}

    def async_create_task(self, coro: Any) -> Any:
        return coro


def _coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> coordinator_module.GoogleFindMyCoordinator:
    monkeypatch.setattr(
        coordinator_module.dr, "async_get", lambda hass: _StubRegistry()
    )
    coordinator = coordinator_module.GoogleFindMyCoordinator.__new__(
        coordinator_module.GoogleFindMyCoordinator
    )
    coordinator.hass = _StubHass()
    coordinator.config_entry = make_config_entry(entry_id="entry-1")
    coordinator._enabled_poll_device_ids = {"device-1"}
    coordinator._get_ignored_set = set
    coordinator._extract_our_identifier = lambda device: getattr(
        device, "identifier", None
    )
    coordinator.data = []
    coordinator._last_device_list = []
    coordinator._device_location_data = {}
    coordinator._identity_key_to_devices = {}
    coordinator._propagating_location = False
    return coordinator


def _row(**extra: Any) -> dict[str, Any]:
    return {"id": "device-1", "identity_key": b"\x01" * 32, **extra}


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (3, 3),
        (1, 1),
        (0, None),
        (-1, None),
        (True, None),
        (False, None),
        ("3", None),
        (3.0, None),
        (None, None),
    ],
)
def test_normalize_component_count(value: object, expected: int | None) -> None:
    assert normalize_component_count(value) == expected


def test_contracts_know_the_field() -> None:
    assert "trackable_components" in _DEVICE_STUB_KEYS
    assert "trackable_components" in _PERSISTED_METADATA_KEYS
    assert "trackable_components" in _METADATA_KEYS
    assert "trackable_components" in {field.name for field in fields(DeviceIdentity)}
    assert (
        DeviceIdentity(
            registry_id="r", canonical_id="c", identity_key=None
        ).trackable_components
        is None
    )


@pytest.mark.parametrize("source", ["data", "last_device_list", "location_data"])
def test_value_comes_from_every_source(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    coordinator = _coordinator(monkeypatch)
    row = _row(trackable_components=3)
    if source == "data":
        coordinator.data = [row]
    elif source == "last_device_list":
        coordinator._last_device_list = [row]
    else:
        coordinator._device_location_data = {"device-1": row}
    identities = coordinator.get_active_device_identities()
    assert [identity.trackable_components for identity in identities] == [3]


def test_none_from_a_younger_source_does_not_overwrite(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = _coordinator(monkeypatch)
    coordinator._device_location_data = {"device-1": _row(trackable_components=3)}
    coordinator.data = [_row(trackable_components=None)]
    identities = coordinator.get_active_device_identities()
    assert identities[0].trackable_components == 3


def test_update_preserve_metadata_keeps_the_value() -> None:
    target: dict[str, Any] = {"trackable_components": 3}
    _update_preserve_metadata(target, {"trackable_components": None})
    assert target["trackable_components"] == 3


@pytest.mark.parametrize("bad", [True, "3", 0, -1, 2.5])
def test_invalid_row_values_become_none(
    monkeypatch: pytest.MonkeyPatch, bad: object
) -> None:
    coordinator = _coordinator(monkeypatch)
    coordinator.data = [_row(trackable_components=bad)]
    assert coordinator.get_active_device_identities()[0].trackable_components is None


def test_candidates_all_carry_the_value(monkeypatch: pytest.MonkeyPatch) -> None:
    coordinator = _coordinator(monkeypatch)
    coordinator.data = [
        _row(
            trackable_components=3,
            identity_key_candidates=[b"\x01" * 32, b"\x02" * 32],
        )
    ]
    identities = coordinator.get_active_device_identities()
    assert len(identities) == 2
    assert {identity.trackable_components for identity in identities} == {3}


def test_cold_start_has_no_value_until_the_first_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing persists device rows: a restart starts without the value."""
    coordinator = _coordinator(monkeypatch)
    coordinator._device_location_data = {"device-1": _row()}
    assert coordinator.get_active_device_identities()[0].trackable_components is None
    coordinator.data = [_row(trackable_components=3)]
    assert coordinator.get_active_device_identities()[0].trackable_components == 3


def test_priming_the_location_cache_sets_no_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coordinator = _coordinator(monkeypatch)
    coordinator.prime_device_location_cache(
        "device-1", {"identity_key": b"\x01" * 32, "latitude": 1.0, "longitude": 2.0}
    )
    assert coordinator.get_active_device_identities()[0].trackable_components is None


@pytest.mark.asyncio
async def test_resolver_mapping_branch_normalizes_the_value() -> None:
    resolver = GoogleFindMyEIDResolver.__new__(GoogleFindMyEIDResolver)

    async def normalize(value: object) -> DeviceIdentity:
        identity = {
            "registry_id": "r",
            "canonical_id": "c",
            "identity_key": b"\x01" * 32,
            "trackable_components": value,
        }
        return (await resolver._normalize_identities([identity], cache=None))[0]

    assert (await normalize(3)).trackable_components == 3
    assert (await normalize("3")).trackable_components is None
    assert (await normalize(None)).trackable_components is None
