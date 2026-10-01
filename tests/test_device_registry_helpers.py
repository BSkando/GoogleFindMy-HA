# tests/test_device_registry_helpers.py
"""Tests for Home Assistant device registry lookup compatibility helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from custom_components.googlefindmy.const import DOMAIN
from custom_components.googlefindmy.device_registry_helpers import (
    async_get_device_by_identifier_compat,
    async_get_device_by_identifiers_compat,
)


@dataclass
class _Device:
    """Minimal device registry entry used by lookup helper tests."""

    id: str
    identifiers: set[tuple[str, str]]
    config_entries: set[str]
    config_entries_subentries: dict[str, set[str | None]] = field(
        default_factory=dict
    )


class _ModernRegistry:
    """Registry exposing the Home Assistant 2026.8 lookup helpers."""

    def __init__(self, devices: list[_Device]) -> None:
        self.devices = devices
        self.deprecated_calls = 0

    def async_get_device_by_identifier(
        self,
        identifier: tuple[str, str],
        config_entry_id: str,
    ) -> _Device | None:
        for device in self.devices:
            if identifier in device.identifiers and config_entry_id in device.config_entries:
                return device
        return None

    def async_get_devices(
        self,
        *,
        identifiers: set[tuple[str, str]] | None = None,
        config_entry_id: str | None = None,
    ) -> list[_Device]:
        identifiers = identifiers or set()
        return [
            device
            for device in self.devices
            if identifiers & device.identifiers
            and (
                config_entry_id is None or config_entry_id in device.config_entries
            )
        ]

    def async_get_device(self, *_args: Any, **_kwargs: Any) -> None:
        self.deprecated_calls += 1
        raise AssertionError("deprecated async_get_device should not be called")


class _ListOnlyRegistry:
    """Registry exposing only the plural Home Assistant 2026.8 lookup helper."""

    def __init__(self, devices: list[_Device]) -> None:
        self.devices = devices

    def async_get_devices(
        self,
        *,
        identifiers: set[tuple[str, str]] | None = None,
        config_entry_id: str | None = None,
    ) -> list[_Device]:
        identifiers = identifiers or set()
        return [
            device
            for device in self.devices
            if identifiers & device.identifiers
            and (
                config_entry_id is None or config_entry_id in device.config_entries
            )
        ]


class _LegacyRegistry:
    """Registry exposing only the deprecated lookup helper used by old cores."""

    def __init__(self, device: _Device | None) -> None:
        self.device = device
        self.calls = 0

    def async_get_device(
        self,
        *,
        identifiers: set[tuple[str, str]],
    ) -> _Device | None:
        self.calls += 1
        if self.device is None:
            return None
        return self.device if identifiers & self.device.identifiers else None


def test_async_get_device_by_identifier_compat_uses_entry_scoped_api() -> None:
    """Use the modern single-identifier helper before deprecated fallback."""

    identifier = (DOMAIN, "entry-a:device-1")
    expected = _Device("device-a", {identifier}, {"entry-a"})
    registry = _ModernRegistry(
        [
            _Device("device-b", {identifier}, {"entry-b"}),
            expected,
        ]
    )

    result = async_get_device_by_identifier_compat(
        registry,
        identifier,
        config_entry_id="entry-a",
    )

    assert result is expected
    assert registry.deprecated_calls == 0


def test_async_get_device_by_identifiers_compat_falls_back_on_legacy_core() -> None:
    """Use deprecated lookup only when modern helpers are unavailable."""

    identifier = (DOMAIN, "entry-a:device-1")
    expected = _Device("device-a", {identifier}, {"entry-a"})
    registry = _LegacyRegistry(expected)

    result = async_get_device_by_identifiers_compat(
        registry,
        (identifier,),
        config_entry_id="entry-a",
    )

    assert result is expected
    assert registry.calls == 1


def test_async_get_device_by_identifiers_compat_prefers_config_entry_match() -> None:
    """Choose the device owned by the relevant entry from plural matches."""

    identifier = (DOMAIN, "shared-device")
    expected = _Device("device-a", {identifier}, {"entry-a"})
    other = _Device("device-b", {identifier}, {"entry-b"})
    registry = _ListOnlyRegistry([other, expected])

    result = async_get_device_by_identifiers_compat(
        registry,
        (identifier,),
        config_entry_id="entry-a",
    )

    assert result is expected
