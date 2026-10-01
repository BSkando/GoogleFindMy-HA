# custom_components/googlefindmy/device_registry_helpers.py
"""Compatibility helpers for Home Assistant device registry lookups."""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from typing import Any

DeviceIdentifier = tuple[str, str]


def _ordered_identifiers(
    identifiers: Collection[DeviceIdentifier],
) -> tuple[DeviceIdentifier, ...]:
    """Return identifiers in deterministic caller order."""

    ordered: list[DeviceIdentifier] = []
    seen: set[DeviceIdentifier] = set()
    for identifier in identifiers:
        if identifier not in seen:
            ordered.append(identifier)
            seen.add(identifier)
    return tuple(ordered)


def _device_has_config_entry(device: Any, config_entry_id: str | None) -> bool:
    """Return whether a registry device belongs to the config entry."""

    if config_entry_id is None:
        return True

    config_entries = getattr(device, "config_entries", None)
    subentry_links = getattr(device, "config_entries_subentries", None)
    if config_entries is None and subentry_links is None:
        return True

    if isinstance(config_entries, str):
        if config_entries == config_entry_id:
            return True
    elif isinstance(config_entries, Iterable):
        if config_entry_id in config_entries:
            return True

    return isinstance(subentry_links, Mapping) and config_entry_id in subentry_links


def _device_has_identifier(device: Any, identifier: DeviceIdentifier) -> bool:
    """Return whether a registry device exposes the identifier."""

    identifiers = getattr(device, "identifiers", None)
    return isinstance(identifiers, Collection) and identifier in identifiers


def _select_device(
    matches: Iterable[Any],
    identifiers: tuple[DeviceIdentifier, ...],
    config_entry_id: str | None,
) -> Any | None:
    """Select a deterministic match, preferring the requested config entry."""

    devices = list(matches)
    if not devices:
        return None

    for identifier in identifiers:
        for device in devices:
            if _device_has_identifier(device, identifier) and _device_has_config_entry(
                device, config_entry_id
            ):
                return device

    for device in devices:
        if _device_has_config_entry(device, config_entry_id):
            return device

    return devices[0]


def async_get_device_by_identifier_compat(
    device_registry: Any,
    identifier: DeviceIdentifier,
    *,
    config_entry_id: str | None,
) -> Any | None:
    """Return a device by identifier without calling deprecated APIs when possible."""

    return async_get_device_by_identifiers_compat(
        device_registry,
        (identifier,),
        config_entry_id=config_entry_id,
    )


def async_get_device_by_identifiers_compat(
    device_registry: Any,
    identifiers: Collection[DeviceIdentifier],
    *,
    config_entry_id: str | None,
) -> Any | None:
    """Return a device for any identifier, preferring the entry-scoped API."""

    ordered = _ordered_identifiers(identifiers)
    if not ordered:
        return None

    get_by_identifier = getattr(
        device_registry,
        "async_get_device_by_identifier",
        None,
    )
    if config_entry_id is not None and callable(get_by_identifier):
        for identifier in ordered:
            try:
                device = get_by_identifier(identifier, config_entry_id)
            except TypeError:
                device = get_by_identifier(
                    identifier=identifier,
                    config_entry_id=config_entry_id,
                )
            if device is not None:
                return device

    get_devices = getattr(device_registry, "async_get_devices", None)
    if callable(get_devices):
        try:
            matches = get_devices(
                identifiers=set(ordered),
                config_entry_id=config_entry_id,
            )
        except TypeError:
            matches = get_devices(identifiers=set(ordered))
        return _select_device(matches, ordered, config_entry_id)

    get_device = getattr(device_registry, "async_get_device", None)
    if callable(get_device):
        try:
            device = get_device(identifiers=set(ordered))
        except TypeError:
            try:
                device = get_device(set(ordered))
            except TypeError:
                device = None
        if device is not None and _device_has_config_entry(device, config_entry_id):
            return device

    return None
