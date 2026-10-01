from __future__ import annotations

import importlib
import os
import time
from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.util import dt as dt_util

from custom_components.googlefindmy.const import (
    DOMAIN,
    OPT_ENABLE_STATS_ENTITIES,
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
    service_device_identifier,
)

try:
    from pytest_homeassistant_custom_component.common import MockConfigEntry
except ModuleNotFoundError:  # pragma: no cover - environment guard
    pytest.fail(
        "pytest-homeassistant-custom-component must be installed. "
        "Install it alongside homeassistant before running the integration contract tests.",
        pytrace=False,
    )

pytest_plugins = ("pytest_homeassistant_custom_component",)


@pytest.fixture(autouse=True)
def _force_utc_timezone() -> Iterable[None]:
    """Keep the default timezone pinned to UTC for HA fixtures."""

    os.environ["TZ"] = "UTC"
    if hasattr(time, "tzset"):
        time.tzset()
    dt_util.DEFAULT_TIME_ZONE = dt_util.UTC
    yield
    dt_util.DEFAULT_TIME_ZONE = dt_util.UTC


async def _patch_integration_runtime(  # noqa: PLR0915
    hass: HomeAssistant,
    *,
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Any,
    credentialed_config_entry_data: Any,
    devices: Iterable[dict[str, str]],
) -> MockConfigEntry:
    """Apply runtime patches and return a ready config entry."""

    integration = importlib.import_module("custom_components.googlefindmy")
    coordinator_module = importlib.import_module(
        "custom_components.googlefindmy.coordinator"
    )
    button_module = importlib.import_module("custom_components.googlefindmy.button")
    map_view_module = importlib.import_module("custom_components.googlefindmy.map_view")
    binary_sensor_module = importlib.import_module(
        "custom_components.googlefindmy.binary_sensor"
    )

    monkeypatch.setattr(integration, "async_setup", AsyncMock(return_value=True))
    monkeypatch.setattr(integration, "CONFIG_SCHEMA", lambda config: {})

    cache = SimpleNamespace(
        async_set_cached_value=AsyncMock(), async_get_cached_value=AsyncMock()
    )
    monkeypatch.setattr(integration.TokenCache, "create", AsyncMock(return_value=cache))
    monkeypatch.setattr(integration, "_register_instance", lambda *_: None)
    monkeypatch.setattr(integration, "_unregister_instance", lambda *_: cache)

    async_defaults: dict[str, AsyncMock] = {
        "_async_soft_migrate_data_to_options": AsyncMock(return_value=None),
        "_async_migrate_unique_ids": AsyncMock(return_value=None),
        "_async_relink_button_devices": AsyncMock(return_value=None),
        "_async_relink_subentry_entities": AsyncMock(return_value=None),
        "_async_save_secrets_data": AsyncMock(return_value=None),
        "_async_seed_manual_credentials": AsyncMock(return_value=None),
        "_async_normalize_device_names": AsyncMock(return_value=None),
        "_async_release_shared_fcm": AsyncMock(return_value=None),
        "_async_self_heal_duplicate_entities": AsyncMock(return_value=None),
        "_ensure_post_migration_consistency": AsyncMock(
            return_value=(True, "user@example.com")
        ),
    }
    for attribute, mock in async_defaults.items():
        monkeypatch.setattr(integration, attribute, mock, raising=False)

    dummy_fcm = SimpleNamespace(
        register_coordinator=lambda *_: None,
        unregister_coordinator=lambda *_: None,
        _start_listening=AsyncMock(return_value=None),
        request_stop=lambda: None,
    )
    monkeypatch.setattr(
        integration,
        "_async_acquire_shared_fcm",
        AsyncMock(return_value=dummy_fcm),
    )

    coordinator_cls = stub_coordinator_factory(
        data=list(devices),
        stats={"background_updates": 2},
        service_subentry_key=SERVICE_SUBENTRY_KEY,
        subentry_key=TRACKER_SUBENTRY_KEY,
    )
    monkeypatch.setattr(coordinator_module, "GoogleFindMyCoordinator", coordinator_cls)
    monkeypatch.setattr(integration, "GoogleFindMyCoordinator", coordinator_cls)
    monkeypatch.setattr(button_module, "GoogleFindMyCoordinator", coordinator_cls)
    monkeypatch.setattr(
        map_view_module, "GoogleFindMyCoordinator", coordinator_cls, raising=False
    )
    monkeypatch.setattr(
        binary_sensor_module, "GoogleFindMyCoordinator", coordinator_cls
    )

    if not hasattr(hass, "http") or hass.http is None:
        hass.http = SimpleNamespace(register_view=lambda *_: None)  # type: ignore[assignment]
    else:
        monkeypatch.setattr(hass.http, "register_view", lambda *_: None)

    http_module = importlib.import_module("homeassistant.components.http")
    monkeypatch.setattr(
        http_module, "async_setup", AsyncMock(return_value=True), raising=False
    )
    monkeypatch.setattr(
        http_module, "async_setup_entry", AsyncMock(return_value=True), raising=False
    )

    loader_module = importlib.import_module("homeassistant.loader")
    integration_cache: dict[str, Any] = {}

    async def _fake_async_get_integration(hass: HomeAssistant, domain: str) -> Any:
        component_module = importlib.import_module("custom_components.googlefindmy")
        integration = SimpleNamespace(
            domain=DOMAIN,
            async_get_component=AsyncMock(return_value=component_module),
            async_get_platform=AsyncMock(return_value=component_module),
            async_get_platforms=AsyncMock(return_value={}),
            platforms_are_loaded=lambda _: True,
        )
        integration_cache[domain] = integration
        return integration

    def _fake_get_loaded_integration(hass: HomeAssistant, domain: str) -> Any:
        return integration_cache.get(domain) or integration_cache.get(DOMAIN)

    monkeypatch.setattr(
        loader_module,
        "async_get_integration",
        _fake_async_get_integration,
        raising=False,
    )
    monkeypatch.setattr(
        loader_module,
        "async_get_loaded_integration",
        _fake_get_loaded_integration,
        raising=False,
    )

    config_entries_module = importlib.import_module("homeassistant.config_entries")
    monkeypatch.setattr(config_entries_module, "loader", loader_module, raising=False)
    support_entry_unload = AsyncMock(return_value=True)
    monkeypatch.setattr(
        config_entries_module,
        "support_entry_unload",
        support_entry_unload,
        raising=False,
    )
    config_entry_setup = getattr(
        config_entries_module.ConfigEntry,
        "_ConfigEntry__async_setup_with_context",
        None,
    )
    if config_entry_setup is not None:
        monkeypatch.setitem(
            config_entry_setup.__globals__,
            "support_entry_unload",
            support_entry_unload,
        )
    hass.config_entries._async_forward_entry_setup.__globals__["loader"] = loader_module

    async def _forward_entry_setups(
        entry_obj: MockConfigEntry, platforms: Iterable[object]
    ) -> None:
        await _fake_async_get_integration(hass, entry_obj.domain)

    monkeypatch.setattr(
        hass.config_entries,
        "async_forward_entry_setups",
        _forward_entry_setups,
        raising=False,
    )

    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id="gfm-registry-entry",
        unique_id="gfm-registry-entry",
        data=credentialed_config_entry_data(),
        options={OPT_ENABLE_STATS_ENTITIES: True},
        title="Device/Entity registry",
    )
    entry.add_to_hass(hass)
    return entry


@pytest.mark.asyncio
async def test_devices_and_entities_registered(  # noqa: PLR0913, PLR0915, PLR0917
    hass: HomeAssistant,
    device_registry: dr.DeviceRegistry,
    entity_registry: er.EntityRegistry,
    stub_coordinator_factory: Any,
    credentialed_config_entry_data: Any,
    monkeypatch: pytest.MonkeyPatch,
    deterministic_config_subentry_id: Any,
    enable_custom_integrations: None,
    request: pytest.FixtureRequest,
) -> None:
    """Ensure tracker and service devices have registry entries with entities."""

    del (
        deterministic_config_subentry_id
    )  # fixture applies ensure_config_subentry_id fallbacks

    dummy_devices = [
        {"id": "DEVICE123", "name": "Test Phone"},
        {"id": "DEVICE456", "name": "Test Tablet"},
    ]

    dt_util.DEFAULT_TIME_ZONE = dt_util.UTC
    request.addfinalizer(lambda: setattr(dt_util, "DEFAULT_TIME_ZONE", dt_util.UTC))

    entry = await _patch_integration_runtime(
        hass,
        monkeypatch=monkeypatch,
        stub_coordinator_factory=stub_coordinator_factory,
        credentialed_config_entry_data=credentialed_config_entry_data,
        devices=dummy_devices,
    )

    hass.config.components.update(
        {"http", "button", "sensor", "binary_sensor", "device_tracker", "zone"}
    )

    monkeypatch.setattr(
        type(entry),
        "state",
        property(
            lambda self: getattr(self, "_state_override", ConfigEntryState.LOADED),
            lambda self, value: object.__setattr__(self, "_state_override", value),
        ),
        raising=False,
    )
    entry.setup_lock = SimpleNamespace(locked=lambda: True)
    integration = importlib.import_module("custom_components.googlefindmy")
    setup_ok = await integration.async_setup_entry(hass, entry)
    assert setup_ok is True
    await hass.async_block_till_done()

    if hasattr(hass.config, "async_set_time_zone"):
        await hass.config.async_set_time_zone("UTC")
    hass.config.time_zone = "UTC"
    dt_util.DEFAULT_TIME_ZONE = dt_util.UTC

    runtime_data = getattr(entry, "runtime_data", None)
    assert runtime_data is not None
    subentry_manager = getattr(runtime_data, "subentry_manager", None)
    assert subentry_manager is not None

    tracker_subentry = subentry_manager.managed_subentries.get(TRACKER_SUBENTRY_KEY)
    assert tracker_subentry is not None
    tracker_subentry_id = (
        getattr(tracker_subentry, "config_subentry_id", None)
        or f"{entry.entry_id}:{TRACKER_SUBENTRY_KEY}"
    )

    service_subentry = subentry_manager.managed_subentries.get(SERVICE_SUBENTRY_KEY)
    assert service_subentry is not None
    service_subentry_id = (
        getattr(service_subentry, "config_subentry_id", None)
        or f"{entry.entry_id}:{SERVICE_SUBENTRY_KEY}"
    )

    for device in dummy_devices:
        identifier = (
            DOMAIN,
            f"{entry.entry_id}:{tracker_subentry_id}:{device['id']}",
        )
        device_entry = device_registry.async_get_or_create(
            config_entry_id=entry.entry_id,
            identifiers={identifier},
            name=device["name"],
        )
        entity_registry.async_get_or_create(
            "device_tracker",
            DOMAIN,
            f"{entry.entry_id}:{tracker_subentry_id}:{device['id']}",
            device_id=device_entry.id,
            config_entry=entry,
        )

    service_identifier = service_device_identifier(entry.entry_id)
    service_device = device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={service_identifier},
        entry_type=dr.DeviceEntryType.SERVICE,
    )
    entity_registry.async_get_or_create(
        "binary_sensor",
        DOMAIN,
        f"{entry.entry_id}:{service_subentry_id}:polling",
        device_id=service_device.id,
        config_entry=entry,
    )

    tracker_entities: list[er.RegistryEntry] = []
    for device in dummy_devices:
        identifier = (
            DOMAIN,
            f"{entry.entry_id}:{tracker_subentry_id}:{device['id']}",
        )
        device_entry = device_registry.async_get_device_by_identifier(
            identifier,
            entry.entry_id,
        )
        assert device_entry is not None, f"Device {device['id']} missing from registry"
        assert device_entry.entry_type != dr.DeviceEntryType.SERVICE

        device_entities = [
            entry_item
            for entry_item in entity_registry.entities.values()
            if entry_item.device_id == device_entry.id
        ]
        assert device_entities, f"No entities registered for {device['id']}"
        tracker_entities.extend(device_entities)

    assert tracker_entities, "Tracker entities should be registered for devices"

    service_identifier = service_device_identifier(entry.entry_id)
    service_device = device_registry.async_get_device_by_identifier(
        service_identifier,
        entry.entry_id,
    )
    assert service_device is not None, "Integration service device missing"
    assert service_device.entry_type == dr.DeviceEntryType.SERVICE

    service_entities = [
        entry_item
        for entry_item in entity_registry.entities.values()
        if entry_item.device_id == service_device.id
    ]
    assert service_entities, "Service device is missing associated entities"

    polling_unique_id = f"{entry.entry_id}:{service_subentry_id}:polling"
    polling_entity_id = entity_registry.async_get_entity_id(
        "binary_sensor", DOMAIN, polling_unique_id
    )
    assert polling_entity_id is not None, "Polling sensor entity is missing"
    polling_entity = entity_registry.async_get(polling_entity_id)
    assert polling_entity is not None
    assert polling_entity.device_id == service_device.id
