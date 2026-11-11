# tests/test_button_setup.py
"""Regression tests for button entity setup edge cases."""

from __future__ import annotations

import importlib
import sys
import time
from collections.abc import Callable
from pathlib import Path
from types import MethodType, ModuleType, SimpleNamespace
from typing import Any

import pytest

from tests.helpers import compile_class_method_from_module
from tests.helpers.config_entries_stub import make_config_entry

pytestmark = pytest.mark.asyncio


def _ensure_button_dependencies() -> None:
    """Populate minimal Home Assistant stubs required for the button module."""

    if "homeassistant" not in sys.modules:
        ha_root = ModuleType("homeassistant")
        ha_root.__path__ = []  # type: ignore[attr-defined]
        sys.modules["homeassistant"] = ha_root
    else:
        ha_root = sys.modules["homeassistant"]

    components_pkg = sys.modules.get("homeassistant.components")
    if components_pkg is None:
        components_pkg = ModuleType("homeassistant.components")
        components_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["homeassistant.components"] = components_pkg
    helpers_pkg = sys.modules.get("homeassistant.helpers")
    if helpers_pkg is None:
        helpers_pkg = ModuleType("homeassistant.helpers")
        helpers_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["homeassistant.helpers"] = helpers_pkg

    setattr(ha_root, "components", components_pkg)
    setattr(ha_root, "helpers", helpers_pkg)

    if "homeassistant.components.button" not in sys.modules:
        button_module = ModuleType("homeassistant.components.button")

        class _ButtonEntity:  # pragma: no cover - structural stub
            pass

        class _ButtonEntityDescription:  # pragma: no cover - structural stub
            def __init__(self, **kwargs: Any) -> None:
                for key, value in kwargs.items():
                    setattr(self, key, value)

        button_module.ButtonEntity = _ButtonEntity
        button_module.ButtonEntityDescription = _ButtonEntityDescription
        sys.modules["homeassistant.components.button"] = button_module
        setattr(components_pkg, "button", button_module)

    if "homeassistant.config_entries" not in sys.modules:
        config_entries = ModuleType("homeassistant.config_entries")

        class _ConfigEntry:  # pragma: no cover - structural stub
            pass

        config_entries.ConfigEntry = _ConfigEntry
        sys.modules["homeassistant.config_entries"] = config_entries

    if "homeassistant.core" not in sys.modules:
        core_module = ModuleType("homeassistant.core")

        class _HomeAssistant:  # pragma: no cover - structural stub
            pass

        def _callback(func):  # pragma: no cover - structural stub
            return func

        core_module.HomeAssistant = _HomeAssistant
        core_module.callback = _callback
        sys.modules["homeassistant.core"] = core_module

    if "homeassistant.exceptions" not in sys.modules:
        exc_module = ModuleType("homeassistant.exceptions")

        class _HomeAssistantError(Exception):
            pass

        exc_module.HomeAssistantError = _HomeAssistantError
        sys.modules["homeassistant.exceptions"] = exc_module

    if "homeassistant.helpers.device_registry" not in sys.modules:
        device_registry = ModuleType("homeassistant.helpers.device_registry")

        class _DeviceEntry:  # pragma: no cover - structural stub
            name: str | None = None
            name_by_user: str | None = None

        class _DeviceRegistry:  # pragma: no cover - structural stub
            def async_get(self, _device_id: str) -> _DeviceEntry | None:
                return _DeviceEntry()

            def async_update_device(self, **_: Any) -> None:
                return None

        def _async_get(_hass: Any) -> _DeviceRegistry:
            return _DeviceRegistry()

        device_registry.async_get = _async_get
        sys.modules["homeassistant.helpers.device_registry"] = device_registry

    if "homeassistant.helpers.entity_registry" not in sys.modules:
        entity_registry = ModuleType("homeassistant.helpers.entity_registry")

        class _EntityEntry:  # pragma: no cover - structural stub
            device_id: str | None = "device"
            name_by_user: str | None = None

        class _EntityRegistry:  # pragma: no cover - structural stub
            def async_get(self, _entity_id: str) -> _EntityEntry | None:
                return _EntityEntry()

        def _async_get(_hass: Any) -> _EntityRegistry:
            return _EntityRegistry()

        entity_registry.async_get = _async_get
        sys.modules["homeassistant.helpers.entity_registry"] = entity_registry

    entity_platform_module = sys.modules.get("homeassistant.helpers.entity_platform")
    if entity_platform_module is None:
        entity_platform_module = ModuleType("homeassistant.helpers.entity_platform")

        class _AddEntitiesCallback:  # pragma: no cover - structural stub
            pass

        entity_platform_module.AddEntitiesCallback = _AddEntitiesCallback
        sys.modules["homeassistant.helpers.entity_platform"] = entity_platform_module

    if not hasattr(entity_platform_module, "async_get_current_platform"):

        class _StubPlatform:
            def async_register_platform_entity_service(
                self, *_: Any, **__: Any
            ) -> None:
                return None

            def async_register_entity_service(self, *_: Any, **__: Any) -> None:
                return None

        entity_platform_module.async_get_current_platform = _StubPlatform

    if "homeassistant.helpers.entity" not in sys.modules:
        entity_module = ModuleType("homeassistant.helpers.entity")

        class _DeviceInfo:  # pragma: no cover - structural stub
            def __init__(self, **_: Any) -> None:
                return None

        entity_module.DeviceInfo = _DeviceInfo
        sys.modules["homeassistant.helpers.entity"] = entity_module

    if "homeassistant.helpers.network" not in sys.modules:
        network_module = ModuleType("homeassistant.helpers.network")

        def _get_url(*_: Any, **__: Any) -> str:
            return "http://example.com"

        network_module.get_url = _get_url
        sys.modules["homeassistant.helpers.network"] = network_module

    if "homeassistant.helpers.update_coordinator" not in sys.modules:
        coordinator_module = ModuleType("homeassistant.helpers.update_coordinator")

        class _CoordinatorEntity:  # pragma: no cover - structural stub
            def __init__(self, *_: Any, **__: Any) -> None:
                return None

        coordinator_module.CoordinatorEntity = _CoordinatorEntity
        sys.modules["homeassistant.helpers.update_coordinator"] = coordinator_module

    # Dead branch in practice: tests/conftest.py imports coordinator.main in
    # pytest_configure, which pulls in this package. Should it ever become
    # reachable again, note that it installs an unpatched module stub that
    # survives the test, which the coordinator identity guard would flag.
    if "custom_components.googlefindmy.coordinator" not in sys.modules:
        coordinator_module = ModuleType("custom_components.googlefindmy.coordinator")

        class _GoogleFindMyCoordinator:  # pragma: no cover - structural stub
            def __init__(self, *_: Any, **__: Any) -> None:
                return None

        coordinator_module.GoogleFindMyCoordinator = _GoogleFindMyCoordinator
        sys.modules["custom_components.googlefindmy.coordinator"] = coordinator_module


def _load_can_request_location_impl() -> Any:
    """Return the coordinator's can_request_location method for isolated testing."""

    # Method moved to locate.py (LocateOperations mixin) during refactoring
    locate_path = (
        Path(__file__).resolve().parent.parent
        / "custom_components"
        / "googlefindmy"
        / "coordinator"
        / "locate.py"
    )
    return compile_class_method_from_module(
        module_path=locate_path,
        class_name="LocateOperations",
        method_name="can_request_location",
        global_overrides={"time": time},
    )


async def test_blank_device_name_populates_buttons(
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> None:
    """Buttons are added even when the device label is blank or missing."""

    del (
        deterministic_config_subentry_id
    )  # fixture side effects patch ensure_config_subentry_id

    _ensure_button_dependencies()
    button_module = importlib.import_module("custom_components.googlefindmy.button")

    class _StubCoordinator(button_module.GoogleFindMyCoordinator):
        def __init__(self, devices: list[dict[str, Any]]) -> None:
            self.hass = SimpleNamespace()
            self.config_entry = make_config_entry(entry_id="entry-id")
            self.data = devices
            self._listeners: list[Any] = []

        def async_add_listener(self, listener):  # type: ignore[override]
            self._listeners.append(listener)
            return lambda: None

        def stable_subentry_identifier(
            self, *, key: str | None = None, feature: str | None = None
        ) -> str:
            assert key is not None, "Buttons must resolve subentry identifier by key"
            return f"{key}-identifier"

        def get_subentry_metadata(
            self, *, key: str | None = None, feature: str | None = None
        ) -> Any:
            if key is not None:
                resolved = key
            elif feature in {"button", "device_tracker", "sensor", "switch"}:
                resolved = button_module.TRACKER_SUBENTRY_KEY
            elif feature == "binary_sensor":
                resolved = button_module.SERVICE_SUBENTRY_KEY
            else:
                resolved = button_module.TRACKER_SUBENTRY_KEY
            return SimpleNamespace(key=resolved)

        def get_subentry_snapshot(
            self, key: str | None = None, *, feature: str | None = None
        ) -> list[dict[str, Any]]:
            return list(self.data)

        def is_device_visible_in_subentry(
            self, subentry_key: str, device_id: str
        ) -> bool:
            return any(dev.get("id") == device_id for dev in self.data)

    class _StubConfigEntry:
        def __init__(self, coordinator: button_module.GoogleFindMyCoordinator) -> None:
            self.runtime_data = coordinator
            self.entry_id = "entry-id"
            self._unsub: list[Any] = []

        def async_on_unload(self, callback):
            self._unsub.append(callback)

    devices = [{"id": "device-1", "name": ""}]
    coordinator = _StubCoordinator(devices)
    config_entry = _StubConfigEntry(coordinator)

    added: list[list[Any]] = []

    def _capture(entities, update_before_add=False):
        added.append(list(entities))
        assert update_before_add is True

    await button_module.async_setup_entry(SimpleNamespace(), config_entry, _capture)

    assert len(added) == 1
    assert len(added[0]) == 3
    for entity in added[0]:
        assert entity._device["name"] == ""
        assert entity.device_label() == "device-1"
        assert entity.subentry_key == button_module.TRACKER_SUBENTRY_KEY
        assert f"{button_module.TRACKER_SUBENTRY_KEY}-identifier" in entity.unique_id

    new_device = {"id": "device-2", "name": None}
    coordinator.data.append(new_device)
    coordinator._listeners[0]()

    assert len(added) == 2
    assert len(added[1]) == 3
    for entity in added[1]:
        assert entity._device["name"] is None
        assert entity.device_label() == "device-2"
        assert entity.subentry_key == button_module.TRACKER_SUBENTRY_KEY
        assert f"{button_module.TRACKER_SUBENTRY_KEY}-identifier" in entity.unique_id


async def test_locate_button_available_when_push_unready() -> None:
    """Locate button stays available even when push transport is not ready."""

    _ensure_button_dependencies()
    button_module = importlib.import_module("custom_components.googlefindmy.button")
    can_request_location_impl = _load_can_request_location_impl()

    class _CoordinatorStub:
        def __init__(self) -> None:
            self.hass = SimpleNamespace()
            self.config_entry = make_config_entry(entry_id="entry-id")
            self.data = [{"id": "device-1", "name": "Tracker"}]
            self._listeners: list[Any] = []
            self._is_polling = False
            self._locate_inflight: set[str] = set()
            self._locate_cooldown_until: dict[str, float] = {}
            self._device_poll_cooldown_until: dict[str, float] = {}
            self.can_request_location = MethodType(
                can_request_location_impl,
                self,
            )

        def async_add_listener(self, listener):  # type: ignore[override]
            self._listeners.append(listener)
            return lambda: None

        def is_ignored(self, device_id: str) -> bool:
            return False

        def _api_push_ready(self) -> bool:
            return False

        def is_device_visible_in_subentry(
            self, subentry_key: str, device_id: str
        ) -> bool:
            return True

        def is_device_present(self, device_id: str) -> bool:
            return True

    coordinator = _CoordinatorStub()
    device = coordinator.data[0]
    locate_button = button_module.GoogleFindMyLocateButton(
        coordinator,
        device,
        device.get("name"),
        subentry_key=button_module.TRACKER_SUBENTRY_KEY,
        subentry_identifier=f"{button_module.TRACKER_SUBENTRY_KEY}-identifier",
    )

    assert locate_button.available is True
    assert locate_button.subentry_key == button_module.TRACKER_SUBENTRY_KEY


async def test_locate_button_unavailable_on_gate_error() -> None:
    """Locate button marks itself unavailable when gating fails unexpectedly."""

    _ensure_button_dependencies()
    button_module = importlib.import_module("custom_components.googlefindmy.button")

    class _CoordinatorStub:
        def __init__(self) -> None:
            self.hass = SimpleNamespace()
            self.config_entry = make_config_entry(entry_id="entry-id")
            self.data = [{"id": "device-1", "name": "Tracker"}]
            self._listeners: list[Any] = []
            self._is_polling = False
            self._locate_inflight: set[str] = set()
            self._locate_cooldown_until: dict[str, float] = {}
            self._device_poll_cooldown_until: dict[str, float] = {}

        def async_add_listener(self, listener):  # type: ignore[override]
            self._listeners.append(listener)
            return lambda: None

        def is_ignored(self, device_id: str) -> bool:
            return False

        def _api_push_ready(self) -> bool:
            return True

        def is_device_visible_in_subentry(
            self, subentry_key: str, device_id: str
        ) -> bool:
            return True

        def is_device_present(self, device_id: str) -> bool:
            return True

        def can_request_location(self, device_id: str) -> bool:  # type: ignore[override]
            raise RuntimeError("gate failed")

    coordinator = _CoordinatorStub()
    device = coordinator.data[0]
    locate_button = button_module.GoogleFindMyLocateButton(
        coordinator,
        device,
        device.get("name"),
        subentry_key=button_module.TRACKER_SUBENTRY_KEY,
        subentry_identifier=f"{button_module.TRACKER_SUBENTRY_KEY}-identifier",
    )
    assert locate_button.available is False
