# custom_components/googlefindmy/ha_typing.py
"""Typed shims for Home Assistant base classes lacking typing metadata."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast

from homeassistant.core import callback as ha_callback

_CallbackT = TypeVar("_CallbackT", bound=Callable[..., Any])


def callback(func: _CallbackT) -> _CallbackT:
    """Return a typed wrapper around Home Assistant's ``callback`` decorator."""
    return cast(_CallbackT, ha_callback(func))


@dataclass(slots=True)
class CloudDiscoveryRuntime:
    """Container for cloud-discovery runtime bookkeeping."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    active_keys: set[str] = field(default_factory=set)
    dispatcher_unsubscribers: list[Callable[[], None]] = field(default_factory=list)
    retry_handles: set[asyncio.Future[Any]] = field(default_factory=set)
    results: _CloudDiscoveryResults | list[dict[str, Any]] | None = None


if TYPE_CHECKING:
    from typing import Self

    from aiohttp import web
    from homeassistant.core import Context, HomeAssistant

    from .discovery import _CloudDiscoveryResults

    _CoordinatorT = TypeVar("_CoordinatorT")
    _DataT = TypeVar("_DataT")

    class HomeAssistantView:
        """Structural type for views served through the HTTP component."""

        url: str
        name: str
        requires_auth: bool
        hass: HomeAssistant

        async def get(
            self, request: web.Request, *args: Any, **kwargs: Any
        ) -> web.Response: ...

    class _EntityBase:
        """Common entity protocol for Home Assistant platform entities."""

        hass: HomeAssistant
        entity_id: str | None
        # Set by Home Assistant when a user acts on the entity (for example a
        # button press); service calls forward it so permissions apply.
        _context: Context | None

        async def async_added_to_hass(self) -> None: ...

        async def async_will_remove_from_hass(self) -> None: ...

        def async_write_ha_state(self) -> None: ...

        # Every state write path ends here on the Core lines CI tests; from 2026.5 the
        # docstring of ``Entity.async_write_ha_state`` names it as the override point.
        def _async_write_ha_state(self) -> None: ...

        @property
        def available(self) -> bool: ...

    class CoordinatorEntity(_EntityBase, Generic[_CoordinatorT]):
        """Structural type for coordinator-backed entities."""

        coordinator: _CoordinatorT

        def __init__(self, coordinator: _CoordinatorT) -> None: ...

    class DataUpdateCoordinator(Generic[_DataT]):
        """Structural type for Home Assistant's data coordinator."""

        hass: HomeAssistant
        data: _DataT
        logger: logging.Logger
        update_interval: timedelta | None

        def __init__(
            self,
            hass: HomeAssistant,
            logger: logging.Logger,
            *,
            name: str | None = None,
            update_interval: timedelta | None = None,
        ) -> None: ...

        def async_set_updated_data(self, data: _DataT) -> None: ...

        def async_set_update_error(self, error: Exception) -> None: ...

        def async_update_listeners(self) -> None: ...

        def async_add_listener(
            self, update_callback: Callable[[], None]
        ) -> Callable[[], None]: ...

        async def async_request_refresh(self) -> None: ...

        async def async_config_entry_first_refresh(self) -> None: ...

        async def async_refresh(self) -> None: ...

        async def async_shutdown(self) -> None: ...

    class ButtonEntity(_EntityBase):
        """Structural type for button platform entities."""

    class BinarySensorEntity(_EntityBase):
        """Structural type for binary_sensor platform entities."""

    class SensorEntity(_EntityBase):
        """Structural type for sensor platform entities."""

        @property
        def native_unit_of_measurement(self) -> str | None: ...

    @dataclass
    class SensorExtraStoredData:
        """Structural type for the restore data of sensor entities."""

        native_value: Any
        native_unit_of_measurement: str | None

        def as_dict(self) -> dict[str, Any]: ...

        @classmethod
        def from_dict(cls, restored: dict[str, Any]) -> Self | None: ...

    class RestoreSensor(SensorEntity):
        """Structural type for restore-capable sensors."""

        async def async_get_last_sensor_data(self) -> Any: ...

        async def async_get_last_extra_data(self) -> Any: ...

        async def async_get_last_state(self) -> Any: ...

    class TrackerEntity(_EntityBase):
        """Structural type for device_tracker entities."""

    class RestoreEntity(_EntityBase):
        """Structural type for restore-capable generic entities."""

        async def async_get_last_state(self) -> Any: ...

else:
    import sys
    from types import ModuleType

    from homeassistant.components.binary_sensor import BinarySensorEntity  # noqa: F401
    from homeassistant.components.button import ButtonEntity  # noqa: F401
    from homeassistant.components.device_tracker import TrackerEntity  # noqa: F401

    try:
        from homeassistant.helpers.http import HomeAssistantView  # noqa: F401
    except ImportError:  # pragma: no cover - fallback for older/stub environments
        _http_module = sys.modules.get("homeassistant.components.http")
        if isinstance(_http_module, ModuleType) and hasattr(
            _http_module, "HomeAssistantView"
        ):
            HomeAssistantView = getattr(  # type: ignore[assignment]
                _http_module, "HomeAssistantView"
            )
        else:

            class _StubHomeAssistantView:  # pragma: no cover - minimal fallback
                """Stand-in base class for environments without HTTP helpers."""

                requires_auth = False
                url = ""
                name = ""

                def __init__(self, hass: Any | None = None) -> None:
                    self.hass = hass

                async def get(self, *_args: Any, **_kwargs: Any) -> Any:
                    raise NotImplementedError

            HomeAssistantView = _StubHomeAssistantView  # type: ignore[assignment]
    from homeassistant.components.sensor import (  # noqa: F401
        RestoreSensor,
        SensorEntity,
        SensorExtraStoredData,
    )
    from homeassistant.helpers.restore_state import RestoreEntity  # noqa: F401
    from homeassistant.helpers.update_coordinator import (
        CoordinatorEntity,  # noqa: F401
        DataUpdateCoordinator,  # noqa: F401
    )
