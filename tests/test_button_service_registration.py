# tests/test_button_service_registration.py
"""Ensure button setup handles missing platform service registration."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy.const import (
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
)
from tests.helpers import GoogleFindMyConfigEntryStub, drain_loop

from .test_button_setup import _ensure_button_dependencies


class _StubHass:
    """Home Assistant stub mirroring the essentials used by the buttons."""

    def __init__(self, loop: asyncio.AbstractEventLoop, domain: str) -> None:
        self.loop = loop
        self.data: dict[str, Any] = {domain: {}, "core.uuid": "ha-uuid"}
        self._tasks: list[asyncio.Task[Any]] = []

    def async_create_task(
        self, coro: Awaitable[Any], *, name: str | None = None
    ) -> asyncio.Task[Any]:
        task = self.loop.create_task(coro, name=name)
        self._tasks.append(task)
        return task

    async def async_add_executor_job(self, func: Callable[..., Any], *args: Any) -> Any:
        return func(*args)


def test_button_setup_skips_service_registration_when_platform_missing(
    monkeypatch: pytest.MonkeyPatch,
    deterministic_config_subentry_id: Callable[[Any, str, str | None], str],
) -> None:
    """The button platform skips service registration if the platform is missing."""

    del (
        deterministic_config_subentry_id
    )  # fixture side effects patch ensure_config_subentry_id

    _ensure_button_dependencies()
    button_module = importlib.import_module("custom_components.googlefindmy.button")

    class _StubCoordinator(button_module.GoogleFindMyCoordinator):
        """Coordinator stub mimicking the runtime data contract."""

        def __init__(
            self,
            hass: Any,
            config_entry: GoogleFindMyConfigEntryStub,
            devices: list[dict[str, Any]],
        ) -> None:
            self.hass = hass
            self.config_entry = config_entry
            self.data = devices
            self._listeners: list[Callable[[], None]] = []
            self._subentry_key = TRACKER_SUBENTRY_KEY

        def async_add_listener(
            self, listener: Callable[[], None]
        ) -> Callable[[], None]:
            self._listeners.append(listener)
            return lambda: None

        async def async_request_refresh(
            self,
        ) -> None:  # pragma: no cover - compatibility hook
            return None

        def stable_subentry_identifier(
            self, *, key: str | None = None, feature: str | None = None
        ) -> str:
            assert key is not None
            return f"{key}-identifier"

        def get_subentry_metadata(
            self, *, key: str | None = None, feature: str | None = None
        ) -> Any:
            if key is not None:
                resolved = key
            elif feature in {"button", "device_tracker", "sensor"}:
                resolved = TRACKER_SUBENTRY_KEY
            elif feature == "binary_sensor":
                resolved = SERVICE_SUBENTRY_KEY
            else:
                resolved = TRACKER_SUBENTRY_KEY
            return SimpleNamespace(key=resolved)

        def get_subentry_snapshot(
            self, key: str | None = None, *, feature: str | None = None
        ) -> list[dict[str, Any]]:
            return list(self.data)

        def is_device_visible_in_subentry(
            self, subentry_key: str, device_id: str
        ) -> bool:
            return True

    loop = asyncio.new_event_loop()

    hass = _StubHass(loop, button_module.DOMAIN)
    config_entry = GoogleFindMyConfigEntryStub()

    coordinator = _StubCoordinator(
        hass,
        config_entry,
        devices=[{"id": "device-1", "name": " Demo "}],
    )
    config_entry.runtime_data = coordinator

    added_entities: list[Any] = []

    def _async_add_entities(
        entities: list[Any], _update_before_add: bool = False, **kwargs: Any
    ) -> None:
        del _update_before_add, kwargs
        added_entities.extend(entities)

    monkeypatch.setattr(
        button_module.entity_platform,
        "async_get_current_platform",
        lambda: None,
    )

    recorded_calls: list[bool] = []

    def _record_call(*_: Any, **__: Any) -> None:
        recorded_calls.append(True)

    monkeypatch.setattr(
        "custom_components.googlefindmy.util_services.register_entity_service",
        _record_call,
    )

    try:
        loop.run_until_complete(
            button_module.async_setup_entry(hass, config_entry, _async_add_entities)
        )
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        drain_loop(loop)

    assert [entity.entity_description.translation_key for entity in added_entities] == [
        "play_sound",
        "stop_sound",
        "locate_device",
    ]
    assert recorded_calls == []


@pytest.mark.asyncio
async def test_sound_buttons_surface_service_validation_errors() -> None:
    """A translated user message must not die in the log.

    The Stop Sound button is the primary path for BSkando#195: it is where a
    user learns that the ring could not be correlated and may keep playing.
    That message exists in ten languages, and a blanket `except Exception`
    turned every one of them into a log line nobody reads. Play Sound carries
    the same class of message and is pinned alongside it.
    """

    button_module = importlib.import_module("custom_components.googlefindmy.button")

    class _RaisingServices:
        async def async_call(self, *args: Any, **kwargs: Any) -> None:
            raise button_module.ServiceValidationError("translated user message")

    for cls_name in ("GoogleFindMyStopSoundButton", "GoogleFindMyPlaySoundButton"):
        base = getattr(button_module, cls_name)

        # Subclass rather than patch: mutating the production class would leak
        # the stubbed gates into every later test in the session.
        class _Pressable(base):  # type: ignore[misc, valid-type]
            device_id = "device-1"
            available = True

            def device_label(self) -> str:
                return "Device 1"

            def _update_last_pressed(self) -> None:
                return None

        button = _Pressable.__new__(_Pressable)
        button.hass = SimpleNamespace(services=_RaisingServices())  # type: ignore[attr-defined]

        with pytest.raises(button_module.ServiceValidationError):
            await button.async_press()


def _pressable(base: type) -> type:
    """Return a subclass with the availability gates stubbed out."""

    # Subclass rather than patch: mutating the production class would leak
    # the stubbed gates into every later test in the session.
    class _Pressable(base):  # type: ignore[misc, valid-type]
        device_id = "device-1"
        available = True

        def device_label(self) -> str:
            return "Device 1"

        def _update_last_pressed(self) -> None:
            return None

    return _Pressable


@pytest.mark.asyncio
async def test_buttons_forward_the_press_context_to_the_service() -> None:
    """The service checks the permissions of the user who pressed the button."""

    button_module = importlib.import_module("custom_components.googlefindmy.button")
    sentinel = SimpleNamespace(user_id="user-1")

    for cls_name in (
        "GoogleFindMyPlaySoundButton",
        "GoogleFindMyStopSoundButton",
        "GoogleFindMyLocateButton",
    ):
        recorded: list[dict[str, Any]] = []

        class _RecordingServices:
            async def async_call(self, *args: Any, **kwargs: Any) -> None:
                recorded.append(kwargs)

        cls = _pressable(getattr(button_module, cls_name))
        button = cls.__new__(cls)
        button.hass = SimpleNamespace(services=_RecordingServices())  # type: ignore[attr-defined]
        button._context = sentinel  # type: ignore[attr-defined]

        await button.async_press()

        assert len(recorded) == 1, cls_name
        assert recorded[0]["context"] is sentinel, cls_name
        assert recorded[0]["context"].user_id == "user-1", cls_name
        if cls_name == "GoogleFindMyLocateButton":
            # The locate button stays fire-and-forget for a responsive UI.
            assert recorded[0]["blocking"] is False


@pytest.mark.asyncio
async def test_sound_buttons_surface_permission_rejections() -> None:
    """A permission rejection reaches the person pressing, not the log."""

    button_module = importlib.import_module("custom_components.googlefindmy.button")
    unauthorized = button_module.Unauthorized
    # Guard against a module-local class of the same name: it must be Home
    # Assistant's class or one of the test doubles standing in for it.
    assert unauthorized.__name__ == "Unauthorized"
    assert unauthorized.__module__ in {
        "homeassistant.exceptions",
        "tests.conftest",
        "tests.helpers.service_doubles",
    }

    class _RaisingServices:
        async def async_call(self, *args: Any, **kwargs: Any) -> None:
            raise unauthorized()

    for cls_name in ("GoogleFindMyStopSoundButton", "GoogleFindMyPlaySoundButton"):
        cls = _pressable(getattr(button_module, cls_name))
        button = cls.__new__(cls)
        button.hass = SimpleNamespace(services=_RaisingServices())  # type: ignore[attr-defined]
        button._context = None  # type: ignore[attr-defined]

        with pytest.raises(unauthorized):
            await button.async_press()
