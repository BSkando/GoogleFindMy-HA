# tests/test_google_home_filter.py
"""Regression coverage for the Google Home passive-zone logic."""

from __future__ import annotations

import logging
import sys
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from custom_components.googlefindmy.const import (
    OPT_GOOGLE_HOME_FILTER_ENABLED,
    OPT_GOOGLE_HOME_FILTER_KEYWORDS,
)
from tests.helpers import install_homeassistant_core_callback_stub


class _FakeState(SimpleNamespace):
    """Minimal stand-in for ``homeassistant.core.State``."""

    def __init__(self, entity_id: str, state: str, attributes: dict[str, Any]):
        super().__init__(entity_id=entity_id, state=state, attributes=attributes)


_FILTER_LOGGER = "custom_components.googlefindmy.google_home_filter"


def _assert_label_only_at_debug(
    records: list[logging.LogRecord], device_id: str, label: str
) -> None:
    """The record above DEBUG names the device id; only DEBUG names the label.

    The filter runs on every location update, so its records repeat
    unattended (``AGENTS.md`` section 5 (b)); a Google Home label is a
    user-chosen name.
    """

    own = [rec for rec in records if rec.name == _FILTER_LOGGER]
    above = [rec for rec in own if rec.levelno >= logging.INFO]
    assert above, [rec.getMessage() for rec in own]
    assert all(label not in rec.getMessage() for rec in above), [
        rec.getMessage() for rec in above
    ]
    assert any(device_id in rec.getMessage() for rec in above)
    assert any(
        rec.levelno == logging.DEBUG and label in rec.getMessage() for rec in own
    ), [rec.getMessage() for rec in own]


def _ensure_core_state_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Install a lightweight ``homeassistant.core`` module if missing."""

    core_module = install_homeassistant_core_callback_stub(monkeypatch)

    if not hasattr(core_module, "State"):
        monkeypatch.setattr(core_module, "State", _FakeState, raising=False)

    return core_module


def _ensure_zone_module() -> None:
    """Provide the ``homeassistant.components.zone`` module."""

    components_pkg = sys.modules.get("homeassistant.components")
    if components_pkg is None:
        components_pkg = ModuleType("homeassistant.components")
        components_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["homeassistant.components"] = components_pkg

    zone_module = sys.modules.get("homeassistant.components.zone")
    if zone_module is None:
        zone_module = ModuleType("homeassistant.components.zone")
        zone_module.DOMAIN = "zone"
        sys.modules["homeassistant.components.zone"] = zone_module
        setattr(components_pkg, "zone", zone_module)


def _ensure_helpers_modules() -> ModuleType:
    """Create ``homeassistant.helpers`` and its entity registry module."""

    helpers_pkg = sys.modules.get("homeassistant.helpers")
    if helpers_pkg is None:
        helpers_pkg = ModuleType("homeassistant.helpers")
        helpers_pkg.__path__ = []  # type: ignore[attr-defined]
        sys.modules["homeassistant.helpers"] = helpers_pkg

    registry_module = sys.modules.get("homeassistant.helpers.entity_registry")
    if registry_module is None:
        registry_module = ModuleType("homeassistant.helpers.entity_registry")
        sys.modules["homeassistant.helpers.entity_registry"] = registry_module
        setattr(helpers_pkg, "entity_registry", registry_module)

    return registry_module


def _ensure_event_helper_module() -> ModuleType:
    """Return (or install) ``homeassistant.helpers.event`` for monkeypatching."""

    module = sys.modules.get("homeassistant.helpers.event")
    if module is None:
        module = ModuleType("homeassistant.helpers.event")
        sys.modules["homeassistant.helpers.event"] = module
    if not hasattr(module, "async_track_state_change_event"):
        module.async_track_state_change_event = lambda *_args, **_kwargs: None  # type: ignore[attr-defined]
    return module


class _FakeStates:
    """Map-based Home Assistant state container used for tests."""

    def __init__(self, mapping: dict[str, _FakeState]):
        self._mapping = mapping

    def get(self, entity_id: str) -> _FakeState | None:
        return self._mapping.get(entity_id)

    def async_all(self, domain: str | None = None) -> list[_FakeState]:
        if domain is None:
            return list(self._mapping.values())
        prefix = f"{domain}."
        return [state for key, state in self._mapping.items() if key.startswith(prefix)]


def test_should_filter_detection_when_zone_passive(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Passive ``zone.home`` prevents substitution for Google Home detections."""

    _ensure_core_state_module(monkeypatch)
    _ensure_zone_module()
    registry_module = _ensure_helpers_modules()
    event_module = _ensure_event_helper_module()

    from custom_components.googlefindmy.google_home_filter import GoogleHomeFilter

    monkeypatch.setattr(
        registry_module,
        "async_get",
        lambda _hass: SimpleNamespace(
            async_get_entity_id=lambda *_args, **_kwargs: None
        ),
        raising=False,
    )
    monkeypatch.setattr(
        event_module,
        "async_track_state_change_event",
        lambda *_args, **_kwargs: None,
        raising=False,
    )
    monkeypatch.setattr(
        GoogleHomeFilter,
        "is_device_at_home",
        lambda self, _device_id: False,
    )

    zone_state = _FakeState(
        entity_id="zone.home",
        state="zoning",
        attributes={
            "latitude": 1.23,
            "longitude": 4.56,
            "passive": True,
        },
    )

    hass = SimpleNamespace()
    hass.states = _FakeStates({"zone.home": zone_state})
    hass.data = {}

    filter_config = {
        OPT_GOOGLE_HOME_FILTER_ENABLED: True,
        OPT_GOOGLE_HOME_FILTER_KEYWORDS: ["nest"],
    }

    gh_filter = GoogleHomeFilter(hass, filter_config)
    caplog.set_level(logging.DEBUG, logger=_FILTER_LOGGER)
    caplog.clear()

    assert gh_filter.should_filter_detection("device-1", "Nest Speaker") == (
        False,
        None,
    )
    _assert_label_only_at_debug(caplog.records, "device-1", "Nest Speaker")


def test_should_filter_detection_substitutes_home_coordinates(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Active ``zone.home`` coordinates substitute semantic Google Home detections."""

    _ensure_core_state_module(monkeypatch)
    _ensure_zone_module()
    registry_module = _ensure_helpers_modules()
    event_module = _ensure_event_helper_module()

    from custom_components.googlefindmy.google_home_filter import GoogleHomeFilter

    monkeypatch.setattr(
        registry_module,
        "async_get",
        lambda _hass: SimpleNamespace(
            async_get_entity_id=lambda *_args, **_kwargs: None
        ),
        raising=False,
    )
    monkeypatch.setattr(
        event_module,
        "async_track_state_change_event",
        lambda *_args, **_kwargs: None,
        raising=False,
    )
    monkeypatch.setattr(
        GoogleHomeFilter,
        "is_device_at_home",
        lambda self, _device_id: False,
    )

    zone_state = _FakeState(
        entity_id="zone.home",
        state="zoning",
        attributes={
            "latitude": 1.23,
            "longitude": 4.56,
            "radius": 10.0,
            "passive": False,
        },
    )

    hass = SimpleNamespace()
    hass.states = _FakeStates({"zone.home": zone_state})
    hass.data = {}

    filter_config = {
        OPT_GOOGLE_HOME_FILTER_ENABLED: True,
        OPT_GOOGLE_HOME_FILTER_KEYWORDS: ["nest"],
    }

    gh_filter = GoogleHomeFilter(hass, filter_config)
    caplog.set_level(logging.DEBUG, logger=_FILTER_LOGGER)
    caplog.clear()

    assert gh_filter.should_filter_detection("device-1", "Nest Speaker") == (
        False,
        {"latitude": 1.23, "longitude": 4.56, "radius": 10.0},
    )
    _assert_label_only_at_debug(caplog.records, "device-1", "Nest Speaker")

    # Second call with literal Home exercises the spam debounce path.
    assert gh_filter.should_filter_detection("device-1", "Home") == (True, None)
