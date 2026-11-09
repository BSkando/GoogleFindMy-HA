# tests/test_coordinator_update_failed_translation.py
"""The two ``UpdateFailed`` of ``_async_update_data`` are translatable.

Home Assistant copies ``translation_key`` of the cause into the setup-retry
reason of the integration card when the first refresh fails. Both raises in
``_async_update_data`` reach that path, so each must carry the integration's
domain and a key that ``strings.json`` defines, while ``str(err)`` keeps the
English wording the logs read.
"""

from __future__ import annotations

import asyncio
import json
import pathlib
import string
from collections.abc import Iterator
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.googlefindmy.const import DOMAIN
from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from custom_components.googlefindmy.coordinator.helpers.stats import FcmStatus
from tests.helpers import drain_loop
from tests.test_coordinator_status import (
    _DummyAPI,
    _DummyCache,
    _DummyEntry,
    _DummyHass,
)


def _template_placeholders(translation_key: str) -> set[str]:
    strings_path = (
        pathlib.Path(__file__).parents[1]
        / "custom_components"
        / "googlefindmy"
        / "strings.json"
    )
    message = json.loads(strings_path.read_text(encoding="utf-8"))["exceptions"][
        translation_key
    ]["message"]
    return {field for _, field, _, _ in string.Formatter().parse(message) if field}


@pytest.fixture
def dummy_api(monkeypatch: pytest.MonkeyPatch) -> _DummyAPI:
    """Provide a DummyAPI instance injected into the coordinator under test."""

    api = _DummyAPI()
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.main.GoogleFindMyAPI",
        lambda *_args, **_kwargs: api,
    )
    return api


@pytest.fixture
def coordinator(
    monkeypatch: pytest.MonkeyPatch, dummy_api: _DummyAPI
) -> Iterator[GoogleFindMyCoordinator]:
    """Instantiate a coordinator with lightweight stubs for hass and cache."""

    loop = asyncio.new_event_loop()
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.GoogleFindMyCoordinator._async_load_stats",
        AsyncMock(return_value=None),
    )
    coord = GoogleFindMyCoordinator(_DummyHass(loop), cache=_DummyCache())
    coord.config_entry = _DummyEntry()
    coord.async_set_updated_data = Mock()
    coord._async_build_device_snapshot_with_fallbacks = AsyncMock(return_value=[])
    coord._async_start_poll_cycle = AsyncMock()
    coord._ensure_registry_for_devices = lambda *_args, **_kwargs: 0
    coord._schedule_short_retry = lambda *_args, **_kwargs: None
    coord._get_ignored_set = set
    coord._is_fcm_ready_soft = lambda: True
    coord._set_fcm_status(FcmStatus.CONNECTED)
    yield coord
    drain_loop(loop)


def test_cold_start_empty_list_is_translatable(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """The cold-start guard names its key and keeps its message."""

    dummy_api.device_list = []
    assert coordinator._last_nonempty_wall == 0.0

    with pytest.raises(UpdateFailed) as excinfo:
        coordinator.hass.loop.run_until_complete(coordinator._async_update_data())

    err = excinfo.value
    assert str(err) == "Cold start: empty device list; treating as transient."
    assert err.translation_domain == DOMAIN
    assert err.translation_key == "cold_start_empty_device_list"
    assert err.translation_placeholders is None
    assert _template_placeholders("cold_start_empty_device_list") == set()


def test_unexpected_update_error_is_translatable(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """An unexpected error is wrapped with its class name as the placeholder."""

    async def _boom() -> list[dict[str, str]]:
        raise KeyError("device")

    dummy_api.async_get_basic_device_list = _boom  # type: ignore[method-assign]

    with pytest.raises(UpdateFailed) as excinfo:
        coordinator.hass.loop.run_until_complete(coordinator._async_update_data())

    err = excinfo.value
    assert str(err) == "KeyError: 'device'"
    assert isinstance(err.__cause__, KeyError)
    assert err.translation_domain == DOMAIN
    assert err.translation_key == "update_unexpected_error"
    assert err.translation_placeholders == {"error_type": "KeyError"}
    assert _template_placeholders("update_unexpected_error") == {"error_type"}
