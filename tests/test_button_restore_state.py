from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from homeassistant.util import dt as dt_util

from custom_components.googlefindmy.button import (
    GoogleFindMyLocateButton,
    GoogleFindMyPlaySoundButton,
)
from custom_components.googlefindmy.const import (
    DOMAIN,
    SERVICE_LOCATE_DEVICE,
    SERVICE_PLAY_SOUND,
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
)
from tests.helpers.config_entries_stub import make_config_entry


@pytest.mark.asyncio
async def test_button_restores_last_pressed(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restoring last state populates ``last_pressed`` when available."""

    hass = SimpleNamespace(data={DOMAIN: {}}, services=SimpleNamespace(), loop=None)
    coordinator = SimpleNamespace(
        hass=hass,
        config_entry=make_config_entry(entry_id="entry-id"),
        is_device_visible_in_subentry=lambda *_args: True,
        can_request_location=lambda _dev_id: True,
        async_request_refresh=AsyncMock(),
    )
    button = GoogleFindMyLocateButton(
        coordinator,
        {"id": "device-1", "name": "Tracker"},
        "Tracker",
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=f"{SERVICE_SUBENTRY_KEY}:tracker",
    )
    button.hass = hass
    button._handle_coordinator_update = lambda: None
    restored_state = SimpleNamespace(
        state="unknown", attributes={"last_press": "2025-01-02T03:04:05+00:00"}
    )
    monkeypatch.setattr(
        button, "async_get_last_state", AsyncMock(return_value=restored_state)
    )

    await button.async_added_to_hass()

    assert button._attr_last_pressed == datetime(
        2025, 1, 2, 3, 4, 5, tzinfo=dt_util.UTC
    )


@pytest.mark.asyncio
async def test_button_records_last_pressed_on_press() -> None:
    """Button presses update ``last_pressed`` so availability recovers cleanly."""

    service_call = AsyncMock()
    hass = SimpleNamespace(
        services=SimpleNamespace(async_call=service_call),
        data={DOMAIN: {}},
        loop=None,
    )
    coordinator = SimpleNamespace(
        hass=hass,
        config_entry=make_config_entry(entry_id="entry-id"),
        is_device_visible_in_subentry=lambda *_args: True,
        can_play_sound=lambda _dev_id: True,
        async_request_refresh=AsyncMock(),
    )
    button = GoogleFindMyPlaySoundButton(
        coordinator,
        {"id": "device-1", "name": "Tracker"},
        "Tracker",
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=f"{SERVICE_SUBENTRY_KEY}:tracker",
    )
    button.hass = hass

    await button.async_press()

    assert button._attr_last_pressed is not None
    service_call.assert_awaited_once_with(
        DOMAIN,
        SERVICE_PLAY_SOUND,
        {"device_id": "device-1"},
        blocking=True,
        context=button._context,
    )


@pytest.mark.asyncio
async def test_locate_button_does_not_claim_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The fire-and-forget locate button reports dispatch, never success.

    ``blocking=False`` means the service result is never observed at this call
    site, so an INFO line saying "Successfully submitted" asserts an outcome
    nobody looked at -- the same class of unbacked success claim that made the
    Stop Sound button report a stop for a ring that kept playing (BSkando#195).
    """

    service_call = AsyncMock()
    hass = SimpleNamespace(
        services=SimpleNamespace(async_call=service_call),
        data={DOMAIN: {}},
        loop=None,
    )
    coordinator = SimpleNamespace(
        hass=hass,
        config_entry=make_config_entry(entry_id="entry-id"),
        is_device_visible_in_subentry=lambda *_args: True,
        can_request_location=lambda _dev_id: True,
        async_request_refresh=AsyncMock(),
    )
    button = GoogleFindMyLocateButton(
        coordinator,
        {"id": "device-1", "name": "Tracker"},
        "Tracker",
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=f"{SERVICE_SUBENTRY_KEY}:tracker",
    )
    button.hass = hass

    with caplog.at_level("INFO", logger="custom_components.googlefindmy.button"):
        await button.async_press()

    service_call.assert_awaited_once_with(
        DOMAIN,
        SERVICE_LOCATE_DEVICE,
        {"device_id": "device-1"},
        blocking=False,
        context=button._context,
    )
    assert "Successfully" not in caplog.text
    assert "outcome not awaited" in caplog.text
