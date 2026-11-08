# tests/test_poll_device_name_at_debug.py
"""The poll cycle names a device by its id above DEBUG and by its name at DEBUG.

`AGENTS.md` section 5 (b): a record that can repeat unattended keeps the
user-chosen device name out of the record at INFO and above. Every poll cycle
runs unattended, so each per-device error or status record of
``PollingOperations._async_start_poll_cycle`` names the device by its id and
a DEBUG companion record carries the name.

One case per branch: permanent SPOT auth failure, empty SPOT response,
permanent Nova auth failure, client error without a credential problem,
transient auth failure, any other exception, the Google Home spam filter, and
an update without location data. The setup follows
``tests/test_poll_timeout_hardening.py``. The shape of these calls is pinned by
``tests/test_guard_unattended_device_names.py``; this file pins the contents.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from custom_components.googlefindmy.NovaApi.nova_request import (
    NovaAuthError,
    NovaAuthPermanentError,
)
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
    SpotApiEmptyResponseError,
)
from custom_components.googlefindmy.SpotApi.spot_request import SpotAuthPermanentError
from tests.helpers import drain_loop
from tests.helpers.config_entries_stub import make_config_entry

_POLLING_LOGGER = "custom_components.googlefindmy.coordinator.polling"

# Injected from outside the code under test.
DEVICE_ID = "dev-kitchen"
DEVICE_NAME = "Kitchen Tag"


class _DummyCache:
    async def async_get_cached_value(self, _key: str) -> Any:  # pragma: no cover
        return None

    async def async_set_cached_value(self, _key: str, _value: Any) -> None:
        return None  # pragma: no cover


class _DummyBus:
    def async_listen(self, *_args: Any, **_kwargs: Any) -> Callable[[], None]:
        return lambda: None  # pragma: no cover

    def async_fire(self, *_args: Any, **_kwargs: Any) -> None:
        return None  # pragma: no cover


class _DummyHass:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.bus = _DummyBus()

    def async_create_task(self, coro: Any, *, name: str | None = None) -> Any:
        return self.loop.create_task(coro, name=name)


class _RaisingAPI:
    """API stub raising one exception for every location request."""

    def __init__(self, error: BaseException) -> None:
        self._error = error

    async def async_get_device_location(self, _dev_id: str, _dev_name: str) -> Any:
        raise self._error


class _LocationAPI:
    """API stub returning one fixed location payload."""

    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    async def async_get_device_location(self, dev_id: str, dev_name: str) -> Any:
        return {"id": dev_id, "name": dev_name, **self._payload}


class _SpamFilter:
    """Google Home filter stub that rejects every detection."""

    def should_filter_detection(
        self, _device_id: str, _semantic_name: str
    ) -> tuple[bool, None]:
        return True, None


def _make_coordinator(
    monkeypatch: pytest.MonkeyPatch,
    loop: asyncio.AbstractEventLoop,
    api: object,
    *,
    google_home_filter: object | None = None,
) -> GoogleFindMyCoordinator:
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.GoogleFindMyCoordinator._async_load_stats",
        AsyncMock(return_value=None),
    )
    coordinator = GoogleFindMyCoordinator(_DummyHass(loop), cache=_DummyCache())
    coordinator.config_entry = make_config_entry(entry_id="entry-id")
    coordinator.api = api
    coordinator._get_google_home_filter = lambda: google_home_filter
    coordinator._is_fcm_ready_soft = lambda: True
    coordinator._get_ignored_set = set
    coordinator._last_device_list = [{"id": DEVICE_ID, "name": DEVICE_NAME}]
    coordinator._request_poll_reauth = MagicMock()
    coordinator.data = []
    coordinator.last_update_success = True
    coordinator.last_exception = None

    def _set_update_error(exc: Exception) -> None:
        coordinator.last_update_success = False
        coordinator.last_exception = exc

    def _set_updated_data(data: Any) -> None:
        coordinator.data = data
        coordinator.last_update_success = True
        coordinator.last_exception = None

    coordinator.async_set_update_error = _set_update_error
    coordinator.async_set_updated_data = _set_updated_data
    return coordinator


def _run_cycle(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    api: object,
    *,
    google_home_filter: object | None = None,
) -> list[logging.LogRecord]:
    loop = asyncio.new_event_loop()
    coordinator = _make_coordinator(
        monkeypatch, loop, api, google_home_filter=google_home_filter
    )
    devices = [{"id": DEVICE_ID, "name": DEVICE_NAME}]
    try:
        with caplog.at_level(logging.DEBUG, logger=_POLLING_LOGGER):
            loop.run_until_complete(
                coordinator._async_start_poll_cycle(devices, force=True)
            )
    finally:
        drain_loop(loop)
    return [rec for rec in caplog.records if rec.name == _POLLING_LOGGER]


def _assert_id_above_debug_and_name_at_debug(
    records: list[logging.LogRecord], needle: str
) -> None:
    above = [rec for rec in records if rec.levelno >= logging.INFO]
    leaking = [rec.getMessage() for rec in above if DEVICE_NAME in rec.getMessage()]
    assert not leaking, leaking
    branch = [rec for rec in above if needle in rec.getMessage()]
    assert branch, [rec.getMessage() for rec in above]
    assert all(DEVICE_ID in rec.getMessage() for rec in branch), [
        rec.getMessage() for rec in branch
    ]
    assert any(
        rec.levelno == logging.DEBUG
        and DEVICE_NAME in rec.getMessage()
        and DEVICE_ID in rec.getMessage()
        for rec in records
    ), [rec.getMessage() for rec in records if rec.levelno == logging.DEBUG]


_ERROR_CASES = {
    "spot_auth_permanent": (SpotAuthPermanentError("expired"), "Authentication failed"),
    "spot_empty_response": (
        SpotApiEmptyResponseError("empty"),
        "Authentication failed",
    ),
    "nova_auth_permanent": (
        NovaAuthPermanentError(401, "expired"),
        "Permanent authentication failure",
    ),
    "client_error": (NovaAuthError(404, "gone"), "Client error (HTTP"),
    "transient_auth": (NovaAuthError(403, "denied"), "Transient auth failure"),
    "other_exception": (RuntimeError("boom"), "Failed to get location"),
}


@pytest.mark.parametrize("case", sorted(_ERROR_CASES))
def test_poll_error_branch_names_device_by_id(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, case: str
) -> None:
    """Each error branch of the poll cycle logs the id above DEBUG."""

    error, needle = _ERROR_CASES[case]
    records = _run_cycle(monkeypatch, caplog, _RaisingAPI(error))
    _assert_id_above_debug_and_name_at_debug(records, needle)


def test_google_home_spam_filter_names_device_by_id(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A filtered Google Home detection logs the id above DEBUG."""

    api = _LocationAPI(
        {
            "latitude": 1.0,
            "longitude": 1.0,
            "accuracy": 5.0,
            "semantic_name": "Living Room Speaker",
            "last_seen": time.time(),
        }
    )
    records = _run_cycle(monkeypatch, caplog, api, google_home_filter=_SpamFilter())
    _assert_id_above_debug_and_name_at_debug(records, "Google Home spam detection")


def test_update_without_location_names_device_by_id(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """An update without coordinates or semantic name logs the id above DEBUG."""

    api = _LocationAPI({"last_seen": time.time()})
    records = _run_cycle(monkeypatch, caplog, api)
    _assert_id_above_debug_and_name_at_debug(records, "No location data")
