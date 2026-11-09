# tests/test_coordinator_status.py
"""Regression tests for coordinator status handling (API vs. FCM)."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import issue_registry as ir

from custom_components.googlefindmy._reauth_reason import ReauthReasonCode
from custom_components.googlefindmy.Auth.fcm_receiver_ha import (
    CRASH_LOOP_FATAL_PREFIX,
)
from custom_components.googlefindmy.binary_sensor import GoogleFindMyPollingSensor
from custom_components.googlefindmy.const import (
    CONF_GOOGLE_EMAIL,
    DOMAIN,
    EVENT_AUTH_ERROR,
    EVENT_AUTH_OK,
    ISSUE_AUTH_EXPIRED_KEY,
    SERVICE_SUBENTRY_KEY,
    TRACKER_SUBENTRY_KEY,
    issue_id_for,
)
from custom_components.googlefindmy.coordinator import (
    _FCM_FALLBACK_POLL_AFTER_S,
    ApiStatus,
    FcmStatus,
    GoogleFindMyCoordinator,
)
from custom_components.googlefindmy.coordinator.polling import (
    _MAX_TRANSIENT_AUTH_FAILURES,
)
from custom_components.googlefindmy.device_tracker import GoogleFindMyDeviceTracker
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
    StaleOwnerKeyError,
)
from custom_components.googlefindmy.NovaApi.nova_request import NovaAuthError
from tests.helpers import drain_loop


class _DummyBus:
    """Capture fired events for assertions."""

    def __init__(self) -> None:
        self.fired: list[tuple[str, dict | None]] = []

    def async_fire(self, event: str, data: dict | None = None) -> None:
        self.fired.append((event, data))


class _DummyConfigEntries:
    """Stub Home Assistant config_entries manager.

    The real ``ConfigEntries`` manager exposes no ``async_start_reauth``
    helper -- reauth is entry-scoped via ``ConfigEntry.async_start_reauth``.
    The stub therefore deliberately omits it: a stray production call to
    ``hass.config_entries.async_start_reauth(...)`` raises ``AttributeError``
    here exactly as it would at runtime, instead of being silently recorded
    by a fictional method.
    """

    def __init__(self) -> None:
        self.setup_calls: list[str] = []

    def async_get_subentries(self, _entry_id: str) -> list[Any]:
        return []

    async def async_setup(self, entry_id: str) -> bool:
        self.setup_calls.append(entry_id)
        return True


class _DummyEntry:
    """Minimal ConfigEntry stub with reauth and background-task helpers."""

    def __init__(self) -> None:
        self.entry_id = "entry-test"
        self.data = {CONF_GOOGLE_EMAIL: "user@example.com"}
        self.reauth_calls = 0

    def async_start_reauth(self, hass) -> None:  # noqa: D401 - stub signature
        # Mirror the real ``ConfigEntry.async_start_reauth``: a synchronous
        # ``@callback`` returning ``None`` (an async stub would mask a stray
        # ``await`` in production code).
        self.reauth_calls += 1

    def async_create_background_task(self, hass, coro, *, name: str):
        """Forward polling to the harness's existing task scheduler."""
        return hass.async_create_task(coro, name=name)


class _DummyHass:
    """Minimal Home Assistant stub for coordinator tests."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.bus = _DummyBus()
        self.config_entries = _DummyConfigEntries()
        self.data: dict[str, dict] = {DOMAIN: {}}

    def async_create_task(self, coro, *, name: str | None = None):  # noqa: D401 - stub
        return self.loop.create_task(coro, name=name)


class _DummyAPI:
    """Minimal API stub implementing the methods touched in tests."""

    def __init__(self) -> None:
        self.raise_auth = False
        self.device_list: list[dict[str, str]] = []
        self.fcm: Any | None = None

    async def async_get_basic_device_list(self) -> list[dict[str, str]]:
        if self.raise_auth:
            raise ConfigEntryAuthFailed("Invalid auth token")
        return list(self.device_list)

    def is_push_ready(self) -> bool:
        return True


class _DummyCache:
    """Entry-scoped cache stub providing async get/set helpers."""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    async def async_get_cached_value(self, key: str) -> Any:
        """Return a stored value or None when absent."""

        return self._store.get(key)

    async def async_set_cached_value(self, key: str, value: Any) -> None:
        """Persist a value in the in-memory store."""

        self._store[key] = value

    async def get(self, key: str) -> Any:  # pragma: no cover - legacy compatibility
        """Compatibility alias for older tests still awaiting get()."""

        return await self.async_get_cached_value(key)


@pytest.fixture
def dummy_api(monkeypatch: pytest.MonkeyPatch) -> _DummyAPI:
    """Provide a DummyAPI instance injected into the coordinator under test."""

    api = _DummyAPI()

    def _factory(*_args, **_kwargs) -> _DummyAPI:
        return api

    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.main.GoogleFindMyAPI",
        _factory,
    )
    return api


@pytest.fixture
def coordinator(
    monkeypatch: pytest.MonkeyPatch, dummy_api: _DummyAPI
) -> GoogleFindMyCoordinator:
    """Instantiate a coordinator with lightweight stubs for hass/cache."""

    loop = asyncio.new_event_loop()
    hass = _DummyHass(loop)
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.GoogleFindMyCoordinator._async_load_stats",
        AsyncMock(return_value=None),
    )
    coord = GoogleFindMyCoordinator(hass, cache=_DummyCache())
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


def test_api_auth_error_preserves_fcm_status(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """ConfigEntryAuthFailed surfaces while keeping push transport marked connected."""

    dummy_api.raise_auth = True

    loop = coordinator.hass.loop
    with pytest.raises(ConfigEntryAuthFailed):
        loop.run_until_complete(coordinator._async_update_data())

    assert coordinator.api_status.state == ApiStatus.REAUTH
    assert coordinator.fcm_status.state == FcmStatus.CONNECTED
    # The coordinator must not start reauth manually; it relies on HA's
    # automatic reauth triggered by raising ConfigEntryAuthFailed. Only the
    # entry-level call exists in the real API, so that is what we guard. The
    # manager has no async_start_reauth, so a stray manager call would raise
    # AttributeError above instead of passing silently.
    assert coordinator.config_entry.reauth_calls == 0
    assert "Invalid" in (coordinator.api_status.reason or "")


def test_fatal_fcm_registration_triggers_reauth(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """A fatal FCM registration error escalates to ConfigEntryAuthFailed."""

    fatal_error = "GCM Registration failed (404): Credentials invalid"

    class _DummyFcm:
        def __init__(self, message: str) -> None:
            self._fatal_error = message
            self._fatal_errors = {coordinator.config_entry.entry_id: message}

    dummy_api.fcm = _DummyFcm(fatal_error)
    coordinator.config_entry.runtime_data = SimpleNamespace(
        coordinator=coordinator, fcm_receiver=dummy_api.fcm
    )
    coordinator._is_fcm_ready_soft = lambda: False

    loop = coordinator.hass.loop
    with pytest.raises(ConfigEntryAuthFailed):
        loop.run_until_complete(coordinator._async_update_data())

    assert coordinator.auth_error_active is True
    assert coordinator._auth_error_message == fatal_error
    assert coordinator.hass.bus.fired[-1] == (
        EVENT_AUTH_ERROR,
        {
            "entry_id": coordinator.config_entry.entry_id,
            "email": coordinator._get_account_email(),
            "message": fatal_error,
        },
    )


def test_threshold_fcm_fatal_tags_reauth_code(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """FIX 3: a non-immediate FCM fatal that persists to the retry threshold
    escalates to ConfigEntryAuthFailed tagged with FCM_AUTH_FATAL."""

    # Non-auth, non-crash-loop fatal ⇒ counter path, not the immediate raise.
    fatal_error = "Transient FCM error: connection reset"

    class _DummyFcm:
        def __init__(self, message: str) -> None:
            self._fatal_error = message
            self._fatal_errors = {coordinator.config_entry.entry_id: message}

    dummy_api.fcm = _DummyFcm(fatal_error)
    coordinator.config_entry.runtime_data = SimpleNamespace(
        coordinator=coordinator, fcm_receiver=dummy_api.fcm
    )
    coordinator._is_fcm_ready_soft = lambda: False
    # One short of the threshold; the same recurring fatal tips it over.
    coordinator._fcm_last_error = fatal_error
    coordinator._fcm_error_count = 2

    loop = coordinator.hass.loop
    with pytest.raises(ConfigEntryAuthFailed) as excinfo:
        loop.run_until_complete(coordinator._async_update_data())

    # Mutation-sharp: the wrong (or missing) tag fails this identity check.
    assert excinfo.value.reauth_code is ReauthReasonCode.FCM_AUTH_FATAL


def test_global_fatal_error_ignored_for_clean_entry(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """A global fatal flag should not trip reauth when entry-scoped errors are clear."""

    fatal_error = "Global fail"

    class _DummyFcm:
        def __init__(self, message: str) -> None:
            self._fatal_error = message
            self._fatal_errors: dict[str, str] = {}

    dummy_api.fcm = _DummyFcm(fatal_error)
    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator.config_entry.runtime_data = SimpleNamespace(
        coordinator=coordinator, fcm_receiver=dummy_api.fcm
    )

    loop = coordinator.hass.loop
    result = loop.run_until_complete(coordinator._async_update_data())

    assert result == []
    assert coordinator.auth_error_active is False
    assert coordinator.hass.bus.fired == []


# --------------------------------------------------------------------------
# AP7 / CA-6: re-auth-escalation dispatch wiring for the crash-loop fatal.
#
# cov-integ's ``test_coordinator_polling_fatal_channel.py`` only pins the
# pure ``_classify_fcm_fatal`` function (RV-G3q scenarios a+b). It never
# drives ``_async_update_data``, so a regression that re-wires the
# CRASH_LOOP_EXCLUDED branch back into the re-auth path (the #1086
# channel-confusion bug, RV-G3q) would not turn any cov-integ test red.
# These two tests pin the consumer-branch wiring (RV-G3q scenarios c+d):
#   (c) no ConfigEntryAuthFailed / no auth-error flip for a cap fatal;
#   (d) a crash-loop fatal resets a stale transient auth-error count.
# --------------------------------------------------------------------------


def test_crash_loop_fatal_excluded_from_reauth_escalation(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """AP7/CA-6 (c): a supervisor short-run-cap fatal must NOT escalate to re-auth.

    The FCM receiver's poison-message cap publishes its terminal state into
    the same ``_fatal_errors`` map the re-auth escalation consumes. Polling
    ``_async_update_data`` well past ``_FCM_ERROR_RETRY_THRESHOLD`` with a
    ``CRASH_LOOP_FATAL_PREFIX`` fatal must never raise ConfigEntryAuthFailed
    nor flip the auth-error state.
    """
    fatal_error = f"{CRASH_LOOP_FATAL_PREFIX} entry abc stopped after 10 short runs"

    class _DummyFcm:
        def __init__(self, message: str) -> None:
            self._fatal_error = message
            self._fatal_errors = {coordinator.config_entry.entry_id: message}

    dummy_api.fcm = _DummyFcm(fatal_error)
    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator.config_entry.runtime_data = SimpleNamespace(
        coordinator=coordinator, fcm_receiver=dummy_api.fcm
    )
    coordinator._is_fcm_ready_soft = lambda: False

    loop = coordinator.hass.loop
    # Five cycles is comfortably past the threshold (3); no escalation allowed.
    for _ in range(5):
        result = loop.run_until_complete(coordinator._async_update_data())
        assert result == []

    assert coordinator.auth_error_active is False
    assert coordinator.hass.bus.fired == []
    assert coordinator._fcm_error_count == 0


def test_crash_loop_fatal_resets_stale_auth_error_count(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """AP7/CA-6 (d): a crash-loop fatal resets a stale transient auth-error count.

    A partial count left over from earlier transient (non-cap) auth fatals
    must not let a later real auth fatal inherit the crash-loop window's
    count and escalate prematurely.
    """
    fatal_error = f"{CRASH_LOOP_FATAL_PREFIX} entry abc stopped after 10 short runs"

    class _DummyFcm:
        def __init__(self, message: str) -> None:
            self._fatal_error = message
            self._fatal_errors = {coordinator.config_entry.entry_id: message}

    dummy_api.fcm = _DummyFcm(fatal_error)
    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator.config_entry.runtime_data = SimpleNamespace(
        coordinator=coordinator, fcm_receiver=dummy_api.fcm
    )
    coordinator._is_fcm_ready_soft = lambda: False
    # Simulate two prior transient (non-cap) auth-error cycles.
    coordinator._fcm_error_count = 2
    coordinator._fcm_last_error = "GCM Registration failed (401): transient"

    loop = coordinator.hass.loop
    result = loop.run_until_complete(coordinator._async_update_data())

    assert result == []
    assert coordinator._fcm_error_count == 0
    assert coordinator._fcm_last_error is None
    assert coordinator.auth_error_active is False


def test_api_status_recovers_after_success(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """Successful polling resets API status and clears the auth error flag."""

    # First, simulate a failure to set reauth state.
    dummy_api.raise_auth = True
    loop = coordinator.hass.loop
    with pytest.raises(ConfigEntryAuthFailed):
        loop.run_until_complete(coordinator._async_update_data())

    # Next, simulate a successful refresh.
    dummy_api.raise_auth = False
    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._async_build_device_snapshot_with_fallbacks.return_value = []

    result = loop.run_until_complete(coordinator._async_update_data())

    assert result == []
    assert coordinator.api_status.state == ApiStatus.OK
    assert coordinator.api_status.reason is None
    assert coordinator.fcm_status.state == FcmStatus.CONNECTED
    assert coordinator.auth_error_active is False


def test_successful_update_clears_lingering_repair_issue(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """A successful poll clears lingering Repairs issues after a restart."""

    hass = coordinator.hass
    entry = coordinator.config_entry
    issue_id = issue_id_for(entry.entry_id)

    ir.async_create_issue(
        hass,
        DOMAIN,
        issue_id,
        is_fixable=True,
        severity=ir.IssueSeverity.ERROR,
        translation_key=ISSUE_AUTH_EXPIRED_KEY,
        translation_placeholders={"email": "user@example.com"},
    )

    registry = ir.async_get(hass)
    assert registry.async_get_issue(DOMAIN, issue_id) is not None

    loop = hass.loop
    dummy_api.raise_auth = False
    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._async_build_device_snapshot_with_fallbacks.return_value = []

    coordinator.data = []
    coordinator.async_set_updated_data.reset_mock()
    called_flag = {"value": False}

    def _mark_called(*_args, **_kwargs) -> None:
        called_flag["value"] = True

    coordinator.async_set_updated_data.side_effect = _mark_called
    result = loop.run_until_complete(coordinator._async_update_data())

    assert result == []
    assert registry.async_get_issue(DOMAIN, issue_id) is None
    ok_events = [event for event, _data in hass.bus.fired if event == EVENT_AUTH_OK]
    assert ok_events == [EVENT_AUTH_OK]
    assert coordinator.async_set_updated_data.called, (
        "Listeners should be notified after clearing issue"
    )
    assert called_flag["value"]


def test_push_updated_keeps_known_name_for_blank_snapshots(
    monkeypatch: pytest.MonkeyPatch,
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """Ensure cached display names survive blank device snapshots and push updates."""

    # Let the coordinator hand the raw device list through to the snapshot builder.
    coordinator._async_build_device_snapshot_with_fallbacks = AsyncMock(
        side_effect=lambda devices: devices
    )

    loop = coordinator.hass.loop
    dummy_api.device_list = [{"id": "dev-1", "name": "Pixel 9"}]
    loop.run_until_complete(coordinator._async_update_data())

    # Simulate a follow-up list without a usable name; cache should preserve the old label.
    dummy_api.device_list = [{"id": "dev-1", "name": ""}]
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._device_names["dev-1"] == "Pixel 9"

    # Prepare cached location data to make push_updated build a rich snapshot.
    now = time.time()
    coordinator._device_location_data["dev-1"] = {
        "device_id": "dev-1",
        "name": "Pixel 9",
        "latitude": 37.0,
        "longitude": -122.0,
        "accuracy": 5,
        "last_updated": now,
    }

    captured: list[list[dict[str, Any]]] = []

    def _capture(snapshot: list[dict[str, Any]]) -> None:
        captured.append(snapshot)
        coordinator.data = snapshot

    coordinator.async_set_updated_data = _capture
    coordinator._is_on_hass_loop = lambda: True

    # Push a snapshot while the latest API payload still lacks a display name.
    coordinator.push_updated(["dev-1"])

    assert captured, "push_updated should publish a snapshot"
    snapshot = captured[-1]
    assert snapshot[0]["name"] == "Pixel 9"

    # Entities should continue to expose the persisted display name.
    monkeypatch.setattr(
        "custom_components.googlefindmy.entity.GoogleFindMyEntity.maybe_update_device_registry_name",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy._opt",
        lambda _entry, _key, default=None: default,
        raising=False,
    )

    subentry_identifier = coordinator.stable_subentry_identifier(
        key=TRACKER_SUBENTRY_KEY
    )
    entity = GoogleFindMyDeviceTracker(
        coordinator,
        {"id": "dev-1", "name": "Pixel 9"},
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=subentry_identifier,
    )
    entity.hass = coordinator.hass
    entity.entity_id = "device_tracker.googlefindmy_dev_1"
    entity._handle_coordinator_update()

    # With has_entity_name=True, _attr_name is None; the entity name is derived
    # from the device registry. The coordinator cache preserves the display name.
    assert entity._attr_name is None
    assert entity._attr_has_entity_name is True
    assert entity.subentry_key == TRACKER_SUBENTRY_KEY
    assert subentry_identifier in entity.unique_id


def test_device_tracker_respects_coordinator_unavailability(
    coordinator: GoogleFindMyCoordinator,
) -> None:
    """Availability should mirror coordinator health before device checks."""

    coordinator.is_device_visible_in_subentry = lambda *_args, **_kwargs: True
    coordinator.get_device_location_data_for_subentry = lambda *_args, **_kwargs: {
        "latitude": 0.0,
        "longitude": 0.0,
    }
    coordinator.is_device_present = lambda _dev_id: True

    subentry_identifier = coordinator.stable_subentry_identifier(
        key=TRACKER_SUBENTRY_KEY
    )
    tracker = GoogleFindMyDeviceTracker(
        coordinator,
        {"id": "dev-1", "name": "Pixel 9"},
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=subentry_identifier,
    )
    tracker.hass = coordinator.hass
    tracker.entity_id = "device_tracker.googlefindmy_dev_1"

    coordinator._last_update_success = False
    assert tracker.available is False

    coordinator._last_update_success = True
    assert tracker.available is True


def test_polling_sensor_inherits_coordinator_availability(
    coordinator: GoogleFindMyCoordinator,
) -> None:
    """Diagnostic polling sensor availability follows the coordinator."""

    subentry_identifier = coordinator.stable_subentry_identifier(
        key=SERVICE_SUBENTRY_KEY
    )
    polling = GoogleFindMyPollingSensor(
        coordinator,
        _DummyEntry(),
        subentry_key=SERVICE_SUBENTRY_KEY,
        subentry_identifier=subentry_identifier,
    )
    polling.hass = coordinator.hass

    coordinator._last_update_success = False
    assert polling.available is False

    coordinator._last_update_success = True
    assert polling.available is True


def test_poll_snapshot_reuses_cached_name_for_blank_payload(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """Poll snapshots should reuse cached names when payload omits them."""

    loop = coordinator.hass.loop
    coordinator._async_build_device_snapshot_with_fallbacks = AsyncMock(
        side_effect=lambda devices: devices,
    )

    dummy_api.device_list = [{"id": "dev-1", "name": "Pixel 9"}]
    initial_snapshot = loop.run_until_complete(coordinator._async_update_data())
    assert initial_snapshot[0]["name"] == "Pixel 9"

    dummy_api.device_list = [{"id": "dev-1", "name": ""}]
    follow_up_snapshot = loop.run_until_complete(coordinator._async_update_data())

    assert follow_up_snapshot[0]["name"] == "Pixel 9"


def test_poll_cycle_forces_after_fcm_timeout(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """After the FCM grace window expires, polling should proceed in degraded mode."""

    loop = coordinator.hass.loop
    coordinator._async_build_device_snapshot_with_fallbacks.return_value = []
    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]

    coordinator._enabled_poll_device_ids = {"dev-1"}
    coordinator._is_fcm_ready_soft = lambda: False
    coordinator._fcm_defer_started_mono = time.monotonic() - (
        _FCM_FALLBACK_POLL_AFTER_S + 5
    )
    coordinator._last_poll_mono = time.monotonic() - (
        coordinator.location_poll_interval + 1
    )
    coordinator._async_start_poll_cycle.reset_mock()

    result = loop.run_until_complete(coordinator._async_update_data())

    assert result == []
    coordinator._async_start_poll_cycle.assert_awaited()
    call = coordinator._async_start_poll_cycle.await_args
    assert call is not None
    assert call.args and call.args[0][0]["id"] == "dev-1"
    assert call.kwargs.get("force") is True


def test_update_skips_devices_without_valid_id(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Invalid or duplicate device entries should be ignored during updates."""

    caplog.set_level("DEBUG")
    coordinator._async_build_device_snapshot_with_fallbacks.return_value = []
    dummy_api.device_list = [
        {},
        {"id": 123, "name": "Numeric identifier"},
        {"id": "  dev-1  ", "name": "Pixel 9"},
        {"id": "dev-1", "name": "Pixel 9 duplicate"},
        {"id": "dev-2", "name": "Pixel 9 Pro"},
    ]

    loop = coordinator.hass.loop
    result = loop.run_until_complete(coordinator._async_update_data())

    assert result == []

    await_args = coordinator._async_build_device_snapshot_with_fallbacks.await_args
    assert await_args is not None
    visible_devices = list(await_args.args[0])
    assert [dev["id"] for dev in visible_devices] == ["dev-1", "dev-2"]
    assert coordinator._present_device_ids == {"dev-1", "dev-2"}
    assert any(
        "Skipping device without valid id" in record.message
        for record in caplog.records
    )
    assert any(
        "Skipping duplicate device entry for id=dev-1" in record.message
        for record in caplog.records
    )


# ---------------------------------------------------------------------------
# Regression tests for push_updated() merge semantics (PR #167 + F-9).
# T1: push merges with self.data, T2: pushed device overwrites only itself,
# T3: subentry index sees the merged_snapshot (F-9, prevents stale index).
# ---------------------------------------------------------------------------


def _prime_push_updated(
    coordinator: GoogleFindMyCoordinator,
) -> list[list[dict[str, Any]]]:
    """Patch coordinator to capture snapshots published by push_updated()."""

    captured: list[list[dict[str, Any]]] = []

    def _capture(snapshot: list[dict[str, Any]]) -> None:
        captured.append(snapshot)
        coordinator.data = snapshot

    coordinator.async_set_updated_data = _capture
    coordinator._is_on_hass_loop = lambda: True
    coordinator._async_build_device_snapshot_with_fallbacks = AsyncMock(
        side_effect=lambda devices: devices
    )
    return captured


def test_push_updated_merges_with_existing_data(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """F-3: push for dev-1 must keep dev-2 (and others) in published snapshot."""

    now = time.time()
    coordinator._device_location_data["dev-1"] = {
        "device_id": "dev-1",
        "name": "Pixel 9",
        "latitude": 37.0,
        "longitude": -122.0,
        "accuracy": 5,
        "last_updated": now,
    }
    coordinator._device_names["dev-1"] = "Pixel 9"
    coordinator._device_names["dev-2"] = "iPhone 15"

    # dev-2 already lives in self.data (the previous published snapshot).
    coordinator.data = [
        {
            "device_id": "dev-2",
            "name": "iPhone 15",
            "latitude": 40.0,
            "longitude": -74.0,
            "accuracy": 8,
            "last_updated": now - 5.0,
        }
    ]

    captured = _prime_push_updated(coordinator)
    coordinator.push_updated(["dev-1"])

    assert captured, "push_updated should publish a snapshot"
    snapshot = captured[-1]
    ids = {row["device_id"] for row in snapshot}
    assert ids == {"dev-1", "dev-2"}, (
        f"merged snapshot must keep both devices; got {ids}"
    )


def test_push_updated_overwrites_pushed_device_only(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """F-3: pushed device record wins on collision; others stay byte-identical."""

    now = time.time()
    coordinator._device_location_data["dev-1"] = {
        "device_id": "dev-1",
        "name": "Pixel 9",
        "latitude": 41.0,
        "longitude": -100.0,
        "accuracy": 3,
        "last_updated": now,
    }
    coordinator._device_names["dev-1"] = "Pixel 9"
    coordinator._device_names["dev-2"] = "iPhone 15"

    stale_dev1 = {
        "device_id": "dev-1",
        "name": "Pixel 9",
        "latitude": 37.0,
        "longitude": -122.0,
        "accuracy": 50,
        "last_updated": now - 60.0,
    }
    untouched_dev2 = {
        "device_id": "dev-2",
        "name": "iPhone 15",
        "latitude": 40.0,
        "longitude": -74.0,
        "accuracy": 8,
        "last_updated": now - 5.0,
    }
    coordinator.data = [stale_dev1, untouched_dev2]

    captured = _prime_push_updated(coordinator)
    coordinator.push_updated(["dev-1"])

    snapshot = captured[-1]
    by_id = {row["device_id"]: row for row in snapshot}
    assert by_id["dev-1"]["latitude"] == 41.0, "pushed value must win on collision"
    assert by_id["dev-1"]["accuracy"] == 3
    assert by_id["dev-2"] == untouched_dev2, (
        "non-pushed devices must remain byte-identical"
    )


def test_push_updated_subentry_index_uses_merged_snapshot(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """F-9: _refresh_subentry_index must see the merged_snapshot (not None/stale)."""

    now = time.time()
    coordinator._device_location_data["dev-1"] = {
        "device_id": "dev-1",
        "name": "Pixel 9",
        "latitude": 37.0,
        "longitude": -122.0,
        "accuracy": 5,
        "last_updated": now,
    }
    coordinator._device_names["dev-1"] = "Pixel 9"
    coordinator._device_names["dev-2"] = "iPhone 15"
    coordinator.data = [
        {
            "device_id": "dev-2",
            "name": "iPhone 15",
            "latitude": 40.0,
            "longitude": -74.0,
            "accuracy": 8,
            "last_updated": now - 5.0,
        }
    ]

    captured_index_calls: list[Any] = []
    original_refresh = coordinator._refresh_subentry_index

    def _spy_refresh(
        visible_devices: Any = None,
        *,
        skip_manager_update: bool = False,
        skip_repair: bool = False,
    ) -> None:
        captured_index_calls.append(visible_devices)
        original_refresh(
            visible_devices,
            skip_manager_update=skip_manager_update,
            skip_repair=skip_repair,
        )

    coordinator._refresh_subentry_index = _spy_refresh  # type: ignore[method-assign]
    _prime_push_updated(coordinator)
    coordinator.push_updated(["dev-1"])

    assert captured_index_calls, "push_updated must refresh the subentry index"
    visible_arg = captured_index_calls[-1]
    assert visible_arg is not None, "F-9: index must receive merged_snapshot, not None"
    ids_seen = {
        dev.get("device_id") or dev.get("id")
        for dev in visible_arg
        if isinstance(dev, dict)
    }
    assert ids_seen == {"dev-1", "dev-2"}, (
        f"F-9: index must see both devices via merged_snapshot; got {ids_seen}"
    )


def test_push_updated_excludes_ignored_devices_from_merge(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """Codex-review regression: an ignored device already present in
    self.data must NOT reappear in the merged push snapshot, otherwise the
    user's ignore setting is silently defeated until a full poll.

    See: codex-review on PR #172 / commit 7291389e2b.
    """

    now = time.time()
    # dev-new is the actively pushed device.
    coordinator._device_names["dev-new"] = "Active"
    coordinator._device_location_data["dev-new"] = {
        "device_id": "dev-new",
        "name": "Active",
        "latitude": 1.0,
        "longitude": 2.0,
        "accuracy": 5,
        "last_updated": now,
    }

    # dev-old already lives in the previously published snapshot.
    coordinator.data = [
        {
            "device_id": "dev-old",
            "name": "Ignored",
            "latitude": 9.0,
            "longitude": 9.0,
            "accuracy": 8,
            "last_updated": now - 10.0,
        },
        {
            "device_id": "dev-new",
            "name": "Active",
            "latitude": 1.0,
            "longitude": 2.0,
            "accuracy": 5,
            "last_updated": now - 1.0,
        },
    ]

    # User ignores dev-old at runtime; ids filter must apply on BOTH sides
    # of the dict-union merge.
    coordinator._get_ignored_set = lambda: {"dev-old"}

    captured = _prime_push_updated(coordinator)
    coordinator.push_updated(["dev-new"])

    assert captured, "push_updated should publish a snapshot"
    snapshot = captured[-1]
    ids = {row["device_id"] for row in snapshot}
    assert "dev-old" not in ids, (
        f"ignored device must not reappear via old_devices merge; got {ids}"
    )
    assert ids == {"dev-new"}, (
        f"merged snapshot must contain only the non-ignored push target; got {ids}"
    )


def test_issue_183_status_stuck_on_false_readiness_then_recovers(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
) -> None:
    """Issue #183: a false-negative readiness pins fcm_status, a true one heals.

    Discriminates the candidate root causes at the coordinator-status layer:

    * H2 (false readiness): while ``api.is_push_ready()`` returns ``False`` the
      poll-top recovery never promotes the status, so it stays non-CONNECTED
      across cycles even though the receiver is healthy -- the "Disconnected
      until restart" symptom.
    * H1 (frozen cache) refuted: the moment readiness returns ``True`` a single
      poll cycle restores CONNECTED, so the status is re-evaluated, not frozen.
    * H3 (receiver down) refuted: only the readiness signal is toggled here; the
      coordinator recovers without any change to the receiver itself.
    """

    loop = coordinator.hass.loop
    coordinator._async_build_device_snapshot_with_fallbacks.return_value = []
    # A real device lets the poll cycle complete (avoids the cold-start guard);
    # the assertions below only inspect the status set by the recovery block.
    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._enabled_poll_device_ids = {"dev-1"}

    # Use the real soft-check so it consults ``api.is_push_ready`` (the desync
    # victim in issue #183), instead of the fixture's hard-wired ``True``.
    coordinator._is_fcm_ready_soft = type(coordinator)._is_fcm_ready_soft.__get__(
        coordinator
    )

    # Reload start state: a healthy receiver, but readiness reports False.
    dummy_api.is_push_ready = lambda: False

    loop.run_until_complete(coordinator._async_update_data())
    assert coordinator.fcm_status.state != FcmStatus.CONNECTED

    # A second cycle keeps it stuck (does not self-heal while readiness is False).
    loop.run_until_complete(coordinator._async_update_data())
    assert coordinator.fcm_status.state != FcmStatus.CONNECTED

    # Readiness becomes correct again -> the very next cycle recovers.
    dummy_api.is_push_ready = lambda: True
    loop.run_until_complete(coordinator._async_update_data())
    assert coordinator.fcm_status.state == FcmStatus.CONNECTED


# --------------------------------------------------------------------------
# The device list as the proof source for the transient auth counter.
#
# Moving the counter reset behind the empty guard in the poll cycle takes away
# its only everyday reset source: a fleet of idle BLE tags returns empty and no
# longer clears anything. Without a replacement a counter that once reached 2
# would sit there forever, and a single later hiccup would ask a user with a
# perfectly good login to sign in again.
#
# ``async_get_basic_device_list`` is the strongest proof source in the tree:
# it has no non-throwing error exit, so a non-throwing return means Nova
# accepted the ACCOUNT token. An expired login raises ``ConfigEntryAuthFailed``
# and never reaches the reset, which is what separates this source from the
# poll path it replaced.
#
# History, because two of the tests below now assert the inverse of what they
# once did: AP-2 made this refresh the transient counter's everyday reset
# source as well. AP-7 took that half back. The proof is sound for what it
# proves -- the auth state is still cleared here -- but the counter counts
# consecutive POLL CYCLES in which the action RPC rejected, and this refresh
# runs on an independent cadence (a fixed 300 s against a 60..3600 s option).
# Zeroing a per-cycle counter from a foreign clock produced starvation at one
# ratio and premature escalation at another. See the AP-7 block at the end of
# this file for both, each pinned as a measurement.
# --------------------------------------------------------------------------


def test_a_fresh_device_list_no_longer_resets_the_transient_auth_counter(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """A refresh proves the account token; it does not break the streak.

    Inverted deliberately rather than deleted: under AP-2 this asserted zero.
    What the refresh proves is unchanged and still used -- the auth state is
    cleared a few lines up. What it does not prove is that the action RPC
    accepts the same token again, and that is what the counter counts.
    """

    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._consecutive_transient_auth_failures = 2

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 2


def test_a_cached_device_list_does_not_reset_the_counter(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """A skipped refresh proves nothing at all, for the counter or the state.

    The cached branch never talks to Nova. Since AP-7 no refresh clears the
    counter anyway, so this now pins the weaker of two claims -- but it pins
    the one that would break first if the auth-state reset ever drifted out of
    the fetch branch: a verdict earned by something that did not happen.
    """

    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._last_device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._last_list_poll_mono = time.monotonic()
    coordinator._consecutive_transient_auth_failures = 2

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 2


def test_a_fresh_device_list_no_longer_clears_the_last_transient_error(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """The stored cause travels with the counter, wherever that is cleared.

    Separate from the counter assertion on purpose: the diagnostic snapshot
    exports both, and the pair has to stay consistent. Under AP-2 both were
    cleared here; under AP-7 both are cleared by the cycle that breaks the
    streak. What must never happen is one without the other -- a zero counter
    beside a cause names a failure that is over, a standing counter beside no
    cause names one nobody can explain.
    """

    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "expired"

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._last_transient_auth_error == "expired"


def test_a_failing_device_list_leaves_the_counter_alone(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """An expired login cannot mask itself through the refresh branch.

    This is the guard that keeps the branch honest: everything it resets sits
    AFTER the await, so a raising call never reaches it. It covered the counter
    under AP-2 and covers `_set_auth_state(failed=False)` since AP-7; the
    counter assertion is kept because the ordering is what is under test and a
    reset drifting back above the await would break both at once.
    """

    dummy_api.raise_auth = True
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "expired"

    loop = coordinator.hass.loop
    with pytest.raises(ConfigEntryAuthFailed):
        loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 2
    assert coordinator._last_transient_auth_error == "expired"


# --------------------------------------------------------------------------
# AP-6, kept as history because AP-7 replaced its mechanism.
#
# AP-2 made the device-list refresh the everyday reset source for the transient
# auth counter. That fixed one starvation and created another, and an external
# reviewer found it before a user did: at the default cadence both the list and
# the location cycle run every 300 seconds, and `_async_update_data` fetches the
# list BEFORE it schedules the location cycle. A tracker whose action RPC keeps
# returning a non-permanent credential rejection therefore had its counter
# zeroed immediately before every attempt, could only ever climb back to 1, and
# never reached the threshold of 3.
#
# AP-6 answered that with a marker: while a booked location failure stood
# unproven, no refresh cleared the count. Two further reviews then found the two
# halves of what the marker could not fix, because it treated a symptom. The
# counter was incremented per poll cycle and zeroed from the device-list
# cadence, and the two clocks are independently configurable -- so every ratio
# produced an error. AP-7 removed the marker entirely and moved the reset onto
# the clock that counts. The tests below survive as the refresh-side pins of
# that rule: no refresh, however often it runs, clears the count.
# --------------------------------------------------------------------------


def test_a_booked_location_failure_survives_the_next_list_refresh(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """A refresh that follows a booked failure must not clear the counter."""

    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "rejected"

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 2
    assert coordinator._last_transient_auth_error == "rejected"


def test_a_clean_cycle_clears_the_counter_without_any_refresh(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """The cycle that breaks the streak clears the count by itself.

    This is the guard against over-correcting. Suppressing every reset would
    strand any counter a one-off hiccup ever raised, which is the defect the
    device-list reset was introduced to remove. Under AP-6 the clean cycle only
    released a marker and the following refresh did the clearing; the name of
    this test said so and is inverted here on purpose. No refresh runs in the
    second half below, and the counter still reaches zero.
    """

    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._consecutive_transient_auth_failures = 1
    coordinator._last_transient_auth_error = "rejected"

    loop = coordinator.hass.loop
    # A refresh alone proves nothing about the action RPC and must not clear it.
    loop.run_until_complete(coordinator._async_update_data())
    assert coordinator._consecutive_transient_auth_failures == 1

    # A poll cycle that books no rejection is what clears it -- with no refresh
    # anywhere near it.
    coordinator.async_set_update_error = Mock()
    del coordinator._async_start_poll_cycle
    dummy_api.async_get_device_location = AsyncMock(return_value={})
    loop.run_until_complete(
        coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Device"}])
    )
    assert coordinator._consecutive_transient_auth_failures == 0
    assert coordinator._last_transient_auth_error is None


def test_two_refreshes_between_two_polls_do_not_clear_the_counter(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """No number of list refreshes clears the count.

    Nothing in the code couples the two cadences. `DEVICE_LIST_POLL_INTERVAL`
    is a fixed 300 s while `location_poll_interval` is an option up to 3600 s,
    and a deferred empty list re-fetches on the next 60 s tick without moving
    `_last_list_poll_mono`. Under AP-6 this pinned that a marker must not be
    consumed by a refresh; three refreshes made the point past any off-by-one
    reading of "consumes one". Under AP-7 there is no marker to consume, and
    the same three refreshes pin the stronger rule directly.

    Scope, stated rather than implied by the name: this drives no poll cycle at
    all (the fixture's `_async_start_poll_cycle` stub stays in place). It
    measures the refresh side of the rule. The booking site and the real
    ordering are measured by
    `test_the_production_order_still_reaches_the_reauth_threshold`.
    """

    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "rejected"

    loop = coordinator.hass.loop
    for _ in range(3):
        # Flag rather than a backdated baseline: `_last_list_poll_mono = 0.0`
        # only reads as "long ago" while `time.monotonic()` already exceeds
        # DEVICE_LIST_POLL_INTERVAL, which is true on a long-running workstation
        # and false on a freshly booted CI runner.
        coordinator._force_device_list_refresh = True
        loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 2
    assert coordinator._last_transient_auth_error == "rejected"


def test_the_production_order_still_reaches_the_reauth_threshold(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """Three failing cycles escalate even though a list refresh precedes each.

    The other tests in this block set the marker by hand and therefore prove
    the reset rule, not the ordering it exists for. This one drives the real
    `_async_update_data` -> `_async_start_poll_cycle` sequence with the real
    booking site, which is exactly what the previous revision's tests missed:
    they called `_async_start_poll_cycle()` directly and so never saw the list
    refresh that ran before it in production.
    """

    dummy_api.device_list = [{"id": "dev-1", "name": "Device"}]
    location_calls: list[str] = []

    async def _reject(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        location_calls.append(device_id)
        # 403, not 401: `is_credential_rejection` records that a 401 surviving
        # the refresh sequence is raised as NovaAuthPermanentError instead, so
        # 403 is the only plain credential rejection a handler actually sees.
        # The seam below this one -- api.py mapping a transport status onto this
        # exception -- is pinned in test_api_basics.py and deliberately not
        # re-tested here; this test owns the coordinator's ordering, not the
        # transport's classification.
        raise NovaAuthError(403, "action rejected")

    dummy_api.async_get_device_location = _reject

    # Run the real poll cycle: the ordering under test exists only between the
    # two, so the fixture's AsyncMock would measure nothing.
    del coordinator._async_start_poll_cycle
    # The cycle reports a failed update through the DataUpdateCoordinator base;
    # without a stub the AttributeError lands in the fire-and-forget task and is
    # swallowed, which would let this test pass over a broken cycle.
    coordinator.async_set_update_error = Mock()

    tasks: list[asyncio.Task[Any]] = []
    original_create = coordinator.hass.async_create_task

    def _tracking_create(coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        task = original_create(coro, name=name)
        tasks.append(task)
        return task

    coordinator.hass.async_create_task = _tracking_create

    loop = coordinator.hass.loop
    for _ in range(_MAX_TRANSIENT_AUTH_FAILURES):
        # Both cadences made due without touching absolute monotonic values:
        # the flag for the list, the coordinator's own helper for the poll.
        # Zeroing the baselines instead would tie the test to the host's
        # uptime.
        coordinator._force_device_list_refresh = True
        coordinator.force_poll_due()
        loop.run_until_complete(coordinator._async_update_data())
        if tasks:
            # gather() re-raises, so a cycle that died on the way to the
            # booking site fails this test instead of being reported as a
            # counter that simply did not move.
            loop.run_until_complete(asyncio.gather(*tasks))
            tasks.clear()

    assert len(location_calls) == _MAX_TRANSIENT_AUTH_FAILURES
    assert coordinator._consecutive_transient_auth_failures >= (
        _MAX_TRANSIENT_AUTH_FAILURES
    )
    assert coordinator.config_entry.reauth_calls == 1
    # The message a user reads must name the tracker that was rejected. The
    # verdict moved out of the device loop, so the name is carried to the end of
    # the cycle by hand; without this assertion a mutation dropping that carry
    # survived the whole tree and the Repairs issue would have read "None".
    assert coordinator._auth_error_message is not None
    assert "Device" in coordinator._auth_error_message


# --------------------------------------------------------------------------
# AP-7: the streak has to be broken on the clock that counts it.
#
# AP-6 bound the release of the booked failure to the poll cycle, which removed
# the dependency on how the two cadences line up but left the COUNT itself on
# the list refresh. Two external reviews then found the two halves of the same
# root cause, and both are reproduced as measurements below: the counter was
# incremented per poll cycle and zeroed by a device-list refresh, and the two
# clocks are independently configurable. Every ratio produces one of the two
# errors -- a long poll interval starves the escalation, a short one escalates
# rejections that were never consecutive.
#
# So the cycle that breaks the streak now zeroes the count itself, cycle-wide.
# `_set_auth_state(failed=False)` stays on the list refresh, where it belongs:
# that branch proves the ACCOUNT token, which is what the auth state is about.
# It says nothing about the action RPC accepting that token, which is what the
# counter is about, and conflating the two is what produced both findings.
#
# Cycle-wide, not per device, is the load-bearing word: the original defect was
# an idle BLE tag clearing the rejection its sibling had just booked. A cycle
# counts as clean only when NO device rejected in it.
# --------------------------------------------------------------------------


def test_a_clean_cycle_between_rejections_breaks_the_streak(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """Rejections separated by a clean cycle must not accumulate to the threshold.

    Measured before the fix: with a 60 s location interval and the fixed 300 s
    device-list interval, up to five poll cycles run without a single refresh.
    Three 403s at minutes 0, 2 and 4, each separated by a clean empty-result
    cycle, then reached the threshold and forced a reauth although they were
    never consecutive. The counter is named `_consecutive_...`; the streak has
    to be broken by the cycle that breaks it, on that cycle's own clock, not by
    a refresh running on a slower and independently configurable one.

    The list stays cached on purpose: the absence of a refresh IS the case.
    """

    device = {"id": "dev-1", "name": "Device"}
    dummy_api.device_list = [device]
    coordinator._last_device_list = [device]
    coordinator._last_list_poll_mono = time.monotonic()

    reject_next = {"value": True}
    attempts: list[str] = []

    async def _alternate(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        attempts.append(device_id)
        if reject_next["value"]:
            # 403 rather than 401: a 401 surviving the refresh sequence is
            # raised as NovaAuthPermanentError, so 403 is the only plain
            # credential rejection a handler sees.
            raise NovaAuthError(403, "action rejected")
        return {}

    dummy_api.async_get_device_location = _alternate
    del coordinator._async_start_poll_cycle
    # Without a stub the base class's AttributeError lands in the fire-and-
    # forget task and is swallowed, which would report a broken cycle as a
    # counter that simply did not move.
    coordinator.async_set_update_error = Mock()

    tasks: list[asyncio.Task[Any]] = []
    original_create = coordinator.hass.async_create_task

    def _tracking_create(coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        task = original_create(coro, name=name)
        tasks.append(task)
        return task

    coordinator.hass.async_create_task = _tracking_create

    loop = coordinator.hass.loop
    counts: list[int] = []
    for cycle in range(5):
        reject_next["value"] = cycle % 2 == 0
        # The coordinator's own helper rather than a zeroed baseline: an
        # absolute monotonic value measures the host's uptime along with the
        # behaviour.
        coordinator.force_poll_due()
        loop.run_until_complete(coordinator._async_update_data())
        if tasks:
            # gather() re-raises, so a cycle that died before reaching the
            # booking site fails this test instead of passing quietly.
            loop.run_until_complete(asyncio.gather(*tasks))
            tasks.clear()
        counts.append(coordinator._consecutive_transient_auth_failures)

    # Every cycle actually asked. Without this the zeros could equally come
    # from the no-pollable-device reset, which is a different rule with its own
    # test, and the assertion below would not measure what its name says.
    assert len(attempts) == 5
    # Before the fix this read [1, 1, 2, 2, 3] and raised a reauth.
    assert counts == [1, 0, 1, 0, 1]
    assert coordinator.config_entry.reauth_calls == 0


def test_a_cycle_with_no_pollable_device_clears_the_stale_count(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """A budget must not be stranded when no cycle can run at all.

    If every tracker is disabled or ignored after a failure was booked, no poll
    cycle runs, so the release path that the cycle owns is unreachable and the
    count would stand indefinitely. Re-enabling a tracker weeks later would
    then let its first rejection inherit the old failures and force a premature
    reauth. A pass that finds nothing to poll saw no rejection either, which is
    the same evidence a clean cycle provides, so it clears the streak the same
    way.

    Deliberately measured on the pre-cooldown list: a device held back only by
    its poll cooldown has not proved anything, and clearing on that would be a
    starvation path of its own.
    """

    device = {"id": "dev-1", "name": "Device"}
    dummy_api.device_list = [device]
    # Cached list: no refresh runs, so nothing but the rule under test can
    # clear the counter.
    coordinator._last_device_list = [device]
    coordinator._last_list_poll_mono = time.monotonic()
    # Known to the registry but not enabled for polling -- the "disabled
    # tracker" case, reached without touching the ignore set. The account still
    # HAS a device; an empty list is a different situation (an outage) and
    # deliberately clears nothing.
    coordinator._devices_with_entry = {"dev-1"}
    coordinator._enabled_poll_device_ids = set()
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "rejected"

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 0
    assert coordinator._last_transient_auth_error is None


def test_an_empty_device_list_does_not_clear_the_count(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """An outage is not evidence, and an empty list is an outage.

    The no-pollable-device reset asks for two things, and the second one is the
    guard: the account must HAVE devices while none of them is pollable. A list
    that came back empty satisfies "nothing to poll" just as well, and the
    two-pass empty-list quorum lets a backend hiccup through as an accepted
    empty list. Clearing a rejection budget on that would be the same fault the
    end-of-cycle condition refuses one layer along -- absence of evidence used
    as evidence.
    """

    dummy_api.device_list = []
    coordinator._last_device_list = []
    coordinator._last_list_poll_mono = time.monotonic()
    # Past the cold-start guard: a first-ever empty list raises UpdateFailed and
    # would never reach the branch under test. This is the later hiccup, the one
    # the two-pass quorum accepts.
    coordinator._last_nonempty_wall = time.time()
    coordinator._force_device_list_refresh = True
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "rejected"

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 2
    assert coordinator._last_transient_auth_error == "rejected"


def test_two_rejecting_devices_count_as_one_failing_cycle(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """The streak counts cycles, so a cycle may raise it by at most one.

    The increment sat inside the device loop and therefore ran once per
    rejecting tracker. Two broken trackers made a single cycle worth two, so the
    threshold of three was reached in the middle of the second cycle instead of
    at the end of the third. The name of the counter says cycles and the reauth
    message says "persisted across %d poll cycles", so the per-device tally was
    wrong in both directions: it escalated early, and it reported a number that
    was not a cycle count.
    """

    devices = [
        {"id": "dev-1", "name": "One"},
        {"id": "dev-2", "name": "Two"},
    ]
    dummy_api.device_list = devices
    coordinator._last_device_list = devices
    coordinator._last_list_poll_mono = time.monotonic()

    async def _reject(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise NovaAuthError(403, "action rejected")

    dummy_api.async_get_device_location = _reject
    del coordinator._async_start_poll_cycle
    coordinator.async_set_update_error = Mock()

    tasks: list[asyncio.Task[Any]] = []
    original_create = coordinator.hass.async_create_task

    def _tracking_create(coro: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        task = original_create(coro, name=name)
        tasks.append(task)
        return task

    coordinator.hass.async_create_task = _tracking_create

    loop = coordinator.hass.loop
    counts: list[int] = []
    for _ in range(2):
        coordinator.force_poll_due()
        loop.run_until_complete(coordinator._async_update_data())
        if tasks:
            loop.run_until_complete(asyncio.gather(*tasks))
            tasks.clear()
        counts.append(coordinator._consecutive_transient_auth_failures)

    # Measured before the fix: [2, 3], with the reauth raised inside cycle two.
    assert counts == [1, 2]
    assert coordinator.config_entry.reauth_calls == 0
    # The cause is recorded per rejecting device even though the count is not.
    # Without this the "record it every time" half of the rule is unpinned: a
    # mutation that drops the assignment survived every other test in the tree.
    assert coordinator._last_transient_auth_error is not None
    assert "403" in coordinator._last_transient_auth_error


def test_the_reauth_verdict_does_not_depend_on_device_order(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """The same answers must give the same verdict, whichever device answers first.

    The threshold check sat inside the device loop and returned as soon as it
    fired, so a cycle that entered at 2 reauthed on the first rejecting tracker
    and never asked the siblings. If a sibling would have returned a location
    WITH content -- the one strong proof on this path, which clears the counter
    outright -- the reauth was raised over evidence the cycle held but had not
    looked at yet. Reversing the device order produced the opposite outcome from
    identical responses.

    The verdict now belongs to the cycle: every device is asked, then the cycle
    decides. Content beats a rejection, because content is proof and a
    non-permanent rejection is not.
    """

    devices = [
        {"id": "dev-bad", "name": "Bad"},
        {"id": "dev-good", "name": "Good"},
    ]

    async def _by_device(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if device_id == "dev-bad":
            raise NovaAuthError(403, "action rejected")
        return {
            "latitude": 37.0,
            "longitude": -122.0,
            "last_seen": int(time.time()),
        }

    # Without this the fixture's AsyncMock stands in for the cycle and the test
    # measures nothing at all -- it passed that way once, with the counter
    # simply untouched.
    del coordinator._async_start_poll_cycle

    for order in (devices, list(reversed(devices))):
        coordinator._consecutive_transient_auth_failures = 2
        coordinator._last_transient_auth_error = "rejected"
        coordinator.config_entry.reauth_calls = 0
        coordinator.async_set_update_error = Mock()
        dummy_api.async_get_device_location = _by_device

        loop = coordinator.hass.loop
        loop.run_until_complete(coordinator._async_start_poll_cycle(order))

        first = order[0]["name"]
        assert coordinator.config_entry.reauth_calls == 0, f"reauth with {first} first"
        assert coordinator._consecutive_transient_auth_failures == 0, (
            f"counter not cleared with {first} first"
        )
        # The mock was here before this assertion was, and that gap was the
        # finding: the cycle accepted the content as proof for the counter and
        # still reported the rejection account-wide, so every tracker entity
        # went unavailable next to the one that had just answered. A mocked
        # collaborator that nothing asserts against measures nothing.
        assert coordinator.async_set_update_error.call_args_list == [], (
            f"account marked failed with {first} first: "
            f"{coordinator.async_set_update_error.call_args_list}"
        )


def test_content_withdraws_only_the_rejection_not_a_timeout(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """Content refutes the rejection it stands next to, and nothing else.

    The mixed-cycle branch withdraws the account-wide report of a credential
    rejection once a sibling has answered WITH content. That withdrawal is bound
    to the identity of the rejection this cycle booked, and this test is why: a
    timeout reached ``last_exception`` first, and one device answering says
    nothing about a device that never answered at all. Dropping the identity
    check would let content silently swallow an unrelated failure report.

    The push transport is taken down on purpose. With FCM connected a timeout is
    deliberately ignored to keep the status healthy, so it would never reach
    ``last_exception`` and this test would measure nothing -- it did exactly
    that on the first attempt, and passed vacuously with an empty call list.
    """

    devices = [
        {"id": "dev-slow", "name": "Slow"},
        {"id": "dev-bad", "name": "Bad"},
        {"id": "dev-good", "name": "Good"},
    ]

    async def _by_device(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if device_id == "dev-slow":
            raise TimeoutError
        if device_id == "dev-bad":
            raise NovaAuthError(403, "action rejected")
        return {
            "latitude": 37.0,
            "longitude": -122.0,
            "last_seen": int(time.time()),
        }

    # Without this the fixture's AsyncMock stands in for the cycle and nothing
    # below measures anything.
    del coordinator._async_start_poll_cycle

    # No push transport: this is what makes the timeout carry a report at all.
    coordinator._fcm_status_state = None
    assert coordinator.is_fcm_connected is False

    # Entering at 2 on purpose. At 0 the "the rejection is withdrawn" assertion
    # below would hold whatever the branch did, and the sentence would claim a
    # measurement it never made.
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "rejected"
    coordinator.config_entry.reauth_calls = 0
    coordinator.async_set_update_error = Mock()
    dummy_api.async_get_device_location = _by_device

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_start_poll_cycle(devices, force=True))

    # The rejection is withdrawn (streak cleared, no reauth), the timeout is not.
    assert coordinator._consecutive_transient_auth_failures == 0
    assert coordinator._last_transient_auth_error is None
    assert coordinator.config_entry.reauth_calls == 0
    reported = coordinator.async_set_update_error.call_args_list
    assert len(reported) == 1, f"expected the timeout alone, got {reported}"
    surfaced = reported[0].args[0]
    assert not isinstance(surfaced, NovaAuthError), (
        f"the withdrawn rejection surfaced anyway: {surfaced!r}"
    )
    assert isinstance(surfaced.__cause__, TimeoutError), (
        f"expected the timeout to survive, got {surfaced!r}"
    )


@pytest.mark.parametrize(
    ("raised", "expect_cause"),
    [
        (TimeoutError(), True),
        (StaleOwnerKeyError("tracker v1 < v2"), False),
        (RuntimeError("boom"), False),
    ],
    ids=["timeout", "stale-owner-key", "generic"],
)
def test_a_later_failure_survives_the_rejection_it_had_to_queue_behind(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
    raised: BaseException,
    expect_cause: bool,
) -> None:
    """An independent failure must outlive a rejection that is later withdrawn.

    ``last_exception`` is a first-come slot, and a credential rejection holds it
    only PROVISIONALLY: the mixed-cycle branch withdraws that report once a
    sibling answers with content. If the rejection arrives FIRST, a later timeout
    finds the slot taken and drops its own report, and the withdrawal then empties
    the slot -- so ``async_set_update_error`` is never called and the coordinator
    calls the cycle a success although a device never answered at all.

    This is the mirror ordering of
    ``test_content_withdraws_only_the_rejection_not_a_timeout``, which has the
    timeout reach the slot first. That one passes either way; only this order
    measures the displacement.

    Parametrised over ALL THREE first-come slots on this path, because the
    displacement rule is written three times and one case leaves two of them
    unmeasured: mutating the stale-owner-key and the generic slot back to
    ``last_exception is None`` both SURVIVED the timeout-only version of this
    test.
    """

    devices = [
        {"id": "dev-bad", "name": "Bad"},
        {"id": "dev-slow", "name": "Slow"},
        {"id": "dev-good", "name": "Good"},
    ]

    async def _by_device(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if device_id == "dev-bad":
            raise NovaAuthError(403, "action rejected")
        if device_id == "dev-slow":
            raise raised
        return {
            "latitude": 37.0,
            "longitude": -122.0,
            "last_seen": int(time.time()),
        }

    # Without this the fixture's AsyncMock stands in for the cycle and nothing
    # below measures anything.
    del coordinator._async_start_poll_cycle

    # No push transport: with FCM connected the timeout is deliberately ignored
    # to keep the status healthy and would never reach the slot at all.
    coordinator._fcm_status_state = None
    assert coordinator.is_fcm_connected is False

    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "rejected"
    coordinator.config_entry.reauth_calls = 0
    coordinator.async_set_update_error = Mock()
    # The timeout and generic branches have no `continue`, so the inter-device
    # delay (default 5s) would be slept for real. This test decides a branch, not
    # a duration.
    coordinator.device_poll_delay = 0
    dummy_api.async_get_device_location = _by_device

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_start_poll_cycle(devices, force=True))

    # The content still withdraws the rejection: streak cleared, no reauth.
    assert coordinator._consecutive_transient_auth_failures == 0
    assert coordinator._last_transient_auth_error is None
    assert coordinator.config_entry.reauth_calls == 0

    # ... but the timeout has to survive it.
    reported = coordinator.async_set_update_error.call_args_list
    assert len(reported) == 1, (
        f"the failure was swallowed with the withdrawn rejection, got {reported}"
    )
    surfaced = reported[0].args[0]
    assert not isinstance(surfaced, NovaAuthError), (
        f"the withdrawn rejection surfaced anyway: {surfaced!r}"
    )
    if expect_cause:
        # The timeout branch wraps: the original sits in ``__cause__``.
        assert isinstance(surfaced.__cause__, type(raised)), (
            f"expected {type(raised).__name__} to survive, got {surfaced!r}"
        )
    else:
        # The other two slots store the exception itself.
        assert surfaced is raised, (
            f"expected {type(raised).__name__} to survive, got {surfaced!r}"
        )


def test_without_content_the_rejection_keeps_the_report_it_booked(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """No content, no withdrawal -- so the rejection must keep the report.

    The second slot exists because whether the withdrawal fires is only known
    AFTER the loop. A first attempt at this simply let the later failure overwrite
    the rejection, and in a cycle WITHOUT content that swapped the more specific
    diagnosis (the credential rejection driving the running escalation) for the
    less specific one ("nobody answered"). Measured: the report was
    ``UpdateFailed('Location request timed out for Slow')`` where it should name
    the rejection. That was the wording at the time of the measurement;
    the timeout text now reads
    ``'Location request timed out for device dev-slow'``.
    """

    devices = [
        {"id": "dev-bad", "name": "Bad"},
        {"id": "dev-slow", "name": "Slow"},
    ]

    async def _by_device(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if device_id == "dev-bad":
            raise NovaAuthError(403, "action rejected")
        raise TimeoutError

    del coordinator._async_start_poll_cycle
    coordinator._fcm_status_state = None
    assert coordinator.is_fcm_connected is False
    coordinator.async_set_update_error = Mock()
    coordinator.device_poll_delay = 0
    dummy_api.async_get_device_location = _by_device

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_start_poll_cycle(devices, force=True))

    reported = coordinator.async_set_update_error.call_args_list
    assert len(reported) == 1, f"expected exactly one report, got {reported}"
    assert isinstance(reported[0].args[0], NovaAuthError), (
        f"the rejection was swapped for the later failure: {reported}"
    )


@pytest.mark.parametrize(
    "order",
    [("dev-slow", "dev-bad", "dev-good"), ("dev-bad", "dev-slow", "dev-good")],
    ids=["first-slot", "second-slot"],
)
def test_timeout_report_names_the_device_by_id(
    coordinator: GoogleFindMyCoordinator,
    dummy_api: _DummyAPI,
    order: tuple[str, str, str],
) -> None:
    """The timeout report names the device by its id, not by its name.

    Home Assistant logs the ``UpdateFailed`` text at ERROR through
    ``async_set_update_error`` on the unattended poll path, where AGENTS.md
    section 5 (b) allows a device name only at DEBUG. Both slots are covered:
    with the timeout first it lands in ``last_exception``; behind a rejection
    that a sibling's content later withdraws it lands in ``independent_error``.
    """

    names = {"dev-slow": "Slow", "dev-bad": "Bad", "dev-good": "Good"}
    devices = [{"id": device_id, "name": names[device_id]} for device_id in order]

    async def _by_device(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if device_id == "dev-slow":
            raise TimeoutError
        if device_id == "dev-bad":
            raise NovaAuthError(403, "action rejected")
        return {
            "latitude": 37.0,
            "longitude": -122.0,
            "last_seen": int(time.time()),
        }

    del coordinator._async_start_poll_cycle
    coordinator._fcm_status_state = None
    assert coordinator.is_fcm_connected is False
    coordinator.async_set_update_error = Mock()
    coordinator.device_poll_delay = 0
    dummy_api.async_get_device_location = _by_device

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_start_poll_cycle(devices, force=True))

    reported = coordinator.async_set_update_error.call_args_list
    assert len(reported) == 1, f"expected exactly one report, got {reported}"
    surfaced = reported[0].args[0]
    assert str(surfaced) == "Location request timed out for device dev-slow"
    assert "Slow" not in str(surfaced)
    assert isinstance(surfaced.__cause__, TimeoutError)
    # The translated reason names the device by id as well (both slots).
    assert surfaced.translation_domain == DOMAIN
    assert surfaced.translation_key == "locate_timeout"
    assert surfaced.translation_placeholders == {"device_id": "dev-slow"}


def test_only_the_provisional_rejection_gives_way_not_an_earlier_diagnosis(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """Displacement is bound to identity, so it stays a narrow exception.

    The three first-come slots let a later failure displace the rejection this
    cycle booked, because that rejection may still be withdrawn. They must not
    become "last failure wins": a stale owner key names one tracker's outdated
    key, a timeout only says nobody answered, and the first, more specific
    diagnosis is the one worth reporting.

    Without this the widened condition could be written as an unconditional
    overwrite and no test would notice -- measured: mutating the timeout slot to
    ``if True`` left every other pin on this path green.
    """

    devices = [
        {"id": "dev-stale", "name": "Stale"},
        {"id": "dev-slow", "name": "Slow"},
    ]
    stale = StaleOwnerKeyError("tracker v1 < v2")

    async def _by_device(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if device_id == "dev-stale":
            raise stale
        raise TimeoutError

    del coordinator._async_start_poll_cycle

    # No push transport, or the timeout is ignored on purpose and never competes
    # for the slot at all.
    coordinator._fcm_status_state = None
    assert coordinator.is_fcm_connected is False

    coordinator.async_set_update_error = Mock()
    coordinator.device_poll_delay = 0
    dummy_api.async_get_device_location = _by_device

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_start_poll_cycle(devices, force=True))

    reported = coordinator.async_set_update_error.call_args_list
    assert len(reported) == 1, f"expected exactly one report, got {reported}"
    assert reported[0].args[0] is stale, (
        f"the later timeout displaced an unrelated earlier diagnosis: {reported}"
    )


def test_the_stored_cause_follows_the_count_in_a_mixed_cycle(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """Counter and cause must end in the same state, whichever device answers first.

    The diagnostic snapshot exports both fields. A cause standing next to a zero
    count names a failure that is over, and the pair used to depend on device
    order: a content-bearing device zeroes both at its own site, but a rejection
    processed after it wrote the cause back and the end-of-cycle branches skipped
    the mixed case entirely.
    """

    devices = [
        {"id": "dev-good", "name": "Good"},
        {"id": "dev-bad", "name": "Bad"},
    ]

    async def _by_device(device_id: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        if device_id == "dev-bad":
            raise NovaAuthError(403, "action rejected")
        return {
            "latitude": 37.0,
            "longitude": -122.0,
            "last_seen": int(time.time()),
        }

    del coordinator._async_start_poll_cycle

    for order in (devices, list(reversed(devices))):
        coordinator._consecutive_transient_auth_failures = 2
        coordinator._last_transient_auth_error = "old"
        coordinator.async_set_update_error = Mock()
        dummy_api.async_get_device_location = _by_device

        loop = coordinator.hass.loop
        loop.run_until_complete(coordinator._async_start_poll_cycle(order))

        first = order[0]["name"]
        assert coordinator._consecutive_transient_auth_failures == 0, first
        assert coordinator._last_transient_auth_error is None, first


def test_a_running_cycle_blocks_the_no_pollable_device_reset(
    coordinator: GoogleFindMyCoordinator, dummy_api: _DummyAPI
) -> None:
    """A cycle still in flight owns the streak; nothing else may clear it.

    The poll cycle runs as a fire-and-forget task, so a coordinator refresh can
    reach this branch while an earlier cycle is still asking its devices. If
    every tracker was disabled in the meantime, the branch saw "nothing to poll"
    and cleared a streak the running cycle was still spending: a streak of 2
    became 0 and then 1 when that cycle booked its rejection, and with no further
    cycle able to run, the escalation was lost for good.
    """

    device = {"id": "dev-1", "name": "Device"}
    dummy_api.device_list = [device]
    coordinator._last_device_list = [device]
    coordinator._last_list_poll_mono = time.monotonic()
    coordinator._devices_with_entry = {"dev-1"}
    coordinator._enabled_poll_device_ids = set()
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "rejected"
    # A cycle started earlier and has not finished.
    coordinator._is_polling = True

    loop = coordinator.hass.loop
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 2
    assert coordinator._last_transient_auth_error == "rejected"

    # The guard defers the reset, it does not suppress it. Once that cycle has
    # retired, the very next refresh finds nothing to poll and no cycle in
    # flight, and clears the final count.
    coordinator._is_polling = False
    loop.run_until_complete(coordinator._async_update_data())

    assert coordinator._consecutive_transient_auth_failures == 0
    assert coordinator._last_transient_auth_error is None
