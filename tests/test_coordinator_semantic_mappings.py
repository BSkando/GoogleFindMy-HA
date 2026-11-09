# tests/test_coordinator_semantic_mappings.py
from __future__ import annotations

import ast
import asyncio
import logging
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from homeassistant.config_entries import ConfigEntryAuthFailed
from homeassistant.exceptions import HomeAssistantError

from custom_components.googlefindmy._reauth_reason import ReauthReasonCode
from custom_components.googlefindmy.const import DOMAIN, OPT_SEMANTIC_LOCATIONS
from custom_components.googlefindmy.coordinator import (
    CryptoStatus,
    GoogleFindMyCoordinator,
)
from custom_components.googlefindmy.coordinator.polling import (
    _MAX_DECRYPT_FAILURES,
    _MAX_TRANSIENT_AUTH_FAILURES,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
    SharedKeyMismatchError,
    StaleOwnerKeyError,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.location_request import (
    LocationRequestNotAcceptedError,
)
from custom_components.googlefindmy.NovaApi.nova_request import (
    NovaAuthError,
    NovaAuthPermanentError,
)
from custom_components.googlefindmy.SpotApi.GetEidInfoForE2eeDevices.get_eid_info_request import (
    SpotApiEmptyResponseError,
)
from custom_components.googlefindmy.SpotApi.spot_request import (
    SpotAuthPermanentError,
)
from tests.helpers.homeassistant import GoogleFindMyConfigEntryStub


class _DummyAPI:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    async def async_get_device_location(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        return dict(self._payload)


class _TrackingFilter:
    def __init__(
        self, should_filter: bool = False, replacement: dict[str, float] | None = None
    ) -> None:
        self.should_filter = should_filter
        self.replacement = replacement
        self.called = 0

    def should_filter_detection(
        self, *_args: Any, **_kwargs: Any
    ) -> tuple[bool, dict[str, float] | None]:
        self.called += 1
        return self.should_filter, self.replacement


class _RaisingFilter:
    def __init__(self) -> None:
        self.called = 0

    def should_filter_detection(
        self, *_args: Any, **_kwargs: Any
    ) -> tuple[bool, dict[str, float] | None]:
        self.called += 1
        raise AssertionError("Spam filter should not run when semantic mapping applies")


def _base_coordinator(
    options: dict[str, Any], google_filter: Any, api_payload: dict[str, Any]
) -> GoogleFindMyCoordinator:
    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator.config_entry = GoogleFindMyConfigEntryStub(options=options)
    coordinator.hass = SimpleNamespace()
    coordinator.increment_stat = lambda *_args, **_kwargs: None
    coordinator.async_set_updated_data = lambda *_args, **_kwargs: None
    coordinator.push_updated = lambda *_args, **_kwargs: None
    coordinator._apply_report_type_cooldown = lambda *_args, **_kwargs: None
    coordinator._should_preserve_precise_home_coordinates = lambda *_args, **_kwargs: (
        False
    )
    coordinator._normalize_coords = lambda *_args, **_kwargs: True
    coordinator._is_significant_update = lambda *_args, **_kwargs: True
    coordinator.update_device_cache = lambda *_args, **_kwargs: None
    coordinator._set_auth_state = lambda **_kwargs: None
    coordinator._device_location_data = {}
    coordinator._device_poll_cooldown_until = {}
    coordinator._present_last_seen = {}
    coordinator._locate_inflight = set()
    coordinator._locate_cooldown_until = {}
    coordinator._device_action_locks = {}
    coordinator._device_update_history = {}
    coordinator._last_poll_mono = 0.0
    coordinator._consecutive_timeouts = 0
    coordinator._consecutive_transient_auth_failures = 0
    coordinator._last_transient_auth_error = None
    # FIX 3: reauth-reason state normally seeded by ``__init__`` (bypassed via
    # ``__new__`` here); the poll-reauth choke point records through these.
    coordinator._reauth_reason = None
    coordinator._reauth_reason_logged = set()
    coordinator._consecutive_decrypt_failures = 0
    coordinator._last_decrypt_reauth_monotonic = None
    coordinator._last_decrypt_error = None
    coordinator._crypto_status_state = CryptoStatus.UNKNOWN
    coordinator._crypto_status_reason = None
    coordinator._crypto_status_changed_at = None
    # Deliberately low monotonic value: simulates a freshly booted runner (process
    # uptime below the reauth cooldown). With the None sentinel the first escalation
    # must fire regardless, so this pins the whole loop-test class against the
    # uptime-dependent flake that slipped through CI.
    coordinator._monotonic = lambda: 100.0
    coordinator.location_poll_interval = 0
    coordinator.data = []
    coordinator._last_device_list = []
    coordinator._get_ignored_set = set
    coordinator._build_snapshot_from_cache = lambda *_args, **_kwargs: []
    coordinator._get_google_home_filter = lambda: google_filter
    coordinator.api = _DummyAPI(api_payload)
    coordinator.get_device_display_name = lambda device_id: device_id
    coordinator.can_request_location = lambda _device_id: True
    coordinator._api_push_ready = lambda: True
    coordinator._is_on_hass_loop = lambda: True
    coordinator._semantic_label_cache = {}
    return coordinator


@pytest.mark.asyncio
async def test_manual_locate_prefers_semantic_mapping() -> None:
    options = {
        OPT_SEMANTIC_LOCATIONS: {
            "Lobby": {"latitude": 1.25, "longitude": 2.5, "accuracy": 4.0}
        }
    }
    google_filter = _RaisingFilter()
    coordinator = _base_coordinator(options, google_filter, {"semantic_name": "lobby"})

    result = await coordinator.async_locate_device("device-1")

    assert result["latitude"] == pytest.approx(1.25)
    assert result["longitude"] == pytest.approx(2.5)
    assert result["accuracy"] == pytest.approx(4.0)
    assert result["location_type"] == "trusted"
    assert google_filter.called == 0


@pytest.mark.parametrize(
    "api_name",
    ["living room", "Living Room", "Near Living Room"],
)
def test_semantic_mapping_normalizes_api_names(api_name: str) -> None:
    options = {
        OPT_SEMANTIC_LOCATIONS: {
            "Living Room": {"latitude": 8.5, "longitude": 9.5, "accuracy": 10.0}
        }
    }
    coordinator = _push_coordinator(options)

    coordinator.update_device_cache(
        "dev-norm",
        {
            "semantic_name": api_name,
            "last_seen": 10,
        },
    )

    cached = coordinator._device_location_data["dev-norm"]
    assert cached["latitude"] == pytest.approx(8.5)
    assert cached["longitude"] == pytest.approx(9.5)
    assert cached["accuracy"] == pytest.approx(10.0)


def test_semantic_mapping_rejects_partial_matches() -> None:
    options = {
        OPT_SEMANTIC_LOCATIONS: {
            "Kitchen": {"latitude": 3.0, "longitude": 4.0, "accuracy": 5.0}
        }
    }
    coordinator = _push_coordinator(options)

    coordinator.update_device_cache(
        "dev-partial",
        {"semantic_name": "Kitchen 2", "last_seen": 20},
    )

    cached = coordinator._device_location_data["dev-partial"]
    assert cached.get("latitude") is None
    assert cached.get("longitude") is None
    assert cached.get("semantic_name") == "Kitchen 2"


def _polling_coordinator(
    options: dict[str, Any], google_filter: Any, api_payload: dict[str, Any]
) -> GoogleFindMyCoordinator:
    coordinator = _base_coordinator(options, google_filter, api_payload)
    coordinator._poll_lock = asyncio.Lock()
    coordinator._is_polling = False
    coordinator._is_fcm_ready_soft = lambda: True
    coordinator._note_fcm_deferral = lambda *_args, **_kwargs: None
    coordinator._schedule_short_retry = lambda *_args, **_kwargs: None
    coordinator._clear_fcm_deferral = lambda: None
    coordinator._schedule_eid_resolver_refresh = lambda: None
    coordinator.note_error = lambda *_args, **_kwargs: None
    coordinator.async_set_update_error = lambda *_args, **_kwargs: None
    coordinator._last_poll_result = None
    coordinator._startup_complete = True
    coordinator._fcm_defer_started_mono = 0.0
    coordinator._fcm_last_stage = 0
    coordinator.device_poll_delay = 0
    coordinator.safe_update_metric = lambda *_args, **_kwargs: None
    return coordinator


def _push_coordinator(options: dict[str, Any]) -> GoogleFindMyCoordinator:
    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator.config_entry = GoogleFindMyConfigEntryStub(options=options)
    coordinator.hass = SimpleNamespace()
    coordinator.increment_stat = lambda *_args, **_kwargs: None
    coordinator._apply_report_type_cooldown = lambda *_args, **_kwargs: None
    coordinator._is_significant_update = lambda *_args, **_kwargs: True
    coordinator._run_on_hass_loop = lambda *_args, **_kwargs: None
    coordinator._is_on_hass_loop = lambda: True
    coordinator._device_location_data = {}
    coordinator._device_name_cache = {}
    coordinator._device_update_history = {}
    coordinator._device_poll_cooldown_until = {}
    coordinator._present_last_seen = {}
    coordinator._semantic_label_cache = {}
    return coordinator


@pytest.mark.asyncio
async def test_poll_cycle_applies_mapping_before_spam_filter() -> None:
    options = {
        OPT_SEMANTIC_LOCATIONS: {
            "HomeHub": {"latitude": 10.0, "longitude": 20.0, "accuracy": 15.0}
        }
    }
    google_filter = _RaisingFilter()
    coordinator = _polling_coordinator(
        options, google_filter, {"semantic_name": "homehub"}
    )

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    cached = coordinator._device_location_data["dev-1"]
    assert cached["latitude"] == pytest.approx(10.0)
    assert cached["longitude"] == pytest.approx(20.0)
    assert cached["accuracy"] == pytest.approx(15.0)
    assert cached["location_type"] == "trusted"
    assert google_filter.called == 0


@pytest.mark.asyncio
async def test_poll_cycle_preserves_spam_filter_for_unmapped_semantics() -> None:
    google_filter = _TrackingFilter(should_filter=True)
    coordinator = _polling_coordinator({}, google_filter, {"semantic_name": "Office"})

    await coordinator._async_start_poll_cycle([{"id": "dev-2", "name": "Device"}])

    assert "dev-2" not in coordinator._device_location_data
    assert google_filter.called == 1


@pytest.mark.asyncio
async def test_poll_cycle_preserves_coordinates_and_updates_semantic_name() -> None:
    google_filter = _TrackingFilter(should_filter=False)
    coordinator = _polling_coordinator(
        {}, google_filter, {"semantic_name": "Unknown Room", "last_seen": 200}
    )
    coordinator._device_location_data["dev-hybrid"] = {
        "latitude": 50.0,
        "longitude": 10.0,
        "accuracy": 5.0,
        "last_seen": 100.0,
    }

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-hybrid", "name": "Hybrid Device"}]
    )

    cached = coordinator._device_location_data["dev-hybrid"]
    assert cached["latitude"] == pytest.approx(50.0)
    assert cached["longitude"] == pytest.approx(10.0)
    assert cached["semantic_name"] == "Unknown Room"
    assert cached["last_seen"] == pytest.approx(200)


@pytest.mark.asyncio
async def test_poll_cycle_home_substitution_names_the_device_only_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The unattended poll path names the device by id at INFO.

    The display name and the Google Home label are user labels; they appear
    only in the DEBUG companion record (AGENTS.md section 5 (b)).
    """

    google_filter = _TrackingFilter(
        should_filter=False, replacement={"latitude": 1.0, "longitude": 2.0}
    )
    coordinator = _polling_coordinator(
        {}, google_filter, {"semantic_name": "Kitchen Display Sentinel"}
    )

    with caplog.at_level(
        logging.DEBUG, logger="custom_components.googlefindmy.coordinator.polling"
    ):
        await coordinator._async_start_poll_cycle(
            [{"id": "dev-home", "name": "Hall Tag Sentinel"}]
        )

    sentinels = ("Hall Tag Sentinel", "Kitchen Display Sentinel")
    info = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.INFO
        and "substituting with Home coordinates" in record.getMessage()
    ]
    assert len(info) == 1
    assert "dev-home" in info[0]
    assert not any(sentinel in info[0] for sentinel in sentinels)

    debug = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.DEBUG and "substituting" in record.getMessage()
    ]
    assert len(debug) == 1
    assert "dev-home" in debug[0]
    assert all(sentinel in debug[0] for sentinel in sentinels)


def test_push_cache_applies_semantic_mapping() -> None:
    options = {
        OPT_SEMANTIC_LOCATIONS: {
            "Lobby": {"latitude": 5.0, "longitude": 6.0, "accuracy": 7.5}
        }
    }
    coordinator = _push_coordinator(options)

    coordinator.update_device_cache(
        "dev-3", {"semantic_name": "lobby", "last_seen": 1234}
    )

    cached = coordinator._device_location_data["dev-3"]
    assert cached["latitude"] == pytest.approx(5.0)
    assert cached["longitude"] == pytest.approx(6.0)
    assert cached["accuracy"] == pytest.approx(7.5)
    assert cached["location_type"] == "trusted"


@pytest.mark.asyncio
async def test_semantic_labels_are_recorded_with_device_ids() -> None:
    google_filter = _TrackingFilter(should_filter=False)
    coordinator = _polling_coordinator(
        {},
        google_filter,
        {
            "semantic_name": "Lobby",
            "latitude": 1.0,
            "longitude": 2.0,
            "accuracy": 3.0,
        },
    )

    await coordinator._async_start_poll_cycle([{"id": "dev-3", "name": "Device"}])

    observations = coordinator.get_observed_semantic_labels()
    assert [obs.label for obs in observations] == ["Lobby"]
    assert observations[0].devices == {"dev-3"}


class _DecryptFailAPI:
    """API stub whose location lookup always raises the given decryption error.

    Used to drive the coordinator poll loop through the decrypt-failure handlers
    end to end (the isolated escalation logic is unit-tested separately in
    tests/test_coordinator_decrypt_reauth_escalation.py).
    """

    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.calls = 0

    async def async_get_device_location(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self.calls += 1
        raise self._exc


@pytest.mark.asyncio
async def test_poll_cycle_escalates_persistent_decrypt_failure_to_reauth() -> None:
    """End-to-end: a persistent account-wide stale shared key escalates the poll
    loop to ConfigEntryAuthFailed after _MAX_DECRYPT_FAILURES cycles, so Home
    Assistant shows the reauth card. The cycles before the threshold must NOT
    escalate (self-heal still gets a chance)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(SharedKeyMismatchError("stale shared key"))
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)

    # Below threshold: keep polling, no reauth escalation yet.
    for _ in range(_MAX_DECRYPT_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert not any(kw.get("failed") for kw in auth_calls)
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1

    # Threshold cycle: escalate to a reauth flow. The poll cycle runs as a
    # fire-and-forget task, so it starts the entry reauth flow directly rather
    # than raising (a raised ConfigEntryAuthFailed would never reach the awaited
    # coordinator refresh and HA's automatic reauth would never fire).
    coordinator.config_entry.async_start_reauth = MagicMock()
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert any(kw.get("failed") for kw in auth_calls)


@pytest.mark.asyncio
async def test_poll_cycle_stale_owner_key_never_escalates() -> None:
    """End-to-end: a per-tracker StaleOwnerKeyError must never escalate to an
    account-wide reauth (an account reauth would not fix one outdated tracker) and
    must not drive the account-wide decrypt counter."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(StaleOwnerKeyError("tracker v1 < v2"))
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)

    for _ in range(_MAX_DECRYPT_FAILURES + 2):
        await coordinator._async_start_poll_cycle([{"id": "tag", "name": "Tag"}])

    assert coordinator._consecutive_decrypt_failures == 0
    assert not any(kw.get("failed") for kw in auth_calls)


class _DecryptThenSucceedAPI:
    """Raises a decryption error for the first ``fail_times`` calls, then succeeds.

    Used to prove the consecutive-decrypt-failure counter resets on a successful
    cycle (self-heal / recovered shared key), so a later isolated failure does not
    trip a premature reauth.
    """

    def __init__(self, exc: Exception, fail_times: int) -> None:
        self._exc = exc
        self._fail = fail_times
        self.calls = 0

    async def async_get_device_location(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self.calls += 1
        if self.calls <= self._fail:
            raise self._exc
        # A genuinely decryptable report: positive proof the account-wide shared
        # key works again. Only such a real decrypt clears the counter -- an empty
        # / no-reporter response (falsy) is an idle outcome and must NOT (see
        # test_poll_cycle_idle_does_not_reset_decrypt_counter).
        return {"latitude": 1.0, "longitude": 2.0, "accuracy": 5.0}


@pytest.mark.asyncio
async def test_poll_cycle_decrypt_counter_resets_on_success() -> None:
    """A successful poll cycle clears the consecutive-decrypt-failure counter, so a
    recovered shared key (or self-heal) does not later trip a spurious reauth."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptThenSucceedAPI(
        SharedKeyMismatchError("transient stale"), fail_times=_MAX_DECRYPT_FAILURES - 1
    )

    # Below-threshold failing cycles raise no reauth and accumulate the counter.
    for _ in range(_MAX_DECRYPT_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1

    # A successful cycle resets the counter back to zero.
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == 0
    # No per-tracker stale key was seen, so the OK gate also clears the diagnostic
    # error class (status and error class move together). Mutation guard: dropping
    # the OK-gate ``_last_decrypt_error = None`` leaves the stale class behind.
    assert coordinator._last_decrypt_error is None
    assert coordinator.crypto_status.state == CryptoStatus.OK


class _DecryptThenIdleAPI:
    """Raises a decryption error for the first ``fail_times`` calls, then returns
    an empty (no-reporter) result.

    Models an intermittently reporting BLE tracker: failing decrypt cycles are
    interleaved with idle cycles that simply return no location data. Such idle
    cycles carry no positive proof the shared key recovered and must therefore
    NOT clear the accumulated decrypt-failure counter.
    """

    def __init__(self, exc: Exception, fail_times: int) -> None:
        self._exc = exc
        self._fail = fail_times
        self.calls = 0

    async def async_get_device_location(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        self.calls += 1
        if self.calls <= self._fail:
            raise self._exc
        return []  # idle: no reporter in range, nothing decrypted this cycle


@pytest.mark.asyncio
async def test_poll_cycle_idle_does_not_reset_decrypt_counter() -> None:
    """An idle cycle (empty/no-reporter result) must NOT clear accumulated decrypt
    failures: without a real successful decrypt there is no proof the stale shared
    key recovered. Otherwise an intermittently reporting BLE tracker would let idle
    cycles perpetually reset the counter, so the reauth threshold is never reached
    and the user stays stuck with no location updates and no reauth prompt
    (Codex finding on PR #182)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptThenIdleAPI(
        SharedKeyMismatchError("transient stale"), fail_times=_MAX_DECRYPT_FAILURES - 1
    )

    # Below-threshold failing cycles accumulate the counter without escalating.
    for _ in range(_MAX_DECRYPT_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1

    # An idle cycle in between must leave the counter intact (not reset to zero).
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1


class _DecryptThenMetadataOnlyAPI:
    """Raises a decryption error for the first ``fail_times`` calls, then returns a
    ``metadata_only`` sentinel row.

    Models a device whose secrets bundle still yields key *material* (so a
    metadata_only row is emitted) while no encrypted report decrypts. The sentinel
    is truthy but proves nothing about the shared key, so such a cycle must NOT
    clear the accumulated decrypt-failure counter (Codex finding on PR #182).
    """

    def __init__(self, exc: Exception, fail_times: int) -> None:
        self._exc = exc
        self._fail = fail_times
        self.calls = 0

    async def async_get_device_location(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self.calls += 1
        if self.calls <= self._fail:
            raise self._exc
        # metadata_only sentinel: key material from the secrets bundle, no decrypt.
        return {"metadata_only": True, "owner_key_version": 7}


@pytest.mark.asyncio
async def test_poll_cycle_metadata_only_does_not_reset_decrypt_counter() -> None:
    """A metadata_only cycle must NOT clear accumulated decrypt failures: the
    sentinel row carries key material from the secrets bundle, not an authenticated
    decrypt, so it is no proof the stale shared key recovered. Otherwise a
    report-less device emitting metadata_only could let such cycles perpetually
    reset the counter, the reauth threshold is never reached, and the user stays
    stuck with no location updates and no reauth prompt (Codex finding on PR
    #182)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptThenMetadataOnlyAPI(
        SharedKeyMismatchError("transient stale"), fail_times=_MAX_DECRYPT_FAILURES - 1
    )

    # Below-threshold failing cycles accumulate the counter without escalating.
    for _ in range(_MAX_DECRYPT_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1

    # A metadata_only cycle in between must leave the counter intact (not reset).
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1


class _DecryptThenSemanticAPI:
    """Raises a decryption error for the first ``fail_times`` calls, then returns a
    SEMANTIC-shaped record (no coordinates).

    Models a device whose server response is a semantic location ("Home") rather
    than an encrypted fix. The decode layer appends SEMANTIC rows with
    ``decrypted_location=b""`` and skips the crypto path, so the record carries a
    ``semantic_name`` and ``last_seen`` but ``latitude``/``longitude`` are ``None``
    and no ``metadata_only`` flag. It is truthy yet authenticates nothing, so such a
    cycle must NOT clear the accumulated decrypt-failure counter (Codex finding on
    PR #182): a denylist on ``metadata_only`` alone would let it through.
    """

    def __init__(self, exc: Exception, fail_times: int) -> None:
        self._exc = exc
        self._fail = fail_times
        self.calls = 0

    async def async_get_device_location(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self.calls += 1
        if self.calls <= self._fail:
            raise self._exc
        return {
            "last_seen": 123.0,
            "semantic_name": "Home",
            "status": "semantic",
            "latitude": None,
            "longitude": None,
        }


@pytest.mark.asyncio
async def test_poll_cycle_semantic_only_does_not_reset_decrypt_counter() -> None:
    """A SEMANTIC-only cycle must NOT clear accumulated decrypt failures: the row
    skips the crypto path (``decrypted_location=b""``) and carries no coordinates,
    so it is no proof the stale shared key recovered. It also lacks the
    ``metadata_only`` flag, so the previous denylist predicate would have wrongly
    reset the counter and could strand a stale key below the reauth threshold
    (Codex finding on PR #182)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptThenSemanticAPI(
        SharedKeyMismatchError("transient stale"), fail_times=_MAX_DECRYPT_FAILURES - 1
    )

    # Below-threshold failing cycles accumulate the counter without escalating.
    for _ in range(_MAX_DECRYPT_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1

    # A SEMANTIC-only cycle in between must leave the counter intact (not reset).
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1


class _DecryptThenHiddenProofAPI:
    """Raises a decryption error for the first ``fail_times`` calls, then returns a
    SEMANTIC-shaped record that still carries the full-list decrypt proof.

    Models the Codex round-8 finding: the same Nova response decrypted a real
    coordinate report, but ``api._select_best_location`` ranked a newer report-less
    SEMANTIC row first and returned it. ``async_get_device_location`` preserves the
    full-list verdict in the internal ``_decrypt_proven`` hint, so even though the
    visible record has no coordinates the cycle DID prove the shared key and the
    accumulated decrypt-failure counter must reset.
    """

    def __init__(self, exc: Exception, fail_times: int) -> None:
        self._exc = exc
        self._fail = fail_times
        self.calls = 0

    async def async_get_device_location(
        self, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self.calls += 1
        if self.calls <= self._fail:
            raise self._exc
        # Display-selected SEMANTIC row (newest last_seen, no coordinates) but the
        # response decrypted a sibling coordinate fix -> hint records the proof.
        return {
            "last_seen": 200.0,
            "semantic_name": "Home",
            "status": "semantic",
            "latitude": None,
            "longitude": None,
            "_decrypt_proven": True,
        }


@pytest.mark.asyncio
async def test_poll_cycle_hidden_decrypt_proof_resets_counter() -> None:
    """A real decrypt hidden behind a newer SEMANTIC row still clears the budget.

    The display selector collapses the response to a report-less SEMANTIC record,
    but the full-list ``_decrypt_proven`` hint shows a coordinate report decrypted.
    The cycle must therefore reset the consecutive-decrypt-failure counter; without
    carrying the full-list proof the success would be lost and a later transient
    failure could trip a spurious reauth (Codex finding on PR #182)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptThenHiddenProofAPI(
        SharedKeyMismatchError("transient stale"), fail_times=_MAX_DECRYPT_FAILURES - 1
    )

    # Below-threshold failing cycles accumulate the counter without escalating.
    for _ in range(_MAX_DECRYPT_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1

    # The hidden-proof cycle resets the counter back to zero.
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator._consecutive_decrypt_failures == 0
    # The internal hint must not leak into the cached payload.
    cached = coordinator._device_location_data.get("dev-1")
    if isinstance(cached, dict):
        assert "_decrypt_proven" not in cached


class _PerDeviceDecryptAPI:
    """Raises a decryption error for one device id, succeeds (empty) for others."""

    def __init__(self, exc: Exception, failing_id: str) -> None:
        self._exc = exc
        self._failing = failing_id

    async def async_get_device_location(
        self, device_id: str, _device_name: str, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        if device_id == self._failing:
            raise self._exc
        return {}


@pytest.mark.asyncio
async def test_poll_cycle_partial_decrypt_failure_still_escalates() -> None:
    """Multi-device: one tracker decrypts fine while another keeps failing with a
    stale account-wide shared key. A per-device reset would null the counter every
    cycle (one healthy tracker masking the others) and never escalate; the
    cycle-gated reset preserves the count so escalation still fires after the
    threshold. Regression guard for the independent-review finding."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceDecryptAPI(
        SharedKeyMismatchError("stale shared key"), failing_id="bad"
    )
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    devices = [{"id": "good", "name": "Good"}, {"id": "bad", "name": "Bad"}]

    for _ in range(_MAX_DECRYPT_FAILURES - 1):
        await coordinator._async_start_poll_cycle(devices)
    # The healthy device must NOT have reset the account-wide counter.
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1

    coordinator.config_entry.async_start_reauth = MagicMock()
    await coordinator._async_start_poll_cycle(devices)
    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert any(kw.get("failed") for kw in auth_calls)


@pytest.mark.asyncio
async def test_poll_cycle_counts_multi_device_decrypt_failure_once() -> None:
    """Codex P2 regression: one poll cycle in which several trackers all hit the
    same account-wide stale shared key must advance the consecutive-failure counter
    exactly ONCE, not once per device.

    Counting per device let a multi-device account cross _MAX_DECRYPT_FAILURES
    within the first cycle and escalate to reauth immediately, defeating the
    documented "N consecutive cycles" budget that gives the in-decrypt self-heal a
    few cycles before re-authentication."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(SharedKeyMismatchError("stale shared key"))
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    # More failing trackers in a single cycle than the escalation threshold: a
    # per-device counter would already escalate here.
    devices = [
        {"id": "dev-1", "name": "One"},
        {"id": "dev-2", "name": "Two"},
        {"id": "dev-3", "name": "Three"},
    ]
    assert len(devices) >= _MAX_DECRYPT_FAILURES

    # First cycle: counter advances by one, no escalation despite 3 failing devices.
    await coordinator._async_start_poll_cycle(devices)
    assert coordinator._consecutive_decrypt_failures == 1
    assert not any(kw.get("failed") for kw in auth_calls)

    # Second cycle: still below threshold (2/3).
    await coordinator._async_start_poll_cycle(devices)
    assert coordinator._consecutive_decrypt_failures == _MAX_DECRYPT_FAILURES - 1
    assert not any(kw.get("failed") for kw in auth_calls)

    # Third consecutive cycle reaches the threshold -> escalate exactly once.
    coordinator.config_entry.async_start_reauth = MagicMock()
    await coordinator._async_start_poll_cycle(devices)
    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert any(kw.get("failed") for kw in auth_calls)


@pytest.mark.asyncio
async def test_poll_cycle_nova_permanent_auth_starts_reauth() -> None:
    """A permanent Nova auth failure in the background poll cycle starts the entry
    reauth flow directly. The cycle runs as a fire-and-forget task
    (``hass.async_create_task``), so a raised ConfigEntryAuthFailed would never reach
    the awaited coordinator refresh and HA's automatic reauth would never fire."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(NovaAuthPermanentError(401, "perm"))
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator.config_entry.async_start_reauth = MagicMock()

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert any(kw.get("failed") for kw in auth_calls)
    # The poll site tags the exception so diagnostics record the specific cause.
    assert coordinator._reauth_reason is not None
    assert coordinator._reauth_reason.code is ReauthReasonCode.NOVA_AUTH_PERMANENT


@pytest.mark.asyncio
async def test_poll_cycle_spot_auth_permanent_tags_reauth_code() -> None:
    """A permanent Spot/AAS auth failure in the poll cycle records
    SPOT_AUTH_PERMANENT (a general Spot-transport condition, not owner-key)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(
        SpotAuthPermanentError("AAS token invalid after refresh.")
    )
    coordinator.config_entry.async_start_reauth = MagicMock()

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert coordinator._reauth_reason is not None
    assert coordinator._reauth_reason.code is ReauthReasonCode.SPOT_AUTH_PERMANENT


@pytest.mark.asyncio
async def test_poll_cycle_spot_empty_response_tags_reauth_code() -> None:
    """An empty SPOT GetEidInfo response in the poll cycle records
    OWNER_KEY_EMPTY_RESPONSE (SpotApiEmptyResponseError is raised only from the
    EID/owner-key retrieval layer, so the owner-key code names the same root)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(
        SpotApiEmptyResponseError("SPOT returned an empty response")
    )
    coordinator.config_entry.async_start_reauth = MagicMock()

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert coordinator._reauth_reason is not None
    assert coordinator._reauth_reason.code is ReauthReasonCode.OWNER_KEY_EMPTY_RESPONSE


@pytest.mark.asyncio
async def test_poll_cycle_transient_nova_auth_starts_reauth_after_threshold() -> None:
    """A transient Nova auth failure escalates only after
    _MAX_TRANSIENT_AUTH_FAILURES consecutive cycles, then starts the entry reauth
    flow directly (the fire-and-forget poll task cannot raise into HA)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(NovaAuthError(401, "transient"))
    coordinator.config_entry.async_start_reauth = MagicMock()

    # Below threshold: keep polling, no reauth escalation yet.
    for _ in range(_MAX_TRANSIENT_AUTH_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    coordinator.config_entry.async_start_reauth.assert_not_called()

    # Threshold cycle escalates exactly once.
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    # Distinct code from the immediate NOVA_AUTH_FAILED: this is the
    # retries-exhausted case, and the counter snapshot proves the escalation.
    assert coordinator._reauth_reason is not None
    assert (
        coordinator._reauth_reason.code
        is ReauthReasonCode.NOVA_AUTH_TRANSIENT_EXHAUSTED
    )
    assert (
        coordinator._reauth_reason.counters["consecutive_transient_auth_failures"]
        == _MAX_TRANSIENT_AUTH_FAILURES
    )


@pytest.mark.asyncio
async def test_poll_cycle_client_error_never_starts_reauth() -> None:
    """A rejected REQUEST must not feed the transient-auth countdown.

    NovaAuthError covers every non-retryable 4xx, so a device removed from the
    account raised the counter once per cycle and produced a re-auth prompt
    after exactly three of them, with the sign-in intact throughout.

    Reachability note: async_get_device_location passes such a status through,
    so this branch is what a rejected device really takes. The API double
    bypasses api.py entirely, so what this test proves is that the rule holds
    locally in the file that owns the counter; the seam itself is pinned by
    test_a_non_credential_4xx_location_is_passed_through. Not proved here: the
    path is still walked.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(NovaAuthError(404, "gone"))
    coordinator.config_entry.async_start_reauth = MagicMock()
    # The base fixture stubs _set_auth_state with a no-op, which would let a
    # re-inserted call slip through unseen; bind it the way this file already
    # does elsewhere so the assertion below can observe anything at all.
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)

    for _ in range(_MAX_TRANSIENT_AUTH_FAILURES + 2):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    coordinator.config_entry.async_start_reauth.assert_not_called()
    assert coordinator._consecutive_transient_auth_failures == 0
    assert not [c for c in auth_calls if c.get("failed")]


@pytest.mark.asyncio
async def test_poll_cycle_client_error_is_recorded_as_an_ordinary_error() -> None:
    """Quiet is not the same as invisible: this branch keeps the skip on record.

    api.py passes such a status through, so a rejected device really does take
    this branch and really does show up in the diagnostics. What this pins is
    that the branch does not degrade into a silent skip.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(NovaAuthError(404, "gone"))
    coordinator.config_entry.async_start_reauth = MagicMock()
    # The polling fixture stubs note_error with a no-op as well.
    coordinator.note_error = MagicMock()

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert coordinator.note_error.call_count == 1
    assert coordinator.note_error.call_args.kwargs["where"] == "poll_client_error"


@pytest.mark.asyncio
async def test_poll_cycle_credential_rejection_still_escalates() -> None:
    """The counterpart, and with 403: the existing test only covers 401.

    Without this row the narrowing is satisfied by a branch that never counts
    anything, which would bury a real expired sign-in.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(NovaAuthError(403, "denied"))
    coordinator.config_entry.async_start_reauth = MagicMock()

    for _ in range(_MAX_TRANSIENT_AUTH_FAILURES):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert coordinator._reauth_reason is not None
    assert (
        coordinator._reauth_reason.code
        is ReauthReasonCode.NOVA_AUTH_TRANSIENT_EXHAUSTED
    )


@pytest.mark.asyncio
async def test_a_client_error_does_not_clear_a_pending_auth_countdown() -> None:
    """The counter is untouched in BOTH directions.

    A 404 says nothing about the credentials -- neither that they are broken
    nor that they work. A test that only checks "stays at 0" would let a reset
    through, and a reset would make a real 401/404/401 sequence unescalatable.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    api = _DecryptFailAPI(NovaAuthError(401, "transient"))
    coordinator.api = api
    coordinator.config_entry.async_start_reauth = MagicMock()

    for _ in range(_MAX_TRANSIENT_AUTH_FAILURES - 1):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    coordinator.config_entry.async_start_reauth.assert_not_called()

    api._exc = NovaAuthError(404, "gone")
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    coordinator.config_entry.async_start_reauth.assert_not_called()

    api._exc = NovaAuthError(401, "transient")
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )


class _PerDeviceAPI:
    """Location stub that answers per device id: raise, or return a payload.

    ``_DecryptFailAPI`` always raises the same error for every device, which
    cannot express "one tracker is rejected while another one is fine" -- the
    exact shape in which a rejected device masks a real credential failure.
    """

    def __init__(self, answers: dict[str, Any]) -> None:
        self._answers = answers
        self.calls = 0

    async def async_get_device_location(
        self, device_id: str, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        self.calls += 1
        answer = self._answers[device_id]
        if isinstance(answer, Exception):
            raise answer
        return answer


@pytest.mark.asyncio
async def test_a_rejected_device_never_clears_the_auth_state() -> None:
    """The reported defect, at the seam where it lives.

    ``api.async_get_device_location`` passes a non-credential 4xx through
    instead of returning ``{}``, so the poll loop never reaches the success
    path for that device. What that was worth when this test was written, kept
    as history: the reset ran BEFORE the empty guard back then, so a returned
    ``{}`` would have called ``_set_auth_state(failed=False)`` and reset the
    transient counter, and a permanently deleted tracker would have wiped a
    pending 401 from another tracker in every single cycle. The reset has since
    moved behind that guard -- see
    ``test_an_empty_return_proves_nothing_about_the_credentials`` -- so the seam
    now holds twice over: the rejection never reaches the success path, and an
    empty result would prove nothing there if it did.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(NovaAuthError(404, "gone"))
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator._consecutive_transient_auth_failures = 2
    # Seed the declared type, not an exception object: `_last_transient_auth_error`
    # is `str | None` (polling.py:189) and its only production writer stores
    # `str(transient_err)`. `mypy` does not catch a wrong shape here because
    # `pyproject.toml` sets `ignore_errors` for `tests.*`.
    coordinator._last_transient_auth_error = "earlier"

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert not [kw for kw in auth_calls if kw.get("failed") is False]
    assert coordinator._consecutive_transient_auth_failures == 2
    # Unchanged, not merely non-empty: a client error must not overwrite the
    # message that a real transient auth failure left behind.
    assert coordinator._last_transient_auth_error == "earlier"


@pytest.mark.asyncio
async def test_an_empty_return_proves_nothing_about_the_credentials() -> None:
    """The reset moved behind the empty guard; this is the same test, inverted.

    History, kept deliberately: this function used to be called
    ``test_an_empty_return_still_clears_the_counter`` and asserted the opposite
    of what it asserts now. It was a characterisation of a reset that was known
    to be wrong on its own terms and was left alone at the time, carried in a
    plan of its own instead. This is the day it changes, and it changes on
    purpose.

    What is wrong with the old behaviour: an empty dict is WEAK evidence that
    the request was accepted, not proof of it. The 5xx, the 429, the network
    error and the failed FCM registration now raise
    ``LocationRequestNotAcceptedError`` instead of flattening into ``{}``, but
    four pre-accept failures still arrive here as an empty dict (enumerated in
    ``test_a_cycle_of_only_empty_results_still_reports_success``). Clearing the
    auth state and the transient counter on that evidence is the false-success
    reasoning this change removes: the counter exists to escalate a genuinely
    expired login after three cycles, and a fleet with one idle BLE tag reset
    it in every single cycle, so the threshold was never reached.

    The positive half is not given up, it moves: a location WITH content still
    clears both, which is ``test_a_real_location_still_clears_the_auth_state``
    and ``test_a_real_location_still_resets_the_transient_counter``.

    AP-7 amendment, and the counter assertions below are inverted a second time
    because of it. What this test owns is unchanged and is what its name says:
    an empty return proves NOTHING about the credentials, so the auth state is
    untouched and nothing is cleared AT THIS DEVICE SITE. What changed is one
    layer up. The counter counts consecutive poll CYCLES in which the action RPC
    rejected, and a cycle in which no device rejected while at least one request
    was accepted breaks that streak at the end of the loop. Leaving the reset on
    the device-list refresh instead was measured twice and failed twice: the
    refresh runs on an independently configurable cadence, which starved the
    escalation at one ratio and escalated non-consecutive rejections at another.
    Weak evidence is enough to break a streak of rejections; it is not enough to
    clear an auth state, and that difference is the whole of what this test
    still pins.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI({"dev-1": {}})
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "expired"

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert not [kw for kw in auth_calls if kw.get("failed") is False]
    # Cleared by the end-of-cycle streak break, not by the device site: no
    # device rejected in this cycle and the empty return counts as an accepted
    # request. Under AP-1 this read 2 and "expired".
    assert coordinator._consecutive_transient_auth_failures == 0
    assert coordinator._last_transient_auth_error is None


@pytest.mark.asyncio
async def test_a_real_location_still_clears_the_auth_state() -> None:
    """The positive half of the rule must survive the fix.

    Moving the reset behind the empty guard is only correct if a location WITH
    content still counts as proof. Without this test the fix could be "improved"
    into removing the reset altogether, which would strand a pending auth error
    until the next device-list refresh.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": {
                "latitude": 50.0,
                "longitude": 10.0,
                "accuracy": 5.0,
                "last_seen": 100.0,
            }
        }
    )
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert [kw for kw in auth_calls if kw.get("failed") is False]


@pytest.mark.asyncio
async def test_a_real_location_still_resets_the_transient_counter() -> None:
    """Same as above for the counter and the stored cause.

    Separate from the auth-state test on purpose: the two live in one block
    today, and a fix that moves only one of them would otherwise pass.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": {
                "latitude": 50.0,
                "longitude": 10.0,
                "accuracy": 5.0,
                "last_seen": 100.0,
            }
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator._consecutive_transient_auth_failures = 2
    coordinator._last_transient_auth_error = "expired"

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert coordinator._consecutive_transient_auth_failures == 0
    assert coordinator._last_transient_auth_error is None


@pytest.mark.asyncio
async def test_an_empty_return_does_not_clear_a_pending_auth_error() -> None:
    """The cross-device case, which is why the order of the guard matters.

    One tracker raises a credential failure, a second one comes back empty in
    the same cycle. With the reset in front of the empty guard, the second
    device wiped the first device's finding on every pass -- and in a fleet with
    one idle BLE tag that is every pass, forever. The devices are ordered so the
    empty one is polled last, which is the order that used to lose the finding.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {"dev-1": NovaAuthError(401, "expired"), "dev-2": {}}
    )
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator.config_entry.async_start_reauth = MagicMock()

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Hub"}, {"id": "dev-2", "name": "Tag"}]
    )

    assert not [kw for kw in auth_calls if kw.get("failed") is False]


@pytest.mark.asyncio
async def test_a_client_error_does_not_overwrite_an_earlier_failure() -> None:
    """``last_exception`` keeps the FIRST failure of the cycle.

    A credential failure on one tracker must stay the reported cause even when
    a rejected tracker follows it in the same cycle; otherwise the surviving
    error names the harmless device and hides the one that needs attention.
    This is the easy order. The hard one -- rejection first, credential failure
    second -- is
    ``test_a_rejected_device_does_not_hide_a_later_credential_failure``, and it
    is the one that used to fail: the client branch no longer claims the
    ``last_exception`` slot at all, so the order stopped mattering.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": NovaAuthError(401, "expired"),
            "dev-2": NovaAuthError(404, "gone"),
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    # The polling fixture stubs note_error with a no-op; the reachability
    # assertion below needs a recording double.
    coordinator.note_error = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Hub"}, {"id": "dev-2", "name": "Gone"}]
    )

    assert recorded, "the cycle reported no error at all"
    assert getattr(recorded[-1], "status", None) == 401
    # The name says "client error", so pin that the client branch really ran.
    # Without this the test passes even if that branch is deleted outright,
    # because the 404 would then fall through to the transient-auth path whose
    # own `if last_exception is None` guard preserves the 401 just the same.
    assert any(
        call.kwargs.get("where") == "poll_client_error"
        for call in coordinator.note_error.call_args_list
    )


@pytest.mark.asyncio
async def test_poll_cycle_reauth_noop_without_config_entry() -> None:
    """Defensive guard: with no config entry bound the poll cycle cannot start
    reauth, but it must not crash (``_request_poll_reauth`` no-entry branch)."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(SharedKeyMismatchError("stale shared key"))
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator.config_entry = None

    # Drive to the escalation threshold; the helper hits its no-entry guard.
    for _ in range(_MAX_DECRYPT_FAILURES):
        await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    # No crash, and the auth state was still flagged at the threshold cycle.
    assert any(kw.get("failed") for kw in auth_calls)


@pytest.mark.asyncio
async def test_poll_cycle_direct_config_entry_auth_failed_starts_reauth() -> None:
    """A ConfigEntryAuthFailed raised directly by the API (e.g. HTTP 401/403 in
    async_get_device_location) reaches the poll loop's ConfigEntryAuthFailed handler
    without passing through the typed SpotAuth/NovaAuth handlers. It must start the
    entry reauth flow directly rather than re-raising into the fire-and-forget poll
    task, where the exception would never reach HA."""
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(ConfigEntryAuthFailed("session expired"))
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator.config_entry.async_start_reauth = MagicMock()

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert any(kw.get("failed") for kw in auth_calls)


class _MixedDecryptAPI:
    """API stub that fails for one device id and returns a payload for others.

    Drives a single poll cycle that contains both a per-tracker stale key and a
    healthy, decrypting device, to prove the crypto sensor does not flicker the
    tracker_key_outdated state to OK when a sibling decrypts successfully.
    """

    def __init__(
        self, fail_id: str, exc: Exception, ok_payload: dict[str, Any]
    ) -> None:
        self._fail_id = fail_id
        self._exc = exc
        self._ok_payload = ok_payload

    async def async_get_device_location(
        self, device_id: str, *_args: Any, **_kwargs: Any
    ) -> dict[str, Any]:
        if device_id == self._fail_id:
            raise self._exc
        return dict(self._ok_payload)


@pytest.mark.asyncio
async def test_poll_cycle_successful_decrypt_sets_crypto_ok() -> None:
    """A cycle that decrypts usable location data reports crypto OK (D6 source)."""

    coordinator = _polling_coordinator(
        {}, _TrackingFilter(), {"latitude": 1.0, "longitude": 2.0, "last_seen": 100}
    )

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert coordinator.crypto_status.state == CryptoStatus.OK


@pytest.mark.asyncio
async def test_poll_cycle_shared_key_failure_sets_crypto_invalid() -> None:
    """An account-wide SharedKeyMismatchError surfaces as shared_key_invalid."""

    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(SharedKeyMismatchError("stale shared key"))
    coordinator._set_auth_state = lambda **_kwargs: None

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert coordinator.crypto_status.state == CryptoStatus.SHARED_KEY_INVALID


@pytest.mark.asyncio
async def test_poll_cycle_stale_key_sets_tracker_outdated() -> None:
    """A per-tracker StaleOwnerKeyError surfaces as tracker_key_outdated."""

    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(StaleOwnerKeyError("tracker v1 < v2"))
    coordinator._set_auth_state = lambda **_kwargs: None

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])

    assert coordinator.crypto_status.state == CryptoStatus.TRACKER_KEY_OUTDATED


@pytest.mark.asyncio
async def test_poll_cycle_timeout_only_does_not_clear_prior_failure() -> None:
    """A cycle without any successful decrypt must NOT downgrade a prior
    shared_key_invalid to OK -- absence of an error is not proof of health.

    Mutation guard for the ``cycle_had_successful_decrypt`` gate: replacing it
    with the weaker ``devices`` condition turns the second assertion red.
    """

    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator._set_auth_state = lambda **_kwargs: None

    # Cycle 1: account-wide failure -> shared_key_invalid (below threshold).
    coordinator.api = _DecryptFailAPI(SharedKeyMismatchError("stale shared key"))
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator.crypto_status.state == CryptoStatus.SHARED_KEY_INVALID

    # Cycle 2: idle (empty location, no successful decrypt) -> state unchanged.
    coordinator.api = _DummyAPI({})
    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Hub"}])
    assert coordinator.crypto_status.state == CryptoStatus.SHARED_KEY_INVALID


@pytest.mark.asyncio
async def test_poll_cycle_mixed_stale_and_success_keeps_tracker_outdated() -> None:
    """A healthy sibling in a cycle that also hit a stale key must NOT flicker the
    sensor to OK.

    Mutation guard for the ``not cycle_had_stale_key`` gate: dropping it lets the
    successful sibling overwrite tracker_key_outdated with OK and turns this red.
    """

    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _MixedDecryptAPI(
        "dev-stale",
        StaleOwnerKeyError("tracker v1 < v2"),
        {"latitude": 1.0, "longitude": 2.0, "last_seen": 100},
    )
    coordinator._set_auth_state = lambda **_kwargs: None

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-stale", "name": "Old"}, {"id": "dev-ok", "name": "Hub"}]
    )

    assert coordinator.crypto_status.state == CryptoStatus.TRACKER_KEY_OUTDATED


@pytest.mark.asyncio
async def test_poll_cycle_mixed_stale_and_success_keeps_last_decrypt_error() -> None:
    """The stale-key diagnostic must survive a sibling's successful decrypt.

    Same mixed cycle as above, but asserting the *error class* the encryption-key
    status sensor exposes (``last_decrypt_error_class`` from ``_last_decrypt_error``).
    The successful sibling clears the account-wide reauth budget via the shared
    success entry point, yet the per-tracker StaleOwnerKeyError diagnostic must
    stay so the sensor can still report which class is outdated.

    Regression guard for the Codex finding: clearing ``_last_decrypt_error``
    unconditionally in ``note_decrypt_success()`` (or gating it only on the
    counter) wipes this on the very cycle a sibling decrypts and turns this red.
    """

    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _MixedDecryptAPI(
        "dev-stale",
        StaleOwnerKeyError("tracker v1 < v2"),
        {"latitude": 1.0, "longitude": 2.0, "last_seen": 100},
    )
    coordinator._set_auth_state = lambda **_kwargs: None

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-stale", "name": "Old"}, {"id": "dev-ok", "name": "Hub"}]
    )

    # Status stays outdated AND the error class behind it survives for triage.
    assert coordinator.crypto_status.state == CryptoStatus.TRACKER_KEY_OUTDATED
    assert coordinator._last_decrypt_error is not None
    assert coordinator._last_decrypt_error.split(":", 1)[0] == "StaleOwnerKeyError"


@pytest.mark.asyncio
async def test_manual_locate_spot_auth_permanent_tags_reauth_code() -> None:
    """The direct manual-locate SpotAuthPermanentError site records
    SPOT_AUTH_PERMANENT, the SAME canonical code the poll-cycle equivalent uses
    (polling.py) for the same exception type. It must NOT record an owner-key code:
    SpotAuthPermanentError is a generic Spot-transport auth failure. Guards the
    poll-vs-direct code-consistency invariant (a mislabel would point diagnostics
    triage at the wrong credential layer)."""
    coordinator = _base_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(
        SpotAuthPermanentError("AAS token invalid after refresh.")
    )
    coordinator.config_entry.async_start_reauth = MagicMock()

    with pytest.raises(HomeAssistantError):
        await coordinator.async_locate_device("device-1")

    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert coordinator._reauth_reason is not None
    assert coordinator._reauth_reason.code is ReauthReasonCode.SPOT_AUTH_PERMANENT


@pytest.mark.asyncio
async def test_manual_locate_stale_shared_key_tags_reauth_code() -> None:
    """The direct manual-locate account-wide DecryptionError site records
    DECRYPT_STALE_KEY, the SAME canonical code the poll-cycle equivalent uses
    (_finalize_cycle_decrypt_state). It must NOT record an AAS/token code: the
    condition is a stale shared key, a different credential layer. Guards the
    poll-vs-direct code-consistency invariant (the Codex finding on PR #1160)."""
    coordinator = _base_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _DecryptFailAPI(SharedKeyMismatchError("stale shared key"))
    # Seed the account-wide counter one below the threshold so this single locate
    # call crosses it and escalates (the first escalation always fires: the
    # cooldown sentinel is None).
    coordinator._consecutive_decrypt_failures = _MAX_DECRYPT_FAILURES - 1
    coordinator.config_entry.async_start_reauth = MagicMock()

    with pytest.raises(HomeAssistantError):
        await coordinator.async_locate_device("device-1")

    coordinator.config_entry.async_start_reauth.assert_called_once_with(
        coordinator.hass
    )
    assert coordinator._reauth_reason is not None
    assert coordinator._reauth_reason.code is ReauthReasonCode.DECRYPT_STALE_KEY


@pytest.mark.asyncio
async def test_a_rejected_device_does_not_make_every_tracker_unavailable() -> None:
    """A per-device rejection must not take the whole account offline.

    ``last_exception`` is the sole driver of ``async_set_update_error`` in the
    cycle's ``finally`` block, and ``GoogleFindMyEntity.available`` follows the
    coordinator's ``last_update_success``. Recording a client rejection there
    therefore marked EVERY tracker entity unavailable -- and unlike a 5xx, a
    tracker deleted from the account never recovers, so the outage would repeat
    on every single poll until the device left the cached list. A rejection of
    one device says nothing about the others.

    Note precisely what the sibling here does: it returns ``{}``. When this test
    was written that was indistinguishable from a 5xx, and the pair
    ``test_a_mixed_cycle_of_rejection_and_empty_siblings_stays_silent`` recorded
    the same observable state read with the opposite expectation -- an ambiguity
    that could only be resolved where the empty result is produced.

    It has been, and the twin now reads the state the same way this one does: a
    5xx raises ``LocationRequestNotAcceptedError`` instead of returning ``{}``,
    so the two are no longer one state under two names. What this test still
    holds is unchanged and unaffected by that: a rejected tracker must not take
    the account offline while a sibling is answering.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI({"dev-1": NovaAuthError(404, "gone"), "dev-2": {}})
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Gone"}, {"id": "dev-2", "name": "Hub"}]
    )

    assert not recorded, (
        "a rejected device failed the coordinator update, which makes every "
        f"tracker entity unavailable: {recorded}"
    )
    # Reachability, so the negative assertion cannot pass vacuously: both
    # devices were actually requested, and the cycle did enter the client
    # branch (which is the only thing that can mark this cycle failed -- the
    # empty success path of dev-2 leaves `cycle_failed` alone).
    assert coordinator.api.calls == 2
    assert coordinator.last_poll_result == "failed"


@pytest.mark.asyncio
async def test_a_rejected_device_still_marks_the_poll_result_failed() -> None:
    """The rejection is recorded, it is only not made account-wide.

    ``cycle_failed`` and ``last_exception`` drive two different things:
    ``cycle_failed`` only writes the ``last_poll_result`` diagnostic attribute
    (``binary_sensor.py`` reads it), while ``last_exception`` drives
    ``async_set_update_error`` and with it entity availability. The branch keeps
    the former so the cycle stays honest about not having polled every device,
    and drops the latter so one device cannot take the account offline.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI({"dev-1": NovaAuthError(404, "gone"), "dev-2": {}})
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    coordinator.async_set_update_error = lambda _exc: None

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Gone"}, {"id": "dev-2", "name": "Hub"}]
    )

    assert coordinator.last_poll_result == "failed"


@pytest.mark.asyncio
async def test_a_rejected_device_does_not_hide_a_later_credential_failure() -> None:
    """Order matters: the reported cause must name the device that needs help.

    ``last_exception`` keeps the FIRST failure of a cycle. While the client
    rejection claimed that slot, a rejected tracker polled before a genuinely
    expired one made the coordinator report "HTTP 404 gone" and hid the 401
    entirely. This is the mirror image of
    ``test_a_client_error_does_not_overwrite_an_earlier_failure``, which pins
    the same guard from the other side.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": NovaAuthError(404, "gone"),
            "dev-2": NovaAuthError(401, "expired"),
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Gone"}, {"id": "dev-2", "name": "Hub"}]
    )

    assert recorded, "the cycle hid the credential failure entirely"
    assert getattr(recorded[-1], "status", None) == 401, (
        "the reported cause names the harmless rejected device instead of the "
        f"tracker whose sign-in expired: {recorded[-1]!r}"
    )


@pytest.mark.asyncio
async def test_a_cycle_where_every_device_is_rejected_still_reports_an_error() -> None:
    """The sibling-success rule needs a sibling. With none, the cycle failed.

    A rejection is treated as a per-device skip because another device's
    success refutes it as an account-wide problem. When EVERY device is
    rejected there is no such refutation, and staying silent would leave the
    coordinator reporting healthy while it delivered nothing at all. The
    device list cannot be relied on to catch this one layer up: it is a
    different RPC, and `DEVICE_LIST_POLL_INTERVAL` (300s) means most cycles
    reuse the cached list without calling it at all.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {"dev-1": NovaAuthError(404, "gone"), "dev-2": NovaAuthError(400, "bad")}
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Gone"}, {"id": "dev-2", "name": "Bad"}]
    )

    assert recorded, "every device was rejected and the cycle still reported success"
    assert coordinator.last_poll_result == "failed"


@pytest.mark.asyncio
async def test_a_cycle_of_only_empty_results_still_reports_success() -> None:
    """The reference case that must NOT flip: healthy idle tags stay successful.

    This test was the pinned known gap. It kept a cycle in which every request
    had failed server-side at ``success``, because the empty dict could not be
    told from a healthy idle BLE tag. The ambiguity is largely gone: the 5xx,
    the 429, the network error and the failed FCM registration now raise
    ``LocationRequestNotAcceptedError`` instead of flattening into ``{}``.

    Largely, not entirely, and the difference is worth keeping straight. Four
    pre-accept failures still arrive here as an empty dict, because they are
    raised before the handler that would convert them: an unregistered FCM
    receiver provider, a provider that returns ``None``, a missing token cache,
    and a failure while binding the lazily imported decrypt / eid-info modules.
    Count the failure modes, not the clauses: an earlier revision of this
    docstring grouped the two provider guards into one and wrote "three", which
    contradicted the "four" at the head of this file. So an empty dict is now
    WEAK evidence that the request was accepted, not proof of it.

    Its name and its meaning changed with that; its expectation deliberately did
    not. An account of BLE tags with no reporter nearby is the ordinary healthy
    state, it produces exactly this cycle, and it must never be reported as a
    failure. This is therefore the counter-weight to
    ``test_a_cycle_where_no_request_was_accepted_reports_an_error``: the two
    together are the claim that the outcomes became DISTINGUISHABLE, rather than
    that everything was declared broken.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI({"dev-1": {}, "dev-2": {}})
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "A"}, {"id": "dev-2", "name": "B"}]
    )

    assert not recorded, f"an all-empty cycle surfaced an error: {recorded}"
    assert coordinator.last_poll_result == "success"
    # Reachability, so neither assertion can pass vacuously.
    assert coordinator.api.calls == 2


@pytest.mark.asyncio
async def test_a_mixed_cycle_of_rejection_and_empty_siblings_stays_silent() -> None:
    """One rejected tracker plus a sibling that came back empty: still silent.

    This too was a pinned known gap, and it too keeps its expectation while
    losing its doubt. The old reasoning was that an empty sibling proves
    nothing, so the guard could not lean on it either way. It leans on it only
    in the safe direction, which is the only direction it may lean: an empty
    sibling makes the guard STAY SILENT, and the guard is deliberately built so
    that anything it fails to recognise has that effect. It is emphatically not
    read as proof that the sibling's request was accepted. One deleted tracker plus one sibling that came back empty is not
    grounds for taking the whole account offline.

    The mirror case is
    ``test_a_mixed_cycle_of_rejection_and_unaccepted_siblings_now_surfaces``:
    same rejection, but the sibling never got through, and there the cycle
    surfaces. The pair is what makes the two states distinguishable at this
    layer instead of collapsed into one.

    Not silent everywhere, and that is the point: the rejection still sets
    ``cycle_failed``, so ``last_poll_result`` reports ``failed`` and the
    diagnostic binary sensor shows it. Only entity availability is left alone,
    which is the deliberate trade -- one deleted tracker must not take the whole
    account offline on every poll.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI({"dev-1": NovaAuthError(404, "gone"), "dev-2": {}})
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Gone"}, {"id": "dev-2", "name": "Idle"}]
    )

    assert not recorded, f"the mixed cycle surfaced an error: {recorded}"
    # The failure IS recorded, just not account-wide.
    assert coordinator.last_poll_result == "failed"
    assert coordinator.api.calls == 2


@pytest.mark.asyncio
async def test_a_cycle_where_no_request_was_accepted_reports_an_error() -> None:
    """The defect this whole change exists to remove, at the cycle level.

    Every tracker hit a 5xx, so not one request was accepted. Before the
    signal existed each of those collapsed into an empty dict, the loop read
    that as an ordinary idle poll, and the coordinator reported ``success``
    while it had delivered nothing at all -- every entity stayed available with
    a cached position that could be hours old, and nothing anywhere said so.

    The counter-weight is
    ``test_a_cycle_of_only_empty_results_still_reports_success``: identical
    shape, empty results instead of refusals, and it must stay green. Without
    that pair this test could be satisfied by declaring every quiet cycle
    broken, which is not a fix but a different defect.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": LocationRequestNotAcceptedError(stage="server_error", status=503),
            "dev-2": LocationRequestNotAcceptedError(stage="server_error", status=503),
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "A"}, {"id": "dev-2", "name": "B"}]
    )

    assert recorded, "no request was accepted and the cycle still reported success"
    assert coordinator.last_poll_result == "failed"
    # Reachability, so neither assertion can pass vacuously.
    assert coordinator.api.calls == 2


@pytest.mark.asyncio
async def test_the_surfaced_error_names_which_counter_fired() -> None:
    """Two ways to reach the same verdict must not read as the same event.

    A deleted tracker (the server's answer ABOUT that device) and a request that
    was never accepted (no answer about the device at all) both leave the cycle
    with nothing, but they need different answers from whoever reads the log:
    one is a configuration change, the other is an outage. The two guards are
    kept apart for exactly that, so this pins that the message says which one
    fired instead of a single generic sentence.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {"dev-1": LocationRequestNotAcceptedError(stage="rate_limited", status=429)}
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "A"}])

    assert recorded
    message = str(recorded[-1])
    assert "was accepted this cycle" in message, message
    assert "1 not accepted" in message, message
    # Not the rejection wording: that guard must not have claimed this cycle.
    assert "was rejected by" not in message, message
    assert recorded[-1].translation_domain == DOMAIN
    assert recorded[-1].translation_key == "locate_not_accepted"
    assert recorded[-1].translation_placeholders == {
        "not_accepted": "1",
        "rejected": "0",
    }


@pytest.mark.asyncio
async def test_a_single_unaccepted_device_does_not_make_every_tracker_unavailable() -> (
    None
):
    """One refused tracker with a sibling that got through stays a skip.

    This is the guard rail against over-correcting. ``last_exception`` drives
    ``async_set_update_error`` and with it entity availability, so recording it
    per device would take the whole account offline whenever a single tracker
    hit a transient 5xx. The sibling's empty dict is what refutes the
    account-wide reading -- not because an empty dict proves the sibling's
    request was accepted (it does not, see
    ``test_a_cycle_of_only_empty_results_still_reports_success``), but because
    the guard demands that EVERY device missed and this cycle does not meet
    that.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": LocationRequestNotAcceptedError(stage="server_error", status=503),
            "dev-2": {},
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Refused"}, {"id": "dev-2", "name": "Idle"}]
    )

    assert not recorded, (
        "one refused tracker failed the coordinator update, which makes every "
        f"tracker entity unavailable: {recorded}"
    )
    # Reachability: both devices were requested and the cycle really did enter
    # the not-accepted branch -- the empty success path of dev-2 leaves
    # `cycle_failed` alone, so `failed` here can only come from dev-1.
    assert coordinator.api.calls == 2
    assert coordinator.last_poll_result == "failed"


@pytest.mark.asyncio
async def test_a_single_unaccepted_device_still_marks_the_poll_result_failed() -> None:
    """The miss is recorded, it is only not made account-wide.

    Same split as the rejection branch: ``cycle_failed`` writes the
    ``last_poll_result`` diagnostic attribute that ``binary_sensor.py`` reads,
    while ``last_exception`` drives availability. Keeping the former is what
    stops the cycle from claiming it polled every device.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": LocationRequestNotAcceptedError(stage="network_error"),
            "dev-2": {},
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    coordinator.async_set_update_error = lambda _exc: None

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Refused"}, {"id": "dev-2", "name": "Idle"}]
    )

    assert coordinator.last_poll_result == "failed"


@pytest.mark.asyncio
async def test_a_mixed_cycle_of_rejection_and_unaccepted_siblings_now_surfaces() -> (
    None
):
    """A deleted tracker plus a request never accepted: nothing got through.

    The mirror of
    ``test_a_mixed_cycle_of_rejection_and_empty_siblings_stays_silent``, and the
    reason the guard sums the two counters instead of testing either alone.
    Neither count reaches ``len(devices)`` here, so a guard on one of them would
    stay silent through a cycle in which no device produced any evidence at all.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": NovaAuthError(404, "gone"),
            "dev-2": LocationRequestNotAcceptedError(stage="server_error", status=503),
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Gone"}, {"id": "dev-2", "name": "Refused"}]
    )

    assert recorded, "no request was accepted and the cycle still reported success"
    message = str(recorded[-1])
    assert "1 not accepted" in message and "1 rejected" in message, message
    assert coordinator.api.calls == 2


@pytest.mark.asyncio
async def test_an_unaccepted_device_does_not_touch_the_transient_auth_counter() -> None:
    """Neither cleared nor raised: a refused request says nothing about credentials.

    Two claims in one, and both matter. NOT CLEARED was the defect this test was
    written against: the success path ran ``_set_auth_state(failed=False)`` and
    zeroed the counter before it looked at the result, so every 5xx wiped the
    escalation budget on its way through. Two independent changes now stand
    between a refused request and that reset: raising skips the path entirely,
    and the reset itself has moved behind the empty guard
    (``test_an_empty_return_proves_nothing_about_the_credentials``). Neither
    undoes a reset; both keep it from happening.

    NOT RAISED is the other half, and it is what a well-meant "treat it like the
    transient-auth branch" edit would break. A server that is down is not a
    credential that expired, and counting it towards the re-auth budget would
    put a sign-in prompt in front of the user for an outage they cannot fix.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {"dev-1": LocationRequestNotAcceptedError(stage="server_error", status=503)}
    )
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append
    coordinator._consecutive_transient_auth_failures = 2

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Refused"}])

    assert not auth_calls, f"the refused request touched the auth state: {auth_calls}"
    assert coordinator._consecutive_transient_auth_failures == 2
    # Reachability, and specifically that the NOT-ACCEPTED branch is what ran.
    # `api.calls == 1` alone would not show that: the broad neighbour also leaves
    # the auth state alone, so both assertions above would survive deleting the
    # branch outright. The guard's own wording is the one observable that only
    # this path produces.
    assert coordinator.api.calls == 1
    assert recorded and "was accepted this cycle" in str(recorded[-1]), recorded


@pytest.mark.asyncio
async def test_an_unaccepted_request_no_longer_clears_the_counter() -> None:
    """The contract pair to
    ``test_an_empty_return_proves_nothing_about_the_credentials``.

    The two are deliberately adjacent claims about the same success path, read
    from opposite sides. A request that was never accepted does not reach that
    path at all; a request that WAS accepted but came back empty reaches it and
    no longer clears anything either, because an empty dict is weak evidence of
    acceptance rather than proof of working credentials. The two mechanisms are
    different and stay separate: one is a raise before the success path, the
    other is a guard inside it. Splitting them is what makes the difference
    between the outcomes checkable instead of a matter of reading the branch.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {"dev-1": LocationRequestNotAcceptedError(stage="fcm_registration_failed")}
    )
    auth_calls: list[dict[str, Any]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append
    coordinator._consecutive_transient_auth_failures = 2

    await coordinator._async_start_poll_cycle([{"id": "dev-1", "name": "Refused"}])

    assert not [kw for kw in auth_calls if kw.get("failed") is False]
    assert coordinator._consecutive_transient_auth_failures == 2
    # See the sibling above: without this the broad neighbour satisfies the test.
    assert recorded and "was accepted this cycle" in str(recorded[-1]), recorded


@pytest.mark.asyncio
async def test_an_all_rejected_cycle_still_uses_the_rejection_wording() -> None:
    """The new guard must not quietly take over the old one's cases.

    Both guards end in ``last_exception is not None``, so a test that only asks
    "did the cycle surface" cannot tell which one fired -- and dropping the
    ``cycle_unaccepted_devices`` term from the new condition would let it claim
    every all-rejected cycle and report an outage where a tracker was deleted.
    Reading the message is the only observable difference, so it is what is
    pinned. The mirror is
    ``test_the_surfaced_error_names_which_counter_fired``.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {"dev-1": NovaAuthError(404, "gone"), "dev-2": NovaAuthError(400, "bad")}
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Gone"}, {"id": "dev-2", "name": "Bad"}]
    )

    assert recorded
    message = str(recorded[-1])
    assert "Every device (2) was rejected by" in message, message
    assert "not accepted" not in message, message
    assert recorded[-1].translation_domain == DOMAIN
    assert recorded[-1].translation_key == "locate_all_rejected"
    assert recorded[-1].translation_placeholders == {"rejected": "2"}


@pytest.mark.asyncio
async def test_an_unaccepted_device_does_not_hide_a_credential_failure() -> None:
    """A cycle with an expired sign-in must report THAT, not the outage beside it.

    What actually protects this is the SUM, not the guard's
    ``last_exception is None`` term, and the distinction is worth stating
    because the obvious reading gets it backwards. A credential failure
    increments neither counter, so it breaks the equality and the guard never
    runs at all. Measured: deleting the ``last_exception is None`` term leaves
    this test green, because it is unreachable today -- every branch that sets
    ``last_exception`` also keeps its device out of both counts.

    So this pins the outcome the user sees ("your sign-in expired", and with it
    the re-auth flow), and deliberately not the mechanism. If a future branch
    both counts a device and reports an error, THAT is when the term starts
    carrying weight, and a test for it can be written against a state that
    exists.
    """
    coordinator = _polling_coordinator({}, _TrackingFilter(), {})
    coordinator.api = _PerDeviceAPI(
        {
            "dev-1": NovaAuthError(401, "expired"),
            "dev-2": LocationRequestNotAcceptedError(stage="server_error", status=503),
        }
    )
    coordinator._set_auth_state = lambda **kwargs: None
    coordinator.config_entry.async_start_reauth = MagicMock()
    coordinator.note_error = MagicMock()
    recorded: list[Exception] = []
    coordinator.async_set_update_error = recorded.append

    await coordinator._async_start_poll_cycle(
        [{"id": "dev-1", "name": "Hub"}, {"id": "dev-2", "name": "Refused"}]
    )

    assert recorded, "the cycle hid the credential failure entirely"
    assert getattr(recorded[-1], "status", None) == 401, (
        "the cycle reported the unaccepted request instead of the tracker whose "
        f"sign-in expired: {recorded[-1]!r}"
    )


class TestTheDocumentedRejectionGuardStaysTrue:
    """The AGENTS.md paragraph on the rejection guard names its tests and counts them.

    That count is load-bearing in the same way `tests/AGENTS.md` describes for
    shared tuples: the paragraph is what stops the next reader from inferring
    that a non-rejected sibling proved something, and it points at the tests
    that hold the two states apart. A hand-maintained "Six tests pin this" goes
    stale the moment one is renamed or added -- it already had to be corrected
    from four to six once -- and nothing would turn red.

    Derived from the AST, not from grep: a name in a docstring or a comment
    must not count as a test.

    When the set legitimately changes, update BOTH the paragraph and nothing
    else -- this row reads the number out of the prose itself, so the prose
    stays the single source.

    A second, wider row exists and is not a replacement:
    `TestTheDocumentedExtentStaysTrue::test_every_test_name_the_component_contracts_cite_exists`
    in `tests/test_nova_request.py` checks that EVERY name cited in any of the
    component's contracts resolves, including class names and the lists this
    sentence pattern does not match. It cannot read a stated number out of
    prose, which is what this class does, so the two are kept apart on purpose.
    Whoever widens one should look at the other first; a copy built without
    knowing about its sibling is how this pair nearly became one.
    """

    _AGENTS = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "googlefindmy"
        / "AGENTS.md"
    )
    _NUMBER_WORDS = {
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5,
        "Six": 6,
        "Seven": 7,
        "Eight": 8,
    }

    def _claim(self) -> tuple[int, list[str]]:
        text = self._AGENTS.read_text(encoding="utf-8")
        match = re.search(
            r"^(\w+) tests pin this:(.*?)\.\n", text, re.MULTILINE | re.DOTALL
        )
        assert match is not None, (
            "the 'N tests pin this' sentence vanished from AGENTS.md"
        )
        claimed = self._NUMBER_WORDS.get(match.group(1))
        assert claimed is not None, f"unhandled number word: {match.group(1)!r}"
        return claimed, re.findall(r"`([A-Za-z0-9_]+)`", match.group(2))

    def _defined_test_names(self) -> set[str]:
        names: set[str] = set()
        for path in sorted(Path(__file__).resolve().parent.rglob("test_*.py")):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(
                    node, ast.FunctionDef | ast.AsyncFunctionDef
                ) and node.name.startswith("test_"):
                    names.add(node.name)
        return names

    def test_the_stated_number_matches_the_names_it_lists(self) -> None:
        claimed, listed = self._claim()
        assert claimed == len(listed), (claimed, listed)

    def test_every_named_test_exists(self) -> None:
        _, listed = self._claim()
        defined = self._defined_test_names()
        assert not [name for name in listed if name not in defined], [
            name for name in listed if name not in defined
        ]


@pytest.mark.asyncio
async def test_manual_locate_marks_a_substituted_zone_radius() -> None:
    """The filter's radius must reach the payload flagged as a zone radius.

    ``substitute_zone_accuracy`` puts the home zone's radius into ``accuracy``
    and marks it, because the accuracy gate would otherwise weigh that number
    against the cached precision and refuse the filter's deliberate move home
    (#216). This is the manual-locate call site; the poll one is covered in
    ``tests/test_cache_accuracy_gate.py``. The marker is transient and popped in
    ``update_device_cache``, which this harness stubs out, so it is still
    present on the returned payload - that is the point being pinned here.
    """
    google_filter = _TrackingFilter(
        should_filter=False,
        replacement={"latitude": 48.5, "longitude": 9.5, "radius": 400.0},
    )
    coordinator = _base_coordinator(
        {},
        google_filter,
        {
            "latitude": 52.0,
            "longitude": 13.0,
            "accuracy": 25.0,
            "semantic_name": "Kitchen speaker",
        },
    )

    result = await coordinator.async_locate_device("device-1")

    assert google_filter.called == 1
    assert result["accuracy"] == pytest.approx(400.0)
    assert result["latitude"] == pytest.approx(48.5)
    assert result["_accuracy_substituted"] is True


@pytest.mark.asyncio
async def test_manual_locate_does_not_count_a_replay() -> None:
    """Third counting site of the accuracy distribution, same replay rule.

    Repeated manual locates that return the report already cached would
    otherwise enter one fix into the distribution once per button press, and
    that distribution is what the accuracy gate's thresholds get re-tuned
    against (#216).
    """
    import time

    stamp = time.time() - 300
    coordinator = _base_coordinator(
        {},
        _TrackingFilter(),
        {
            "latitude": 52.0,
            "longitude": 13.0,
            "accuracy": 25.0,
            "last_seen": stamp,
        },
    )
    coordinator._device_location_data["device-1"] = {"last_seen": stamp}
    # The report was already tallied once. Seeded through the real claim rather than by
    # setting the cache row alone: a stored row proves a report was seen, not that a
    # bucket was counted for it, and the replay predicate now asks the latter.
    assert coordinator.claim_report_for_tally(
        "device-1", {"accuracy": 25.0, "last_seen": stamp}
    )

    counted: list[object] = []
    coordinator.count_accuracy_class = lambda row: counted.append(row.get("accuracy"))

    result = await coordinator.async_locate_device("device-1")

    assert result is not None, "the locate must have run, or this proves nothing"
    assert counted == [], f"a replayed locate was counted: {counted}"
