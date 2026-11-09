# tests/test_coordinator_locate_basics.py
"""Branch-Coverage tests for ``coordinator.locate``.

Phase 3 AP-C: target ``coordinator/locate.py`` branch-coverage by
exercising the pure helpers (``_normalize_coords``, ``_get_device_lock``,
``can_request_location``, ``can_play_sound``) and the gating branches of
the async helpers (``async_locate_device``, ``async_play_sound``,
``async_stop_sound``). Deep success-path branches (Nova roundtrip,
Google Home filter, weighted fusion, cache commit) remain out of scope
and stay for Phase 4.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from aiohttp import ClientConnectionError
from homeassistant.exceptions import ConfigEntryAuthFailed, HomeAssistantError

from custom_components.googlefindmy.const import (
    DEFAULT_MIN_POLL_INTERVAL,
    PlaySoundOutcome,
    PlaySoundResult,
    SoundDispatchOutcome,
    StopSoundOutcome,
)
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.decrypt_locations import (
    DecryptionError,
    OwnerKeyLookupTransientError,
    SharedKeyMismatchError,
    StaleOwnerKeyError,
)
from custom_components.googlefindmy.NovaApi.nova_request import NovaAuthError
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.locate_mixin_stub import LocateStub


@pytest.fixture
def coord() -> LocateStub:
    """Return a default :class:`LocateStub` bound to a synthetic config entry."""

    entry = make_config_entry(entry_id="locate-test-entry")
    return LocateStub(config_entry=entry)


class TestNormalizeCoords:
    """Exercise ``_normalize_coords`` branches."""

    def test_missing_lat_returns_false(self, coord: LocateStub) -> None:
        payload: dict = {"longitude": 12.0}
        assert coord._normalize_coords(payload) is False
        coord.increment_stat.assert_not_called()

    def test_missing_lon_returns_false(self, coord: LocateStub) -> None:
        payload: dict = {"latitude": 50.0}
        assert coord._normalize_coords(payload) is False
        coord.increment_stat.assert_not_called()

    def test_non_numeric_increments_invalid_and_returns_false(
        self, coord: LocateStub
    ) -> None:
        payload: dict = {"latitude": "abc", "longitude": "def"}
        assert coord._normalize_coords(payload) is False
        coord.increment_stat.assert_called_once_with("invalid_coords")

    def test_non_numeric_warn_off_skips_warning(self, coord: LocateStub) -> None:
        payload: dict = {"latitude": "abc", "longitude": "def"}
        assert coord._normalize_coords(payload, warn_on_invalid=False) is False
        coord.increment_stat.assert_called_once_with("invalid_coords")

    @pytest.mark.parametrize(
        ("lat", "lon"),
        [
            (math.nan, 0.0),
            (math.inf, 0.0),
            (0.0, math.inf),
            (95.0, 0.0),
            (0.0, -181.0),
        ],
    )
    def test_out_of_range_increments_invalid(
        self, coord: LocateStub, lat: float, lon: float
    ) -> None:
        payload: dict = {"latitude": lat, "longitude": lon}
        assert coord._normalize_coords(payload) is False
        coord.increment_stat.assert_called_once_with("invalid_coords")

    def test_valid_coords_returns_true_and_normalizes(self, coord: LocateStub) -> None:
        payload: dict = {"latitude": "50.0", "longitude": "12.0"}
        assert coord._normalize_coords(payload) is True
        assert payload["latitude"] == 50.0
        assert payload["longitude"] == 12.0

    def test_accuracy_valid_kept(self, coord: LocateStub) -> None:
        payload: dict = {"latitude": 50.0, "longitude": 12.0, "accuracy": "5.0"}
        assert coord._normalize_coords(payload) is True
        assert payload["accuracy"] == 5.0

    def test_accuracy_invalid_dropped(self, coord: LocateStub) -> None:
        payload: dict = {"latitude": 50.0, "longitude": 12.0, "accuracy": 0.0}
        assert coord._normalize_coords(payload) is True
        assert "accuracy" not in payload

    def test_accuracy_unparsable_dropped(self, coord: LocateStub) -> None:
        payload: dict = {"latitude": 50.0, "longitude": 12.0, "accuracy": "junk"}
        assert coord._normalize_coords(payload) is True
        assert "accuracy" not in payload


class TestGetDeviceLock:
    """Exercise ``_get_device_lock`` branches."""

    def test_create_new_lock(self, coord: LocateStub) -> None:
        lock = coord._get_device_lock("dev-1")
        assert isinstance(lock, asyncio.Lock)
        assert coord._device_action_locks["dev-1"] is lock

    def test_return_existing_lock(self, coord: LocateStub) -> None:
        first = coord._get_device_lock("dev-1")
        second = coord._get_device_lock("dev-1")
        assert first is second


class TestCanRequestLocation:
    """Exercise ``can_request_location`` branches."""

    def test_ignored_device_blocked(self, coord: LocateStub) -> None:
        coord.is_ignored.return_value = True
        assert coord.can_request_location("dev-1") is False

    def test_polling_in_progress_blocked(self, coord: LocateStub) -> None:
        coord._is_polling = True
        assert coord.can_request_location("dev-1") is False

    def test_inflight_blocked(self, coord: LocateStub) -> None:
        coord._locate_inflight.add("dev-1")
        assert coord.can_request_location("dev-1") is False

    def test_manual_cooldown_active_blocked(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 100.0,
        )
        coord._locate_cooldown_until["dev-1"] = 200.0
        assert coord.can_request_location("dev-1") is False

    def test_poll_cooldown_active_blocked(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 100.0,
        )
        coord._device_poll_cooldown_until["dev-1"] = 200.0
        assert coord.can_request_location("dev-1") is False

    def test_all_clear_allows(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 1000.0,
        )
        assert coord.can_request_location("dev-1") is True

    def test_expired_cooldown_allows(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 300.0,
        )
        coord._locate_cooldown_until["dev-1"] = 200.0
        coord._device_poll_cooldown_until["dev-1"] = 250.0
        assert coord.can_request_location("dev-1") is True


class TestCanPlaySound:
    """Exercise ``can_play_sound`` branches."""

    def test_capability_known_true(self, coord: LocateStub) -> None:
        coord._device_caps["dev-1"] = {"can_ring": True}
        assert coord.can_play_sound("dev-1") is True

    def test_capability_known_false(self, coord: LocateStub) -> None:
        coord._device_caps["dev-1"] = {"can_ring": False}
        assert coord.can_play_sound("dev-1") is False

    def test_push_not_ready_with_cooldown_blocks(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        coord._api_push_ready.return_value = False
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 100.0,
        )
        coord._push_cooldown_until = 200.0
        assert coord.can_play_sound("dev-1") is False

    def test_push_not_ready_no_cooldown_falls_back_optimistic(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        coord._api_push_ready.return_value = False
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 1000.0,
        )
        coord._ensure_device_name_cache.return_value = {"dev-1": "Phone"}
        assert coord.can_play_sound("dev-1") is True

    def test_known_device_optimistic_true(self, coord: LocateStub) -> None:
        coord._ensure_device_name_cache.return_value = {"dev-1": "Phone"}
        assert coord.can_play_sound("dev-1") is True

    def test_unknown_device_falls_back_optimistic(self, coord: LocateStub) -> None:
        coord._ensure_device_name_cache.return_value = {}
        assert coord.can_play_sound("dev-1") is True


class TestAsyncLocateDeviceGating:
    """Exercise gating branches of ``async_locate_device`` (no Nova roundtrip)."""

    async def test_blocks_when_cannot_request_location(self, coord: LocateStub) -> None:
        coord.is_ignored.return_value = True
        result = await coord.async_locate_device("dev-1")
        assert result == {}
        coord.api.async_get_device_location.assert_not_called()

    async def test_blocks_when_push_cooldown_active(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 100.0,
        )
        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = 200.0
        result = await coord.async_locate_device("dev-1")
        assert result == {}
        coord.api.async_get_device_location.assert_not_called()

    async def test_empty_payload_returns_empty(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 1000.0,
        )
        coord.api.async_get_device_location.return_value = None
        result = await coord.async_locate_device("dev-1")
        assert result == {}
        assert "dev-1" not in coord._locate_inflight  # finally branch cleared it

    async def test_payload_without_coords_returns_empty(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 1000.0,
        )
        coord.api.async_get_device_location.return_value = {
            "last_seen": 1234567890,
        }
        result = await coord.async_locate_device("dev-1")
        assert result == {}

    async def test_a_refused_fix_still_notifies_the_listeners(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A fix refused by the fusion gate still reaches the entities.

        The gate refuses the report as a POSITION but retains it as side
        information, which the tracker publishes as its ``coarse_*``
        attributes. Those attributes are recomputed in
        ``_handle_coordinator_update``, so something has to notify the
        listeners or the user presses the button and sees nothing until an
        unrelated poll arrives.

        Nothing on the refusal path does that explicitly - the notification
        comes from the ``finally`` block that closes ``async_locate_device``
        for every exit, refused or not, via
        ``async_set_updated_data(self.data)``. That is a load-bearing
        property of an unconditional cleanup block roughly 350 lines below
        the refusal, i.e. exactly the kind of thing a later edit removes
        without noticing. Measured before this test existed: dropping that
        one line left every suite that drives a locate green.

        The ORDER is what carries the property, not the count. A locate
        notifies twice: once on entry, when the device goes in-flight and the
        button turns unavailable, and once on the way out. Only the second one
        can show a coarse fix, because at entry the report has not been
        fetched yet, let alone refused and retained.

        See ``custom_components/googlefindmy/AGENTS.md``, "A refusal is not
        silence".
        """
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 1000.0,
        )
        coord.api.async_get_device_location.return_value = {
            "latitude": 50.0,
            "longitude": 12.0,
            "accuracy": 1600.0,
            "last_seen": 1234567890,
        }
        coord._apply_weighted_location_fusion.return_value = False

        order = MagicMock()
        order.attach_mock(coord._apply_weighted_location_fusion, "refuse")
        order.attach_mock(coord.async_set_updated_data, "notify")

        result = await coord.async_locate_device("dev-1")

        # The refusal branch really was the one taken, and it committed nothing.
        assert result == {}
        coord.update_device_cache.assert_not_called()
        coord.push_updated.assert_not_called()

        # ... and a notification followed the refusal.
        assert [call[0] for call in order.mock_calls] == [
            "notify",
            "refuse",
            "notify",
        ]


class TestAsyncLocateDeviceDecryptFailure:
    """Codex P2: manual locate must handle stale/missing shared-key failures the
    same way the poll path does (feed the shared escalation counter, start reauth),
    not swallow ``DecryptionError`` into a generic ``HomeAssistantError``."""

    @pytest.fixture(autouse=True)
    def _pass_cooldown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Move the clock past every cooldown gate so the Nova call is reached."""
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 1000.0,
        )

    async def test_stale_owner_key_records_per_tracker_no_reauth(
        self, coord: LocateStub
    ) -> None:
        """A per-tracker StaleOwnerKeyError feeds note_decrypt_failure(stale=True),
        never starts an account reauth, and returns an empty result."""
        coord.note_decrypt_failure = MagicMock(return_value=False)
        coord.config_entry.async_start_reauth = MagicMock()
        coord.api.async_get_device_location.side_effect = StaleOwnerKeyError(
            "tracker v1 < v2"
        )

        result = await coord.async_locate_device("dev-1")

        assert result == {}
        coord.note_decrypt_failure.assert_called_once()
        assert coord.note_decrypt_failure.call_args.kwargs.get("stale") is True
        coord.config_entry.async_start_reauth.assert_not_called()

    async def test_decrypt_failure_below_threshold_returns_empty(
        self, coord: LocateStub
    ) -> None:
        """Below the escalation threshold the manual locate degrades gracefully to
        an empty result (mirrors the poll path that keeps polling), and must not
        start a reauth flow."""
        coord.note_decrypt_failure = MagicMock(return_value=False)
        coord.config_entry.async_start_reauth = MagicMock()
        coord.api.async_get_device_location.side_effect = SharedKeyMismatchError(
            "stale shared key"
        )

        result = await coord.async_locate_device("dev-1")

        assert result == {}
        coord.note_decrypt_failure.assert_called_once()
        assert coord.note_decrypt_failure.call_args.kwargs.get("stale") is False
        coord.config_entry.async_start_reauth.assert_not_called()

    async def test_transient_owner_key_lookup_returns_empty_no_reauth_counter(
        self, coord: LocateStub
    ) -> None:
        """R4/AP5: a transient owner-key lookup miss during a manual locate degrades
        to an empty result WITHOUT feeding the account-wide reauth counter.

        ``OwnerKeyLookupTransientError`` is not a ``DecryptionError``, so the
        dedicated ``except OwnerKeyLookupTransientError`` block (before the
        ``DecryptionError`` handler) skips the device without calling
        ``note_decrypt_failure`` and never starts a reauth flow.
        """
        coord.note_decrypt_failure = MagicMock(return_value=False)
        coord.config_entry.async_start_reauth = MagicMock()
        coord.api.async_get_device_location.side_effect = OwnerKeyLookupTransientError(
            "Owner key retrieval did not complete (transient)."
        )

        result = await coord.async_locate_device("dev-1")

        assert result == {}
        coord.note_decrypt_failure.assert_not_called()
        coord._set_auth_state.assert_not_called()
        coord.config_entry.async_start_reauth.assert_not_called()

    async def test_decrypt_failure_at_threshold_starts_reauth(
        self, coord: LocateStub
    ) -> None:
        """When the shared counter says escalate (note_decrypt_failure True), manual
        locate marks the auth state failed, starts the reauth flow, and surfaces a
        user-facing HomeAssistantError instead of an empty result."""
        coord.note_decrypt_failure = MagicMock(return_value=True)
        coord.config_entry.async_start_reauth = MagicMock()
        coord.api.async_get_device_location.side_effect = DecryptionError(
            "shared key gone"
        )

        with pytest.raises(HomeAssistantError) as excinfo:
            await coord.async_locate_device("dev-1")

        assert "re-authentication has been started" in str(excinfo.value)
        coord._set_auth_state.assert_called_once()
        assert coord._set_auth_state.call_args.kwargs.get("failed") is True
        coord.config_entry.async_start_reauth.assert_called_once()


class TestAsyncLocateDeviceNovaAuthClassification:
    """Manual locate must classify a Nova refusal by its STATUS, not its type.

    NovaAuthError covers every non-retryable 4xx, so a device removed from the
    account flipped the integration-wide auth state: Repairs issue,
    EVENT_AUTH_ERROR, diagnostic sensor on -- with the sign-in intact. The
    403 row is a characterisation test: it was green before this change and
    must stay green, otherwise the narrowing is satisfied by a branch that
    never names credentials at all.
    """

    @pytest.fixture(autouse=True)
    def _pass_cooldown(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Move the clock past every cooldown gate so the Nova call is reached."""
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.time.monotonic",
            lambda: 1000.0,
        )

    async def test_manual_locate_credential_rejection_sets_the_auth_state(
        self, coord: LocateStub, caplog: pytest.LogCaptureFixture
    ) -> None:
        coord.api.async_get_device_location.side_effect = NovaAuthError(403, "denied")

        with caplog.at_level(logging.DEBUG):
            result = await coord.async_locate_device("dev-1")

        assert result == {}
        coord._set_auth_state.assert_called_once()
        assert coord._set_auth_state.call_args.kwargs.get("failed") is True
        assert "failed (authentication)" in caplog.text

    async def test_manual_locate_client_error_leaves_the_auth_state_alone(
        self, coord: LocateStub
    ) -> None:
        """Neither direction: the state is not flagged AND not cleared.

        Flagging it was the original defect. Clearing it is the mirror image and
        just as wrong: a manual locate on a tracker the server rejects proves
        nothing about the credentials, so it must not wipe a pending auth error
        raised by some other device.

        The second half of this pair used to depend on ``api.py`` passing the
        error through rather than returning ``{}``; it no longer does. The reset
        now sits behind the empty guard, so an empty return would not clear the
        state either. Both routes are covered, and
        ``test_an_empty_manual_locate_does_not_clear_the_auth_state`` is the one
        that covers the other route on purpose.
        """
        coord.api.async_get_device_location.side_effect = NovaAuthError(404, "gone")

        result = await coord.async_locate_device("dev-1")

        assert result == {}
        assert not [
            c for c in coord._set_auth_state.call_args_list if c.kwargs.get("failed")
        ]
        assert not [
            c
            for c in coord._set_auth_state.call_args_list
            if c.kwargs.get("failed") is False
        ]

    async def test_manual_locate_client_error_is_still_recorded(
        self, coord: LocateStub
    ) -> None:
        """This branch keeps the failure on record; a silent {} would be worse.

        api.py passes a non-credential rejection through, so a real manual
        locate on a deleted tracker reaches exactly this branch.
        """
        coord.api.async_get_device_location.side_effect = NovaAuthError(404, "gone")

        await coord.async_locate_device("dev-1")

        coord.note_error.assert_called_once()
        assert coord.note_error.call_args.kwargs.get("where") == "async_locate_device"

    async def test_manual_locate_client_error_does_not_say_authentication(
        self, coord: LocateStub, caplog: pytest.LogCaptureFixture
    ) -> None:
        coord.api.async_get_device_location.side_effect = NovaAuthError(404, "gone")

        with caplog.at_level(logging.DEBUG):
            await coord.async_locate_device("dev-1")

        assert "failed (client error): HTTP 404" in caplog.text
        assert "failed (authentication)" not in caplog.text
        levels = {
            r.levelno
            for r in caplog.records
            if "failed (client error): HTTP 404" in r.message
        }
        assert levels == {logging.WARNING}

    async def test_an_empty_manual_locate_does_not_clear_the_auth_state(
        self, coord: LocateStub
    ) -> None:
        """An empty result proves nothing here either, so it clears nothing.

        The manual path carried the same false-success reasoning as the poll
        cycle: it cleared the auth state before it had looked at whether the
        result had any content. A user pressing "locate" on an idle BLE tag
        therefore wiped a pending credential finding raised by another device,
        which is precisely the evidence the Repairs issue rests on.
        """
        coord.api.async_get_device_location.return_value = {}

        result = await coord.async_locate_device("dev-1")

        assert result == {}
        assert not [
            c
            for c in coord._set_auth_state.call_args_list
            if c.kwargs.get("failed") is False
        ]

    async def test_a_successful_manual_locate_still_clears_the_auth_state(
        self, coord: LocateStub
    ) -> None:
        """The positive half survives the move: a real fix still counts as proof."""
        coord.api.async_get_device_location.return_value = {
            "latitude": 50.0,
            "longitude": 10.0,
            "accuracy": 5.0,
            "last_seen": 1234567890,
        }

        await coord.async_locate_device("dev-1")

        assert [
            c
            for c in coord._set_auth_state.call_args_list
            if c.kwargs.get("failed") is False
        ]

    async def test_a_record_without_coordinates_still_proves_the_credentials(
        self, coord: LocateStub
    ) -> None:
        """The guard against overshooting in the other direction.

        A record carrying only ``last_seen`` is an authenticated server answer:
        the account was accepted, the row simply has no coordinate report. The
        method returns ``{}`` for it (see
        ``test_payload_without_coords_returns_empty``), which makes it tempting
        to push the reset further down, past the coordinate check. That would
        throw away a proof the server actually gave us. The reset therefore sits
        behind the EMPTY guard and in front of the COORDINATE check, and this
        test is what holds it there.
        """
        coord.api.async_get_device_location.return_value = {
            "last_seen": 1234567890,
        }

        result = await coord.async_locate_device("dev-1")

        assert result == {}
        assert [
            c
            for c in coord._set_auth_state.call_args_list
            if c.kwargs.get("failed") is False
        ]


# One row per ``SoundDispatchOutcome`` member: (dispatch, resulting
# PlaySoundOutcome, may arm the push cooldown). Kept at module level so the
# exhaustiveness guards below read the same table the parametrisation runs on.
#
# Every non-acceptance maps to FAILED, and none of them to SUPPRESSED: once
# api.py has been asked, no answer of its is undone by waiting a moment. The
# one condition that is -- the local readiness gate declining to send -- never
# reaches this table, because it returns before the api call. Rate limiting is
# the deliberate blunt edge (see PlaySoundOutcome.FAILED), matching the stop
# path rather than sorting the same condition two ways.
PLAY_OUTCOME_CASES: list[tuple[SoundDispatchOutcome, PlaySoundOutcome, bool]] = [
    (SoundDispatchOutcome.ACCEPTED, PlaySoundOutcome.ACCEPTED, False),
    (SoundDispatchOutcome.REJECTED_AUTH, PlaySoundOutcome.FAILED, False),
    (SoundDispatchOutcome.REJECTED_RATE_LIMIT, PlaySoundOutcome.FAILED, False),
    (SoundDispatchOutcome.REJECTED_SERVER, PlaySoundOutcome.FAILED, False),
    (SoundDispatchOutcome.NOT_SENT, PlaySoundOutcome.FAILED, False),
    (SoundDispatchOutcome.INTERNAL_ERROR, PlaySoundOutcome.FAILED, False),
    (SoundDispatchOutcome.TRANSPORT_FAILED, PlaySoundOutcome.FAILED, True),
]


class TestAsyncPlaySoundGating:
    """Exercise gating branches of ``async_play_sound``."""

    async def test_a_device_that_cannot_ring_is_a_failure_not_a_suppression(
        self, coord: LocateStub, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The permanent half of the gate must not advise waiting.

        ``can_play_sound`` answers two conditions with one ``False``: a
        capability that says the device cannot ring, and an active push
        cooldown. Only the second passes with time. Reporting the first as
        SUPPRESSED would tell the owner of a non-ringing tracker to try again in
        a moment, forever.
        """

        coord._device_caps["dev-1"] = {"can_ring": False}
        coord.get_device_display_name = MagicMock(return_value="Jens keyring")
        with caplog.at_level(logging.WARNING):
            outcome = await coord.async_play_sound("dev-1")
        assert outcome is PlaySoundOutcome.FAILED
        coord.api.async_play_sound.assert_not_called()
        # The warning is written in normal operation, so it falls under
        # AGENTS.md section 5 -- both halves of it: no device id, and no derived
        # identifying information such as a user-provided device name. Asserting
        # the absence here rather than trusting the review that caught it twice.
        assert "cannot ring" in caplog.text
        assert "dev-1" not in caplog.text
        assert "Jens keyring" not in caplog.text

    async def test_an_active_push_cooldown_suppresses(self, coord: LocateStub) -> None:
        """The transient half of the same gate, and the only source of SUPPRESSED.

        No answer from ``api.py`` can produce SUPPRESSED (see
        ``PLAY_OUTCOME_CASES``); this branch is the whole of it, so without this
        test the member could be removed and the suite would stay green.

        Note what the gate does NOT do, and what this test therefore has to
        arrange: a cached ``can_ring: True`` is answered on the fast path and
        never reaches the cooldown check at all. The suppressible case is the
        one where the capability is still unknown -- which is also why the
        cooldown is the sole source of this member rather than one of three.
        """

        coord._device_caps.pop("dev-1", None)
        coord._api_push_ready = MagicMock(return_value=False)
        coord._push_cooldown_until = time.monotonic() + 90

        outcome = await coord.async_play_sound("dev-1")

        assert outcome is PlaySoundOutcome.SUPPRESSED
        coord.api.async_play_sound.assert_not_called()

    async def test_success_stores_uuid(self, coord: LocateStub) -> None:
        coord._device_caps["dev-1"] = {"can_ring": True}
        outcome = await coord.async_play_sound("dev-1")
        assert outcome is PlaySoundOutcome.ACCEPTED
        assert coord._sound_request_uuids.get("dev-1") == "uuid-stub"
        coord._async_save_sound_uuids.assert_awaited_once()

    async def test_failure_notes_problem(self, coord: LocateStub) -> None:
        coord._device_caps["dev-1"] = {"can_ring": True}
        coord.api.async_play_sound.return_value = PlaySoundResult(
            SoundDispatchOutcome.TRANSPORT_FAILED
        )
        outcome = await coord.async_play_sound("dev-1")
        assert outcome is PlaySoundOutcome.FAILED
        coord._note_push_transport_problem.assert_called_once()

    async def test_unexpected_exception_is_not_a_transport_problem(
        self, coord: LocateStub
    ) -> None:
        """A bug of our own must not be reported as a broken push transport.

        ``api.async_play_sound`` classifies every ``Exception`` in band and
        returns ``INTERNAL_ERROR`` instead of raising, so nothing that reaches
        this handler came from the push transport: what is left is the
        coordinator's own body around the call, or an ``api`` implementation
        that breaks the Protocol. Arming the push cooldown for either is the
        self-inflicted outage this contract was written to stop. The error is
        still recorded.
        """

        coord._device_caps["dev-1"] = {"can_ring": True}
        coord.api.async_play_sound.side_effect = RuntimeError("boom")
        outcome = await coord.async_play_sound("dev-1")
        assert outcome is PlaySoundOutcome.FAILED
        coord.note_error.assert_called_once()
        coord._note_push_transport_problem.assert_not_called()

    async def test_failed_play_does_not_clear_auth_state(
        self, coord: LocateStub
    ) -> None:
        """A play that was not accepted must not vouch for the credentials.

        ``api.async_play_sound`` collapses a 401/403 rejection into the same
        ``(False, None)`` as a timeout, so clearing the auth-failure state here
        deleted the signal an expired sign-in produces. The stop path never did
        this (see ``async_stop_sound``); the two paths now agree.
        """

        coord._device_caps["dev-1"] = {"can_ring": True}
        coord.api.async_play_sound.return_value = PlaySoundResult(
            SoundDispatchOutcome.TRANSPORT_FAILED
        )

        assert await coord.async_play_sound("dev-1") is PlaySoundOutcome.FAILED

        coord._set_auth_state.assert_not_called()

    async def test_accepted_play_still_clears_auth_state(
        self, coord: LocateStub
    ) -> None:
        """The positive half of the rule must not be lost with the fix."""

        coord._device_caps["dev-1"] = {"can_ring": True}

        assert await coord.async_play_sound("dev-1") is PlaySoundOutcome.ACCEPTED

        coord._set_auth_state.assert_called_once_with(failed=False)

    @pytest.mark.parametrize(
        ("outcome", "expect_outcome", "expect_cooldown"), PLAY_OUTCOME_CASES
    )
    async def test_only_a_transport_failure_arms_the_push_cooldown(
        self,
        coord: LocateStub,
        outcome: SoundDispatchOutcome,
        expect_outcome: PlaySoundOutcome,
        expect_cooldown: bool,
    ) -> None:
        """A server saying no is not a network outage.

        Every non-acceptance used to arrive as a plain ``False``, so all of them
        armed the 90-second push cooldown, flipped the integration to
        ``FcmStatus.DEGRADED`` and made ``can_play_sound`` report the button as
        unavailable. ``SoundDispatchOutcome`` names the cause; only a transport
        that never gave us a usable answer may arm that cooldown. The list is
        exhaustive over the enum on purpose: a new member added without a
        decision here shows up as a missing parametrisation, not as a silent
        default.
        """

        coord._device_caps["dev-1"] = {"can_ring": True}
        coord.api.async_play_sound.return_value = PlaySoundResult(outcome)

        assert await coord.async_play_sound("dev-1") is expect_outcome

        assert coord._note_push_transport_problem.called is expect_cooldown

    def test_the_play_parametrisation_covers_every_outcome(self) -> None:
        """Guard the exhaustiveness the case table claims for itself.

        The decision "which outcome may arm the cooldown" has to be taken for
        every member of the enum. A member added later without a row here would
        otherwise silently inherit whatever the ``if`` cascade happens to do.
        """

        assert {case[0] for case in PLAY_OUTCOME_CASES} == set(SoundDispatchOutcome)

    def test_the_play_outcome_type_stays_three_valued(self) -> None:
        """Pin the size of ``PlaySoundOutcome`` and the reachability of each member.

        A fourth member -- ``UNSUPPORTED`` for the device that cannot ring was
        the candidate -- would need a user-facing message of its own in eleven
        files, and the button is not offered for such a device in the first
        place. The decision was to keep three; a later member added without
        revisiting that trade-off shows up here rather than in a translation
        review.

        The second assertion pins the case table, not the code: SUPPRESSED must
        NOT appear as a result of any dispatch outcome, because it is reachable
        only from the readiness gate. A row claiming otherwise would be a
        contradiction the parametrised run itself could not catch, since it
        would then simply assert the wrong expectation. What covers SUPPRESSED
        behaviourally is ``test_an_active_push_cooldown_suppresses``, and
        nothing here substitutes for it.
        """

        assert set(PlaySoundOutcome) == {
            PlaySoundOutcome.ACCEPTED,
            PlaySoundOutcome.SUPPRESSED,
            PlaySoundOutcome.FAILED,
        }
        assert {case[1] for case in PLAY_OUTCOME_CASES} == {
            PlaySoundOutcome.ACCEPTED,
            PlaySoundOutcome.FAILED,
        }

    async def test_a_config_entry_auth_failure_is_a_failure(
        self, coord: LocateStub
    ) -> None:
        """The one FAILED exit that no dispatch outcome can reach.

        ``api.py`` does not raise ``ConfigEntryAuthFailed`` on the sound paths --
        it classifies in band -- so this handler guards against an ``api``
        implementation that does, ``api`` being a Protocol. Untested, it was the
        only new FAILED exit with nothing holding it there.
        """

        coord._device_caps["dev-1"] = {"can_ring": True}
        coord.api.async_play_sound.side_effect = ConfigEntryAuthFailed("expired")

        outcome = await coord.async_play_sound("dev-1")

        assert outcome is PlaySoundOutcome.FAILED
        coord._set_auth_state.assert_called_once()
        assert coord._set_auth_state.call_args.kwargs["failed"] is True
        coord.async_request_refresh.assert_awaited_once()
        coord._note_push_transport_problem.assert_not_called()

    async def test_a_failing_refresh_does_not_change_the_auth_outcome(
        self, coord: LocateStub
    ) -> None:
        """The refresh is a courtesy, not part of the verdict.

        ``async_request_refresh`` is called for the user's benefit after an auth
        failure and its own failure is swallowed on purpose. Untested, the
        swallow could start swallowing the outcome too.
        """

        coord._device_caps["dev-1"] = {"can_ring": True}
        coord.api.async_play_sound.side_effect = ConfigEntryAuthFailed("expired")
        coord.async_request_refresh.side_effect = RuntimeError("refresh is down")

        outcome = await coord.async_play_sound("dev-1")

        assert outcome is PlaySoundOutcome.FAILED

    async def test_an_outcome_from_outside_the_enum_is_not_success(
        self, coord: LocateStub
    ) -> None:
        """The negative form, probed rather than asserted about.

        The mapping is written as ``is not ACCEPTED`` and not as a cascade over
        the seven members, so that a value from outside the enum -- a stale test
        double, a member added later without visiting the call site -- ends as
        FAILED instead of falling through to success.
        """

        coord._device_caps["dev-1"] = {"can_ring": True}
        coord.api.async_play_sound.return_value = PlaySoundResult(
            "a-value-that-is-not-a-member"  # type: ignore[arg-type]
        )

        outcome = await coord.async_play_sound("dev-1")

        assert outcome is PlaySoundOutcome.FAILED


# One row per ``SoundDispatchOutcome`` member on the stop side: (dispatch,
# resulting StopSoundOutcome, may arm the push cooldown, may vouch for the
# credentials). ACCEPTED lands on UNCORRELATED here because the key is passed
# in by the caller and is none of ours -- that split is pinned by its own tests.
STOP_OUTCOME_CASES: list[tuple[SoundDispatchOutcome, StopSoundOutcome, bool, bool]] = [
    (SoundDispatchOutcome.ACCEPTED, StopSoundOutcome.UNCORRELATED, False, True),
    (SoundDispatchOutcome.REJECTED_AUTH, StopSoundOutcome.FAILED, False, False),
    (SoundDispatchOutcome.REJECTED_RATE_LIMIT, StopSoundOutcome.FAILED, False, False),
    (SoundDispatchOutcome.REJECTED_SERVER, StopSoundOutcome.FAILED, False, False),
    # NOT_SENT is FAILED, not SUPPRESSED: it never reached the wire, but a
    # receiver that is up and yields no action token is not answered by
    # waiting. StopSoundOutcome.FAILED states the rule; this row enforces it.
    (SoundDispatchOutcome.NOT_SENT, StopSoundOutcome.FAILED, False, False),
    (SoundDispatchOutcome.INTERNAL_ERROR, StopSoundOutcome.FAILED, False, False),
    (SoundDispatchOutcome.TRANSPORT_FAILED, StopSoundOutcome.FAILED, True, False),
]


class TestAsyncStopSoundGating:
    """Exercise gating branches of ``async_stop_sound``."""

    @pytest.mark.parametrize(
        ("dispatch", "expect_outcome", "expect_cooldown", "expect_auth_cleared"),
        STOP_OUTCOME_CASES,
    )
    async def test_only_a_transport_failure_arms_the_push_cooldown(
        self,
        coord: LocateStub,
        dispatch: SoundDispatchOutcome,
        expect_outcome: StopSoundOutcome,
        expect_cooldown: bool,
        expect_auth_cleared: bool,
    ) -> None:
        """The same rule as on the play path, on the stop path.

        A stop the server refused on credentials, refused outright or rate
        limited reached this method as a plain ``False`` before the contract
        existed, so each of them armed the 90-second cooldown -- which then
        suppressed the user's next attempt for a minute and a half over a
        problem the network never had.
        """

        coord.api.async_stop_sound.return_value = dispatch

        outcome = await coord.async_stop_sound("dev-1", request_uuid="foreign-key")

        assert outcome is expect_outcome
        assert coord._note_push_transport_problem.called is expect_cooldown
        assert coord._set_auth_state.called is expect_auth_cleared

    def test_the_stop_parametrisation_covers_every_outcome(self) -> None:
        """Guard the exhaustiveness the case table claims for itself."""

        assert {case[0] for case in STOP_OUTCOME_CASES} == set(SoundDispatchOutcome)

    async def test_unexpected_exception_is_not_a_transport_problem(
        self, coord: LocateStub
    ) -> None:
        """Mirror of the play-path rule: our own bug is not an outage.

        ``api.async_stop_sound`` returns ``INTERNAL_ERROR`` for every unexpected
        ``Exception`` instead of raising, so this handler only sees failures of
        the coordinator's own bookkeeping around the call.
        """

        coord.api.async_stop_sound.side_effect = RuntimeError("boom")

        outcome = await coord.async_stop_sound("dev-1", request_uuid="x")

        assert outcome is StopSoundOutcome.FAILED
        coord.note_error.assert_called_once()
        coord._note_push_transport_problem.assert_not_called()

    async def test_blocks_when_push_not_ready(self, coord: LocateStub) -> None:
        coord._api_push_ready.return_value = False
        outcome = await coord.async_stop_sound("dev-1")
        # A suppressed stop was never sent, so it is a failure, not a silent
        # "uncorrelated". The service layer has to raise on it -- and it is
        # SUPPRESSED, not FAILED: nothing left this machine, so the advice
        # "try again shortly" is true here and false for a rejected stop.
        assert outcome is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()

    async def test_rejected_submission_is_failed_not_suppressed(
        self, coord: LocateStub
    ) -> None:
        """A stop the transport refused must not claim a local, transient cause.

        ``api.async_stop_sound`` swallows every exception and returns a
        ``SoundDispatchOutcome`` instead of raising, so auth failures, 401/403,
        server errors, rate limits and network errors all arrive as a
        non-ACCEPTED value here. Reporting them as SUPPRESSED would tell a user
        with an expired sign-in to wait a moment.
        """

        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.TRANSPORT_FAILED
        outcome = await coord.async_stop_sound("dev-1")
        assert outcome is StopSoundOutcome.FAILED
        coord.api.async_stop_sound.assert_awaited_once()

    async def test_uses_cached_uuid_when_none_passed(self, coord: LocateStub) -> None:
        coord._sound_request_uuids["dev-1"] = "cached-uuid"
        outcome = await coord.async_stop_sound("dev-1")
        assert outcome is StopSoundOutcome.CANCELLED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "cached-uuid")
        # A correlated stop keeps the key for a repeated press
        # (IRR-CA-STOP-KEEPS-CANCEL-KEY).
        assert coord._sound_request_uuids["dev-1"] == "cached-uuid"

    async def test_explicit_uuid_does_not_drop_our_own_cached_key(
        self, coord: LocateStub
    ) -> None:
        """A foreign cancel key must not evict our own handle, nor claim success.

        The caller may pass the key of a *different* ring (that is precisely the
        BSkando#195 scenario). Popping our cached key on its behalf would throw
        away the only handle for a ring that may still be running -- and calling
        it CANCELLED would be the same unbacked success claim one layer up:
        correlation is something we prove, not something the caller asserts.
        """

        coord._sound_request_uuids["dev-1"] = "cached-uuid"
        outcome = await coord.async_stop_sound("dev-1", request_uuid="explicit")
        assert outcome is StopSoundOutcome.UNCORRELATED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "explicit")
        assert coord._sound_request_uuids["dev-1"] == "cached-uuid"

    async def test_explicit_uuid_equal_to_our_fresh_key_is_correlated(
        self, coord: LocateStub
    ) -> None:
        """Passing back our own live key is the one provable caller claim.

        It is not a foreign key at all, so it correlates -- the verdict follows
        the proof, not the presence of an argument. Like an implicit correlated
        stop it leaves the key in place (IRR-CA-STOP-KEEPS-CANCEL-KEY).
        """

        coord._sound_request_uuids["dev-1"] = "cached-uuid"
        outcome = await coord.async_stop_sound("dev-1", request_uuid="cached-uuid")
        assert outcome is StopSoundOutcome.CANCELLED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "cached-uuid")
        assert coord._sound_request_uuids["dev-1"] == "cached-uuid"

    async def test_missing_uuid_reports_uncorrelated(self, coord: LocateStub) -> None:
        outcome = await coord.async_stop_sound("dev-1")
        # Submitted, but nothing proves an effect.
        assert outcome is StopSoundOutcome.UNCORRELATED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", None)

    async def test_failure_notes_problem(self, coord: LocateStub) -> None:
        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.TRANSPORT_FAILED
        outcome = await coord.async_stop_sound("dev-1", request_uuid="x")
        assert outcome is StopSoundOutcome.FAILED
        coord._note_push_transport_problem.assert_called_once()

    async def test_failed_stop_keeps_a_fresh_cancel_key(
        self, coord: LocateStub
    ) -> None:
        """IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY: a rejected stop spends nothing."""

        coord._sound_request_uuids["dev-1"] = "cached-uuid"
        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.TRANSPORT_FAILED
        outcome = await coord.async_stop_sound("dev-1")
        assert outcome is StopSoundOutcome.FAILED
        assert coord._sound_request_uuids["dev-1"] == "cached-uuid"


class TestStopBreaksSelfInflictedCooldown:
    """AP-5 / F2: the cancel key a failed play preserved must be usable at once.

    A play that reached the wire and then lost the answer does two things in the
    same breath: it stores a cancel key, and it arms the 90-second push cooldown
    (correctly, because that IS a transport failure). ``_api_push_ready()``
    short-circuits to False while the cooldown runs, so the stop that the key
    exists for was suppressed for the first 90 seconds after that play, which is
    exactly when a user reaches for the Stop button. (How long the ring itself
    lasts is a different timer on a different layer and is not claimed here.)
    See IRR-CA-STOP-BREAKS-SELF-INFLICTED-COOLDOWN.

    The exception is deliberately narrow. It applies only when the stop would be
    correlated (our own, fresh cancel key); a stop that would report
    UNCORRELATED buys nothing, so the anti-spam purpose of the cooldown is kept
    for every case that has no provable benefit. It is not free either: a stop
    that used to end as SUPPRESSED now reaches the transport and can end as
    FAILED instead, which is a different service-level message. That change of
    outcome class is pinned below rather than left implicit.
    """

    async def test_ambiguous_play_does_not_block_the_following_stop(
        self, coord: LocateStub
    ) -> None:
        """The whole F2 chain, end to end: play loses the answer, stop follows."""

        coord._device_caps["dev-1"] = {"can_ring": True}

        def _note(cooldown_s: int = 90) -> None:
            coord._push_cooldown_until = time.monotonic() + cooldown_s

        coord._note_push_transport_problem = MagicMock(side_effect=_note)
        coord._api_push_ready = MagicMock(
            side_effect=lambda: time.monotonic() >= coord._push_cooldown_until
        )
        coord.api.async_play_sound.return_value = PlaySoundResult(
            SoundDispatchOutcome.TRANSPORT_FAILED, cancel_key="uuid-ambiguous"
        )

        assert await coord.async_play_sound("dev-1") is PlaySoundOutcome.FAILED
        assert coord._sound_request_uuids.get("dev-1") == "uuid-ambiguous"
        assert coord._push_cooldown_until > time.monotonic()

        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.ACCEPTED
        outcome = await coord.async_stop_sound("dev-1")

        assert outcome is StopSoundOutcome.CANCELLED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "uuid-ambiguous")

    async def test_explicit_own_fresh_key_breaks_the_cooldown(
        self, coord: LocateStub
    ) -> None:
        """The service handler passes the key explicitly; same right to be sent."""

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() + 90.0
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"

        outcome = await coord.async_stop_sound("dev-1", request_uuid="uuid-fresh")

        assert outcome is StopSoundOutcome.CANCELLED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "uuid-fresh")

    async def test_blank_explicit_key_falls_back_to_the_cached_key(
        self, coord: LocateStub
    ) -> None:
        """A blank argument means "no opinion" here too, not "no key"."""

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() + 90.0
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"

        outcome = await coord.async_stop_sound("dev-1", request_uuid="   ")

        assert outcome is StopSoundOutcome.CANCELLED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "uuid-fresh")

    async def test_a_broken_through_stop_that_fails_does_not_extend_the_window(
        self, coord: LocateStub
    ) -> None:
        """Breaking a window must never lengthen it (the amplification guard).

        ``_note_push_transport_problem`` sets ``_push_cooldown_until`` to
        ``monotonic() + 90`` ABSOLUTELY and flags the transport DEGRADED, so
        every call restarts the window rather than topping it up. Before this
        package that call was unreachable while a window ran -- the suppression
        was the first statement of the method -- and the stop button has no
        availability guard (``can_stop_sound`` does not exist on the
        coordinator, button.py only probes for it). Without this guard a user
        who keeps pressing Stop during an outage would restart the window on
        every press, and that window also gates Play Sound and manual locate:
        two unrelated features stay disabled for as long as the pressing goes
        on. The stop itself is still sent -- that is the point of the exception
        -- it just may not lengthen the window that let it through.
        """

        def _arm(cooldown_s: int = 90) -> None:
            coord._push_cooldown_until = time.monotonic() + cooldown_s

        coord._note_push_transport_problem = MagicMock(side_effect=_arm)
        coord._api_push_ready.return_value = False
        window_ends_at = time.monotonic() + 90.0
        coord._push_cooldown_until = window_ends_at
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"
        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.TRANSPORT_FAILED

        outcome = await coord.async_stop_sound("dev-1")

        assert outcome is StopSoundOutcome.FAILED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "uuid-fresh")
        assert coord._push_cooldown_until == window_ends_at

    async def test_repeated_broken_through_stops_never_move_the_window_end(
        self, coord: LocateStub
    ) -> None:
        """The amplification chain: repeated presses must not push the end forward.

        All five presses fall inside the one window under test, which is the
        situation the guard is for. The claim is constancy of the end within a
        window, not termination in general: that already follows from
        ``_note_push_transport_problem`` setting the deadline absolutely.
        """

        def _arm(cooldown_s: int = 90) -> None:
            coord._push_cooldown_until = time.monotonic() + cooldown_s

        coord._note_push_transport_problem = MagicMock(side_effect=_arm)
        coord._api_push_ready.side_effect = lambda: (
            time.monotonic() >= coord._push_cooldown_until
        )
        window_ends_at = time.monotonic() + 90.0
        coord._push_cooldown_until = window_ends_at
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"
        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.TRANSPORT_FAILED

        for _ in range(5):
            assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.FAILED

        assert coord.api.async_stop_sound.await_count == 5
        assert coord._push_cooldown_until == window_ends_at

    async def test_a_failing_stop_still_arms_a_window_when_none_is_running(
        self, coord: LocateStub
    ) -> None:
        """The guard is about EXTENDING, not about arming: the normal path is untouched."""

        coord._api_push_ready.return_value = True
        coord._push_cooldown_until = 0.0
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"
        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.TRANSPORT_FAILED

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.FAILED
        coord._note_push_transport_problem.assert_called_once()

    async def test_a_raised_connection_error_does_not_extend_the_window_either(
        self, coord: LocateStub
    ) -> None:
        """The same guard covers the typed exception handler, not just the outcome.

        ``api`` is a Protocol, so an implementation that lets an aiohttp error
        escape reaches the typed handler rather than returning
        TRANSPORT_FAILED. That handler arms the cooldown too, and a
        broken-through stop can now reach it, so it must not restart a running
        window either.
        """

        def _arm(cooldown_s: int = 90) -> None:
            coord._push_cooldown_until = time.monotonic() + cooldown_s

        coord._note_push_transport_problem = MagicMock(side_effect=_arm)
        coord._api_push_ready.return_value = False
        window_ends_at = time.monotonic() + 90.0
        coord._push_cooldown_until = window_ends_at
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"
        coord.api.async_stop_sound.side_effect = ClientConnectionError("boom")

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.FAILED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "uuid-fresh")
        assert coord._push_cooldown_until == window_ends_at

    async def test_a_raised_connection_error_arms_a_window_when_none_is_running(
        self, coord: LocateStub
    ) -> None:
        """Counter-case, so the guard above cannot pass by disarming the handler."""

        coord._api_push_ready.return_value = True
        coord._push_cooldown_until = 0.0
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"
        coord.api.async_stop_sound.side_effect = ClientConnectionError("boom")

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.FAILED
        coord._note_push_transport_problem.assert_called_once()

    async def test_the_exception_changes_the_reported_outcome_class(
        self, coord: LocateStub
    ) -> None:
        """The price of the exception, stated as a test rather than as prose.

        Breaking the window means the stop reaches the transport, so a case that
        used to end as SUPPRESSED can now end as FAILED. services.py maps the
        two to different exception translation keys
        (``stop_sound_suppressed`` vs ``stop_sound_rejected``), so this is
        user-visible and must not drift unnoticed. The counter-case in the same
        test keeps the old class for a stop that is NOT correlated.
        """

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() + 90.0
        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.TRANSPORT_FAILED

        # No key: unchanged, still never sent.
        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()

        # Same window, same transport, but now a proven key.
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"
        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.FAILED
        coord.api.async_stop_sound.assert_awaited_once_with("dev-1", "uuid-fresh")

    # ---- the boundary: everything below must stay suppressed ----

    async def test_keyless_stop_stays_suppressed_during_the_cooldown(
        self, coord: LocateStub
    ) -> None:
        """Without a key the stop would be UNCORRELATED, so it buys nothing."""

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() + 90.0

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()

    async def test_stop_stays_suppressed_when_push_is_down_without_a_cooldown(
        self, coord: LocateStub
    ) -> None:
        """The exception is bound to the cooldown, not to push readiness at large.

        A genuinely disconnected push transport is not something this stop
        inflicted on itself, and sending into it proves nothing.
        """

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = 0.0
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()

    async def test_expired_cooldown_does_not_break_a_push_outage(
        self, coord: LocateStub
    ) -> None:
        """Boundary of the window: a cooldown that has run out grants nothing."""

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() - 0.01
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()

    async def test_stale_cached_key_stays_suppressed_during_the_cooldown(
        self, coord: LocateStub
    ) -> None:
        """A key older than SOUND_UUID_MAX_AGE_S cannot be the one this cooldown made.

        The cooldown lasts 90 seconds, the key aged past 30 minutes: it belongs
        to an older play, and a stop carrying it would report UNCORRELATED. That
        is the keyless case with extra steps, so it stays suppressed.
        """

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() + 90.0
        coord._sound_request_uuids["dev-1"] = "uuid-old"
        coord._sound_request_timestamps["dev-1"] = time.time() - 3600.0

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()

    async def test_foreign_explicit_key_stays_suppressed_during_the_cooldown(
        self, coord: LocateStub
    ) -> None:
        """An unverifiable key is a claim, not a handle, so it grants no exception.

        Mirrors the rule one layer down: an explicitly passed key only proves
        correlation when it IS our own fresh cached key.

        We DO hold a live key for this device here, and that is the point: the
        discriminating fact is not "some key exists for dev-1" but "the key
        going on the wire is ours". Sending the caller's string would report
        CANCELLED for a ring we never addressed, and spend our own handle doing
        it.
        """

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() + 90.0
        coord._sound_request_uuids["dev-1"] = "uuid-ours-fresh"

        outcome = await coord.async_stop_sound("dev-1", request_uuid="foreign-uuid")

        assert outcome is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()


class TestCorrelationPredicateIsShared:
    """The cooldown gate, the outcome and the stop delay must read ONE definition.

    ``_stop_would_be_correlated`` was extracted precisely because several
    decisions depend on the same question -- may this stop break a
    self-inflicted push cooldown, is an accepted stop CANCELLED, and may it be
    held back until STOP_SOUND_MIN_DELAY_AFTER_PLAY_S -- and a second, inline
    re-derivation at any site is free to drift away from the first. (The
    second decision used to be "may an accepted stop spend the cached key";
    a stop no longer spends it, IRR-CA-STOP-KEEPS-CANCEL-KEY.) These two tests
    bind the sites to the predicate by making the predicate disagree with the
    raw cache state: an inline re-derivation would read the cache and answer
    the opposite, so it cannot pass.
    """

    async def test_gate_follows_the_predicate_against_the_raw_cache(
        self, coord: LocateStub
    ) -> None:
        """Predicate says no while the cache holds a fresh key of ours."""

        coord._api_push_ready.return_value = False
        coord._push_cooldown_until = time.monotonic() + 90.0
        coord._sound_request_uuids["dev-1"] = "uuid-fresh"
        coord._stop_would_be_correlated = MagicMock(return_value=False)

        assert await coord.async_stop_sound("dev-1") is StopSoundOutcome.SUPPRESSED
        coord.api.async_stop_sound.assert_not_called()
        coord._stop_would_be_correlated.assert_called_once_with("dev-1", None)

    async def test_outcome_and_delay_follow_the_predicate_against_the_raw_cache(
        self, coord: LocateStub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Predicate says yes while the cached key has aged past the limit."""

        coord._api_push_ready.return_value = True
        coord._sound_request_uuids["dev-1"] = "uuid-old"
        coord._sound_request_timestamps["dev-1"] = time.time() - 3600.0
        coord._sound_play_accepted_at = {"dev-1": ("uuid-old", time.monotonic() - 3.0)}
        sleep = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "custom_components.googlefindmy.coordinator.locate.asyncio.sleep", sleep
        )
        coord._stop_would_be_correlated = MagicMock(return_value=True)
        coord.api.async_stop_sound.return_value = SoundDispatchOutcome.ACCEPTED

        outcome = await coord.async_stop_sound("dev-1")

        assert outcome is StopSoundOutcome.CANCELLED
        sleep.assert_awaited_once()
        assert coord._sound_request_uuids["dev-1"] == "uuid-old"


_ = DEFAULT_MIN_POLL_INTERVAL  # silence unused-import lint when production no-ops
