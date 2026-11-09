# tests/test_stop_sound_keep_key_and_defer.py
"""Pin the two Stop Sound rules that came out of the live measurement series.

Both rules rest on a live test against a moto tag 2 (2026-10-17/18, ten rings,
recorded in the fork's issue tracker as the follow-up to BSkando#209):

* **The cancel key survives a correlated stop.** A stop that went out with our
  own fresh key used to pop that key, so a second press sent no key at all and
  was reported as "could not be matched" for a ring this integration had
  started itself. The key is now kept until it ages out or a newer play
  replaces it; a repeated stop carries the same key again.

* **A stop is not sent into the start of the ring.** In two of two runs a stop
  that reached the tracker roughly when the ring was about to start left the
  tracker silent (no BLE advertisements until a long button press). Stops that
  arrived 11.6 s or later after Nova accepted the play went through in four of
  four runs. A stop issued earlier than ``STOP_SOUND_MIN_DELAY_AFTER_PLAY_S``
  after the acceptance of OUR play is therefore held back until that point.
  Only our own accepted play arms the delay: a stop without a key, with an aged
  key, with a foreign key, or after a merely ambiguous play is sent at once,
  because nothing tells us when that ring began.
"""

from __future__ import annotations

import logging
import time
from types import SimpleNamespace

import pytest

from custom_components.googlefindmy.const import (
    PlaySoundOutcome,
    PlaySoundResult,
    SoundDispatchOutcome,
    StopSoundOutcome,
)
from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from custom_components.googlefindmy.coordinator import locate as locate_module

# Literal on purpose: the measured boundary, not a re-export of the constant.
# test_min_delay_constant_matches_the_measured_boundary ties the two together.
_MIN_DELAY_S = 12.0


@pytest.fixture
def events(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, object]]:
    """Record the deferral sleep and the sends of one test in one ordered list.

    ``asyncio.sleep`` (which ``coordinator.locate`` looks up at call time) is
    replaced process-wide for this test, so the production line that waits
    runs without waiting (tests/AGENTS.md: no sleeps, monkeypatch instead).
    """

    recorded: list[tuple[str, object]] = []

    async def _sleep(delay: float) -> None:
        recorded.append(("wait", delay))

    monkeypatch.setattr(locate_module.asyncio, "sleep", _sleep)
    return recorded


def _coordinator(
    events: list[tuple[str, object]],
    *,
    uuids: dict[str, str] | None = None,
    accepted_at: dict[str, tuple[str, float]] | None = None,
) -> GoogleFindMyCoordinator:
    """Build a bare coordinator whose sends land in ``events`` next to the waits."""

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = dict(uuids or {})  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {  # type: ignore[attr-defined]
        device_id: time.time() for device_id in (uuids or {})
    }
    coordinator._sound_play_accepted_at = dict(accepted_at or {})  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]
    coordinator._push_cooldown_until = 0.0  # type: ignore[attr-defined]

    async def _save() -> None:
        return None

    coordinator._async_save_sound_uuids = _save  # type: ignore[attr-defined]

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        events.append(("send", request_uuid))
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]
    return coordinator


def test_min_delay_constant_matches_the_measured_boundary() -> None:
    """The production constant is the boundary the live runs support."""

    from custom_components.googlefindmy.const import (
        STOP_SOUND_MIN_DELAY_AFTER_PLAY_S,
    )

    assert STOP_SOUND_MIN_DELAY_AFTER_PLAY_S == _MIN_DELAY_S


@pytest.mark.asyncio
async def test_a_correlated_stop_keeps_the_cancel_key_for_a_second_press(
    events: list[tuple[str, object]],
) -> None:
    """K9: the second press carries the same key and is still correlated."""

    coordinator = _coordinator(events, uuids={"device-1": "uuid-1"})

    first = await coordinator.async_stop_sound("device-1")
    second = await coordinator.async_stop_sound("device-1")

    assert first is StopSoundOutcome.CANCELLED
    assert second is StopSoundOutcome.CANCELLED
    assert events == [("send", "uuid-1"), ("send", "uuid-1")]
    assert coordinator._sound_request_uuids == {"device-1": "uuid-1"}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_a_kept_key_lets_a_repeated_stop_through_a_running_cooldown(
    events: list[tuple[str, object]],
) -> None:
    """The cooldown exception still applies to the repeated stop, and cannot feed itself.

    Before the key was kept, the second press had no key, so it was not
    correlated and the gate suppressed it while a window ran. Now it is our own
    fresh key again: it goes out, and a transport failure on it leaves the
    running window exactly where it was
    (IRR-CA-STOP-BREAKS-SELF-INFLICTED-COOLDOWN).
    """

    coordinator = _coordinator(events, uuids={"device-1": "uuid-1"})
    first = await coordinator.async_stop_sound("device-1")
    assert first is StopSoundOutcome.CANCELLED

    window_ends_at = time.monotonic() + 60.0
    coordinator._push_cooldown_until = window_ends_at  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: False  # type: ignore[attr-defined]

    def _arm_absolutely() -> None:
        coordinator._push_cooldown_until = time.monotonic() + 90.0  # type: ignore[attr-defined]

    coordinator._note_push_transport_problem = _arm_absolutely  # type: ignore[attr-defined]

    async def _failing_stop(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        events.append(("send", request_uuid))
        return SoundDispatchOutcome.TRANSPORT_FAILED

    coordinator.api = SimpleNamespace(async_stop_sound=_failing_stop)  # type: ignore[attr-defined]

    second = await coordinator.async_stop_sound("device-1")

    assert second is StopSoundOutcome.FAILED
    assert events[-1] == ("send", "uuid-1")
    assert coordinator._push_cooldown_until == window_ends_at  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_an_accepted_play_records_when_nova_accepted_it(
    events: list[tuple[str, object]],
) -> None:
    """The delay is measured from the acceptance of THIS play, tied to its key."""

    coordinator = _coordinator(events)
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        return PlaySoundResult(SoundDispatchOutcome.ACCEPTED, "uuid-play")

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    before = time.monotonic()
    result = await coordinator.async_play_sound("device-1")
    after = time.monotonic()

    assert result is PlaySoundOutcome.ACCEPTED
    key, accepted_at = coordinator._sound_play_accepted_at["device-1"]  # type: ignore[attr-defined]
    assert key == "uuid-play"
    assert before <= accepted_at <= after


@pytest.mark.asyncio
async def test_an_ambiguous_play_does_not_arm_the_delay(
    events: list[tuple[str, object]],
) -> None:
    """A play that was not accepted proves no start time, so nothing is held back."""

    coordinator = _coordinator(events)
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        return PlaySoundResult(SoundDispatchOutcome.TRANSPORT_FAILED, "uuid-maybe")

    coordinator.api = SimpleNamespace(  # type: ignore[attr-defined]
        async_play_sound=_async_play_sound,
        async_stop_sound=coordinator.api.async_stop_sound,  # type: ignore[attr-defined]
    )

    await coordinator.async_play_sound("device-1")
    assert coordinator._sound_request_uuids == {"device-1": "uuid-maybe"}  # type: ignore[attr-defined]
    assert "device-1" not in coordinator._sound_play_accepted_at  # type: ignore[attr-defined]

    await coordinator.async_stop_sound("device-1")
    assert [kind for kind, _ in events] == ["send"]


@pytest.mark.asyncio
async def test_a_stop_three_seconds_after_acceptance_waits_until_twelve(
    caplog: pytest.LogCaptureFixture,
    events: list[tuple[str, object]],
) -> None:
    """K15: the stop is sent only after the remaining delay, and the wait is logged."""

    coordinator = _coordinator(
        events,
        uuids={"device-1": "uuid-1"},
        accepted_at={"device-1": ("uuid-1", time.monotonic() - 3.0)},
    )

    with caplog.at_level(logging.INFO):
        result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.CANCELLED
    assert [kind for kind, _ in events] == ["wait", "send"]
    delay = events[0][1]
    assert isinstance(delay, float)
    assert _MIN_DELAY_S - 3.0 - 0.5 < delay <= _MIN_DELAY_S - 3.0
    assert events[1] == ("send", "uuid-1")
    assert "Stop Sound deferred" in caplog.text


@pytest.mark.asyncio
async def test_a_stop_thirteen_seconds_after_acceptance_is_sent_at_once(
    events: list[tuple[str, object]],
) -> None:
    """Past the boundary there is nothing to wait for."""

    coordinator = _coordinator(
        events,
        uuids={"device-1": "uuid-1"},
        accepted_at={"device-1": ("uuid-1", time.monotonic() - 13.0)},
    )

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.CANCELLED
    assert events == [("send", "uuid-1")]


@pytest.mark.asyncio
async def test_a_stop_without_a_key_is_never_deferred(
    events: list[tuple[str, object]],
) -> None:
    """No key of ours, no known start: the stop goes out at once."""

    coordinator = _coordinator(events)

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.UNCORRELATED
    assert events == [("send", None)]


@pytest.mark.asyncio
async def test_a_foreign_key_is_not_deferred_by_our_own_recent_play(
    events: list[tuple[str, object]],
) -> None:
    """The delay belongs to our key; a caller-supplied foreign key does not inherit it."""

    coordinator = _coordinator(
        events,
        uuids={"device-1": "uuid-ours"},
        accepted_at={"device-1": ("uuid-ours", time.monotonic() - 3.0)},
    )

    result = await coordinator.async_stop_sound("device-1", "uuid-foreign")

    assert result is StopSoundOutcome.UNCORRELATED
    assert events == [("send", "uuid-foreign")]


@pytest.mark.asyncio
async def test_an_acceptance_stamp_for_an_older_key_does_not_defer(
    events: list[tuple[str, object]],
) -> None:
    """A stamp left by a previous play does not hold back the stop of a newer key."""

    coordinator = _coordinator(
        events,
        uuids={"device-1": "uuid-new"},
        accepted_at={"device-1": ("uuid-old", time.monotonic() - 3.0)},
    )

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.CANCELLED
    assert events == [("send", "uuid-new")]


@pytest.mark.asyncio
async def test_a_suppressed_stop_does_not_wait_first(
    events: list[tuple[str, object]],
) -> None:
    """A stop the gate declines returns at once instead of sleeping and then declining."""

    coordinator = _coordinator(
        events,
        accepted_at={"device-1": ("uuid-1", time.monotonic() - 3.0)},
    )
    coordinator._api_push_ready = lambda: False  # type: ignore[attr-defined]

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.SUPPRESSED
    assert events == []


@pytest.mark.asyncio
async def test_the_delay_follows_the_correlation_predicate_when_it_says_no(
    events: list[tuple[str, object]],
) -> None:
    """Only a stop ``_stop_would_be_correlated`` vouches for is held back.

    The stamp alone matches here (same key, three seconds old), so a delay that
    read the stamp without asking the predicate would wait. The predicate is
    the single definition behind the cooldown gate, the CANCELLED outcome and
    this delay; the counterpart in test_coordinator_locate_basics.py pins the
    "yes" direction.
    """

    coordinator = _coordinator(
        events,
        uuids={"device-1": "uuid-1"},
        accepted_at={"device-1": ("uuid-1", time.monotonic() - 3.0)},
    )
    coordinator._stop_would_be_correlated = lambda _d, _u: False  # type: ignore[method-assign]

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.UNCORRELATED
    assert events == [("send", "uuid-1")]
