# tests/test_coordinator_sound_uuid.py
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
from custom_components.googlefindmy.coordinator.helpers.cache import (
    SOUND_UUID_MAX_AGE_S,
    is_sound_uuid_expired,
)


@pytest.mark.asyncio
async def test_async_play_sound_stores_uuid() -> None:
    """Play sound should cache the returned request UUID per device."""

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {}  # type: ignore[attr-defined]
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    api_calls: list[SimpleNamespace] = []

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        api_calls.append(SimpleNamespace(device_id=device_id))
        return PlaySoundResult(SoundDispatchOutcome.ACCEPTED, "uuid-1")

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.ACCEPTED
    assert coordinator._sound_request_uuids == {"device-1": "uuid-1"}  # type: ignore[attr-defined]
    assert api_calls == [SimpleNamespace(device_id="device-1")]


@pytest.mark.asyncio
async def test_async_play_sound_skips_store_on_non_accepted_play() -> None:
    """Do not cache a UUID when the command was not accepted.

    Under the success-only contract api.async_play_sound returns ``(False, None)``
    for *every* non-acceptance — a pre-dispatch guard (e.g. no FCM token), a
    connection-setup error, or a server rejection (401/403/5xx). The play never
    started a ring, so there is no cancel key to keep and the cache must stay
    empty (while a push-transport problem is still flagged).
    Regression for IRR-CA-CANCEL-KEY-ON-SUCCESS-ONLY.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {}  # type: ignore[attr-defined]
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    transport_problems: list[bool] = []
    coordinator._note_push_transport_problem = (  # type: ignore[attr-defined]
        lambda: transport_problems.append(True)
    )
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        # Non-accepted play (pre-dispatch guard / rejection): no cancel key.
        return PlaySoundResult(SoundDispatchOutcome.TRANSPORT_FAILED)

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.FAILED
    assert coordinator._sound_request_uuids == {}  # type: ignore[attr-defined]
    assert transport_problems == [True]


@pytest.mark.asyncio
async def test_non_accepted_play_keeps_existing_cancel_key() -> None:
    """A non-accepted Play must not clobber an earlier, still-ringing play's key.

    Codex regression (PR #1100, iter-4): a device is already ringing from an
    earlier successful Play whose cancel key is cached. A *new* Play attempt is
    not accepted — either it fails before reaching the wire OR the server rejects
    it with 401/403/5xx. Under the success-only contract api.async_play_sound
    returns ``(False, None)`` for *both* causes, so the coordinator keeps the
    existing key intact. Overwriting it (the iter-4 bug: a rejected play used to
    return its never-accepted UUID) would make the later Stop send a fresh,
    non-correlating UUID and leave the device ringing.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    # Earlier successful Play cached a valid cancel key for this device.
    coordinator._sound_request_uuids = {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        # New attempt not accepted (e.g. server 401/403, or pre-dispatch): the
        # success-only contract yields (False, None) — no cancel key.
        return PlaySoundResult(SoundDispatchOutcome.TRANSPORT_FAILED)

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.FAILED
    # The previous, still-valid cancel key survives untouched.
    assert coordinator._sound_request_uuids == {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_post_dispatch_ambiguous_play_caches_uuid() -> None:
    """A dispatched-but-unconfirmed Play caches its key so Stop can still cancel.

    Codex regression (PR #1100, iter-5): a network failure at/after the request
    reached the wire (server disconnect, read timeout) may have started a ring
    even without a 200. api.async_play_sound then returns ``(False, <uuid>)`` —
    ``ok`` is False, but the cancel key is preserved. The coordinator caches every
    non-null UUID, so it stores this key (letting a later Stop target the
    possibly-active ring) while still flagging the push-transport problem.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {}  # type: ignore[attr-defined]
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    transport_problems: list[bool] = []
    coordinator._note_push_transport_problem = (  # type: ignore[attr-defined]
        lambda: transport_problems.append(True)
    )
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        # Post-dispatch ambiguity: not confirmed (ok=False), but a ring may be
        # active, so the cancel key is returned for caching.
        return PlaySoundResult(SoundDispatchOutcome.TRANSPORT_FAILED, "uuid-postsend")

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.FAILED
    # The ambiguous-dispatch key is cached so a later Stop can target it.
    assert coordinator._sound_request_uuids == {"device-1": "uuid-postsend"}  # type: ignore[attr-defined]
    assert transport_problems == [True]


@pytest.mark.asyncio
async def test_ambiguous_play_does_not_overwrite_known_cancel_key() -> None:
    """An ambiguous new Play must not clobber a known-good cancel key.

    Codex regression (PR #1106, follow-up): a device is already ringing from an
    earlier accepted Play whose cancel key is cached. A *new* Play attempt hits a
    transient 5xx that may have been generated before Nova accepted the command,
    so api.async_play_sound returns ``(False, <new-uuid>)`` — ok is False but a
    fresh UUID is present. Storing every non-null UUID would overwrite the
    known-good key for the still-ringing earlier play, making the default Stop
    target the wrong request. The storing rule (overwrite only on ok=True or an
    empty slot) keeps the cached key intact while still flagging the transport
    problem.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    # Earlier accepted Play cached a valid cancel key for this device.
    coordinator._sound_request_uuids = {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    transport_problems: list[bool] = []
    coordinator._note_push_transport_problem = (  # type: ignore[attr-defined]
        lambda: transport_problems.append(True)
    )
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        # Ambiguous transient 5xx: not accepted (ok=False), fresh UUID present.
        return PlaySoundResult(
            SoundDispatchOutcome.TRANSPORT_FAILED, "uuid-ambiguous-new"
        )

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.FAILED
    # The known-good cancel key survives; the ambiguous UUID is discarded.
    assert coordinator._sound_request_uuids == {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]
    assert transport_problems == [True]


@pytest.mark.asyncio
async def test_ambiguous_play_keeps_fresh_tracked_cancel_key() -> None:
    """A fresh, timestamp-tracked cancel key still survives an ambiguous Play.

    The expiry-aware guard must only treat *aged* keys as absent. With a
    recent timestamp present, the known-good key is younger than
    SOUND_UUID_MAX_AGE_S, so an ambiguous ``(False, <new-uuid>)`` must not
    clobber it (the still-ringing earlier play keeps its cancel handle).
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {"device-1": time.time()}  # type: ignore[attr-defined]
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        return PlaySoundResult(
            SoundDispatchOutcome.TRANSPORT_FAILED, "uuid-ambiguous-new"
        )

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.FAILED
    assert coordinator._sound_request_uuids == {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_ambiguous_play_replaces_expired_cancel_key() -> None:
    """An expired cancel key must not block caching a fresh ambiguous UUID.

    Codex regression (PR #1106, follow-up 2): a prior Play key lingers past the
    30-minute sound-UUID expiry, then a new Play hits a dispatch-ambiguous
    failure returning ``(False, <new-uuid>)``. The reload filter would have
    discarded the stale key, so the store-path guard must treat it as absent
    and cache the new UUID — otherwise the only handle on the possibly-current
    ring is dropped and the default Stop keeps targeting the expired request.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-expired"}  # type: ignore[attr-defined]
    # Timestamp older than the max age: the load path would discard this key.
    coordinator._sound_request_timestamps = {  # type: ignore[attr-defined]
        "device-1": time.time() - (SOUND_UUID_MAX_AGE_S + 60)
    }
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    transport_problems: list[bool] = []
    coordinator._note_push_transport_problem = (  # type: ignore[attr-defined]
        lambda: transport_problems.append(True)
    )
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        return PlaySoundResult(
            SoundDispatchOutcome.TRANSPORT_FAILED, "uuid-fresh-ambiguous"
        )

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.FAILED
    # The expired key is treated as absent, so the fresh handle is cached.
    assert coordinator._sound_request_uuids == {"device-1": "uuid-fresh-ambiguous"}  # type: ignore[attr-defined]
    assert transport_problems == [True]


@pytest.mark.asyncio
async def test_async_stop_sound_sends_expired_cached_uuid_uncorrelated() -> None:
    """An aged cancel key is SENT but not TRUSTED, and it survives the attempt.

    Deliberate reversal of the previous behaviour. Nova queues an action until
    the tracker is reachable, which can take hours to days (BSkando#108), so an
    aged key may still be the handle on the ring that is audible right now. It
    cannot hit a different ring (it is device-bound) and a fresher key would
    have replaced it, so sending it can only help or do nothing, whereas
    dropping it guaranteed no correlation at all.

    Not trusted: the outcome stays UNCORRELATED, which reaches the user as an
    error rather than as a confirmed cancel. Not popped: unspent, it remains
    the best handle a second attempt has.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-expired"}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {  # type: ignore[attr-defined]
        "device-1": time.time() - (SOUND_UUID_MAX_AGE_S + 60)
    }
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]

    api_calls: list[tuple[str, str | None]] = []

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        api_calls.append((device_id, request_uuid))
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_stop_sound("device-1")

    # Aged key means unproven correlation, so the stop is not a success.
    assert result is StopSoundOutcome.UNCORRELATED
    # ... but it IS the key that goes on the wire, not None.
    assert api_calls == [("device-1", "uuid-expired")]
    # And it is not spent: no stop spends a key
    # (IRR-CA-STOP-KEEPS-CANCEL-KEY).
    assert coordinator._sound_request_uuids == {"device-1": "uuid-expired"}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_fresh_cancel_key_is_reported_cancelled_and_kept() -> None:
    """Counterpart to the aged-key case: a fresh key proves correlation.

    Same setup, only the timestamp differs. This pins that the aged-key
    behaviour above is driven by the age and by nothing else. The CANCELLED
    path used to spend the key it used; it now keeps it, because HTTP 200 does
    not prove the tracker executed the stop and a repeated press needs the
    same handle (IRR-CA-STOP-KEEPS-CANCEL-KEY).
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-fresh"}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {"device-1": time.time()}  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]

    async def _save() -> None:
        return None

    coordinator._async_save_sound_uuids = _save  # type: ignore[attr-defined]

    api_calls: list[tuple[str, str | None]] = []

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        api_calls.append((device_id, request_uuid))
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.CANCELLED
    assert api_calls == [("device-1", "uuid-fresh")]
    assert coordinator._sound_request_uuids == {"device-1": "uuid-fresh"}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_ambiguous_play_keeps_untracked_cancel_key() -> None:
    """An untracked cancel key (no timestamp entry) is treated as fresh.

    The staleness check must not crash or wrongly expire a key that lives in
    ``_sound_request_uuids`` without a matching ``_sound_request_timestamps``
    entry (a transient/anomalous state). Such a key cannot be proven aged, so
    an ambiguous Play must leave it intact rather than drop the only handle.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]
    # Timestamp map exists but has no entry for this device.
    coordinator._sound_request_timestamps = {}  # type: ignore[attr-defined]
    coordinator.can_play_sound = lambda _device_id: True  # type: ignore[assignment]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_play_sound(device_id: str) -> PlaySoundResult:
        return PlaySoundResult(
            SoundDispatchOutcome.TRANSPORT_FAILED, "uuid-ambiguous-new"
        )

    coordinator.api = SimpleNamespace(async_play_sound=_async_play_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_play_sound("device-1")

    assert result is PlaySoundOutcome.FAILED
    assert coordinator._sound_request_uuids == {"device-1": "uuid-ringing"}  # type: ignore[attr-defined]


def test_is_sound_uuid_expired_boundary() -> None:
    """The shared expiry predicate is exclusive at the max-age boundary."""

    now = 10_000.0
    # Exactly at the boundary is not yet expired (strictly greater-than).
    assert (
        is_sound_uuid_expired(now - SOUND_UUID_MAX_AGE_S, now, SOUND_UUID_MAX_AGE_S)
        is False
    )
    # One second past the boundary is expired.
    assert (
        is_sound_uuid_expired(now - SOUND_UUID_MAX_AGE_S - 1, now, SOUND_UUID_MAX_AGE_S)
        is True
    )
    # A just-stored key is fresh.
    assert is_sound_uuid_expired(now, now, SOUND_UUID_MAX_AGE_S) is False


@pytest.mark.asyncio
async def test_async_stop_sound_uses_cached_uuid() -> None:
    """Stop sound should look up a cached UUID when none is provided."""

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-1"}  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]

    api_calls: list[tuple[str, str | None]] = []

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        api_calls.append((device_id, request_uuid))
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.CANCELLED
    # Kept for a repeated press (IRR-CA-STOP-KEEPS-CANCEL-KEY).
    assert coordinator._sound_request_uuids == {"device-1": "uuid-1"}  # type: ignore[attr-defined]
    assert api_calls == [("device-1", "uuid-1")]


@pytest.mark.asyncio
async def test_async_stop_sound_warns_when_uuid_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Stop sound should warn when no cached UUID is available."""

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {}  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]

    api_calls: list[tuple[str, str | None]] = []

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        api_calls.append((device_id, request_uuid))
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]

    with caplog.at_level(logging.WARNING):
        result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.UNCORRELATED
    assert api_calls == [("device-1", None)]
    assert "No cancel key for device-1" in caplog.text
    assert "the ring may keep playing" in caplog.text


@pytest.mark.asyncio
async def test_blank_request_uuid_falls_back_to_the_cached_cancel_key() -> None:
    """A blank UUID means "no opinion", not "deliberately uncorrelated".

    The optional service field can be left blank, and a blank value used to
    take a shortcut past the cached-key lookup: the stop then travelled without
    any ``requestUuid`` (proto3 drops an empty string) while the caller was
    told ``CANCELLED``. Two defects at once -- a live cancel key was withheld,
    and an unprovable stop was reported as a correlated one.
    """

    for blank in ("", "   "):
        sent: list[str | None] = []
        coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
        coordinator._sound_request_uuids = {"device-1": "cached-key"}  # type: ignore[attr-defined]
        coordinator._sound_request_timestamps = {"device-1": time.time()}  # type: ignore[attr-defined]
        coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]
        coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
        coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

        async def _save() -> None:
            return None

        coordinator._async_save_sound_uuids = _save  # type: ignore[attr-defined]

        async def _api_stop(
            device_id: str, request_uuid: str | None
        ) -> SoundDispatchOutcome:
            sent.append(request_uuid)
            return SoundDispatchOutcome.ACCEPTED

        coordinator.api = SimpleNamespace(async_stop_sound=_api_stop)  # type: ignore[attr-defined]

        outcome = await coordinator.async_stop_sound("device-1", blank)

        assert sent == ["cached-key"], blank
        assert outcome is StopSoundOutcome.CANCELLED, blank
        assert coordinator._sound_request_uuids == {"device-1": "cached-key"}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_blank_request_uuid_without_a_cached_key_is_uncorrelated() -> None:
    """Blank in, no cache: the stop is submitted, but nothing proves an effect."""

    for blank in ("", "   "):
        sent: list[str | None] = []
        coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
        coordinator._sound_request_uuids = {}  # type: ignore[attr-defined]
        coordinator._sound_request_timestamps = {}  # type: ignore[attr-defined]
        coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]
        coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
        coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

        async def _api_stop(
            device_id: str, request_uuid: str | None
        ) -> SoundDispatchOutcome:
            sent.append(request_uuid)
            return SoundDispatchOutcome.ACCEPTED

        coordinator.api = SimpleNamespace(async_stop_sound=_api_stop)  # type: ignore[attr-defined]

        outcome = await coordinator.async_stop_sound("device-1", blank)

        assert sent == [None], blank
        assert outcome is StopSoundOutcome.UNCORRELATED, blank


@pytest.mark.asyncio
async def test_blank_uuid_falls_through_to_the_aged_cached_key() -> None:
    """The cached-key fallback must not depend on how "no key" was spelled.

    Before the blank values were normalised, passing "" skipped the cache
    lookup entirely: a valid, frontal cancel key sat unused while an empty
    field went on the wire. The key here is aged, so it is sent and reported
    UNCORRELATED, but it is sent -- that is the regression this pins.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "stale-key"}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {  # type: ignore[attr-defined]
        "device-1": time.time() - (SOUND_UUID_MAX_AGE_S + 60)
    }
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    saved: list[bool] = []

    async def _save() -> None:
        saved.append(True)

    coordinator._async_save_sound_uuids = _save  # type: ignore[attr-defined]

    sent: list[str | None] = []

    async def _api_stop(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        sent.append(request_uuid)
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_api_stop)  # type: ignore[attr-defined]

    outcome = await coordinator.async_stop_sound("device-1", "")

    assert sent == ["stale-key"]
    assert outcome is StopSoundOutcome.UNCORRELATED
    # Unproven, therefore unspent: still available to a retry.
    assert coordinator._sound_request_uuids == {"device-1": "stale-key"}  # type: ignore[attr-defined]
    assert saved == []


@pytest.mark.asyncio
async def test_a_play_landing_during_the_stop_keeps_its_fresh_key() -> None:
    """An interleaved Play keeps the fresher key it stored during the stop.

    The key is read before the Nova round trip. A Play that lands in between
    stores a fresher key for the same device, and nothing after the round trip
    may evict it -- that would throw away the handle of a ring that just
    started. This used to be guarded by popping the spent key by VALUE; since a
    stop spends no key at all (IRR-CA-STOP-KEEPS-CANCEL-KEY), the test pins the
    same outcome without a pop, so a reintroduced pop by device id fails here.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-old"}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {"device-1": time.time()}  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]

    async def _save() -> None:
        return None

    coordinator._async_save_sound_uuids = _save  # type: ignore[attr-defined]

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        # A Play completes while the stop is in flight.
        coordinator._sound_request_uuids[device_id] = "uuid-new"  # type: ignore[attr-defined]
        coordinator._sound_request_timestamps[device_id] = time.time()  # type: ignore[attr-defined]
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.CANCELLED
    assert coordinator._sound_request_uuids == {"device-1": "uuid-new"}  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_a_rejected_stop_does_not_clear_the_auth_failure_state() -> None:
    """A False from the api layer is not proof that the credentials worked.

    api.async_stop_sound swallows NovaAuthError and HTTP 401/403 into the same
    False as a timeout, so clearing the auth-failure state on that path erases
    the signal an expired sign-in produces.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {}  # type: ignore[attr-defined]
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]

    auth_calls: list[dict[str, object]] = []
    coordinator._set_auth_state = lambda **kwargs: auth_calls.append(kwargs)  # type: ignore[attr-defined]

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        return SoundDispatchOutcome.TRANSPORT_FAILED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]

    result = await coordinator.async_stop_sound("device-1")

    assert result is StopSoundOutcome.FAILED
    assert auth_calls == []


@pytest.mark.asyncio
async def test_an_explicit_key_that_is_ours_but_aged_is_not_called_foreign(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Our own aged key matches; it is only too old to vouch for.

    Warning the user that the key "does not match a live Play Sound request"
    would be untrue, and the implicit path already tells the two apart.
    """

    coordinator = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    coordinator._sound_request_uuids = {"device-1": "uuid-aged"}  # type: ignore[attr-defined]
    coordinator._sound_request_timestamps = {  # type: ignore[attr-defined]
        "device-1": time.time() - (SOUND_UUID_MAX_AGE_S + 60)
    }
    coordinator._api_push_ready = lambda: True  # type: ignore[attr-defined]
    coordinator._note_push_transport_problem = lambda: None  # type: ignore[attr-defined]
    coordinator._set_auth_state = lambda **kwargs: None  # type: ignore[attr-defined]

    async def _async_stop_sound(
        device_id: str, request_uuid: str | None
    ) -> SoundDispatchOutcome:
        return SoundDispatchOutcome.ACCEPTED

    coordinator.api = SimpleNamespace(async_stop_sound=_async_stop_sound)  # type: ignore[attr-defined]

    with caplog.at_level("DEBUG"):
        result = await coordinator.async_stop_sound("device-1", "uuid-aged")

    assert result is StopSoundOutcome.UNCORRELATED
    assert "does not match a live Play Sound request" not in caplog.text
    assert "has aged out" in caplog.text
