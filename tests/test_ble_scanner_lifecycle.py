# tests/test_ble_scanner_lifecycle.py
"""Lifecycle of the domain-wide BLE scanner callback and the shared EID resolver.

Several config entries share one FMDN BLE callback and one
``GoogleFindMyEIDResolver``. These tests pin the lifecycle:

* every entry setup replaces the registered callback, so exactly one stays
  active and Home Assistant replays the advertisement history for the entry
  that just loaded;
* the shared singletons are released only when no other entry of the domain
  is ``LOADED``; an entry in ``SETUP_RETRY`` (still listed in
  ``hass.data[DOMAIN]["entries"]``) does not keep them alive, and a setup that
  lost them to such a release restores them at its end;
* two concurrent entry setups create one resolver, and neither returns before
  the running refresh, including the pass the second one requested, is done;
  a failed holder hands the pass to the waiter, and the wait is bounded.

The helpers in ``custom_components.googlefindmy`` are reached through the
module attribute inside each test, so a mutant that drops a helper fails the
one test instead of the collection of the whole file.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from collections.abc import Callable
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from homeassistant.config_entries import ConfigEntryState

import custom_components.googlefindmy as gfm
from custom_components.googlefindmy import eid_resolver as eid_resolver_module
from custom_components.googlefindmy.const import DATA_EID_RESOLVER, DOMAIN
from custom_components.googlefindmy.eid_resolver import GoogleFindMyEIDResolver
from custom_components.googlefindmy.fmdn_finder.ble_scanner import (
    DATA_BLE_SCANNER_UNSUB,
    async_setup_ble_scanner,
)
from tests.helpers.config_entries_stub import make_config_entry

_BT_MODULE = "homeassistant.components.bluetooth"
_SCANNER_MODULE = "custom_components.googlefindmy.fmdn_finder.ble_scanner"


@pytest.fixture
def fake_bluetooth(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Install a fake bluetooth module that replays a history on registration.

    Modelled on ``fake_bluetooth`` in ``tests/test_ble_scanner_contracts.py``,
    plus the part of ``BluetoothManager.async_register_callback`` that matters
    here: it hands every cached advertisement to the new callback before it
    returns. Each registration gets a distinct handle; ``active`` holds the
    callbacks registered and not yet removed.
    """
    history = [SimpleNamespace(service_data={}, address="AA", rssi=-50, time=1.0)]
    state = SimpleNamespace(
        handles=[], active=set(), replays=0, history=history, register=None
    )

    def _register(hass: object, callback: Any, matcher: object, mode: object) -> Mock:
        number = len(state.handles) + 1
        state.active.add(number)

        def _remove() -> None:
            # HA removes the matcher from a list and raises when it is gone.
            if number not in state.active:
                raise ValueError("list.remove(x): x not in list")
            state.active.discard(number)

        handle = Mock(name=f"unsub-{number}", side_effect=_remove)
        state.handles.append(handle)
        for service_info in state.history:
            callback(service_info, "advertisement")
            state.replays += 1
        return handle

    state.register = MagicMock(side_effect=_register)

    module = ModuleType(_BT_MODULE)
    module.BluetoothChange = object  # type: ignore[attr-defined]
    module.BluetoothServiceInfoBleak = object  # type: ignore[attr-defined]
    module.BluetoothScanningMode = SimpleNamespace(PASSIVE="passive")  # type: ignore[attr-defined]
    module.async_register_callback = state.register  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, _BT_MODULE, module)
    return state


def _entry(entry_id: str, state: ConfigEntryState) -> SimpleNamespace:
    """Build a config entry stub in ``state`` (the enum, not its value)."""
    entry = make_config_entry(entry_id=entry_id)
    entry.state = state
    return entry


def _hass_with_entries(
    bucket: dict[str, Any], *entries: SimpleNamespace
) -> SimpleNamespace:
    """Build a hass double whose config entry manager lists ``entries``."""

    def _async_entries(domain: str | None = None) -> list[SimpleNamespace]:
        return [e for e in entries if domain is None or e.domain == domain]

    return SimpleNamespace(
        data={DOMAIN: bucket},
        config=SimpleNamespace(components={"bluetooth"}),
        config_entries=SimpleNamespace(async_entries=_async_entries),
        async_create_task=asyncio.ensure_future,
    )


def _bluetooth_hass(bucket: dict[str, Any]) -> SimpleNamespace:
    """Build a hass double with the bluetooth integration loaded."""
    return SimpleNamespace(
        data={DOMAIN: bucket}, config=SimpleNamespace(components={"bluetooth"})
    )


_TIMER_FLAGS = {"restart_check_registered": True, "url_refresh_registered": True}


# --------------------------------------------------------------------------- #
# T1: one active callback, history replayed for every entry setup              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_second_setup_does_not_register_again(
    fake_bluetooth: SimpleNamespace, caplog: pytest.LogCaptureFixture
) -> None:
    """A second setup replaces the callback: one stays active, history replays."""
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})
    caplog.set_level(logging.INFO, logger=_SCANNER_MODULE)

    assert await async_setup_ble_scanner(hass) is True
    assert await async_setup_ble_scanner(hass) is True

    assert fake_bluetooth.active == {2}
    assert fake_bluetooth.handles[0].call_count == 1
    assert fake_bluetooth.handles[1].call_count == 0
    assert bucket[DATA_BLE_SCANNER_UNSUB] is fake_bluetooth.handles[1]
    # The second entry got the history as well.
    assert fake_bluetooth.replays == 2 * len(fake_bluetooth.history)
    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    assert [
        r.getMessage().startswith("FMDN BLE scanner registered") for r in infos
    ] == [True]


@pytest.mark.asyncio
async def test_setup_keeps_existing_callback_when_not_replacing(
    fake_bluetooth: SimpleNamespace,
) -> None:
    """``replace_existing=False`` keeps a registered callback untouched."""
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})

    assert await async_setup_ble_scanner(hass) is True
    assert await async_setup_ble_scanner(hass, replace_existing=False) is True

    assert fake_bluetooth.register.call_count == 1
    assert fake_bluetooth.handles[0].call_count == 0
    assert bucket[DATA_BLE_SCANNER_UNSUB] is fake_bluetooth.handles[0]


@pytest.mark.asyncio
async def test_setup_replaces_a_handle_that_was_already_removed(
    fake_bluetooth: SimpleNamespace,
) -> None:
    """A stale handle (ValueError on removal) does not stop the new registration."""
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})
    assert await async_setup_ble_scanner(hass) is True
    fake_bluetooth.active.clear()  # removed behind our back

    assert await async_setup_ble_scanner(hass) is True

    assert fake_bluetooth.active == {2}
    assert bucket[DATA_BLE_SCANNER_UNSUB] is fake_bluetooth.handles[1]


@pytest.mark.asyncio
async def test_failed_replacement_keeps_the_old_callback(
    fake_bluetooth: SimpleNamespace,
) -> None:
    """The old callback is removed only after the new one is registered."""
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})
    assert await async_setup_ble_scanner(hass) is True
    fake_bluetooth.register.side_effect = RuntimeError("registration failed")

    with pytest.raises(RuntimeError):
        await async_setup_ble_scanner(hass)

    assert fake_bluetooth.active == {1}
    assert fake_bluetooth.handles[0].call_count == 0
    assert bucket[DATA_BLE_SCANNER_UNSUB] is fake_bluetooth.handles[0]


# --------------------------------------------------------------------------- #
# T2, T3, T6 and more: release only when no other entry is LOADED              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_unload_with_remaining_loaded_entry_keeps_callback() -> None:
    """Unloading one of two loaded entries leaves the callback registered."""
    unsub = Mock(name="unsub")
    bucket: dict[str, Any] = {DATA_BLE_SCANNER_UNSUB: unsub}
    hass = _hass_with_entries(
        bucket,
        _entry("entry_a", ConfigEntryState.UNLOAD_IN_PROGRESS),
        _entry("entry_b", ConfigEntryState.LOADED),
    )

    released = await gfm._async_release_shared_singletons_if_last(
        hass, bucket, "entry_a"
    )

    assert released is False
    assert unsub.call_count == 0
    assert bucket[DATA_BLE_SCANNER_UNSUB] is unsub


@pytest.mark.asyncio
async def test_unload_of_last_loaded_entry_unregisters_exactly_once() -> None:
    """Unloading the only entry unregisters the callback once."""
    unsub = Mock(name="unsub")
    bucket: dict[str, Any] = {DATA_BLE_SCANNER_UNSUB: unsub}
    hass = _hass_with_entries(
        bucket, _entry("entry_a", ConfigEntryState.UNLOAD_IN_PROGRESS)
    )

    assert await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")
    # A second unload finds no handle and must not call it again.
    await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")

    assert unsub.call_count == 1
    assert DATA_BLE_SCANNER_UNSUB not in bucket


@pytest.mark.asyncio
async def test_unload_unregisters_when_other_entry_is_in_setup_retry() -> None:
    """An entry stuck in SETUP_RETRY does not keep the callback registered.

    Its setup raised ``ConfigEntryNotReady`` after it joined the entries
    bucket, and Home Assistant never unloads an entry in that state.
    """
    unsub = Mock(name="unsub")
    entry_b = _entry("entry_b", ConfigEntryState.SETUP_RETRY)
    bucket: dict[str, Any] = {
        DATA_BLE_SCANNER_UNSUB: unsub,
        "entries": {"entry_b": object()},
    }
    hass = _hass_with_entries(
        bucket, _entry("entry_a", ConfigEntryState.UNLOAD_IN_PROGRESS), entry_b
    )

    assert await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")

    assert unsub.call_count == 1
    assert DATA_BLE_SCANNER_UNSUB not in bucket


@pytest.mark.asyncio
async def test_unloading_entry_does_not_count_itself_as_loaded() -> None:
    """The entry being unloaded never keeps the singletons alive, whatever its state.

    Home Assistant puts it in ``UNLOAD_IN_PROGRESS``; a caller outside that
    flow may still see it ``LOADED``, and the entry id excludes it then.
    """
    unsub = Mock(name="unsub")
    bucket: dict[str, Any] = {DATA_BLE_SCANNER_UNSUB: unsub}
    hass = _hass_with_entries(bucket, _entry("entry_a", ConfigEntryState.LOADED))

    assert await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")

    assert unsub.call_count == 1
    assert DATA_BLE_SCANNER_UNSUB not in bucket


@pytest.mark.asyncio
async def test_release_tolerates_an_already_removed_handle(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A handle that HA already removed (ValueError) does not abort the release."""
    unsub = Mock(name="unsub", side_effect=ValueError("list.remove(x): x not in list"))
    bucket: dict[str, Any] = {
        DATA_BLE_SCANNER_UNSUB: unsub,
        "restart_check_unsub": Mock(),
    }
    hass = _hass_with_entries(
        bucket, _entry("entry_a", ConfigEntryState.UNLOAD_IN_PROGRESS)
    )
    caplog.set_level(logging.DEBUG, logger=gfm.__name__)

    assert await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")

    assert unsub.call_count == 1
    assert DATA_BLE_SCANNER_UNSUB not in bucket
    assert "restart_check_unsub" not in bucket
    assert "BLE scanner handle was already removed" in caplog.text


@pytest.mark.asyncio
async def test_release_without_scanner_module_still_releases(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the scanner module the remaining singletons are released anyway."""
    monkeypatch.setitem(sys.modules, _SCANNER_MODULE, None)  # import -> ImportError
    resolver = Mock(spec=GoogleFindMyEIDResolver)
    bucket: dict[str, Any] = {DATA_EID_RESOLVER: resolver}
    hass = _hass_with_entries(
        bucket, _entry("entry_a", ConfigEntryState.UNLOAD_IN_PROGRESS)
    )

    assert await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")

    resolver.stop.assert_called_once_with()
    assert DATA_EID_RESOLVER not in bucket


@pytest.mark.asyncio
async def test_release_surfaces_unexpected_unsub_errors() -> None:
    """Only the documented ValueError is absorbed; anything else surfaces."""
    unsub = Mock(name="unsub", side_effect=KeyError("unexpected"))
    bucket: dict[str, Any] = {DATA_BLE_SCANNER_UNSUB: unsub}
    hass = _hass_with_entries(
        bucket, _entry("entry_a", ConfigEntryState.UNLOAD_IN_PROGRESS)
    )

    with pytest.raises(KeyError):
        await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")


@pytest.mark.asyncio
async def test_setup_helper_skips_when_bluetooth_is_not_loaded(
    fake_bluetooth: SimpleNamespace,
) -> None:
    """Without the bluetooth integration no registration is attempted."""
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(
        data={DOMAIN: bucket}, config=SimpleNamespace(components=set())
    )

    await gfm._async_setup_shared_ble_scanner(hass)

    assert fake_bluetooth.register.call_count == 0
    assert DATA_BLE_SCANNER_UNSUB not in bucket


@pytest.mark.asyncio
async def test_setup_helper_surfaces_runtime_errors_from_registration(
    fake_bluetooth: SimpleNamespace,
) -> None:
    """With bluetooth loaded, a RuntimeError from the registration propagates."""
    fake_bluetooth.register.side_effect = RuntimeError("unexpected")
    bucket: dict[str, Any] = {}

    with pytest.raises(RuntimeError):
        await gfm._async_setup_shared_ble_scanner(_bluetooth_hass(bucket))

    assert DATA_BLE_SCANNER_UNSUB not in bucket


@pytest.mark.asyncio
async def test_setup_helper_without_scanner_module_goes_on(
    monkeypatch: pytest.MonkeyPatch, fake_bluetooth: SimpleNamespace
) -> None:
    """A missing scanner module (ImportError) is the one optional fallback."""
    monkeypatch.setitem(sys.modules, _SCANNER_MODULE, None)  # import -> ImportError
    bucket: dict[str, Any] = {}

    await gfm._async_setup_shared_ble_scanner(_bluetooth_hass(bucket))

    assert fake_bluetooth.register.call_count == 0
    assert DATA_BLE_SCANNER_UNSUB not in bucket


# --------------------------------------------------------------------------- #
# T4, T4b and the bounded wait: one resolver, no early scanner registration    #
# --------------------------------------------------------------------------- #


def _probe_resolver_class(
    log: list[str],
    release: asyncio.Event,
    constructed: list[Any],
    *,
    fail_first: bool = False,
) -> type[GoogleFindMyEIDResolver]:
    """Return a subclass of the real resolver that records its refreshes.

    Only the lock, the pending flag and the pass counter are set up (no
    ``__post_init__``, no timers). ``async_refresh`` is inherited unchanged,
    so the early return of a caller that finds the lock held behaves as in
    production.
    """

    class _ProbeResolver(GoogleFindMyEIDResolver):
        def __init__(self, hass: Any) -> None:  # noqa: ARG002
            self._refresh_lock = asyncio.Lock()
            self._pending_refresh = False
            self._refresh_generation = 0
            self._refresh_calls = 0
            constructed.append(self)

        async def _refresh_cache(self) -> None:
            self._refresh_calls += 1
            log.append("refresh_start")
            await release.wait()
            if fail_first and self._refresh_calls == 1:
                log.append("refresh_failed")
                raise RuntimeError("first refresh failed")
            log.append("refresh_end")

        def _purge_uwt_state(self) -> None:
            return None

        def stop(self) -> None:
            log.append("stop")

    return _ProbeResolver


def _setup_runner(
    hass: SimpleNamespace, bucket: dict[str, Any], log: list[str]
) -> Callable[[str], Any]:
    """Return a coroutine factory mirroring the order in ``async_setup_entry``."""

    async def _setup(name: str) -> None:
        await gfm._async_ensure_shared_eid_resolver(hass, bucket)
        log.append(f"scanner:{name}")

    return _setup


async def _until(condition: Callable[[], bool]) -> None:
    """Handshake: yield until ``condition`` holds, at most two seconds."""
    async with asyncio.timeout(2):
        while not condition():
            await asyncio.sleep(0)


def _second_setup_arrived(constructed: list[Any]) -> bool:
    """True once the second setup has requested its pass (or built its own)."""
    return len(constructed) > 1 or (
        bool(constructed) and constructed[0]._pending_refresh
    )


@pytest.mark.asyncio
async def test_concurrent_setups_create_one_resolver_and_wait_for_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent setups share one resolver and wait for its full refresh."""
    log: list[str] = []
    release = asyncio.Event()
    constructed: list[Any] = []
    monkeypatch.setattr(
        gfm,
        "GoogleFindMyEIDResolver",
        _probe_resolver_class(log, release, constructed),
    )
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})
    setup = _setup_runner(hass, bucket, log)

    both = asyncio.ensure_future(asyncio.gather(setup("E1"), setup("E2")))
    await _until(lambda: _second_setup_arrived(constructed))
    # Snapshot first and assert after the setups finished, so a failing
    # assertion does not leave the gathered setups pending.
    before_release = list(log)

    release.set()
    async with asyncio.timeout(2):
        await both

    # (i) While the first refresh is blocked, no setup has reached the scanner.
    assert not [i for i in before_release if i.startswith("scanner:")], log

    # (iv) One resolver, and the bucket holds it.
    assert len(constructed) == 1, log
    assert bucket[DATA_EID_RESOLVER] is constructed[0]
    last_end = max(i for i, item in enumerate(log) if item == "refresh_end")
    scanner_positions = [i for i, item in enumerate(log) if item.startswith("scanner:")]
    # (ii) Every scanner registration follows the last finished refresh.
    assert len(scanner_positions) == 2, log
    assert all(pos > last_end for pos in scanner_positions), log
    # (iii) The pass requested by the second setup ran as well.
    assert log.count("refresh_start") == 2, log


@pytest.mark.asyncio
async def test_waiting_setup_returns_when_first_refresh_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the holder's refresh raises, the waiting setup runs the pass itself."""
    log: list[str] = []
    release = asyncio.Event()
    constructed: list[Any] = []
    monkeypatch.setattr(
        gfm,
        "GoogleFindMyEIDResolver",
        _probe_resolver_class(log, release, constructed, fail_first=True),
    )
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})
    setup = _setup_runner(hass, bucket, log)

    both = asyncio.ensure_future(
        asyncio.gather(setup("E1"), setup("E2"), return_exceptions=True)
    )
    await _until(lambda: _second_setup_arrived(constructed))
    release.set()
    async with asyncio.timeout(2):
        results = await both

    assert isinstance(results[0], RuntimeError)
    assert results[1] is None
    # The waiter ran the requested pass before it reached the scanner.
    assert log == [
        "refresh_start",
        "refresh_failed",
        "refresh_start",
        "refresh_end",
        "scanner:E2",
    ], log
    resolver = constructed[0]
    # No stale request is left behind to block later refresh scheduling.
    assert resolver._pending_refresh is False
    assert resolver._refresh_generation == 1
    # The instance stays in the bucket for the next setup attempt.
    assert len(constructed) == 1
    assert bucket[DATA_EID_RESOLVER] is resolver


@pytest.mark.asyncio
async def test_failed_refresh_does_not_leave_a_pending_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A request queued during a failing pass is dropped with the failure.

    ``_pending_refresh`` also tells the resolve path that a refresh is on its
    way; left set, it would keep the resolver from scheduling one while the
    lookup table is empty.
    """
    log: list[str] = []
    release = asyncio.Event()
    constructed: list[Any] = []
    probe_class = _probe_resolver_class(log, release, constructed, fail_first=True)
    resolver = probe_class(None)

    holder = asyncio.ensure_future(resolver.async_refresh())
    await _until(lambda: log == ["refresh_start"])
    await resolver.async_refresh()  # lock held, no wait: only a request
    assert resolver._pending_refresh is True
    release.set()
    with pytest.raises(RuntimeError):
        async with asyncio.timeout(2):
            await holder

    assert resolver._pending_refresh is False
    assert not resolver._refresh_lock.locked()


@pytest.mark.asyncio
async def test_waiting_refresh_gives_up_at_the_wait_limit(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A waiter returns after the wait limit while the holder still runs."""
    monkeypatch.setattr(eid_resolver_module, "_REFRESH_WAIT_LIMIT_S", 0.05)
    log: list[str] = []
    release = asyncio.Event()
    constructed: list[Any] = []
    resolver = _probe_resolver_class(log, release, constructed)(None)
    caplog.set_level(logging.DEBUG, logger=eid_resolver_module.__name__)

    holder = asyncio.ensure_future(resolver.async_refresh())
    await _until(lambda: log == ["refresh_start"])
    try:
        async with asyncio.timeout(2):
            await resolver.async_refresh(wait=True)
        # Snapshot while the holder still runs; assert after it finished, so
        # a failing assertion does not leave the holder pending.
        at_return = (list(log), resolver._refresh_lock.locked())
        pending_at_return = resolver._pending_refresh
    finally:
        release.set()
        async with asyncio.timeout(2):
            await holder

    assert at_return == (["refresh_start"], True)
    assert pending_at_return is True  # still queued for the holder
    assert "EID refresh still running at the wait limit" in caplog.text
    assert log == ["refresh_start", "refresh_end", "refresh_start", "refresh_end"]


# --------------------------------------------------------------------------- #
# R1-F2: a setup restores singletons released while it was still running      #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_setup_restores_singletons_released_by_a_concurrent_unload(
    monkeypatch: pytest.MonkeyPatch, fake_bluetooth: SimpleNamespace
) -> None:
    """Entry A unloads while B is still setting up; B ends with both singletons."""
    log: list[str] = []
    release = asyncio.Event()
    release.set()
    constructed: list[Any] = []
    monkeypatch.setattr(
        gfm, "GoogleFindMyEIDResolver", _probe_resolver_class(log, release, constructed)
    )
    rearmed: list[object] = []

    async def _rearm(hass: Any, bucket: Any) -> None:
        rearmed.append(bucket)
        bucket.update(_TIMER_FLAGS)

    monkeypatch.setattr(gfm, "_async_ensure_restart_required_check", _rearm)
    bucket: dict[str, Any] = {}
    hass = _hass_with_entries(
        bucket,
        _entry("entry_a", ConfigEntryState.UNLOAD_IN_PROGRESS),
        _entry("entry_b", ConfigEntryState.SETUP_IN_PROGRESS),
    )

    # B's setup: resolver, then scanner.
    await gfm._async_ensure_shared_eid_resolver(hass, bucket)
    await gfm._async_setup_shared_ble_scanner(hass)
    first_resolver = bucket[DATA_EID_RESOLVER]
    # A unloads in B's window: B is not LOADED yet, so everything is released.
    assert await gfm._async_release_shared_singletons_if_last(hass, bucket, "entry_a")
    assert DATA_EID_RESOLVER not in bucket
    assert fake_bluetooth.active == set()

    # The end of B's setup.
    await gfm._async_restore_shared_singletons(hass, bucket)

    assert isinstance(bucket[DATA_EID_RESOLVER], GoogleFindMyEIDResolver)
    assert bucket[DATA_EID_RESOLVER] is not first_resolver
    assert fake_bluetooth.active == {2}
    assert bucket[DATA_BLE_SCANNER_UNSUB] is fake_bluetooth.handles[1]
    assert rearmed == [bucket]


@pytest.mark.asyncio
async def test_setup_end_leaves_present_singletons_alone(
    monkeypatch: pytest.MonkeyPatch, fake_bluetooth: SimpleNamespace
) -> None:
    """Without a release in between, the end of setup changes nothing."""
    resolver = Mock(spec=GoogleFindMyEIDResolver)
    unsub = Mock(name="unsub")
    bucket: dict[str, Any] = {
        DATA_EID_RESOLVER: resolver,
        DATA_BLE_SCANNER_UNSUB: unsub,
        **_TIMER_FLAGS,
    }
    hass = _bluetooth_hass(bucket)

    await gfm._async_restore_shared_singletons(hass, bucket)

    assert bucket[DATA_EID_RESOLVER] is resolver
    assert bucket[DATA_BLE_SCANNER_UNSUB] is unsub
    assert fake_bluetooth.register.call_count == 0
    resolver.async_refresh.assert_not_called()


@pytest.mark.asyncio
async def test_setup_end_gives_up_after_bounded_restore_attempts(
    monkeypatch: pytest.MonkeyPatch,
    fake_bluetooth: SimpleNamespace,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Releases racing every restore round end the loop instead of spinning."""
    rounds: list[object] = []

    async def _never_kept(hass: Any, bucket: Any) -> None:
        # Another unload releases the resolver again in every round. The
        # yield lets the outer timeout fire should the loop lose its bound.
        await asyncio.sleep(0)
        rounds.append(bucket)
        bucket.pop(DATA_EID_RESOLVER, None)

    async def _rearm(hass: Any, bucket: Any) -> None:
        return None

    monkeypatch.setattr(gfm, "_async_ensure_shared_eid_resolver", _never_kept)
    monkeypatch.setattr(gfm, "_async_ensure_restart_required_check", _rearm)
    caplog.set_level(logging.DEBUG, logger=gfm.__name__)
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})

    async with asyncio.timeout(2):
        await gfm._async_restore_shared_singletons(hass, bucket)

    assert len(rounds) == gfm._SHARED_SINGLETON_RESTORE_ATTEMPTS
    assert DATA_EID_RESOLVER not in bucket
    assert "Shared BLE singletons still missing after restore attempts" in caplog.text


@pytest.mark.asyncio
async def test_setup_end_restores_a_missing_scanner_next_to_a_present_resolver(
    monkeypatch: pytest.MonkeyPatch, fake_bluetooth: SimpleNamespace
) -> None:
    """Resolver present, BLE handle missing: only the scanner is registered.

    A concurrent setup recreates the resolver before its first await; if its
    refresh or scanner setup then fails, the resolver alone is no witness
    that the callback exists.
    """
    resolver = Mock(spec=GoogleFindMyEIDResolver)
    bucket: dict[str, Any] = {DATA_EID_RESOLVER: resolver, **_TIMER_FLAGS}

    async def _unexpected(hass: Any, bucket: Any) -> None:
        raise AssertionError("only the scanner was missing")

    monkeypatch.setattr(gfm, "_async_ensure_shared_eid_resolver", _unexpected)
    monkeypatch.setattr(gfm, "_async_ensure_restart_required_check", _unexpected)

    await gfm._async_restore_shared_singletons(_bluetooth_hass(bucket), bucket)

    assert fake_bluetooth.register.call_count == 1
    assert bucket[DATA_BLE_SCANNER_UNSUB] is fake_bluetooth.handles[0]
    assert bucket[DATA_EID_RESOLVER] is resolver


@pytest.mark.asyncio
async def test_setup_end_restores_in_the_last_round(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A restore that succeeds in the last allowed round logs no failure."""
    rounds: list[object] = []

    async def _kept_in_last_round(hass: Any, bucket: Any) -> None:
        await asyncio.sleep(0)
        rounds.append(bucket)
        if len(rounds) == gfm._SHARED_SINGLETON_RESTORE_ATTEMPTS:
            bucket[DATA_EID_RESOLVER] = Mock(spec=GoogleFindMyEIDResolver)
        else:
            bucket.pop(DATA_EID_RESOLVER, None)

    monkeypatch.setattr(gfm, "_async_ensure_shared_eid_resolver", _kept_in_last_round)
    caplog.set_level(logging.DEBUG, logger=gfm.__name__)
    bucket: dict[str, Any] = dict(_TIMER_FLAGS)
    hass = SimpleNamespace(data={DOMAIN: bucket})  # bluetooth not loaded

    async with asyncio.timeout(2):
        await gfm._async_restore_shared_singletons(hass, bucket)

    assert len(rounds) == gfm._SHARED_SINGLETON_RESTORE_ATTEMPTS
    assert isinstance(bucket[DATA_EID_RESOLVER], GoogleFindMyEIDResolver)
    assert "still missing" not in caplog.text


@pytest.mark.asyncio
async def test_missing_scanner_module_is_no_missing_singleton(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the scanner module only resolver and timers are witnesses."""
    monkeypatch.setitem(sys.modules, _SCANNER_MODULE, None)  # import -> ImportError
    bucket: dict[str, Any] = {
        DATA_EID_RESOLVER: Mock(spec=GoogleFindMyEIDResolver),
        **_TIMER_FLAGS,
    }

    assert gfm._missing_shared_singletons(_bluetooth_hass(bucket), bucket) == set()
    del bucket[DATA_EID_RESOLVER]
    assert gfm._missing_shared_singletons(_bluetooth_hass(bucket), bucket) == {
        "resolver"
    }
