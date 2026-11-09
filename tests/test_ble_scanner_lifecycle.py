# tests/test_ble_scanner_lifecycle.py
"""Lifecycle of the domain-wide BLE scanner callback and the shared EID resolver.

Several config entries share one FMDN BLE callback and one
``GoogleFindMyEIDResolver``. These tests pin the lifecycle:

* a second ``async_setup_ble_scanner`` on the same domain bucket does not
  register another callback;
* the parent-unload helper unregisters the callback only when no other entry
  of the domain is ``LOADED``; an entry in ``SETUP_RETRY`` (still listed in
  ``hass.data[DOMAIN]["entries"]``) does not keep it alive;
* two concurrent entry setups create one resolver, and neither returns before
  the running refresh, including the pass the second one requested, is done.

The helpers in ``custom_components.googlefindmy`` are reached through the
module attribute inside each test, so a mutant that drops a helper fails the
one test instead of the collection of the whole file.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from homeassistant.config_entries import ConfigEntryState

import custom_components.googlefindmy as gfm
from custom_components.googlefindmy.const import DATA_EID_RESOLVER, DOMAIN
from custom_components.googlefindmy.eid_resolver import GoogleFindMyEIDResolver
from custom_components.googlefindmy.fmdn_finder.ble_scanner import (
    DATA_BLE_SCANNER_UNSUB,
    async_setup_ble_scanner,
)
from tests.helpers.config_entries_stub import make_config_entry

_BT_MODULE = "homeassistant.components.bluetooth"


@pytest.fixture
def fake_bluetooth(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Install a fake bluetooth module whose register call returns a new handle.

    Modelled on ``fake_bluetooth`` in ``tests/test_ble_scanner_contracts.py``.
    Each call returns a distinct ``Mock``, so a second registration would
    replace the stored handle and orphan the first one.
    """
    handles: list[Mock] = []

    def _register(
        hass: object, callback: object, matcher: object, mode: object
    ) -> Mock:
        handle = Mock(name=f"unsub-{len(handles) + 1}")
        handles.append(handle)
        return handle

    register_mock = MagicMock(side_effect=_register)

    module = ModuleType(_BT_MODULE)
    module.BluetoothChange = object  # type: ignore[attr-defined]
    module.BluetoothServiceInfoBleak = object  # type: ignore[attr-defined]
    module.BluetoothScanningMode = SimpleNamespace(PASSIVE="passive")  # type: ignore[attr-defined]
    module.async_register_callback = register_mock  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, _BT_MODULE, module)
    return SimpleNamespace(register=register_mock, handles=handles)


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
        config_entries=SimpleNamespace(async_entries=_async_entries),
    )


# --------------------------------------------------------------------------- #
# T1: one registration per domain                                              #
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_second_setup_does_not_register_again(
    fake_bluetooth: SimpleNamespace,
) -> None:
    """A second entry setup reuses the callback registered by the first."""
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})

    assert await async_setup_ble_scanner(hass) is True
    assert await async_setup_ble_scanner(hass) is True

    assert fake_bluetooth.register.call_count == 1
    assert bucket[DATA_BLE_SCANNER_UNSUB] is fake_bluetooth.handles[0]


# --------------------------------------------------------------------------- #
# T2, T3, T6: unregister only when no other entry is LOADED                    #
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

    await gfm._async_unload_ble_scanner_if_last(hass, "entry_a")

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

    await gfm._async_unload_ble_scanner_if_last(hass, "entry_a")
    # A second unload finds no handle and must not call it again.
    await gfm._async_unload_ble_scanner_if_last(hass, "entry_a")

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

    await gfm._async_unload_ble_scanner_if_last(hass, "entry_a")

    assert unsub.call_count == 1
    assert DATA_BLE_SCANNER_UNSUB not in bucket


# --------------------------------------------------------------------------- #
# T4, T4b: one resolver, and no setup returns before the refresh is done       #
# --------------------------------------------------------------------------- #


def _probe_resolver_class(
    log: list[str],
    release: asyncio.Event,
    constructed: list[object],
    *,
    fail_first: bool = False,
) -> type[GoogleFindMyEIDResolver]:
    """Return a subclass of the real resolver that records its refreshes.

    Only the lock and the pending flag are set up (no ``__post_init__``, no
    timers). ``async_refresh`` is inherited unchanged, so the early return of
    a caller that finds the lock held behaves as in production.
    """

    class _ProbeResolver(GoogleFindMyEIDResolver):
        def __init__(self, hass: Any) -> None:  # noqa: ARG002
            self._refresh_lock = asyncio.Lock()
            self._pending_refresh = False
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

    return _ProbeResolver


def _setup_runner(
    hass: SimpleNamespace, bucket: dict[str, Any], log: list[str]
) -> Callable[[str], Any]:
    """Return a coroutine factory mirroring the order in ``async_setup_entry``."""

    async def _setup(name: str) -> None:
        await gfm._async_ensure_shared_eid_resolver(hass, bucket)
        log.append(f"scanner:{name}")

    return _setup


@pytest.mark.asyncio
async def test_concurrent_setups_create_one_resolver_and_wait_for_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two concurrent setups share one resolver and wait for its full refresh."""
    log: list[str] = []
    release = asyncio.Event()
    constructed: list[object] = []
    monkeypatch.setattr(
        gfm,
        "GoogleFindMyEIDResolver",
        _probe_resolver_class(log, release, constructed),
    )
    bucket: dict[str, Any] = {}
    hass = SimpleNamespace(data={DOMAIN: bucket})
    setup = _setup_runner(hass, bucket, log)

    both = asyncio.ensure_future(asyncio.gather(setup("E1"), setup("E2")))
    for _ in range(10):
        await asyncio.sleep(0)
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
    """A waiting setup is released, not stranded, when the holder's refresh raises."""
    log: list[str] = []
    release = asyncio.Event()
    constructed: list[object] = []
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
    for _ in range(10):
        await asyncio.sleep(0)
    release.set()
    async with asyncio.timeout(2):
        results = await both

    assert isinstance(results[0], RuntimeError)
    assert results[1] is None
    assert "scanner:E1" not in log
    assert "scanner:E2" in log
    # The instance stays in the bucket for the next setup attempt.
    assert len(constructed) == 1
    assert bucket[DATA_EID_RESOLVER] is constructed[0]
