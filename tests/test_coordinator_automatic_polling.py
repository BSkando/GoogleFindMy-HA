# tests/test_coordinator_automatic_polling.py
"""Per-tracker switch for automatic location polling (#242).

A tracker whose switch is off must not be located by any automatic path: the
regular tick, the cold start, a refresh requested through
``async_request_refresh``, ``force_poll_due``, a cycle that is already running
when the switch goes off, and the first refresh after a restart. A manual locate
stays allowed. The switch also has to work as a measuring instrument for the
comparison in #242: every change writes one INFO line without the device name,
the time of the last change survives a restart, and the diagnostics count the
requests that really left the host, split into automatic and manual.

New names of the feature are looked up with ``getattr`` and a ``pytest.fail``
instead of an import, so that on the commit before the feature this module is
collected and fails test by test instead of erroring at collection.

Test harness note: in this repository ``DataUpdateCoordinator`` is a stub
(``tests/conftest.py``) whose ``async_request_refresh`` does nothing. In Home
Assistant core it runs ``_async_refresh``, which awaits ``_async_update_data``.
The refresh test below replaces the stubbed method with exactly that step;
calling the stub alone would pass without polling anything.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import math
import re
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

import custom_components.googlefindmy as integration
from custom_components.googlefindmy import const, diagnostics
from custom_components.googlefindmy.const import DOMAIN
from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from custom_components.googlefindmy.coordinator.helpers import locate_clock
from custom_components.googlefindmy.NovaApi import nova_request as nova_request_module
from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker.location_request import (
    LocationRequestNotAcceptedError,
)
from custom_components.googlefindmy.NovaApi.nova_request import NovaAuthError
from tests.helpers.config_entries_stub import make_config_entry
from tests.helpers.locate_mixin_stub import LocateStub
from tests.test_diagnostics_p1_per_device import (
    _EXPECTED_DEVICE_KEYS,
    _make_entry_and_hass,
    _patch_registries,
    _PerDeviceCoordinator,
    _run,
)

# The option keys are spelled out here on purpose: the stored option is a
# contract with existing installations, so the string itself is under test.
_OPT_DISABLED = "automatic_polling_disabled"
_OPT_CHANGED_AT = "automatic_polling_changed_at"
_ENTRY_ID = "entry-auto"
_DEV_ON = {"id": "dev-on", "name": "Tag Polled"}
_DEV_OFF = {"id": "dev-off", "name": "Tag Quiet"}
_DEVICES = [_DEV_ON, _DEV_OFF]
_UTC_STAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_NEW_DEVICE_KEYS = {
    "automatic_polling",
    "automatic_polling_changed_at",
    "locate_requests_auto",
    "locate_requests_manual",
    "last_locate_request_age_s",
    "uwt_device_index",
}
_INTEGRATION_ROOT = Path(__file__).resolve().parents[1] / (
    "custom_components/googlefindmy"
)


def _require(owner: Any, name: str) -> Any:
    """Return ``owner.name`` or fail the test: the feature is not there yet."""
    value = getattr(owner, name, None)
    if value is None:
        pytest.fail(f"{getattr(owner, '__name__', owner)!s} has no {name}")
    return value


def _require_parameter(func: Any, name: str) -> None:
    """Fail the test unless ``func`` accepts the keyword ``name``."""
    if name not in inspect.signature(func).parameters:
        pytest.fail(f"{func.__qualname__} has no parameter {name}")


# ---------------------------------------------------------------------------
# Harness for the automatic path
# ---------------------------------------------------------------------------


class _Cache:
    async def async_get_cached_value(self, _key: str) -> None:
        return None

    async def async_set_cached_value(self, _key: str, _value: object) -> None:
        return None


class _Bus:
    def async_listen(self, *_args: object, **_kwargs: object) -> Any:
        return lambda: None

    def async_fire(self, *_args: object, **_kwargs: object) -> None:
        return None


class _Hass:
    """Minimal hass that keeps the background tasks a refresh starts."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop
        self.bus = _Bus()
        self.data: dict[str, Any] = {DOMAIN: {}}
        self.background: list[asyncio.Task[Any]] = []

    def async_create_task(self, coro: Any, *, name: str | None = None) -> Any:
        return self.loop.create_task(coro, name=name)

    def async_create_background_task(self, coro: Any, *, name: str) -> Any:
        task = self.loop.create_task(coro, name=name)
        self.background.append(task)
        return task


class _DispatchingAPI:
    """Stands in for ``api.async_get_device_location``.

    It calls the transport's dispatch observer where the real request POSTs,
    exactly like ``tests/test_coordinator_locate_request_clock.py``; without
    that call no counter could ever move and the counter tests would measure
    nothing. ``on_call`` runs before the hand-over and may change the options.
    """

    def __init__(self, on_call: Any = None) -> None:
        self.calls: list[str] = []
        self.on_call = on_call

    def is_push_ready(self) -> bool:
        return True

    async def async_get_device_location(self, dev_id: str, _dev_name: str) -> Any:
        self.calls.append(dev_id)
        if self.on_call is not None:
            self.on_call(dev_id)
        nova_request_module._notify_request_dispatch()
        return {}


def _make_coordinator(
    monkeypatch: pytest.MonkeyPatch,
    *,
    options: dict[str, Any],
    api: _DispatchingAPI,
) -> GoogleFindMyCoordinator:
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.GoogleFindMyCoordinator._async_load_stats",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.main.GoogleFindMyAPI",
        lambda *_args, **_kwargs: api,
    )
    hass = _Hass(asyncio.get_running_loop())
    coord = GoogleFindMyCoordinator(
        hass,
        cache=_Cache(),
        location_poll_interval=200,
        min_poll_interval=60,
        device_poll_delay=0,
    )
    entry = make_config_entry(entry_id=_ENTRY_ID, options=options, data={})
    # Not part of the canonical factory (tests/AGENTS.md): the refresh hands
    # its poll cycle to the entry, which hands it to hass.
    entry.async_create_background_task = lambda h, coro, name: (
        h.async_create_background_task(coro, name=name)
    )
    coord.config_entry = entry
    coord.api = api
    coord.async_set_updated_data = lambda _data: None
    coord.async_set_update_error = lambda _exc: None
    coord._get_google_home_filter = lambda: None
    coord._is_fcm_ready_soft = lambda: True
    coord._last_device_list = [dict(dev) for dev in _DEVICES]
    coord._last_list_poll_mono = time.monotonic()
    monkeypatch.setattr(coord, "_ensure_service_device_exists", lambda: None)
    monkeypatch.setattr(coord, "_ensure_registry_for_devices", lambda *_a, **_k: 0)
    monkeypatch.setattr(coord, "_refresh_subentry_index", lambda *_a, **_k: None)
    monkeypatch.setattr(
        coord,
        "_async_build_device_snapshot_with_fallbacks",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(coord, "_store_subentry_snapshots", lambda _snap: None)
    return coord


async def _drain(coord: GoogleFindMyCoordinator) -> None:
    """Await the poll cycles a refresh started, then the stats save task."""
    tasks = list(coord.hass.background)
    if tasks:
        await asyncio.gather(*tasks)
    save_task = getattr(coord, "_stats_save_task", None)
    if save_task is not None:
        save_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await save_task


async def _core_request_refresh(coord: GoogleFindMyCoordinator) -> None:
    """What ``DataUpdateCoordinator.async_request_refresh`` does in core."""
    coord.data = await coord._async_update_data()


async def _refresh(coord: GoogleFindMyCoordinator, trigger: str) -> None:
    """Run one refresh in the state each automatic trigger produces."""
    now = time.monotonic()
    if trigger == "cold_start":
        # First install: never polled, no location for any device.
        coord._last_poll_mono = 0.0
        coord._device_location_data.clear()
    else:
        for dev in _DEVICES:
            coord._device_location_data[dev["id"]] = {"last_seen": time.time()}
        if trigger == "restart":
            # First refresh after a restart: locations restored, never polled.
            coord._last_poll_mono = 0.0
        elif trigger == "force_poll_due":
            coord._last_poll_mono = now
            coord.force_poll_due()
        else:
            coord._last_poll_mono = now - 1000.0
    if trigger == "request_refresh":
        await _core_request_refresh(coord)
    else:
        await coord._async_update_data()
    await _drain(coord)


def _auto_count(coord: Any, device_id: str) -> int:
    reader = _require(coord, "locate_request_counts")
    auto, _manual = reader(device_id)
    return auto


_AUTOMATIC_TRIGGERS = [
    "tick",
    "cold_start",
    "request_refresh",
    "force_poll_due",
    "restart",
]


# ---------------------------------------------------------------------------
# (a) (b) (c) (d) (f) and (p): no automatic request for a switched-off tracker
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", _AUTOMATIC_TRIGGERS)
async def test_switched_off_tracker_is_not_polled(
    monkeypatch: pytest.MonkeyPatch, trigger: str
) -> None:
    """(a) tick, (b) cold start, (c) requested refresh, (d) force_poll_due, (f) restart.

    One tracker off, one on, the same refresh: exactly one request, and it is
    for the tracker that is on. The control half (the tracker that is on is
    polled) keeps the test from passing when no cycle runs at all.
    """
    api = _DispatchingAPI()
    coord = _make_coordinator(
        monkeypatch, options={_OPT_DISABLED: [_DEV_OFF["id"]]}, api=api
    )

    await _refresh(coord, trigger)

    assert api.calls == [_DEV_ON["id"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("trigger", _AUTOMATIC_TRIGGERS)
async def test_counter_auto_stays_zero_for_switched_off_tracker(
    monkeypatch: pytest.MonkeyPatch, trigger: str
) -> None:
    """(p) The automatic counter of a tracker that is off stays 0; the one on counts 1."""
    api = _DispatchingAPI()
    coord = _make_coordinator(
        monkeypatch, options={_OPT_DISABLED: [_DEV_OFF["id"]]}, api=api
    )

    await _refresh(coord, trigger)

    assert _auto_count(coord, _DEV_OFF["id"]) == 0
    assert _auto_count(coord, _DEV_ON["id"]) == 1
    counts = _require(coord, "_locate_request_counts")
    assert _UTC_STAMP.match(counts.since)


# ---------------------------------------------------------------------------
# (e) and (p): switch goes off while the cycle is already running
# ---------------------------------------------------------------------------


def _switch_off_later(coord: GoogleFindMyCoordinator) -> Any:
    def _on_call(dev_id: str) -> None:
        if dev_id == _DEV_ON["id"]:
            options = dict(coord.config_entry.options)
            options[_OPT_DISABLED] = [_DEV_OFF["id"]]
            coord.config_entry.options = options

    return _on_call


@pytest.mark.asyncio
async def test_switch_off_during_running_cycle_skips_the_device(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(e) The cycle was planned with both trackers; the second is off by its turn."""
    api = _DispatchingAPI()
    coord = _make_coordinator(monkeypatch, options={}, api=api)
    api.on_call = _switch_off_later(coord)

    await coord._async_start_poll_cycle([dict(d) for d in _DEVICES], force=True)
    await _drain(coord)

    assert api.calls == [_DEV_ON["id"]]


@pytest.mark.asyncio
async def test_counter_auto_zero_when_switched_off_during_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(p) Same as (e), measured on the counter."""
    api = _DispatchingAPI()
    coord = _make_coordinator(monkeypatch, options={}, api=api)
    api.on_call = _switch_off_later(coord)

    await coord._async_start_poll_cycle([dict(d) for d in _DEVICES], force=True)
    await _drain(coord)

    assert _auto_count(coord, _DEV_ON["id"]) == 1
    assert _auto_count(coord, _DEV_OFF["id"]) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "key", "placeholders"),
    [
        (
            LocationRequestNotAcceptedError(stage="server_error", status=503),
            "locate_not_accepted",
            {"not_accepted": "1", "rejected": "0"},
        ),
        (NovaAuthError(404, "gone"), "locate_all_rejected", {"rejected": "1"}),
    ],
    ids=["not_accepted", "all_rejected"],
)
async def test_switched_off_mid_cycle_does_not_hide_the_cycle_verdict(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
    key: str,
    placeholders: dict[str, str],
) -> None:
    """(e2) A tracker skipped mid-cycle leaves the cycle's denominator (AP5-F1).

    Both trackers were planned; the one that is on is refused, and the other
    goes off at that moment and is skipped at its turn. Every tracker that was
    asked was refused, so the cycle must report the same verdict as without the
    skipped one, for both post-loop guards. Counting the skipped tracker in
    ``len(devices)`` left each sum one short and the cycle read as success.
    """
    api = _DispatchingAPI()
    coord = _make_coordinator(monkeypatch, options={}, api=api)
    switch_off = _switch_off_later(coord)

    async def _refuse(dev_id: str, _dev_name: str) -> Any:
        api.calls.append(dev_id)
        switch_off(dev_id)
        raise error

    api.async_get_device_location = _refuse  # type: ignore[method-assign]
    coord._set_auth_state = lambda **_kwargs: None  # type: ignore[method-assign]
    recorded: list[Exception] = []
    coord.async_set_update_error = recorded.append  # type: ignore[method-assign]

    await coord._async_start_poll_cycle([dict(d) for d in _DEVICES], force=True)
    await _drain(coord)

    # Reachability: the second tracker really was skipped, not polled.
    assert api.calls == [_DEV_ON["id"]]
    assert recorded, "the only asked tracker was refused and the cycle read as success"
    assert getattr(recorded[-1], "translation_key", None) == key
    assert recorded[-1].translation_placeholders == placeholders


# ---------------------------------------------------------------------------
# (j) every tracker off
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_trackers_off_runs_no_cycle_and_releases_auth_streak(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(j) No cycle, and the transient-auth streak is released as for "all disabled".

    With no pollable tracker the cycle that owns the streak can never run, so
    the refresh releases it (``polling.py``, "No pollable device remains").
    """
    api = _DispatchingAPI()
    coord = _make_coordinator(
        monkeypatch,
        options={_OPT_DISABLED: [d["id"] for d in _DEVICES]},
        api=api,
    )
    coord._consecutive_transient_auth_failures = 2

    await _refresh(coord, "tick")

    assert api.calls == []
    assert coord.hass.background == []
    assert coord._consecutive_transient_auth_failures == 0


# ---------------------------------------------------------------------------
# (h) prediction ignores switched-off and ignored trackers
# ---------------------------------------------------------------------------


def _regular_history(now: float) -> Any:
    from collections import deque

    # Regular 120 s rhythm whose next update is 30 s away.
    return deque([now - 330, now - 210, now - 90], maxlen=4)


@pytest.mark.asyncio
async def test_prediction_ignores_switched_off_and_ignored_histories(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(h) Only a tracker that is on and not ignored may steer the prediction."""
    now = time.time()
    api = _DispatchingAPI()
    coord = _make_coordinator(
        monkeypatch,
        options={_OPT_DISABLED: ["dev-off"], const.OPT_IGNORED_DEVICES: ["dev-ign"]},
        api=api,
    )
    coord._device_update_history.clear()
    coord._device_update_history["dev-off"] = _regular_history(now)
    coord._device_update_history["dev-ign"] = _regular_history(now)

    assert coord._get_predicted_poll_time() is None

    # Control: the same history on a tracker that is on does predict, so the
    # assertion above is not met by a history the predictor rejects anyway.
    coord._device_update_history["dev-on"] = _regular_history(now)
    assert coord._get_predicted_poll_time() == pytest.approx(now + 30.0)


# ---------------------------------------------------------------------------
# (g) and (q): manual locate stays allowed and counts as manual
# ---------------------------------------------------------------------------


def _manual_stub() -> tuple[LocateStub, _DispatchingAPI]:
    entry = make_config_entry(
        entry_id=_ENTRY_ID, options={_OPT_DISABLED: [_DEV_OFF["id"]]}
    )
    coord = LocateStub(config_entry=entry)
    api = _DispatchingAPI()
    coord.api.async_get_device_location = api.async_get_device_location
    return coord, api


@pytest.mark.asyncio
async def test_manual_locate_allowed_while_switched_off() -> None:
    """(g) The switch governs automatic requests only: one manual request goes out."""
    coord, api = _manual_stub()

    await coord.async_locate_device(_DEV_OFF["id"])

    assert api.calls == [_DEV_OFF["id"]]


@pytest.mark.asyncio
async def test_counter_manual_counts_manual_locate_only() -> None:
    """(q) A manual locate raises ``manual`` by one and leaves ``auto`` alone."""
    coord, _api = _manual_stub()

    await coord.async_locate_device(_DEV_OFF["id"])

    counts = _require(coord, "_locate_request_counts")
    assert counts.by_device.get(_DEV_OFF["id"], {}).get("manual") == 1
    assert counts.by_device.get(_DEV_OFF["id"], {}).get("auto", 0) == 0


# ---------------------------------------------------------------------------
# (r) the counter follows the hand-over, not the attempt
# ---------------------------------------------------------------------------


def test_counter_ignores_abort_before_post_and_counts_each_post() -> None:
    """(r) Nothing before the POST counts; a retry after a POST counts again."""
    ensure_locate_counts = _require(locate_clock, "ensure_locate_counts")
    _require_parameter(locate_clock.stamp_on_dispatch, "counts")
    _require_parameter(locate_clock.stamp_on_dispatch, "kind")
    stamps: dict[str, float] = {}
    counts = ensure_locate_counts(None)

    with locate_clock.stamp_on_dispatch(stamps, "dev", counts=counts, kind="auto"):
        pass  # the attempt stops before the transport POSTs
    assert counts.by_device.get("dev", {}).get("auto", 0) == 0

    with locate_clock.stamp_on_dispatch(stamps, "dev", counts=counts, kind="auto"):
        nova_request_module._notify_request_dispatch()
        nova_request_module._notify_request_dispatch()  # retry after the POST
    assert counts.by_device["dev"]["auto"] == 2

    # The same object comes back, so a second send site keeps counting.
    assert ensure_locate_counts(counts) is counts


# ---------------------------------------------------------------------------
# (i) coercion and option registration
# ---------------------------------------------------------------------------


def test_option_keys_are_registered() -> None:
    """(i) Both keys are known options; ``async_step_migrate`` rebuilds only those."""
    disabled_key = _require(const, "OPT_AUTOMATIC_POLLING_DISABLED")
    changed_key = _require(const, "OPT_AUTOMATIC_POLLING_CHANGED_AT")
    assert disabled_key == _OPT_DISABLED
    assert changed_key == _OPT_CHANGED_AT
    for key in (disabled_key, changed_key):
        assert key in const.OPTION_KEYS
    for name in (
        "OPT_AUTOMATIC_POLLING_DISABLED",
        "OPT_AUTOMATIC_POLLING_CHANGED_AT",
        "coerce_device_id_list",
        "coerce_changed_at_map",
    ):
        assert name in const.__all__
    assert const.DEFAULT_OPTIONS[_OPT_DISABLED] == []
    assert const.DEFAULT_OPTIONS[_OPT_CHANGED_AT] == {}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, []),
        (5, []),
        (math.nan, []),
        ("dev-a", []),
        (["dev-b", "", "dev-a", "dev-b", 3, math.nan], ["dev-a", "dev-b"]),
        (("dev-c",), ["dev-c"]),
    ],
    ids=["none", "int", "nan", "bare-string", "mixed-list", "tuple"],
)
def test_coerce_device_id_list(raw: Any, expected: list[str]) -> None:
    """(i) Only non-empty strings, deduplicated and sorted, never an exception."""
    coerce = _require(const, "coerce_device_id_list")
    assert coerce(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, {}),
        ([], {}),
        (
            {
                "dev-a": "2026-11-10T20:00:00Z",
                "": "2026-11-10T20:00:00Z",
                "dev-b": "yesterday",
                "dev-c": 1731268800,
                3: "2026-11-10T20:00:00Z",
            },
            {"dev-a": "2026-11-10T20:00:00Z"},
        ),
    ],
    ids=["none", "list", "mixed-map"],
)
def test_coerce_changed_at_map(raw: Any, expected: dict[str, str]) -> None:
    """(i) Only non-empty string keys with a UTC stamp of the fixed form survive."""
    coerce = _require(const, "coerce_changed_at_map")
    assert coerce(raw) == expected


# ---------------------------------------------------------------------------
# (k) identifiers in the legacy entry-scoped form
# ---------------------------------------------------------------------------


class _CoreLikeConfigEntries:
    """``async_update_entry`` with the comparison core uses (``!=``)."""

    def __init__(self) -> None:
        self.updates: list[dict[str, Any]] = []

    def async_update_entry(
        self, entry: Any, *, options: Any = None, **_kw: Any
    ) -> bool:
        if options is None or options == entry.options:
            return False
        entry.options = dict(options)
        self.updates.append(dict(options))
        return True

    def async_update_subentry(self, *_args: Any, **_kwargs: Any) -> bool:
        return False


@pytest.mark.asyncio
async def test_legacy_entry_scoped_ids_are_normalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(k) ``<entry_id>:<device_id>`` in the stored option still switches the tracker off."""
    entry = make_config_entry(
        entry_id=_ENTRY_ID,
        options={
            _OPT_DISABLED: [f"{_ENTRY_ID}:dev-off", "dev-other"],
            _OPT_CHANGED_AT: {f"{_ENTRY_ID}:dev-off": "2026-11-10T20:00:00Z"},
        },
    )
    hass = SimpleNamespace(config_entries=_CoreLikeConfigEntries())

    integration._migrate_entry_identifier_namespaces(hass, entry)

    assert entry.options[_OPT_DISABLED] == ["dev-off", "dev-other"]
    assert entry.options[_OPT_CHANGED_AT] == {"dev-off": "2026-11-10T20:00:00Z"}

    api = _DispatchingAPI()
    coord = _make_coordinator(monkeypatch, options=dict(entry.options), api=api)
    is_enabled = _require(coord, "is_automatic_polling_enabled")
    assert is_enabled("dev-off") is False
    assert is_enabled("dev-on") is True


# ---------------------------------------------------------------------------
# (l) the choke point: two send sites, no more
# ---------------------------------------------------------------------------


def test_location_requests_leave_only_through_two_send_sites() -> None:
    """(l) Every call of ``async_get_device_location`` outside ``api.py``.

    The switch filters at the poll cycle; a third call site would bypass it.
    Same reading as the plan's grep: definitions are not calls.
    """
    found: set[str] = set()
    for path in _INTEGRATION_ROOT.rglob("*.py"):
        relative = path.relative_to(_INTEGRATION_ROOT).as_posix()
        if relative == "api.py":
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            if "async_get_device_location(" in line and "def " not in line:
                found.add(relative)
    assert found == {"coordinator/polling.py", "coordinator/locate.py"}


# ---------------------------------------------------------------------------
# (m) (n) (o): the writer, against the real config-entry manager
# ---------------------------------------------------------------------------


def _switch_coordinator(
    monkeypatch: pytest.MonkeyPatch, hass: Any, entry: MockConfigEntry
) -> GoogleFindMyCoordinator:
    """Coordinator on the real core manager, so its ``!=`` decides (RV-6)."""
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.GoogleFindMyCoordinator._async_load_stats",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.coordinator.main.GoogleFindMyAPI",
        lambda *_args, **_kwargs: _DispatchingAPI(),
    )
    coord = GoogleFindMyCoordinator(_Hass(asyncio.get_running_loop()), cache=_Cache())
    coord.hass = hass
    coord.config_entry = entry
    coord._ensure_device_name_cache()[_DEV_OFF["id"]] = _DEV_OFF["name"]
    coord.data = [{"device_id": _DEV_OFF["id"]}, {"device_id": _DEV_ON["id"]}]
    return coord


def _core_entry(hass: Any, options: dict[str, Any]) -> MockConfigEntry:
    # MockConfigEntry rather than make_config_entry: this test needs the core's
    # own update and comparison, which only a real entry in a real manager has.
    entry = MockConfigEntry(domain=DOMAIN, entry_id=_ENTRY_ID, options=options)
    entry.add_to_hass(hass)
    return entry


@pytest.mark.asyncio
async def test_switch_writes_a_new_list_copy_on_write(
    hass: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(m) The stored list is replaced, never changed in place, and core stores it."""
    entry = _core_entry(hass, {_OPT_DISABLED: []})
    coord = _switch_coordinator(monkeypatch, hass, entry)
    stored_before = entry.options[_OPT_DISABLED]
    results: list[bool] = []
    original = hass.config_entries.async_update_entry

    def _spy(*args: Any, **kwargs: Any) -> bool:
        result = original(*args, **kwargs)
        results.append(result)
        return result

    monkeypatch.setattr(hass.config_entries, "async_update_entry", _spy)
    switch = _require(coord, "async_set_automatic_polling")

    switch(_DEV_OFF["id"], False)

    assert results == [True]
    assert entry.options[_OPT_DISABLED] == [_DEV_OFF["id"]]
    assert entry.options[_OPT_DISABLED] is not stored_before
    assert stored_before == []
    assert const.DEFAULT_OPTIONS[_OPT_DISABLED] == []


@pytest.mark.asyncio
async def test_switch_change_writes_one_info_line_without_name(
    hass: Any, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """(n) One INFO line per change, none without a change, never the name."""
    entry = _core_entry(hass, {})
    coord = _switch_coordinator(monkeypatch, hass, entry)
    switch = _require(coord, "async_set_automatic_polling")
    caplog.set_level(logging.DEBUG, logger="custom_components.googlefindmy")

    switch(_DEV_OFF["id"], False)
    switch(_DEV_OFF["id"], False)  # no change: no write, no line
    switch(_DEV_OFF["id"], True)

    ours = [
        r for r in caplog.records if r.name.startswith("custom_components.googlefindmy")
    ]
    change_lines = [
        r.getMessage()
        for r in ours
        if r.getMessage().startswith("AUTOMATIC_POLLING_CHANGE ")
    ]
    pattern = re.compile(
        r"^AUTOMATIC_POLLING_CHANGE canonical=dev-off new=(on|off) "
        r"changed=(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)$"
    )
    matches = [pattern.match(line) for line in change_lines]
    assert all(matches), change_lines
    assert [m.group(1) for m in matches if m] == ["off", "on"]
    assert all(
        r.levelno == logging.INFO
        for r in ours
        if r.getMessage().startswith("AUTOMATIC_POLLING_CHANGE ")
    )
    last = matches[-1]
    assert last is not None
    assert entry.options[_OPT_CHANGED_AT][_DEV_OFF["id"]] == last.group(2)
    assert not [
        r
        for r in ours
        if r.levelno >= logging.INFO and _DEV_OFF["name"] in r.getMessage()
    ]


@pytest.mark.asyncio
async def test_changed_at_survives_a_new_coordinator(
    hass: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(o) The time of the last change comes back after a restart; never switched is null."""
    entry = _core_entry(hass, {})
    first = _switch_coordinator(monkeypatch, hass, entry)
    _require(first, "async_set_automatic_polling")(_DEV_OFF["id"], False)
    stored = entry.options[_OPT_CHANGED_AT][_DEV_OFF["id"]]

    second = _switch_coordinator(monkeypatch, hass, entry)
    rows = second.build_per_device_diagnostics()

    # Rows are ordered by sorted device id: dev-off before dev-on.
    assert len(rows) == 2
    assert rows[0]["automatic_polling"] is False
    assert rows[0]["automatic_polling_changed_at"] == stored
    assert rows[1]["automatic_polling"] is True
    assert rows[1]["automatic_polling_changed_at"] is None
    assert const.DEFAULT_OPTIONS[_OPT_CHANGED_AT] == {}


# ---------------------------------------------------------------------------
# (s) and (t): diagnostics rows
# ---------------------------------------------------------------------------


def test_coordinator_without_init_reports_zero_counts_and_full_rows() -> None:
    """(s) No ``__init__``, no counter attribute: (0, 0) and complete rows, not ``[]``."""
    bare = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    assert _require(bare, "locate_request_counts")("dev-a") == (0, 0)

    coordinator = _PerDeviceCoordinator(
        data=[{"device_id": "dev-a"}, {"device_id": "dev-b"}]
    )
    rows = coordinator.build_per_device_diagnostics()

    assert len(rows) == 2
    for row in rows:
        assert set(row) >= _EXPECTED_DEVICE_KEYS | _NEW_DEVICE_KEYS
        assert row["automatic_polling"] is True
        assert row["locate_requests_auto"] == 0
        assert row["locate_requests_manual"] == 0
        assert row["last_locate_request_age_s"] is None
        assert row["uwt_device_index"] is None


def test_rows_link_to_uwt_events_and_request_age() -> None:
    """(t) ``uwt_device_index`` is the index the event block uses; the age comes from the stamp.

    Like every age in the device rows, the request age is rounded to 10 s
    (``_round_age``: sub-poll timing is hidden), so 42 s reads as 40.
    """
    _require_parameter(
        GoogleFindMyCoordinator.build_per_device_diagnostics, "uwt_order"
    )
    coordinator = _PerDeviceCoordinator(
        data=[{"device_id": "dev-a"}, {"device_id": "dev-b"}]
    )
    coordinator._locate_requested_mono = {"dev-a": time.monotonic() - 42.0}

    rows = coordinator.build_per_device_diagnostics(uwt_order={"dev-b": 0})

    assert rows[0]["uwt_device_index"] is None
    assert rows[1]["uwt_device_index"] == 0
    assert rows[0]["last_locate_request_age_s"] == 40
    assert rows[1]["last_locate_request_age_s"] is None


class _ResolverStub:
    """Counts snapshot reads; one read must feed both blocks (A16)."""

    def __init__(self, canonical_ids: list[str]) -> None:
        self.reads = 0
        self._ids = canonical_ids

    def uwt_transition_snapshot(self, _entry_id: str) -> Any:
        self.reads += 1
        events = [
            SimpleNamespace(
                canonical_id=cid,
                **{name: None for name in diagnostics._UWT_EVENT_FIELDS},
            )
            for cid in self._ids
        ]
        return SimpleNamespace(
            capacity=8, dropped=0, info_lines_per_device_hour=None, events=events
        )


def test_dump_links_device_rows_to_uwt_events_from_one_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(t) In the dump, a device row and its events carry the same index."""
    coordinator = _PerDeviceCoordinator(
        data=[{"device_id": "dev-a"}, {"device_id": "dev-b"}]
    )
    entry, hass = _make_entry_and_hass(coordinator)
    resolver = _ResolverStub(["dev-b"])
    hass.data[DOMAIN][diagnostics.DATA_EID_RESOLVER] = resolver
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))

    events = payload["uwt_transitions"]["events"]
    rows = payload["coordinator"]["devices"]
    assert resolver.reads == 1
    assert [event["device_index"] for event in events] == [0]
    assert rows[1]["uwt_device_index"] == events[0]["device_index"]
    assert rows[0]["uwt_device_index"] is None


def test_dump_counts_and_redacts_the_switch_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(u) Config shows a count only; both options carry ids and are redacted.

    The coordinator block names the start of the in-memory request counters,
    and the device row of a switched-off tracker reports it as off.
    """
    coordinator = _PerDeviceCoordinator(
        data=[{"device_id": "dev-a"}, {"device_id": "dev-b"}]
    )
    entry, hass = _make_entry_and_hass(coordinator)
    entry.options = {
        _OPT_DISABLED: ["dev-a", "dev-c"],
        _OPT_CHANGED_AT: {"dev-a": "2026-11-10T20:00:00Z"},
    }
    coordinator.config_entry = entry
    coordinator._locate_request_counts = locate_clock.ensure_locate_counts(None)
    _patch_registries(monkeypatch)

    payload = _run(diagnostics.async_get_config_entry_diagnostics(hass, entry))

    assert payload["config"]["automatic_polling_disabled_count"] == 2
    effective = payload["effective_config"]
    assert effective[_OPT_DISABLED] == [diagnostics.REDACTED] * 2
    assert effective[_OPT_CHANGED_AT] == [diagnostics.REDACTED]
    assert "dev-a" not in repr(effective)
    since = payload["coordinator"]["locate_requests_counted_since"]
    assert since == coordinator._locate_request_counts.since
    assert _UTC_STAMP.match(since)
    rows = payload["coordinator"]["devices"]
    assert [row["automatic_polling"] for row in rows] == [False, True]
    assert rows[0]["automatic_polling_changed_at"] == "2026-11-10T20:00:00Z"
