# tests/test_stale_device_removal.py
"""Devices that leave the Google account must leave Home Assistant too.

The coordinator fetches the complete device list of the account every
``DEVICE_LIST_POLL_INTERVAL`` seconds. A tracker device of this config entry
whose Google ids are all missing from two consecutive *fresh* lists is removed
together with its entities (Integration Quality Scale rule ``stale-devices``).
Until now such devices only disappeared as a side effect of the registry purge
on unload; once that purge is gone, nothing else would remove them.

Protection rules, one test each:

* S-R1  an empty or deferred list never removes anything, and a deferred
  list is no confirmation of an earlier miss (S-R1b);
* S-R2  a cached list never counts as a fresh one;
* S-R3a a fetch that dropped candidates without a valid id removes nothing;
* S-R3b a fetch with a canonicless ``warn`` drop removes nothing, while a
  ``benign`` drop (live: one per fetch in both accounts) does not block;
* S-R4  the service device stays;
* S-R5  each entry is checked against its own list only;
* S-R3d a device whose name the decoder dropped from this fetch for lack of a
  canonical id is still in the account and is never counted missing (F8);
* S-R6  removal does not add the device to ``ignored_devices``, and a removed
  device that comes back reloads the entry once so that the platforms set up
  its entities again (F1, V-8 (b));
* S-R7  an error in the removal never fails the update cycle;
* S-R8  a device is removed only when *none* of its Google ids is listed, and
  never when no id can be read from it;
* S-R9  a device must be missing from two consecutive fresh lists.

Log contract: a removal is logged at INFO with ``no longer in the Google
account``, and only for devices that are really gone (F5); the reload for a
returned device at INFO with ``are back in the Google account``; the first miss at INFO with ``missing from the Google device list``;
a blocked removal at DEBUG with ``stale-device removal skipped`` and
``reason=<key>``.

The tests run a real coordinator against the real Home Assistant registries
and drive whole update cycles with a mocked device list.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterator
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

_DOMAIN = "googlefindmy"
_PKG = "custom_components.googlefindmy"
_REMOVED = "no longer in the Google account"
_PENDING = "missing from the Google device list"
_SKIPPED = "stale-device removal skipped"


@pytest.fixture(autouse=True)
def _real_modules(use_real_homeassistant_modules: None) -> None:
    """Run every test in this module against the real Home Assistant modules."""


@pytest.fixture
def gfmy(monkeypatch: pytest.MonkeyPatch) -> Iterator[ModuleType]:
    """Import the integration fresh so it binds to the real registries."""
    from homeassistant.helpers import device_registry as real_dr
    from homeassistant.helpers import entity_registry as real_er

    import custom_components

    def _ours() -> set[str]:
        return {n for n in sys.modules if n == _PKG or n.startswith(f"{_PKG}.")}

    before = _ours()
    for name in before:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(
        custom_components,
        "googlefindmy",
        getattr(custom_components, "googlefindmy", None),
    )
    import custom_components.googlefindmy as mod

    assert mod.er is real_er, "integration still bound to a stub entity registry"
    assert mod.dr is real_dr, "integration still bound to a stub device registry"
    yield mod
    for name in _ours() - before:
        del sys.modules[name]


class _Cache:
    """Token cache stand-in; the update cycle reads nothing from it here."""

    entry_id: str | None = None

    async def async_get_cached_value(self, _key: str) -> Any:
        return None

    async def async_set_cached_value(self, _key: str, _value: Any) -> None:
        return None

    def get_cached_value(self, _key: str) -> Any:
        return None


def _dev(gid: str, name: str | None = None) -> dict[str, Any]:
    return {"id": gid, "name": name or f"Tracker {gid}"}


class _World:
    """One config entry, a real coordinator and a scripted device list."""

    def __init__(
        self, hass: Any, monkeypatch: pytest.MonkeyPatch, title: str = "stale"
    ) -> None:
        from homeassistant import config_entries as ce
        from homeassistant.config_entries import ConfigSubentryData
        from homeassistant.helpers import device_registry as dr
        from homeassistant.helpers import entity_registry as er
        from homeassistant.helpers import frame

        from custom_components.googlefindmy.coordinator import main as cmain
        from custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker import (
            foreign_reading_tracker as frt,
        )
        from custom_components.googlefindmy.ProtoDecoders import decoder

        self.hass = hass
        self.monkeypatch = monkeypatch
        self.decoder = decoder
        self.entry = MockConfigEntry(
            domain=_DOMAIN,
            title=title,
            data={},
            subentries_data=[
                ConfigSubentryData(
                    data={"group_key": "service"},
                    subentry_type="service",
                    title="svc",
                    unique_id="svc",
                ),
                ConfigSubentryData(
                    data={"group_key": "core_tracking"},
                    subentry_type="tracker",
                    title="trk",
                    unique_id="trk",
                ),
            ],
        )
        self.entry.add_to_hass(hass)
        self.eid = self.entry.entry_id
        self.dev_reg = dr.async_get(hass)
        self.ent_reg = er.async_get(hass)
        self.api = AsyncMock()
        # Synchronous on the real API; an AsyncMock here leaks a coroutine.
        self.api.is_push_ready = lambda: True
        self.api.async_get_basic_device_list = AsyncMock(return_value=[])
        monkeypatch.setattr(cmain, "GoogleFindMyAPI", lambda *_a, **_k: self.api)
        monkeypatch.setattr(cmain, "async_get_clientsession", lambda _h: object())
        monkeypatch.setattr(
            cmain.GoogleFindMyCoordinator,
            "_async_load_stats",
            AsyncMock(return_value=None),
        )
        frame.async_setup(hass)
        self._cmain = cmain
        self._ce = ce
        self.purged: list[str] = []
        self.restart()
        self.forgotten: list[str] = []

        def _forget_spy(_entry_id: str | None, canonic_id: str) -> None:
            self.forgotten.append(canonic_id)

        monkeypatch.setattr(frt.FOREIGN_READING_TRACKER, "forget_device", _forget_spy)
        self.counts(total=0, benign=0, warn=0)

    def restart(self) -> None:
        """Start a new coordinator for the entry, as a reload's setup half does."""
        token = self._ce.current_entry.set(self.entry)
        try:
            self.coord = self._cmain.GoogleFindMyCoordinator(self.hass, cache=_Cache())
        finally:
            self._ce.current_entry.reset(token)
        self.coord.config_entry = self.entry
        self.coord._startup_complete = True
        self.coord.async_set_updated_data = lambda _data: None  # type: ignore[method-assign]
        self.monkeypatch.setattr(self.coord, "_async_start_poll_cycle", AsyncMock())
        original_purge = self.coord.purge_device

        def _purge_spy(device_id: str) -> None:
            self.purged.append(device_id)
            original_purge(device_id)

        self.monkeypatch.setattr(self.coord, "purge_device", _purge_spy)

    def counts(self, *, total: int, benign: int, warn: int) -> None:
        """Set the decoder's per-fetch canonicless tally for this entry."""
        self.monkeypatch.setitem(
            self.decoder._canonicless_counts_by_entry,
            self.eid,
            {"total": total, "benign": benign, "warn": warn},
        )

    async def fresh(self, devices: list[Any]) -> Any:
        """Run one update cycle that fetches the device list."""
        self.api.async_get_basic_device_list = AsyncMock(return_value=devices)
        self.coord._last_list_poll_mono = 0.0
        result = await self.coord._async_update_data()
        await self.hass.async_block_till_done()
        return result

    async def cached(self) -> Any:
        """Run one update cycle inside the list interval (cached list)."""
        self.api.async_get_basic_device_list = AsyncMock(
            side_effect=AssertionError("cached cycle must not fetch")
        )
        self.coord._last_list_poll_mono = time.monotonic()
        result = await self.coord._async_update_data()
        await self.hass.async_block_till_done()
        return result

    def seed(self, devices: list[dict[str, Any]]) -> None:
        """Put devices into the registry as an earlier run would have."""
        self.coord._ensure_service_device_exists()
        self.coord._ensure_registry_for_devices(devices, set())

    def trackers(self) -> dict[str, Any]:
        """Map Google id to tracker device for this entry."""
        from homeassistant.helpers import device_registry as dr

        out: dict[str, Any] = {}
        for device in dr.async_entries_for_config_entry(self.dev_reg, self.eid):
            if device.entry_type is not None:
                continue
            for domain, ident in device.identifiers:
                if domain == _DOMAIN:
                    out[ident.split(":")[-1]] = device
        return out

    def service_device(self) -> Any:
        from custom_components.googlefindmy.const import service_device_identifier

        identifier = service_device_identifier(self.eid)
        # Core 2026.8 and newer reject async_get_device; the declared minimum
        # (2025.9.1) has only that lookup.
        if hasattr(self.dev_reg, "async_get_device_by_identifier"):
            return self.dev_reg.async_get_device_by_identifier(identifier, self.eid)
        return self.dev_reg.async_get_device(identifiers={identifier})

    def add_entity(self, gid: str) -> str:
        """Attach one tracker entity to the device of ``gid``."""
        device = self.trackers()[gid]
        entity = self.ent_reg.async_get_or_create(
            "device_tracker",
            _DOMAIN,
            f"{self.eid}:{gid}",
            config_entry=self.entry,
            config_subentry_id=_subentry_of(device, self.eid),
            device_id=device.id,
        )
        return str(entity.entity_id)


def _subentry_of(device: Any, entry_id: str) -> str:
    """The one subentry of ``entry_id`` that ``device`` sits on, on either core.

    Core 2026.8 and newer expose ``config_subentry_id``; the declared minimum
    (2025.9.1) only has the ``config_entries_subentries`` mapping
    (tests/AGENTS.md, device registry section).
    """
    if hasattr(type(device), "config_subentry_id"):
        assert device.config_entry_id == entry_id, device.config_entry_id
        subentries = {device.config_subentry_id}
    else:
        subentries = set(device.config_entries_subentries.get(entry_id, set()))
    subentries.discard(None)
    assert len(subentries) == 1, subentries
    return str(subentries.pop())


def _belongs_to(device: Any, entry_id: str) -> bool:
    """Whether ``device`` belongs to ``entry_id``, on either core."""
    if hasattr(type(device), "config_entry_id"):
        return bool(device.config_entry_id == entry_id)
    return entry_id in device.config_entries


def _records(caplog: pytest.LogCaptureFixture, text: str) -> list[logging.LogRecord]:
    return [
        r for r in caplog.records if r.name.startswith(_PKG) and text in r.getMessage()
    ]


# ---------------------------------------------------------------------------
# Characterisation of the removal path (M-10)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_detach_from_tracker_subentry_removes_device_and_entities(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """M-10: DETACH on the owning subentry removes device and entities.

    Without ``detach_subentry_id`` DETACH names the hub link and leaves a
    tracker device untouched; the removal must name the tracker subentry.
    """
    from custom_components.googlefindmy.coordinator.helpers.registry import (
        OwnershipIntent,
    )

    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA")])
    entity_id = w.add_entity("AAAA")
    device = w.trackers()["AAAA"]

    w.coord._apply_device_ownership(
        w.dev_reg,
        intent=OwnershipIntent.DETACH,
        device_id=device.id,
        entry_id=w.eid,
    )
    await hass.async_block_till_done()
    assert w.dev_reg.async_get(device.id) is not None, "hub-link DETACH removed it"

    w.coord._apply_device_ownership(
        w.dev_reg,
        intent=OwnershipIntent.DETACH,
        device_id=device.id,
        entry_id=w.eid,
        detach_subentry_id=_subentry_of(device, w.eid),
    )
    await hass.async_block_till_done()
    assert w.dev_reg.async_get(device.id) is None
    assert w.ent_reg.async_get(entity_id) is None


# ---------------------------------------------------------------------------
# Positive cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_missing_from_two_fresh_lists_is_removed(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """S-P1: removed live, with entities, after two fresh misses."""
    caplog.set_level(logging.DEBUG)
    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA"), _dev("BBBB")])
    entity_b = w.add_entity("BBBB")
    a_before = w.trackers()["AAAA"].id

    await w.fresh([_dev("AAAA")])
    assert "BBBB" in w.trackers(), "removed after a single miss (S-R9)"
    assert _records(caplog, _PENDING), "first miss is not logged"

    await w.fresh([_dev("AAAA")])
    trackers = w.trackers()
    assert "BBBB" not in trackers
    assert w.ent_reg.async_get(entity_b) is None
    assert trackers["AAAA"].id == a_before
    assert w.purged.count("BBBB") == 1
    assert w.forgotten.count("BBBB") == 1
    removed = _records(caplog, _REMOVED)
    assert len(removed) == 1 and removed[0].levelno == logging.INFO
    assert w.service_device() is not None


@pytest.mark.asyncio
async def test_device_names_stay_at_debug(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """V-7a: S runs on the poll path; INFO carries the device id, never the name.

    AGENTS.md section 5, class (b): a record that can repeat unattended keeps
    the user-provided device name at DEBUG.
    """
    caplog.set_level(logging.DEBUG)
    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA"), _dev("BBBB", "Koffer Gruen")])
    b_id = w.trackers()["BBBB"].id
    caplog.clear()

    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert "BBBB" not in w.trackers()

    ours = [r for r in caplog.records if r.name.startswith(_PKG)]
    loud = [r.getMessage() for r in ours if r.levelno >= logging.INFO]
    assert not [m for m in loud if "Koffer Gruen" in m], loud
    pending = _records(caplog, _PENDING)
    removed = _records(caplog, _REMOVED)
    assert [r.levelno for r in removed] == [logging.INFO]
    assert b_id in removed[0].getMessage()
    assert any(r.levelno == logging.INFO and b_id in r.getMessage() for r in pending)
    quiet = [r.getMessage() for r in ours if r.levelno == logging.DEBUG]
    assert any("Koffer Gruen" in m and b_id in m and _PENDING in m for m in quiet)
    assert any("Koffer Gruen" in m and "Removed stale device" in m for m in quiet)


@pytest.mark.asyncio
async def test_device_gone_after_first_detach_stops_the_loop(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S-P3: a device that vanished after its first link was detached is left alone.

    Core 2025.9, the oldest supported release, lets a device sit on several
    links of one entry; Core 2026.10 refuses that setup, so the second link
    is reported by the link lookup only.
    """
    from custom_components.googlefindmy.coordinator import registry as reg_mod

    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA"), _dev("BBBB")])
    device = w.trackers()["BBBB"]
    real_links = reg_mod._extract_subentry_links_impl

    def _two_links(dev: Any, entry_id: str) -> set[str | None]:
        links = set(real_links(dev, entry_id))
        if dev.id == device.id:
            links.add("zz-second-link")  # sorts after the real ULID link
        return links

    monkeypatch.setattr(reg_mod, "_extract_subentry_links_impl", _two_links)
    detached: list[str | None] = []
    original = w.coord._apply_device_ownership

    def _spy(*args: Any, **kwargs: Any) -> Any:
        detached.append(kwargs.get("detach_subentry_id"))
        return original(*args, **kwargs)

    monkeypatch.setattr(w.coord, "_apply_device_ownership", _spy)

    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert w.dev_reg.async_get(device.id) is None
    assert detached == [_subentry_of(device, w.eid)]
    assert w.purged.count("BBBB") == 1


def test_plan_skips_devices_it_cannot_attribute(gfmy: ModuleType) -> None:
    """S-R5/S-R8 at the helper: unreadable, foreign or unlinked devices stay."""
    from custom_components.googlefindmy.coordinator.helpers.registry import (
        plan_stale_device_removals,
    )

    def device(dev_id: Any, identifiers: Any, links: Any) -> SimpleNamespace:
        return SimpleNamespace(
            id=dev_id,
            entry_type=None,
            identifiers=identifiers,
            config_entries={"e1"} if links else set(),
            config_entries_subentries={"e1": links} if links else {},
            config_entry_id="e1" if links else None,
        )

    devices = [
        device(None, {(_DOMAIN, "e1:XXXX")}, {"sub"}),  # no device id
        device("d-shape", {("bad",), (_DOMAIN, 7)}, {"sub"}),  # malformed ids
        device("d-foreign", {("other", "e1:XXXX")}, {"sub"}),  # foreign domain
        device("d-unlinked", {(_DOMAIN, "e1:XXXX")}, None),  # no link to e1
        device("d-gone", {(_DOMAIN, "e1:sub:XXXX")}, {"sub"}),  # counted
    ]
    common: dict[str, Any] = {
        "domain": _DOMAIN,
        "service_identifiers": (),
        "dropped_candidates": 0,
        "canonicless_warn": 0,
        "canonicless_names": {},
        "previous_misses": {},
    }
    plan = plan_stale_device_removals(
        devices, entry_id="e1", fresh_list_ids={"AAAA"}, **common
    )
    assert plan.skipped_reason is None
    assert [d.id for d, _ in plan.first_misses] == ["d-gone"]
    assert plan.misses == {"d-gone": 1}

    for entry_id, listed in ((None, {"AAAA"}), ("e1", set())):
        empty = plan_stale_device_removals(
            devices,
            entry_id=entry_id,
            fresh_list_ids=listed,
            **{**common, "previous_misses": {"d-gone": 1}},
        )
        assert empty.skipped_reason == "no_fresh_list"
        assert empty.misses == {"d-gone": 1}
        assert not empty.remove


def test_typing_stubs_never_reach_the_coordinator(gfmy: ModuleType) -> None:
    """The ``_MixinBase`` stubs S added are shadowed by the real methods.

    The stubs only declare the mixin contract for ``mypy``; at runtime the
    coordinator must resolve both names to their implementations.
    """
    from custom_components.googlefindmy.coordinator._mixin_typing import _MixinBase
    from custom_components.googlefindmy.coordinator.main import (
        GoogleFindMyCoordinator,
    )
    from custom_components.googlefindmy.coordinator.registry import (
        RegistryOperations,
    )

    coordinator = GoogleFindMyCoordinator
    for name, owner in (
        ("_remove_stale_devices", RegistryOperations),
        ("purge_device", GoogleFindMyCoordinator),
    ):
        resolved = getattr(coordinator, name)
        assert resolved is not getattr(_MixinBase, name), name
        assert resolved is owner.__dict__[name], name


def test_removal_without_config_entry_is_a_no_op(gfmy: ModuleType) -> None:
    """S-R7 guard: without a bound entry nothing is planned or removed."""
    from custom_components.googlefindmy.coordinator.registry import (
        RegistryOperations,
    )

    holder = SimpleNamespace(config_entry=None, entry=None)
    removed = RegistryOperations._remove_stale_devices(
        holder,  # type: ignore[arg-type]
        {"AAAA"},
        dropped_candidates=0,
        canonicless_warn=0,
    )
    assert removed == 0


@pytest.mark.asyncio
async def test_device_gone_while_stopped_is_removed_after_setup(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-P2: the registry holds B from an earlier run; the account no longer does."""
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB")])
    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert set(w.trackers()) == {"AAAA"}


# ---------------------------------------------------------------------------
# Protection rules
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_lists_never_remove(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R1: deferred empty list and accepted empty list both keep everything."""
    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA"), _dev("BBBB")])
    removed: list[Any] = []
    hass.bus.async_listen(
        "device_registry_updated",
        lambda e: removed.append(e) if e.data.get("action") == "remove" else None,
    )
    await w.fresh([])  # streak 1: deferred, old list reused
    await w.fresh([])  # quorum met: empty list accepted
    await w.fresh([])
    assert set(w.trackers()) == {"AAAA", "BBBB"}
    assert removed == []


@pytest.mark.asyncio
async def test_deferred_empty_list_is_no_confirmation(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R1b: a deferred empty list reuses the old list and is not a second miss."""
    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA"), _dev("BBBB")])
    await w.fresh([_dev("AAAA")])  # B: first miss
    await w.fresh([])  # deferred: the old list [A] is reused, nothing was fetched
    assert "BBBB" in w.trackers(), "a deferred list counted as a fresh miss"


@pytest.mark.asyncio
async def test_cached_cycles_never_count(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R2: a device absent from the cached list is not removed by cache cycles."""
    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA")])
    w.seed([_dev("CCCC")])
    for _ in range(3):
        await w.cached()
    assert "CCCC" in w.trackers()


@pytest.mark.asyncio
async def test_dropped_candidates_block_removal(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """S-R3a: a candidate without valid id makes the list incomplete."""
    caplog.set_level(logging.DEBUG)
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB")])
    for _ in range(3):
        await w.fresh([_dev("AAAA"), {"name": "x"}])
    assert "BBBB" in w.trackers()
    assert any(
        "reason=dropped_candidates" in r.getMessage()
        for r in _records(caplog, _SKIPPED)
    )


@pytest.mark.asyncio
async def test_canonicless_warn_blocks_removal(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """S-R3b: a tracker-like canonicless drop blocks removal."""
    caplog.set_level(logging.DEBUG)
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB")])
    w.counts(total=1, benign=0, warn=1)
    for _ in range(3):
        await w.fresh([_dev("AAAA")])
    assert "BBBB" in w.trackers()
    assert any(
        "reason=canonicless_warn" in r.getMessage() for r in _records(caplog, _SKIPPED)
    )


@pytest.mark.asyncio
async def test_benign_canonicless_drop_does_not_block(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R3c: the live form of both accounts (total 1, benign 1) must not block."""
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB")])
    w.counts(total=1, benign=1, warn=0)
    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert "BBBB" not in w.trackers()


@pytest.mark.asyncio
async def test_service_device_is_never_removed(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R4: the service device is not a tracker and stays."""
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB")])
    service_id = w.service_device().id
    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert "BBBB" not in w.trackers()
    assert w.dev_reg.async_get(service_id) is not None


@pytest.mark.asyncio
async def test_each_entry_checks_its_own_list(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R5: the same tracker in two accounts is two devices, judged apart."""
    w1 = _World(hass, monkeypatch, title="one")
    w2 = _World(hass, monkeypatch, title="two")
    w1.seed([_dev("AAAA"), _dev("KKKK", "Koffer")])
    w2.seed([_dev("KKKK", "Koffer")])
    k2_before = w2.trackers()["KKKK"].id
    # Watch removals: the next fetch of account two would re-create a wrongly
    # removed device under its old id (core restores it from the trash), so the
    # id at the end proves nothing.
    removed: list[str] = []
    hass.bus.async_listen(
        "device_registry_updated",
        lambda e: (
            removed.append(e.data["device_id"])
            if e.data.get("action") == "remove"
            else None
        ),
    )
    for _ in range(2):
        await w1.fresh([_dev("AAAA")])
        await w2.fresh([_dev("KKKK", "Koffer")])
    assert "KKKK" not in w1.trackers()
    assert k2_before not in removed, "account one removed the device of account two"
    assert w2.trackers()["KKKK"].id == k2_before


@pytest.mark.asyncio
async def test_removal_does_not_ignore_the_device(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R6: a device added back to the account comes back to Home Assistant."""
    from custom_components.googlefindmy.const import OPT_IGNORED_DEVICES

    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("DDDD")])
    old_id = w.trackers()["DDDD"].id
    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert "DDDD" not in w.trackers()
    assert "DDDD" not in (w.entry.options.get(OPT_IGNORED_DEVICES) or {})

    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    back = w.trackers().get("DDDD")
    assert back is not None
    assert _belongs_to(back, w.eid)
    # Recorded, not asserted: core restores the old device id from its trash.
    logging.getLogger(__name__).info("restored same device id: %s", back.id == old_id)


_FOREIGN_ERRORS = ("RuntimeError", "ValueError", "KeyError", "HomeAssistantError")


def _foreign_error(name: str, text: str) -> Exception:
    from homeassistant.exceptions import HomeAssistantError

    classes: dict[str, type[Exception]] = {
        "RuntimeError": RuntimeError,
        "ValueError": ValueError,
        "KeyError": KeyError,
        "HomeAssistantError": HomeAssistantError,
    }
    return classes[name](text)


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["detach", "planner"])
@pytest.mark.parametrize("error", _FOREIGN_ERRORS)
async def test_removal_error_never_fails_the_cycle(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    error: str,
    source: str,
) -> None:
    """S-R7 and B3: a failing removal never fails the cycle or leaks its text.

    AGENTS.md section 5 forbids e-mail addresses in logs at every level, so no
    record of any level may carry the text of a foreign exception; the class
    name is enough to tell the failures apart (review findings F4, R2-2). The
    error comes from one device's removal (caught per device) or from the
    decision itself (caught by the update cycle).
    """
    from custom_components.googlefindmy.coordinator import registry as coord_reg
    from custom_components.googlefindmy.coordinator.helpers.registry import (
        OwnershipIntent,
    )

    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB")])
    original = w.coord._apply_device_ownership
    original_plan = coord_reg.plan_stale_device_removals

    def _failing(dev_reg: Any, **kwargs: Any) -> Any:
        if kwargs.get("intent") is OwnershipIntent.DETACH:
            raise _foreign_error(error, "owner someone@example.com")
        return original(dev_reg, **kwargs)

    calls = {"plan": 0}

    def _failing_plan(*args: Any, **kwargs: Any) -> Any:
        calls["plan"] += 1
        if calls["plan"] > 1:
            raise _foreign_error(error, "owner someone@example.com")
        return original_plan(*args, **kwargs)

    if source == "detach":
        monkeypatch.setattr(w.coord, "_apply_device_ownership", _failing)
    else:
        monkeypatch.setattr(coord_reg, "plan_stale_device_removals", _failing_plan)
    await w.fresh([_dev("AAAA")])
    caplog.clear()
    caplog.set_level(logging.DEBUG)
    result = await w.fresh([_dev("AAAA")])
    assert isinstance(result, list)
    assert "BBBB" in w.trackers()
    ours = [r for r in caplog.records if r.name.startswith(_PKG)]
    assert any(error in r.getMessage() for r in ours if r.levelno == logging.DEBUG)
    warnings = [r for r in ours if r.levelno == logging.WARNING]
    assert len(warnings) == 1, [r.getMessage() for r in warnings]
    assert error in warnings[0].getMessage()
    leaks = [r.getMessage() for r in ours if "someone@example.com" in r.getMessage()]
    assert leaks == [], f"exception text reached the log: {leaks}"


@pytest.mark.asyncio
async def test_device_with_two_google_ids_needs_both_missing(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R8 (V-6 c'): one listed id keeps the device; none listed removes it.

    Two trackers of the same name are merged into one device (M-13), which
    gives that device the ids of both.
    """
    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA", "Tag")])
    await w.fresh([_dev("AAAA", "Tag"), _dev("ZZZZ", "Tag")])
    merged = w.trackers()
    assert merged["AAAA"].id == merged["ZZZZ"].id, "precondition (M-13) not met"

    # Either id alone keeps the device, whichever one a reader would see first.
    for listed in ("AAAA", "ZZZZ"):
        for _ in range(3):
            await w.fresh([_dev(listed, "Tag")])
        assert "AAAA" in w.trackers(), f"removed although {listed} is listed"

    await w.fresh([_dev("QQQQ")])
    await w.fresh([_dev("QQQQ")])
    assert "AAAA" not in w.trackers()
    assert "ZZZZ" not in w.trackers()


@pytest.mark.asyncio
async def test_device_without_readable_id_is_never_removed(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R8: no Google id can be read, so nothing proves the device gone."""
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA")])
    tracker_sub = _subentry_of(w.trackers()["AAAA"], w.eid)
    odd = w.dev_reg.async_get_or_create(
        config_entry_id=w.eid,
        config_subentry_id=tracker_sub,
        identifiers={(_DOMAIN, f"{w.eid}:")},
        name="odd",
    )
    for _ in range(3):
        await w.fresh([_dev("AAAA")])
    assert w.dev_reg.async_get(odd.id) is not None


@pytest.mark.asyncio
async def test_two_consecutive_fresh_misses_are_required(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S-R9: a miss followed by a hit resets; cached cycles do not count."""
    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA"), _dev("BBBB")])
    await w.fresh([_dev("AAAA")])
    await w.cached()
    await w.fresh([_dev("AAAA"), _dev("BBBB")])
    await w.fresh([_dev("AAAA")])
    await w.cached()
    assert "BBBB" in w.trackers(), "miss counter not reset by the hit"
    await w.fresh([_dev("AAAA")])
    assert "BBBB" not in w.trackers()


# ---------------------------------------------------------------------------
# Coupling with unload (T9)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_device_deleted_in_the_account_leaves_after_a_reload(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T9: a reload keeps the remaining devices, S removes the deleted one.

    Unload no longer purges the registry, so a device deleted in the Google
    account while Home Assistant reloads must still go. The new coordinator
    starts with an empty miss counter and removes it after its own two fresh
    lists; the device that stays keeps its ids and user settings.
    """
    from homeassistant.helpers import area_registry as ar

    w = _World(hass, monkeypatch)
    await w.fresh([_dev("AAAA"), _dev("BBBB")])
    entity_a = w.add_entity("AAAA")
    entity_b = w.add_entity("BBBB")
    area = ar.async_get(hass).async_get_or_create("Hall")
    w.ent_reg.async_update_entity(entity_a, name="Keys", area_id=area.id)
    device_a = w.trackers()["AAAA"]
    w.dev_reg.async_update_device(device_a.id, name_by_user="My keys")
    entity_a_id = w.ent_reg.async_get(entity_a).id

    # Reload: the real parent unload, then the setup half with a new coordinator.
    monkeypatch.setattr(
        hass.config_entries, "async_unload_platforms", AsyncMock(return_value=True)
    )
    assert await gfmy._async_unload_parent_entry(hass, w.entry) is True
    w.restart()
    await w.fresh([_dev("AAAA")])
    w.add_entity("AAAA")  # the platform adds the entity of the listed device
    await w.fresh([_dev("AAAA")])

    trackers = w.trackers()
    assert "BBBB" not in trackers, "device deleted in the account survived a reload"
    assert w.ent_reg.async_get(entity_b) is None
    assert trackers["AAAA"].id == device_a.id
    assert trackers["AAAA"].name_by_user == "My keys"
    kept = w.ent_reg.async_get(entity_a)
    assert kept is not None
    assert kept.id == entity_a_id
    assert kept.name == "Keys"
    assert kept.area_id == area.id


# ---------------------------------------------------------------------------
# Review round AP8: F1 (return reloads once), F5 (refused removal), F8 (S-R3d)
# ---------------------------------------------------------------------------

_RETURNED = "are back in the Google account"


def _reload_calls(hass: Any, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record scheduled reloads instead of running them."""
    calls: list[str] = []
    monkeypatch.setattr(hass.config_entries, "async_schedule_reload", calls.append)
    return calls


async def _remove_dddd(w: _World) -> None:
    w.seed([_dev("AAAA"), _dev("DDDD")])
    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert "DDDD" not in w.trackers(), "precondition: DDDD not removed"


@pytest.mark.asyncio
async def test_returned_device_reloads_the_entry_once(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F1 (V-8 b): the first fresh list with a removed id reloads, once.

    The platforms keep the ids they have set up for as long as their setup
    lives (``tests/test_stale_device_return_platform.py``), so only a reload
    brings the entities of a returned tracker back.
    """
    caplog.set_level(logging.DEBUG)
    calls = _reload_calls(hass, monkeypatch)
    w = _World(hass, monkeypatch)
    await _remove_dddd(w)
    assert calls == [], "a removal alone must not reload"
    await w.fresh([_dev("AAAA")])
    assert calls == [], "a list without the removed id must not reload"

    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert calls == [w.eid]
    assert "DDDD" in w.trackers(), "the cycle did not register the device again"
    announced = _records(caplog, _RETURNED)
    assert [r.levelno for r in announced] == [logging.INFO]
    assert "Tracker DDDD" not in announced[0].getMessage()
    assert gfmy.claim_pending_entry_reload(hass, w.eid) is False, (
        "the reload was scheduled without the shared latch"
    )

    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert calls == [w.eid], "a second reload for the same return"


@pytest.mark.asyncio
async def test_return_while_a_reload_is_pending_stands_down(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F1: a reload already on its way rebuilds the platforms just the same."""
    calls = _reload_calls(hass, monkeypatch)
    w = _World(hass, monkeypatch)
    await _remove_dddd(w)
    assert gfmy.claim_pending_entry_reload(hass, w.eid) is True
    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert calls == []
    # B6 (R2-4): the remembered ids stay. If the pending reload lands, the
    # coordinator is replaced and the set with it; if it never lands, the next
    # fresh list tries again, and the latch keeps that from doubling a reload.
    assert w.coord._stale_removed_google_ids == {"DDDD"}
    gfmy.discard_pending_entry_reload(hass, w.eid)
    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert calls == [w.eid], "the return was forgotten with the failed claim"


@pytest.mark.asyncio
async def test_return_to_an_entry_that_cannot_reload_waits(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F1: no promise to reload where none can land; a later list reloads."""
    from custom_components.googlefindmy import entry_reload_gate

    calls = _reload_calls(hass, monkeypatch)
    w = _World(hass, monkeypatch)
    await _remove_dddd(w)
    monkeypatch.setattr(
        entry_reload_gate, "entry_reload_is_hopeless", lambda *_a, **_k: True
    )
    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert calls == []
    assert w.coord._stale_removed_google_ids == {"DDDD"}
    assert gfmy.claim_pending_entry_reload(hass, w.eid) is True, "latch was taken"
    gfmy.discard_pending_entry_reload(hass, w.eid)

    monkeypatch.setattr(
        entry_reload_gate, "entry_reload_is_hopeless", lambda *_a, **_k: False
    )
    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert calls == [w.eid]


@pytest.mark.asyncio
async def test_failed_reload_schedule_hands_the_latch_back(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F1: a schedule that fails releases the latch and keeps the ids."""
    w = _World(hass, monkeypatch)
    await _remove_dddd(w)

    def _boom(_eid: str) -> None:
        raise RuntimeError("someone@example.com")

    monkeypatch.setattr(hass.config_entries, "async_schedule_reload", _boom)
    caplog.set_level(logging.DEBUG)
    result = await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert isinstance(result, list), "the update cycle failed"
    assert w.coord._stale_removed_google_ids == {"DDDD"}
    assert gfmy.claim_pending_entry_reload(hass, w.eid) is True, "latch kept"
    warnings = [
        r
        for r in caplog.records
        if r.name.startswith(_PKG) and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "RuntimeError" in warnings[0].getMessage()
    assert "someone@example.com" not in warnings[0].getMessage()


@pytest.mark.asyncio
async def test_device_never_removed_does_not_reload(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F1: only ids S removed itself reload; a missed-once device does not."""
    calls = _reload_calls(hass, monkeypatch)
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("DDDD")])
    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA"), _dev("DDDD")])
    assert calls == []
    assert "DDDD" in w.trackers()


@pytest.mark.asyncio
async def test_refused_removal_is_not_reported_and_is_retried(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """F5: a DETACH that core refuses removes nothing, so nothing is reported.

    ``_apply_device_ownership`` logs the refusal as an error and returns; the
    device stays. Neither the INFO ``Removed`` line nor the purge nor the
    remembered id may claim otherwise, and the next fresh list tries again.
    """
    from custom_components.googlefindmy.coordinator.helpers.registry import (
        OwnershipIntent,
    )

    calls = _reload_calls(hass, monkeypatch)
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("DDDD")])
    original = w.coord._apply_device_ownership

    def _refusing(dev_reg: Any, **kwargs: Any) -> Any:
        if kwargs.get("intent") is OwnershipIntent.DETACH:
            return kwargs.get("device")
        return original(dev_reg, **kwargs)

    monkeypatch.setattr(w.coord, "_apply_device_ownership", _refusing)
    caplog.set_level(logging.DEBUG)
    await w.fresh([_dev("AAAA")])
    await w.fresh([_dev("AAAA")])
    assert "DDDD" in w.trackers()
    assert _records(caplog, _REMOVED) == []
    assert w.purged == []
    assert w.forgotten == []
    assert w.coord._stale_removed_google_ids == set()

    monkeypatch.setattr(w.coord, "_apply_device_ownership", original)
    await w.fresh([_dev("AAAA")])
    assert "DDDD" not in w.trackers(), "the refused removal was not retried"
    assert len(_records(caplog, _REMOVED)) == 1
    assert w.coord._stale_removed_google_ids == {"DDDD"}
    assert calls == []


def test_name_matching_helpers(gfmy: ModuleType) -> None:
    """B11: the arms of the name folding and of the registry name keys."""
    from custom_components.googlefindmy.coordinator.helpers import registry as hr

    kind = "no_information"
    raw: dict[Any, Any] = {
        (" A ", kind): 1,
        ("a", kind): 2,
        ("a", "android"): 1,
        ("", kind): 1,
        ("  ", kind): 1,
        ("B", kind): 0,
        ("C", kind): "x",
        "D": 1,
        ("E", ""): 1,
        ("F", None): 1,
        ("G", kind): True,
    }
    assert hr._normalized_name_counts(raw) == {("a", kind): 3, ("a", "android"): 1}
    assert hr._registry_name_keys(SimpleNamespace(name=None)) == frozenset()
    assert hr._registry_name_keys(SimpleNamespace(name="Tag (red)")) == {
        "tag (red)",
        "tag",
    }
    assert hr._registry_name_keys(SimpleNamespace(name=" (abc)")) == {"(abc)"}


def _canonicless(w: _World, names: list[str] | list[str | tuple[str, str]]) -> None:
    """Set the drops the decoder recorded for the next fetch, with repeats.

    A benign drop per name keeps S-R3b out of the way; the names are the only
    trace S-R3d has of a device listed without canonical id. A drop is a
    ``(name, kind)`` pair (R3-5); a bare name is an accessory without an
    ``information`` block.
    """
    from collections import Counter

    keys = [n if isinstance(n, tuple) else (n, "no_information") for n in names]
    w.counts(total=len(names), benign=len(names), warn=0)
    # No getattr default: a decoder without the table fails here (R4-3).
    counts = w.decoder._canonicless_name_counts_by_entry
    w.monkeypatch.setitem(counts, w.eid, dict(Counter(keys)))


async def _lists(
    w: _World,
    devices: list[dict[str, Any]],
    names: list[str] | list[str | tuple[str, str]],
    n: int,
) -> None:
    for _ in range(n):
        _canonicless(w, names)
        await w.fresh(devices)


# B1 (R2-1, R2-7): the name forms a Google name and the registry name can take.
_NAME_FORMS = ("same", "edge_spaces", "case", "renamed_in_ha_then_google", "hub_suffix")


@pytest.mark.asyncio
@pytest.mark.parametrize("form", _NAME_FORMS)
async def test_device_dropped_without_canonical_id_is_never_removed(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch, form: str
) -> None:
    """B1 (S-R3d): a tracker Google lists without canonical id is still there.

    It drops as ``benign`` (a phone, or an accessory without an
    ``information`` block), so S-R3b does not see it; its name is the trace.
    The name must match in every form it can take between Google and the
    registry: Google's raw text with edge spaces or another case, a user
    name in Home Assistant followed by a rename in the Google app (the
    registry name follows Google, as core keeps it, ``name_by_user`` stays),
    and the hub collision suffix. The other missing device is removed.
    """
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB", "Phone"), _dev("CCCC")])
    phone = w.trackers()["BBBB"]
    dropped = "Phone"
    if form == "edge_spaces":
        dropped = " Phone "
    elif form == "case":
        dropped = "PHONE"
    elif form == "renamed_in_ha_then_google":
        w.dev_reg.async_update_device(phone.id, name_by_user="Mein Telefon")
        await _lists(w, [_dev("AAAA"), _dev("BBBB", "Phone 2"), _dev("CCCC")], [], 1)
        assert w.trackers()["BBBB"].name_by_user == "Mein Telefon"
        dropped = "Phone 2"
    elif form == "hub_suffix":
        w.dev_reg.async_update_device(phone.id, name="Phone (a1b2c3)")

    await _lists(w, [_dev("AAAA")], [dropped], 3)
    trackers = w.trackers()
    assert "BBBB" in trackers, "removed although Google still lists it"
    assert "CCCC" not in trackers, "S-R3d blocked an unrelated removal"
    assert "BBBB" not in w.purged


# B2 (R2-6): a shared name protects only while it explains a new drop.
_SHARED_NAME = {
    # scenario: (lists as (listed ids, dropped names, repeats), BBBB kept?)
    "phone_already_dropped_tracker_leaves": (
        [(["AAAA", "BBBB"], ["Pixel"], 1), (["AAAA"], ["Pixel"], 2)],
        False,
    ),
    "tracker_drops_next_to_dropped_phone": (
        [(["AAAA", "BBBB"], ["Pixel"], 1), (["AAAA"], ["Pixel", "Pixel"], 3)],
        True,
    ),
    "tracker_drops_then_leaves": (
        [(["AAAA", "BBBB"], [], 1), (["AAAA"], ["Pixel"], 3), (["AAAA"], [], 2)],
        False,
    ),
    "restart_without_baseline": (
        [
            (["AAAA", "BBBB"], ["Pixel"], 1),
            ("restart", [], 0),
            (["AAAA"], ["Pixel"], 3),
        ],
        True,
    ),
    "two_new_drops": (
        [(["AAAA", "BBBB"], [], 1), (["AAAA"], ["Pixel", "Pixel"], 3)],
        True,
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", sorted(_SHARED_NAME))
async def test_shared_name_protects_only_a_new_drop(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch, scenario: str
) -> None:
    """B2 (R2-6): a phone of the same name does not keep a tracker forever.

    S-R3d protects a device only when the number of drops under its name has
    grown since the last counting list that still carried the device with
    its id. A phone ``Pixel`` that was already dropped then explains its own
    drop, and the tracker ``Pixel`` that left the account is removed. After a
    restart nothing is known about earlier lists, so the name alone protects.
    """
    lists, kept = _SHARED_NAME[scenario]
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB", "Pixel")])
    for listed, names, repeats in lists:
        if listed == "restart":
            w.restart()
            continue
        devices = [_dev(gid, "Pixel" if gid == "BBBB" else None) for gid in listed]
        await _lists(w, devices, names, repeats)
    assert ("BBBB" in w.trackers()) is kept


# B13 (R3-5 rim B): drops count per name and class, so a role swap shows.
_PHONE = ("Pixel", "android")
_TAG = ("Pixel", "no_information")
_ROLE_SWAP: dict[str, tuple[list[Any], bool]] = {
    # scenario: (lists as (listed ids, drops, repeats), BBBB kept?)
    "swap_phone_gets_id_tracker_loses_it": (
        [(["AAAA", "BBBB"], [_PHONE], 1), (["AAAA", "PPPP"], [_TAG], 3)],
        True,
    ),
    "phone_stays_dropped_tracker_leaves": (
        [(["AAAA", "BBBB"], [_PHONE], 1), (["AAAA"], [_PHONE], 2)],
        False,
    ),
    "swap_then_tracker_leaves": (
        [
            (["AAAA", "BBBB"], [_PHONE], 1),
            (["AAAA", "PPPP"], [_TAG], 1),
            (["AAAA", "PPPP"], [], 2),
        ],
        False,
    ),
    "restart_without_baseline": (
        [
            (["AAAA", "BBBB"], [_PHONE], 1),
            ("restart", [], 0),
            (["AAAA", "PPPP"], [_TAG], 3),
        ],
        True,
    ),
    # Declared limit: two devices of one name AND one class cannot be told
    # apart; giving them different names in the Find Hub app is the remedy.
    "same_class_swap_is_the_declared_limit": (
        [(["AAAA", "BBBB"], [_TAG], 1), (["AAAA", "PPPP"], [_TAG], 2)],
        False,
    ),
}


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", sorted(_ROLE_SWAP))
async def test_role_swap_of_two_same_named_devices_keeps_the_tracker(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch, scenario: str
) -> None:
    """B13 (R3-5 rim B): a tracker that only lost its id is not removed.

    A phone ``Pixel`` is listed without canonical id while the tracker
    ``Pixel`` is listed with one. Then, in one list, the phone gets its id
    (``PPPP``) and the tracker loses its own. Counted by name alone the drops
    stay at one, so the tracker looked gone. Counted per name and class the
    drop moved from ``android`` to ``no_information``: a new drop, the
    tracker is still in the account. A phone that stays dropped still lets a
    tracker that left go (R2-6).
    """
    lists, kept = _ROLE_SWAP[scenario]
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("BBBB", "Pixel")])
    for listed, drops, repeats in lists:
        if listed == "restart":
            w.restart()
            continue
        devices = [
            _dev(gid, "Pixel" if gid in ("BBBB", "PPPP") else None) for gid in listed
        ]
        await _lists(w, devices, drops, repeats)
    assert ("BBBB" in w.trackers()) is kept


def _detach_spy(w: _World, behaviour: Any) -> list[str]:
    """Count DETACH calls and let ``behaviour`` decide what each one does."""
    from custom_components.googlefindmy.coordinator.helpers.registry import (
        OwnershipIntent,
    )

    calls: list[str] = []
    original = w.coord._apply_device_ownership

    def _spy(dev_reg: Any, **kwargs: Any) -> Any:
        if kwargs.get("intent") is OwnershipIntent.DETACH:
            calls.append(kwargs["device_id"])
            return behaviour(original, dev_reg, kwargs)
        return original(dev_reg, **kwargs)

    w.monkeypatch.setattr(w.coord, "_apply_device_ownership", _spy)
    return calls


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["gone", "foreign_link_only", "refused"])
async def test_removed_means_gone_or_no_link_to_this_entry(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch, outcome: str
) -> None:
    """B4 (R2-3): a device counts as removed when no link to this entry is left.

    On a core that lets a device belong to several config entries, a DETACH
    can take this entry's link while another entry keeps the device. Core
    drops this entry's entities then, so the device must be purged and
    remembered like a deleted one, or its return would never reload.
    """
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("DDDD")])
    target = w.trackers()["DDDD"]

    def _behaviour(original: Any, dev_reg: Any, kwargs: dict[str, Any]) -> Any:
        if outcome == "gone":
            return original(dev_reg, **kwargs)
        if outcome == "refused":
            return kwargs.get("device")
        # A core below 2026.8 keeps the device for another config entry: the
        # pre-2026.8 shape (no ``config_entry_id``, only the mapping) without a
        # link to this entry. Core 2026.9 cannot hold that state any more.
        held = SimpleNamespace(
            id=target.id,
            identifiers=target.identifiers,
            name=target.name,
            name_by_user=None,
            config_entries={"other-entry"},
            config_entries_subentries={"other-entry": {None}},
        )
        real_get = dev_reg.async_get
        w.monkeypatch.setattr(
            dev_reg,
            "async_get",
            lambda dev_id: held if dev_id == target.id else real_get(dev_id),
        )
        return held

    _detach_spy(w, _behaviour)
    await _lists(w, [_dev("AAAA")], [], 2)
    removed = outcome != "refused"
    assert ("DDDD" in w.purged) is removed
    assert ("DDDD" in w.coord._stale_removed_google_ids) is removed


@pytest.mark.asyncio
async def test_refused_removal_is_retried_once_then_given_up(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """B5 (R2-3): a refusal is retried once, then S gives the device up.

    ``_apply_device_ownership`` logs a DETACH it refuses as an error, so an
    unbounded retry could add one ERROR per device list for as long as the
    setup lives. After the
    second refusal S stops trying until the next setup and says so once.
    """
    w = _World(hass, monkeypatch)
    w.seed([_dev("AAAA"), _dev("DDDD")])
    calls = _detach_spy(w, lambda _o, _r, kwargs: kwargs.get("device"))
    caplog.set_level(logging.DEBUG)
    await _lists(w, [_dev("AAAA")], [], 6)
    target = w.trackers()["DDDD"].id
    assert calls.count(target) == 2, calls
    gave_up = [
        r
        for r in caplog.records
        if r.name.startswith(_PKG) and "Giving up" in r.getMessage()
    ]
    assert [r.levelno for r in gave_up] == [logging.WARNING]
    assert target in gave_up[0].getMessage()
    assert "Tracker DDDD" not in gave_up[0].getMessage()

    w.restart()
    calls_after_setup = _detach_spy(w, lambda _o, _r, kwargs: kwargs.get("device"))
    await _lists(w, [_dev("AAAA")], [], 2)
    assert calls_after_setup.count(target) == 1, "a new setup must try again"


@pytest.mark.asyncio
@pytest.mark.parametrize("failing", [0, 1, 2], ids=["first", "middle", "last"])
async def test_error_on_one_device_keeps_the_others_reported(
    hass: Any,
    gfmy: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failing: int,
) -> None:
    """B7 (R2-5): one failing removal costs neither the report nor the count.

    The devices removed in the same pass are still reported at INFO, and the
    failing one is retried with the next fresh list instead of starting over
    at ``missing (1/2)``.
    """
    w = _World(hass, monkeypatch)
    gids = ["BBBB", "CCCC", "DDDD"]
    w.seed([_dev("AAAA")] + [_dev(g) for g in gids])
    order = sorted(w.trackers()[g].id for g in gids)
    bad = order[failing]

    def _behaviour(original: Any, dev_reg: Any, kwargs: dict[str, Any]) -> Any:
        if kwargs["device_id"] == bad:
            raise RuntimeError("owner someone@example.com")
        return original(dev_reg, **kwargs)

    _detach_spy(w, _behaviour)
    caplog.set_level(logging.DEBUG)
    result = None
    for _ in range(2):
        _canonicless(w, [])
        result = await w.fresh([_dev("AAAA")])
    assert isinstance(result, list)
    removed = _records(caplog, _REMOVED)
    assert len(removed) == 1, [r.getMessage() for r in removed]
    message = removed[0].getMessage()
    assert "2 device(s)" in message
    assert all(dev_id in message for dev_id in order if dev_id != bad)
    assert bad not in message
    assert not any("someone@example.com" in r.getMessage() for r in caplog.records)

    # The failing device is retried with the next list, not counted from 1.
    caplog.clear()
    _canonicless(w, [])
    await w.fresh([_dev("AAAA")])
    restarted = [r for r in _records(caplog, _PENDING) if bad in r.getMessage()]
    assert restarted == [], "the failing device starts over at missing (1/2)"
