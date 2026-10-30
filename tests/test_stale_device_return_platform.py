# tests/test_stale_device_return_platform.py
"""Why a tracker that comes back needs a reload (review finding F1, V-8 (b)).

The stale-device removal takes a tracker device out of Home Assistant once it
left the Google account. When it is added back, the update cycle registers
the device again, but the tracker platform does not set up its entities: the
platform keeps the ids it has set up in ``known_ids`` for as long as its setup
lives, and a known id coming back through the coordinator listener adds
nothing. A second setup of the platform, which is what a reload runs, starts
with an empty set and adds them.

The first test pins that platform behaviour on the real tracker platform; the
coordinator side (the reload is scheduled exactly once when a removed id comes
back) is covered in ``tests/test_stale_device_removal.py``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest
from homeassistant.config_entries import ConfigSubentry

from custom_components.googlefindmy import device_tracker
from tests.test_device_tracker_subentry_setup import (
    _ConfigEntryStub,
    _make_add_entities,
    _make_hass,
)

_DEVICE = {"id": "device-1", "name": "Device"}


def _notify(coordinator: Any) -> None:
    """Run every coordinator listener, as an update cycle does."""
    for listener in list(coordinator._listeners):
        listener()


@pytest.mark.asyncio
async def test_returned_tracker_gets_entities_only_from_a_new_setup(
    stub_coordinator_factory: Any,
) -> None:
    """F1: live, a returned id adds nothing; a new platform setup adds it."""
    loop = asyncio.get_running_loop()
    hass = _make_hass(loop)
    entry = _ConfigEntryStub()
    tracker_subentry = ConfigSubentry(
        data={"group_key": "tracker", "features": ("device_tracker",)},
        subentry_type="tracker",
        title="Tracker",
        subentry_id="tracker-subentry",
    )
    entry.subentries = {tracker_subentry.subentry_id: tracker_subentry}
    coordinator = stub_coordinator_factory(data=[dict(_DEVICE)])(
        hass, cache=SimpleNamespace(entry_id=entry.entry_id)
    )
    coordinator.config_entry = entry
    entry.runtime_data = SimpleNamespace(coordinator=coordinator)

    add_entities, added, pending = _make_add_entities(hass, loop)
    await device_tracker.async_setup_entry(hass, entry, add_entities)
    await asyncio.gather(*pending)
    assert len(added) == 2, "setup did not create the tracker entities"
    assert coordinator._listeners, "platform registered no coordinator listener"

    # The device leaves the account, then comes back, while this setup lives.
    coordinator.data = []
    _notify(coordinator)
    coordinator.data = [dict(_DEVICE)]
    _notify(coordinator)
    await asyncio.gather(*pending)
    assert len(added) == 2, "a returned id now adds entities live; F1 needs no reload"

    # What a reload runs: a new setup of the platform with the same snapshot.
    add_again, added_again, pending_again = _make_add_entities(hass, loop)
    await device_tracker.async_setup_entry(hass, entry, add_again)
    await asyncio.gather(*pending_again)
    unique_ids = {entity.unique_id for entity, _ in added_again}
    prefix = f"{entry.entry_id}:{tracker_subentry.subentry_id}:device-1"
    assert unique_ids == {prefix, f"{prefix}:last_location"}


@pytest.mark.asyncio
async def test_tracker_returning_after_a_restart_gets_entities_live(
    stub_coordinator_factory: Any,
) -> None:
    """A setup that never saw the id adds it live when it appears.

    This is the case of a restart (or any reload) between the removal and the
    return: the coordinator's memory of removed ids is gone, and it does not
    need to be there, because the new platform setup starts with an empty
    ``known_ids``. Only a return within the lifetime of one setup needs the
    reload of F1.
    """
    loop = asyncio.get_running_loop()
    hass = _make_hass(loop)
    entry = _ConfigEntryStub()
    tracker_subentry = ConfigSubentry(
        data={"group_key": "tracker", "features": ("device_tracker",)},
        subentry_type="tracker",
        title="Tracker",
        subentry_id="tracker-subentry",
    )
    entry.subentries = {tracker_subentry.subentry_id: tracker_subentry}
    coordinator = stub_coordinator_factory(data=[])(
        hass, cache=SimpleNamespace(entry_id=entry.entry_id)
    )
    coordinator.config_entry = entry
    entry.runtime_data = SimpleNamespace(coordinator=coordinator)

    add_entities, added, pending = _make_add_entities(hass, loop)
    await device_tracker.async_setup_entry(hass, entry, add_entities)
    await asyncio.gather(*pending)
    assert added == [], "setup without the device created entities"

    coordinator.data = [dict(_DEVICE)]
    _notify(coordinator)
    # B10 (R2-9): the listener adds the entities synchronously, so they are
    # there before any await; only their ``async_added_to_hass`` tasks remain,
    # and those are awaited by name, not by yielding one loop tick.
    assert len(added) == 2, "the listener did not add the entities synchronously"
    await asyncio.gather(*pending)
    prefix = f"{entry.entry_id}:{tracker_subentry.subentry_id}:device-1"
    assert {entity.unique_id for entity, _ in added} == {
        prefix,
        f"{prefix}:last_location",
    }
