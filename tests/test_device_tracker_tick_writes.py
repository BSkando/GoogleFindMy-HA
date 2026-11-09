# tests/test_device_tracker_tick_writes.py
"""Ticks without a new fix must not make a tracker look fresher (#1321).

Home Assistant's ``person`` integration picks the tracker with the newest
``last_updated``. The tracker therefore (a) publishes ``location_age`` and
``location_status`` as of the last observation, so a coordinator tick writes
identical attributes, and (b) reports ``force_update`` as ``False``, so Core
treats an identical write as a report (``state_reported``, only
``last_reported`` moves) instead of a change. Every tick still writes, so Core
re-evaluates zones and ``last_reported`` keeps working as a heartbeat.

Two layers, because ``tests/conftest.py`` binds the integration to stubbed
entity base classes (re-importing it against the real ones is ruled out in
``tests/AGENTS.md``): the entity tests pin what the tracker publishes and that
it writes on every tick; the contract test feeds exactly that output through
the real state machine with the tracker's own ``force_update`` value. Time is
driven by ``freezer``, never by sleeping.
"""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from custom_components.googlefindmy.const import TRACKER_SUBENTRY_KEY
from tests.helpers.config_entries_stub import make_config_entry

if TYPE_CHECKING:
    from freezegun.api import FrozenDateTimeFactory
    from homeassistant.core import Event, HomeAssistant


class _CoordinatorStub:
    """Coordinator serving one mutable device row."""

    def __init__(self, row: dict[str, Any] | None) -> None:
        self.hass = SimpleNamespace()
        self.config_entry = make_config_entry(
            entry_id="entry-ticks", options={}, runtime_data=None
        )
        self.row = row
        self.visible = True
        # The coarse fix the accuracy gate discarded, as the coordinator's
        # freshness accessor would return it; ``None`` = none or expired.
        self.coarse: dict[str, Any] | None = None

    def async_add_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        return lambda: None

    def is_device_visible_in_subentry(self, key: str, device_id: str) -> bool:
        return self.visible

    def is_device_present(self, device_id: str) -> bool:
        return True

    def get_device_location_data_for_subentry(
        self, key: str | None, device_id: str
    ) -> dict[str, Any] | None:
        return self.row

    def get_display_location_data_for_subentry(
        self, key: str | None, device_id: str
    ) -> dict[str, Any] | None:
        return self.row

    def get_subentry_snapshot(
        self, key: str | None = None, feature: str | None = None
    ) -> list[dict[str, Any]]:
        return [self.row] if self.row else []

    def get_fresh_coarse_fix(self, device_id: str) -> dict[str, Any] | None:
        return dict(self.coarse) if self.coarse else None

    def prime_device_location_cache(self, device_id: str, data: dict[str, Any]) -> None:
        self.row = {"id": device_id, "device_id": device_id, "name": "Pixel", **data}

    def stable_subentry_identifier(
        self, *, key: str | None = None, feature: str | None = None
    ) -> str | None:
        return key

    def get_subentry_metadata(
        self, *, key: str | None = None, feature: str | None = None
    ) -> Any:
        return SimpleNamespace(
            config_subentry_id=key,
            visible_device_ids=["dev-1"],
            enabled_device_ids=["dev-1"],
        )


def _row(last_seen: float, *, lat: float = 48.1) -> dict[str, Any]:
    return {
        "id": "dev-1",
        "device_id": "dev-1",
        "name": "Pixel",
        "latitude": lat,
        "longitude": 11.5,
        "accuracy": 10.0,
        "last_seen": last_seen,
    }


def _tracker(row: dict[str, Any], *, last_location: bool = False) -> Any:
    from custom_components.googlefindmy import device_tracker

    cls = (
        device_tracker.GoogleFindMyLastLocationTracker
        if last_location
        else device_tracker.GoogleFindMyDeviceTracker
    )
    entity = cls(
        _CoordinatorStub(row),
        {"id": "dev-1", "name": "Pixel"},
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=TRACKER_SUBENTRY_KEY,
    )
    entity.hass = SimpleNamespace()
    entity.entity_id = "device_tracker.googlefindmy_pixel"
    return entity


def _published(entity: Any) -> dict[str, Any]:
    entity._sync_location_attrs()
    return dict(entity._attr_extra_state_attributes)


def _coarse(last_seen: float) -> dict[str, Any]:
    """A coarse fix far from the published position, as the gate keeps it."""

    return {
        "latitude": 49.9,
        "longitude": 10.9,
        "accuracy": 1600.0,
        "last_seen": last_seen,
    }


def _status_sensor(entity: Any) -> Any:
    """The ``location_status`` sensor over the tracker's coordinator."""

    from custom_components.googlefindmy import sensor

    return sensor.GoogleFindMyLocationStatusSensor(
        entity.coordinator,
        {"id": "dev-1", "name": "Pixel"},
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=TRACKER_SUBENTRY_KEY,
    )


@pytest.mark.parametrize("last_location", [False, True])
def test_force_update_is_false(last_location: bool) -> None:
    """Both tracker classes report identical writes instead of changing state."""

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()), last_location=last_location)
    assert entity.force_update is False


def test_tf01_observation_signature_ignores_clock_driven_values() -> None:
    """Only a new published observation changes the signature.

    Taken over from #1321 (``test_location_age_stays_excluded_from_signature``)
    and extended: ``location_age``, ``location_status`` and the ``coarse_*``
    side information change with the clock or the coarse-fix window, so none
    of them may make a tick look like a new observation. The coarse case
    inverts #1321's ``test_coarse_fix_change_is_not_swallowed_by_signature``
    (the coarse fix is held with the observation and is no new observation).
    """

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp() - 3600))
    _published(entity)
    baseline = entity._state_signature()

    clocked = dict(entity._attr_extra_state_attributes)
    clocked["location_age"] = clocked["location_age"] + 60
    clocked["location_status"] = "aging"
    clocked.update(
        coarse_latitude=49.9,
        coarse_longitude=10.9,
        coarse_accuracy=1600.0,
        coarse_last_seen=1.0,
    )
    entity._attr_extra_state_attributes = clocked
    assert entity._state_signature() == baseline

    entity._attr_extra_state_attributes = {**clocked, "battery_level": 5}
    assert entity._state_signature() != baseline
    entity._attr_extra_state_attributes = clocked
    entity._attr_latitude = 48.2
    assert entity._state_signature() != baseline


def test_tf02_main_tracker_aging_without_fix_publishes_identical_attributes(
    freezer: FrozenDateTimeFactory,
) -> None:
    """``current`` -> ``aging`` without a new fix leaves the attributes alone."""

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()))
    first = _published(entity)
    assert first["location_status"] == "current"

    status_sensor = _status_sensor(entity)
    assert status_sensor.native_value == "current"

    freezer.tick(entity._get_stale_threshold() * 0.75)
    # Control: without holding, the status would now read "aging".
    assert entity._get_location_status() == "aging"
    assert _published(entity) == first, "last_updated would move: attributes changed"
    # The live class moved to the sensor.
    assert status_sensor.native_value == "aging"


def test_tf03_last_location_tracker_aging_without_fix_publishes_identical_attributes(
    freezer: FrozenDateTimeFactory,
) -> None:
    """The last-location tracker holds ``current`` until a new observation."""

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()), last_location=True)
    first = _published(entity)

    freezer.tick(entity._get_stale_threshold() * 2)
    assert entity._get_location_status() == "last_known"
    assert _published(entity) == first, "last_updated would move: attributes changed"


def test_tf12_location_age_tick_alone_publishes_identical_attributes(
    freezer: FrozenDateTimeFactory,
) -> None:
    """Sixty seconds later only ``location_age`` would change; it is held."""

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()))
    first = _published(entity)
    assert "location_age" in first

    freezer.tick(60)
    assert entity._get_location_status() == "current"
    assert _published(entity) == first, "last_updated would move: attributes changed"


def test_new_fix_publishes_fresh_age_and_status(freezer: FrozenDateTimeFactory) -> None:
    """A new observation replaces the held values."""

    from homeassistant.util import dt as dt_util

    start = dt_util.utcnow().timestamp()
    entity = _tracker(_row(start))
    _published(entity)
    freezer.tick(entity._get_stale_threshold() * 0.75)
    aged = _published(entity)
    assert aged["location_status"] == "current"

    entity.coordinator.row = _row(dt_util.utcnow().timestamp(), lat=48.2)
    fresh = _published(entity)
    assert fresh["location_status"] == "current"
    assert fresh["location_age"] == 0
    assert fresh != aged


@pytest.mark.asyncio
async def test_tf04_stale_transition_is_a_change_at_tick_time(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """Going stale withholds the coordinates: a change, stamped at the tick.

    Rebuilt from #1321's ``test_stale_transition_is_plain_now_write`` on the
    public surface: no new fix arrives, the clock passes the stale threshold,
    the state becomes ``unknown`` with ``location_status`` ``stale``, and
    ``last_updated``/``last_changed`` are the time of that tick, not of the
    old fix.
    """

    entity = _tracker(_row(_now() - 30))
    _wire_entity_write_path(entity, hass)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    fresh = hass.states.get(entity.entity_id)
    assert fresh is not None
    assert fresh.attributes["location_status"] == "current"
    assert fresh.attributes.get("latitude") is not None

    freezer.tick(entity._get_stale_threshold() + 60)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    stale = hass.states.get(entity.entity_id)
    assert stale is not None
    assert stale.state == "unknown"
    assert stale.attributes["location_status"] == "stale"
    assert stale.attributes.get("latitude") is None
    assert stale.attributes.get("last_latitude") == 48.1
    assert stale.last_updated.timestamp() == pytest.approx(_now(), abs=1e-3)
    assert stale.last_changed.timestamp() == pytest.approx(_now(), abs=1e-3)


@pytest.mark.asyncio
async def test_tf05_recovery_from_unavailable_is_a_change_at_tick_time(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """Coming back with the same old fix is written now, after the gap.

    Rebuilt from #1321's
    ``test_recovery_from_unavailable_with_old_fix_is_plain_write_and_orders_after``:
    the history stays in order, nothing is backdated to the old fix.
    """

    entity = _tracker(_row(_now() - 30))
    _wire_entity_write_path(entity, hass)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()

    freezer.tick(60)
    entity.coordinator.visible = False
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    gap = hass.states.get(entity.entity_id)
    assert gap is not None
    assert gap.state == "unavailable"

    freezer.tick(60)
    entity.coordinator.visible = True
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    back = hass.states.get(entity.entity_id)
    assert back is not None
    assert back.state == "not_home"
    assert back.last_updated > gap.last_updated
    assert back.last_updated.timestamp() == pytest.approx(_now(), abs=1e-3)


@pytest.mark.asyncio
async def test_tf06_restore_publishes_the_restored_fix_then_only_reports(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """After a restart: one change with the restored fix, then reports only.

    Rebuilt from #1321's ``test_first_write_after_restart_is_plain``. The
    held values are in place before the first write, so the ticks after it
    publish identical attributes.
    """

    from homeassistant.const import EVENT_STATE_CHANGED, EVENT_STATE_REPORTED
    from homeassistant.core import State, callback

    entity = _tracker(_row(_now()))
    entity.coordinator.row = None  # cold boot: the coordinator has not polled
    _wire_entity_write_path(entity, hass)
    restored = State(
        entity.entity_id,
        "not_home",
        {
            "latitude": 48.1,
            "longitude": 11.5,
            "accuracy_m": 10.0,
            "last_seen": _now() - 30,
        },
    )

    async def _last_state() -> State:
        return restored

    entity.async_get_last_state = _last_state
    events: list[str] = []

    @callback
    def _record(event: Event) -> None:
        if event.data.get("entity_id") == entity.entity_id:
            events.append(event.event_type)

    hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    hass.bus.async_listen(
        EVENT_STATE_REPORTED, _record, event_filter=callback(lambda _data: True)
    )

    await entity.async_added_to_hass()
    await hass.async_block_till_done()
    first = hass.states.get(entity.entity_id)
    assert first is not None
    assert first.attributes["latitude"] == 48.1
    assert events == [EVENT_STATE_CHANGED]
    first_updated = first.last_updated

    for _ in range(2):
        freezer.tick(60)
        entity._handle_coordinator_update()
        await hass.async_block_till_done()
    assert events == [EVENT_STATE_CHANGED, EVENT_STATE_REPORTED, EVENT_STATE_REPORTED]
    after = hass.states.get(entity.entity_id)
    assert after is not None
    assert after.last_updated == first_updated, "last_updated moved on a tick"


@pytest.mark.asyncio
async def test_restore_while_hidden_publishes_the_restored_fix_once_visible() -> None:
    """A restore for a hidden device still shows the restored fix later.

    The restore runs before the first publish, and that publish clears the
    tracker's own last-good cache for a device the coordinator does not show.
    Measured: the restored fix is not lost, because the same restore primes
    the coordinator cache, which the tracker reads once the device is visible.
    """

    entity = _tracker(_row(_now()))
    stub = entity.coordinator
    stub.row = None
    stub.visible = False

    def _gated(key: str | None, device_id: str) -> dict[str, Any] | None:
        # The real coordinator gates both reads on visibility.
        return stub.row if stub.visible else None

    stub.get_device_location_data_for_subentry = _gated
    stub.get_display_location_data_for_subentry = _gated
    restored = SimpleNamespace(
        attributes={
            "latitude": 48.3,
            "longitude": 11.6,
            "accuracy_m": 10.0,
            "last_seen": _now() - 30,
        }
    )

    async def _last_state() -> Any:
        return restored

    entity.async_get_last_state = _last_state
    entity.async_write_ha_state = lambda: None
    await entity.async_added_to_hass()
    assert entity._attr_latitude is None  # hidden: nothing published

    stub.visible = True
    entity._handle_coordinator_update()
    assert entity._attr_latitude == 48.3
    assert entity._attr_longitude == 11.6


def test_tf07_show_location_age_off_holds_the_status(
    freezer: FrozenDateTimeFactory,
) -> None:
    """Without ``location_age`` the held status alone keeps the tick identical."""

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()))
    entity.coordinator.config_entry.options["show_location_age"] = False
    first = _published(entity)
    assert "location_age" not in first
    assert first["location_status"] == "current"

    freezer.tick(entity._get_stale_threshold() * 0.75)
    assert entity._get_location_status() == "aging"
    assert _published(entity) == first, "last_updated would move: attributes changed"


@pytest.mark.parametrize("last_location", [False, True])
def test_every_coordinator_tick_writes(
    freezer: FrozenDateTimeFactory, last_location: bool
) -> None:
    """No tick is suppressed: Core must re-evaluate zones and ``last_reported``.

    An unchanged observation still produces one write per tick; with
    ``force_update`` false Core turns it into a report (contract test below)
    and still recomputes the zone of the unchanged position.
    """

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()), last_location=last_location)
    writes: list[dict[str, Any]] = []
    entity.async_write_ha_state = lambda: writes.append(
        dict(entity._attr_extra_state_attributes)
    )

    for _ in range(3):
        entity._handle_coordinator_update()
        freezer.tick(60)

    assert len(writes) == 3
    assert writes[0] == writes[1] == writes[2]


@pytest.mark.asyncio
async def test_contract_identical_write_is_a_report_not_a_change(
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
    freezer: FrozenDateTimeFactory,
) -> None:
    """Real state machine: the tracker's output plus its ``force_update``.

    Publishing the attributes of an unchanged observation again must not move
    ``last_updated`` (the value ``person`` ranks by); a new fix must.
    """

    from homeassistant.const import EVENT_STATE_CHANGED, EVENT_STATE_REPORTED
    from homeassistant.core import callback
    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()))
    events: list[str] = []

    @callback
    def _record(event: Event) -> None:
        events.append(event.event_type)

    hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    hass.bus.async_listen(
        EVENT_STATE_REPORTED, _record, event_filter=callback(lambda _data: True)
    )

    def _write() -> None:
        hass.states.async_set(
            entity.entity_id,
            "not_home",
            _published(entity),
            force_update=entity.force_update,
        )

    _write()
    await hass.async_block_till_done()
    first = hass.states.get(entity.entity_id)
    assert first is not None
    first_updated, first_reported = first.last_updated, first.last_reported
    events.clear()

    freezer.tick(entity._get_stale_threshold() * 0.75)
    _write()
    await hass.async_block_till_done()
    ticked = hass.states.get(entity.entity_id)
    assert ticked is not None
    assert events == [EVENT_STATE_REPORTED]
    assert ticked.last_updated == first_updated, "last_updated moved on a tick"
    assert ticked.last_reported > first_reported

    events.clear()
    entity.coordinator.row = _row(dt_util.utcnow().timestamp(), lat=48.2)
    _write()
    await hass.async_block_till_done()
    moved = hass.states.get(entity.entity_id)
    assert moved is not None
    assert events == [EVENT_STATE_CHANGED]
    assert moved.last_updated > first_updated


def test_show_location_age_switched_on_is_held_from_then_on(
    freezer: FrozenDateTimeFactory,
) -> None:
    """Switching the option on publishes the age once, then holds it."""

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp()))
    entity.coordinator.config_entry.options["show_location_age"] = False
    assert "location_age" not in _published(entity)

    entity.coordinator.config_entry.options["show_location_age"] = True
    switched_on = _published(entity)
    assert "location_age" in switched_on

    freezer.tick(120)
    assert _published(entity) == switched_on


def _record_events(hass: HomeAssistant, entity_id: str) -> list[str]:
    """Collect ``state_changed``/``state_reported`` for one entity, in order."""

    from homeassistant.const import EVENT_STATE_CHANGED, EVENT_STATE_REPORTED
    from homeassistant.core import callback

    events: list[str] = []

    @callback
    def _record(event: Event) -> None:
        if event.data.get("entity_id") == entity_id:
            events.append(event.event_type)

    hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    hass.bus.async_listen(
        EVENT_STATE_REPORTED, _record, event_filter=callback(lambda _data: True)
    )
    return events


@pytest.mark.asyncio
async def test_tf08_tick_without_new_observation_is_one_report(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """Through the entity: a tick writes, Core only reports it.

    Replaces #1321's ``test_noop_update_writes_plainly_without_restamp_or_state_change``:
    the write is no longer suppressed, so ``last_reported`` keeps moving
    while ``last_updated`` stays.
    """

    from homeassistant.const import EVENT_STATE_REPORTED

    entity = _tracker(_row(_now() - 30))
    _wire_entity_write_path(entity, hass)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    first = hass.states.get(entity.entity_id)
    assert first is not None
    # A report updates the same State object in place: keep the values.
    first_updated, first_reported = first.last_updated, first.last_reported
    events = _record_events(hass, entity.entity_id)

    freezer.tick(entity._get_stale_threshold() * 0.75)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    ticked = hass.states.get(entity.entity_id)
    assert ticked is not None
    assert events == [EVENT_STATE_REPORTED]
    assert ticked.last_updated == first_updated, "last_updated moved on a tick"
    assert ticked.last_reported > first_reported


@pytest.mark.asyncio
async def test_tf09_coarse_fix_expiry_moves_nothing_on_the_main_tracker(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """An expiring coarse fix waits for the next observation.

    Codex thread ``4247583316`` on #1353: expiry alone used to drop
    ``coarse_*``, which Core wrote as a change, and ``person`` could then rank
    the old coordinates as the freshest. The held keys stay until a new
    observation; the live value is on the coarse-fix sensor.
    """

    from homeassistant.const import EVENT_STATE_CHANGED, EVENT_STATE_REPORTED

    entity = _tracker(_row(_now() - 30))
    entity.coordinator.coarse = _coarse(_now() - 10)
    _wire_entity_write_path(entity, hass)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    first = hass.states.get(entity.entity_id)
    assert first is not None
    assert first.attributes["coarse_latitude"] == 49.9
    first_updated = first.last_updated
    first_coarse_seen = first.attributes["coarse_last_seen"]
    events = _record_events(hass, entity.entity_id)

    freezer.tick(60)
    entity.coordinator.coarse = None  # expired in the coordinator
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    held = hass.states.get(entity.entity_id)
    assert held is not None
    assert events == [EVENT_STATE_REPORTED], "last_updated moved: coarse_* changed"
    assert held.last_updated == first_updated
    assert held.attributes["coarse_latitude"] == 49.9
    assert held.attributes["coarse_last_seen"] == first_coarse_seen

    events.clear()
    entity.coordinator.row = _row(_now(), lat=48.2)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    moved = hass.states.get(entity.entity_id)
    assert moved is not None
    assert events == [EVENT_STATE_CHANGED]
    assert not [key for key in moved.attributes if key.startswith("coarse_")]


def test_tf10_coarse_fix_appearing_moves_nothing_on_the_main_tracker(
    freezer: FrozenDateTimeFactory,
) -> None:
    """The gate rejects a fix, the published position stays.

    The coarse fix is held back until the next observation, so the tick
    publishes identical attributes and ``last_updated`` stays.
    A later observation carries it if it is still fresh then.
    """

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp() - 30))
    first = _published(entity)
    assert not [key for key in first if key.startswith("coarse_")]

    freezer.tick(60)
    entity.coordinator.coarse = _coarse(dt_util.utcnow().timestamp() - 5)
    assert _published(entity) == first, "last_updated would move: coarse_* appeared"

    entity.coordinator.row = _row(dt_util.utcnow().timestamp() - 20, lat=48.2)
    assert _published(entity)["coarse_latitude"] == 49.9


@pytest.mark.asyncio
async def test_tf11_zone_created_around_the_unchanged_position_changes_state(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """Every tick writes, so Core re-evaluates zones.

    A zone added around the unchanged position changes the state on the next
    tick without a new fix; a suppressed write would have kept ``not_home``.
    """

    from homeassistant.components import zone

    zone.async_setup_track_zone_entity_ids(hass)
    entity = _tracker(_row(_now() - 30))
    _wire_entity_write_path(entity, hass)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    before = hass.states.get(entity.entity_id)
    assert before is not None
    assert before.state == "not_home"

    hass.states.async_set(
        "zone.office",
        "0",
        {
            "latitude": 48.1,
            "longitude": 11.5,
            "radius": 200.0,
            "friendly_name": "Office",
        },
    )
    await hass.async_block_till_done()
    freezer.tick(60)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    after = hass.states.get(entity.entity_id)
    assert after is not None
    assert after.state == "Office"


def test_tf13_last_location_tracker_carries_no_coarse_fix(
    freezer: FrozenDateTimeFactory,
) -> None:
    """The never-stale tracker has no ``coarse_*`` at all.

    Neither appearing nor expiring changes its attributes,
    so it never moves ``last_updated`` for a coarse fix.
    """

    from homeassistant.util import dt as dt_util

    entity = _tracker(_row(dt_util.utcnow().timestamp() - 30), last_location=True)
    entity.coordinator.coarse = _coarse(dt_util.utcnow().timestamp() - 5)
    first = _published(entity)
    assert not [key for key in first if key.startswith("coarse_")]

    freezer.tick(60)
    entity.coordinator.coarse = None
    assert _published(entity) == first, "last_updated would move: coarse_* changed"
    entity.coordinator.row = _row(dt_util.utcnow().timestamp(), lat=48.2)
    entity.coordinator.coarse = _coarse(dt_util.utcnow().timestamp())
    assert not [key for key in _published(entity) if key.startswith("coarse_")]


@pytest.mark.asyncio
async def test_registry_override_is_left_to_the_core_write_path(
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """Registry name and icon overrides reach the state through Core.

    Rebuilt from #1321's ``test_registry_override_reaches_the_written_state``
    without a double. Core applies the overrides in
    its entity state calculation (part 1, a real Entity). The tracker
    keeps them because it writes only through Core's write path: none of its
    own classes overrides the write or the state calculation (part 2), and
    ``test_every_coordinator_update_branch_ends_in_one_plain_write`` pins
    that it never writes ``hass.states`` around it.
    """

    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers.entity import Entity

    from custom_components.googlefindmy import device_tracker

    class _ProbeEntity(Entity):
        """Minimal real Entity whose own name/icon differ from the override."""

        _attr_name = "Entity Default Name"
        _attr_icon = "mdi:entity-default"

    registry = er.async_get(hass)
    entry = registry.async_get_or_create("device_tracker", "googlefindmy", "probe-1")
    entry = registry.async_update_entity(
        entry.entity_id, name="User Renamed Tracker", icon="mdi:user-custom-icon"
    )
    probe = _ProbeEntity()
    probe.hass = hass
    probe.entity_id = entry.entity_id
    probe.registry_entry = entry
    probe.async_write_ha_state()
    written = hass.states.get(entry.entity_id)
    assert written is not None
    assert written.attributes["friendly_name"] == "User Renamed Tracker"
    assert written.attributes["icon"] == "mdi:user-custom-icon"

    # Every variant of Core's write and state calculation carries one of these
    # parts in its name (public, private, thread-safe).
    write_path_parts = ("write_ha_state", "calculate_state")
    for cls in (
        device_tracker.GoogleFindMyDeviceTracker,
        device_tracker.GoogleFindMyLastLocationTracker,
    ):
        own = [
            (klass.__name__, name)
            for klass in cls.__mro__
            if klass.__module__.startswith("custom_components.")
            for name in vars(klass)
            if any(part in name for part in write_path_parts)
        ]
        assert own == [], f"{cls.__name__} overrides Core's write path: {own}"


# ---------------------------------------------------------------------------
# Codex review of #1321 at ``ceb5e70`` (three threads on the restamp path).
#
# The findings concern writes that bypass the entity's own write path:
# ``hass.states.async_set(..., timestamp=<old fix time>)``. The tests below
# therefore drive ticks through ``_handle_coordinator_update()`` against the
# real state machine. The conftest stubs give the tracker a no-op
# ``async_write_ha_state`` and none of Entity's private surface, so
# ``_wire_entity_write_path`` supplies both: a write that does what Core's
# ``Entity._async_write_ha_state`` does with the timestamp (none, so Core
# stamps "now") and with ``force_update``, plus the TrackerEntity zone logic
# for the state string. Code that writes through ``async_write_ha_state`` is
# measured by the assertions; code that writes around it is measured too,
# because the assertions read the state machine, not the double.
#
# ``freezer`` comes first among the fixtures: ``use_real_homeassistant_modules``
# re-imports Home Assistant, and a ``dt_util`` imported after the freeze
# started keeps reading the real clock while ``time.time()`` and the state
# machine follow the frozen one (measured). Timestamps are compared with an
# absolute tolerance; ``pytest.approx``'s relative default is about half an
# hour at today's epoch values and would hide a restamp to the old fix.
# ---------------------------------------------------------------------------


def _core_state(
    entity: Any, hass: HomeAssistant, *, legacy_zone_api: bool = False
) -> tuple[str, list[str] | None]:
    """State string and ``in_zones`` as ``TrackerEntity`` derives them.

    ``zone.async_in_zones`` and the ``in_zones`` attribute are newer than the
    declared Home Assistant floor; 2025.9.1 and 2026.1.3 derive the state from
    ``zone.async_active_zone`` and publish no ``in_zones``. Follow whichever
    API the installed core provides and return ``None`` for ``in_zones`` on
    the older line, in every branch. ``legacy_zone_api`` forces the older
    path on a newer core; deleting ``async_in_zones`` cannot simulate it,
    because there ``async_active_zone`` itself calls ``async_in_zones``
    (measured on 2026.9.3: ``NameError``).
    """

    from homeassistant.components import zone
    from homeassistant.const import STATE_HOME, STATE_NOT_HOME

    in_zones_fn = None if legacy_zone_api else getattr(zone, "async_in_zones", None)
    lat, lon = entity._attr_latitude, entity._attr_longitude
    if lat is None or lon is None:
        return "unknown", ([] if in_zones_fn is not None else None)
    radius = entity._attr_location_accuracy or 0
    in_zones: list[str] | None
    if in_zones_fn is None:
        active = zone.async_active_zone(hass, lat, lon, radius)
        in_zones = None
    else:
        active, in_zones = in_zones_fn(hass, lat, lon, radius)
    if active is None:
        return STATE_NOT_HOME, in_zones
    if active.entity_id == zone.ENTITY_ID_HOME:
        return STATE_HOME, in_zones
    return active.name, in_zones


def _wire_entity_write_path(
    entity: Any, hass: HomeAssistant, *, legacy_zone_api: bool = False
) -> None:
    """Give a stub-bound tracker Core's write path on a real state machine.

    The write does what Core's ``Entity._async_write_ha_state`` does with the
    parts these tests measure: no timestamp (Core stamps "now"), the entity's
    ``force_update``, ``unavailable`` without attributes when the entity is not
    available, otherwise the TrackerEntity zone state and the extra
    attributes. It does not reproduce ``TrackerEntity.state_attributes``
    (``latitude``, ``longitude``, ``gps_accuracy`` from the ``_attr_*``
    values) nor the friendly name or other registry-derived keys; that is
    enough for timestamps and the zone state, and the plain ``latitude`` the
    tests read comes from the tracker's own extra attributes.
    """

    from homeassistant.const import STATE_UNAVAILABLE

    entity.hass = hass

    def _write() -> None:
        if not entity.available:
            state, attributes = STATE_UNAVAILABLE, {}
        else:
            state, in_zones = _core_state(entity, hass, legacy_zone_api=legacy_zone_api)
            attributes = dict(entity._attr_extra_state_attributes)
            if in_zones is not None:
                attributes["in_zones"] = in_zones
        hass.states.async_set(
            entity.entity_id,
            state,
            attributes,
            # Core's TrackerEntity default for a push entity is ``True``
            # (``not should_poll``); the stub base defines no value.
            force_update=getattr(entity, "force_update", True),
        )

    entity.async_write_ha_state = _write


def _now() -> float:
    from homeassistant.util import dt as dt_util

    return dt_util.utcnow().timestamp()


@pytest.mark.parametrize(
    "branch",
    [
        "device_not_visible",
        "no_row",
        "reliable_fix",
        "first_unreliable_fix",
        "unreliable_fix_after_reliable",
    ],
)
@pytest.mark.asyncio
async def test_every_coordinator_update_branch_ends_in_one_plain_write(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
    branch: str,
) -> None:
    """Contract of ``custom_components/googlefindmy/AGENTS.md`` (Codex P1, #1321).

    Every branch of ``_handle_coordinator_update`` ends in exactly one
    ``async_write_ha_state`` and never writes ``hass.states`` around it. A
    state of the entity is already in the state machine, so a write path that
    restamps an existing state is reached in every branch.
    """

    entity = _tracker(_row(_now() - 30))
    _wire_entity_write_path(entity, hass)
    hass.states.async_set(entity.entity_id, "not_home", {})
    await hass.async_block_till_done()

    estimated = {**_row(_now() - 30), "accuracy_estimated": True}
    if branch == "device_not_visible":
        entity.coordinator.visible = False
    elif branch == "no_row":
        entity.coordinator.row = None
    elif branch == "first_unreliable_fix":
        entity.coordinator.row = estimated
    elif branch == "unreliable_fix_after_reliable":
        entity._last_good_accuracy_data = _row(_now() - 60)
        entity.coordinator.row = estimated

    writes: list[None] = []
    direct: list[tuple[Any, ...]] = []
    # A pure counter: passing the call on would itself write hass.states and
    # hide a direct write behind it.
    entity.async_write_ha_state = lambda: writes.append(None)

    class _RecordingStates:
        """``hass.states`` as the entity sees it; counts writes around it.

        Both public write entry points of the state machine are counted:
        ``async_set`` and ``async_set_internal`` (the one Core's own entity
        write uses). A write through another object than ``self.hass`` is out
        of reach of this view; the timestamp tests below read the state
        machine itself and would see its effect.
        """

        def __getattr__(self, name: str) -> Any:
            return getattr(hass.states, name)

        def async_set(self, *args: Any, **kwargs: Any) -> None:
            direct.append(args)
            hass.states.async_set(*args, **kwargs)

        def async_set_internal(self, *args: Any, **kwargs: Any) -> None:
            direct.append(args)
            hass.states.async_set_internal(*args, **kwargs)

    class _HassView:
        """The real ``hass`` with the recording ``states`` in its place."""

        states = _RecordingStates()

        def __getattr__(self, name: str) -> Any:
            return getattr(hass, name)

    entity.hass = _HassView()
    freezer.tick(120)
    entity._handle_coordinator_update()

    assert len(writes) == 1, f"{branch}: {len(writes)} writes"
    assert direct == [], f"{branch}: wrote hass.states directly"


@pytest.mark.asyncio
async def test_last_reported_never_moves_backward(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
) -> None:
    """``last_reported`` is a heartbeat and never jumps back (Codex P2, #1321).

    Ticks go through the entity: an identical tick, a tick in which the age
    of the unchanged fix enters a new minute, and a new fix. After each one
    ``last_reported`` is the time of that tick.
    """

    entity = _tracker(_row(_now() - 30))
    entity.coordinator.config_entry.options["show_location_age"] = True
    _wire_entity_write_path(entity, hass)

    seen: list[float] = []

    async def _tick(seconds: float) -> None:
        freezer.tick(seconds)
        entity._handle_coordinator_update()
        await hass.async_block_till_done()
        state = hass.states.get(entity.entity_id)
        assert state is not None
        now = _now()
        assert state.last_reported.timestamp() == pytest.approx(now, abs=1e-3), (
            "last_reported is not the time of the tick"
        )
        assert state.last_updated <= state.last_reported
        seen.append(state.last_reported.timestamp())

    await _tick(0)
    await _tick(5)  # identical observation
    await _tick(120)  # age of the same fix enters a new minute
    entity.coordinator.row = _row(_now() + 10, lat=48.2)
    await _tick(10)  # new fix

    assert seen == sorted(seen)


@pytest.mark.parametrize("legacy_zone_api", [False, True])
@pytest.mark.asyncio
async def test_zone_change_without_new_fix_stamps_last_changed_now(
    freezer: FrozenDateTimeFactory,
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
    legacy_zone_api: bool,
) -> None:
    """A new zone of an unchanged fix is a change at tick time (Codex P2, #1321).

    The zone moves away from the unchanged position; the next tick carries
    the same observation and a new age minute. The derived state changes, so
    ``last_changed`` must be the time of that tick, not of the old fix.
    """

    from homeassistant.components import zone

    zone.async_setup_track_zone_entity_ids(hass)
    zone_attrs = {"latitude": 48.1, "longitude": 11.5, "radius": 200.0}
    hass.states.async_set("zone.office", "0", {**zone_attrs, "friendly_name": "Office"})
    await hass.async_block_till_done()

    entity = _tracker(_row(_now() - 30))
    entity.coordinator.config_entry.options["show_location_age"] = True
    _wire_entity_write_path(entity, hass, legacy_zone_api=legacy_zone_api)

    entity._handle_coordinator_update()
    await hass.async_block_till_done()
    before = hass.states.get(entity.entity_id)
    assert before is not None
    assert before.state == "Office"

    hass.states.async_set(
        "zone.office",
        "0",
        {**zone_attrs, "latitude": 48.3, "friendly_name": "Office"},
    )
    freezer.tick(120)
    entity._handle_coordinator_update()
    await hass.async_block_till_done()

    after = hass.states.get(entity.entity_id)
    assert after is not None
    assert after.state == "not_home"
    assert after.last_changed.timestamp() == pytest.approx(_now(), abs=1e-3), (
        "last_changed carries the old fix time instead of the tick time"
    )


# ---------------------------------------------------------------------------
# Repair issue announcing the held attributes.
#
# ``issue_registry_capture`` records every argument the platform passes to
# ``async_create_issue`` and every deletion. Home Assistant's own
# ``async_at_started`` runs unchanged, against a small hass that offers the
# surface it reads (``state``, ``bus.async_listen_once``,
# ``async_run_hass_job``) in the helper's own module generation. A real
# ``hass`` was measured unusable for this: the fixture's ``homeassistant.core``
# predates the re-imported helpers, so ``hass.state is CoreState.running``
# fails and the job then runs on a path where the re-imported frame helper is
# not set up; and the real ``automation`` integration cannot be set up in this
# harness. Entity registry and the two reference helpers are replaced by
# doubles of their public functions.
# ---------------------------------------------------------------------------


class _StartingHass:
    """The hass surface ``async_at_started`` reads, with a firable bus."""

    def __init__(self, *, started: bool) -> None:
        from custom_components.googlefindmy import device_tracker

        helper_globals = device_tracker.async_at_started.__globals__
        core_state = helper_globals["CoreState"]
        self._core_state = core_state
        self._started_event = helper_globals["EVENT_HOMEASSISTANT_STARTED"]
        self.state = core_state.running if started else core_state.starting
        self.listeners: dict[Any, list[Callable[[Any], None]]] = {}
        self.bus = SimpleNamespace(async_listen_once=self._listen_once)

    def _listen_once(self, event_type: Any, listener: Callable[[Any], None]) -> Any:
        self.listeners.setdefault(event_type, []).append(listener)

        def _remove() -> None:
            self.listeners[event_type].remove(listener)

        return _remove

    def async_run_hass_job(self, job: Any, *args: Any) -> None:
        job.target(*args)

    def fire_started(self) -> None:
        self.state = self._core_state.running
        for listener in list(self.listeners.pop(self._started_event, [])):
            listener(SimpleNamespace(event_type=self._started_event))


class _Entry:
    """A config entry with its unload callbacks."""

    def __init__(self, entry_id: str) -> None:
        self.entry_id = entry_id
        self.title = entry_id
        self.disabled_by: str | None = None
        self.on_unload: list[Callable[[], None]] = []

    def async_on_unload(self, func: Callable[[], None]) -> None:
        self.on_unload.append(func)

    def unload(self) -> None:
        for func in self.on_unload:
            func()
        self.on_unload.clear()


@pytest.fixture
def repair_platform(monkeypatch: pytest.MonkeyPatch) -> Any:
    """The tracker module with registry and reference helpers as doubles."""

    from custom_components.googlefindmy import device_tracker

    trackers = {
        "entry-a": ["device_tracker.a"],
        "entry-b": ["device_tracker.b"],
        "entry-late": ["device_tracker.late"],
        "entry-gone": ["device_tracker.gone"],
    }
    # Replacement sensors as a German installation names them: the entity id
    # follows the translated name, the unique_id keeps the English suffix.
    sensors = {
        "entry-a": [
            (
                "sensor.a_standortstatus",
                "googlefindmy_entry-a_tracker_a_location_status",
            ),
            ("sensor.a_zuletzt_gesehen", "googlefindmy_entry-a_tracker_a_last_seen"),
            ("sensor.a_grobortung", "googlefindmy_entry-a_tracker_a_coarse_fix"),
        ],
        "entry-b": [
            ("sensor.b_coarse_fix", "googlefindmy_entry-b_tracker_b_coarse_fix"),
        ],
    }
    references: dict[str, list[str]] = {}
    script_references: dict[str, list[str]] = {}

    def _entries(registry: Any, entry_id: str) -> list[SimpleNamespace]:
        registered = [
            (entity_id, f"googlefindmy_{entry_id}_{entity_id}")
            for entity_id in trackers.get(entry_id, [])
        ]
        registered += sensors.get(entry_id, [])
        # Another sensor of the entry that carries no held attribute.
        registered.append(("sensor.other", f"googlefindmy_{entry_id}_x_plus_code"))
        return [
            SimpleNamespace(
                entity_id=entity_id,
                domain=entity_id.split(".")[0],
                unique_id=unique_id,
            )
            for entity_id, unique_id in registered
        ]

    monkeypatch.setattr(
        device_tracker,
        "er",
        SimpleNamespace(
            async_get=lambda hass: "registry",
            async_entries_for_config_entry=_entries,
        ),
    )
    monkeypatch.setattr(
        device_tracker,
        "automations_with_entity",
        lambda hass, entity_id: references.get(entity_id, []),
    )
    monkeypatch.setattr(
        device_tracker,
        "scripts_with_entity",
        lambda hass, entity_id: script_references.get(entity_id, []),
    )
    return SimpleNamespace(
        module=device_tracker,
        references=references,
        script_references=script_references,
    )


def test_repair_issue_one_per_entry_kept_over_an_unload(
    repair_platform: Any, issue_registry_capture: Any
) -> None:
    """One WARNING per entry with the removal version, kept over an unload.

    Deleting it on unload would drop the user's "ignore" with every reload;
    ``async_remove_entry`` deletes it instead.
    """

    from custom_components.googlefindmy.const import (
        DOMAIN,
        TRACKER_ATTRS_BREAKS_IN_HA_VERSION,
    )

    hass = _StartingHass(started=True)
    first, second = _Entry("entry-a"), _Entry("entry-b")
    repair_platform.module._async_track_frozen_attributes_issue(hass, first)
    repair_platform.module._async_track_frozen_attributes_issue(hass, second)

    issues = issue_registry_capture.registry.issues
    assert set(issues) == {
        "tracker_attributes_frozen_entry-a",
        "tracker_attributes_frozen_entry-b",
    }
    issue = issues["tracker_attributes_frozen_entry-a"]
    assert issue["domain"] == DOMAIN
    assert issue["breaks_in_ha_version"] == TRACKER_ATTRS_BREAKS_IN_HA_VERSION
    assert issue["severity"] == repair_platform.module.ir.IssueSeverity.WARNING
    assert issue["is_fixable"] is False
    assert issue["is_persistent"] is False
    assert issue["translation_key"] == "tracker_attributes_frozen_no_automations"
    assert issue["translation_placeholders"] == {
        "entry_title": "entry-a",
        "replacement_sensors": (
            "- sensor.a_grobortung\n- sensor.a_standortstatus\n- sensor.a_zuletzt_gesehen"
        ),
        "breaks_version": TRACKER_ATTRS_BREAKS_IN_HA_VERSION,
    }

    assert (
        issues["tracker_attributes_frozen_entry-b"]["translation_placeholders"][
            "replacement_sensors"
        ]
        == "- sensor.b_coarse_fix"
    )

    first.unload()
    assert set(issues) == {
        "tracker_attributes_frozen_entry-a",
        "tracker_attributes_frozen_entry-b",
    }
    assert issue_registry_capture.deleted == []

    # A reload sets the platform up again and refreshes the same issue.
    repair_platform.module._async_track_frozen_attributes_issue(hass, first)
    assert [item["issue_id"] for item in issue_registry_capture.created].count(
        "tracker_attributes_frozen_entry-a"
    ) == 2


def test_repair_issue_lists_automations_loaded_after_the_platform(
    repair_platform: Any, issue_registry_capture: Any
) -> None:
    """The list is built once Home Assistant has started.

    ``automation`` may load after this platform; asked during the setup it
    would report no references. After ``homeassistant_started`` the issue
    names the automations and scripts that use the entry's trackers (not those
    of other entries) and uses the key with the list.
    """

    hass = _StartingHass(started=False)
    entry = _Entry("entry-late")
    repair_platform.module._async_track_frozen_attributes_issue(hass, entry)
    assert issue_registry_capture.created == []

    # ``automation`` finishes loading; only then does Home Assistant start.
    repair_platform.references["device_tracker.late"] = ["automation.arrives"]
    repair_platform.references["device_tracker.a"] = ["automation.elsewhere"]
    repair_platform.script_references["device_tracker.late"] = [
        "script.notify",
        "automation.arrives",
    ]
    # Another entity of the same entry is not a tracker and is not listed.
    repair_platform.script_references["sensor.other"] = ["script.unrelated"]
    hass.fire_started()

    (issue,) = issue_registry_capture.created
    assert issue["issue_id"] == "tracker_attributes_frozen_entry-late"
    assert issue["translation_key"] == "tracker_attributes_frozen"
    assert issue["translation_placeholders"]["automations"] == (
        "- automation.arrives\n- script.notify"
    )
    # No replacement sensor registered for this entry: a placeholder, not "",
    # and not "-", which under the sentence would render it as a heading.
    assert issue["translation_placeholders"]["replacement_sensors"] == "\u2026"


def test_repair_issue_skipped_while_no_tracker_is_registered(
    repair_platform: Any, issue_registry_capture: Any
) -> None:
    """An entry added on a running Home Assistant gets no issue yet.

    The start callback runs at once, before any entity of the entry is
    registered; without a tracker there is nobody using the held attributes.
    """

    hass = _StartingHass(started=True)
    entry = _Entry("entry-new")
    repair_platform.module._async_track_frozen_attributes_issue(hass, entry)

    assert issue_registry_capture.created == []
    assert issue_registry_capture.registry.issues == {}


def test_repair_issue_deleted_when_the_entry_is_disabled(
    repair_platform: Any, issue_registry_capture: Any
) -> None:
    """Disabling the entry deletes its issue; a plain unload keeps it."""

    from custom_components.googlefindmy.const import DOMAIN

    hass = _StartingHass(started=True)
    kept, disabled = _Entry("entry-a"), _Entry("entry-b")
    repair_platform.module._async_track_frozen_attributes_issue(hass, kept)
    repair_platform.module._async_track_frozen_attributes_issue(hass, disabled)

    kept.unload()
    disabled.disabled_by = "user"
    disabled.unload()

    assert issue_registry_capture.deleted == [
        (DOMAIN, "tracker_attributes_frozen_entry-b")
    ]
    assert set(issue_registry_capture.registry.issues) == {
        "tracker_attributes_frozen_entry-a"
    }


def test_repair_issue_not_created_when_unloaded_before_start(
    repair_platform: Any, issue_registry_capture: Any
) -> None:
    """Unloading before ``homeassistant_started`` cancels the pending issue."""

    hass = _StartingHass(started=False)
    entry = _Entry("entry-gone")
    repair_platform.module._async_track_frozen_attributes_issue(hass, entry)
    entry.unload()
    hass.fire_started()

    assert issue_registry_capture.created == []
    assert issue_registry_capture.registry.issues == {}


def test_repair_issue_skipped_for_a_hass_without_start_state(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A hass double without ``state``/``bus`` skips the issue and only logs.

    Production always has both; the branch keeps the platform setup of fast
    test doubles working, and it catches no exception.
    """

    import logging

    from custom_components.googlefindmy import device_tracker

    entry = _Entry("entry-stub")
    with caplog.at_level(logging.DEBUG, logger=device_tracker.__name__):
        device_tracker._async_track_frozen_attributes_issue(SimpleNamespace(), entry)
    assert entry.on_unload == []
    assert "Held-attributes repair issue not scheduled" in caplog.text


@pytest.mark.asyncio
async def test_redelivered_report_is_reported_not_changed(
    hass: HomeAssistant,
    use_real_homeassistant_modules: None,
    freezer: FrozenDateTimeFactory,
) -> None:
    """The same report arriving again must not make the tracker look fresher.

    The rows come from the real ``update_device_cache``: an older fix, then a
    report that is fused with it, then the very same report once more. The
    second row is fed through the tracker and the real state machine; it must
    be a report, not a change, or ``person`` would rank the tracker by a write
    that carried no new location.
    """

    from homeassistant.const import EVENT_STATE_CHANGED, EVENT_STATE_REPORTED
    from homeassistant.core import callback
    from homeassistant.util import dt as dt_util

    from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator

    now = dt_util.utcnow().timestamp()
    cache = GoogleFindMyCoordinator.__new__(GoogleFindMyCoordinator)
    cache._device_location_data = {}
    cache._device_names = {}
    cache._device_update_history = {}
    cache.increment_stat = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    cache._is_on_hass_loop = lambda: True  # type: ignore[method-assign]
    report = {
        "latitude": 48.1003,
        "longitude": 11.5,
        # 50 m on both sides overlaps the ~33 m step, so the report is fused.
        "accuracy": 50.0,
        "status": "coordinate",
    }
    cache.update_device_cache(
        "dev-1", {**report, "latitude": 48.1, "last_seen": now - 60}
    )
    cache.update_device_cache("dev-1", {**report, "last_seen": now - 30})
    fused = dict(cache._device_location_data["dev-1"])
    assert fused["status"] == "Fused (Weighted)"
    cache.update_device_cache("dev-1", {**report, "last_seen": now - 30})
    redelivered = dict(cache._device_location_data["dev-1"])

    identity = {"id": "dev-1", "device_id": "dev-1", "name": "Pixel"}
    entity = _tracker({**fused, **identity})
    events: list[str] = []

    @callback
    def _record(event: Event) -> None:
        events.append(event.event_type)

    hass.bus.async_listen(EVENT_STATE_CHANGED, _record)
    hass.bus.async_listen(
        EVENT_STATE_REPORTED, _record, event_filter=callback(lambda _data: True)
    )

    def _write() -> None:
        hass.states.async_set(
            entity.entity_id,
            "not_home",
            _published(entity),
            force_update=entity.force_update,
        )

    _write()
    await hass.async_block_till_done()
    first = hass.states.get(entity.entity_id)
    assert first is not None
    first_updated = first.last_updated
    events.clear()

    freezer.tick(30)
    entity.coordinator.row = {**redelivered, **identity}
    _write()
    await hass.async_block_till_done()
    second = hass.states.get(entity.entity_id)
    assert second is not None
    assert events == [EVENT_STATE_REPORTED]
    assert second.last_updated == first_updated
