# tests/test_switch_automatic_polling.py
"""Per-tracker switch ``automatic_location_polling`` (#242).

The switch is the user-facing side of the coordinator option
``automatic_polling_disabled``. These tests hold the platform to its contract:
one CONFIG switch per visible tracker on the tracker's device, the platform is
loaded for existing installations, toggling writes the options without
reloading the entry, the other per-tracker entities stay unchanged, recovery
recreates a missing switch, unloading works without trackers, and switching off
does not change the availability of the protection mode sensor.
"""

from __future__ import annotations

import asyncio
import time
import types
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.helpers.entity import EntityCategory

from custom_components.googlefindmy import EntityRecoveryManager, button, switch
from custom_components.googlefindmy.const import (
    DOMAIN,
    SERVICE_FEATURE_PLATFORMS,
    SERVICE_SUBENTRY_KEY,
    TRACKER_FEATURE_PLATFORMS,
    TRACKER_SUBENTRY_KEY,
)
from custom_components.googlefindmy.coordinator import GoogleFindMyCoordinator
from tests.test_button_subentry_setup import (
    _ConfigEntryStub,
    _make_add_entities,
    _make_hass,
)

_OPT_DISABLED = "automatic_polling_disabled"
_SUBENTRY_ID = "tracker-subentry"
_DEVICES = [
    {"id": "dev-a", "name": "Keys"},
    {"id": "dev-b", "name": "Backpack"},
]


def _switch_uid(entry_id: str, dev_id: str) -> str:
    return f"{DOMAIN}_{entry_id}_{_SUBENTRY_ID}_{dev_id}_automatic_location_polling"


def _tracker_subentry(features: tuple[str, ...] = ("switch",)) -> ConfigSubentry:
    return ConfigSubentry(
        data={"group_key": TRACKER_SUBENTRY_KEY, "features": features},
        subentry_type="tracker",
        title="Tracker",
        subentry_id=_SUBENTRY_ID,
    )


def _coordinator(
    stub_coordinator_factory: Callable[..., type[Any]],
    hass: Any,
    entry: Any,
    *,
    devices: list[dict[str, Any]],
    hidden: frozenset[str] = frozenset(),
) -> Any:
    """Stub coordinator whose switch state lives in the entry options."""
    base = stub_coordinator_factory(data=devices)

    class _Coordinator(base):  # type: ignore[misc, valid-type]
        def is_device_visible_in_subentry(
            self, subentry_key: str, device_id: str
        ) -> bool:
            return device_id not in hidden

        def is_automatic_polling_enabled(self, device_id: str) -> bool:
            return device_id not in set(
                self.config_entry.options.get(_OPT_DISABLED, [])
            )

        def async_set_automatic_polling(self, device_id: str, enabled: bool) -> None:
            current = set(self.config_entry.options.get(_OPT_DISABLED, []))
            current = current - {device_id} if enabled else current | {device_id}
            self.config_entry.options = {
                **self.config_entry.options,
                _OPT_DISABLED: sorted(current),
            }

    coordinator = _Coordinator(hass, cache=SimpleNamespace(entry_id=entry.entry_id))
    coordinator.config_entry = entry
    return coordinator


async def _setup_switches(
    stub_coordinator_factory: Callable[..., type[Any]],
    *,
    devices: list[dict[str, Any]] = _DEVICES,
    hidden: frozenset[str] = frozenset(),
    recovery: bool = False,
) -> tuple[Any, Any, Any, list[tuple[Any, str | None]], list[asyncio.Task[Any]]]:
    loop = asyncio.get_running_loop()
    hass = _make_hass(loop)
    entry = _ConfigEntryStub()
    entry.subentries = {_SUBENTRY_ID: _tracker_subentry()}
    coordinator = _coordinator(
        stub_coordinator_factory, hass, entry, devices=devices, hidden=hidden
    )
    runtime = SimpleNamespace(coordinator=coordinator)
    if recovery:
        runtime.entity_recovery_manager = EntityRecoveryManager(
            hass, entry, coordinator
        )
    entry.runtime_data = runtime
    add_entities, added, pending = _make_add_entities(hass, loop)
    await switch.async_setup_entry(hass, entry, add_entities)
    await asyncio.gather(*pending)
    return hass, entry, coordinator, added, pending


# ---------------------------------------------------------------------------
# (a) one CONFIG switch per tracker, on the tracker's device
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_one_config_switch_per_tracker_on_its_device(
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """(a) N trackers give N switches, category CONFIG, the tracker's device."""
    hass, entry, coordinator, added, _ = await _setup_switches(stub_coordinator_factory)

    switches = [entity for entity, _ in added]
    assert {entity.unique_id for entity in switches} == {
        _switch_uid(entry.entry_id, dev["id"]) for dev in _DEVICES
    }
    assert {config for _, config in added} == {_SUBENTRY_ID}
    for entity in switches:
        assert entity.entity_description.entity_category is EntityCategory.CONFIG
        assert entity.entity_description.translation_key == (
            "automatic_location_polling"
        )
        locate = button.GoogleFindMyLocateButton(
            coordinator,
            dict(entity._device),
            None,
            subentry_key=TRACKER_SUBENTRY_KEY,
            subentry_identifier=_SUBENTRY_ID,
        )
        assert vars(entity.device_info) == vars(locate.device_info)
        assert entity.is_on is True


# ---------------------------------------------------------------------------
# (b) an existing entry without "switch" in its stored features loads it
# ---------------------------------------------------------------------------


def test_switch_is_a_tracker_feature_platform() -> None:
    """(b, static) ``switch`` is one of the canonical tracker platforms."""
    assert "switch" in TRACKER_FEATURE_PLATFORMS
    assert list(TRACKER_FEATURE_PLATFORMS) == sorted(TRACKER_FEATURE_PLATFORMS)


@pytest.mark.asyncio
async def test_existing_entry_without_switch_feature_loads_the_platform(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """(b) Stored features from before the update are rewritten at setup (A3).

    ``async_setup_entry`` syncs the tracker subentry definition from
    ``TRACKER_FEATURE_PLATFORMS``; the subentry stored by an older version
    therefore carries ``switch`` afterwards, and forwarding loads the platform.
    """
    from tests.test_hass_data_layout import _prepare_async_setup_entry_harness

    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, asyncio.get_running_loop()
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass
    # Both subentries as an installation from before the update stored them.
    stored = (
        (
            "old-tracker",
            "tracker",
            TRACKER_SUBENTRY_KEY,
            ["button", "device_tracker", "sensor"],
        ),
        (
            "old-service",
            "service",
            SERVICE_SUBENTRY_KEY,
            list(SERVICE_FEATURE_PLATFORMS),
        ),
    )
    for subentry_id, subentry_type, key, features in stored:
        entry.subentries[subentry_id] = ConfigSubentry(
            data={"group_key": key, "features": features},
            subentry_type=subentry_type,
            title=key,
            subentry_id=subentry_id,
            unique_id=f"{entry.entry_id}-{key}",
        )
        hass.config_entries._registered_subentry_ids.add(subentry_id)
    harness.cache.values = {integration.username_string: "user@example.com"}

    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    while pending := [task for task in hass._tasks if not task.done()]:
        await asyncio.gather(*pending, return_exceptions=True)

    trackers = [
        sub
        for sub in entry.subentries.values()
        if getattr(sub, "subentry_type", None) == "tracker"
    ]
    assert [sub.subentry_id for sub in trackers] == ["old-tracker"]
    for sub in trackers:
        assert "switch" in sub.data["features"]

    forwarded = {
        str(getattr(platform, "value", platform))
        for _entry, platforms in hass.config_entries.forward_calls
        for platform in platforms
    }
    assert "switch" in forwarded


# ---------------------------------------------------------------------------
# (c) toggling writes the options through the real setup and never reloads
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_toggle_does_not_reload_entry(
    monkeypatch: pytest.MonkeyPatch,
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """(c) Off and on store the options; the update listener schedules no reload.

    The entry is set up through ``async_setup_entry``, which installs the real
    options update listener ``_async_refresh_watch_paths``. Home Assistant core
    calls the update listeners as tasks when ``async_update_entry`` changes the
    entry; the stub manager below does the same and the tasks are awaited.
    The coordinator's own ``async_set_automatic_polling`` does the writing.
    """
    from tests.test_hass_data_layout import _prepare_async_setup_entry_harness

    harness = _prepare_async_setup_entry_harness(
        monkeypatch, stub_coordinator_factory, asyncio.get_running_loop()
    )
    integration = harness.integration
    entry = harness.entry
    hass = harness.hass
    harness.cache.values = {integration.username_string: "user@example.com"}
    assert await integration.async_setup(hass, {}) is True
    assert await integration.async_setup_entry(hass, entry) is True
    listeners = list(entry._update_listeners)
    assert listeners, "async_setup_entry installs the options update listener"

    stored = hass.config_entries.async_update_entry
    listener_tasks: list[asyncio.Task[Any]] = []

    def _core_like_update(target: Any, **kwargs: Any) -> bool:
        before = (dict(target.options), dict(target.data))
        stored(target, **kwargs)
        changed = before != (dict(target.options), dict(target.data))
        if changed:
            for listener in listeners:
                listener_tasks.append(
                    asyncio.get_running_loop().create_task(listener(hass, target))
                )
        return changed

    monkeypatch.setattr(hass.config_entries, "async_update_entry", _core_like_update)

    coordinator = entry.runtime_data.coordinator
    coordinator.hass = hass
    coordinator.config_entry = entry
    for name in (
        "async_set_automatic_polling",
        "is_automatic_polling_enabled",
        "_get_automatic_polling_disabled_set",
    ):
        setattr(
            coordinator,
            name,
            types.MethodType(getattr(GoogleFindMyCoordinator, name), coordinator),
        )
    entity = switch.GoogleFindMyAutomaticPollingSwitch(
        coordinator,
        {"id": "dev-a", "name": "Keys"},
        None,
        subentry_key=TRACKER_SUBENTRY_KEY,
        subentry_identifier=_SUBENTRY_ID,
    )
    entity.hass = hass
    entity.async_write_ha_state = lambda: None

    await entity.async_turn_off()
    await asyncio.gather(*listener_tasks)
    assert entry.options[_OPT_DISABLED] == ["dev-a"]
    assert entity.is_on is False

    await entity.async_turn_on()
    await asyncio.gather(*listener_tasks)
    assert entry.options[_OPT_DISABLED] == []
    assert entity.is_on is True

    assert len(listener_tasks) == 2, "each change notifies the listener once"
    assert hass.config_entries.scheduled_reloads == [], (
        "toggling the switch must not reload the entry"
    )


# ---------------------------------------------------------------------------
# (d) the other per-tracker entities stay the same
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_buttons_unchanged_by_the_switch_platform(
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """(d) Button entities keep their number and unique ids (R6)."""
    loop = asyncio.get_running_loop()
    hass = _make_hass(loop)
    entry = _ConfigEntryStub()
    entry.subentries = {
        _SUBENTRY_ID: _tracker_subentry(tuple(TRACKER_FEATURE_PLATFORMS))
    }
    coordinator = _coordinator(stub_coordinator_factory, hass, entry, devices=_DEVICES)
    entry.runtime_data = SimpleNamespace(coordinator=coordinator)

    add_buttons, buttons, pending = _make_add_entities(hass, loop)
    await button.async_setup_entry(hass, entry, add_buttons)
    await asyncio.gather(*pending)
    add_switches, switches, pending_switches = _make_add_entities(hass, loop)
    await switch.async_setup_entry(hass, entry, add_switches)
    await asyncio.gather(*pending_switches)

    expected_buttons = {
        f"{DOMAIN}_{entry.entry_id}_{_SUBENTRY_ID}_{dev['id']}_{action}"
        for dev in _DEVICES
        for action in ("play_sound", "stop_sound", "locate_device")
    }
    assert {entity.unique_id for entity, _ in buttons} == expected_buttons
    assert len(buttons) == 3 * len(_DEVICES)
    assert len(switches) == len(_DEVICES)
    assert not expected_buttons & {entity.unique_id for entity, _ in switches}


# ---------------------------------------------------------------------------
# (e) ignored and hidden trackers get no switch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hidden_or_ignored_trackers_get_no_switch(
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """(e) Invisible devices (ignored ones are invisible) get no switch.

    The coordinator leaves ignored devices out of ``visible_device_ids``, so
    ``is_device_visible_in_subentry`` is the single gate for both.
    """
    _, entry, _, added, _ = await _setup_switches(
        stub_coordinator_factory, hidden=frozenset({"dev-b"})
    )
    assert {entity.unique_id for entity, _ in added} == {
        _switch_uid(entry.entry_id, "dev-a")
    }


# ---------------------------------------------------------------------------
# (f) recovery recreates a missing switch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_recreates_a_missing_switch(
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """(f) A switch missing from the entity registry is built and added again."""
    hass, entry, _, added, pending = await _setup_switches(
        stub_coordinator_factory, recovery=True
    )
    manager = entry.runtime_data.entity_recovery_manager
    assert manager.expected_unique_ids_for_platform("switch") == {
        _switch_uid(entry.entry_id, dev["id"]) for dev in _DEVICES
    }

    present = _switch_uid(entry.entry_id, "dev-a")
    registry = SimpleNamespace(
        entities={
            "switch.keys": SimpleNamespace(
                domain="switch",
                platform=DOMAIN,
                unique_id=present,
                config_entry_id=entry.entry_id,
            )
        },
        async_entries_for_config_entry=lambda entry_id: [
            row for row in registry.entities.values() if row.config_entry_id == entry_id
        ],
    )
    hass.data["entity_registry"] = registry
    before = len(added)

    await manager.async_recover_missing_entities()
    await asyncio.gather(*pending)

    recovered = [entity.unique_id for entity, _ in added[before:]]
    assert recovered == [_switch_uid(entry.entry_id, "dev-b")]


# ---------------------------------------------------------------------------
# (g) unloading with the switch platform works without trackers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unload_with_switch_platform_and_no_tracker(
    stub_coordinator_factory: Callable[..., type[Any]],
) -> None:
    """(g) Setup without trackers adds nothing; unload runs every callback."""
    from homeassistant.const import Platform

    from custom_components.googlefindmy import PLATFORMS

    assert Platform.SWITCH in PLATFORMS
    _, entry, _, added, _ = await _setup_switches(stub_coordinator_factory, devices=[])
    assert added == []
    assert entry._unload_callbacks
    for unload in entry._unload_callbacks:
        unload()


# ---------------------------------------------------------------------------
# (h) switching off does not change the protection mode sensor's availability
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uwt_sensor_availability_unchanged_by_switching_off(
    hass: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(h) Availability follows presence, not the switch (R-5)."""
    from tests.test_coordinator_automatic_polling import (
        _core_entry,
        _switch_coordinator,
    )
    from tests.test_uwt_mode_binary_sensor import _build_uwt_sensor

    entry = _core_entry(hass, {})
    coordinator = _switch_coordinator(monkeypatch, hass, entry)
    coordinator.last_update_success = True
    coordinator.is_device_visible_in_subentry = lambda _key, _dev: True
    coordinator._present_last_seen["dev-off"] = time.monotonic()
    sensor = _build_uwt_sensor(device_id="dev-off", coordinator=coordinator, hass=hass)

    before = sensor.available
    coordinator.async_set_automatic_polling("dev-off", False)
    assert coordinator.is_automatic_polling_enabled("dev-off") is False

    assert before is True
    assert sensor.available is before


def test_switch_entity_has_no_name_in_the_change_log() -> None:
    """The entity itself logs nothing; the coordinator writes the change line."""
    source = switch.__file__
    with open(source, encoding="utf-8") as handle:
        text = handle.read()
    body = text.split("class GoogleFindMyAutomaticPollingSwitch", 1)[1]
    assert "_LOGGER." not in body
