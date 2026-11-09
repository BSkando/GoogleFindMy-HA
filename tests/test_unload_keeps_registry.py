# tests/test_unload_keeps_registry.py
"""Unloading a loaded entry must keep its registry entries.

Home Assistant keeps entity and device registry entries across a reload or a
disable; removing a subentry or the entry is cleaned up by Core itself
(``ConfigEntries.async_remove_subentry``, entry removal). The integration may
only purge the registry entries of a subentry whose platforms never loaded.

Until now the coroutine branch of the parent unload purged every subentry on
every unload, so each reload sent all entities through ``deleted_entities``
and devices through ``deleted_devices``. These tests run against the real Home
Assistant registries:

* T1  entities survive the unload of a loaded entry with id, ``entity_id``
      and user settings, and no removal event is sent;
* T2  a subentry whose platforms never loaded is still purged (subentry
      unload and the legacy parent branch);
* T3  Core removes the entities and devices of a removed subentry without the
      integration (characterisation; catches a change in Core);
* T4  devices survive the unload without a detour through
      ``deleted_devices``.
"""

from __future__ import annotations

import inspect
import sys
from collections.abc import Iterator
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from tests.helpers.config_entries_stub import make_config_entry

_DOMAIN = "googlefindmy"
_PKG = "custom_components.googlefindmy"
_GIDS = ("AAAA", "BBBB")


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


class _World:
    """One entry with a service and a tracker subentry, as after a setup."""

    def __init__(self, hass: Any) -> None:
        from homeassistant.config_entries import ConfigSubentryData
        from homeassistant.helpers import area_registry as ar
        from homeassistant.helpers import device_registry as dr
        from homeassistant.helpers import entity_registry as er

        from custom_components.googlefindmy.const import service_device_identifier

        self.hass = hass
        self.entry = MockConfigEntry(
            domain=_DOMAIN,
            title="unload",
            data={},
            subentries_data=[
                ConfigSubentryData(
                    data={"group_key": "service", "subentry_type": "service"},
                    subentry_type="service",
                    title="svc",
                    unique_id="svc",
                ),
                ConfigSubentryData(
                    data={"group_key": "core_tracking", "subentry_type": "tracker"},
                    subentry_type="tracker",
                    title="trk",
                    unique_id="trk",
                ),
            ],
        )
        self.entry.add_to_hass(hass)
        self.eid = self.entry.entry_id
        self.svc = next(
            k for k, v in self.entry.subentries.items() if v.unique_id == "svc"
        )
        self.trk = next(
            k for k, v in self.entry.subentries.items() if v.unique_id == "trk"
        )
        self.dev_reg = dr.async_get(hass)
        self.ent_reg = er.async_get(hass)
        self.area = ar.async_get(hass).async_get_or_create("Hall")

        self.service_device = self.dev_reg.async_get_or_create(
            config_entry_id=self.eid,
            config_subentry_id=self.svc,
            identifiers={service_device_identifier(self.eid)},
            entry_type=dr.DeviceEntryType.SERVICE,
            name="service",
        )
        self.entities: dict[str, str] = {}
        self.devices: dict[str, Any] = {}
        for gid in _GIDS:
            device = self.dev_reg.async_get_or_create(
                config_entry_id=self.eid,
                config_subentry_id=self.trk,
                identifiers={(_DOMAIN, f"{self.eid}:{self.trk}:{gid}")},
                name=f"Tracker {gid}",
            )
            self.dev_reg.async_update_device(
                device.id, name_by_user=f"My {gid}", area_id=self.area.id
            )
            self.devices[gid] = self.dev_reg.async_get(device.id)
            entity = self.ent_reg.async_get_or_create(
                "device_tracker",
                _DOMAIN,
                f"{self.eid}:{self.trk}:{gid}",
                config_entry=self.entry,
                config_subentry_id=self.trk,
                device_id=device.id,
                suggested_object_id=f"tracker_{gid.lower()}",
            )
            self.ent_reg.async_update_entity(
                entity.entity_id, name=f"Keys {gid}", area_id=self.area.id
            )
            self.entities[gid] = entity.entity_id
        service_entity = self.ent_reg.async_get_or_create(
            "binary_sensor",
            _DOMAIN,
            f"{self.eid}:{self.svc}:polling",
            config_entry=self.entry,
            config_subentry_id=self.svc,
            device_id=self.service_device.id,
            suggested_object_id="polling",
        )
        self.entities["service"] = service_entity.entity_id

        self.entity_events: list[dict[str, Any]] = []
        self.device_events: list[dict[str, Any]] = []
        hass.bus.async_listen(
            er.EVENT_ENTITY_REGISTRY_UPDATED,
            lambda event: self.entity_events.append(dict(event.data)),
        )
        hass.bus.async_listen(
            dr.EVENT_DEVICE_REGISTRY_UPDATED,
            lambda event: self.device_events.append(dict(event.data)),
        )

    def snapshot_entities(self) -> dict[str, tuple[Any, ...]]:
        out: dict[str, tuple[Any, ...]] = {}
        for key, entity_id in self.entities.items():
            entry = self.ent_reg.async_get(entity_id)
            assert entry is not None, f"entity {entity_id} is gone"
            out[key] = (entry.id, entry.entity_id, entry.name, entry.area_id)
        return out

    def snapshot_devices(self) -> dict[str, tuple[Any, ...]]:
        out: dict[str, tuple[Any, ...]] = {}
        for gid, device in self.devices.items():
            current = self.dev_reg.async_get(device.id)
            assert current is not None, f"device {gid} is gone"
            out[gid] = (
                current.id,
                current.created_at,
                current.name_by_user,
                current.area_id,
            )
        return out

    def removals(self, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [event for event in events if event.get("action") == "remove"]

    def deleted_entities_of_entry(self) -> list[Any]:
        return [
            deleted
            for deleted in self.ent_reg.deleted_entities.values()
            if deleted.config_entry_id == self.eid
        ]

    def deleted_devices_of_entry(self) -> list[Any]:
        # Core 2026.10 reports ``deleted_devices`` as deprecated (an error in
        # tests); the store itself is ``_deleted_devices``. Older Core only has
        # the public name.
        store = getattr(self.dev_reg, "_deleted_devices", None)
        if store is None:
            store = self.dev_reg.deleted_devices
        return [deleted for deleted in store.values() if self._owned(deleted)]

    def _owned(self, deleted: Any) -> bool:
        # 2026.10 deprecates ``config_entries`` on deleted devices for the
        # single ``config_entry_id``.
        if hasattr(type(deleted), "config_entry_id") or hasattr(
            deleted, "config_entry_id"
        ):
            return bool(deleted.config_entry_id == self.eid)
        return self.eid in deleted.config_entries


def _quiet_teardown(monkeypatch: pytest.MonkeyPatch, mod: ModuleType) -> None:
    """Stub the parts of the parent unload that are not under test."""
    monkeypatch.setattr(mod, "_async_release_shared_fcm", AsyncMock(return_value=None))
    monkeypatch.setattr(mod, "_unregister_instance", lambda _entry_id: None)


async def _unload_loaded_entry(
    hass: Any, mod: ModuleType, w: _World, monkeypatch: pytest.MonkeyPatch
) -> bool:
    """Unload as Home Assistant does for a reload of a loaded entry."""
    # M-4: in Core the forward unload is a coroutine function, so the parent
    # unload always takes its coroutine branch.
    assert inspect.iscoroutinefunction(hass.config_entries.async_forward_entry_unload)
    monkeypatch.setattr(
        hass.config_entries, "async_unload_platforms", AsyncMock(return_value=True)
    )
    _quiet_teardown(monkeypatch, mod)
    result: bool = await mod.async_unload_entry(hass, w.entry)
    await hass.async_block_till_done()
    return result


@pytest.mark.asyncio
async def test_unload_keeps_entities_and_user_settings(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T1: a reload must not send entities through ``deleted_entities``."""
    w = _World(hass)
    before = w.snapshot_entities()
    w.entity_events.clear()

    assert await _unload_loaded_entry(hass, gfmy, w, monkeypatch) is True

    assert w.removals(w.entity_events) == [], "unload removed entities"
    assert w.deleted_entities_of_entry() == []
    assert w.snapshot_entities() == before


@pytest.mark.asyncio
async def test_unload_keeps_devices_without_detour(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T4: devices keep id and ``created_at``; none passes ``deleted_devices``."""
    w = _World(hass)
    before = w.snapshot_devices()
    service_before = w.service_device.id
    w.device_events.clear()

    assert await _unload_loaded_entry(hass, gfmy, w, monkeypatch) is True

    assert w.removals(w.device_events) == [], "unload removed devices"
    assert w.deleted_devices_of_entry() == []
    assert w.snapshot_devices() == before
    assert w.dev_reg.async_get(service_before) is not None


@pytest.mark.asyncio
async def test_never_loaded_subentry_is_still_purged(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T2: platforms that never loaded leave nothing to keep; purge stays."""
    w = _World(hass)
    # Subentry linkage fields are not part of the canonical factory signature;
    # they are what ``_async_unload_subentry`` reads to find the parent.
    child = make_config_entry(
        entry_id="child-entry",
        data={"subentry_type": "tracker", "group_key": "core_tracking"},
        parent_entry_id=w.eid,
        subentry_id=w.trk,
        subentry_type="tracker",
    )

    async def _never_loaded(*_args: Any, **_kwargs: Any) -> bool:
        raise ValueError("never loaded")

    monkeypatch.setattr(hass.config_entries, "async_unload_platforms", _never_loaded)

    assert await gfmy._async_unload_subentry(hass, child) is True

    for gid in _GIDS:
        assert w.ent_reg.async_get(w.entities[gid]) is None
        assert w.dev_reg.async_get(w.devices[gid].id) is None
    assert w.ent_reg.async_get(w.entities["service"]) is not None
    assert w.dev_reg.async_get(w.service_device.id) is not None


@pytest.mark.asyncio
async def test_legacy_parent_branch_purges_a_never_loaded_platform(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T2 (legacy parent branch): ``ValueError`` from a synchronous forward unload."""
    w = _World(hass)

    def _forward_unload(_entry: Any, _platform: str) -> bool:
        raise ValueError("never loaded")

    monkeypatch.setattr(
        hass.config_entries, "async_forward_entry_unload", _forward_unload
    )
    monkeypatch.setattr(
        hass.config_entries, "async_unload_platforms", AsyncMock(return_value=True)
    )
    _quiet_teardown(monkeypatch, gfmy)

    assert await gfmy.async_unload_entry(hass, w.entry) is True

    for key in (*_GIDS, "service"):
        assert w.ent_reg.async_get(w.entities[key]) is None
    for gid in _GIDS:
        assert w.dev_reg.async_get(w.devices[gid].id) is None


@pytest.mark.asyncio
async def test_core_removes_registry_entries_of_a_removed_subentry(
    hass: Any, gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T3: removing a subentry needs no purge by the integration."""
    w = _World(hass)
    purge = AsyncMock(return_value=(0, 0))
    monkeypatch.setattr(gfmy, "_async_purge_unloaded_subentry_registrations", purge)

    assert hass.config_entries.async_remove_subentry(w.entry, w.trk) is True
    await hass.async_block_till_done()

    purge.assert_not_awaited()
    for gid in _GIDS:
        assert w.ent_reg.async_get(w.entities[gid]) is None
        assert w.dev_reg.async_get(w.devices[gid].id) is None
    assert w.ent_reg.async_get(w.entities["service"]) is not None
    assert w.dev_reg.async_get(w.service_device.id) is not None


@pytest.mark.asyncio
async def test_service_purge_skips_subentries_without_identifier(
    gfmy: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    """T8: the ``rebuild`` purge only visits subentries it can name."""
    calls: list[tuple[str, Any]] = []

    async def _purge_one(
        _hass: Any, *, parent_entry_id: str, config_subentry_id: str, entry_type: Any
    ) -> tuple[int, int]:
        calls.append((config_subentry_id, entry_type))
        return 1, 2

    monkeypatch.setattr(
        gfmy, "_async_purge_unloaded_subentry_registrations", _purge_one
    )
    # ``coordinator`` and ``subentries`` stay outside the canonical factory
    # signature; the purge collects subentries from them.
    entry = make_config_entry(
        entry_id="parent",
        runtime_data=None,
        coordinator=None,
        subentries={
            "a": SimpleNamespace(
                subentry_id="sub-a", data={"subentry_type": "tracker"}
            ),
            "b": SimpleNamespace(subentry_id="", data={"subentry_type": "tracker"}),
            "c": SimpleNamespace(subentry_id="sub-c", data=None),
        },
    )

    totals = await gfmy._async_purge_entry_registrations(object(), entry)

    assert sorted(calls, key=lambda c: c[0]) == [("sub-a", "tracker"), ("sub-c", None)]
    assert totals == (2, 4)
