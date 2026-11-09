# tests/test_relink_uwt_mode_anchor.py
"""Relink must keep device-scoped binary sensors on their tracker device.

The UWT mode sensor is the only binary sensor whose ``unique_id`` carries a
Google device id. ``_async_relink_subentry_entities`` used to send every
``binary_sensor`` to the service device, so on each start all UWT sensors of a
subentry ended up on one device under one ``translation_key``.
``_async_self_heal_duplicate_entities`` then treated them as duplicates and
deleted all but one; the platform recreated the survivors' siblings on their
trackers, and the next start repeated the cycle.

Two notes on the test world, both measured while writing these tests:

* The tests run against the real Home Assistant device and entity registries.
  A stub registry hides the cycle, because the purge on unload and the
  duplicate cleanup only interact through real registry state.
* ``use_real_homeassistant_modules`` swaps ``homeassistant`` modules but does
  not reload an integration module that is already imported; that module then
  keeps talking to the stub registry. The ``gfmy`` fixture therefore imports
  the integration fresh and asserts that it uses the real registry module.

``test_relink_and_reload_do_not_delete_uwt_sensors`` covers the coupling with
the unload path: as long as the unload purges the registry, a reload hides the
cycle and only a cold start shows it; once the purge is gone, every reload
would show it. The test runs both worlds so neither can mask the other.
"""

from __future__ import annotations

import ast
import logging
import sys
from collections.abc import Iterator
from pathlib import Path
from types import FunctionType, ModuleType
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

_DOMAIN = "googlefindmy"
_PKG = "custom_components.googlefindmy"
_SERVICE_BINARY_SENSOR_KEYS = ("polling", "auth_status", "connectivity")


@pytest.fixture(autouse=True)
def _real_modules(use_real_homeassistant_modules: None) -> None:
    """Run every test in this module against the real Home Assistant modules."""


def _is_stale_ha_binding(value: object) -> bool:
    """Whether ``value`` comes from a Home Assistant module no longer loaded.

    A module counts if ``sys.modules`` no longer holds it under its name,
    whether the name is gone or now holds another module (R6-4). A Python
    function counts if the module it was defined in, read from its
    ``__globals__``, is gone or is now another object, also one without a
    ``__dict__`` (B23, R6-3; B26, R7-3); its ``__module__`` and
    ``__qualname__`` are not used, ``functools.wraps`` copies them from the
    wrapped function. This holds for a function at any nesting depth. A
    class or other callable at module level counts if its module is gone or
    now holds another object under its name (B19, R5-2). A nested class or
    other callable (``__qualname__`` with a dot) and other values are not
    judged.
    """
    if isinstance(value, ModuleType):
        return value.__name__.startswith("homeassistant") and (
            sys.modules.get(value.__name__) is not value
        )
    if isinstance(value, FunctionType):
        namespace = value.__globals__
        defined_in = namespace.get("__name__")
        if not isinstance(defined_in, str):
            defined_in = ""
        loaded = sys.modules.get(defined_in)
        return defined_in.startswith("homeassistant") and (
            getattr(loaded, "__dict__", None) is not namespace
        )
    if not (isinstance(value, type) or callable(value)):
        return False
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if not (isinstance(module, str) and module.startswith("homeassistant")):
        return False
    if not isinstance(qualname, str) or "." in qualname:
        return False
    current = sys.modules.get(module)
    return current is None or getattr(current, qualname, None) is not value


def _stale_bound_modules() -> set[str]:
    """Integration modules bound to a Home Assistant module no longer loaded.

    Such a module was imported against the real Home Assistant modules and
    outlived the swap back to the stubs (``tests/AGENTS.md``, fresh import of
    an integration module). It may hold the module itself or only a class or
    function from it.
    """
    stale: set[str] = set()
    for name, module in list(sys.modules.items()):
        if not (name == _PKG or name.startswith(f"{_PKG}.")):
            continue
        if not isinstance(module, ModuleType):
            continue
        if any(_is_stale_ha_binding(v) for v in list(vars(module).values())):
            stale.add(name)
    return stale


@pytest.fixture(autouse=True, scope="module")
def _no_real_bound_module_left() -> Iterator[None]:
    """B15 (R4-2): this file leaves no module bound to the real HA modules.

    Measured before the first and after the last test of the file, in the stub
    world on both sides. Only what this file leaves behind is reported: a
    module that was stale already before the file stays out of the count, so
    an earlier file's leak is not this file's failure.
    """
    before = _stale_bound_modules()
    yield
    leaked = sorted(_stale_bound_modules() - before)
    assert not leaked, f"modules bound to the real Home Assistant modules: {leaked}"


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
    # The fresh import rebinds the package attribute; restore it with the modules.
    monkeypatch.setattr(
        custom_components,
        "googlefindmy",
        getattr(custom_components, "googlefindmy", None),
    )
    import custom_components.googlefindmy as mod

    assert mod.er is real_er, "integration still bound to a stub entity registry"
    assert mod.dr is real_dr, "integration still bound to a stub device registry"
    yield mod
    # monkeypatch restores the modules deleted above. Submodules first imported
    # during the test are bound to the real Home Assistant modules and would leak
    # real timers into later tests, so they are dropped here.
    for name in _ours() - before:
        del sys.modules[name]


class _World:
    """One config entry with a service and a tracker subentry."""

    def __init__(self, hass: Any, gids: tuple[str, ...]) -> None:
        from homeassistant.config_entries import ConfigSubentryData
        from homeassistant.helpers import device_registry as dr
        from homeassistant.helpers import entity_registry as er

        self.hass = hass
        self.gids = gids
        self.entry = MockConfigEntry(
            domain=_DOMAIN,
            title="relink-uwt",
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
        subs = self.entry.subentries.items()
        self.svc = next(s for s, v in subs if v.unique_id == "svc")
        self.trk = next(s for s, v in subs if v.unique_id == "trk")
        self.eid = self.entry.entry_id
        self.dev_reg = dr.async_get(hass)
        self.ent_reg = er.async_get(hass)
        self.service_dev: Any = None
        self.trackers: dict[str, Any] = {}
        self.ensure_devices()

    def ensure_devices(self) -> None:
        """Create (or restore) the devices the platforms would announce."""
        from homeassistant.helpers import device_registry as dr

        from custom_components.googlefindmy.const import service_device_identifier

        self.service_dev = self.dev_reg.async_get_or_create(
            config_entry_id=self.eid,
            config_subentry_id=self.svc,
            identifiers={service_device_identifier(self.eid)},
            entry_type=dr.DeviceEntryType.SERVICE,
            name="service",
        )
        for gid in self.gids:
            self.trackers[gid] = self.dev_reg.async_get_or_create(
                config_entry_id=self.eid,
                config_subentry_id=self.trk,
                identifiers={(_DOMAIN, f"{self.eid}:{self.trk}:{gid}")},
                name=f"tracker {gid}",
            )

    def uwt_uid(self, gid: str) -> str:
        """Return the UWT unique_id in the shape ``binary_sensor.py`` builds."""
        return f"{_DOMAIN}_{self.eid}_{self.trk}_{gid}_uwt_mode"

    def add_uwt(self, gid: str, device_id: str | None) -> str:
        entry = self.ent_reg.async_get_or_create(
            "binary_sensor",
            _DOMAIN,
            self.uwt_uid(gid),
            config_entry=self.entry,
            config_subentry_id=self.trk,
            device_id=device_id,
            translation_key="uwt_mode",
            suggested_object_id=f"{gid}_uwt",
        )
        return entry.entity_id

    def add_service_binary(self, key: str, device_id: str) -> str:
        entry = self.ent_reg.async_get_or_create(
            "binary_sensor",
            _DOMAIN,
            f"{self.eid}:{self.svc}:{key}",
            config_entry=self.entry,
            config_subentry_id=self.svc,
            device_id=device_id,
            translation_key=key,
            suggested_object_id=key,
        )
        return entry.entity_id

    def platforms(self) -> None:
        """Emulate the platforms after setup: devices plus UWT on its tracker."""
        self.ensure_devices()
        for gid in self.gids:
            self.add_uwt(gid, self.trackers[gid].id)
        self.add_service_binary("polling", self.service_dev.id)

    def device_of(self, entity_id: str) -> str | None:
        entry = self.ent_reg.async_get(entity_id)
        assert entry is not None, f"{entity_id} is no longer in the registry"
        return entry.device_id

    def deleted_own(self) -> list[str]:
        """Unique ids of this entry in the registry's trash.

        Other tests may leave theirs there.
        """
        return sorted(
            key[2]
            for key, deleted in self.ent_reg.deleted_entities.items()
            if deleted.config_entry_id == self.eid
        )

    def uwt_entries(self) -> list[Any]:
        return [
            e
            for e in self.ent_reg.entities.values()
            if e.config_entry_id == self.eid and e.unique_id.endswith("_uwt_mode")
        ]


def _duplicate_log_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if "duplicate Google Find My entities" in r.getMessage()
    ]


async def _setup_chain(hass: Any, mod: ModuleType, world: _World) -> None:
    """The registry steps of ``async_setup_entry`` in code order, then platforms."""
    await mod._async_relink_subentry_entities(hass, world.entry)
    await mod._async_self_heal_duplicate_entities(hass, world.entry)
    await hass.async_block_till_done()
    world.platforms()


@pytest.mark.asyncio
@pytest.mark.parametrize("start", ["service", "tracker"])
async def test_relink_anchors_uwt_to_its_tracker(
    hass: Any, gfmy: ModuleType, caplog: pytest.LogCaptureFixture, start: str
) -> None:
    """T5: UWT sensors end on their own tracker and none is deleted."""
    caplog.set_level(logging.DEBUG, logger=_PKG)
    world = _World(hass, ("gidA", "gidB"))
    ids = {
        gid: world.add_uwt(
            gid,
            world.service_dev.id if start == "service" else world.trackers[gid].id,
        )
        for gid in world.gids
    }

    await gfmy._async_relink_subentry_entities(hass, world.entry)
    await gfmy._async_self_heal_duplicate_entities(hass, world.entry)
    await hass.async_block_till_done()

    for gid, entity_id in ids.items():
        assert world.device_of(entity_id) == world.trackers[gid].id, gid
    assert world.deleted_own() == []
    assert _duplicate_log_lines(caplog) == []


@pytest.mark.asyncio
async def test_relink_leaves_uwt_alone_when_tracker_is_missing(
    hass: Any, gfmy: ModuleType
) -> None:
    """T5b: no tracker device for the Google id -> the entry is not moved."""
    world = _World(hass, ("gidA",))
    orphan = world.add_uwt("gidZ", None)

    await gfmy._async_relink_subentry_entities(hass, world.entry)
    await hass.async_block_till_done()

    assert world.device_of(orphan) is None


@pytest.mark.asyncio
async def test_relink_leaves_uwt_alone_without_google_id(
    hass: Any, gfmy: ModuleType
) -> None:
    """T5c: a UWT unique_id without a readable Google id is not moved either."""
    world = _World(hass, ("gidA",))
    entry = world.ent_reg.async_get_or_create(
        "binary_sensor",
        _DOMAIN,
        f"{_DOMAIN}_{world.eid}_{world.trk}__uwt_mode",
        config_entry=world.entry,
        config_subentry_id=world.trk,
        device_id=world.trackers["gidA"].id,
        translation_key="uwt_mode",
        suggested_object_id="blank_uwt",
    )

    await gfmy._async_relink_subentry_entities(hass, world.entry)
    await hass.async_block_till_done()

    assert world.device_of(entry.entity_id) == world.trackers["gidA"].id


@pytest.mark.asyncio
@pytest.mark.parametrize("key", _SERVICE_BINARY_SENSOR_KEYS)
async def test_relink_keeps_service_binary_sensors_on_service_device(
    hass: Any, gfmy: ModuleType, key: str
) -> None:
    """T6: service-scoped binary sensors still go to the service device."""
    world = _World(hass, ("gidA",))
    entity_id = world.add_service_binary(key, world.trackers["gidA"].id)

    await gfmy._async_relink_subentry_entities(hass, world.entry)
    await hass.async_block_till_done()

    assert world.device_of(entity_id) == world.service_dev.id


_UID_MARK = "GIDMARK"


def _package_subclasses(package: ModuleType, base: type) -> list[type]:
    """Import the modules of ``package``; return its subclasses of ``base``.

    Imported is each ``.py`` file that ``Path.rglob`` finds below an entry of
    the package's ``__path__``, entry by entry (B24, R6-6), also below a
    directory without ``__init__.py`` (a namespace package, B18, R5-1). A
    file whose module name an earlier entry already supplies is hidden by
    that module, as in any import, and is not imported itself.
    ``rglob`` does not follow a directory symlink, so a module there is not
    imported (``symlinked_dir``, B21, R6-1). Transitive through
    ``__subclasses__``, so intermediate classes, aliases, imports and mixins
    are one class graph. Only classes defined in the package count; a test
    elsewhere may subclass the entities.
    """
    import importlib
    import pathlib

    for entry in package.__path__:
        root = pathlib.Path(entry)
        for path in sorted(root.rglob("*.py")):
            parts = path.relative_to(root).with_suffix("").parts
            if parts[-1] == "__init__":
                parts = parts[:-1]
            if parts:
                importlib.import_module(".".join((package.__name__, *parts)))
    prefix = f"{package.__name__}."
    found: list[type] = []
    stack = [base]
    while stack:
        for sub in type.__subclasses__(stack.pop()):
            if sub in found:
                continue
            found.append(sub)
            stack.append(sub)
    return [
        cls
        for cls in found
        if cls.__module__ == package.__name__ or cls.__module__.startswith(prefix)
    ]


def _device_scoped_binary_sensor_suffixes(
    package: ModuleType | None = None,
    base: type | None = None,
    binary_base: type | None = None,
) -> dict[str, str]:
    """Map each device-scoped binary sensor class to its measured unique_id tail.

    Measured at runtime, not read from the source (review findings F3, R2-7,
    R3-1). Scope: the classes that exist once ``_package_subclasses`` has
    imported the modules it finds, that report a module of the package, and
    that subclass ``base`` and ``binary_base``, found transitively through
    ``__subclasses__``. The
    forms covered and pinned by B12 are a direct subclass, an intermediate
    class, an alias, an import, an inherited class, a class in a submodule
    nobody imports, an id built with ``join_parts`` and one built by a mixin
    behind the base. The tail is measured on the id the class builds while it
    is constructed with a stand-in coordinator and a device whose id is a
    marker; the tail is what follows the marker.

    Within that scope an abstract class is skipped, a class without a unique
    id after construction counts as an intermediate class and is skipped, and
    a class that cannot be constructed, or whose id does not carry the device
    id, fails the guard. The skip also hides real device-scoped classes; among
    them (B14 pins these, R4-1, R6-2): ``late_id``, a leaf class that sets its
    id only after construction, and ``missing_device_data``, a class that sets
    its id only from device data the stand-in device lacks. Outside the scope
    the guard sees nothing and reports nothing, for example
    ``foreign_module``, a class whose ``__module__`` names another module.
    Classes made by calling a function are covered only as far as
    ``_factory_class_sites`` reports their sites. The forms named here are
    examples; the set of forms the guard cannot see is open, so a green run
    shows the covered forms, not that no other form exists.
    """
    import importlib
    import inspect
    from unittest.mock import MagicMock

    if package is None:
        package = importlib.import_module(_PKG)
    if base is None:
        base = importlib.import_module(f"{_PKG}.entity").GoogleFindMyDeviceEntity
    if binary_base is None:
        # The class the integration itself binds; the suite's conftest may
        # stand in for core's.
        binary_base = importlib.import_module(
            f"{_PKG}.binary_sensor"
        ).BinarySensorEntity
    found: dict[str, str] = {}
    for cls in _package_subclasses(package, base):
        if not issubclass(cls, binary_base):
            continue
        if inspect.isabstract(cls):
            continue
        try:
            entity = cls(
                MagicMock(),
                {"id": _UID_MARK, "name": "guard"},
                subentry_key="guard_key",
                subentry_identifier="guard_sub",
            )
        except Exception as err:  # noqa: BLE001 - reported as a guard failure
            pytest.fail(f"{cls.__qualname__}: guard cannot construct it ({err!r})")
        unique_id = getattr(entity, "unique_id", None)
        if unique_id is None:
            continue
        assert isinstance(unique_id, str) and _UID_MARK in unique_id, (
            f"{cls.__qualname__}: unique_id {unique_id!r} does not carry the device id"
        )
        assert cls.__name__ not in found, f"two classes named {cls.__name__}"
        found[cls.__name__] = unique_id.rsplit(_UID_MARK, 1)[1]
    return found


def _factory_class_sites(package: ModuleType | None = None) -> list[str]:
    """Places in ``package`` that could create a class only when called.

    Two forms, named by file and bases (no line number, so an edit elsewhere
    does not move them): a ``class`` statement with a base inside a function,
    and a call of the bare name ``type`` with three arguments. The runtime
    guard cannot see such a class; the sites of today are pinned, and a new
    one fails T6b until its suffix is judged and the pin updated. Every entry
    of ``__path__`` is read (B24, R6-6). Other ways to make a class are not
    reported; among them (B14 pins these, R4-1): ``new_class``
    (``types.new_class``), ``type_alias`` (``type`` bound to another name),
    ``builtins_type`` (``builtins.type``), ``metatype_call`` (calling the
    metaclass of a base, ``type(Base)(...)``), ``type_star_args`` (``type``
    called with unpacked arguments, ``type(*args)``).
    """
    import importlib
    import pathlib

    if package is None:
        package = importlib.import_module(_PKG)
    sites: set[str] = set()
    for entry in package.__path__:
        root = pathlib.Path(entry)
        for path in sorted(root.rglob("*.py")):
            sites.update(_factory_sites_in(path, path.relative_to(root).as_posix()))
    return sorted(sites)


def _factory_sites_in(path: Any, where: str) -> set[str]:
    """The factory sites of one file, named as ``_factory_class_sites`` says."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    sites: set[str] = set()
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(func):
            if isinstance(node, ast.ClassDef) and node.bases:
                bases = ", ".join(ast.unparse(b) for b in node.bases)
                sites.add(f"{where} class {node.name}({bases})")
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "type"
            and len(node.args) == 3
        ):
            sites.add(f"{where} type({ast.unparse(node.args[1])})")
    return sites


#: Today's three-argument ``type()`` calls in the package: fallbacks for older
#: cores and stand-ins, none of them an entity (read 2026-10-31).
_KNOWN_FACTORY_SITES = sorted(
    {
        "__init__.py type((_FallbackHAError,))",
        "__init__.py type((cast('type[Exception]', HomeAssistantError),))",
        "__init__.py type((object,))",
        "config_flow.py type((HomeAssistantError,))",
        "main.py type((Exception,))",
        "main.py type(())",
        "main.py type((_Dummy,))",
        "main.py type((self._HomeAssistantError,))",
    }
)


# B12 (R3-1): a fixture package with the covered forms a device-scoped class
# can take; the blind ones are in ``_FX_BLIND``. ``ctor`` is the constructor
# signature of the real entities.
_FX_PKG = "fxguard"
_FX_FILES = {
    "__init__.py": "",
    "base.py": """
class Entity:
    def __init__(self, coordinator, device, *, subentry_key, subentry_identifier):
        self.device = device
        self.unique_id = None
        post = getattr(self, "_post_init", None)
        if post is not None:
            post()

    @staticmethod
    def join_parts(*parts, separator=":"):
        return separator.join(p for p in parts if isinstance(p, str) and p)

    def build_unique_id(self, *parts, separator=":"):
        return self.join_parts(*parts, separator=separator)


class DeviceEntity(Entity):
    pass


class BinarySensor:
    pass
""",
    "other.py": """
from fxguard.base import BinarySensor, DeviceEntity


class Imported(DeviceEntity, BinarySensor):
    def _post_init(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_d", separator="_")
""",
    "main.py": """
from fxguard import base
from fxguard.base import BinarySensor, DeviceEntity
from fxguard.other import Imported

B = DeviceEntity


class Direct(DeviceEntity, BinarySensor):
    def _post_init(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_a", separator="_")


class Mid(base.DeviceEntity):
    pass


class Indirect(Mid, BinarySensor):
    def _post_init(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_b", separator="_")


class ViaAlias(B, BinarySensor):
    def _post_init(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_c", separator="_")


class Inherits(Direct):
    pass


class JoinParts(DeviceEntity, BinarySensor):
    def _post_init(self):
        self.unique_id = self.join_parts("d", self.device["id"], "e", separator="_")


class _UidMixin:
    def _post_init(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_f", separator="_")


class MixinBehind(DeviceEntity, _UidMixin, BinarySensor):
    pass


class NoId(DeviceEntity, BinarySensor):
    pass
""",
    "sub/__init__.py": "",
    "sub/hidden.py": """
from fxguard.base import BinarySensor, DeviceEntity


class Hidden(DeviceEntity, BinarySensor):
    def _post_init(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_g", separator="_")
""",
    "factory.py": """
from fxguard.base import BinarySensor, DeviceEntity


def make():
    class Made(DeviceEntity, BinarySensor):
        pass

    return Made


def make_dynamic():
    return type("Dyn", (DeviceEntity, BinarySensor), {})
""",
}
_FX_EXPECTED = {
    "Direct": "_a",
    "Indirect": "_b",
    "ViaAlias": "_c",
    "Imported": "_d",
    "Inherits": "_a",
    "JoinParts": "_e",
    "MixinBehind": "_f",
    "Hidden": "_g",
}
_FX_FACTORY_SITES = [
    "factory.py class Made(DeviceEntity, BinarySensor)",
    "factory.py type((DeviceEntity, BinarySensor))",
]


@pytest.fixture
def fx_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """Write the fixture package; yield a loader that adds extra modules."""
    import importlib

    def _load(extra: dict[str, str] | None = None) -> ModuleType:
        for rel, source in {**_FX_FILES, **(extra or {})}.items():
            path = tmp_path / _FX_PKG / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
        return importlib.import_module(_FX_PKG)

    monkeypatch.syspath_prepend(str(tmp_path))
    yield _load
    # Plain pop: a ``monkeypatch.delitem`` here would be undone afterwards and
    # hand the next test this test's package.
    for name in [n for n in sys.modules if n == _FX_PKG or n.startswith(f"{_FX_PKG}.")]:
        sys.modules.pop(name, None)


def _fx_bases() -> tuple[type, type]:
    import importlib

    base = importlib.import_module(f"{_FX_PKG}.base")
    return base.DeviceEntity, base.BinarySensor


def test_suffix_guard_sees_the_covered_inheritance_paths(fx_package: Any) -> None:
    """B12 (R3-1): the guard finds each covered form with its real tail.

    Direct, through an intermediate class, an alias, an import, inherited, a
    submodule nobody imports, an id built with ``join_parts`` and one built
    by a mixin behind the base; a class without an id is skipped, and the two
    factory forms are reported where they can be made.
    """
    package = fx_package()
    found = _device_scoped_binary_sensor_suffixes(package, *_fx_bases())
    assert found == _FX_EXPECTED
    assert _factory_class_sites(package) == _FX_FACTORY_SITES


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (
            "class Broken(DeviceEntity, BinarySensor):\n"
            "    def __init__(self, *args, **kwargs):\n"
            "        raise RuntimeError('needs a real coordinator')\n",
            "guard cannot construct it",
        ),
        (
            "class Unmarked(DeviceEntity, BinarySensor):\n"
            "    def _post_init(self):\n"
            "        self.unique_id = 'fixed_tail'\n",
            "does not carry the device id",
        ),
    ],
    ids=["cannot_construct", "id_without_device_id"],
)
def test_suffix_guard_fails_loudly(fx_package: Any, source: str, message: str) -> None:
    """B12 (R3-1): within its scope, a class the guard cannot judge fails it."""
    header = "from fxguard.base import BinarySensor, DeviceEntity\n\n\n"
    package = fx_package({"loud.py": header + source})
    with pytest.raises((AssertionError, pytest.fail.Exception), match=message):
        _device_scoped_binary_sensor_suffixes(package, *_fx_bases())


#: B14 (R4-1): forms the guard is blind to, measured 2026-10-31 and, for
#: ``symlinked_dir`` and ``missing_device_data``, 2026-11-01 (R6-1, R6-2).
#: Examples, not a closed list. Each label must appear in the guard's
#: docstrings, and the pins below fail if the guard starts or stops seeing one
#: of them.
_GUARD_BLIND_SPOTS = (
    "late_id",
    "missing_device_data",
    "foreign_module",
    "new_class",
    "type_alias",
    "builtins_type",
    "metatype_call",
    "type_star_args",
    "symlinked_dir",
)
_FX_BLIND = """
import builtins
import types

from fxguard.base import BinarySensor, DeviceEntity


class LateId(DeviceEntity, BinarySensor):
    def late_hook(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_h", separator="_")


class NeedsSerial(DeviceEntity, BinarySensor):
    def _post_init(self):
        if "serial" in self.device:
            self.unique_id = self.build_unique_id(
                "d", f"{self.device['id']}_s", separator="_"
            )


class ForeignModule(DeviceEntity, BinarySensor):
    __module__ = "elsewhere"

    def _post_init(self):
        self.unique_id = self.build_unique_id("d", f"{self.device['id']}_i", separator="_")


def make_new_class():
    return types.new_class("ViaNewClass", (DeviceEntity, BinarySensor))


def make_type_alias():
    t = type
    return t("ViaTypeAlias", (DeviceEntity, BinarySensor), {})


def make_builtins_type():
    return builtins.type("ViaBuiltins", (DeviceEntity, BinarySensor), {})


def make_metatype_call():
    return type(DeviceEntity)("ViaMeta", (DeviceEntity, BinarySensor), {})


def make_type_star_args():
    args = ("ViaStar", (DeviceEntity, BinarySensor), {})
    return type(*args)
"""


def test_suffix_guard_declared_blind_spots(fx_package: Any) -> None:
    """B14 (R4-1, R6-2): the guard is blind to each declared form.

    The module holds one class or factory per label of ``_GUARD_BLIND_SPOTS``
    except ``symlinked_dir`` (B21). The guard finds none of them, and each one
    is real: the late id carries the device id once set, the serial id once
    the device has a serial, the foreign class is device-scoped, and every
    factory makes a device-scoped class. A guard that learns one of them
    fails here as well; its docstring and this pin then move together. The
    labels are examples; a green run does not show that no other form exists.
    """
    package = fx_package({"blind.py": _FX_BLIND})
    found = _device_scoped_binary_sensor_suffixes(package, *_fx_bases())
    assert found == _FX_EXPECTED
    assert _factory_class_sites(package) == _FX_FACTORY_SITES
    import importlib
    from unittest.mock import MagicMock

    blind = importlib.import_module(f"{_FX_PKG}.blind")
    device_base, binary_base = _fx_bases()
    late = blind.LateId(
        MagicMock(), {"id": _UID_MARK}, subentry_key="k", subentry_identifier="s"
    )
    assert late.unique_id is None
    late.late_hook()
    assert late.unique_id.endswith(f"{_UID_MARK}_h")
    serial = blind.NeedsSerial(
        MagicMock(),
        {"id": _UID_MARK, "serial": "s1"},
        subentry_key="k",
        subentry_identifier="s",
    )
    assert serial.unique_id.endswith(f"{_UID_MARK}_s")
    assert issubclass(blind.ForeignModule, device_base)
    for make in (
        blind.make_new_class,
        blind.make_type_alias,
        blind.make_builtins_type,
        blind.make_metatype_call,
        blind.make_type_star_args,
    ):
        made = make()
        assert issubclass(made, device_base) and issubclass(made, binary_base)


def test_suffix_guard_docstrings_name_their_blind_spots() -> None:
    """B14 (R4-1): the guard's own text names each form it cannot see.

    Goes away with ``_GUARD_BLIND_SPOTS``: once the guard declares no blind
    form, there is nothing left for its text to name.
    """
    text = " ".join(
        f.__doc__ or ""
        for f in (
            _package_subclasses,
            _device_scoped_binary_sensor_suffixes,
            _factory_class_sites,
        )
    )
    missing = [label for label in _GUARD_BLIND_SPOTS if f"``{label}``" not in text]
    assert not missing, f"guard docstrings do not name: {missing}"


def test_suffix_guard_sees_a_namespace_subdirectory(
    fx_package: Any, tmp_path: Path
) -> None:
    """B18 (R5-1): a class below directories without ``__init__.py`` is found.

    Python imports such a directory as a namespace package, so the class
    exists once its module is imported and falls within the guard's scope.
    """
    leaf = (
        "from fxguard.base import BinarySensor, DeviceEntity\n\n\n"
        "class InNamespace(DeviceEntity, BinarySensor):\n"
        "    def _post_init(self):\n"
        "        self.unique_id = self.build_unique_id(\n"
        "            'd', f\"{self.device['id']}_j\", separator='_'\n"
        "        )\n"
    )
    package = fx_package({"ns/deep/leaf.py": leaf})
    assert not (tmp_path / _FX_PKG / "ns" / "__init__.py").exists()
    assert not (tmp_path / _FX_PKG / "ns" / "deep" / "__init__.py").exists()
    found = _device_scoped_binary_sensor_suffixes(package, *_fx_bases())
    assert found == {**_FX_EXPECTED, "InNamespace": "_j"}


_FX_HEADER = "from fxguard.base import BinarySensor, DeviceEntity\n\n\n"


def _fx_class(name: str, tail: str) -> str:
    return (
        f"class {name}(DeviceEntity, BinarySensor):\n"
        "    def _post_init(self):\n"
        "        self.unique_id = self.build_unique_id(\n"
        f"            'd', f\"{{self.device['id']}}{tail}\", separator='_'\n"
        "        )\n"
    )


def test_suffix_guard_blind_to_a_symlinked_directory(
    fx_package: Any, tmp_path: Path
) -> None:
    """B21 (R6-1): a module below a symlinked directory stays unseen.

    Python imports it through the link and its class is device-scoped, but
    ``Path.rglob`` does not follow the link, so the guard does not import it
    (``symlinked_dir``). The guard runs first; the import afterwards shows the
    class is real.
    """
    import importlib

    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "leaf.py").write_text(
        _FX_HEADER + _fx_class("ViaLink", "_l"), encoding="utf-8"
    )
    (tmp_path / _FX_PKG).mkdir()
    (tmp_path / _FX_PKG / "linked").symlink_to(outside, target_is_directory=True)
    package = fx_package()
    found = _device_scoped_binary_sensor_suffixes(package, *_fx_bases())
    assert found == _FX_EXPECTED
    leaf = importlib.import_module(f"{_FX_PKG}.linked.leaf")
    device_base, binary_base = _fx_bases()
    assert issubclass(leaf.ViaLink, device_base)
    assert issubclass(leaf.ViaLink, binary_base)


def test_suffix_guard_reads_every_path_entry(fx_package: Any, tmp_path: Path) -> None:
    """B24 (R6-6): a class and a factory site in a second ``__path__`` entry count."""
    second = tmp_path / "second"
    second.mkdir()
    (second / "extra.py").write_text(
        _FX_HEADER + _fx_class("InSecondPath", "_k") + "\n\ndef make_extra():\n"
        "    class MadeExtra(DeviceEntity, BinarySensor):\n"
        "        pass\n\n"
        "    return MadeExtra\n",
        encoding="utf-8",
    )
    package = fx_package()
    package.__path__.append(str(second))
    found = _device_scoped_binary_sensor_suffixes(package, *_fx_bases())
    assert found == {**_FX_EXPECTED, "InSecondPath": "_k"}
    assert _factory_class_sites(package) == sorted(
        [*_FX_FACTORY_SITES, "extra.py class MadeExtra(DeviceEntity, BinarySensor)"]
    )


def _module_from_source(tmp_path: Path, name: str, source: str) -> ModuleType:
    """Load ``source`` as a module named ``name``, not entered in ``sys.modules``.

    The source is written to a file below ``tmp_path`` and loaded through
    ``importlib.util``; each call returns a new module object, so loading the
    same name twice gives an earlier and a later copy (B25, R7-7).
    """
    import importlib.util

    path = tmp_path / f"{name.replace('.', '_')}.py"
    path.write_text(source, encoding="utf-8")
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_leak_check_sees_bound_classes_and_functions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """B19 (R5-2): a module holding only a class or a function counts too.

    Each holder module binds one object from a Home Assistant module: one
    that is no longer loaded (class, function), one that is loaded but now
    holds another object under that name, and one that is loaded and holds
    the same object. Only the last is not stale.
    """

    def _owned(obj: Any, module: str, name: str) -> Any:
        obj.__module__ = module
        obj.__qualname__ = name
        obj.__name__ = name
        return obj

    gone_cls = _owned(type("GoneCls", (), {}), "homeassistant.fx_gone", "GoneCls")
    gone = _module_from_source(
        tmp_path, "homeassistant.fx_gone", "def gone_fn():\n    pass\n"
    )
    gone_fn = vars(gone)["gone_fn"]
    live_cls = _owned(type("LiveCls", (), {}), "homeassistant.fx_live", "LiveCls")
    old_cls = _owned(type("Swapped", (), {}), "homeassistant.fx_live", "Swapped")
    live = ModuleType("homeassistant.fx_live")
    vars(live)["LiveCls"] = live_cls
    vars(live)["Swapped"] = _owned(
        type("Swapped", (), {}), "homeassistant.fx_live", "Swapped"
    )
    monkeypatch.setitem(sys.modules, "homeassistant.fx_live", live)
    monkeypatch.delitem(sys.modules, "homeassistant.fx_gone", raising=False)
    holders = {
        "cls": gone_cls,
        "fn": gone_fn,
        "swapped": old_cls,
        "live": live_cls,
        "module": gone,
    }
    for key, value in holders.items():
        holder = ModuleType(f"{_PKG}._fx_holds_{key}")
        vars(holder)["held"] = value
        monkeypatch.setitem(sys.modules, holder.__name__, holder)
    stale = {n for n in _stale_bound_modules() if n.startswith(f"{_PKG}._fx_holds_")}
    assert stale == {
        f"{_PKG}._fx_holds_cls",
        f"{_PKG}._fx_holds_fn",
        f"{_PKG}._fx_holds_swapped",
        f"{_PKG}._fx_holds_module",
    }


def test_leak_check_judges_a_function_by_its_namespace(tmp_path: Path) -> None:
    """B23 (R6-3): a function belongs to the module it was defined in.

    ``functools.wraps`` copies ``__module__`` and ``__qualname__`` from the
    wrapped function, so a wrapper in a loaded module names an object that is
    not itself; it is not stale. A function from an earlier copy of a module
    that has been imported again since is stale.
    """
    source = (
        "import functools\n\n\n"
        "def target():\n    pass\n\n\n"
        "@functools.wraps(target)\n"
        "def wrapper():\n    return target()\n"
    )
    live = _module_from_source(tmp_path, "homeassistant.fx_wraps", source)
    old = _module_from_source(tmp_path, "homeassistant.fx_again", source)
    again = _module_from_source(tmp_path, "homeassistant.fx_again", source)
    assert old is not again
    with pytest.MonkeyPatch.context() as patch:
        patch.setitem(sys.modules, live.__name__, live)
        patch.setitem(sys.modules, again.__name__, again)
        assert vars(live)["wrapper"].__qualname__ == "target"
        assert not _is_stale_ha_binding(vars(live)["wrapper"])
        assert not _is_stale_ha_binding(vars(live)["target"])
        assert not _is_stale_ha_binding(vars(again)["wrapper"])
        assert _is_stale_ha_binding(vars(old)["wrapper"])
        assert _is_stale_ha_binding(vars(old)["target"])


def test_leak_check_survives_an_object_without_dict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """B26 (R7-3): an object without ``__dict__`` under the name is another object.

    ``sys.modules`` may hold any object; one without a namespace is not the
    module a function was defined in, so the function counts as stale and
    the check does not raise.
    """
    module = _module_from_source(
        tmp_path, "homeassistant.fx_nodict", "def fn():\n    pass\n"
    )
    monkeypatch.setitem(sys.modules, module.__name__, 0)
    assert _is_stale_ha_binding(vars(module)["fn"])


def test_relink_knows_the_device_scoped_binary_sensors(gfmy: ModuleType) -> None:
    """T6b: a new device-scoped binary sensor must be registered with the relink.

    Within the guard's scope (see ``_device_scoped_binary_sensor_suffixes``).
    A class made by calling a function is invisible to the runtime guard; the
    sites the guard can report are pinned, and a new one fails here.
    """
    scoped = _device_scoped_binary_sensor_suffixes()
    assert scoped, "guard found no device-scoped binary sensor; it is blind"
    known = set(getattr(gfmy, "_DEVICE_SCOPED_BINARY_SENSOR_SUFFIXES", ()))
    missing = {cls: sfx for cls, sfx in scoped.items() if sfx not in known}
    assert not missing, f"relink does not know these suffixes: {missing}"
    assert _factory_class_sites() == _KNOWN_FACTORY_SITES


@pytest.mark.usefixtures("gfmy")
def test_suffix_guard_parser_sees_uwt() -> None:
    """Self-test of the T6b guard against the class it must find today.

    Through ``gfmy``: the guard imports every module of the package, and the
    fixture drops the ones bound to the real Home Assistant modules (R4-2).
    """
    assert (
        _device_scoped_binary_sensor_suffixes().get("GoogleFindMyUWTModeSensor")
        == "_uwt_mode"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("purge_on_unload", [True, False], ids=["purge", "no_purge"])
async def test_relink_and_reload_do_not_delete_uwt_sensors(
    hass: Any,
    gfmy: ModuleType,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    purge_on_unload: bool,
) -> None:
    """T7: two starts and a reload delete no UWT sensor, with or without purge."""
    from homeassistant.helpers import entity_registry as er

    caplog.set_level(logging.DEBUG, logger=_PKG)
    world = _World(hass, ("gidA", "gidB", "gidC"))
    world.platforms()

    removed: list[str] = []
    counting = False

    def _on_registry_update(event: Any) -> None:
        if counting and event.data.get("action") == "remove":
            removed.append(str(event.data.get("entity_id")))

    hass.bus.async_listen(er.EVENT_ENTITY_REGISTRY_UPDATED, _on_registry_update)

    if not purge_on_unload:

        async def _no_purge(*_args: Any, **_kwargs: Any) -> tuple[int, int]:
            return (0, 0)

        monkeypatch.setattr(
            gfmy, "_async_purge_unloaded_subentry_registrations", _no_purge
        )
    monkeypatch.setattr(
        hass.config_entries, "async_unload_platforms", AsyncMock(return_value=True)
    )

    # Only the setup steps are counted: what the unload purge removes is the
    # subject of the unload fix, not of the relink.
    counting = True
    await _setup_chain(hass, gfmy, world)  # first start
    await _setup_chain(hass, gfmy, world)  # second start (cold start)
    counting = False
    await gfmy._async_unload_parent_entry(hass, world.entry)
    counting = True
    await _setup_chain(hass, gfmy, world)  # setup after the reload
    await hass.async_block_till_done()
    counting = False

    assert _duplicate_log_lines(caplog) == []
    assert [r for r in removed if r.endswith("_uwt")] == []
    uwt = world.uwt_entries()
    assert len(uwt) == len(world.gids)
    assert {e.device_id for e in uwt} == {d.id for d in world.trackers.values()}
