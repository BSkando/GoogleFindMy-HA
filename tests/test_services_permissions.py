# tests/test_services_permissions.py
"""Permission checks of the device and maintenance actions under real Home Assistant.

The four device actions check ``control`` on the enabled entities of this
integration at the target device when a user calls them; the three maintenance
actions are registered as admin actions. These tests drive the actions through
the real service registry, real users and real exception classes.

``services`` is imported freshly inside the ``services_module`` fixture after
``use_real_homeassistant_modules`` swapped in the real Home Assistant modules,
so the module binds the real ``Unauthorized``, the real registries and the real
admin helper. Every ``custom_components.googlefindmy.*`` key the import creates
is handed to ``monkeypatch`` and removed again on teardown, so no module bound
to the real Home Assistant modules stays behind for later stub-world tests.

Home Assistant itself is built here as well (``hass`` and the three user
fixtures below override the plugin's), from a fresh import of
``pytest_homeassistant_custom_component.common``. The plugin's own ``hass``
fixture builds Home Assistant from the modules imported when the plugin was
loaded, while ``use_real_homeassistant_modules`` imports fresh copies; with two
copies, enums such as ``HassJobType`` differ by identity, and the job of an
admin action registered through the fresh ``async_register_admin_service`` is
never run by the plugin's core. One copy for core, registries, auth and the
module under test avoids that.

The canonical resolver is a test resolver, not the integration's
``_resolve_canonical_from_any``: ``custom_components.googlefindmy`` stays bound
to the stubs. ``tests/test_services_handler_contracts.py`` covers the real
resolver in the stub world.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import sys
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest

if (
    importlib.util.find_spec("pytest_homeassistant_custom_component") is None
):  # pragma: no cover - optional dependency
    pytest.skip(
        "pytest-homeassistant-custom-component is required for the permission tests",
        allow_module_level=True,
    )

pytest_plugins = ("pytest_homeassistant_custom_component",)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

pytestmark = pytest.mark.asyncio

DOMAIN = "googlefindmy"
PACKAGE = "custom_components.googlefindmy"
DEVICE_SERVICES = ("locate_device", "locate_external", "play_sound", "stop_sound")
ADMIN_SERVICES = ("refresh_device_urls", "rebuild_device_registry", "rebuild_registry")
TRACKER_NAME = "Tracker"


@pytest.fixture(autouse=True)
def _use_real_ha_modules(use_real_homeassistant_modules: None) -> None:
    """Activate real Home Assistant modules from the central conftest fixture."""


def _fresh_import(
    monkeypatch: pytest.MonkeyPatch, name: str, prefix: str
) -> ModuleType:
    """Import ``name`` freshly and hand every new ``prefix*`` key to monkeypatch.

    A key the import creates is popped and set again through ``monkeypatch``;
    because it was absent at ``setitem`` time, ``monkeypatch`` removes it on
    teardown. A key that existed before was removed through ``monkeypatch`` and
    gets its old value back.
    """

    def _keys() -> set[str]:
        return {k for k in sys.modules if k == prefix or k.startswith(f"{prefix}.")}

    before = _keys()
    monkeypatch.delitem(sys.modules, name, raising=False)
    parent_name, _, child = name.rpartition(".")
    parent = importlib.import_module(parent_name)
    monkeypatch.setattr(parent, child, getattr(parent, child, None), raising=False)
    module = importlib.import_module(name)
    for key in sorted(_keys() - before):
        created = sys.modules.pop(key)
        monkeypatch.setitem(sys.modules, key, created)
    return module


@pytest.fixture
def services_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import ``services`` freshly against the real Home Assistant modules."""

    return _fresh_import(monkeypatch, f"{PACKAGE}.services", PACKAGE)


@pytest.fixture
def ha_common(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import the plugin's ``common`` freshly against the real Home Assistant modules."""

    return _fresh_import(
        monkeypatch,
        "pytest_homeassistant_custom_component.common",
        "pytest_homeassistant_custom_component.common",
    )


@pytest.fixture
async def hass(ha_common: ModuleType) -> AsyncIterator[HomeAssistant]:
    """Home Assistant built from the same module copy as the module under test.

    Mirrors the plugin's ``hass`` fixture where it matters here: the frame
    helper is set up after start, and the instance is stopped on teardown so no
    delayed registry write lingers.
    """

    from homeassistant.helpers import frame

    with ha_common.mock_storage():
        async with ha_common.async_test_home_assistant() as instance:
            frame.async_setup(instance)
            yield instance
            await instance.async_stop(force=True)


async def _group_user(hass: HomeAssistant, common: ModuleType, group_id: str) -> Any:
    group = await hass.auth.async_get_group(group_id)
    return common.MockUser(groups=[group]).add_to_hass(hass)


@pytest.fixture
async def hass_admin_user(hass: HomeAssistant, ha_common: ModuleType) -> Any:
    """Return a user of the ``system-admin`` group."""

    from homeassistant.auth.const import GROUP_ID_ADMIN

    return await _group_user(hass, ha_common, GROUP_ID_ADMIN)


@pytest.fixture
async def hass_read_only_user(hass: HomeAssistant, ha_common: ModuleType) -> Any:
    """Return a user of the ``system-read-only`` group."""

    from homeassistant.auth.const import GROUP_ID_READ_ONLY

    return await _group_user(hass, ha_common, GROUP_ID_READ_ONLY)


@pytest.fixture
async def hass_user(hass: HomeAssistant, ha_common: ModuleType) -> Any:
    """Return a user of the default ``system-users`` group."""

    from homeassistant.auth.const import GROUP_ID_USER

    return await _group_user(hass, ha_common, GROUP_ID_USER)


def _mock_user(hass: HomeAssistant) -> Any:
    """Return a non-admin user without groups; tests give it a policy."""

    common = sys.modules["pytest_homeassistant_custom_component.common"]
    return common.MockUser().add_to_hass(hass)


@dataclass
class _Device:
    """One registry device with its entities and the coordinator behind it."""

    device_id: str
    canonical_id: str
    enabled_entities: list[str]
    disabled_entities: list[str] = field(default_factory=list)
    foreign_entities: list[str] = field(default_factory=list)


@dataclass
class _World:
    """The registry world of one test."""

    canon1: _Device
    canon2: _Device
    canon3: _Device
    coordinator: SimpleNamespace
    soft_migrate: AsyncMock
    second_canon1: _Device | None = None
    second_canon3: _Device | None = None
    second_coordinator: SimpleNamespace | None = None


def _coordinator(services: ModuleType) -> SimpleNamespace:
    known = {"CANON1", "CANON2", "CANON3"}
    return SimpleNamespace(
        async_locate_device=AsyncMock(return_value={}),
        async_play_sound=AsyncMock(return_value=services.PlaySoundOutcome.ACCEPTED),
        async_stop_sound=AsyncMock(return_value=services.StopSoundOutcome.CANCELLED),
        classify_stop_correlation=lambda cid, request_uuid: None,
        get_device_display_name=lambda cid: TRACKER_NAME if cid in known else None,
        get_device_location_data=lambda cid: None,
    )


def _test_resolver(hass: HomeAssistant, value: Any) -> tuple[str, str]:
    """Resolve a device id, an entity id of this integration or a canonical id."""

    from homeassistant.exceptions import HomeAssistantError
    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er

    if not isinstance(value, str):
        raise HomeAssistantError("unsupported value")
    dev_reg = dr.async_get(hass)
    device = dev_reg.async_get(value)
    if device is None:
        entity = er.async_get(hass).async_get(value)
        if entity is not None and entity.platform == DOMAIN and entity.device_id:
            device = dev_reg.async_get(entity.device_id)
    if device is not None:
        for domain, identifier in device.identifiers:
            if domain == DOMAIN:
                return identifier.rsplit(":", 1)[-1], device.name or ""
        raise HomeAssistantError("device without identifier")
    for candidate in dev_reg.devices.values():
        for domain, identifier in candidate.identifiers:
            if domain == DOMAIN and identifier.rsplit(":", 1)[-1] == value:
                return value, candidate.name or ""
    raise HomeAssistantError("unknown device")


def _add_device(
    hass: HomeAssistant,
    entry: Any,
    canonical_id: str,
    *,
    enabled: int,
    disabled: int = 0,
    foreign: int = 0,
) -> _Device:
    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er

    device = dr.async_get(hass).async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers={(DOMAIN, f"{entry.entry_id}:sub:{canonical_id}")},
        name=TRACKER_NAME,
    )
    ent_reg = er.async_get(hass)
    result = _Device(device.id, canonical_id, [])
    for index in range(enabled + disabled):
        entity = ent_reg.async_get_or_create(
            "button",
            DOMAIN,
            f"{entry.entry_id}-{canonical_id}-{index}",
            config_entry=entry,
            device_id=device.id,
            disabled_by=er.RegistryEntryDisabler.USER if index >= enabled else None,
        )
        target = (
            result.enabled_entities if index < enabled else result.disabled_entities
        )
        target.append(entity.entity_id)
    for index in range(foreign):
        entity = ent_reg.async_get_or_create(
            "sensor",
            "other_integration",
            f"other-{canonical_id}-{index}",
            device_id=device.id,
        )
        result.foreign_entities.append(entity.entity_id)
    return result


async def _build_world(
    hass: HomeAssistant,
    services: ModuleType,
    *,
    second_account: bool = False,
    title: str = "Account",
) -> _World:
    common = sys.modules["pytest_homeassistant_custom_component.common"]
    entry = common.MockConfigEntry(domain=DOMAIN, entry_id="entry1", title=title)
    entry.add_to_hass(hass)
    canon1 = _add_device(hass, entry, "CANON1", enabled=2, disabled=1, foreign=1)
    canon2 = _add_device(hass, entry, "CANON2", enabled=1)
    canon3 = _add_device(hass, entry, "CANON3", enabled=0, disabled=1)
    coordinator = _coordinator(services)
    entries = hass.data.setdefault(DOMAIN, {}).setdefault("entries", {})
    entries[entry.entry_id] = SimpleNamespace(coordinator=coordinator)

    world = _World(canon1, canon2, canon3, coordinator, AsyncMock(return_value=None))
    if second_account:
        entry2 = common.MockConfigEntry(
            domain=DOMAIN, entry_id="entry2", title="Account 2"
        )
        entry2.add_to_hass(hass)
        world.second_canon1 = _add_device(hass, entry2, "CANON1", enabled=1)
        world.second_canon3 = _add_device(hass, entry2, "CANON3", enabled=1)
        world.second_coordinator = _coordinator(services)
        entries[entry2.entry_id] = SimpleNamespace(coordinator=world.second_coordinator)

    ctx: dict[str, Any] = {
        "domain": DOMAIN,
        "resolve_canonical": _test_resolver,
        "is_active_entry": lambda _entry: True,
        "primary_active_entry": lambda entries_list: None,
        "opt": lambda _entry, _key, default=None: default,
        "soft_migrate_entry": world.soft_migrate,
    }
    await services.async_register_services(hass, ctx)
    return world


def _coordinator_method(service: str) -> str:
    return {
        "locate_device": "async_locate_device",
        "locate_external": "async_locate_device",
        "play_sound": "async_play_sound",
        "stop_sound": "async_stop_sound",
    }[service]


def _awaits(coordinator: SimpleNamespace, service: str) -> int:
    mock: AsyncMock = getattr(coordinator, _coordinator_method(service))
    return mock.await_count


async def _call(
    hass: HomeAssistant,
    service: str,
    data: dict[str, Any],
    user_id: str | None,
) -> None:
    from homeassistant.core import Context

    context = Context(user_id=user_id) if user_id is not None else None
    await hass.services.async_call(
        DOMAIN, service, data, blocking=True, context=context
    )


def _unauthorized() -> type[Exception]:
    from homeassistant.exceptions import Unauthorized

    return Unauthorized


# ---------------------------------------------------------------------------
# Read-only users are rejected, normal users and admins pass, no user passes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("service", DEVICE_SERVICES)
async def test_read_only_user_is_rejected(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_read_only_user: Any,
    service: str,
) -> None:
    """A read-only user cannot run a device action; the coordinator is not reached."""

    world = await _build_world(hass, services_module)
    with pytest.raises(_unauthorized()):
        await _call(
            hass, service, {"device_id": world.canon1.device_id}, hass_read_only_user.id
        )
    assert _awaits(world.coordinator, service) == 0


@pytest.mark.parametrize("service", DEVICE_SERVICES)
@pytest.mark.parametrize("who", ["admin", "user"])
async def test_admin_and_user_reach_coordinator(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_admin_user: Any,
    hass_user: Any,
    service: str,
    who: str,
) -> None:
    """Admins and users of ``system-users`` keep running the device actions."""

    world = await _build_world(hass, services_module)
    user = hass_admin_user if who == "admin" else hass_user
    await _call(hass, service, {"device_id": world.canon1.device_id}, user.id)
    assert _awaits(world.coordinator, service) == 1


@pytest.mark.parametrize("service", DEVICE_SERVICES)
async def test_call_without_user_reaches_coordinator(
    hass: HomeAssistant, services_module: ModuleType, service: str
) -> None:
    """Automations and scripts without a user context are not checked."""

    world = await _build_world(hass, services_module)
    await _call(hass, service, {"device_id": world.canon1.device_id}, None)
    assert _awaits(world.coordinator, service) == 1


async def test_unknown_user_raises_unknown_user(
    hass: HomeAssistant, services_module: ModuleType
) -> None:
    """A user id Home Assistant does not know is rejected as ``UnknownUser``."""

    from homeassistant.exceptions import UnknownUser

    world = await _build_world(hass, services_module)
    with pytest.raises(UnknownUser):
        await _call(
            hass, "locate_device", {"device_id": world.canon1.device_id}, "gone"
        )
    assert world.coordinator.async_locate_device.await_count == 0


@pytest.mark.parametrize("who", ["user", "admin", "none"])
async def test_no_active_entry_names_no_account(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_admin_user: Any,
    hass_user: Any,
    who: str,
) -> None:
    """Without an active entry, no caller learns the entry titles.

    Entry titles are usually the account e-mail. The placeholder carries the
    em dash for every caller, and the English fallback message, which Home
    Assistant logs for every failed call, carries only the counts.
    """

    from homeassistant.exceptions import ServiceValidationError

    title = "owner@example.com"
    world = await _build_world(hass, services_module, title=title)
    hass.data[DOMAIN]["entries"].clear()
    user_id = {"user": hass_user.id, "admin": hass_admin_user.id, "none": None}[who]

    with pytest.raises(ServiceValidationError) as excinfo:
        await _call(
            hass, "locate_device", {"device_id": world.canon1.device_id}, user_id
        )

    err = excinfo.value
    assert err.translation_key == "no_active_entry"
    placeholders = dict(err.translation_placeholders or {})
    assert placeholders["active_count"] == "1"
    assert placeholders["total_count"] == "1"
    assert placeholders["entries"] == "\u2014"
    assert all(title not in str(value) for value in placeholders.values())
    assert title not in str(err)
    assert title not in str(err.args)
    assert world.coordinator.async_locate_device.await_count == 0


# ---------------------------------------------------------------------------
# Input forms, target set, disabled and foreign entities
# ---------------------------------------------------------------------------


def _form(device: _Device, form: str) -> str:
    return {
        "device_id": device.device_id,
        "entity_id": device.enabled_entities[0],
        "canonical_id": device.canonical_id,
    }[form]


@pytest.mark.parametrize("form", ["device_id", "entity_id", "canonical_id"])
async def test_read_only_rejected_for_every_input_form(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_read_only_user: Any,
    form: str,
) -> None:
    """No input form bypasses the check."""

    world = await _build_world(hass, services_module)
    with pytest.raises(_unauthorized()):
        await _call(
            hass,
            "play_sound",
            {"device_id": _form(world.canon1, form)},
            hass_read_only_user.id,
        )
    assert world.coordinator.async_play_sound.await_count == 0


@pytest.mark.parametrize("form", ["device_id", "entity_id", "canonical_id"])
async def test_device_assignment_per_input_form(
    hass: HomeAssistant, services_module: ModuleType, form: str
) -> None:
    """Control on CANON1 passes for CANON1 and is rejected for CANON2."""

    world = await _build_world(hass, services_module)
    user = _mock_user(hass)
    user.mock_policy(
        {"entities": {"entity_ids": dict.fromkeys(world.canon1.enabled_entities, True)}}
    )
    await _call(hass, "play_sound", {"device_id": _form(world.canon1, form)}, user.id)
    assert world.coordinator.async_play_sound.await_count == 1
    with pytest.raises(_unauthorized()):
        await _call(
            hass, "play_sound", {"device_id": _form(world.canon2, form)}, user.id
        )
    assert world.coordinator.async_play_sound.await_count == 1


async def test_unknown_device_user_rejected_admin_gets_not_found(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_admin_user: Any,
    hass_user: Any,
) -> None:
    """An unknown device: non-admins get ``Unauthorized``, admins the old error."""

    from homeassistant.exceptions import ServiceValidationError

    world = await _build_world(hass, services_module)
    with pytest.raises(_unauthorized()):
        await _call(hass, "locate_device", {"device_id": "nope"}, hass_user.id)
    with pytest.raises(ServiceValidationError) as excinfo:
        await _call(hass, "locate_device", {"device_id": "nope"}, hass_admin_user.id)
    assert excinfo.value.translation_key == "device_not_found"
    assert world.coordinator.async_locate_device.await_count == 0


async def test_stop_sound_rejection_precedes_request_uuid_check(
    hass: HomeAssistant, services_module: ModuleType, hass_read_only_user: Any
) -> None:
    """A malformed ``request_uuid`` is not echoed to an unauthorized caller."""

    world = await _build_world(hass, services_module)
    with pytest.raises(_unauthorized()) as excinfo:
        await _call(
            hass,
            "stop_sound",
            {"device_id": world.canon1.device_id, "request_uuid": 123},
            hass_read_only_user.id,
        )
    assert type(excinfo.value) is _unauthorized()


async def test_device_without_enabled_entity(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_admin_user: Any,
    hass_user: Any,
) -> None:
    """A device whose entities are all disabled is not callable for non-admins."""

    world = await _build_world(hass, services_module)
    with pytest.raises(_unauthorized()):
        await _call(
            hass, "locate_device", {"device_id": world.canon3.device_id}, hass_user.id
        )
    assert world.coordinator.async_locate_device.await_count == 0
    await _call(
        hass, "locate_device", {"device_id": world.canon3.device_id}, hass_admin_user.id
    )
    assert world.coordinator.async_locate_device.await_count == 1


@pytest.mark.parametrize("form", ["device_id", "entity_id", "canonical_id"])
async def test_same_tracker_in_two_accounts(
    hass: HomeAssistant, services_module: ModuleType, form: str
) -> None:
    """A tracker in two accounts needs control on both devices, for every form."""

    world = await _build_world(hass, services_module, second_account=True)
    assert world.second_canon1 is not None and world.second_coordinator is not None
    user = _mock_user(hass)
    user.mock_policy(
        {"entities": {"entity_ids": dict.fromkeys(world.canon1.enabled_entities, True)}}
    )
    with pytest.raises(_unauthorized()):
        await _call(
            hass,
            "locate_device",
            {"device_id": _form(world.canon1, form)},
            user.id,
        )
    assert world.coordinator.async_locate_device.await_count == 0
    assert world.second_coordinator.async_locate_device.await_count == 0

    both = world.canon1.enabled_entities + world.second_canon1.enabled_entities
    user.mock_policy({"entities": {"entity_ids": dict.fromkeys(both, True)}})
    await _call(
        hass, "locate_device", {"device_id": _form(world.canon1, form)}, user.id
    )
    reached = (
        world.coordinator.async_locate_device.await_count
        + world.second_coordinator.async_locate_device.await_count
    )
    assert reached == 1


@pytest.mark.parametrize("form", ["device_id", "entity_id"])
async def test_named_device_without_enabled_entity_in_two_accounts(
    hass: HomeAssistant, services_module: ModuleType, form: str
) -> None:
    """A device with only disabled entities stays closed in a second account too."""

    world = await _build_world(hass, services_module, second_account=True)
    assert world.second_canon3 is not None and world.second_coordinator is not None
    user = _mock_user(hass)
    user.mock_policy(
        {
            "entities": {
                "entity_ids": dict.fromkeys(world.second_canon3.enabled_entities, True)
            }
        }
    )
    value = {
        "device_id": world.canon3.device_id,
        "entity_id": world.canon3.disabled_entities[0],
    }[form]
    with pytest.raises(_unauthorized()):
        await _call(hass, "locate_device", {"device_id": value}, user.id)
    assert world.coordinator.async_locate_device.await_count == 0
    assert world.second_coordinator.async_locate_device.await_count == 0

    # Named by its Google ID, the tracker is the target, not the closed device.
    await _call(
        hass, "locate_device", {"device_id": world.canon3.canonical_id}, user.id
    )
    reached = (
        world.coordinator.async_locate_device.await_count
        + world.second_coordinator.async_locate_device.await_count
    )
    assert reached == 1


async def test_device_that_does_not_resolve_does_not_block_others(
    hass: HomeAssistant, services_module: ModuleType
) -> None:
    """A device of this integration whose id does not resolve is skipped."""

    from homeassistant.helpers import device_registry as dr
    from homeassistant.helpers import entity_registry as er

    world = await _build_world(hass, services_module)
    entry = hass.config_entries.async_get_entry("entry1")
    orphan = dr.async_get(hass).async_get_or_create(
        config_entry_id="entry1", identifiers={("other_domain", "orphan")}
    )
    er.async_get(hass).async_get_or_create(
        "button", DOMAIN, "orphan-0", config_entry=entry, device_id=orphan.id
    )
    user = _mock_user(hass)
    user.mock_policy(
        {"entities": {"entity_ids": dict.fromkeys(world.canon1.enabled_entities, True)}}
    )
    await _call(hass, "locate_device", {"device_id": world.canon1.device_id}, user.id)
    assert world.coordinator.async_locate_device.await_count == 1


async def test_missing_control_on_one_enabled_entity_rejects(
    hass: HomeAssistant, services_module: ModuleType
) -> None:
    """Every enabled entity of this integration at the device must carry control."""

    world = await _build_world(hass, services_module)
    first, second = world.canon1.enabled_entities
    user = _mock_user(hass)
    user.mock_policy(
        {"entities": {"entity_ids": {first: True, second: {"read": True}}}}
    )
    with pytest.raises(_unauthorized()):
        await _call(
            hass, "locate_device", {"device_id": world.canon1.device_id}, user.id
        )
    assert world.coordinator.async_locate_device.await_count == 0


async def test_disabled_and_foreign_entities_do_not_count(
    hass: HomeAssistant, services_module: ModuleType
) -> None:
    """Disabled entities and entities of other integrations are not checked."""

    world = await _build_world(hass, services_module)
    assert world.canon1.disabled_entities and world.canon1.foreign_entities
    user = _mock_user(hass)
    user.mock_policy(
        {"entities": {"entity_ids": dict.fromkeys(world.canon1.enabled_entities, True)}}
    )
    await _call(hass, "locate_device", {"device_id": world.canon1.device_id}, user.id)
    assert world.coordinator.async_locate_device.await_count == 1


# ---------------------------------------------------------------------------
# The rejection is a bare ``Unauthorized``, identical for all cases
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("service", DEVICE_SERVICES)
async def test_rejection_is_unauthorized_not_a_failure(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_read_only_user: Any,
    service: str,
) -> None:
    """The rejection is neither converted to ``*_failed`` nor to a validation error."""

    world = await _build_world(hass, services_module)
    with pytest.raises(Exception) as excinfo:
        await _call(
            hass, service, {"device_id": world.canon1.device_id}, hass_read_only_user.id
        )
    assert type(excinfo.value) is _unauthorized()


async def _rejection(
    hass: HomeAssistant, service: str, device_value: Any, user_id: str
) -> Exception:
    with pytest.raises(_unauthorized()) as excinfo:
        await _call(hass, service, {"device_id": device_value}, user_id)
    return excinfo.value


@pytest.mark.parametrize("case", ["unknown", "empty", "list"])
@pytest.mark.parametrize("service", DEVICE_SERVICES)
async def test_uniform_rejection(
    hass: HomeAssistant,
    services_module: ModuleType,
    hass_read_only_user: Any,
    caplog: pytest.LogCaptureFixture,
    service: str,
    case: str,
) -> None:
    """Known device without right, unknown, empty and list ids look the same."""

    world = await _build_world(hass, services_module)
    caplog.set_level(logging.DEBUG)
    # Only the records of the rejections count, not those of building the world.
    caplog.clear()
    reference = await _rejection(
        hass, service, world.canon1.device_id, hass_read_only_user.id
    )
    value: Any = {"unknown": "nope", "empty": "", "list": [world.canon1.device_id]}[
        case
    ]
    other = await _rejection(hass, service, value, hass_read_only_user.id)
    for err in (reference, other):
        assert type(err) is _unauthorized()
        assert err.entity_id is None  # type: ignore[attr-defined]
        assert TRACKER_NAME not in str(err)
    assert str(reference) == str(other)
    assert world.canon1.device_id not in caplog.text
    assert TRACKER_NAME not in caplog.text
    assert hass_read_only_user.id not in caplog.text
    for entity_id in world.canon1.enabled_entities:
        assert entity_id not in caplog.text
    assert _awaits(world.coordinator, service) == 0


# ---------------------------------------------------------------------------
# Maintenance actions are admin actions
# ---------------------------------------------------------------------------


def _spy_maintenance(
    hass: HomeAssistant,
    services: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    world: _World,
) -> Callable[[str], int]:
    """Return a counter of how often each maintenance handler body ran."""

    rebuild_device_registry = AsyncMock(return_value=None)
    monkeypatch.setattr(
        services, "async_rebuild_device_registry", rebuild_device_registry
    )
    original_entries = hass.config_entries.async_entries
    refresh_calls: list[str] = []

    def _entries(*args: Any, **kwargs: Any) -> Any:
        refresh_calls.append("entries")
        return original_entries(*args, **kwargs)

    monkeypatch.setattr(hass.config_entries, "async_entries", _entries)

    def _count(service: str) -> int:
        if service == "rebuild_device_registry":
            return rebuild_device_registry.await_count
        if service == "rebuild_registry":
            return world.soft_migrate.await_count
        return len(refresh_calls)

    return _count


@pytest.mark.parametrize("service", ADMIN_SERVICES)
@pytest.mark.parametrize("who", ["read_only", "user"])
async def test_maintenance_rejects_non_admins(
    hass: HomeAssistant,
    services_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    hass_read_only_user: Any,
    hass_user: Any,
    service: str,
    who: str,
) -> None:
    """Non-admin users cannot run a maintenance action."""

    world = await _build_world(hass, services_module)
    count = _spy_maintenance(hass, services_module, monkeypatch, world)
    user = hass_read_only_user if who == "read_only" else hass_user
    with pytest.raises(_unauthorized()):
        await _call(hass, service, {"mode": "migrate"}, user.id)
    assert count(service) == 0


@pytest.mark.parametrize("service", ADMIN_SERVICES)
@pytest.mark.parametrize("who", ["admin", "none"])
async def test_maintenance_runs_for_admin_and_without_user(
    hass: HomeAssistant,
    services_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    hass_admin_user: Any,
    service: str,
    who: str,
) -> None:
    """Admins and calls without a user reach the handler; ``mode`` is accepted."""

    world = await _build_world(hass, services_module)
    count = _spy_maintenance(hass, services_module, monkeypatch, world)
    user_id = hass_admin_user.id if who == "admin" else None
    await _call(hass, service, {"mode": "migrate"}, user_id)
    assert count(service) >= 1
