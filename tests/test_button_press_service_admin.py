# tests/test_button_press_service_admin.py
"""A maintenance button pressed through the real ``button.press`` action.

``tests/test_button_maintenance_permissions.py`` drives ``async_press`` with a
Home Assistant double and sets ``_context`` by hand, so it relies on Home
Assistant to hand the caller's context to the entity before the press. This
file checks that hand-over end to end: Home Assistant is real (built from a
fresh import of the plugin's ``common``, as in
``tests/test_services_permissions.py``), the users are real, and the press goes
through ``hass.services.async_call("button", "press", ...)`` with the caller's
``Context``. The action's own permission check lets a user of the default
group through (``control`` on every entity), so a rejection can only come from
the button.

The button under test is the production statistics button, subclassed so it
needs no coordinator setup: the constructor and the restore step are skipped,
everything from ``async_press`` on is the production code. ``button`` is
imported freshly under the real Home Assistant modules, so the raised
``Unauthorized`` is the real class; every new module key goes through
``monkeypatch`` and is removed again on teardown.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from collections.abc import AsyncIterator
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest

if (
    importlib.util.find_spec("pytest_homeassistant_custom_component") is None
):  # pragma: no cover - optional dependency
    pytest.skip(
        "pytest-homeassistant-custom-component is required for this test",
        allow_module_level=True,
    )

pytest_plugins = ("pytest_homeassistant_custom_component",)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

pytestmark = pytest.mark.asyncio

PACKAGE = "custom_components.googlefindmy"
ENTITY_ID = "button.maintenance_probe"


@pytest.fixture(autouse=True)
def _use_real_ha_modules(use_real_homeassistant_modules: None) -> None:
    """Activate real Home Assistant modules from the central conftest fixture."""


def _fresh_import(
    monkeypatch: pytest.MonkeyPatch, names: tuple[str, ...], prefix: str
) -> ModuleType:
    """Import ``names`` freshly and hand every new ``prefix*`` key to monkeypatch.

    Same pattern as ``tests/test_services_permissions.py``; the last name is
    returned.
    """

    def _keys() -> set[str]:
        return {k for k in sys.modules if k == prefix or k.startswith(f"{prefix}.")}

    before = _keys()
    for name in names:
        monkeypatch.delitem(sys.modules, name, raising=False)
        parent_name, _, child = name.rpartition(".")
        parent = importlib.import_module(parent_name)
        monkeypatch.setattr(parent, child, getattr(parent, child, None), raising=False)
    module: ModuleType | None = None
    for name in names:
        module = importlib.import_module(name)
    for key in sorted(_keys() - before):
        created = sys.modules.pop(key)
        monkeypatch.setitem(sys.modules, key, created)
    assert module is not None
    return module


@pytest.fixture
def ha_common(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Import the plugin's ``common`` freshly against the real Home Assistant modules."""

    name = "pytest_homeassistant_custom_component.common"
    return _fresh_import(monkeypatch, (name,), name)


@pytest.fixture
def button_module(monkeypatch: pytest.MonkeyPatch, ha_common: ModuleType) -> ModuleType:
    """Import ``button`` freshly against the real Home Assistant modules."""

    del ha_common
    return _fresh_import(
        monkeypatch,
        (f"{PACKAGE}.ha_typing", f"{PACKAGE}.entity", f"{PACKAGE}.button"),
        PACKAGE,
    )


@pytest.fixture
async def hass(ha_common: ModuleType) -> AsyncIterator[HomeAssistant]:
    """Home Assistant built from the same module copy as the module under test."""

    from homeassistant.helpers import frame

    with ha_common.mock_storage():
        async with ha_common.async_test_home_assistant() as instance:
            frame.async_setup(instance)
            yield instance
            await instance.async_stop(force=True)


async def _group_user(hass: HomeAssistant, common: ModuleType, group_id: str) -> Any:
    group = await hass.auth.async_get_group(group_id)
    return common.MockUser(groups=[group]).add_to_hass(hass)


async def _setup_button(
    hass: HomeAssistant, common: ModuleType, button_module: ModuleType
) -> MagicMock:
    """Register the statistics button under ``button`` and return its effect spy."""

    from homeassistant.setup import async_setup_component

    begin_epoch = MagicMock(return_value=True)
    base = button_module.GoogleFindMyStatsResetButton

    class _ProbeButton(base):  # type: ignore[misc, valid-type]
        """The production button without coordinator wiring."""

        def __init__(self) -> None:
            # Skip the coordinator entity constructor; the press path reads
            # only ``coordinator`` and ``hass``.
            self.coordinator = SimpleNamespace(
                hass=hass,
                begin_new_stats_epoch=begin_epoch,
                _diag=None,
                _schedule_stats_persist=MagicMock(),
                async_update_listeners=MagicMock(),
            )
            self.entity_id = ENTITY_ID
            self._attr_unique_id = None
            self._attr_name = "Maintenance probe"
            self._attr_has_entity_name = False

        async def async_added_to_hass(self) -> None:
            return None

        @property
        def device_info(self) -> None:
            return None

        @property
        def available(self) -> bool:
            return True

        @property
        def entry_id(self) -> str:
            return "entry-maintenance-probe"

    common.setup_test_component_platform(hass, "button", [_ProbeButton()])
    assert await async_setup_component(hass, "button", {"button": {"platform": "test"}})
    await hass.async_block_till_done()
    assert hass.states.get(ENTITY_ID) is not None
    return begin_epoch


async def _press(hass: HomeAssistant, user_id: str | None) -> None:
    from homeassistant.core import Context

    await hass.services.async_call(
        "button",
        "press",
        {"entity_id": ENTITY_ID},
        blocking=True,
        context=Context(user_id=user_id),
    )


async def test_non_admin_press_through_the_action_is_unauthorized(
    hass: HomeAssistant, ha_common: ModuleType, button_module: ModuleType
) -> None:
    """A user of the default group is rejected by the button, before any effect."""

    from homeassistant.auth.const import GROUP_ID_USER
    from homeassistant.exceptions import Unauthorized

    begin_epoch = await _setup_button(hass, ha_common, button_module)
    user = await _group_user(hass, ha_common, GROUP_ID_USER)
    assert not user.is_admin

    with pytest.raises(Unauthorized) as excinfo:
        await _press(hass, user.id)

    assert excinfo.value.context is not None
    assert excinfo.value.context.user_id == user.id
    assert begin_epoch.call_count == 0


async def test_admin_press_through_the_action_runs(
    hass: HomeAssistant, ha_common: ModuleType, button_module: ModuleType
) -> None:
    """Control: an administrator's press reaches the reset."""

    from homeassistant.auth.const import GROUP_ID_ADMIN

    begin_epoch = await _setup_button(hass, ha_common, button_module)
    admin = await _group_user(hass, ha_common, GROUP_ID_ADMIN)

    await _press(hass, admin.id)

    assert begin_epoch.call_count == 1


async def test_press_without_user_through_the_action_runs(
    hass: HomeAssistant, ha_common: ModuleType, button_module: ModuleType
) -> None:
    """Control: an automation or script (no user in the context) is not checked."""

    begin_epoch = await _setup_button(hass, ha_common, button_module)

    await _press(hass, None)

    assert begin_epoch.call_count == 1
