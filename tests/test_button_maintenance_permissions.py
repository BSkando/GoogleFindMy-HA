# tests/test_button_maintenance_permissions.py
"""Administrator check and entity category of the three maintenance buttons.

Reset statistics, Regenerate FCM token and Regenerate ADM token change state
of the whole integration. The maintenance actions behind them are registered
as admin actions, so a press by a user who is not an administrator is
rejected with ``Unauthorized`` before anything happens, the same way Home
Assistant's ``async_register_admin_service`` wrapper rejects such a call. A
press without a user (automation, script) and a press by an administrator
work as before; an unknown user yields ``UnknownUser``.

The press tests run in the stub world and subclass the production buttons
instead of patching them, so nothing leaks into later tests. The category
test imports ``button`` freshly under the real Home Assistant modules, because
the stub entity base has no ``entity_category`` at all; a failure there must
be an ``AssertionError`` on the value, not an ``AttributeError``.
"""

from __future__ import annotations

import importlib
import logging
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers.config_entries_stub import make_config_entry

PACKAGE = "custom_components.googlefindmy"
TOKEN_REFRESH = f"{PACKAGE}.Auth.token_refresh"
STATS_BUTTON = "GoogleFindMyStatsResetButton"
FCM_BUTTON = "GoogleFindMyRegenerateFcmTokenButton"
ADM_BUTTON = "GoogleFindMyRegenerateAdmTokenButton"
MAINTENANCE_BUTTONS = (STATS_BUTTON, FCM_BUTTON, ADM_BUTTON)

# Identifiers of the pressing user and of the button; neither may show up in
# the rejection log record or in the text of the raised exception.
USER_ID = "user-7f3a91c2"
ENTITY_ID = "button.maintenance_probe_7f3a91c2"
ENTRY_ID = "entry-maintenance-probe"

# Expected values come from the decision for these buttons, not from the
# descriptions in the module under test.
EXPECTED_CATEGORY = "config"
EXPECTED_ENABLED_DEFAULT = {STATS_BUTTON: True, FCM_BUTTON: False, ADM_BUTTON: False}


def _button_module() -> ModuleType:
    return importlib.import_module(f"{PACKAGE}.button")


def _unknown_user() -> type[Exception]:
    """Return the ``UnknownUser`` class the module under test is bound to.

    Read from ``homeassistant.exceptions`` rather than from ``button`` so that a
    missing check fails on the behaviour, not on a missing module attribute;
    ``test_admin_check_raises_home_assistant_classes`` pins the binding itself.
    """

    exceptions: Any = importlib.import_module("homeassistant.exceptions")
    unknown_user: type[Exception] = exceptions.UnknownUser
    return unknown_user


@dataclass
class _World:
    """A pressable button and the spies that record its effects."""

    button: Any
    effects: list[MagicMock | AsyncMock]

    def effect_count(self) -> int:
        """Return how many effect calls happened across all spies."""

        return sum(spy.call_count for spy in self.effects)


def _hass(user: Any) -> SimpleNamespace:
    """Return a Home Assistant double whose auth returns ``user``."""

    return SimpleNamespace(
        data={"googlefindmy": {}},
        auth=SimpleNamespace(async_get_user=AsyncMock(return_value=user)),
    )


def _context(kind: str) -> Any:
    if kind == "none":
        return None
    if kind == "no_user":
        return SimpleNamespace(user_id=None)
    return SimpleNamespace(user_id=USER_ID)


def _user(kind: str) -> Any:
    if kind == "admin":
        return SimpleNamespace(id=USER_ID, is_admin=True)
    if kind == "non_admin":
        return SimpleNamespace(id=USER_ID, is_admin=False)
    return None


def _pressable(base: type, *, available: bool) -> type:
    """Return a subclass with the entity plumbing stubbed out."""

    # Subclass rather than patch: mutating the production class would leak
    # the stubbed gates into every later test in the session.
    class _Pressable(base):  # type: ignore[misc, valid-type]
        entry_id = ENTRY_ID
        entity_id = ENTITY_ID

        def _update_last_pressed(self) -> None:
            return None

    if base.__name__ == STATS_BUTTON:
        return _Pressable

    is_available = available

    class _Gated(_Pressable):
        # The token buttons read their cooldown here; the statistics button
        # has no such gate.
        available = is_available

    return _Gated


@pytest.fixture
def issue_registry(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    """Replace the issue registry seen by ``button`` with a recording double."""

    button_module = _button_module()
    registry = SimpleNamespace(issues={("googlefindmy", "probe_issue"): {}})
    double = SimpleNamespace(
        async_get=lambda _hass: registry,
        async_delete_issue=MagicMock(),
    )
    monkeypatch.setattr(button_module, "ir", double)
    return double


@pytest.fixture
def token_refresh() -> Iterator[SimpleNamespace]:
    """Replace both token regeneration helpers with spies."""

    with (
        patch(
            f"{TOKEN_REFRESH}.async_regenerate_fcm_token",
            new_callable=AsyncMock,
            return_value=True,
        ) as fcm,
        patch(
            f"{TOKEN_REFRESH}.async_regenerate_adm_token",
            new_callable=AsyncMock,
            return_value=True,
        ) as adm,
    ):
        yield SimpleNamespace(fcm=fcm, adm=adm)


def _world(
    cls_name: str,
    *,
    context: str,
    user: str,
    issue_registry: SimpleNamespace,
    token_refresh: SimpleNamespace,
    available: bool = True,
) -> _World:
    button_module = _button_module()
    cls = _pressable(getattr(button_module, cls_name), available=available)
    button = cls.__new__(cls)
    hass = _hass(_user(user))
    button.hass = hass
    button._context = _context(context)

    if cls_name == STATS_BUTTON:
        begin_epoch = MagicMock(return_value=True)
        button.coordinator = SimpleNamespace(
            hass=hass,
            begin_new_stats_epoch=begin_epoch,
            _diag=None,
            _schedule_stats_persist=MagicMock(),
            async_update_listeners=MagicMock(),
            config_entry=make_config_entry(entry_id=ENTRY_ID),
        )
        return _World(button, [begin_epoch, issue_registry.async_delete_issue])

    if cls_name == FCM_BUTTON:
        button.coordinator = SimpleNamespace(
            hass=hass, config_entry=make_config_entry(entry_id=ENTRY_ID)
        )
        return _World(button, [token_refresh.fcm])

    button.coordinator = SimpleNamespace(
        hass=hass,
        config_entry=make_config_entry(
            entry_id=ENTRY_ID,
            runtime_data=SimpleNamespace(
                token_cache=SimpleNamespace(entry_id=ENTRY_ID)
            ),
        ),
    )
    return _World(button, [token_refresh.adm])


@pytest.mark.asyncio
@pytest.mark.parametrize("cls_name", MAINTENANCE_BUTTONS)
async def test_press_without_context_runs(
    cls_name: str, issue_registry: SimpleNamespace, token_refresh: SimpleNamespace
) -> None:
    """A press without a context (automation, script) is not checked."""

    world = _world(
        cls_name,
        context="none",
        user="admin",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
    )

    await world.button.async_press()

    assert world.effects[0].call_count == 1
    world.button.hass.auth.async_get_user.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cls_name", MAINTENANCE_BUTTONS)
async def test_press_without_user_runs(
    cls_name: str, issue_registry: SimpleNamespace, token_refresh: SimpleNamespace
) -> None:
    """A press whose context carries no user is not checked."""

    world = _world(
        cls_name,
        context="no_user",
        user="non_admin",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
    )

    await world.button.async_press()

    assert world.effects[0].call_count == 1
    world.button.hass.auth.async_get_user.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cls_name", MAINTENANCE_BUTTONS)
async def test_admin_press_runs(
    cls_name: str, issue_registry: SimpleNamespace, token_refresh: SimpleNamespace
) -> None:
    """An administrator's press works as before."""

    world = _world(
        cls_name,
        context="user",
        user="admin",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
    )

    await world.button.async_press()

    assert world.effects[0].call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cls_name", MAINTENANCE_BUTTONS)
async def test_non_admin_press_is_rejected_before_any_effect(
    cls_name: str, issue_registry: SimpleNamespace, token_refresh: SimpleNamespace
) -> None:
    """A user who is not an administrator gets ``Unauthorized`` and no effect."""

    button_module = _button_module()
    world = _world(
        cls_name,
        context="user",
        user="non_admin",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
    )

    with pytest.raises(button_module.Unauthorized) as excinfo:
        await world.button.async_press()

    assert not isinstance(excinfo.value, _unknown_user())
    assert world.effect_count() == 0
    world.button.hass.auth.async_get_user.assert_awaited_once_with(USER_ID)


@pytest.mark.asyncio
@pytest.mark.parametrize("cls_name", MAINTENANCE_BUTTONS)
async def test_unknown_user_press_is_rejected(
    cls_name: str, issue_registry: SimpleNamespace, token_refresh: SimpleNamespace
) -> None:
    """A user ID that Home Assistant does not know yields ``UnknownUser``."""

    world = _world(
        cls_name,
        context="user",
        user="unknown",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
    )

    with pytest.raises(_unknown_user()):
        await world.button.async_press()

    assert world.effect_count() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("cls_name", (FCM_BUTTON, ADM_BUTTON))
async def test_token_button_checks_rights_before_the_cooldown(
    cls_name: str, issue_registry: SimpleNamespace, token_refresh: SimpleNamespace
) -> None:
    """During the cooldown a non-admin press is rejected, not silently ignored."""

    button_module = _button_module()
    world = _world(
        cls_name,
        context="user",
        user="non_admin",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
        available=False,
    )

    with pytest.raises(button_module.Unauthorized):
        await world.button.async_press()

    assert world.effect_count() == 0


@pytest.mark.asyncio
async def test_stats_button_checks_rights_before_touching_statistics(
    issue_registry: SimpleNamespace, token_refresh: SimpleNamespace
) -> None:
    """A rejected reset starts no new epoch and deletes no repair issue."""

    button_module = _button_module()
    world = _world(
        STATS_BUTTON,
        context="user",
        user="non_admin",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
    )

    with pytest.raises(button_module.Unauthorized):
        await world.button.async_press()

    world.button.coordinator.begin_new_stats_epoch.assert_not_called()
    world.button.coordinator._schedule_stats_persist.assert_not_called()
    issue_registry.async_delete_issue.assert_not_called()


def test_admin_check_raises_home_assistant_classes() -> None:
    """The rejection uses Home Assistant's classes, not module-local look-alikes."""

    button_module = _button_module()
    allowed = {
        "homeassistant.exceptions",
        "tests.conftest",
        "tests.helpers.service_doubles",
    }
    for name in ("Unauthorized", "UnknownUser"):
        cls = getattr(button_module, name, None)
        assert cls is not None, f"button does not import {name}"
        assert cls.__name__ == name
        assert cls.__module__ in allowed, cls


@pytest.mark.asyncio
@pytest.mark.parametrize("cls_name", MAINTENANCE_BUTTONS)
async def test_rejection_log_names_no_user_or_entity(
    cls_name: str,
    issue_registry: SimpleNamespace,
    token_refresh: SimpleNamespace,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The rejection is logged once, without the user ID or the entity ID."""

    button_module = _button_module()
    world = _world(
        cls_name,
        context="user",
        user="non_admin",
        issue_registry=issue_registry,
        token_refresh=token_refresh,
    )
    caplog.set_level(logging.WARNING, logger=button_module.__name__)
    caplog.clear()

    with pytest.raises(button_module.Unauthorized) as excinfo:
        await world.button.async_press()

    records = [r for r in caplog.records if r.name == button_module.__name__]
    assert len(records) == 1, [r.getMessage() for r in records]
    assert records[0].levelno == logging.WARNING
    for text in (records[0].getMessage(), str(excinfo.value)):
        assert USER_ID not in text
        assert ENTITY_ID not in text
        assert ENTRY_ID not in text


def _fresh_import(
    monkeypatch: pytest.MonkeyPatch, names: tuple[str, ...]
) -> ModuleType:
    """Import ``names`` freshly and hand every new package key to monkeypatch.

    Same pattern as ``tests/test_services_permissions.py``: a key the import
    creates is popped and set again through ``monkeypatch`` so it is removed on
    teardown; a key that existed before is removed through ``monkeypatch`` and
    gets its old value back. The last name is returned.
    """

    def _keys() -> set[str]:
        return {k for k in sys.modules if k == PACKAGE or k.startswith(f"{PACKAGE}.")}

    before = _keys()
    module: ModuleType | None = None
    for name in names:
        monkeypatch.delitem(sys.modules, name, raising=False)
        parent_name, _, child = name.rpartition(".")
        parent = importlib.import_module(parent_name)
        monkeypatch.setattr(parent, child, getattr(parent, child, None), raising=False)
    for name in names:
        module = importlib.import_module(name)
    for key in sorted(_keys() - before):
        created = sys.modules.pop(key)
        monkeypatch.setitem(sys.modules, key, created)
    assert module is not None
    return module


def test_maintenance_buttons_category_and_default(
    use_real_homeassistant_modules: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """All three buttons carry the ``config`` category at runtime.

    Home Assistant reads the category from ``_attr_entity_category`` or from
    ``entity_description``; an ``_attr_entity_description`` attribute is never
    read. The token buttons stay disabled by default, the statistics button
    stays enabled.
    """

    del use_real_homeassistant_modules
    button_module = _fresh_import(
        monkeypatch, (f"{PACKAGE}.ha_typing", f"{PACKAGE}.entity", f"{PACKAGE}.button")
    )
    for cls_name in MAINTENANCE_BUTTONS:
        cls = getattr(button_module, cls_name)
        entity = cls.__new__(cls)
        assert entity.entity_category == EXPECTED_CATEGORY, cls_name
        assert (
            entity.entity_registry_enabled_default is EXPECTED_ENABLED_DEFAULT[cls_name]
        ), cls_name
