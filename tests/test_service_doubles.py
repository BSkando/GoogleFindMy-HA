# tests/test_service_doubles.py
"""Contract of the shared service doubles in ``tests.helpers.service_doubles``.

The doubles rebuild two Home Assistant shapes: the admin wrapper that
``async_register_admin_service`` registers, and the keyword arguments of
``Unauthorized``. These tests pin both against the real Home Assistant code, so
a change upstream turns them red instead of letting the doubles drift.
"""

from __future__ import annotations

import functools
import inspect
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import voluptuous as vol

from tests.helpers.service_doubles import (
    FakeContext,
    FakeServiceCall,
    FakeServiceRegistry,
    install_service_auth_stubs,
    unwrap_admin_handler,
)


async def _handler(call: Any) -> None:
    """Service handler used as registration target."""


def _real_admin_helper() -> Any:
    from homeassistant.helpers.service import async_register_admin_service

    return async_register_admin_service


def test_real_admin_helper_registers_against_fake_registry() -> None:
    """The real admin helper registers through the shared registry double."""

    helper = _real_admin_helper()
    module = inspect.getmodule(helper)
    assert helper.__module__ == "homeassistant.helpers.service"
    assert module is not None and getattr(module, "__file__", None)

    registry = FakeServiceRegistry()
    hass = SimpleNamespace(services=registry)
    schema = vol.Schema({}, extra=vol.ALLOW_EXTRA)

    helper(hass, "googlefindmy", "rebuild_registry", _handler, schema)

    key = ("googlefindmy", "rebuild_registry")
    assert registry.handlers[key] is _handler
    assert registry.registered[key] is _handler
    assert isinstance(registry.raw_handlers[key], functools.partial)
    assert key in registry.admin_services
    assert registry.schemas[key]({"mode": "migrate"}) == {"mode": "migrate"}


def test_plain_registration_is_not_admin() -> None:
    """A plain registration keeps the handler and is not marked as admin."""

    registry = FakeServiceRegistry()
    registry.async_register("googlefindmy", "locate_device", _handler)

    key = ("googlefindmy", "locate_device")
    assert registry.handlers[key] is _handler
    assert registry.raw_handlers[key] is _handler
    assert key not in registry.admin_services
    assert registry.schemas[key] is None


def test_foreign_partial_fails_loudly() -> None:
    """Any partial other than the admin wrapper raises instead of passing."""

    def _other(hass: Any, job: Any, call: Any) -> None:
        return None

    with pytest.raises(AssertionError, match="admin wrapper"):
        unwrap_admin_handler(functools.partial(_other, object(), object()))


def test_admin_shaped_partial_with_unexpected_binding_fails_loudly() -> None:
    """A wrapper with keyword bindings or without a job target is not unwrapped."""

    def _async_admin_handler(hass: Any, job: Any, call: Any, **kwargs: Any) -> None:
        return None

    job = SimpleNamespace(target=_handler)
    with pytest.raises(AssertionError, match="admin wrapper"):
        unwrap_admin_handler(
            functools.partial(_async_admin_handler, object(), job, extra=True)
        )
    with pytest.raises(AssertionError, match="admin wrapper"):
        unwrap_admin_handler(
            functools.partial(_async_admin_handler, object(), object())
        )
    assert (
        unwrap_admin_handler(functools.partial(_async_admin_handler, object(), job))
        is _handler
    )


def test_stub_entity_base_carries_context() -> None:
    """The stub entity base exposes ``_context`` like the real ``Entity``."""

    from homeassistant.components.button import ButtonEntity

    assert ButtonEntity._context is None  # type: ignore[attr-defined]


def test_fake_service_call_defaults_to_no_user() -> None:
    """A call without context behaves like an automation call."""

    call = FakeServiceCall({})
    assert call.context.user_id is None
    assert call.data == {}
    assert FakeServiceCall(context=FakeContext(user_id="u")).context.user_id == "u"


def test_reregistration_as_plain_clears_admin_flag() -> None:
    """Registering a service again without the wrapper drops its admin flag."""

    registry = FakeServiceRegistry()
    hass = SimpleNamespace(services=registry)
    _real_admin_helper()(hass, "googlefindmy", "rebuild_registry", _handler)
    key = ("googlefindmy", "rebuild_registry")
    assert key in registry.admin_services

    registry.async_register("googlefindmy", "rebuild_registry", _handler)
    assert key not in registry.admin_services
    assert registry.raw_handlers[key] is _handler


def test_old_three_arg_registry_rejects_admin_helper() -> None:
    """The real admin helper needs the extended registry signature.

    The registry here is deliberately incompatible: it accepts only the old
    three positional arguments, which is what the local test registries did
    before ``tests.helpers.service_doubles`` existed.
    """

    def _three_arg_register(domain: str, service: str, handler: Any) -> None:
        return None

    helper = _real_admin_helper()
    hass = SimpleNamespace(services=SimpleNamespace(async_register=_three_arg_register))
    with pytest.raises(
        TypeError, match=r"unexpected keyword argument|positional arguments? but"
    ):
        helper(hass, "googlefindmy", "rebuild_registry", _handler)


def _stub_exception_classes() -> tuple[type[Exception], type[Exception]]:
    """Install the stubs into fresh modules and return both stub classes."""

    class HomeAssistantError(Exception):
        pass

    core_module = ModuleType("stub_core")
    exceptions_module = ModuleType("stub_exceptions")
    exceptions_module.HomeAssistantError = HomeAssistantError  # type: ignore[attr-defined]
    install_service_auth_stubs(
        core_module=core_module, exceptions_module=exceptions_module
    )
    assert core_module.ServiceCall is FakeServiceCall  # type: ignore[attr-defined]
    assert core_module.Context is FakeContext  # type: ignore[attr-defined]
    stub_unauthorized: type[Exception] = exceptions_module.Unauthorized  # type: ignore[attr-defined]
    stub_unknown: type[Exception] = exceptions_module.UnknownUser  # type: ignore[attr-defined]
    assert issubclass(stub_unauthorized, HomeAssistantError)
    return stub_unauthorized, stub_unknown


def _parameters(func: Any) -> list[tuple[str, Any]]:
    """Return parameter names and defaults without evaluating annotations.

    ``inspect.signature`` evaluates annotations on Python 3.14, and the real
    ``Unauthorized`` annotates with a name imported only for type checking.
    """

    code = func.__code__
    names = code.co_varnames[1 : code.co_argcount + code.co_kwonlyargcount]
    defaults = func.__defaults__ or ()
    padded = (inspect.Parameter.empty,) * (len(names) - len(defaults)) + defaults
    return list(zip(names, padded, strict=True))


@pytest.mark.usefixtures("use_real_homeassistant_modules")
def test_stubs_match_real_home_assistant_shapes() -> None:
    """Stub classes and doubles expose what the real Home Assistant classes do."""

    from homeassistant.core import Context, ServiceCall
    from homeassistant.exceptions import Unauthorized, UnknownUser
    from homeassistant.helpers.entity import Entity

    stub_unauthorized, stub_unknown = _stub_exception_classes()
    assert _parameters(stub_unauthorized.__init__) == _parameters(Unauthorized.__init__)
    assert issubclass(stub_unknown, stub_unauthorized)
    assert issubclass(UnknownUser, Unauthorized)

    real_error = Unauthorized(context=Context(user_id="u"))
    stub_error = stub_unauthorized(context=FakeContext(user_id="u"))
    assert str(stub_error) == str(real_error) == "Unauthorized"
    assert stub_error.user_id == real_error.user_id == "u"  # type: ignore[attr-defined]
    assert stub_error.entity_id is real_error.entity_id is None  # type: ignore[attr-defined]

    real_call = ServiceCall(
        SimpleNamespace(),  # hass, unused by the attributes below
        "googlefindmy",
        "locate_device",
        {},
        Context(),
    )
    for attribute in ("data", "context", "domain", "service"):
        assert hasattr(real_call, attribute)
        assert hasattr(FakeServiceCall(), attribute)
    assert hasattr(Context(), "user_id")
    assert "_context" in vars(Entity) or hasattr(Entity, "_context")
