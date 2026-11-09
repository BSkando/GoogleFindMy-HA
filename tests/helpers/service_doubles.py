# tests/helpers/service_doubles.py
"""Shared service registry and service call doubles for the test suite.

The module deliberately imports nothing from ``homeassistant`` at module level:
``tests/conftest.py`` loads it while it installs its Home Assistant stubs, and a
module-level import would bind whatever ``homeassistant`` happens to be in
``sys.modules`` at that moment.

Two Home Assistant shapes are rebuilt here and pinned by
``tests/test_service_doubles.py`` against the real modules:

* ``async_register_admin_service`` registers ``functools.partial(
  _async_admin_handler, hass, HassJob(handler, ...))`` and passes the schema,
  ``supports_response`` and the keyword ``description_placeholders``.
  :class:`FakeServiceRegistry` accepts that signature and stores the original
  handler, so tests keep calling the handler body directly.
* ``homeassistant.exceptions.Unauthorized`` and ``UnknownUser`` take the
  keyword arguments ``context``, ``user_id``, ``entity_id``,
  ``config_entry_id``, ``perm_category`` and ``permission``.

In the stub world ``homeassistant.helpers.service`` is the real module (the
pytest plugin imports it before ``tests/conftest.py`` installs its stubs), so
the admin wrapper stored in ``FakeServiceRegistry.raw_handlers`` raises the
real ``Unauthorized``, not the stub class in ``sys.modules``. Test the admin
rejection itself under real Home Assistant modules, as
``tests/test_services_permissions.py`` does; ``handlers`` holds the unwrapped
handler bodies for everything else.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from types import ModuleType
from typing import Any

__all__ = [
    "FakeContext",
    "FakeServiceCall",
    "FakeServiceRegistry",
    "install_service_auth_stubs",
    "unwrap_admin_handler",
]

_ADMIN_HANDLER_NAME = "_async_admin_handler"


@dataclass
class FakeContext:
    """Context double carrying only the calling user."""

    user_id: str | None = None


class FakeServiceCall:
    """Service call double, call-compatible with the old stub ``ServiceCall(data)``."""

    def __init__(
        self,
        data: dict[str, Any] | None = None,
        context: FakeContext | None = None,
        domain: str = "googlefindmy",
        service: str = "",
    ) -> None:
        self.data: dict[str, Any] = data or {}
        self.context: FakeContext = context if context is not None else FakeContext()
        self.domain = domain
        self.service = service


def unwrap_admin_handler(handler: Any) -> Any:
    """Return the original handler behind Home Assistant's admin wrapper.

    A plain handler is returned unchanged. The admin wrapper is a
    ``functools.partial`` of ``_async_admin_handler`` with ``hass`` and a
    ``HassJob`` bound and no keyword arguments; the job's ``target`` is the
    original handler. Any other ``partial`` fails loudly (another function, a
    different number of bound arguments, bound keyword arguments, or a second
    argument without ``target``), because silently passing it through would let
    tests call a wrapper they did not mean to call.
    """

    if not isinstance(handler, functools.partial):
        return handler
    func_name = getattr(handler.func, "__name__", "")
    if (
        func_name != _ADMIN_HANDLER_NAME
        or len(handler.args) != 2
        or handler.keywords
        or not hasattr(handler.args[1], "target")
    ):
        raise AssertionError(
            "Unexpected partial handler; expected Home Assistant's admin wrapper "
            f"partial({_ADMIN_HANDLER_NAME}, hass, HassJob), got {handler!r}"
        )
    return handler.args[1].target


class FakeServiceRegistry:
    """Store registered service handlers for direct invocation in tests."""

    def __init__(self) -> None:
        self.handlers: dict[tuple[str, str], Callable[..., Any]] = {}
        self.raw_handlers: dict[tuple[str, str], Callable[..., Any]] = {}
        self.schemas: dict[tuple[str, str], Any] = {}
        self.admin_services: set[tuple[str, str]] = set()

    @property
    def registered(self) -> dict[tuple[str, str], Callable[..., Any]]:
        """Alias kept for tests written against the former local doubles."""

        return self.handlers

    def async_register(
        self,
        domain: str,
        service: str,
        handler: Callable[..., Any],
        schema: Any = None,
        supports_response: Any = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Register a handler keyed by ``(domain, service)``."""

        key = (domain, service)
        self.raw_handlers[key] = handler
        self.handlers[key] = unwrap_admin_handler(handler)
        self.schemas[key] = schema
        if handler is not self.handlers[key]:
            self.admin_services.add(key)
        else:
            self.admin_services.discard(key)


def install_service_auth_stubs(
    *, core_module: ModuleType, exceptions_module: ModuleType
) -> None:
    """Install the service call and authorization stubs into stub modules."""

    base_error: type[Exception] = exceptions_module.HomeAssistantError

    class Unauthorized(base_error):  # type: ignore[misc, valid-type]
        """Stub of ``homeassistant.exceptions.Unauthorized``."""

        def __init__(
            self,
            context: Any | None = None,
            user_id: str | None = None,
            entity_id: str | None = None,
            config_entry_id: str | None = None,
            perm_category: str | None = None,
            permission: str | None = None,
        ) -> None:
            super().__init__(self.__class__.__name__)
            self.context = context
            if user_id is None and context is not None:
                user_id = context.user_id
            self.user_id = user_id
            self.entity_id = entity_id
            self.config_entry_id = config_entry_id
            self.perm_category = perm_category
            self.permission = permission

    class UnknownUser(Unauthorized):
        """Stub of ``homeassistant.exceptions.UnknownUser``."""

    class SupportsResponse(StrEnum):
        """Stub of ``homeassistant.core.SupportsResponse``."""

        NONE = "none"
        OPTIONAL = "optional"
        ONLY = "only"

    core_module.ServiceCall = FakeServiceCall  # type: ignore[attr-defined]
    core_module.SupportsResponse = SupportsResponse  # type: ignore[attr-defined]
    core_module.Context = FakeContext  # type: ignore[attr-defined]
    exceptions_module.Unauthorized = Unauthorized  # type: ignore[attr-defined]
    exceptions_module.UnknownUser = UnknownUser  # type: ignore[attr-defined]
