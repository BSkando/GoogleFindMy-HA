# tests/test_services_permissions_teardown.py
"""Stub-world probe that runs after ``tests/test_services_permissions.py``.

The permission tests import ``custom_components.googlefindmy.services`` freshly
against the real Home Assistant modules. Their fixture hands every module the
import creates to ``monkeypatch``, so the session continues with modules bound
to the stubs. This file sorts after the permission tests and checks that state
from the stub world: a ``services`` module imported here must use the stubbed
device registry, not the real one.

The probe only measures something when it runs after the permission tests in
the same session (the default file order, without ``pytest-xdist``). Run on
its own it passes trivially.
"""

from __future__ import annotations

import importlib
import sys


def test_services_binds_stub_registry_after_permission_tests() -> None:
    """No ``services`` module bound to the real Home Assistant modules is left."""

    services = importlib.import_module("custom_components.googlefindmy.services")
    assert services.dr is sys.modules["homeassistant.helpers.device_registry"]
    assert services.Unauthorized is sys.modules["homeassistant.exceptions"].Unauthorized
