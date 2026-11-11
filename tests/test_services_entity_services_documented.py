# tests/test_services_entity_services_documented.py
"""Every registered entity service is described and callable on every target.

Home Assistant shows an entity service without a description and without a
target picker unless ``services.yaml`` and the translations describe it. The
target filter narrows by integration and domain; the buttons of this
integration have no device class or feature flag that would separate them, and
a call from YAML bypasses the filter, so every button can be a target and each
of them must carry the method the service calls.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from custom_components import googlefindmy
from custom_components.googlefindmy.button import _CoordinatorRefreshServiceMixin

_PACKAGE = Path(googlefindmy.__file__).resolve().parent


def _registered_entity_services() -> dict[str, str]:
    """Map service name to method name for every ``register_entity_service`` call."""
    found: dict[str, str] = {}
    for path in sorted(_PACKAGE.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "register_entity_service"
                and len(node.args) >= 4
            ):
                continue
            name, method = node.args[1], node.args[3]
            assert isinstance(name, ast.Constant) and isinstance(name.value, str)
            assert isinstance(method, ast.Constant) and isinstance(method.value, str)
            found[name.value] = method.value
    return found


_SERVICES = _registered_entity_services()


def test_entity_services_are_found() -> None:
    """The scan is not empty (guards against a renamed helper)."""
    assert "trigger_device_refresh" in _SERVICES


@pytest.mark.parametrize("service", sorted(_SERVICES))
def test_entity_service_is_in_services_yaml(service: str) -> None:
    services = yaml.safe_load((_PACKAGE / "services.yaml").read_text(encoding="utf-8"))
    entry = services[service]
    entity = entry["target"]["entity"]
    assert entity["domain"] == "button"
    assert entity["integration"] == "googlefindmy"
    assert entry["name"]
    assert entry["description"]


@pytest.mark.parametrize(
    "path",
    [_PACKAGE / "strings.json", *sorted((_PACKAGE / "translations").glob("*.json"))],
    ids=lambda p: p.name,
)
def test_entity_services_are_translated(path: Path) -> None:
    services = json.loads(path.read_text(encoding="utf-8"))["services"]
    for service in _SERVICES:
        assert services[service]["name"], service
        assert services[service]["description"], service


def _button_classes() -> dict[str, ast.ClassDef]:
    tree = ast.parse((_PACKAGE / "button.py").read_text(encoding="utf-8"))
    return {node.name: node for node in tree.body if isinstance(node, ast.ClassDef)}


def _base_names(node: ast.ClassDef) -> list[str]:
    return [base.id for base in node.bases if isinstance(base, ast.Name)]


def _is_button(name: str, classes: dict[str, ast.ClassDef]) -> bool:
    node = classes.get(name)
    if node is None:
        return False
    return any(
        base == "ButtonEntity" or _is_button(base, classes)
        for base in _base_names(node)
    )


def _has_method(name: str, method: str, classes: dict[str, ast.ClassDef]) -> bool:
    node = classes.get(name)
    if node is None:
        return False
    own = {
        item.name
        for item in node.body
        if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    return method in own or any(
        _has_method(base, method, classes) for base in _base_names(node)
    )


def test_entity_service_method_on_every_platform_button() -> None:
    """Each button class in ``button.py`` carries the method the service calls."""
    classes = _button_classes()
    buttons = sorted(name for name in classes if _is_button(name, classes))
    assert len(buttons) >= 3
    method = _SERVICES["trigger_device_refresh"]
    missing = [name for name in buttons if not _has_method(name, method, classes)]
    assert missing == []


@pytest.mark.asyncio
async def test_refresh_method_requests_a_coordinator_refresh() -> None:
    """The shared method asks the coordinator of the entry for a refresh."""
    calls: list[str] = []

    class _Coordinator:
        async def async_request_refresh(self) -> None:
            calls.append("refresh")

    target = _CoordinatorRefreshServiceMixin()
    target.coordinator = cast(Any, _Coordinator())

    await target.async_trigger_coordinator_refresh()

    assert calls == ["refresh"]
