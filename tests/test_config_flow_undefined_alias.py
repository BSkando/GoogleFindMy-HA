# tests/test_config_flow_undefined_alias.py
"""``config_flow`` never imports the deprecated ``UNDEFINED`` aliases on a modern core.

Home Assistant 2026.12 replaced ``UNDEFINED`` / ``UndefinedType`` in
``homeassistant.helpers.typing`` with the ``Undefined`` sentinel and turned the
old names into deprecated aliases (``DeprecatedAlias(..., "2027.12")``).  Every
import of them from a custom integration is logged, and from 2027.12 it raises
``ImportError``.

``config_flow.py`` only needs the sentinel for ``_FallbackConfigSubentryFlow``,
which exists solely in the ``except ImportError`` branch taken by cores without
``ConfigSubentryFlow``.  Importing the aliases next to ``ConfigSubentryFlow`` in
the ``try`` body is therefore unused on a modern core, logs two warnings at every
start from 2026.12, and from 2027.12 would push a modern core onto the legacy
fallback path.  This guard keeps the import inside an ``except ImportError``
handler.
"""

from __future__ import annotations

import ast
from pathlib import Path

_CONFIG_FLOW = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "googlefindmy"
    / "config_flow.py"
)
_DEPRECATED_ALIASES = frozenset({"UNDEFINED", "UndefinedType"})


def _alias_imports_outside_import_error_handlers(tree: ast.AST) -> list[int]:
    """Return the lines importing a deprecated alias outside ``except ImportError``."""

    guarded: set[int] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.ImportFrom):
                guarded.add(id(child))

    offenders: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module != "homeassistant.helpers.typing":
            continue
        if not any(alias.name in _DEPRECATED_ALIASES for alias in node.names):
            continue
        if id(node) not in guarded:
            offenders.append(node.lineno)
    return sorted(offenders)


def test_guard_detects_an_unguarded_alias_import() -> None:
    """The guard reports the shape it exists to reject, so it cannot pass vacuously."""

    tree = ast.parse(
        "try:\n"
        "    from homeassistant.helpers.typing import UNDEFINED, UndefinedType\n"
        "except ImportError:\n"
        "    from homeassistant.helpers.typing import UNDEFINED\n"
    )

    assert _alias_imports_outside_import_error_handlers(tree) == [2]


def test_config_flow_imports_undefined_aliases_only_in_fallback() -> None:
    """The deprecated aliases are imported only on the legacy fallback path."""

    tree = ast.parse(_CONFIG_FLOW.read_text(encoding="utf-8"))

    assert _alias_imports_outside_import_error_handlers(tree) == []
