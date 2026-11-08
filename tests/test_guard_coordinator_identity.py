# tests/test_guard_coordinator_identity.py
"""Self-test for the cross-test coordinator-identity guard in ``conftest.py``.

The guard itself lives in ``tests/conftest.py``: after every test,
``pytest_runtest_teardown`` calls
:func:`tests.conftest.detect_coordinator_identity_leaks` and fails the *causing*
test when an integration module still holds a foreign
``GoogleFindMyCoordinator``.  That mechanism is what turns a session-wide import
poisoning into a local, attributable failure instead of an avalanche of
``HomeAssistantError: googlefindmy coordinator not ready`` in unrelated files
minutes later.

A guard nobody tests is a guard nobody can trust, so this file pins the
properties it must have:

* it reports a module whose symbol has been replaced (otherwise it is decoration
  and the next leak passes unnoticed),
* it stays silent on a clean session (otherwise it reddens innocent tests and
  gets switched off), and
* it *restores* the production class, so the blame stays with one test instead
  of spreading over every test that follows.

The file also pins the *fix* statically: the consumer list must still match the
modules that actually bind the symbol, ``tests.conftest.import_coordinator_consumers``
must import exactly that list with literal ``import`` statements, and each
harness that patches the symbol must call the helper before its first
``monkeypatch.setattr``.  All three are
read out of the source with ``ast`` rather than reproduced at runtime.  An
earlier draft did reproduce it, by dropping seven modules from ``sys.modules``
and re-importing them inside a patch window; that test spread a second symbol
leak of its own (``entity.get_url``) and reddened
``test_metadata_helpers.py``.  A reproduction that has to practise the disease
in order to demonstrate it is the wrong instrument here.

Deliberately out of scope: whether ``pytest_runtest_teardown`` actually calls
the detector.  Verifying that inside the very session the hook governs would
mean leaving a real leak behind, which the hook would then, correctly, report.
That wiring is verified by mutation instead (drop the early imports from
``_prepare_async_setup_entry_harness`` and
``test_programmatic_subentry_creation_triggers_setup_and_entities`` errors at
teardown, naming ``device_tracker``), and it is recorded in
``tests/AGENTS.md``.
"""

from __future__ import annotations

import ast
import importlib
import pathlib
import sys

import pytest

from tests.conftest import (
    COORDINATOR_CONSUMER_MODULES,
    _restore_coordinator_identity,
    detect_coordinator_identity_leaks,
)

_PROBE_MODULE = "custom_components.googlefindmy.device_tracker"

#: Anchored on this file rather than on the process working directory.  A
#: CWD-relative path turns a wrong invocation directory into an empty scan and
#: therefore into a completeness assertion that fails with a seven-element set
#: difference, which reads like a real drift finding.
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
_INTEGRATION_ROOT = _REPO_ROOT / "custom_components" / "googlefindmy"
_SYMBOL = "GoogleFindMyCoordinator"

#: Every harness that patches the symbol in module namespaces.  All three must
#: import the consumers first; pinning only the one that was repaired would let
#: the other two drift back into the same defect.
_HARNESSES: tuple[tuple[str, str], ...] = (
    ("tests/test_hass_data_layout.py", "_prepare_async_setup_entry_harness"),
    ("tests/test_device_entity_registration.py", "_patch_integration_runtime"),
    (
        "tests/test_entity_device_info_contract.py",
        "test_integration_device_info_uses_service_device",
    ),
)

#: The module that re-exports the symbol.  Harnesses patch this one on purpose,
#: so it is the source of the copies rather than a consumer of them.
_SYMBOL_SOURCE_MODULE = "custom_components.googlefindmy.coordinator"


def _module_level_consumers() -> set[str]:
    """Return integration modules binding the symbol at module level."""

    paths = sorted(_INTEGRATION_ROOT.rglob("*.py"))
    assert paths, (
        f"no sources found under {_INTEGRATION_ROOT}; the completeness check "
        "below would silently pass on an empty set"
    )

    consumers: set[str] = set()
    for path in paths:
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as err:
            # Skipping would shrink the expected set and turn the very drift
            # this test guards against into a false green.
            raise AssertionError(f"cannot parse {path}: {err}") from err
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.col_offset != 0:
                continue
            if not any(alias.name == _SYMBOL for alias in node.names):
                continue
            dotted = ".".join(path.relative_to(_REPO_ROOT).with_suffix("").parts)
            consumers.add(dotted.removesuffix(".__init__"))
    return consumers


_HELPER = "import_coordinator_consumers"


_MAPPING_WRITERS = frozenset(
    {"update", "setdefault", "pop", "__setitem__", "__delitem__"}
)


def _mapping_write_names(call: ast.Call) -> set[str]:
    """Names a ``update``/``setdefault``/``pop``/``__setitem__`` call writes.

    Covers ``globals().update(name=...)``, ``.update({"name": ...})`` and the
    string first argument of the other methods, whatever mapping they act on.
    """

    if not (
        isinstance(call.func, ast.Attribute) and call.func.attr in _MAPPING_WRITERS
    ):
        return set()
    names = {kw.arg for kw in call.keywords if kw.arg}
    for arg in call.args:
        if isinstance(arg, ast.Dict):
            names |= {
                key.value
                for key in arg.keys
                if isinstance(key, ast.Constant) and isinstance(key.value, str)
            }
    if (
        call.func.attr != "update"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and isinstance(call.args[0].value, str)
    ):
        names.add(call.args[0].value)
    return names


def _bindings_of(tree: ast.AST, name: str) -> list[ast.AST]:
    """Every node anywhere in ``tree`` that binds, deletes or rebinds ``name``.

    Counts definitions, parameters, assignments (also under ``if``/``try``/
    loops), ``del``, ``except … as``, the capture names of ``match`` patterns,
    ``global``/``nonlocal`` declarations, import aliases, an attribute of that
    name assigned or deleted on any object (``sys.modules[__name__].name =``),
    ``setattr``/``delattr`` (also ``builtins.setattr``) and
    ``operator.setitem``/``operator.delitem`` with the name as a string, the
    string form
    ``globals()["name"]`` and the mapping writers ``update``/``setdefault``/
    ``pop``/``__setitem__``/``__delitem__`` with the name as keyword, key or
    first argument. Walking the whole tree instead of the module body is what
    catches a rebinding hidden in a branch or a function.

    Not seen: a name built at run time (``setattr(m, prefix + "s", f)``,
    ``globals()[key]``), code run through ``exec``/``eval`` and writes from
    another module.
    """

    found: list[ast.AST] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            if node.name == name:
                found.append(node)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store | ast.Del):
            if node.id == name:
                found.append(node)
        elif isinstance(node, ast.Attribute) and isinstance(
            node.ctx, ast.Store | ast.Del
        ):
            if node.attr == name:
                found.append(node)
        elif isinstance(node, ast.arg):
            if node.arg == name:
                found.append(node)
        elif isinstance(node, ast.ExceptHandler | ast.MatchAs | ast.MatchStar):
            if node.name == name:
                found.append(node)
        elif isinstance(node, ast.MatchMapping):
            if node.rest == name:
                found.append(node)
        elif isinstance(node, ast.Global | ast.Nonlocal):
            if name in node.names:
                found.append(node)
        elif isinstance(node, ast.Import | ast.ImportFrom):
            if any(
                (alias.asname or alias.name.split(".")[0]) == name
                for alias in node.names
            ):
                found.append(node)
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
            if node.slice.value == name:
                found.append(node)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                called: str | None = node.func.id
            elif isinstance(node.func, ast.Attribute):
                called = node.func.attr
            else:
                called = None
            if (
                called in {"setattr", "delattr", "setitem", "delitem"}
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == name
            ) or name in _mapping_write_names(node):
                found.append(node)
    return found


def _helper_imports() -> list[str]:
    """Module names the shared import helper imports, in source order.

    Only plain ``import a.b.c`` statements directly in the function body count.
    An import under ``if``/``try`` or inside a nested function would not run
    unconditionally, so it is reported as a failure instead of being counted.
    """

    tree = ast.parse((_REPO_ROOT / "tests" / "conftest.py").read_text(encoding="utf-8"))
    bindings = _bindings_of(tree, _HELPER)
    assert len(bindings) == 1, (
        f"tests/conftest.py must bind {_HELPER} exactly once; found "
        f"{len(bindings)} binding(s) at line(s) "
        f"{[getattr(node, 'lineno', '?') for node in bindings]}"
    )
    helper = bindings[0]
    assert isinstance(helper, ast.FunctionDef) and helper in tree.body, (
        "the helper must be a plain module-level function"
    )
    # A decorator (e.g. a cache) or a parameter could turn later calls into
    # no-ops or make the imports depend on the caller.
    assert not helper.decorator_list, (
        "import_coordinator_consumers must not be decorated"
    )
    assert not (
        helper.args.args
        or helper.args.posonlyargs
        or helper.args.kwonlyargs
        or helper.args.vararg
        or helper.args.kwarg
    ), "import_coordinator_consumers must not take parameters"
    body = helper.body
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
    ):
        body = body[1:]
    names: list[str] = []
    for statement in body:
        assert isinstance(statement, ast.Import), (
            "import_coordinator_consumers may only contain plain import "
            f"statements; found {ast.unparse(statement)!r} at line {statement.lineno}"
        )
        names.extend(alias.name for alias in statement.names if alias.asname is None)
        assert all(alias.asname is None for alias in statement.names), (
            f"aliased import at line {statement.lineno} binds a different name"
        )
    return names


def test_import_helper_imports_exactly_the_consumer_list() -> None:
    """The literal imports and the tuple must name the same modules.

    The harnesses import the consumers through the helper, the identity check
    and the drift test below read the tuple. If the two disagreed, a consumer
    could be listed yet never imported before the patch window.
    """

    names = _helper_imports()
    assert sorted(names) == sorted(COORDINATOR_CONSUMER_MODULES)
    assert len(names) == len(set(names))


def test_consumer_list_covers_every_module_level_binding() -> None:
    """The harness's consumer tuple must not drift away from the sources.

    A module that starts importing ``GoogleFindMyCoordinator`` at module level
    silently joins the risk group.  Deriving the expected set from the sources
    makes that visible the moment it happens, instead of the next time the
    import order shifts.
    """

    expected = _module_level_consumers() - {_SYMBOL_SOURCE_MODULE}
    assert set(COORDINATOR_CONSUMER_MODULES) == expected


def test_the_consumer_list_has_no_duplicates() -> None:
    """A duplicate would slip past the set comparison above unnoticed."""

    assert len(COORDINATOR_CONSUMER_MODULES) == len(set(COORDINATOR_CONSUMER_MODULES))


@pytest.mark.parametrize(("relative_path", "function"), _HARNESSES)
def test_harness_imports_consumers_before_the_first_patch(
    relative_path: str, function: str
) -> None:
    """Importing the consumers after the first patch would defeat the fix.

    Order is the whole point: a consumer imported inside the patch window copies
    the stub.  This reads the harness source rather than its behaviour, because
    exercising the behaviour means poisoning the session on purpose.
    """

    tree = ast.parse((_REPO_ROOT / relative_path).read_text(encoding="utf-8"))
    harness = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and node.name == function
        ),
        None,
    )
    assert harness is not None, f"{function} no longer exists in {relative_path}"

    # The call below only means something if the name is the conftest helper:
    # a local function, lambda or reassignment of the same name would satisfy
    # the call check while importing nothing.
    bindings = _bindings_of(tree, _HELPER)
    assert len(bindings) == 1, (
        f"{relative_path} must bind {_HELPER} exactly once (the import from "
        f"tests.conftest); found {len(bindings)} binding(s)"
    )
    binding = bindings[0]
    assert (
        isinstance(binding, ast.ImportFrom)
        and binding.module == "tests.conftest"
        and any(
            alias.name == _HELPER and alias.asname is None for alias in binding.names
        )
    ), f"{relative_path} must import {_HELPER} from tests.conftest without an alias"

    # Only a call that is itself a statement of the harness body counts: one
    # under `if`, `try`, a loop or a nested function may never run.
    import_line = min(
        (
            statement.lineno
            for statement in harness.body
            if isinstance(statement, ast.Expr)
            and isinstance(statement.value, ast.Call)
            and isinstance(statement.value.func, ast.Name)
            and statement.value.func.id == "import_coordinator_consumers"
            and not statement.value.args
            and not statement.value.keywords
        ),
        default=None,
    )
    assert import_line is not None, (
        f"{relative_path}::{function} no longer imports the consumer modules; "
        "without a direct call to import_coordinator_consumers() in the harness "
        "body before the first patch, a lazy first import inside the patch "
        "window keeps the stub"
    )

    first_patch_line = min(
        (
            node.lineno
            for node in ast.walk(harness)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "setattr"
        ),
        default=None,
    )
    assert first_patch_line is not None, (
        f"{relative_path}::{function} no longer patches anything"
    )
    assert import_line < first_patch_line, (
        f"{relative_path}::{function} imports the coordinator consumers at line "
        f"{import_line}, after the first monkeypatch.setattr at line "
        f"{first_patch_line}; move the imports above it"
    )


def test_guard_is_silent_without_a_leak() -> None:
    """A clean session must not produce findings."""

    assert detect_coordinator_identity_leaks() == []


def test_guard_reports_a_replaced_symbol(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replacing the symbol in one module must surface exactly that module.

    Patched through ``monkeypatch`` on purpose: a bare assignment with a
    ``finally`` would leave the foreign class behind if anything above the
    ``try`` ever started raising, and the teardown guard would then report this
    very self-test as the culprit.
    """

    module = importlib.import_module(_PROBE_MODULE)

    class _ForeignCoordinator:
        """Stand-in for a stub that outlived its monkeypatch window."""

    with monkeypatch.context() as patcher:
        patcher.setattr(module, "GoogleFindMyCoordinator", _ForeignCoordinator)
        assert detect_coordinator_identity_leaks() == [_PROBE_MODULE]

    assert detect_coordinator_identity_leaks() == []


def test_restoring_a_poisoned_module_ends_the_finding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reporting without restoring would redden every following test.

    The teardown hook restores before it fails, so exactly one test carries the
    blame. Without the restore, the stub stays in the consumer namespace and the
    next test, and the one after it, fail in teardown for a leak they did not
    cause.
    """

    module = importlib.import_module(_PROBE_MODULE)

    class _ForeignCoordinator:
        """Stand-in for a stub that outlived its monkeypatch window."""

    with monkeypatch.context() as patcher:
        patcher.setattr(module, "GoogleFindMyCoordinator", _ForeignCoordinator)
        assert detect_coordinator_identity_leaks() == [_PROBE_MODULE]

        _restore_coordinator_identity([_PROBE_MODULE])

        assert module.GoogleFindMyCoordinator is not _ForeignCoordinator
        assert detect_coordinator_identity_leaks() == []


def test_restoring_tolerates_a_module_that_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A name that vanished from ``sys.modules`` must not raise in teardown.

    The list of leaks is taken before the restore runs; a test that removes a
    module in between would otherwise turn a diagnostic into a crash.
    """

    monkeypatch.delitem(sys.modules, _PROBE_MODULE, raising=False)

    _restore_coordinator_identity([_PROBE_MODULE])


def test_guard_ignores_the_lazily_bound_package_symbol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The integration package rebinds its symbol lazily and is exempt.

    ``custom_components.googlefindmy`` starts with a placeholder class and swaps
    in the real one via ``_ensure_runtime_imports()``.  The exemption carries
    this case on its own, independently of the healing helper in the teardown
    hook; counting it as a leak would fail tests over a benign, self-repairing
    condition.
    """

    package = sys.modules["custom_components.googlefindmy"]
    if getattr(package, "GoogleFindMyCoordinator", None) is None:
        pytest.skip("integration package has not bound the symbol yet")

    class _PlaceholderCoordinator:
        """Mimics the package's own placeholder class."""

    with monkeypatch.context() as patcher:
        patcher.setattr(package, "GoogleFindMyCoordinator", _PlaceholderCoordinator)
        assert detect_coordinator_identity_leaks() == []


_N = "import_coordinator_consumers"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (f"def {_N}():\n    pass", 1),
        (f"{_N} = None", 1),
        (f'globals()["{_N}"] = None', 1),
        (f'setattr(sys.modules[__name__], "{_N}", f)', 1),
        (f'delattr(sys.modules[__name__], "{_N}")', 1),
        (f'builtins.setattr(sys.modules[__name__], "{_N}", f)', 1),
        (f'builtins.delattr(sys.modules[__name__], "{_N}")', 1),
        (f'operator.setitem(globals(), "{_N}", f)', 1),
        (f'operator.delitem(globals(), "{_N}")', 1),
        (f"sys.modules[__name__].{_N} = f", 1),
        (f"del sys.modules[__name__].{_N}", 1),
        (f"globals().update({_N}=f)", 1),
        (f'globals().update({{"{_N}": f}})', 1),
        (f'globals().__setitem__("{_N}", f)', 1),
        (f'globals().setdefault("{_N}", f)', 1),
        (f'globals().pop("{_N}")', 1),
        (f"try:\n    pass\nexcept E as {_N}:\n    pass", 1),
        (f"match x:\n    case [*{_N}]:\n        pass", 1),
        (f"match x:\n    case {{**{_N}}}:\n        pass", 1),
        (f"match x:\n    case _ as {_N}:\n        pass", 1),
        (f"def g({_N}):\n    pass", 1),
        (f"f({_N})", 0),
        (f'print("{_N}")', 0),
        (f'getattr(sys.modules[__name__], "{_N}")', 0),
    ],
)
def test_bindings_of_sees_every_binding_form(source: str, expected: int) -> None:
    """Each binding, deletion or rebinding counts once; a read counts zero.

    ``setattr(sys.modules[__name__], ...)`` used to stay invisible, so a second
    definition of the helper through it passed the "bound exactly once" check.
    """

    assert len(_bindings_of(ast.parse(source), _N)) == expected
