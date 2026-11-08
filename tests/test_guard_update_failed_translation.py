# tests/test_guard_update_failed_translation.py
"""Guard: every ``UpdateFailed`` of the integration is translatable.

Home Assistant copies ``translation_key`` of an ``UpdateFailed`` into the
setup-retry reason of the integration card when the first refresh fails, and
the frontend shows that reason in the user's language. A raise without a key
falls back to the English text again. The checks run on the syntax tree of
every module under ``custom_components/googlefindmy``:

(a) coverage: each ``UpdateFailed(...)`` call passes a message text (not a
    starred argument, not an empty string or ``f""``), and ``translation_domain`` as the
    name ``DOMAIN`` and a ``translation_key`` (without a text, ``str()`` is
    built from the key, which is the bare key while no translations are
    cached). No module may hide a raise from this scan: an import under an
    alias, a subclass, ``getattr(…, "UpdateFailed")``, a subscript with the
    key ``"UpdateFailed"`` (``uc.__dict__[…]``, ``vars(uc)[…]``), keywords
    passed through a ``**`` mapping, ``type(err)(…)`` or ``err.__class__(…)``
    inside an ``except`` clause that catches the class as ``err`` (also
    through a module tuple holding it, and also after ``cls = type(err)`` in
    that clause), or any use
    of the class as a value (``UF = UpdateFailed``,
    ``functools.partial(UpdateFailed)``, a bare ``raise UpdateFailed``, a
    ``:=`` inside an annotation) fails. Allowed positions are the called
    function, base classes, annotations, the first argument of ``cast``, and
    the type positions ``except``, ``case UpdateFailed()``, the second argument
    of ``isinstance``/``issubclass`` and the arguments of ``suppress``, each as
    a name, a tuple or a ``|`` union. A module-level tuple holding the class is
    allowed while every use of its name in the package is such a type
    position. At least ``_MIN_CALLS`` calls must be found, so a wrong path
    cannot turn the guard green by finding nothing;
(b) parity: the key is a string literal, ``strings.json`` and every file in
    ``translations/`` define ``exceptions.<key>.message``, the template in
    ``strings.json`` uses exactly the placeholders the call passes, and no
    locale uses one the call does not pass;
(c) privacy: placeholder names come from ``_ALLOWED_PLACEHOLDERS``, and no
    placeholder value reads an identifier, or a string key of a subscript or
    of ``get``/``getattr``/``pop``/``setdefault``, that has ``name``,
    ``title`` or ``email`` as a word (``dev_name``, ``devName``,
    ``device["name"]``; not ``_namespace``; dunders such as ``__name__``
    excepted). A device name is derived information under ``AGENTS.md``
    section 5 (b) and would land in the integration card and the log; devices
    are named by ID.

Not seen: ``getattr`` or a subscript with a computed name; ``type(x)(…)`` and
``x.__class__(…)`` outside a handler that binds ``x`` to the class (they would
hit unrelated code such as ``type(message)()``), or on a copy of the bound name
(``other = err``); a module tuple re-exported
under another name or held in a class or function scope (reported, not
allowed); code built at run time (``eval``, ``exec``); a placeholder value
whose word is glued to another (``devname``, ``username``) or that reaches the
name through a variable or another call. Each arm has a self-test on a
synthetic source that must fail, and each allowed form one that must pass.
"""

from __future__ import annotations

import ast
import json
import re
import string
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

_PACKAGE = Path(__file__).resolve().parents[1] / "custom_components" / "googlefindmy"
_MIN_CALLS = 15
_ALLOWED_PLACEHOLDERS = frozenset(
    {"device_id", "status", "error_type", "not_accepted", "rejected"}
)
_FORBIDDEN_VALUE_WORDS = ("name", "title", "email")
_CLASS_NAME = "UpdateFailed"


@dataclass
class _Call:
    where: str
    keywords: dict[str, ast.expr]
    has_text: bool


@dataclass
class _Scan:
    calls: list[_Call] = field(default_factory=list)
    hidden: list[str] = field(default_factory=list)


def _name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _type_expression_ids(node: ast.expr) -> set[int]:
    """Return the ids of a type expression, without assignment expressions.

    An annotation or the first argument of ``cast`` names classes without
    calling them; a ``:=`` inside binds a value and is not part of the type.
    """

    ids = {id(sub) for sub in ast.walk(node)}
    for sub in ast.walk(node):
        if isinstance(sub, ast.NamedExpr):
            ids -= {id(inner) for inner in ast.walk(sub.value)}
            ids.discard(id(sub.target))
    return ids


def _allowed_positions(tree: ast.AST) -> set[int]:
    """Return the ids of nodes where naming the class hides no raise.

    Type positions are the class pattern of a ``case``, ``except`` clauses,
    the second argument of ``isinstance``/``issubclass`` and the arguments of
    ``suppress``, each as a name, a tuple or a ``|`` union of those, plus
    annotations and the first argument of ``cast``.
    """

    allowed: set[int] = set()

    def _type_spec(node: ast.expr | None) -> None:
        if node is None:
            return
        allowed.add(id(node))
        if isinstance(node, ast.Tuple):
            for elt in node.elts:
                _type_spec(elt)
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
            _type_spec(node.left)
            _type_spec(node.right)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            allowed.add(id(node.func))
            called = _name(node.func)
            if called in {"isinstance", "issubclass"} and len(node.args) == 2:
                _type_spec(node.args[1])
            elif called == "suppress":
                for arg in node.args:
                    _type_spec(arg)
            elif called == "cast" and node.args:
                allowed.update(_type_expression_ids(node.args[0]))
        elif isinstance(node, ast.ExceptHandler):
            _type_spec(node.type)
        elif isinstance(node, ast.ClassDef):
            allowed.update(id(base) for base in node.bases)
        elif isinstance(node, ast.MatchClass):
            allowed.add(id(node.cls))
        annotations: list[ast.expr | None] = []
        if isinstance(node, ast.arg):
            annotations.append(node.annotation)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            annotations.append(node.returns)
        elif isinstance(node, ast.AnnAssign):
            annotations.append(node.annotation)
        for annotation in annotations:
            if annotation is not None:
                allowed.update(_type_expression_ids(annotation))
    return allowed


def _names_the_class(node: ast.expr) -> bool:
    return _name(node) == _CLASS_NAME


def _module_tuples(tree: ast.Module) -> dict[str, ast.Tuple]:
    """Return module-level names bound to a tuple that contains the class."""

    tuples: dict[str, ast.Tuple] = {}
    for stmt in tree.body:
        if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
            target, value = stmt.targets[0], stmt.value
        elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
            target, value = stmt.target, stmt.value
        else:
            continue
        if (
            isinstance(target, ast.Name)
            and isinstance(value, ast.Tuple)
            and any(_names_the_class(elt) for elt in value.elts)
        ):
            tuples[target.id] = value
    return tuples


def _tuple_used_as_type_only(
    name: str, trees: dict[str, ast.Module], allowed: dict[str, set[int]]
) -> bool:
    """Return True when every use of ``name`` in every module is a type position.

    An import under another name is not followed, so it disqualifies the tuple.
    """

    for path, tree in trees.items():
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == name and alias.asname not in (None, name)
                for alias in node.names
            ):
                return False
            loads = (
                isinstance(node, ast.Name)
                and node.id == name
                and isinstance(node.ctx, ast.Load)
            ) or (
                isinstance(node, ast.Attribute)
                and node.attr == name
                and isinstance(node.ctx, ast.Load)
            )
            if loads and id(node) not in allowed[path]:
                return False
    return True


def _caught_names(handler: ast.ExceptHandler, tuples: frozenset[str]) -> bool:
    """The handler catches the class, directly or through a module tuple."""

    node = handler.type
    if node is None:
        return False
    elts = node.elts if isinstance(node, ast.Tuple) else [node]
    return any(
        _names_the_class(elt) or (isinstance(elt, ast.Name) and elt.id in tuples)
        for elt in elts
    )


def _class_aliases(body: list[ast.stmt], caught: str) -> set[str]:
    """Names a handler body binds to ``type(err)`` or ``err.__class__``."""

    aliases: set[str] = set()
    for stmt in body:
        for sub in ast.walk(stmt):
            if isinstance(sub, ast.Assign) and _recreates_caught(sub.value, caught):
                aliases.update(
                    target.id for target in sub.targets if isinstance(target, ast.Name)
                )
            elif (
                isinstance(sub, ast.AnnAssign)
                and sub.value is not None
                and isinstance(sub.target, ast.Name)
                and _recreates_caught(sub.value, caught)
            ):
                aliases.add(sub.target.id)
    return aliases


def _recreates_caught(func: ast.expr, caught: str) -> bool:
    """``type(err)`` or ``err.__class__`` for the name bound by the handler."""

    if (
        isinstance(func, ast.Call)
        and _name(func.func) == "type"
        and len(func.args) == 1
        and isinstance(func.args[0], ast.Name)
        and func.args[0].id == caught
    ):
        return True
    return (
        isinstance(func, ast.Attribute)
        and func.attr == "__class__"
        and isinstance(func.value, ast.Name)
        and func.value.id == caught
    )


def _has_text(node: ast.Call) -> bool:
    """A starred first argument or an empty string literal is no message text."""

    if not node.args:
        return False
    first = node.args[0]
    if isinstance(first, ast.Starred):
        return False
    if isinstance(first, ast.JoinedStr):
        # ``f""`` and an f-string of empty literal parts render nothing.
        return not all(
            isinstance(part, ast.Constant) and part.value == "" for part in first.values
        )
    return not (isinstance(first, ast.Constant) and first.value == "")


def _scan(sources: dict[str, str]) -> _Scan:
    scan = _Scan()
    trees = {path: ast.parse(source) for path, source in sorted(sources.items())}
    allowed = {path: _allowed_positions(tree) for path, tree in trees.items()}
    tuple_names = frozenset(
        name for tree in trees.values() for name in _module_tuples(tree)
    )
    for path, tree in trees.items():
        for name, value in _module_tuples(tree).items():
            if _tuple_used_as_type_only(name, trees, allowed):
                allowed[path].update(id(elt) for elt in value.elts)
    for path, tree in trees.items():
        for node in ast.walk(tree):
            where = f"{path}:{getattr(node, 'lineno', 0)}"
            if (
                isinstance(node, ast.ExceptHandler)
                and node.name
                and _caught_names(node, tuple_names)
            ):
                aliases = _class_aliases(node.body, node.name)
                scan.hidden += [
                    f"{path}:{sub.lineno} re-creates the caught UpdateFailed "
                    f"through {ast.unparse(sub.func)}"
                    for stmt in node.body
                    for sub in ast.walk(stmt)
                    if isinstance(sub, ast.Call)
                    and (
                        _recreates_caught(sub.func, node.name)
                        or (isinstance(sub.func, ast.Name) and sub.func.id in aliases)
                    )
                ]
            if isinstance(node, ast.ImportFrom):
                scan.hidden += [
                    f"{where} imports UpdateFailed as {alias.asname}"
                    for alias in node.names
                    if alias.name == _CLASS_NAME and alias.asname
                ]
            elif isinstance(node, ast.ClassDef):
                if any(_name(base) == _CLASS_NAME for base in node.bases):
                    scan.hidden.append(f"{where} subclasses UpdateFailed")
            elif isinstance(node, ast.Subscript):
                if (
                    isinstance(node.slice, ast.Constant)
                    and node.slice.value == _CLASS_NAME
                ):
                    scan.hidden.append(f"{where} looks UpdateFailed up by name")
            elif isinstance(node, ast.Call):
                if _name(node.func) == _CLASS_NAME:
                    keywords = {kw.arg: kw.value for kw in node.keywords if kw.arg}
                    if any(kw.arg is None for kw in node.keywords):
                        scan.hidden.append(
                            f"{where} passes keywords through a ** mapping"
                        )
                    scan.calls.append(_Call(where, keywords, _has_text(node)))
                elif _name(node.func) == "getattr" and any(
                    isinstance(arg, ast.Constant) and arg.value == _CLASS_NAME
                    for arg in node.args
                ):
                    scan.hidden.append(f"{where} looks UpdateFailed up by name")
            elif (
                isinstance(node, (ast.Name, ast.Attribute))
                and _name(node) == _CLASS_NAME
                and isinstance(node.ctx, ast.Load)
                and id(node) not in allowed[path]
            ):
                scan.hidden.append(f"{where} uses UpdateFailed as a value")
    return scan


def _package_sources() -> dict[str, str]:
    return {
        str(path.relative_to(_PACKAGE.parent)): path.read_text(encoding="utf-8")
        for path in sorted(_PACKAGE.rglob("*.py"))
    }


def _package_messages() -> list[tuple[str, dict[str, Any]]]:
    files = [
        _PACKAGE / "strings.json",
        *sorted((_PACKAGE / "translations").glob("*.json")),
    ]
    return [
        (path.name, json.loads(path.read_text(encoding="utf-8")).get("exceptions", {}))
        for path in files
    ]


def _coverage_problems(scan: _Scan) -> list[str]:
    problems = list(scan.hidden)
    for call in scan.calls:
        domain = call.keywords.get("translation_domain")
        if not call.has_text:
            problems.append(f"{call.where} has no message text")
        if "translation_key" not in call.keywords:
            problems.append(f"{call.where} has no translation_key")
        if not (isinstance(domain, ast.Name) and domain.id == "DOMAIN"):
            problems.append(f"{call.where} does not pass translation_domain=DOMAIN")
    return problems


def _given_placeholders(call: _Call) -> set[str] | None:
    """Return the passed placeholder names, or ``None`` if they are not literal."""

    node = call.keywords.get("translation_placeholders")
    if node is None or (isinstance(node, ast.Constant) and node.value is None):
        return set()
    if isinstance(node, ast.Dict) and all(
        isinstance(key, ast.Constant) and isinstance(key.value, str)
        for key in node.keys
    ):
        return {key.value for key in node.keys if isinstance(key, ast.Constant)}
    return None


def _fields(message: str) -> set[str]:
    return {name for _, name, _, _ in string.Formatter().parse(message) if name}


def _parity_problems(
    scan: _Scan, messages: list[tuple[str, dict[str, Any]]]
) -> list[str]:
    problems: list[str] = []
    for call in scan.calls:
        key = call.keywords.get("translation_key")
        if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
            problems.append(f"{call.where} translation_key is not a string literal")
            continue
        given = _given_placeholders(call)
        if given is None:
            problems.append(f"{call.where} translation_placeholders is not literal")
            continue
        for index, (name, exceptions) in enumerate(messages):
            message = exceptions.get(key.value, {}).get("message")
            if message is None:
                problems.append(f"{call.where} {key.value} missing in {name}")
                continue
            used = _fields(message)
            if used - given or (index == 0 and used != given):
                problems.append(
                    f"{call.where} {key.value} placeholders {sorted(used)} in {name}, "
                    f"call passes {sorted(given)}"
                )
    return problems


_WORD_RE = re.compile(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|[0-9]+")
_LOOKUP_CALLS = frozenset({"get", "getattr", "pop", "setdefault"})


def _words(text: str) -> set[str]:
    """Split an identifier or key into lowercase words (``dev_name``, ``devName``)."""

    return {word.lower() for word in _WORD_RE.findall(text)}


def _read_words(value: ast.expr) -> dict[str, set[str]]:
    """Return what a placeholder value reads, with the words of each item.

    Items are identifiers (dunders such as ``type(err).__name__`` excepted, they
    name a class) and string keys of a subscript or of ``get``/``getattr``/
    ``pop``/``setdefault``.
    """

    read: dict[str, set[str]] = {}
    for sub in ast.walk(value):
        if isinstance(sub, (ast.Name, ast.Attribute)):
            ident = _name(sub)
            if ident is not None and not (
                ident.startswith("__") and ident.endswith("__")
            ):
                read[ident.lower()] = _words(ident)
        if isinstance(sub, ast.Subscript):
            keys: list[ast.expr] = [sub.slice]
        elif isinstance(sub, ast.Call) and _name(sub.func) in _LOOKUP_CALLS:
            keys = list(sub.args)
        else:
            keys = []
        for key in keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                read[repr(key.value)] = _words(key.value)
    return read


def _placeholder_name_problems(scan: _Scan) -> list[str]:
    problems: list[str] = []
    for call in scan.calls:
        given = _given_placeholders(call) or set()
        problems += [
            f"{call.where} placeholder {name!r} is not allowed"
            for name in sorted(given - _ALLOWED_PLACEHOLDERS)
        ]
        node = call.keywords.get("translation_placeholders")
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                read = _read_words(value)
                if any(words & set(_FORBIDDEN_VALUE_WORDS) for words in read.values()):
                    label = key.value if isinstance(key, ast.Constant) else "?"
                    problems.append(
                        f"{call.where} placeholder {label!r} reads {sorted(read)}"
                    )
    return problems


# --- the guard ---------------------------------------------------------------


def test_every_update_failed_is_translatable() -> None:
    scan = _scan(_package_sources())
    assert len(scan.calls) >= _MIN_CALLS, (
        f"found only {len(scan.calls)} UpdateFailed calls under {_PACKAGE}"
    )
    assert _coverage_problems(scan) == []


def test_keys_and_placeholders_match_every_locale() -> None:
    messages = _package_messages()
    assert len(messages) == 11, [name for name, _ in messages]
    assert _parity_problems(_scan(_package_sources()), messages) == []


def test_placeholder_names_are_allowed() -> None:
    assert _placeholder_name_problems(_scan(_package_sources())) == []


# --- self-tests: every arm must fail on a synthetic source -------------------

_GOOD = """
from homeassistant.helpers.update_coordinator import UpdateFailed
from .const import DOMAIN
raise UpdateFailed("x", translation_domain=DOMAIN, translation_key="k",
                   translation_placeholders={"status": "500"})
"""
_MESSAGES = [
    ("strings.json", {"k": {"message": "Error {status}."}}),
    ("de.json", {"k": {"message": "Fehler {status}."}}),
]


def test_self_test_good_source_passes_every_arm() -> None:
    scan = _scan({"good.py": _GOOD})
    assert len(scan.calls) == 1
    assert _coverage_problems(scan) == []
    assert _parity_problems(scan, _MESSAGES) == []
    assert _placeholder_name_problems(scan) == []


def test_self_test_coverage_arm_fails() -> None:
    sources = {
        "nokey.py": 'raise UpdateFailed("x", translation_domain=DOMAIN)',
        "literal.py": 'raise UpdateFailed("x", translation_domain="googlefindmy", translation_key="k")',
        "notext.py": 'raise UpdateFailed(translation_domain=DOMAIN, translation_key="k")',
        "alias.py": "from homeassistant.helpers.update_coordinator import UpdateFailed as UF",
        "sub.py": "class Mine(UpdateFailed):\n    pass",
        "assign.py": 'UF = UpdateFailed\nraise UF("x")',
        "partial.py": 'mk = functools.partial(UpdateFailed)\nraise mk("x")',
        "bare.py": "raise UpdateFailed",
        "barefrom.py": "raise UpdateFailed from err",
        "getattr.py": 'raise getattr(uc, "UpdateFailed")("x")',
        "attr.py": "UF = update_coordinator.UpdateFailed",
    }
    problems = _coverage_problems(_scan(sources))
    assert any("nokey.py" in p and "translation_key" in p for p in problems)
    assert any("literal.py" in p and "DOMAIN" in p for p in problems)
    assert any("notext.py" in p and "message text" in p for p in problems)
    for name in (
        "alias",
        "sub",
        "assign",
        "partial",
        "bare",
        "barefrom",
        "getattr",
        "attr",
    ):
        assert any(p.startswith(f"{name}.py:") for p in problems), name


def test_self_test_allowed_positions_pass() -> None:
    """Naming the class where it raises nothing must not be reported."""

    source = """
from homeassistant.helpers.update_coordinator import UpdateFailed
def check(err: UpdateFailed | None) -> UpdateFailed | None:
    try:
        pass
    except (UpdateFailed, ValueError):
        pass
    except UpdateFailed as exc:
        del exc
    result: list[UpdateFailed] = []
    del result
    return err if isinstance(err, UpdateFailed) else None
"""
    scan = _scan({"ok.py": source})
    assert scan.calls == []
    assert _coverage_problems(scan) == []


def test_self_test_parity_arm_fails() -> None:
    scan = _scan({"good.py": _GOOD})
    missing_locale = [*_MESSAGES, ("fr.json", {})]
    extra_in_locale = [*_MESSAGES, ("fr.json", {"k": {"message": "{status} {x}"}})]
    fewer_in_template = [("strings.json", {"k": {"message": "Error."}})]
    assert any(
        "missing in fr.json" in p for p in _parity_problems(scan, missing_locale)
    )
    assert any("fr.json" in p for p in _parity_problems(scan, extra_in_locale))
    assert any("strings.json" in p for p in _parity_problems(scan, fewer_in_template))
    computed = _scan(
        {
            "key.py": "raise UpdateFailed('x', translation_domain=DOMAIN, translation_key=k)"
        }
    )
    assert any(
        "not a string literal" in p for p in _parity_problems(computed, _MESSAGES)
    )


def test_self_test_placeholder_names_arm_fails() -> None:
    source = (
        "raise UpdateFailed('x', translation_domain=DOMAIN, translation_key='k', "
        "translation_placeholders={'name': str(dev_id)})"
    )
    problems = _placeholder_name_problems(_scan({"name.py": source}))
    assert problems == ["name.py:1 placeholder 'name' is not allowed"]


def test_self_test_placeholder_values_arm_fails() -> None:
    sources = {
        "dev.py": "raise UpdateFailed('x', translation_domain=DOMAIN, translation_key='k', "
        "translation_placeholders={'device_id': dev_name})",
        "title.py": "raise UpdateFailed('x', translation_domain=DOMAIN, translation_key='k', "
        "translation_placeholders={'device_id': str(entry.title)})",
        "mail.py": "raise UpdateFailed('x', translation_domain=DOMAIN, translation_key='k', "
        "translation_placeholders={'status': user_email})",
    }
    problems = _placeholder_name_problems(_scan(sources))
    assert [p.split(":")[0] for p in problems] == ["dev.py", "mail.py", "title.py"]
    class_name = _scan(
        {
            "cls.py": "raise UpdateFailed('x', translation_domain=DOMAIN, translation_key='k', "
            "translation_placeholders={'error_type': type(err).__name__})"
        }
    )
    assert _placeholder_name_problems(class_name) == []


# --- self-tests: forms the scan must report, and forms it must allow ----------

_HEAD = (
    "from homeassistant.helpers.update_coordinator import UpdateFailed\n"
    "from .const import DOMAIN\n"
)
_KEYED = 'translation_domain=DOMAIN, translation_key="k"'

_REPORTED_FORMS = {
    "starstar_next_to_key": (
        f'raise UpdateFailed("x", {_KEYED}, '
        '**{"translation_placeholders": {"device_id": dev_name}})',
        "** mapping",
    ),
    "dunder_dict_lookup": ('raise uc.__dict__["UpdateFailed"]("x")', "by name"),
    "vars_lookup": ('raise vars(uc)["UpdateFailed"]("x")', "by name"),
    "type_of_caught": (
        'try:\n    pass\nexcept UpdateFailed as err:\n    raise type(err)("x")',
        "re-creates",
    ),
    "class_of_caught": (
        "try:\n    pass\nexcept (ValueError, UpdateFailed) as err:\n"
        '    raise err.__class__("x")',
        "re-creates",
    ),
    "starred_text": (f"raise UpdateFailed(*(), {_KEYED})", "message text"),
    "empty_text": (f'raise UpdateFailed("", {_KEYED})', "message text"),
    "value_by_subscript": (
        f'raise UpdateFailed("x", {_KEYED}, '
        'translation_placeholders={"device_id": device["name"]})',
        "reads",
    ),
    "value_by_get": (
        f'raise UpdateFailed("x", {_KEYED}, '
        'translation_placeholders={"device_id": device.get("display_name")})',
        "reads",
    ),
    "walrus_in_annotation": (
        'x: (UF := UpdateFailed) = None\nraise UF("x")',
        "as a value",
    ),
    "type_of_caught_through_module_tuple": (
        "_RETRY = (UpdateFailed, TimeoutError)\n"
        'try:\n    pass\nexcept _RETRY as err:\n    raise type(err)("x")',
        "re-creates",
    ),
    "class_of_caught_through_annotated_tuple": (
        "_R: tuple[type[Exception], ...] = (UpdateFailed,)\n"
        'try:\n    pass\nexcept _R as e:\n    raise e.__class__("x")',
        "re-creates",
    ),
    "type_of_caught_bound_first": (
        'try:\n    pass\nexcept UpdateFailed as err:\n    cls = type(err)\n    raise cls("x")',
        "re-creates",
    ),
    "empty_fstring_text": (f'raise UpdateFailed(f"", {_KEYED})', "message text"),
    "module_tuple_indexed": (
        '_RETRY = (UpdateFailed, TimeoutError)\nraise _RETRY[0]("x")',
        "as a value",
    ),
}


@pytest.mark.parametrize("form", sorted(_REPORTED_FORMS))
def test_self_test_form_is_reported(form: str) -> None:
    """Each form hides a raise or a device name; the scan must report it."""

    source, fragment = _REPORTED_FORMS[form]
    scan = _scan({f"{form}.py": _HEAD + source})
    problems = _coverage_problems(scan) + _placeholder_name_problems(scan)

    assert any(fragment in problem for problem in problems), problems


_ALLOWED_FORMS = {
    "isinstance_union": "ok = isinstance(err, ValueError | UpdateFailed)",
    "issubclass_nested": "ok = issubclass(cls, (KeyError, ValueError | UpdateFailed))",
    "suppress": "with contextlib.suppress(UpdateFailed, TimeoutError):\n    pass",
    "cast": "failure = cast(UpdateFailed, err)",
    "match_case": "match err:\n    case UpdateFailed():\n        pass",
    "module_tuple_in_except": (
        "_RETRY = (UpdateFailed, TimeoutError)\n"
        "try:\n    pass\nexcept _RETRY:\n    pass\n"
        "ok = isinstance(err, _RETRY)"
    ),
    "namespace_value": (
        f'raise UpdateFailed("x", {_KEYED}, '
        'translation_placeholders={"device_id": self._namespace()})'
    ),
    "type_outside_handler": "copy = type(message)()",
}


@pytest.mark.parametrize("form", sorted(_ALLOWED_FORMS))
def test_self_test_form_is_allowed(form: str) -> None:
    """Each form names the class or a word without raising or leaking."""

    scan = _scan({f"{form}.py": _HEAD + _ALLOWED_FORMS[form]})
    messages = [("strings.json", {"k": {"message": "E {device_id}."}})]

    assert _coverage_problems(scan) == []
    assert _placeholder_name_problems(scan) == []
    assert _parity_problems(scan, messages) == []


def test_self_test_module_tuple_imported_and_indexed_is_reported() -> None:
    """A module tuple stays allowed only while every use is a type position."""

    sources = {
        "errors.py": _HEAD + "_RETRY = (UpdateFailed, TimeoutError)\n",
        "user.py": 'from .errors import _RETRY\nraise _RETRY[0]("x")\n',
    }

    problems = _coverage_problems(_scan(sources))

    assert any(p.startswith("errors.py:") and "as a value" in p for p in problems)
