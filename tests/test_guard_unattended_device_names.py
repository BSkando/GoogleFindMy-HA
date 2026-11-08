# tests/test_guard_unattended_device_names.py
"""Guard: five unattended paths name no device above DEBUG.

`AGENTS.md` section 5 (b): a record that can repeat unattended keeps the
user-chosen device name out of the record at INFO and above and carries it
only at DEBUG. This guard pins that for five functions that run without a
user action and used to log the display name at INFO, WARNING or ERROR:

* `api.py` `GoogleFindMyAPI.async_get_device_location` (the location
  lookup shared by every poll cycle and the manual locate);
* `ProtoDecoders/decoder.py` `get_devices_with_location` (the device list
  decoder, run on each unattended device list refresh);
* `coordinator/registry.py` `_heal_tracker_device_subentry`, nested in
  `RegistryOperations._ensure_registry_for_devices` (the registry sync of
  every poll cycle);
* `coordinator/polling.py` `PollingOperations._async_start_poll_cycle` (the
  per-device error and status records of every poll cycle);
* `google_home_filter.py` `GoogleHomeFilter.should_filter_detection` (the
  Google Home spam filter, run on every location update; its records named
  the Google Home device label).

Inside each function, every log call whose level is INFO or above is
checked.

Log calls, as a closed list: a call whose callee is `<logger>.<method>`,
`getattr(<logger>, <method>)` with or without a default, a conditional
expression with at least one branch of these forms
(`(_LOGGER.warning if c else _LOGGER.error)(...)`), or any of these followed
by `.__call__`, once or more. `<logger>` is a name or attribute whose last
part is `log` or `logger` in any case, leading underscores ignored
(`_LOGGER`, `LOGGER`, `self.logger`, `self.log`), the module `logging`
itself (`logging.warning(...)`), or a call to a function or method named
`getLogger` or `getChild`. No other callee is read.

Levels: `info`, `warning`, `warn`, `error`, `exception`, `critical` and
`fatal` are above DEBUG. For `log`, the level passed positionally or as
`level=` decides: `logging.DEBUG`, `DEBUG`, `logging.NOTSET`, `NOTSET` or an
integer constant up to 10 is DEBUG and skipped; any other level, a variable
included, counts as above DEBUG, and so does a `log` call whose level is not
visible (none given, or passed through `*args` or `**kwargs`). A `getattr`
whose method name is not a string constant counts as above DEBUG. When a
conditional expression mixes `log` with another method, no argument is read
as a level. Each of
these rules errs towards a false alarm, not towards a miss.

Checked in each log call: all positional arguments, a `*args` argument
included, with the format argument as an f-string or the right side of a
`%` format, and all keyword values; a visible level argument is not checked
for names. An
offender is any leaf below them that is a name, an attribute (other than
`__name__`), a subscript with a string key, or a string constant passed to a
nested call as a positional or keyword argument
(`getattr(device, "userDefinedDeviceName", "")`, `device.get("name")`,
`device.get(key="name")`), whose spelling contains `name` or `label`, in any
case. The three field-name records of the missing-key
dump in the decoder log protobuf field names (`f.name`), not device names;
they are listed in `_FIELD_NAME_ROWS` with path, leaf and format prefix, and
each entry must match exactly one call so that a copied line does not
inherit the exception.

Written as a pin on five functions rather than a package-wide rule,
because the package still has records bound to one invocation (buttons,
services, the manual locate) that may name the device by section 5 (b), and
a package-wide name predicate would need an exception list longer than the
rule.

Blind spots, declared: every callee outside the closed list above (a logger
under another name, a logger factory, a method bound to a local name or
taken from a container, `functools.partial`, a lambda); a value copied into
a variable, an attribute or a subscript whose spelling has neither `name`
nor `label` (`fields = ...` in the decoder is such a case and is correct;
`self._last = device_name` followed by a record with `self._last` would be
a miss); a device object whose spelling has neither word rendered whole
(`str(device)`, `repr(device)`, `%s` or `%r` with `device`, `{device!r}`);
an exception whose own text carries the name; and records in helpers these
functions call. The caplog tests in `tests/test_api_location_name_at_debug.py` and
`tests/test_decoder_registry_name_at_debug.py` pin the record contents;
this guard pins the shape of the call.

Retire this guard when a package-wide device-name guard covers these five
functions.
"""

from __future__ import annotations

import ast
from pathlib import Path

from custom_components import googlefindmy

_PACKAGE_ROOT = Path(googlefindmy.__file__).resolve().parent

_LEVELS_ABOVE_DEBUG = frozenset(
    {"info", "warning", "warn", "error", "exception", "critical", "fatal"}
)

# Level arguments of ``.log(level, ...)`` that are DEBUG or below.
_DEBUG_LEVEL_SPELLINGS = frozenset(
    {"logging.DEBUG", "DEBUG", "logging.NOTSET", "NOTSET"}
)

# (path relative to the package, qualified name, minimum number of log calls
# above DEBUG measured on 2026-10-18, the last two rows on 2026-11-08). The
# minimum is the vacuum guard: a function that shrinks below it was renamed,
# moved or rewritten, and the guard would no longer see what it was written
# for.
_UNATTENDED_FUNCTIONS: tuple[tuple[str, str, int], ...] = (
    ("api.py", "GoogleFindMyAPI.async_get_device_location", 11),
    ("ProtoDecoders/decoder.py", "get_devices_with_location", 7),
    (
        "coordinator/registry.py",
        "RegistryOperations._ensure_registry_for_devices._heal_tracker_device_subentry",
        1,
    ),
    ("coordinator/polling.py", "PollingOperations._async_start_poll_cycle", 15),
    ("google_home_filter.py", "GoogleHomeFilter.should_filter_detection", 2),
)

# Records that log protobuf field names, reviewed on 2026-10-18
# (PLAN_GFMY_LOG_NAME_REST AP1, class C).
_FIELD_NAME_ROWS: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("ProtoDecoders/decoder.py", "f.name", " -> Info fields"),
        ("ProtoDecoders/decoder.py", "f.name", " -> Registration fields"),
        ("ProtoDecoders/decoder.py", "f.name", " -> Secrets fields"),
    }
)

Offender = tuple[str, int, str, str]


def _find_function(
    tree: ast.Module, qualname: str
) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    """Return the definition reached by following ``qualname`` from the module."""
    scope: ast.AST = tree
    found: ast.AST | None = None
    for part in qualname.split("."):
        found = None
        for node in ast.walk(scope):
            if node is scope:
                continue
            if (
                isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == part
            ):
                found = node
                break
        if found is None:
            return None
        scope = found
    if isinstance(found, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return found
    return None


_UNKNOWN_METHOD = "?"


_LOGGER_FACTORIES = frozenset({"getLogger", "getChild"})


def _is_logger(receiver: ast.expr) -> bool:
    """True for ``_LOGGER``, ``self.logger``, ``logging`` or ``getLogger(...)``."""
    if isinstance(receiver, ast.Call):
        callee = receiver.func
        if isinstance(callee, ast.Attribute):
            return callee.attr in _LOGGER_FACTORIES
        return isinstance(callee, ast.Name) and callee.id in _LOGGER_FACTORIES
    if isinstance(receiver, ast.Attribute):
        last = receiver.attr
    elif isinstance(receiver, ast.Name):
        if receiver.id == "logging":
            # The module-level functions log through the root logger.
            return True
        last = receiver.id
    else:
        return False
    return last.lstrip("_").lower() in {"log", "logger"}


def _log_methods(func: ast.expr) -> set[str]:
    """Logger methods a callee expression may invoke; empty for other callees.

    ``_UNKNOWN_METHOD`` stands for a ``getattr`` whose name is not a constant.
    """
    if isinstance(func, ast.Attribute):
        if func.attr == "__call__":
            return _log_methods(func.value)
        return {func.attr} if _is_logger(func.value) else set()
    if isinstance(func, ast.IfExp):
        return _log_methods(func.body) | _log_methods(func.orelse)
    if (
        isinstance(func, ast.Call)
        and isinstance(func.func, ast.Name)
        and func.func.id == "getattr"
        and len(func.args) >= 2
        and _is_logger(func.args[0])
    ):
        method = func.args[1]
        if isinstance(method, ast.Constant) and isinstance(method.value, str):
            return {method.value}
        return {_UNKNOWN_METHOD}
    return set()


def _level_argument(node: ast.Call) -> ast.expr | None:
    """The visible level of a ``.log`` call, positional or ``level=``.

    ``None`` when no level is visible: none given, or passed through
    ``*args`` or ``**kwargs``.
    """
    for keyword in node.keywords:
        if keyword.arg == "level":
            return keyword.value
    if node.args and not isinstance(node.args[0], ast.Starred):
        return node.args[0]
    return None


def _is_debug_level(level: ast.expr) -> bool:
    if isinstance(level, ast.Constant):
        return isinstance(level.value, int) and level.value <= 10
    return ast.unparse(level) in _DEBUG_LEVEL_SPELLINGS


def _is_log_call_above_debug(node: ast.Call) -> bool:
    methods = _log_methods(node.func)
    if methods & (_LEVELS_ABOVE_DEBUG | {_UNKNOWN_METHOD}):
        return True
    if "log" in methods:
        level = _level_argument(node)
        # No visible level: counted; with no level at all the call raises.
        return level is None or not _is_debug_level(level)
    return False


def _spells_a_name(text: str) -> bool:
    lowered = text.lower()
    return "name" in lowered or "label" in lowered


def _name_leaves(node: ast.AST) -> set[str]:
    """Leaves under ``node`` whose spelling contains ``name`` or ``label``."""
    found: set[str] = set()
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and _spells_a_name(child.id):
            found.add(child.id)
        elif (
            isinstance(child, ast.Attribute)
            and child.attr != "__name__"
            and _spells_a_name(child.attr)
        ):
            found.add(ast.unparse(child))
        elif isinstance(child, ast.Call):
            # A string key passed to a nested call reads the value by name:
            # ``getattr(device, "userDefinedDeviceName", "")``, ``d.get("name")``,
            # ``d.get(key="name")``.
            for argument in [*child.args, *(kw.value for kw in child.keywords)]:
                if (
                    isinstance(argument, ast.Constant)
                    and isinstance(argument.value, str)
                    and _spells_a_name(argument.value)
                ):
                    found.add(ast.unparse(child))
        elif (
            isinstance(child, ast.Subscript)
            and isinstance(child.slice, ast.Constant)
            and isinstance(child.slice.value, str)
            and _spells_a_name(child.slice.value)
        ):
            found.add(ast.unparse(child))
    return found


def _format_prefix(node: ast.Call) -> str:
    # `.log(level, msg, ...)` carries the format string at index 1, unless the
    # level is passed as `level=`; `msg=` may carry it as a keyword.
    level_positional = _log_methods(node.func) == {"log"} and not any(
        keyword.arg == "level" for keyword in node.keywords
    )
    index = 1 if level_positional else 0
    first = node.args[index] if len(node.args) > index else None
    if first is None:
        first = next((kw.value for kw in node.keywords if kw.arg == "msg"), None)
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value
    return ""


def _is_excepted(
    rows: frozenset[tuple[str, str, str]], relative: str, leaf: str, fmt: str
) -> bool:
    # An empty prefix would excuse every call of the function.
    assert all(prefix for _path, _leaf, prefix in rows), "empty prefix in exceptions"
    return any(
        relative == path and leaf == name and fmt.startswith(prefix)
        for path, name, prefix in rows
    )


def scan(
    rows: frozenset[tuple[str, str, str]] = _FIELD_NAME_ROWS,
) -> tuple[list[Offender], dict[str, int]]:
    """Return ((path, line, leaf, format) offenders, log calls per function)."""
    offenders: list[Offender] = []
    counts: dict[str, int] = {}
    for relative, qualname, _minimum in _UNATTENDED_FUNCTIONS:
        path = _PACKAGE_ROOT / relative
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        function = _find_function(tree, qualname)
        assert function is not None, f"{relative}: {qualname} not found"
        counts[qualname] = 0
        for node in ast.walk(function):
            if not isinstance(node, ast.Call) or not _is_log_call_above_debug(node):
                continue
            counts[qualname] += 1
            fmt = _format_prefix(node)
            # Only an unambiguous `.log` call has a level to skip.
            level = (
                _level_argument(node) if _log_methods(node.func) == {"log"} else None
            )
            leaves: set[str] = set()
            for argument in [*node.args, *(kw.value for kw in node.keywords)]:
                if argument is not level:
                    leaves |= _name_leaves(argument)
            for leaf in sorted(leaves):
                if _is_excepted(rows, relative, leaf, fmt):
                    continue
                offenders.append((relative, node.lineno, leaf, fmt))
    return offenders, counts


def test_functions_are_found() -> None:
    _offenders, counts = scan()
    for _relative, qualname, minimum in _UNATTENDED_FUNCTIONS:
        assert counts[qualname] >= minimum, (
            f"{qualname}: only {counts[qualname]} log calls above DEBUG, expected "
            f"at least {minimum}; the guard would no longer see the records it "
            "was written for"
        )


def test_no_device_name_above_debug() -> None:
    offenders, _counts = scan()
    assert offenders == [], (
        "Log calls above DEBUG on an unattended path name a device (AGENTS.md "
        "section 5 b): name the device by a class (a)/(b) identifier and log the "
        "display name in a DEBUG companion record:\n"
        + "\n".join(
            f"OFFENDER {path}:{line} ({leaf})" for path, line, leaf, _fmt in offenders
        )
    )


def test_exception_rows_match_exactly_one_call() -> None:
    """Each field-name row excuses one vetted call, no more, no less."""
    offenders, _counts = scan(rows=frozenset())
    matches = {
        row: [
            (path, line)
            for path, line, leaf, fmt in offenders
            if _is_excepted(frozenset({row}), path, leaf, fmt)
        ]
        for row in _FIELD_NAME_ROWS
    }
    assert all(len(found) == 1 for found in matches.values()), matches
