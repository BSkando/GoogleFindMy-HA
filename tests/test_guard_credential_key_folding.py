# tests/test_guard_credential_key_folding.py
"""Fork-only extension of the credential guard: keys the module computes.

``tests/test_guard_credential_entry_writes.py`` (upstream) knows a key in four
spellings only and states every computed key as a detection limit (``V-10``
(c)). This file plugs a folding resolver into the same scan, so the fork still
holds the receiver to the computed forms. It imports only underscore names of
the upstream guard and keeps no copy of its rules.

A key is any expression ``_values`` may evaluate to the member: string
constants, ``+``, f-strings without conversion or format spec, names bound at
module level, constants of ``const.py`` reached by import (``from ..const import
X``, ``const.X``) and keyword arguments. A module name holds every value any of
its bindings gives it, in any block that runs at import and in any order, so a
write keyed by ``K`` after ``A = 'fcm_'; K = A + 'credentials'; A = 'x'``
fails. A binding that does not fold makes the name unknown: ``+=``, ``for``,
``with``, ``match``, ``:=`` at any depth of nested comprehensions, ``global``
in a function or class body. So does a cycle other than a plain alias (``A = A
+ 'a'``, ``B = A * 0``), or more than ``_VALUE_LIMIT`` values. Any other
expression is unknown when it reads a module name (``P[0]``, ``'%s' % A``,
``f'{A!s}'``, ``chr(X)``, ``NS.A``) or only constants and builtins
(``'FCM_'.lower()``); one that reads a local or a parameter holds no string. An
unknown key of a subscript store or ``del``, a dict display, ``setdefault`` or
``pop`` fails as unclassified. ``test_python_is_the_oracle_for_module_keys``
runs generated module code in four families and holds the folding to what
Python computes.

A name a class body binds and reads there holds the class value and is
unknown (``class Q: P = 'fcm_'``), and so is a type parameter (``class
Q[fcm_]``), a dunder name the file does not bind (``__doc__``, ``__file__``,
``__class__``) and a comprehension target with any iterable that reads no
local or parameter (``for p in ['']``, also ``for p in data for p in
['fcm_']``) or reads the target itself, directly or through another target
(``for x in A for x in x``, ``for x in data for y in x for x in y``), or may
yield a value no read gives: a constant or a call result as an element
(``[data, 'fcm_']``, ``[data, bytes((102, 99, 109, 95)).decode()]``), also
where a method of a local may return an argument
(``data.get('x', ['fcm_'])``); a method without arguments returns none
(``data.items()``). ``sorted``, ``list``, ``tuple`` and ``reversed`` of one
argument yield its elements where the file does not bind the name
(``sorted(data)``), ``and``/``or`` the elements of each side (``data or
[]``) and a dict display its keys (``{**data}``); any other call
(``dict(data)``), a starred argument (``sorted(data, *[])``) and a repetition
(``data * 2``) may yield another value. What
a subscript or a condition reads is not yielded
(``data['items']``, ``(x for x in data if x != 'skip')``). A name
read inside a subscript target (``for d2[P] in data``) is no target. An
unknown key fails at each write position of ``_Writes``: a
subscript store or ``del`` and the subscripts it writes into (``bundle[C]['gcm']
= v``), a dict display or comprehension, ``setdefault``, ``pop``,
``__setitem__``, ``operator.setitem`` and their unbound forms, also under an
import alias (``import operator as op``, ``from operator import setitem``) and
called on a class (``type(d).__setitem__(d, C, v)``), and the pairs or keys
handed to ``dict``, ``update`` or ``fromkeys``, also behind an operator
(``[(C, v)] * 1``). A method called on a chain of names whose head a class
statement or an import binds, or on an expression that is no chain of names
(a call, a subscript), is read at both positions; a chain over any other head
is taken as an instance with its key first. On a class an instance method
inherited from ``dict`` takes the mapping first and raises on a key there
(``Store.__setitem__(C, creds)``), while ``fromkeys``, a method the class
defines itself or inherits from another base, a classmethod or a staticmethod
may run (``Store.setdefault(C, creds)``), so both positions are read.
The price: a mapping the guard cannot decide fails at
either position of such a receiver, though the code runs
(``Store.__setitem__(CACHE, 'x', creds)`` with ``CACHE = Store()``,
``type(CACHE).__setitem__(CACHE, 'x', creds)``,
``dict().setdefault('x', CACHE)``). A key that reads a comprehension target
over constants that can never give the member fails the same way
(``{p + 'credentials': creds for p in ('a', 'b')}``),
since the guard does not fold a target, and so does a key the value holds
(``{'x': {p + 'credentials': creds} for p in ('a', 'b')}``); a target no key
reads does not (``{'x': p for p in ('a', 'b')}``). A write function
reached other than by a call of it fails
as well (``s = d.__setitem__``, ``getattr(d, '__setitem__')``). A string
longer than the member is dropped from a concatenation, so a name that
doubles per binding stays small.

Not seen here either: a name imported from a module other than ``const.py``,
a mapping or pairs a call builds for ``update`` or ``dict``
(``d.update(zip(...))``, ``dict(zip(...))``, also spread into a display,
``{**dict(zip(...))}``; as the iterable of a target such a call is unknown, see
above), a name bound only at runtime (``globals()``, ``exec``), and a local a
statement binds, even over constants, to a value no literal spells whole
(``for p in ('fcm_',): updated_data[p + 'credentials'] = creds``,
``p = 'FCM_CREDENTIALS'.lower(); updated_data[p] = creds``,
``p = ''.join(['fcm_', 'credentials']); updated_data[p] = creds``,
``for p in ('FCM_CREDENTIALS'.lower(),): updated_data[p] = creds``). A literal
that spells the whole member fails in a binding (``p = 'fcm_credentials'``,
``for p in ('fcm_credentials',): updated_data[p] = creds``), while a membership
test reads it without a finding (``'fcm_credentials' in updated_data``,
``'fcm_credentials' not in creds``). A write function is followed only in the
forms listed above, and a class only as ``type(d)``, ``dict`` or a class of the
file. Not followed, by decision after round 13 instead of form by form: a
write function reached through ``from operator import __setitem__`` or
``__delitem__``, a local
bound to a module (``op = operator``), ``operator.attrgetter``, ``getattr``
with a name that is not a literal (``getattr(d, M)``) or
``__getattribute__``; a class reached through ``d.__class__`` or a name bound
to it (``D = dict``, ``T = type(d)``); pairs handed to ``d |= [(C, v)]``,
``__ior__`` or ``dict.__init__``; a private name read in a class body, which
Python mangles (``__P`` in ``class Q`` reads ``_Q__P``).
"""

from __future__ import annotations

import ast
import builtins
import contextlib
import functools
import io
import pathlib
import re
import sys
import types
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from typing import NamedTuple

import pytest
from hypothesis import assume, event, given, settings
from hypothesis import strategies as st

from tests.test_guard_credential_entry_writes import (
    _ACCEPTED,
    _BROKEN,
    _CHAINS,
    _CONST,
    _MEMBER,
    _NESTED_SCOPES,
    _NOT_SEEN,
    _RECEIVER,
    _SNIPPET_CONSTANTS,
    _at_module_level,
    _bound_by,
    _bound_identifiers,
    _changed,
    _docstring_problems,
    _dotted,
    _executed_work,
    _LiteralKeys,
    _local_reads,
    _outer_parts,
    _parents,
    _parse,
    _shallow_stack,
    _skip_newer_syntax,
)
from tests.test_guard_credential_entry_writes import (
    _receiver_problems as _literal_receiver_problems,
)

#: The adapter interface of the upstream guard, re-exported for tools that run
#: a form catalogue against this file.
__all__ = ["_RECEIVER", "_SNIPPET_CONSTANTS", "_parse", "_receiver_problems"]

#: Expressions that may fold to the member key.
_FOLDABLE = (ast.Constant, ast.Name, ast.Attribute, ast.BinOp, ast.JoinedStr)

_UNKNOWN = None

#: The values a module name may hold, ``_UNKNOWN`` if not bounded.
Values = frozenset[str] | None

#: More values than this make a name unknown.
_VALUE_LIMIT = 32

#: Names Python resolves without any binding in the file.
_BUILTINS = frozenset(dir(builtins))


def _is_implicit(name: str) -> bool:
    """A dunder name: Python binds such names without a statement in a module,
    a class body or a method (``__doc__``, ``__file__``, ``__class__``,
    ``__firstlineno__``, ``__type_params__``), and new versions add more; one
    the file does not bind is unknown (R8-F2, F-D)."""

    return len(name) > 4 and name.startswith("__") and name.endswith("__")


#: Scopes whose body has its own local names (``global`` aside).
_FUNCTION_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)

_COMPREHENSION_NODES = (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)


def _join(left: Values, right: Values) -> Values:
    """Every concatenation of a value of ``left`` with one of ``right``.

    A string longer than the member never becomes it by concatenation, so it is
    dropped; a name that doubles itself per binding stays small (R8-F4).
    """

    if left is _UNKNOWN or right is _UNKNOWN:
        return _UNKNOWN
    limit = len(_MEMBER)
    joined = frozenset(a + b for a in left for b in right if len(a) + len(b) <= limit)
    return _UNKNOWN if len(joined) > _VALUE_LIMIT else joined


def _fold_children(node: ast.AST) -> list[ast.AST] | None:
    """The operands ``node`` folds over, or ``None`` if it does not fold."""

    if isinstance(node, ast.Constant | ast.Name | ast.Attribute):
        return []
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return [node.left, node.right]
    if isinstance(node, ast.JoinedStr):
        children: list[ast.AST] = []
        for part in node.values:
            if isinstance(part, ast.FormattedValue):
                if part.conversion != -1 or part.format_spec is not None:
                    return None
                children.append(part.value)
            else:
                children.append(part)
        return children
    return None


def _module_root(node: ast.AST) -> ast.Name | None:
    """The name at the root of ``a.b.c``, else ``None``."""

    while isinstance(node, ast.Attribute):
        node = node.value
    return node if isinstance(node, ast.Name) else None


def _read_kind(
    node: ast.Name,
    env: Mapping[str, Values],
    local: frozenset[int],
    unknown: frozenset[int],
) -> str:
    """What a name read reads: ``runtime``, ``unknown``, ``module`` or ``builtin``.

    A local, a parameter or a comprehension target over runtime values is
    ``runtime``; a read in ``unknown`` (a class body name, a comprehension
    target over constants or module names) is ``unknown``, and so is a name
    Python binds implicitly (``__doc__``, R8-F2). A name bound nowhere else
    raises ``NameError`` when read, so it holds no string: ``runtime`` as well.
    """

    if id(node) in local:
        return "runtime"
    if id(node) in unknown or (_is_implicit(node.id) and node.id not in env):
        return "unknown"
    if node.id in env:
        return "module"
    if node.id in _BUILTINS:
        return "builtin"
    return "runtime"


def _opaque(
    node: ast.AST,
    env: Mapping[str, Values],
    local: frozenset[int],
    unknown: frozenset[int] = frozenset(),
) -> Values:
    """Approach A (V-8) for an expression that does not fold.

    It is unknown when any of its names is a module name, whatever values that
    name holds (``chr(X)`` with ``X = 102``), or a name ``_read_kind`` calls
    unknown, and when it reads nothing but constants and builtins
    (``'FCM_'.lower()``), unless it is a plain literal. Otherwise it reads a
    local or a parameter: no string the guard can see.
    """

    inside: set[str] = set()
    reads: list[ast.Name] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.arg):
            inside.add(sub.arg)
        elif isinstance(sub, ast.Name):
            if isinstance(sub.ctx, ast.Load):
                reads.append(sub)
            else:
                inside.add(sub.id)
    if any(
        (id(name) not in local and name.id in env) or id(name) in unknown
        for name in reads
    ):
        return _UNKNOWN
    # A name bound inside the expression (a comprehension target, a lambda
    # parameter) derives from it; any other name decides by its kind.
    kinds = {
        _read_kind(name, env, local, unknown) for name in reads if name.id not in inside
    }
    if "unknown" in kinds:
        return _UNKNOWN
    if "runtime" in kinds:
        return frozenset()
    with contextlib.suppress(Exception):
        value = ast.literal_eval(node)  # type: ignore[arg-type]
        return frozenset({value}) if isinstance(value, str) else frozenset()
    return _UNKNOWN


def _fold_path(
    node: ast.Attribute,
    env: Mapping[str, Values],
    local: frozenset[int],
    unknown: frozenset[int] = frozenset(),
) -> Values:
    """``a.b.c``: a ``const`` path from ``env``; under any other module name
    unknown (``NS.A``); under a local, a parameter or a builtin, no string."""

    path = _dotted(node)
    root = _module_root(node)
    if path is None or root is None:
        return _opaque(node, env, local, unknown)
    if path in env:
        return env[path]
    if _read_kind(root, env, local, unknown) in {"runtime", "builtin"}:
        return frozenset()
    return _UNKNOWN


def _fold_one(
    node: ast.AST,
    env: Mapping[str, Values],
    local: frozenset[int],
    memo: dict[int, Values],
    unknown: frozenset[int] = frozenset(),
) -> Values:
    if isinstance(node, ast.Constant):
        return frozenset({node.value}) if isinstance(node.value, str) else frozenset()
    if isinstance(node, ast.Name):
        kind = _read_kind(node, env, local, unknown)
        return {"module": env.get(node.id), "unknown": _UNKNOWN}.get(kind, frozenset())
    if isinstance(node, ast.Attribute):
        return _fold_path(node, env, local, unknown)
    if isinstance(node, ast.BinOp):
        return _join(memo[id(node.left)], memo[id(node.right)])
    # ``_values`` folds only nodes with ``_fold_children``: an f-string is left.
    joined: Values = frozenset({""})
    for child in _fold_children(node) or []:
        joined = _join(joined, memo[id(child)])
    return joined


def _values(
    node: ast.AST,
    env: Mapping[str, Values],
    local: frozenset[int] = frozenset(),
    memo: dict[int, Values] | None = None,
    unknown: frozenset[int] = frozenset(),
) -> Values:
    """The strings ``node`` may evaluate to, or ``_UNKNOWN``.

    Folds string constants, ``+``, f-strings without conversion or format
    spec, and names and ``a.b`` paths through ``env``; a name read as a local
    or parameter (its id in ``local``) is not a module name. Any other
    expression follows ``_opaque``. Iterative with a memo per node, so a long
    chain of terms neither recurses nor is folded twice.
    """

    memo = {} if memo is None else memo
    stack: list[tuple[ast.AST, bool]] = [(node, False)]
    while stack:
        current, ready = stack.pop()
        if id(current) in memo:
            continue
        children = _fold_children(current)
        if not ready and children:
            stack.append((current, True))
            stack.extend((child, False) for child in children if id(child) not in memo)
            continue
        if children is None:
            memo[id(current)] = _opaque(current, env, local, unknown)
        else:
            memo[id(current)] = _fold_one(current, env, local, memo, unknown)
    return memo[id(node)]


#: What one module-level binding gives its name: an expression to fold, the
#: values of an imported ``const`` constant, or ``_UNKNOWN``.
_Site = ast.expr | frozenset[str] | None


def _own_globals(node: ast.AST) -> frozenset[str]:
    """Names a def or class body declares ``global`` itself, not in nested scopes."""

    names: set[str] = set()
    stack = list(getattr(node, "body", []))
    while stack:
        current = stack.pop()
        if isinstance(current, ast.Global):
            names.update(current.names)
        elif not isinstance(current, _NESTED_SCOPES):
            stack.extend(ast.iter_child_nodes(current))
    return frozenset(names)


class _ModuleBindings:
    """Every binding of a module name, whatever statement or block it is in.

    Scopes follow the language reference: a def, lambda or class body binds its
    own names unless they are declared ``global`` there, and such a ``global``
    binding is unknown (its value comes from the body, R7-F2); a comprehension
    binds its targets, while ``:=`` at any depth of nested comprehensions binds
    in the nearest enclosing scope that is not one (PEP 572, R7-F3), a lambda
    being such a scope. Decorators, defaults and bases run in the enclosing
    scope. Iterative, so a deep expression does not recurse.
    """

    def __init__(self, constants: Mapping[str, Values]) -> None:
        self.constants = constants
        self.sites: dict[str, list[_Site]] = {}
        self.paths: dict[str, Values] = {}

    def bind(
        self, scope: str, declared: frozenset[str], name: str, site: _Site
    ) -> None:
        if scope == "module":
            self.sites.setdefault(name, []).append(site)
        elif name in declared:
            self.sites.setdefault(name, []).append(_UNKNOWN)

    def visit_module(self, tree: ast.Module) -> None:
        #: (node, scope, declared, inside a comprehension of that scope)
        stack: list[tuple[ast.AST, str, frozenset[str], bool]] = [
            (statement, "module", frozenset(), False)
            for statement in reversed(tree.body)
        ]
        while stack:
            node, scope, declared, inner = stack.pop()
            # Reversed onto the stack, so bindings are recorded in source order.
            stack.extend(reversed(self._step(node, scope, declared, inner)))

    def _step(
        self, node: ast.AST, scope: str, declared: frozenset[str], inner: bool
    ) -> list[tuple[ast.AST, str, frozenset[str], bool]]:
        Item = tuple[ast.AST, str, frozenset[str], bool]
        if inner:
            return self._step_in_comprehension(node, scope, declared)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            self.bind(scope, declared, node.name, _UNKNOWN)
            outer: list[ast.AST] = list(node.decorator_list)
            if isinstance(node, ast.ClassDef):
                outer += [*node.bases, *(keyword.value for keyword in node.keywords)]
            else:
                outer += [*node.args.defaults, *(d for d in node.args.kw_defaults if d)]
            kind = "class" if isinstance(node, ast.ClassDef) else "function"
            own = _own_globals(node)
            items: list[Item] = [(child, scope, declared, False) for child in outer]
            items += [(statement, kind, own, False) for statement in node.body]
        elif isinstance(node, ast.Lambda):
            defaults = [d for d in [*node.args.defaults, *node.args.kw_defaults] if d]
            items = [(d, scope, declared, False) for d in defaults]
            items.append((node.body, "function", frozenset(), False))
        elif isinstance(node, _COMPREHENSION_NODES):
            first = node.generators[0].iter
            items = [(first, scope, declared, False)]
            items += [
                (child, scope, declared, True)
                for child in [
                    *ast.iter_child_nodes(node),
                    *ast.iter_child_nodes(node.generators[0]),
                ]
                if child is not node.generators[0] and child is not first
            ]
        elif isinstance(node, ast.Assign | ast.AnnAssign):
            follow = self._step_assignment(node, scope, declared)
            items = [(child, scope, declared, False) for child in follow]
        elif isinstance(node, ast.Import | ast.ImportFrom):
            self._visit_import(node, scope, declared)
            items = []
        else:
            if isinstance(node, ast.Name) and not isinstance(
                node.ctx, ast.Load | ast.Del
            ):
                self.bind(scope, declared, node.id, _UNKNOWN)
            for name in _bound_identifiers(node):
                self.bind(scope, declared, name, _UNKNOWN)
            items = [
                (child, scope, declared, False) for child in ast.iter_child_nodes(node)
            ]
        return items

    def _step_assignment(
        self, node: ast.Assign | ast.AnnAssign, scope: str, declared: frozenset[str]
    ) -> list[ast.AST]:
        """Bind each plain name target to the value; the rest is visited."""

        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        follow: list[ast.AST] = []
        for target in targets:
            if isinstance(target, ast.Name):
                # An annotation without a value binds nothing (N7).
                if node.value is not None:
                    self.bind(scope, declared, target.id, node.value)
            else:
                follow.append(target)
        follow += [
            child
            for child in (node.value, getattr(node, "annotation", None))
            if child is not None
        ]
        return follow

    def _step_in_comprehension(
        self, node: ast.AST, scope: str, declared: frozenset[str]
    ) -> list[tuple[ast.AST, str, frozenset[str], bool]]:
        """Inside a comprehension owned by ``scope``: only ``:=`` binds there."""

        if isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            self.bind(scope, declared, node.target.id, _UNKNOWN)
            return [(node.value, scope, declared, True)]
        if isinstance(node, ast.Lambda):
            defaults = [d for d in [*node.args.defaults, *node.args.kw_defaults] if d]
            return [(d, scope, declared, True) for d in defaults] + [
                (node.body, "function", frozenset(), False)
            ]
        return [(child, scope, declared, True) for child in ast.iter_child_nodes(node)]

    def _visit_import(
        self, node: ast.Import | ast.ImportFrom, scope: str, declared: frozenset[str]
    ) -> None:
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".")[0]
                self.bind(scope, declared, bound, frozenset())
                if alias.name.split(".")[-1] == "const":
                    self._add_paths(alias.asname or alias.name)
            return
        from_const = (node.module or "").split(".")[-1] == "const"
        for alias in node.names:
            bound = alias.asname or alias.name
            if alias.name == "*":
                # Any name may be rebound; ``_copy_is_the_module`` fails the
                # file, and the keys it reads stay as unknown as they are.
                continue
            if from_const:
                site = self.constants.get(alias.name, frozenset())
            elif alias.name == "const":
                site = frozenset()
                self._add_paths(bound)
            else:
                # A name from another module: not a string the guard can see.
                site = frozenset()
            self.bind(scope, declared, bound, site)

    def _add_paths(self, bound: str) -> None:
        for key, value in self.constants.items():
            self.paths[f"{bound}.{key}"] = value


def _site_names(site: ast.AST, local: frozenset[int]) -> frozenset[str]:
    """Every name ``site`` reads that is not a local, in folding parts or not."""

    return frozenset(
        node.id
        for node in ast.walk(site)
        if isinstance(node, ast.Name) and id(node) not in local
    )


def _components(graph: Mapping[str, frozenset[str]]) -> list[list[str]]:
    """Strongly connected components, dependencies first (Tarjan, iterative)."""

    index: dict[str, int] = {}
    low: dict[str, int] = {}
    on_stack: set[str] = set()
    stack: list[str] = []
    found: list[list[str]] = []
    for start in graph:
        if start in index:
            continue
        work: list[tuple[str, list[str]]] = [(start, sorted(graph[start]))]
        index[start] = low[start] = len(index)
        stack.append(start)
        on_stack.add(start)
        while work:
            name, pending = work[-1]
            if pending:
                successor = pending.pop()
                if successor not in index:
                    index[successor] = low[successor] = len(index)
                    stack.append(successor)
                    on_stack.add(successor)
                    work.append((successor, sorted(graph[successor])))
                elif successor in on_stack:
                    low[name] = min(low[name], index[successor])
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[name])
            if low[name] == index[name]:
                component: list[str] = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == name:
                        break
                found.append(component)
    return found


def _module_values(
    tree: ast.Module,
    constants: Mapping[str, Values],
    local: frozenset[int] | None = None,
) -> dict[str, Values]:
    """The values every module name may hold.

    Flow-insensitive: a name holds every value any of its bindings gives it,
    in any order (``A = 'fcm_'; K = A + 'credentials'; A = 'x'`` keeps
    ``K`` the member). A binding that does not fold makes the name unknown.
    Names are evaluated by strongly connected components of the graph from a
    name to every name its bindings read, dependencies first, each binding at
    most once (R7-F5). A component whose inner bindings are all plain names
    (``A = B; B = A``) gives every member the values flowing in from outside;
    any other cycle (``A = A + 'a'``, ``B = A * 0``) is unknown.
    """

    bindings = _ModuleBindings(constants)
    bindings.visit_module(tree)
    if local is None:
        local = _local_reads(tree, _parents(tree))
    env: dict[str, Values] = dict(bindings.paths)
    reads: dict[int, frozenset[str]] = {}
    graph: dict[str, frozenset[str]] = {}
    for name, sites in bindings.sites.items():
        edges: set[str] = set()
        for site in sites:
            if isinstance(site, ast.expr):
                reads[id(site)] = frozenset(
                    read for read in _site_names(site, local) if read in bindings.sites
                )
                edges |= reads[id(site)]
        graph[name] = frozenset(edges)

    def union(sites: list[_Site]) -> Values:
        values: Values = frozenset()
        for site in sites:
            found = _values(site, env, local) if isinstance(site, ast.expr) else site
            if found is _UNKNOWN:
                return _UNKNOWN
            values = values | found
            if len(values) > _VALUE_LIMIT:
                return _UNKNOWN
        return values

    for component in _components(graph):
        members = frozenset(component)
        cyclic = len(component) > 1 or component[0] in graph[component[0]]
        sites = [site for name in component for site in bindings.sites[name]]
        inner = [
            site
            for site in sites
            if isinstance(site, ast.expr) and reads[id(site)] & members
        ]
        values: Values = _UNKNOWN
        if not cyclic or all(
            isinstance(site, ast.Name) and site.id in members for site in inner
        ):
            inner_ids = {id(site) for site in inner}
            values = union([site for site in sites if id(site) not in inner_ids])
        for name in component:
            env[name] = values
    return env


@functools.cache
def _project_constants() -> dict[str, Values]:
    """Values of the names of ``const.py``, as a receiver import would see them."""

    return _module_values(_parse(_CONST), {})


def _class_body_nodes(cls: ast.ClassDef) -> list[ast.AST]:
    """Nodes a class body evaluates in its own namespace, nested bodies aside."""

    found: list[ast.AST] = []
    stack: list[ast.AST] = list(cls.body)
    while stack:
        node = stack.pop()
        found.append(node)
        if isinstance(node, _NESTED_SCOPES):
            stack.extend(_outer_parts(node))
        else:
            stack.extend(ast.iter_child_nodes(node))
    return found


#: A comprehension target: the comprehension and the name.
_Target = tuple[int, str]


def _comprehension_body(node: ast.AST) -> list[ast.AST]:
    if isinstance(node, ast.DictComp):
        return [node.key, node.value]
    return [node.elt]  # type: ignore[attr-defined]


def _target_names(target: ast.AST) -> list[str]:
    """The names a ``for`` target binds.

    R13-F8, R14-F3: a subscript or attribute target binds no name; the names
    read inside it (``for d2[P] in data``), and the targets of a comprehension
    inside it (``for d2[[q for q in data][0]] in data``), are not targets of the
    generator."""

    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Starred):
        return _target_names(target.value)
    if isinstance(target, (ast.Tuple, ast.List)):
        return [name for element in target.elts for name in _target_names(element)]
    return []


#: R16-F2: builtins whose result holds the elements of their one argument and
#: nothing else; a second positional argument raises, a keyword only orders in
#: ``sorted`` and raises in the others (R18-F4).
#: A starred argument is not followed, also where it is empty and Python
#: runs the call (R17-F3: ``sorted(data, *[])`` is failed, not accepted).
_REORDERING = frozenset({"sorted", "list", "tuple", "reversed"})


def _reordering_builtins(tree: ast.Module) -> frozenset[str]:
    """The names of ``_REORDERING`` the file binds nowhere, so each is the builtin."""

    bound = {name for node in ast.walk(tree) for name in _bound_by(node)}
    return frozenset() if "*" in bound else _REORDERING - bound


def _reordered(call: ast.Call, reordering: frozenset[str]) -> ast.AST | None:
    """The one argument whose elements ``call`` returns, if it is a builtin of
    ``reordering`` called with one positional argument (R16-F2). A starred
    argument is no value a read gives and stays undecided."""

    func = call.func
    builtin = isinstance(func, ast.Name) and func.id in reordering
    return call.args[0] if builtin and len(call.args) == 1 else None


def _yields_reads(iterable: ast.AST, reordering: frozenset[str]) -> bool:
    """Whether every element ``iterable`` yields is a value a name read gives.

    R15-F1, R15-F3: decided per element, not by a string literal anywhere in
    the iterable. A list, tuple or set display yields its elements, a dict
    display its keys (the keys of a spread), an operator, a
    conditional or ``and``/``or`` the elements of each side, a comprehension
    its element (a dict comprehension its key); a name, a subscript or an
    attribute yields the elements of its value, and so do ``sorted``,
    ``list``, ``tuple`` and ``reversed`` of one argument where the file does
    not bind the name (R16-F2). A constant yields itself, and any other call
    result is not followed, also where a method of a runtime value may return
    an argument, unless it is called without arguments.
    What is read inside a subscript or a condition is not yielded."""

    if isinstance(iterable, (ast.List, ast.Tuple, ast.Set, ast.Dict)):
        return _display_yields_reads(iterable, reordering)
    sides = _sides(iterable)
    if sides is not None:
        return all(_yields_reads(side, reordering) for side in sides)
    if isinstance(iterable, ast.DictComp):
        return _is_read_value(iterable.key)
    if isinstance(iterable, _COMPREHENSION_NODES):
        return _is_read_value(iterable.elt)  # type: ignore[attr-defined]
    if isinstance(iterable, ast.Call):
        passed = _reordered(iterable, reordering)
        if passed is not None:
            return _yields_reads(passed, reordering)
    return _is_read_value(iterable)


def _display_yields_reads(
    display: ast.List | ast.Tuple | ast.Set | ast.Dict, reordering: frozenset[str]
) -> bool:
    """Whether a display yields only values a read gives: its elements, the
    keys of a dict display, what a starred element or a spread yields."""

    if isinstance(display, ast.Dict):
        return all(
            _yields_reads(value, reordering) if key is None else _is_read_value(key)
            for key, value in zip(display.keys, display.values, strict=True)
        )
    return all(
        _yields_reads(element.value, reordering)
        if isinstance(element, ast.Starred)
        else _is_read_value(element)
        for element in display.elts
    )


def _sides(value: ast.AST) -> list[ast.AST] | None:
    """The operands of an operator, a conditional or ``and``/``or``."""

    if isinstance(value, ast.BinOp):
        return [value.left, value.right]
    if isinstance(value, ast.IfExp):
        return [value.body, value.orelse]
    if isinstance(value, ast.BoolOp):
        return list(value.values)
    return None


def _is_read_value(value: ast.AST) -> bool:
    """Whether ``value`` is a value a name read gives: the name, a subscript
    or attribute of it, a method of it called without arguments, a walrus
    over it, a conditional, ``and``/``or`` or an operator of such values, or
    an f-string of their placeholders without format spec."""

    if isinstance(value, ast.Name):
        return True
    parts = _read_parts(value)
    return parts is not None and all(_is_read_value(part) for part in parts)


def _read_parts(value: ast.AST) -> list[ast.AST] | None:
    """The values ``value`` is made of, ``None`` if it may hold another value."""

    if isinstance(value, (ast.Subscript, ast.Attribute, ast.NamedExpr)):
        return [value.value]
    sides = _sides(value)
    if sides is not None:
        return sides
    if isinstance(value, ast.Call):
        # A method without arguments returns no argument (``data.items()``).
        method = isinstance(value.func, ast.Attribute)
        bare = not value.args and not value.keywords
        return [value.func.value] if method and bare else None  # type: ignore[attr-defined]
    if isinstance(value, ast.JoinedStr):
        placeholders = [
            part for part in value.values if isinstance(part, ast.FormattedValue)
        ]
        plain = len(placeholders) == len(value.values)
        bare = all(part.format_spec is None for part in placeholders)
        return [part.value for part in placeholders] if plain and bare else None
    return None


class _ScopeReads:
    """Name reads ``_read_kind`` cannot tell from the module and the locals alone.

    A name a class body binds and reads there holds the class value (R8-F1):
    unknown, and so is a type parameter (``class Q[fcm_]``, F-D). A
    comprehension target holds what its iterable yields (R8-F2): runtime when
    the iterable reads a local, a parameter or such a target, no module name,
    and yields only values such reads give (R14-F4, R15-F1: no constant and no
    call result as an element), builtin aside; otherwise unknown, as for
    ``[{p + A + 'credentials': v} for p in ['']]``. A generator's iterable sees
    the targets of the generators before it and of the comprehensions around
    it, and a target of a comprehension inside it.

    Each read is bound to its target in one walk from the top (F-E: a walk up
    the parents per read, repeated for every iterable around it, grew with
    the cube of the nesting depth), and each target is decided once, after
    the targets its iterables read.
    """

    def __init__(
        self,
        tree: ast.Module,
        parents: dict[ast.AST, ast.AST],
        env: Mapping[str, Values],
        local: frozenset[int],
    ) -> None:
        self.parents = parents
        self.env = env
        self.local = local
        self.unknown: set[int] = set()
        self.runtime: set[int] = set()
        self.reordering = _reordering_builtins(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                self._class_reads(node)
            if getattr(node, "type_params", None):
                self._type_parameter_reads(node)
        binding, iterables = self._bind_targets(tree)
        runtime = self._decide_targets(binding, iterables)
        for read, target in binding.items():
            if read not in self.unknown:
                (self.runtime if runtime.get(target) else self.unknown).add(read)

    def _class_reads(self, cls: ast.ClassDef) -> None:
        nodes = _class_body_nodes(cls)
        bound = {name for node in nodes for name in _bound_by(node)}
        self.unknown.update(
            id(node)
            for node in nodes
            if isinstance(node, ast.Name)
            and isinstance(node.ctx, ast.Load)
            and (node.id in bound or "*" in bound)
            and id(node) not in self.local
        )

    def _type_parameter_reads(self, node: ast.AST) -> None:
        names = {parameter.name for parameter in node.type_params}  # type: ignore[attr-defined]
        self.unknown.update(
            id(read)
            for read in ast.walk(node)
            if isinstance(read, ast.Name)
            and isinstance(read.ctx, ast.Load)
            and read.id in names
            and id(read) not in self.local
        )

    def _bind_targets(
        self, tree: ast.Module
    ) -> tuple[dict[int, _Target], dict[_Target, list[ast.AST]]]:
        """The target each read reads, and the iterables of each target."""

        binding: dict[int, _Target] = {}
        iterables: dict[_Target, list[ast.AST]] = {}
        stack: list[tuple[ast.AST, dict[str, _Target]]] = [(tree, {})]
        while stack:
            node, seen = stack.pop()
            if isinstance(node, _COMPREHENSION_NODES):
                own: dict[str, _Target] = {}
                for generator in node.generators:
                    stack.append((generator.iter, {**seen, **own}))
                    for name in _target_names(generator.target):
                        key = own.setdefault(name, (id(node), name))
                        iterables.setdefault(key, []).append(generator.iter)
                inner = {**seen, **own}
                for generator in node.generators:
                    stack.append((generator.target, inner))
                    stack.extend((condition, inner) for condition in generator.ifs)
                stack.extend((part, inner) for part in _comprehension_body(node))
                continue
            if (
                isinstance(node, ast.Name)
                and isinstance(node.ctx, ast.Load)
                and node.id in seen
                and id(node) not in self.local
            ):
                binding[id(node)] = seen[node.id]
            stack.extend((child, seen) for child in ast.iter_child_nodes(node))
        return binding, iterables

    def _decide_targets(
        self,
        binding: dict[int, _Target],
        iterables: dict[_Target, list[ast.AST]],
    ) -> dict[_Target, bool]:
        """Whether each target holds runtime values only.

        A target bound by several generators is runtime only if every
        iterable is, also where Python keeps only the last binding (R14-F7:
        ``for p in ['fcm_'] for p in data`` fails, deliberately). A target
        whose iterable reads the target itself (``for x in A for x in x``)
        reads an undecided value there: unknown.
        """

        # R13-F1: each iterable is decided on its own reads; an iterable that
        # reads nothing (``for p in data for p in ['fcm_']``) is not runtime.
        groups = {
            target: [
                [
                    read
                    for read in ast.walk(iterable)
                    if isinstance(read, ast.Name) and isinstance(read.ctx, ast.Load)
                ]
                for iterable in found
            ]
            for target, found in iterables.items()
        }
        # R14-F4, R15-F1: an iterable that may yield a constant
        # (``[data, 'fcm_']``, ``[data, bytes(...).decode()]``) is not runtime.
        literal = {
            target: [not _yields_reads(iterable, self.reordering) for iterable in found]
            for target, found in iterables.items()
        }
        reads = {
            target: [r for group in found for r in group]
            for target, found in groups.items()
        }
        runtime: dict[_Target, bool] = {}
        position: dict[_Target, int] = {}
        for start in reads:
            path = [start]
            on_path = {start}
            while path:
                target = path[-1]
                found = reads[target]
                index = position.get(target, 0)
                while index < len(found):
                    needed = binding.get(id(found[index]))
                    if (
                        needed is not None
                        and needed not in runtime
                        and needed not in on_path
                    ):
                        break
                    index += 1
                position[target] = index
                if index < len(found):
                    needed = binding[id(found[index])]
                    path.append(needed)
                    on_path.add(needed)
                    continue
                if target not in runtime:
                    runtime[target] = all(
                        not has_string
                        and self._runtime_iterable(group, binding, runtime)
                        for group, has_string in zip(
                            groups[target], literal[target], strict=True
                        )
                    )
                path.pop()
                on_path.discard(target)
        return runtime

    def _runtime_iterable(
        self,
        reads: list[ast.Name],
        binding: dict[int, _Target],
        runtime: dict[_Target, bool],
    ) -> bool:
        """An iterable yields runtime values only: it reads a local, a parameter
        or a runtime target, and no module name or unknown value."""

        kinds = {self._kind(read, binding, runtime) for read in reads}
        return "runtime" in kinds and not kinds & {"module", "unknown"}

    def _kind(
        self,
        read: ast.Name,
        binding: dict[int, _Target],
        runtime: dict[_Target, bool],
    ) -> str:
        if id(read) in self.local:
            return "runtime"
        if id(read) in binding:
            return "runtime" if runtime.get(binding[id(read)]) else "unknown"
        if id(read) in self.unknown:
            return "unknown"
        return _read_kind(read, self.env, self.local, frozenset())


class _Scan:
    """One fixed module environment, the local reads and a shared fold memo."""

    def __init__(
        self,
        env: Mapping[str, Values],
        local: frozenset[int],
        unknown: frozenset[int] = frozenset(),
    ) -> None:
        self.env = env
        self.local = local
        self.unknown = unknown
        self.memo: dict[int, Values] = {}

    def values(self, node: ast.AST) -> Values:
        return _values(node, self.env, self.local, self.memo, self.unknown)

    def is_member(self, node: ast.AST) -> bool:
        values = self.values(node)
        return values is not _UNKNOWN and _MEMBER in values


#: Methods whose first argument is a key of the mapping they are called on.
_KEY_METHODS = frozenset({"setdefault", "pop", "__setitem__", "__delitem__"})

#: The same methods called unbound or through ``operator``: the key is second.
_KEY_FUNCTIONS = frozenset(
    {
        "operator.setitem",
        "operator.delitem",
        *(f"dict.{name}" for name in _KEY_METHODS),
    }
)

#: Every method that writes or deletes a key, and its function forms: one
#: reached other than by a call of it is an unknown write (F-A).
_WRITE_METHODS = _KEY_METHODS | {"update", "fromkeys"}
_WRITE_FUNCTIONS = _KEY_FUNCTIONS | {"dict.update", "dict.fromkeys"}

#: Calls that reach a method by its name as a string.
_BY_NAME = frozenset({"getattr", "operator.methodcaller"})


def _arguments(call: ast.Call) -> list[ast.AST]:
    """Positional arguments with starred displays spread (``*(C, v)``); a
    starred argument of any other form ends the list and stands for every
    position from there on."""

    found: list[ast.AST] = []
    for argument in call.args:
        if not isinstance(argument, ast.Starred):
            found.append(argument)
        elif isinstance(argument.value, ast.Tuple | ast.List):
            found.extend(argument.value.elts)
        else:
            found.append(argument.value)
            break
    return found


def _at(arguments: list[ast.AST], call: ast.Call, index: int) -> list[ast.AST]:
    if index < len(arguments):
        return [arguments[index]]
    spread = any(
        isinstance(a, ast.Starred) and not isinstance(a.value, ast.Tuple | ast.List)
        for a in call.args
    )
    return arguments[-1:] if spread else []


def _element_keys(iterable: ast.AST, pairs: bool) -> list[ast.AST]:
    """Keys an iterable hands to ``dict``, ``update`` (``pairs``) or ``fromkeys``.

    A display or a comprehension gives its elements, or their first part for
    pairs (``[(C, v)]``). An operator, a conditional, ``:=``, ``or`` and a
    slice pass on the elements of their operands (``[(C, v)] * 1``, F-C).
    Any other expression stands for every key it may hold; a dict display
    or comprehension is a write position of its own. Pairs or a mapping a
    call builds (``d.update(zip(...))``) are not followed.
    """

    keys: list[ast.AST] = []
    stack = [iterable]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.List | ast.Tuple | ast.Set):
            elements: list[ast.AST] = list(node.elts)
        elif isinstance(node, ast.ListComp | ast.SetComp | ast.GeneratorExp):
            elements = [node.elt]
        elif isinstance(node, ast.BinOp):
            stack.extend((node.left, node.right))
            continue
        elif isinstance(node, ast.IfExp):
            stack.extend((node.body, node.orelse))
            continue
        elif isinstance(node, ast.BoolOp):
            stack.extend(node.values)
            continue
        elif isinstance(node, ast.NamedExpr | ast.Starred):
            stack.append(node.value)
            continue
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Slice):
            stack.append(node.value)
            continue
        elif isinstance(node, ast.Call | ast.Dict | ast.DictComp):
            continue
        else:
            keys.append(node)
            continue
        for element in elements:
            item = element.value if isinstance(element, ast.Starred) else element
            if pairs and isinstance(item, ast.Tuple | ast.List) and item.elts:
                keys.append(item.elts[0])
            else:
                keys.append(item)
    return keys


def _import_aliases(tree: ast.Module) -> dict[str, str]:
    """The dotted name each import binds, anywhere in the file
    (``import operator as op``: ``op`` is ``operator``; ``from operator import
    setitem``: ``setitem`` is ``operator.setitem``). A star import
    (``from operator import *``) is left out: upstream fails it anywhere in the
    file."""

    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    aliases[alias.asname] = alias.name
                else:
                    root = alias.name.partition(".")[0]
                    aliases[root] = root
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            for alias in node.names:
                aliases[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return aliases


class _Writes:
    """The write positions of one file (``keys``) and the write functions it
    reaches other than by a call (``escapes``)."""

    def __init__(self, tree: ast.Module, parents: dict[ast.AST, ast.AST]) -> None:
        self.parents = parents
        self.aliases = _import_aliases(tree)
        # The rule after round 19: a key is read at position 0 only where the
        # receiver is a chain of names whose head no class statement and no
        # import binds, so most likely an instance; every other receiver is
        # read at both positions, and no proof that a name holds a class is
        # needed (R13-F2 to R19-F1 were each a gap in such a proof). On a class
        # an instance method inherited from dict takes the mapping first and
        # raises on a key there; fromkeys, a method the class defines or
        # inherits from another base, a classmethod or a staticmethod may run.
        # The price (R20-F1, R21-F4): an undecided
        # mapping at either position of such a receiver fails, though the code
        # runs.
        self.classes = {
            node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)
        }

    def canonical(self, node: ast.AST) -> str | None:
        """The dotted name of ``node`` with an import alias at its root resolved."""

        dotted = _dotted(node)
        if dotted is None:
            return None
        head, dot, rest = dotted.partition(".")
        return self.aliases.get(head, head) + dot + rest

    def positions(self, receiver: ast.AST) -> tuple[int, ...]:
        """Where a method called on ``receiver`` takes its key (F-B).

        A chain whose head is a class of the file or an import, and any
        receiver that is no chain of names (``type(d)``, a call, a subscript),
        may be a class and is read at both positions. A chain over any other
        head (a local, ``self``, a module-level name) is taken as an instance
        and takes the key first.
        """

        dotted = _dotted(receiver)
        if dotted is None:
            return (0, 1)
        head = dotted.partition(".")[0]
        if head in self.classes or head in self.aliases:
            return (0, 1)
        return (0,)

    def call_keys(self, call: ast.Call) -> list[ast.AST]:
        """Keys a call writes or deletes in a mapping (R8-F3, F-A, F-B)."""

        func = call.func
        name = self.canonical(func)
        arguments = _arguments(call)
        spread = [kw.value for kw in call.keywords if kw.arg is None]
        keys: list[ast.AST] = []
        pairs: list[ast.AST] = []
        if name in _KEY_FUNCTIONS:
            keys = _at(arguments, call, 1)
        elif name == "dict.update":
            pairs = _at(arguments, call, 1)
        elif name == "dict":
            pairs = [*_at(arguments, call, 0), *spread]
        elif isinstance(func, ast.Attribute):
            keys, pairs = self._method_arguments(func, call, arguments, spread)
        return keys + [k for a in pairs for k in _element_keys(a, True)]

    def _method_arguments(
        self,
        func: ast.Attribute,
        call: ast.Call,
        arguments: list[ast.AST],
        spread: list[ast.AST],
    ) -> tuple[list[ast.AST], list[ast.AST]]:
        """Keys and pair iterables of a method call."""

        if func.attr == "fromkeys":
            return [
                k for a in _at(arguments, call, 0) for k in _element_keys(a, False)
            ], []
        found = [a for i in self.positions(func.value) for a in _at(arguments, call, i)]
        if func.attr in _KEY_METHODS:
            return found, []
        if func.attr == "update":
            return [], [*found, *spread]
        return [], []

    def escapes(self, node: ast.AST) -> bool:
        """A write function reached other than by a call of it: bound to a
        name (``s = d.__setitem__``), handed on (``functools.partial``), or
        looked up by its name (``getattr(d, '__setitem__')``)."""

        if isinstance(node, ast.Call):
            if self.canonical(node.func) not in _BY_NAME:
                return False
            return any(
                isinstance(a, ast.Constant) and a.value in _WRITE_METHODS
                for a in node.args
            )
        if isinstance(node, ast.Attribute) and node.attr in _WRITE_METHODS:
            pass
        elif not (
            isinstance(node, ast.Name | ast.Attribute)
            and self.canonical(node) in _WRITE_FUNCTIONS
        ):
            return False
        parent = self.parents.get(node)
        return isinstance(node.ctx, ast.Load) and not (  # type: ignore[attr-defined]
            isinstance(parent, ast.Call) and parent.func is node
        )

    def keys(self, tree: ast.Module) -> list[ast.AST]:
        """Keys of a write: a subscript store or ``del`` and the subscripts it
        writes into, a dict display or comprehension, and the calls of
        ``call_keys``."""

        keys: list[ast.AST] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Subscript) and _store_chain(node, self.parents):
                keys.append(node.slice)
            elif isinstance(node, ast.Dict):
                keys.extend(key for key in node.keys if key is not None)
            elif isinstance(node, ast.DictComp):
                keys.append(node.key)
            elif isinstance(node, ast.Call):
                keys.extend(self.call_keys(node))
        return keys


def _store_chain(node: ast.Subscript, parents: dict[ast.AST, ast.AST]) -> bool:
    """``node`` is a store or ``del`` target, or the container of one
    (``bundle[C]["gcm"] = v`` writes into ``bundle[C]``)."""

    current: ast.AST = node
    while True:
        if isinstance(current, ast.Subscript | ast.Attribute) and not isinstance(
            current.ctx, ast.Load
        ):
            return True
        parent = parents.get(current)
        if not (
            isinstance(parent, ast.Subscript | ast.Attribute)
            and parent.value is current
        ):
            return False
        current = parent


def _unknown_keys(
    tree: ast.Module, parents: dict[ast.AST, ast.AST], scan: _Scan
) -> list[ast.AST]:
    """Keys at a write position of ``_Writes`` whose value the guard cannot
    bound, and the write functions reached other than by a call.

    A key that may be the member and that nothing bounds (``str(K3)``) fails
    like an unclassified occurrence. A read keyed so (``list[Any]``, ``x[i]``) is not a
    write and stays out.
    """

    writes = _Writes(tree, parents)
    return [key for key in writes.keys(tree) if scan.values(key) is _UNKNOWN] + [
        node for node in ast.walk(tree) if writes.escapes(node)
    ]


class _FoldingKeys:
    """The resolver for ``_receiver_problems``: folds module keys (``_Scan``)
    and reports the keys of a write it cannot bound.

    For every ``const.py`` value of at most ``_VALUE_LIMIT`` strings it is at
    least as strict as the upstream ``_LiteralKeys``: a key that resolver
    bounds is bounded here too (``C2 = const``), and every place it reports a
    ``const.py`` member constant it does not follow is reported here, once,
    unless the folding follows it. A larger ``const.py`` value is unknown here:
    a write keyed with it is an unknown key, but a place that only reaches it
    is not reported (R10-F4)."""

    key_nodes: tuple[type[ast.AST], ...] = _FOLDABLE

    def __init__(
        self,
        tree: ast.Module,
        parents: dict[ast.AST, ast.AST],
        local: frozenset[int],
        constants: Mapping[str, Values],
    ) -> None:
        env = _module_values(tree, constants, local)
        reads = _ScopeReads(tree, parents, env, local)
        self.scan = _Scan(
            env, local | frozenset(reads.runtime), frozenset(reads.unknown)
        )
        # A ``const.py`` value the folding cannot bound stays unknown at every
        # write keyed with it (``_unknown_keys``); the literal resolver gets the
        # bounded ones.
        bounded = {k: v for k, v in constants.items() if v is not _UNKNOWN}
        self.literal = _LiteralKeys(tree, parents, local, bounded)
        self.parents = parents

    def is_member(self, node: ast.AST) -> bool:
        return self.scan.is_member(node) or self.literal.is_member(node)

    def unknown_keys(self, tree: ast.Module) -> list[ast.AST]:
        found = [
            key
            for key in _unknown_keys(tree, self.parents, self.scan)
            if self.literal.values(key) is None
        ]
        reported = {id(key) for key in found}
        return found + [
            node
            for node in self.literal.unknown_keys(tree)
            if id(node) not in reported and not self._follows(node)
        ]

    def _follows(self, node: ast.AST) -> bool:
        """The folding resolves this report of ``_LiteralKeys`` as the member."""

        if isinstance(node, ast.alias):
            values = self.scan.env.get(node.asname or node.name)
            return (
                _at_module_level(node, self.parents)
                and values is not _UNKNOWN
                and values is not None
                and _MEMBER in values
            )
        if isinstance(node, ast.Attribute):
            return isinstance(node.ctx, ast.Load) and self.scan.is_member(node)
        return False


def _receiver_problems(
    tree: ast.Module, constants: Mapping[str, Values | str] | None = None
) -> tuple[int, list[str], list[str]]:
    """The upstream scan with the folding resolver; ``const.py`` folded as well."""

    if constants is None:
        known: Mapping[str, Values] = _project_constants()
    else:
        known = {
            name: frozenset({value}) if isinstance(value, str) else value
            for name, value in constants.items()
        }

    def factory(
        tree: ast.Module,
        parents: dict[ast.AST, ast.AST],
        local: frozenset[int],
        _constants: Mapping[str, frozenset[str]],
    ) -> _FoldingKeys:
        return _FoldingKeys(tree, parents, local, known)

    return _literal_receiver_problems(tree, {}, keys_factory=factory)


def _snippet_problems(source: str) -> tuple[int, list[str], list[str]]:
    return _receiver_problems(ast.parse(source), _SNIPPET_CONSTANTS)


def test_every_receiver_write_is_a_deep_copy_with_folded_keys() -> None:
    """R1 over computed keys: the receiver passes the folding scan too."""

    writes, unclassified, shared = _receiver_problems(_parse(_RECEIVER))
    # One today: the bundle member (the top-level copy is no longer written).
    # An empty scan would pass the checks below silently.
    assert writes >= 1, f"expected the receiver writes, found {writes}"
    assert not unclassified, unclassified
    assert not shared, shared


#: Forms only the folding decides, each a one-place change to ``_CLEAN``; the
#: upstream forms run here as well.
_FOLDED_BROKEN: dict[str, tuple[str, ...]] = {
    # R4-F6: keys written as constant expressions (V-7 (c)).
    "f-string key with a constant placeholder": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[f\"fcm_{'credentials'}\"] = creds\n",
    ),
    "key concatenated from constants": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        '            updated_data["fcm_" + "credentials"] = creds\n',
    ),
    "key name growing without bound": (
        "K2 = K\n",
        "K2 = K\nA = 'a'\nA = A + 'a'\nK3 = A\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    # Round 6: a module name takes every value any of its bindings gives it,
    # and a binding that does not fold makes it unknown; an unknown key fails.
    "key built from a name rebound later": (
        "K2 = K\n",
        "K2 = K\nA = 'fcm_'\nK3 = A + 'credentials'\nA = 'x'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key built by augmented assignment": (
        "K2 = K\n",
        "K2 = K\nK3 = 'fcm_'\nK3 += 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key bound by a module-level call": (
        "K2 = K\n",
        "K2 = K\nK3 = ''.join(('fcm_', 'credentials'))\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    # Round 7 (AP9c): a value bound through global comes from the body, so it is
    # unknown; a parameter default shows the mechanism, a literal would not.
    "key bound through global in a function": (
        "K2 = K\n",
        "K2 = K\nK3 = 'x'\ndef _set(a='fcm_'):\n    global K3\n"
        "    K3 = a + 'credentials'\n_set()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key bound through global in a class body": (
        "K2 = K\n",
        "K2 = K\nclass _K:\n    PART = 'fcm_'\n    global K3\n"
        "    K3 = PART + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    # Round 7 (AP9c, V-8 A): an expression that does not fold is unknown when it
    # reads a module name or only constants and builtins.
    "key part taken by subscript": (
        "K2 = K\n",
        "K2 = K\nP = ('fcm_', 'x')\nK3 = P[0] + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key part from a method of a constant": (
        "K2 = K\n",
        "K2 = K\nK3 = 'FCM_'.lower() + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key formatted with percent": (
        "K2 = K\n",
        "K2 = K\nA = 'fcm_'\nK3 = '%scredentials' % A\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key from an f-string with a conversion": (
        "K2 = K\n",
        "K2 = K\nA = 'fcm_'\nK3 = f'{A!s}credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key from an f-string with a format spec": (
        "K2 = K\n",
        "K2 = K\nA = 'fcm_'\nK3 = f'{A:s}credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key part from a class attribute": (
        "K2 = K\n",
        "K2 = K\nclass NS:\n    A = 'fcm_'\nK3 = NS.A + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "unknown module key inside a call": (
        "K2 = K\n",
        "K2 = K\nK3 = ''.join(('fcm_', 'credentials'))\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[str(K3)] = creds\n",
    ),
    "key from a call over a module number": (
        "K2 = K\n",
        "K2 = K\nX = 102\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[chr(X) + 'cm_credentials'] = creds\n",
    ),
    "key from a method of a module bytes value": (
        "K2 = K\n",
        "K2 = K\nX = b'fcm_' + b'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[X.decode()] = creds\n",
    ),
    # Round 7 (AP9c): := binds in the enclosing scope at any comprehension depth.
    "key part bound by walrus two comprehensions deep": (
        "K2 = K\n",
        "K2 = K\n_ = [[(A := 'fcm_') for _ in (0,)] for _ in (0,)]\n"
        "K3 = A + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    # Round 7 (AP9c): components of the binding graph, dependencies first.
    "key grown by a self-referencing binding": (
        "K2 = K\n",
        "K2 = K\nA = 'fcm'\nA = A + '_'\nA = A + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[A] = creds\n",
    ),
    "key sliced from a name bound later": (
        "K2 = K\n",
        "K2 = K\nK3 = P[0:4] + 'credentials'\nP = 'fcm_x'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K3] = creds\n",
    ),
    "key through a cycle over a multiplication": (
        "K2 = K\n",
        "K2 = K\nX = 'fcm_'\nA = X\nB = A * 0\nA = B\nC = B + 'fcm_' + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[C] = creds\n",
    ),
    "comprehension target does not hide a module key": (
        "K2 = K\n",
        "K2 = K\nK3 = ''.join(('fcm_', 'credentials'))\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            _ = [0 for K3 in ()]\n            updated_data[K3] = creds\n",
    ),
    # A target is not folded: constants that cannot give the member fail as well.
    "comprehension target over constants": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {p + 'credentials': creds for p in ('a', 'b')}\n",
    ),
    # The value holds a key of its own: the target is read by a key.
    "comprehension target over constants read by a key inside the value": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {'x': {p + 'credentials': creds} for p in ('a', 'b')}\n",
    ),
    # A literal spells the whole member: no folding needed.
    "local bound to the whole member": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            p = 'fcm_credentials'\n"
        "            updated_data[p] = creds\n",
    ),
    "statement loop over the whole member": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            for p in ('fcm_credentials',): updated_data[p] = creds\n",
    ),
    # More values than the limit: unknown even with a deep copy written.
    "name with more bindings than the limit": (
        "K2 = K\n",
        "K2 = K\n"
        + "".join(f"K3 = 'v' + '{i}'\n" for i in range(_VALUE_LIMIT))
        + "K3 = 'fcm_' + 'credentials'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            updated_data[K3] = copy.deepcopy(registration)\n",
    ),
    "key with more values than the limit": (
        "K2 = K\n",
        "K2 = K\nP1 = 'a'\nP1 = 'fcm_'\nP2 = 'b'\nP2 = 'cr'\nP3 = 'c'\nP3 = 'ed'\n"
        "P4 = 'd'\nP4 = 'en'\nP5 = 'e'\nP5 = 'tia'\nP6 = 'f'\nP6 = 'ls'\n"
        "K3 = P1 + P2 + P3 + P4 + P5 + P6\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            updated_data[K3] = copy.deepcopy(registration)\n",
    ),
    # Review round 8 (AP9e): class bodies, implicit names and comprehension
    # targets (R8-F1, R8-F2), keys of a write in call form (R8-F3).
    "first iterable reads the enclosing name, not a later target": (
        "K2 = K\n",
        "K2 = K\nP = 'fcm_'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        "            other.update({q + 'credentials': creds for q in [P] for P in data})\n",
    ),
    "write in the first iterable keyed by the enclosing name": (
        "K2 = K\n",
        "K2 = K\nP = 'fcm_'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        "            _ = [0 for q in [other.setdefault(P + 'credentials', 1)] for P in data]\n",
    ),
    "key from a target of an earlier generator over constants": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        "            other.update({k + 'credentials': 1 for row in [['fcm_']] for k in row})\n",
    ),
    "comprehension in a class body over a name of the body": (
        "K2 = K\n",
        "K2 = K\nclass Q:\n    P = ['fcm_']\n"
        "    X = {p + 'credentials': copy.deepcopy(G) for p in P}\n",
    ),
    "unknown key in generated pairs": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        "            other.update((C, v) for v in [creds])\n",
    ),
    "unknown arguments spread into operator.setitem": (
        "K2 = K\n",
        "K2 = K\nARGS = ({}, 'FCM_CREDENTIALS'.lower(), 1)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            operator.setitem(*ARGS)\n",
    ),
    "comprehension over constants inside a call over a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        "            other[''.join([p for p in ['fcm_', 'credentials']] + list(creds))] = 1\n",
    ),
    "unknown pair spread into setdefault": (
        "K2 = K\n",
        "K2 = K\nNSP = ('FCM_CREDENTIALS'.lower(), 1)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        "            other.setdefault(*NSP)\n",
    ),
    "key built from a name of a class body": (
        "K2 = K\n",
        "K2 = K\nclass Q:\n    P = 'fcm_'\n    data = {}\n    data[P + 'credentials'] = copy.deepcopy(G)\n",
    ),
    "key built from the module docstring": (
        "import copy\n",
        '"""fcm_"""\nimport copy\n',
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other[__doc__ + 'credentials'] = creds\n",
    ),
    "key built from an implicit module name": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other[__file__[:0] + 'fcm_' + 'credentials'] = creds\n",
    ),
    "key built from a comprehension target over constants": (
        "K2 = K\n",
        "K2 = K\nA = 'fcm_'\n_ = [{p + A + 'credentials': copy.deepcopy(G)} for p in ['']]\n",
    ),
    "dict comprehension key over constants": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({p + 'credentials': creds for p in ['fcm_']})\n",
    ),
    "unknown key written by __setitem__": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.__setitem__(C, creds)\n",
    ),
    "unknown key written by operator.setitem": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            operator.setitem(other, C, creds)\n",
    ),
    "unknown key written by an unbound setdefault": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            dict.setdefault(other, C, creds)\n",
    ),
    "unknown key in starred setdefault arguments": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.setdefault(*(C, creds))\n",
    ),
    "unknown key of a dict comprehension": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({C: v for v in [creds]})\n",
    ),
    "unknown key in pairs handed to dict": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update(dict([(C, creds)]))\n",
    ),
    "unknown key handed to fromkeys": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update(dict.fromkeys([C], creds))\n",
    ),
    "unknown key in pairs handed to update": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update([(C, creds)])\n",
    ),
    "unknown key of a container written into": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        '            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other[C]["gcm"] = creds\n',
    ),
    "unknown key removed by pop": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.pop(C)\n",
    ),
    "unknown key in pairs handed to an unbound update": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            dict.update(other, [(C, creds)])\n",
    ),
    "unknown mapping spread into update": (
        "K2 = K\n",
        "K2 = K\nKEYS = ('FCM_CREDENTIALS'.lower(),)\nNS = dict(zip(KEYS, (1,)))\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update(**NS)\n",
    ),
    # Round 12 (AP9e): F-A to F-D and the targets of an inner comprehension.
    "unknown key written through an import alias of operator": (
        "import copy\n",
        "import copy\nimport operator as op\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            op.setitem(other, C, creds)\n",
    ),
    "unknown key written through setitem imported from operator": (
        "import copy\n",
        "import copy\nfrom operator import setitem\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            setitem(other, C, creds)\n",
    ),
    "unknown key written through setitem imported under another name": (
        "import copy\n",
        "import copy\nfrom operator import setitem as put\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            put(other, C, creds)\n",
    ),
    "unknown key written through a star import of operator": (
        "import copy\n",
        "import copy\nfrom operator import *\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            setitem(other, C, creds)\n",
    ),
    "unknown key written through operator imported in a function": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            import operator as op\n            op.setitem(other, C, creds)\n",
    ),
    "write method bound to a name": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            s = other.__setitem__\n            s(C, creds)\n",
    ),
    "setdefault bound to a name": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            s = other.setdefault\n            s(C, creds)\n",
    ),
    "write method handed to functools.partial": (
        "import copy\n",
        "import copy\nimport functools\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            functools.partial(other.__setitem__, C)(creds)\n",
    ),
    "operator.setitem handed to functools.partial": (
        "import copy\n",
        "import copy\nimport functools, operator\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            functools.partial(operator.setitem, other, C)(creds)\n",
    ),
    "write method looked up by getattr": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            getattr(other, '__setitem__')(C, creds)\n",
    ),
    "unknown key written through the type of the mapping": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            type(other).__setitem__(other, C, creds)\n",
    ),
    "unknown pairs handed to update of an imported class": (
        "import copy\n",
        "import copy\nfrom collections.abc import MutableMapping\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            MutableMapping.update(other, [(C, creds)])\n",
    ),
    "unknown key written by __setitem__ of an imported class": (
        "import copy\n",
        "import copy\nimport collections.abc\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            collections.abc.MutableMapping.__setitem__(other, C, creds)\n",
    ),
    "unknown key written on a mapping a call returns": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.copy().__setitem__(C, creds)\n",
    ),
    "unknown key written through a class of the file": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            M.__setitem__(other, C, creds)\n",
    ),
    "unknown key written through a class a call returns": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            (lambda x: type(x))(other).__setitem__(other, C, creds)\n",
    ),
    "write method named for operator.methodcaller": (
        "import copy\n",
        "import copy\nimport operator\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n"
        "            operator.methodcaller('__setitem__', C, creds)(other)\n",
    ),
    "unknown pairs handed to update of the type of the mapping": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            type(other).update(other, [(C, creds)])\n",
    ),
    "unknown key in pairs behind an operator": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update([(C, creds)] * 1)\n",
    ),
    "unknown key in pairs behind a conditional": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update([(C, creds)] if creds else [])\n",
    ),
    "unknown key in pairs behind an assignment expression": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update(p := [(C, creds)])\n",
    ),
    "unknown key in pairs behind a slice": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update([(C, creds)][:])\n",
    ),
    "unknown key in pairs added up for dict": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other = dict([] + [(C, creds)])\n",
    ),
    "unknown key behind or handed to fromkeys": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other = dict.fromkeys([] or [C], creds)\n",
    ),
    "key built from __class__ in a method": (
        "K2 = K\n",
        "K2 = K\nclass fcm_:\n    def m(s, v):\n        d = {}\n        d[__class__.__name__ + 'credentials'] = copy.deepcopy(v)\n",
    ),
    "key built from __firstlineno__ in a class body": (
        "K2 = K\n",
        "K2 = K\nclass Q:\n    d = {}\n    d[str(__firstlineno__)] = 1\n",
    ),
    "key built from a type parameter": (
        "K2 = K\n",
        "K2 = K\nclass Q[fcm_]:\n    def m(s, v):\n        d = {}\n        d[fcm_.__name__ + 'credentials'] = copy.deepcopy(v)\n",
    ),
    "key built from __type_params__": (
        "K2 = K\n",
        "K2 = K\nclass Q[fcm_]:\n    def m(s, v):\n        d = {}\n        d[__type_params__[0].__name__ + 'credentials'] = copy.deepcopy(v)\n",
    ),
    "key built from __qualname__ in a class body": (
        "K2 = K\n",
        "K2 = K\nclass fcm_:\n    d = {}\n    d[__qualname__ + 'credentials'] = 1\n",
    ),
    "key built from an inner comprehension target in an outer iterable": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({p + 'credentials': creds for p in [q for q in ['fcm_']]})\n",
    ),
    # Round 13: each iterable decides (R13-F1), a shadowed class name is no
    # class (R13-F2), a subscript target binds no name (R13-F8).
    "comprehension target bound by a second generator over constants": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({p + 'credentials': creds for p in data for p in ['fcm_']})\n",
    ),
    "comprehension target bound by a first generator over constants": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({p + 'credentials': creds for p in ['fcm_'] for p in data})\n",
    ),
    "comprehension target rebound through another target": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({x + 'credentials': creds for x in data for y in x for x in y})\n",
    ),
    "module name read inside a subscript comprehension target": (
        "K2 = K\n",
        "K2 = K\nP = 'fcm_'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({P + 'credentials': creds for d2[P] in data})\n",
    ),
    "unknown key popped from a local that shadows a class name": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass other(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.pop(C, None)\n",
    ),
    "unknown key popped from a type parameter that shadows a class name": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass other(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            def inner[other]() -> None:\n                other.pop(C, None)\n",
    ),
    "unknown key popped from a class name a star import may rebind": (
        "import copy\n",
        "import copy\nfrom operator import *\n",
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass other(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other.pop(C, None)\n",
    ),
    "comprehension target rebound from itself": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({x + 'credentials': creds for x in data for x in x})\n",
    ),
    # Round 14 (R14-F4): an iterable with a string literal may yield it, so
    # its target is no runtime value even where it also reads a local.
    "comprehension target over a list of a local and a constant": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data, 'fcm_']})\n",
    ),
    "comprehension target over a local plus a constant list": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in data + ['fcm_']})\n",
    ),
    "comprehension target over a list joined to a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in ['fcm_'] + data})\n",
    ),
    "comprehension target over a generator of a constant": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in ('fcm_' for _ in data)})\n",
    ),
    "comprehension target over a conditional with a constant list": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in (['fcm_'] if data else data)})\n",
    ),
    # Round 14 (R14-F3): a comprehension inside a subscript target binds no
    # target of the generator around it.
    "module name rebound by a comprehension inside a subscript target": (
        "K2 = K\n",
        "K2 = K\nq = 'fcm_'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({q + 'credentials': creds for d2[[q for q in data][0]] in data})\n",
    ),
    # Round 14 (R14-F1): a class name the file also binds otherwise is read
    # at both positions, so the key of an unbound call on the class is seen.
    "unknown key set through a class name a parameter elsewhere shadows": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(dict):\n    pass\ndef helper(Store):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            Store.__setitem__(other, C, creds)\n",
    ),
    "unknown key popped through a class name a local elsewhere shadows": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(dict):\n    pass\ndef helper():\n    Store = 1\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            Store.pop(other, C)\n",
    ),
    "unknown key defaulted through a class name an except clause shadows": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(dict):\n    pass\ndef helper():\n    try:\n        pass\n    except Exception as Store:\n        pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            Store.setdefault(other, C, creds)\n",
    ),
    "unknown key set through a class name a type parameter elsewhere shadows": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(dict):\n    pass\ndef helper[Store]():\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            Store.__setitem__(other, C, creds)\n",
    ),
    # Round 15 (R15-F1): decided per element; a constant without a string
    # literal, built by a call, may be yielded as well.
    "comprehension target over a local and a decoded bytes constant": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data, bytes((102, 99, 109, 95)).decode()]})\n",
    ),
    "comprehension target over a local and a sum of chr calls": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data, chr(102) + chr(99) + chr(109) + chr(95)]})\n",
    ),
    "comprehension target over a starred local and a call result": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [*data, bytes((102, 99, 109, 95)).decode()]})\n",
    ),
    "comprehension target over a method result of a local": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in data.get('x', ['fcm_'])})\n",
    ),
    "comprehension target over a local and a starred constant list": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data, *['fcm_']]})\n",
    ),
    "comprehension target over the keys of a dict comprehension": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in {'fcm_': v for v in data}})\n",
    ),
    "comprehension target over a local and an f-string constant": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data, f'fcm_']})\n",
    ),
    "comprehension target over a formatted local with a spec": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [f'{data:>4}']})\n",
    ),
    "comprehension target over a local concatenated with a constant": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data + 'fcm_']})\n",
    ),
    "comprehension target over a conditional of a local and a constant": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data if data else 'fcm_']})\n",
    ),
    # Round 15 (R15-F2): a decorator may bind a class name to an instance.
    "unknown key set through a decorated class name": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n@lambda c: c()\nclass Store(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.__setitem__(C, creds)\n",
    ),
    "unknown key popped through a decorated class name": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n@lambda c: c()\nclass Store(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    # Round 16 (R16-F1): the metaclass, named or inherited, may bind a class
    # name to an instance.
    "unknown key set through a class name with a metaclass": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(metaclass=lambda *a: {}):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.__setitem__(C, creds)\n",
    ),
    "unknown key popped through a class name with a metaclass": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(metaclass=lambda *a: {}):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    "unknown key popped through a class with a base that has a metaclass": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Base(metaclass=lambda *a: {}):\n    pass\nclass Store(Base):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    "unknown key popped through a class with spread keywords": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nM = {'metaclass': lambda *a: {}}\nclass Store(**M):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    "unknown key popped through a class whose base the file rebinds": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(type):\n    def __new__(mcs, *a):\n        return {}\ndict = type.__new__(M, 'X', (), {})\nclass Store(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    # Round 17 (R17-F1): a base of the file the file also binds otherwise may
    # hold a class with another metaclass; R17-F3: a starred argument stays
    # undecided, also an empty one.
    "unknown key popped through a class whose base of the file is rebound": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(type):\n    def __new__(mcs, *a):\n        return {}\nclass A:\n    pass\nA = type.__new__(M, 'X', (), {})\nclass Store(A):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    "unknown key set through a class whose base of the file is a loop target": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(type):\n    def __new__(mcs, *a):\n        return {}\nclass A:\n    pass\nfor A in [type.__new__(M, 'X', (), {})]:\n    pass\nclass Store(A):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.__setitem__(C, creds)\n",
    ),
    "unknown key popped through a class whose base of the file is a walrus target": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(type):\n    def __new__(mcs, *a):\n        return {}\nclass A:\n    pass\n(A := type.__new__(M, 'X', (), {}))\nclass Store(A):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    # Round 18 (R18-F1): a class statement may bind a builtin base; R18-F2:
    # an attribute of a class holds what the class holds there.
    "unknown key popped through a class whose base object is a class of the file": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(type):\n    def __new__(mcs, *a):\n        return {}\n@lambda c: type.__new__(M, 'X', (), {})\nclass object:\n    pass\nclass Store(object):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.pop(C)\n",
    ),
    "unknown key set through a class whose base dict is a class of the file": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(type):\n    def __new__(mcs, *a):\n        return {}\n@lambda c: type.__new__(M, 'X', (), {})\nclass dict:\n    pass\nclass Store(dict):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.__setitem__(C, creds)\n",
    ),
    "unknown key popped from a dict attribute of a class": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Outer:\n    d = {}\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Outer.d.pop(C)\n",
    ),
    "unknown key set as default in a dict attribute of a class": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Outer:\n    d = {}\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Outer.d.setdefault(C, creds)\n",
    ),
    "unknown key popped through a nested class with a metaclass": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass M(type):\n    def __new__(mcs, *a):\n        return {}\nclass Outer:\n    class Store(metaclass=M):\n        pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Outer.Store.pop(C)\n",
    ),
    # Round 19: every class is read at both positions. A key at position 0 of
    # a true class makes Python raise, so the guard fails it too; R19-F1: a
    # class nested in a class takes the key at position 1.
    "unknown key set through a class whose bases are the builtins dict and object": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(dict, object):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.__setitem__(C, creds)\n",
    ),
    "unknown key set through a class whose base is a class of the file": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass A(dict):\n    pass\nclass Store(A):\n    pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.__setitem__(C, creds)\n",
    ),
    "unknown key set through a dict class nested in a class": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Outer:\n    class Store(dict):\n        pass\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            cache = {}\n            Outer.Store.__setitem__(cache, C, creds)\n",
    ),
    "unknown key popped at the first position of the type of a mapping": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            type(other).pop(C)\n",
    ),
    "unknown key set as default in a mapping of an imported module": (
        "K2 = K\n",
        "K2 = K\nimport os\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            os.environ.setdefault(C, creds)\n",
    ),
    # Round 20: R20-F1 the declared price of the rule, R20-F2 a classmethod
    # runs on the class, R20-F5 a class defined in the method.
    "undecided mapping at the first position of a class call": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(dict):\n    pass\nCACHE = Store()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.__setitem__(CACHE, 'x', creds)\n",
    ),
    # AP9f: a nested function reads the module name through global although the
    # method binds a local of the same name (the global rule of
    # _function_locals, which upstream cannot observe).
    "member read through global beside a local of the same name": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            def inner():\n                global K2\n                return {K2: 1}\n"
        "            K2 = 'x'\n            other = {}\n            other.update(inner())\n"
        "            updated_data[K2] = copy.deepcopy(registration)\n",
    ),
    # Round 21 (R21-F4): the price at the first position of ``type(d)`` and at
    # the second position of a call receiver.
    "undecided mapping at the first position of the type of a mapping": (
        "K2 = K\n",
        "K2 = K\nCACHE = dict()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            type(CACHE).__setitem__(CACHE, 'x', creds)\n",
    ),
    "undecided mapping at the second position of a call receiver": (
        "K2 = K\n",
        "K2 = K\nCACHE = dict()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            dict().setdefault('x', CACHE)\n",
    ),
    "unknown key set through a classmethod of a class of the file": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\nclass Store(dict):\n    data = {}\n    @classmethod\n    def setdefault(cls, key, value=None):\n        return cls.data.setdefault(key, value)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            Store.setdefault(C, creds)\n",
    ),
    "unknown key set through a class defined in the method": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            class Store(dict):\n                pass\n            cache = {}\n            Store.__setitem__(cache, C, creds)\n",
    ),
    "comprehension target over sorted with an empty spread": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in sorted(data, *[])})\n",
    ),
    # Round 16 (R16-F2): a reordering builtin, ``or`` and a dict display yield
    # what their operands yield, a constant as well.
    "comprehension target over sorted of a local and a constant": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in sorted([data, 'fcm_'])})\n",
    ),
    "comprehension target over a local or a constant list": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in data or ['fcm_']})\n",
    ),
    "comprehension target over a dict display with a constant key": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in {data: 1, 'fcm_': 2}})\n",
    ),
    "comprehension target over sorted next to a star import": (
        "K2 = K\n",
        "K2 = K\nfrom operator import *\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in sorted(data)})\n",
    ),
    "comprehension target over sorted a local rebinds": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            sorted = lambda x: ['fcm_']\n            other.update({p + 'credentials': creds for p in sorted(data)})\n",
    ),
    "comprehension target over sorted the file rebinds": (
        "K2 = K\n",
        "K2 = K\ndef sorted(x):\n    return ['fcm_']\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in sorted(data)})\n",
    ),
    "comprehension target over a call to dict of a local": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in dict(data)})\n",
    ),
    "comprehension target over a repeated local": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in data * 2})\n",
    ),
}

_FOLDED_ACCEPTED: dict[str, tuple[str, ...]] = {
    "comprehension target over constants read only by the value": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {'x': p for p in ('a', 'b')}\n",
    ),
    "name with as many bindings as the limit": (
        "K2 = K\n",
        "K2 = K\n" + "".join(f"K3 = 'v' + '{i}'\n" for i in range(_VALUE_LIMIT)),
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            updated_data[K3] = copy.deepcopy(registration)\n",
    ),
    # Round 17 (R17-F4): ``and``/``or`` as an element and in the element of a
    # comprehension.
    "comprehension target over a list of a subscript or a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data['a'] or data]})\n",
    ),
    "comprehension target over a list of a parameter and a subscript": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [data and data['a']]})\n",
    ),
    "comprehension target over a generator of x or x": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in (x or x for x in data)})\n",
    ),
    # Round 15 (R15-F3): what a subscript or a condition reads is not yielded.
    "comprehension target over a subscript of a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in data['items']})\n",
    ),
    "comprehension target over a starred parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [*data]})\n",
    ),
    "comprehension target over an f-string of a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in [f'{data}']})\n",
    ),
    "comprehension target over a filtered generator of a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in (x for x in data if x != 'skip')})\n",
    ),
    # Round 16 (R16-F2): a reordering builtin of one argument, ``or`` and a
    # dict display yield what a read gives.
    "comprehension target over sorted of a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in sorted(data)})\n",
    ),
    "comprehension target over sorted with key and reverse": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in sorted(data, key=len, reverse=True)})\n",
    ),
    "comprehension target over list of a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in list(data)})\n",
    ),
    "comprehension target over tuple of a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in tuple(data)})\n",
    ),
    "comprehension target over reversed of a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in reversed(data)})\n",
    ),
    "comprehension target over a parameter or an empty list": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in data or []})\n",
    ),
    "comprehension target over a dict display keyed by a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in {data: 1}})\n",
    ),
    "comprehension target over a spread parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other, d2 = {}, {}\n            other.update({p + 'credentials': creds for p in {**data}})\n",
    ),
    "key constant built by concatenation": (
        'K = "fcm_credentials"\n',
        'K = "fcm_" + "credentials"\n',
    ),
    "f-string key without a runtime value": (
        '        updated_bundle["fcm_credentials"] = registration\n',
        "        updated_bundle[f\"fcm_{'credentials'}\"] = registration\n",
    ),
    # AP9e: the same write positions with keys the guard can bound.
    "key from a target of an earlier generator over a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        "            other.update({k: 1 for row in data for k in row})\n",
    ),
    "starred pair with a constant key": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        '            other.setdefault(*("other", C))\n',
    ),
    "pairs with a constant key and an unknown value": (
        "K2 = K\n",
        "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n"
        "            other = {}\n"
        '            other.update([("other", C)])\n',
    ),
    "dict comprehension over a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({k: v for k, v in data.items()})\n",
    ),
    "update with a local mapping": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update(data)\n",
    ),
    "setdefault with another constant key": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        '            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.setdefault("other", creds)\n',
    ),
    "container written into under another constant key": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        '            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other["x"]["gcm"] = creds\n',
    ),
    "key part bound nowhere": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other[NOWHERE + 'x'] = creds\n",
    ),
    # Round 12 (AP9e): forms the F-A to F-I rules leave alone.
    "key from an inner comprehension target over a parameter": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({p + 'x': creds for p in [q for q in data]})\n",
    ),
    "dict display handed to update": (
        "K2 = K\n",
        "K2 = K\nX = 'x'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({X: creds})\n",
    ),
    "pairs a call builds from a module name": (
        "K2 = K\n",
        "K2 = K\nX = 'x'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update(zip([X], [creds]))\n",
    ),
    "comprehension target hides a module name in a key": (
        "K2 = K\n",
        "K2 = K\nP = 'fcm_'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({P + 'credentials': creds for P in data})\n",
    ),
    "local named like a write function": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            setitem = other.get\n            setitem(data)\n",
    ),
    "operator function that writes nothing": (
        "import copy\n",
        "import copy\nimport operator\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            operator.itemgetter(0)(data)\n",
    ),
    "pairs over locals behind an operator": (
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update([(data, creds)] * 1)\n",
    ),
    # R14-F3: a starred element of a tuple target binds its name.
    "module name rebound by a starred comprehension target": (
        "K2 = K\n",
        "K2 = K\np = 'fcm_'\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n",
        "            updated_data[K2] = copy.deepcopy(registration)\n            other = {}\n            other.update({p + 'credentials': creds for (_, *p) in data})\n",
    ),
}


#: Upstream forms the folding decides the other way, with the reason.
_UPSTREAM_ONLY: dict[str, str] = {
    "known key copied to a name with another binding": (
        "an annotation without a value binds nothing, so the folding knows the "
        "name and checks the write as a deep copy"
    ),
    "known key copied to a name that also reads a cycle": (
        "a cycle of plain aliases holds only what flows in from outside, here "
        "nothing, so the name holds the member alone and its write is checked"
    ),
}

#: Upstream limits that stay limits here, with the reason.
_STILL_NOT_SEEN: dict[str, str] = {
    "key imported from a module other than const": (
        "a name from another module holds no string the folding sees"
    ),
}

#: Forms the fork states it does not follow (round 13, R13-F4 to R13-F7),
#: each a one-place change to ``_CLEAN`` with an unknown key ``C``; the
#: write stays unseen, so the scan reports nothing.
_UNKNOWN_C = ("K2 = K\n", "K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n")
_SECOND_WRITE = "            updated_data[K2] = copy.deepcopy(registration)\n"


def _unseen(line: str, *head: str) -> tuple[str, ...]:
    write = _SECOND_WRITE + "            other = {}\n" + line
    return (*head, *_UNKNOWN_C, _SECOND_WRITE, write)


_FORK_NOT_SEEN: dict[str, tuple[str, ...]] = {
    "statement loop over constants binding a local": (
        _SECOND_WRITE,
        _SECOND_WRITE
        + "            for p in ('fcm_',): updated_data[p + 'credentials'] = creds\n",
    ),
    "local bound by a method of a constant": (
        _SECOND_WRITE,
        _SECOND_WRITE
        + "            p = 'FCM_CREDENTIALS'.lower(); updated_data[p] = creds\n",
    ),
    "local bound by a join of constants": (
        _SECOND_WRITE,
        _SECOND_WRITE
        + "            p = ''.join(['fcm_', 'credentials']); updated_data[p] = creds\n",
    ),
    "statement loop over a method of a constant": (
        _SECOND_WRITE,
        _SECOND_WRITE
        + "            for p in ('FCM_CREDENTIALS'.lower(),): updated_data[p] = creds\n",
    ),
    "membership test of the whole member": (
        _SECOND_WRITE,
        _SECOND_WRITE + "            y = 'fcm_credentials' in updated_data\n",
    ),
    "negated membership test of the whole member": (
        _SECOND_WRITE,
        _SECOND_WRITE + "            y = 'fcm_credentials' not in creds\n",
    ),
    "pairs a call builds from a computed key": _unseen(
        "            other.update(zip((C,), (creds,)))\n"
    ),
    "pairs a call builds for dict": _unseen(
        "            other = dict(zip((C,), (creds,)))\n"
    ),
    "pairs a call builds for dict spread into a display": _unseen(
        "            other = {**dict(zip((C,), (creds,)))}\n"
    ),
    "write function imported as __setitem__": _unseen(
        "            __setitem__(other, C, creds)\n",
        "import copy\n", "import copy\nfrom operator import __setitem__\n",
    ),
    "delete function imported as __delitem__": _unseen(
        "            __delitem__(other, C)\n",
        "import copy\n", "import copy\nfrom operator import __delitem__\n",
    ),
    "write function through a local bound to operator": _unseen(
        "            op = operator\n            op.setitem(other, C, creds)\n",
        "import copy\n", "import copy\nimport operator\n",
    ),
    "write function through attrgetter": _unseen(
        "            operator.attrgetter('__setitem__')(other)(C, creds)\n",
        "import copy\n", "import copy\nimport operator\n",
    ),
    "write function looked up by getattr with a name": _unseen(
        "            M = '__setitem__'\n            getattr(other, M)(C, creds)\n"
    ),
    "write function through __getattribute__": _unseen(
        "            other.__getattribute__('__setitem__')(C, creds)\n"
    ),
    "write function of the class reached through __class__": _unseen(
        "            other.__class__.__setitem__(other, C, creds)\n"
    ),
    "write function of dict bound to a local": _unseen(
        "            D = dict\n            D.__setitem__(other, C, creds)\n"
    ),
    "write function of a type bound to a local": _unseen(
        "            T = type(other)\n            T.__setitem__(other, C, creds)\n"
    ),
    "pairs merged by |=": _unseen("            other |= [(C, creds)]\n"),
    "pairs merged by __ior__": _unseen("            other.__ior__([(C, creds)])\n"),
    "pairs handed to dict.__init__": _unseen(
        "            dict.__init__(other, [(C, creds)])\n"
    ),
    "private name mangled in a class body": (
        "K2 = K\n",
        "K2 = K\n_Q__P = 'fcm_'\n__P = 'x'\nclass Q:\n    d = {}\n    d[__P + 'credentials'] = 1\n",
    ),
}  # fmt: skip

_ALL_BROKEN = {
    **{k: v for k, v in _BROKEN.items() if k not in _UPSTREAM_ONLY},
    **_FOLDED_BROKEN,
}
_ALL_ACCEPTED = {
    **_ACCEPTED,
    **{k: v for k, v in _BROKEN.items() if k in _UPSTREAM_ONLY},
    **_FOLDED_ACCEPTED,
}

#: The messages of every rejected form, unknown keys first, then the shared
#: places (N2 after round 13): a rule that only changes why a form fails, not
#: whether, is caught here. Regenerate the entry of a form whose cause changed.
_REJECTED_MESSAGES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "cache recognised by name only": (("line 14: self._bundle_cache.set('fcm_credentials', creds)",), ()),
    "class constant as key": (("line 9: KEY = 'fcm_credentials'",), ()),
    "comprehension in a class body over a name of the body": (("line 9: unknown key p + 'credentials'",), ()),
    "comprehension over constants inside a call over a parameter": (("line 24: unknown key ''.join([p for p in ['fcm_', 'credentials']] + list(creds))",), ()),
    "comprehension target bound by a first generator over constants": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target bound by a second generator over constants": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target does not hide a module key": (("line 24: unknown key K3",), ()),
    "comprehension target of the copied name": ((), ("line 19: updated_bundle['fcm_credentials']",)),
    "comprehension target rebound from itself": (("line 24: unknown key x + 'credentials'",), ()),
    "comprehension target rebound through another target": (("line 24: unknown key x + 'credentials'",), ()),
    "const attribute as key": ((), ("line 22: updated_data[const.FCM_CREDENTIALS]",)),
    "const constant deleted and imported again": ((), ("line 24: updated_data[FCM_CREDENTIALS]",)),
    "const constant imported again inside a function under its name": (("line 15: unknown key FCM_CREDENTIALS",), ()),
    "const constant imported inside a function": (("line 15: unknown key FCM_CREDENTIALS as F2",), ()),
    "const constant imported with a fallback that is no key": ((), ("line 25: updated_data[FC]",)),
    "const constant rebound through its handle": (("line 7: unknown key const.FCM_CREDENTIALS",), ()),
    "const handle alias with a binding that is no handle": (("line 24: unknown key C2.FCM_CREDENTIALS",), ()),
    "const handle deleted and imported again": ((), ("line 24: updated_data[const.FCM_CREDENTIALS]",)),
    "const handle rebound in the module": ((), ("line 24: updated_data[const.FCM_CREDENTIALS]",)),
    "const imported inside a function": ((), ("line 23: updated_data[c2.FCM_CREDENTIALS]",)),
    "const reached through the package path": (("line 23: unknown key custom_components.googlefindmy.const.FCM_CREDENTIALS",), ()),
    "container bound to a second name": ((), ("line 21: the container alias is not a fresh local",)),
    "container handed positionally to a call outside the writer list": ((), ("line 25: the container updated_data is shared",)),
    "container handed to a call outside the writer list": ((), ("line 25: the container updated_data is shared",)),
    "container kept by the receiver after the write": ((), ("line 21: the container updated_bundle is shared",)),
    "container returned": ((), ("line 26: the container updated_data is shared",)),
    "container taken from receiver state": ((), ("line 17: the container updated_bundle is not a fresh local",)),
    "copy bound as a class type parameter": ((), ("copy is not only bound by 'import copy' and called as deepcopy",)),
    "copy bound as a function type parameter": ((), ("copy is not only bound by 'import copy' and called as deepcopy",)),
    "copy dispatch table patched": ((), ("copy is not only bound by 'import copy' and called as deepcopy",)),
    "copy imported under a second name": ((), ("copy or dict is reached other than by its name and may be replaced",)),
    "copy patched through its __dict__": ((), ("copy is not only bound by 'import copy' and called as deepcopy",)),
    "copy reached through __import__": ((), ("copy or dict is reached other than by its name and may be replaced",)),
    "copy reached through importlib": ((), ("copy or dict is reached other than by its name and may be replaced",)),
    "copy shadowed by a parameter": ((), ("copy is not only bound by 'import copy' and called as deepcopy",)),
    "copy.deepcopy reassigned": ((), ("copy is not only bound by 'import copy' and called as deepcopy", "copy or dict is reached other than by its name and may be replaced")),
    "copy.deepcopy removed through delattr": ((), ("copy is not only bound by 'import copy' and called as deepcopy", "copy or dict is reached other than by its name and may be replaced")),
    "copy.deepcopy replaced through setattr": ((), ("copy is not only bound by 'import copy' and called as deepcopy", "copy or dict is reached other than by its name and may be replaced")),
    "deep copy also bound to a local in one statement": ((), ("line 22: one value bound to several targets",)),
    "deep copy also bound to an attribute in one statement": ((), ("line 22: one value bound to several targets",)),
    "deep copy also kept by the receiver": ((), ("line 16: registration is used beside its write",)),
    "deep copy bound only in a nested class body": ((), ("line 19: updated_bundle['fcm_credentials']",)),
    "deep copy bound only in a nested def": ((), ("line 19: updated_bundle['fcm_credentials']",)),
    "deep copy rebound through nonlocal": ((), ("line 21: updated_bundle['fcm_credentials']",)),
    "deep copy written twice": ((), ("line 18: registration is used beside its write",)),
    "deepcopy replaced through sys.modules": ((), ("copy or dict is reached other than by its name and may be replaced",)),
    "deepcopy with a memo": ((), ("line 22: registration is used beside its write", "line 22: updated_data[K]")),
    "detached local handed to deepcopy as a memo": ((), ("line 17: registration is used beside its write",)),
    "dict comprehension key over constants": (("line 24: unknown key p + 'credentials'",), ()),
    "dict display without copy": ((), ("line 18: {**bundle, 'fcm_credentials': dict(creds)}",)),
    "dict rebound by a parameter": ((), ("dict is bound in the module, so dict(x) may not be a new dict",)),
    "dict replaced through builtins": ((), ("copy or dict is reached other than by its name and may be replaced",)),
    "display container kept by the receiver": ((), ("line 19: the container updated_bundle is shared",)),
    "f-string key with a constant placeholder": ((), ("line 22: updated_data[f\"fcm_{'credentials'}\"]",)),
    "first iterable reads the enclosing name, not a later target": (("line 25: unknown key q + 'credentials'",), ()),
    "fully qualified const constant as key": ((), ("line 23: updated_data[custom_components.googlefindmy.const.FCM_CREDENTIALS]",)),
    "inner container read back by index": ((), ("line 26: an element of the container updated_data is read back",)),
    "key bound by a module-level call": (("line 23: unknown key K3",), ()),
    "key bound through global in a class body": (("line 26: unknown key K3",), ()),
    "key bound through global in a function": (("line 27: unknown key K3",), ()),
    "key bound to an attribute at module level": (("line 7: SETTINGS.key = K",), ()),
    "key built by augmented assignment": (("line 24: unknown key K3",), ()),
    "key built from __class__ in a method": (("line 10: unknown key __class__.__name__ + 'credentials'",), ()),
    "key built from __firstlineno__ in a class body": (("line 9: unknown key str(__firstlineno__)",), ()),
    "key built from __qualname__ in a class body": (("line 9: unknown key __qualname__ + 'credentials'",), ()),
    "key built from __type_params__": (("line 10: unknown key __type_params__[0].__name__ + 'credentials'",), ()),
    "key built from a comprehension target over constants": (("line 8: unknown key p + A + 'credentials'",), ()),
    "key built from a name of a class body": (("line 10: unknown key P + 'credentials'",), ()),
    "key built from a name rebound later": ((), ("line 25: updated_data[K3]",)),
    "key built from a type parameter": (("line 10: unknown key fcm_.__name__ + 'credentials'",), ()),
    "key built from an implicit module name": (("line 24: unknown key __file__[:0] + 'fcm_' + 'credentials'",), ()),
    "key built from an inner comprehension target in an outer iterable": (("line 24: unknown key p + 'credentials'",), ()),
    "key built from the module docstring": (("line 25: unknown key __doc__ + 'credentials'",), ()),
    "key concatenated from constants": ((), ("line 22: updated_data['fcm_' + 'credentials']",)),
    "key constant bound in a module-level for block": ((), ("line 24: updated_data[K3]",)),
    "key constant rebound to the member": ((), ("line 24: updated_data[K3]",)),
    "key formatted with percent": (("line 24: unknown key K3",), ()),
    "key from a call over a module number": (("line 23: unknown key chr(X) + 'cm_credentials'",), ()),
    "key from a method of a module bytes value": (("line 23: unknown key X.decode()",), ()),
    "key from a target of an earlier generator over constants": (("line 24: unknown key k + 'credentials'",), ()),
    "key from an f-string with a conversion": (("line 24: unknown key K3",), ()),
    "key from an f-string with a format spec": (("line 24: unknown key K3",), ()),
    "key grown by a self-referencing binding": (("line 25: unknown key A",), ()),
    "key name growing without bound": (("line 25: unknown key K3",), ()),
    "key part bound by walrus two comprehensions deep": (("line 24: unknown key K3",), ()),
    "key part from a class attribute": (("line 25: unknown key K3",), ()),
    "key part from a method of a constant": (("line 23: unknown key K3",), ()),
    "key part taken by subscript": (("line 24: unknown key K3",), ()),
    "key sliced from a name bound later": (("line 24: unknown key K3",), ()),
    "key through a cycle over a multiplication": (("line 27: unknown key C",), ()),
    "comprehension target over constants": (("line 23: unknown key p + 'credentials'",), ()),
    "comprehension target over constants read by a key inside the value": (("line 23: unknown key p + 'credentials'",), ()),
    "local bound to the whole member": (("line 23: p = 'fcm_credentials'",), ()),
    "statement loop over the whole member": (("line 23: ('fcm_credentials',)",), ()),
    "name with more bindings than the limit": (("line 56: unknown key K3",), ()),
    "key with more values than the limit": (("line 36: unknown key K3",), ()),
    "member constant name as an attribute of another object": (("line 22: unknown key self.FCM_CREDENTIALS",), ()),
    "module name read inside a subscript comprehension target": (("line 25: {P + 'credentials': creds for d2[P] in data}",), ()),
    "name imported from const as key": ((), ("line 22: updated_data[FCM_CREDENTIALS]",)),
    "nested write through a read": (("line 13: bundle['fcm_credentials']",), ()),
    "one deep copy bound to two names": ((), ("line 16: updated_data[K]", "line 19: updated_bundle['fcm_credentials']")),
    "one deep copy written to two keys": ((), ("line 22: one value written to several keys",)),
    "operator.setitem handed to functools.partial": (("line 26: unknown key operator.setitem",), ()),
    "outer container kept by the receiver": ((), ("line 26: the container updated_data is shared",)),
    "read handed to receiver state": (("line 13: bundle.get('fcm_credentials')",), ()),
    "setdefault bound to a name": (("line 25: unknown key other.setdefault",), ()),
    "setdefault with the member": (("line 21: updated_data.setdefault('fcm_credentials', creds)",), ("line 21: the container updated_data is shared",)),
    "shallow write": ((), ("line 18: updated_bundle['fcm_credentials']",)),
    "shared write through an alias of the const handle": ((), ("line 23: updated_data[C2.FCM_CREDENTIALS]",)),
    "star import from const": (("line 4: unknown key from custom_components.googlefindmy.const import *",), ("copy is not only bound by 'import copy' and called as deepcopy", "dict is bound in the module, so dict(x) may not be a new dict")),
    "star import may rebind copy": ((), ("copy is not only bound by 'import copy' and called as deepcopy", "dict is bound in the module, so dict(x) may not be a new dict")),
    "t-string spelling the member": (("line 18: t'fcm_credentials'", "line 18: unknown key t'fcm_credentials'"), ()),
    "unknown arguments spread into operator.setitem": (("line 24: unknown key ARGS",), ()),
    "unknown key behind or handed to fromkeys": (("line 25: unknown key C",), ()),
    "unknown key handed to fromkeys": (("line 25: unknown key C",), ()),
    "unknown key in generated pairs": (("line 25: unknown key C",), ()),
    "unknown key in pairs added up for dict": (("line 25: unknown key C",), ()),
    "unknown key in pairs behind a conditional": (("line 25: unknown key C",), ()),
    "unknown key in pairs behind a slice": (("line 25: unknown key C",), ()),
    "unknown key in pairs behind an assignment expression": (("line 25: unknown key C",), ()),
    "unknown key in pairs behind an operator": (("line 25: unknown key C",), ()),
    "unknown key in pairs handed to an unbound update": (("line 25: unknown key C",), ()),
    "unknown key in pairs handed to dict": (("line 25: unknown key C",), ()),
    "unknown key in pairs handed to update": (("line 25: unknown key C",), ()),
    "unknown key in starred setdefault arguments": (("line 25: unknown key C",), ()),
    "unknown key of a container written into": (("line 25: unknown key C",), ()),
    "unknown key of a dict comprehension": (("line 25: unknown key C",), ()),
    "unknown key popped from a class name a star import may rebind": (("line 27: unknown key C",), ("copy is not only bound by 'import copy' and called as deepcopy", "dict is bound in the module, so dict(x) may not be a new dict")),
    "unknown key popped from a local that shadows a class name": (("line 27: unknown key C",), ()),
    "comprehension target over a list of a local and a constant": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a local plus a constant list": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a generator of a constant": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a conditional with a constant list": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a local and a decoded bytes constant": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a local concatenated with a constant": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a conditional of a local and a constant": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a formatted local with a spec": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a local and a starred constant list": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a local and an f-string constant": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over the keys of a dict comprehension": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a local and a sum of chr calls": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a method result of a local": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a starred local and a call result": (("line 24: unknown key p + 'credentials'",), ()),
    "module name rebound by a comprehension inside a subscript target": (("line 25: {q + 'credentials': creds for d2[[q for q in data][0]] in data}",), ()),
    "unknown key set through a class name a parameter elsewhere shadows": (("line 29: unknown key C",), ()),
    "unknown key popped through a class name a local elsewhere shadows": (("line 29: unknown key C",), ()),
    "unknown key popped through a decorated class name": (("line 27: unknown key C",), ()),
    "unknown key set through a decorated class name": (("line 27: unknown key C",), ()),
    'comprehension target over sorted a local rebinds': (("line 25: unknown key p + 'credentials'",), ()),
    'comprehension target over sorted next to a star import': (("line 25: unknown key p + 'credentials'",), ("copy is not only bound by 'import copy' and called as deepcopy", 'dict is bound in the module, so dict(x) may not be a new dict')),
    'unknown key popped through a class whose base the file rebinds': (('line 30: unknown key C',), ('dict is bound in the module, so dict(x) may not be a new dict',)),
    'unknown key popped through a class whose base of the file is rebound': (('line 32: unknown key C',), ()),
    'unknown key set through a class whose base of the file is a loop target': (('line 33: unknown key C',), ()),
    'unknown key popped through a class whose base of the file is a walrus target': (('line 32: unknown key C',), ()),
    'unknown key popped through a class whose base object is a class of the file': (('line 32: unknown key C',), ()),
    'unknown key set through a class whose base dict is a class of the file': (('line 32: unknown key C',), ('dict is bound in the module, so dict(x) may not be a new dict',)),
    'unknown key popped from a dict attribute of a class': (('line 26: unknown key C',), ()),
    'unknown key set as default in a dict attribute of a class': (('line 26: unknown key C',), ()),
    'unknown key popped through a nested class with a metaclass': (('line 30: unknown key C',), ()),
    'unknown key set through a class whose bases are the builtins dict and object': (('line 26: unknown key C',), ()),
    'unknown key set through a class whose base is a class of the file': (('line 28: unknown key C',), ()),
    'unknown key set through a dict class nested in a class': (('line 28: unknown key C',), ()),
    'unknown key popped at the first position of the type of a mapping': (('line 25: unknown key C',), ()),
    'unknown key set as default in a mapping of an imported module': (('line 25: unknown key C',), ()),
    'undecided mapping at the first position of a class call': (('line 27: unknown key CACHE',), ()),
    'undecided mapping at the first position of the type of a mapping': (('line 24: unknown key CACHE',), ()),
    'member read through global beside a local of the same name': ((), ('line 24: {K2: 1}',)),
    'undecided mapping at the second position of a call receiver': (('line 24: unknown key CACHE',), ()),
    'comprehension target over a list joined to a parameter': (("line 24: unknown key p + 'credentials'",), ()),
    'unknown key set through a classmethod of a class of the file': (('line 29: unknown key C',), ()),
    'unknown key set through a class defined in the method': (('line 27: unknown key C',), ()),
    "comprehension target over sorted with an empty spread": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a call to dict of a local": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a dict display with a constant key": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a local or a constant list": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over a repeated local": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over sorted of a local and a constant": (("line 24: unknown key p + 'credentials'",), ()),
    "comprehension target over sorted the file rebinds": (("line 26: unknown key p + 'credentials'",), ()),
    "unknown key popped through a class name with a metaclass": (("line 26: unknown key C",), ()),
    "unknown key popped through a class with a base that has a metaclass": (("line 28: unknown key C",), ()),
    "unknown key popped through a class with spread keywords": (("line 27: unknown key C",), ()),
    "unknown key set through a class name with a metaclass": (("line 26: unknown key C",), ()),
    "unknown key defaulted through a class name an except clause shadows": (("line 32: unknown key C",), ()),
    "unknown key set through a class name a type parameter elsewhere shadows": (("line 29: unknown key C",), ()),
    "unknown key popped from a type parameter that shadows a class name": (("line 27: unknown key C",), ()),
    "unknown key removed by pop": (("line 25: unknown key C",), ()),
    "unknown key written by __setitem__": (("line 25: unknown key C",), ()),
    "unknown key written by __setitem__ of an imported class": (("line 26: unknown key C",), ()),
    "unknown key written by an unbound setdefault": (("line 25: unknown key C",), ()),
    "unknown key written by operator.setitem": (("line 25: unknown key C",), ()),
    "unknown key written on a mapping a call returns": (("line 25: unknown key C",), ()),
    "unknown key written through a class a call returns": (("line 25: unknown key C",), ()),
    "unknown key written through a class of the file": (("line 27: unknown key C",), ()),
    "unknown key written through a star import of operator": ((), ("copy is not only bound by 'import copy' and called as deepcopy", "dict is bound in the module, so dict(x) may not be a new dict")),
    "unknown key written through an import alias of operator": (("line 26: unknown key C",), ()),
    "unknown key written through operator imported in a function": (("line 26: unknown key C",), ()),
    "unknown key written through setitem imported from operator": (("line 26: unknown key C",), ()),
    "unknown key written through setitem imported under another name": (("line 26: unknown key C",), ()),
    "unknown key written through the type of the mapping": (("line 25: unknown key C",), ()),
    "unknown mapping spread into update": (("line 26: unknown key NS",), ()),
    "unknown module key inside a call": (("line 23: unknown key str(K3)",), ()),
    "unknown pair spread into setdefault": (("line 25: unknown key NSP",), ()),
    "unknown pairs handed to update of an imported class": (("line 26: unknown key C",), ()),
    "unknown pairs handed to update of the type of the mapping": (("line 25: unknown key C",), ()),
    "write in the first iterable keyed by the enclosing name": (("line 25: other.setdefault(P + 'credentials', 1)",), ()),
    "write into a parameter": ((), ("line 11: the container bundle is shared", "line 17: the container bundle is shared", "line 9: the container bundle is not a fresh local")),
    "write method bound to a name": (("line 25: unknown key other.__setitem__",), ()),
    "write method handed to functools.partial": (("line 26: unknown key other.__setitem__",), ()),
    "write method looked up by getattr": (("line 25: unknown key getattr(other, '__setitem__')",), ()),
    "write method named for operator.methodcaller": (("line 26: unknown key operator.methodcaller('__setitem__', C, creds)",), ()),
    "write through a module alias of the key": ((), ("line 22: updated_data[K2]",)),
}  # fmt: skip

#: What the folding gives for each upstream limit it decides (count of
#: writes, unknown keys, shared places).
_DECIDED_HERE: dict[str, tuple[int, tuple[str, ...], tuple[str, ...]]] = {
    "key bound by augmented assignment": (1, ("line 24: unknown key K3",), ()),
    "key bound in a module-level for block": (1, ("line 7: ('fcm_' + 'credentials',)", "line 24: unknown key K3"), ()),
    "key bound through global in a function": (1, ("line 10: K3 = 'fcm_' + 'credentials'", "line 26: unknown key K3"), ()),
    "key built by concatenation": (2, (), ()),
    "key from a class attribute": (1, ("line 8: A = 'fcm_' + 'credentials'", "line 24: unknown key NS.A"), ()),
    "key from an f-string": (2, (), ()),
}  # fmt: skip


@pytest.mark.parametrize("label", sorted(_ALL_BROKEN))
def test_the_folding_guard_rejects_a_shared_or_unknown_form(label: str) -> None:
    """Each form of ``_ALL_BROKEN`` is reported."""

    _skip_newer_syntax(label)
    _writes, unclassified, shared = _snippet_problems(
        _changed(_ALL_BROKEN[label], label)
    )
    assert unclassified or shared, label
    assert (tuple(unclassified), tuple(shared)) == _REJECTED_MESSAGES[label], label


def test_every_rejected_form_has_its_messages_pinned() -> None:
    """Each form of ``_ALL_BROKEN`` has its messages pinned, and no pin is left over."""

    assert set(_REJECTED_MESSAGES) == set(_ALL_BROKEN)


@pytest.mark.parametrize("label", sorted(_ALL_ACCEPTED))
def test_the_folding_guard_accepts_an_equivalent_form(label: str) -> None:
    """Each equivalent form passes with both writes counted."""

    source = _changed(_ALL_ACCEPTED[label], label)
    assert _snippet_problems(source) == (2, [], []), label


@pytest.mark.parametrize("label", sorted(_FORK_NOT_SEEN))
def test_a_form_the_fork_does_not_follow_is_not_seen(label: str) -> None:
    """The stated limits of round 13 hold in code: the write is not reported.
    A form that becomes seen moves to ``_FOLDED_BROKEN`` with its docstring."""

    _writes, unclassified, shared = _snippet_problems(
        _changed(_FORK_NOT_SEEN[label], label)
    )
    assert (unclassified, shared) == ([], []), label


def test_every_upstream_limit_decided_here_has_its_result_pinned() -> None:
    """R15-F6: the pinned results and the parametrized limits are one set, so
    a limit upstream adds or drops is not silently left out."""

    assert set(_DECIDED_HERE) == set(_NOT_SEEN) - set(_STILL_NOT_SEEN)


@pytest.mark.parametrize("label", sorted(set(_NOT_SEEN) - set(_STILL_NOT_SEEN)))
def test_a_key_form_upstream_does_not_see_is_seen_here(label: str) -> None:
    """Every stated upstream limit on computed keys is decided by the folding."""

    writes, unclassified, shared = _snippet_problems(_changed(_NOT_SEEN[label], label))
    assert (writes, unclassified, shared) != (1, [], []), label
    assert (writes, tuple(unclassified), tuple(shared)) == _DECIDED_HERE[label], label


class _Runtime:
    """A runtime value the guard knows nothing of: ``+`` with a list
    concatenates as a list does (R20-F6: ``['fcm_'] + data`` writes), ``+``
    with anything else keeps it opaque. Its truth value and whether it holds an element are given
    apart, and every oracle runs all four products (``_VARIANTS``), so the
    right side of ``or``/``and``, both branches of a conditional and an empty
    iteration each run in some variant (R18-F3, R19-F2). A value that holds an
    element yields itself once (R14-F6: a swallowed ``TypeError`` hid twelve
    writes); an empty one yields nothing and raises ``IndexError`` on a
    subscript, as an empty list does."""

    def __init__(self, truth: bool, holds: bool) -> None:
        self.truth = truth
        self.holds = holds

    def __add__(self, other: object) -> object:
        return [*self, *other] if isinstance(other, list) else self

    def __radd__(self, other: object) -> object:
        return [*other, *self] if isinstance(other, list) else self

    def __iter__(self) -> Iterator[_Runtime]:
        return iter((self,) if self.holds else ())

    def __getitem__(self, _key: object) -> _Runtime:
        if not self.holds:
            raise IndexError(_key)
        return self

    def __len__(self) -> int:
        return int(self.truth)

    def __lt__(self, _other: object) -> bool:
        return False

    def __gt__(self, _other: object) -> bool:
        return False


#: Every product of truth value and holding an element.
_VARIANTS = ((True, True), (False, True), (True, False), (False, False))


@pytest.mark.parametrize(("truth", "holds"), _VARIANTS)
def test_the_runtime_value_stays_opaque_beside_a_string(
    truth: bool, holds: bool
) -> None:
    """Concatenated with a string on either side, the runtime value is itself
    again, so a key the guard cannot know never becomes the member, not even
    beside the member's own name (R21 follow-up: no case told the opaque
    branch of either operator from one that returns the string)."""

    value = _Runtime(truth, holds)
    assert value + _MEMBER is value
    assert _MEMBER + value is value


def _python_writes(comprehension: str, scope: Mapping[str, object]) -> list[bool]:
    """Whether Python builds the member key, per variant of ``data``; a
    lookup that raises on an empty value builds nothing."""

    written = []
    for truth, holds in _VARIANTS:
        try:
            value = eval(  # noqa: S307 - generated test code only
                comprehension, {**scope, "data": _Runtime(truth, holds)}
            )
        except LookupError:
            value = {}
        written.append(_MEMBER in value)
    return written


#: Iterables of a generated comprehension: the runtime parameter ``data``,
#: constants with and without the prefix, a target bound before (``{t}``),
#: iterables that read ``data`` and also yield a constant (R14-F4), in either
#: branch of a conditional (R19-F3), on either side of ``+`` (R20-F6), also one
#: without a string literal (R15-F1), a subscript and a filtered generator of
#: runtime values (R15-F3), and a comprehension nested in the iterable.
_TARGET_ITERABLES = (
    "data",
    "['fcm_']",
    "['x']",
    "[{t}]",
    "{t}",
    "[data, 'fcm_']",
    "data + ['fcm_']",
    "['fcm_'] + data",
    "('fcm_' for _ in data)",
    "(['fcm_'] if data else data)",
    "(data if data else ['fcm_'])",
    "[data, bytes((102, 99, 109, 95)).decode()]",
    "data['items']",
    "(y for y in {t} if y != 'skip')",
    "[y for y in {t}]",
)
#: The first five, for three generators: enough for an indirect cycle.
_TARGET_ITERABLES_OF_THREE = _TARGET_ITERABLES[:5]
#: Iterables that yield only values a read gives: over runtime values only,
#: the guard promises to accept the key unless a target reads itself.
_RUNTIME_ITERABLES = frozenset(
    {
        "data",
        "[{t}]",
        "{t}",
        "data['items']",
        "(y for y in {t} if y != 'skip')",
        "[y for y in {t}]",
    }
)
#: A condition filters what is bound; it binds nothing.
_TARGET_CONDITIONS = ("", " if {t} != 'x'")


class _Generated(NamedTuple):
    key: str
    generators: str
    promised: bool


def _reads_itself(edges: set[tuple[str, str]]) -> bool:
    """Whether a target's iterable reads the target, directly or through others."""

    graph: dict[str, set[str]] = {}
    for target, read in edges:
        graph.setdefault(target, set()).add(read)
    for start, reads in graph.items():
        seen: set[str] = set()
        todo = list(reads)
        while todo:
            name = todo.pop()
            if name == start:
                return True
            if name not in seen:
                seen.add(name)
                todo.extend(graph.get(name, ()))
    return False


def test_a_read_shared_by_two_targets_is_followed_once() -> None:
    """Targets that read the same names in a ladder are visited once per start.

    Without the visited set the walk doubles with every rung. With it, each of
    the starts walks every name once, so doubling the rungs gives four times the
    work."""

    def ladder(rungs: int) -> set[tuple[str, str]]:
        return {
            (f"{a}{i}", f"{b}{i + 1}") for i in range(rungs) for a in "ab" for b in "ab"
        }

    def work(rungs: int) -> int:
        edges = ladder(rungs)

        def run() -> None:
            assert not _reads_itself(edges)

        return _executed_work(run)

    assert work(16) <= 5 * work(8)
    assert _reads_itself(ladder(4) | {("b4", "a0")})


def _target_comprehensions(
    count: int, forms: tuple[str, ...], conditions: tuple[str, ...]
) -> Iterator[_Generated]:
    """Every comprehension of ``count`` generators over the targets ``p`` and
    ``q`` with the given iterables and conditions, keyed by each bound target."""

    def grow(
        bound: tuple[str, ...],
        parts: tuple[str, ...],
        used: frozenset[str],
        edges: frozenset[tuple[str, str]],
    ) -> Iterator[_Generated]:
        if len(parts) == count:
            promised = used <= _RUNTIME_ITERABLES and not _reads_itself(set(edges))
            for key in sorted(set(bound)):
                yield _Generated(key, " ".join(parts), promised)
            return
        for target in ("p", "q"):
            for form in forms:
                reads = sorted(set(bound)) if "{t}" in form else [""]
                for read in reads if bound or "{t}" not in form else []:
                    now = (*bound, target)
                    for condition in conditions:
                        for test in sorted(set(now)) if condition else [""]:
                            part = f"for {target} in {form.format(t=read)}"
                            yield from grow(
                                now,
                                (*parts, part + condition.format(t=test)),
                                used | {form},
                                edges | ({(target, read)} if read else set()),
                            )

    yield from grow((), (), frozenset(), frozenset())


def _all_target_comprehensions() -> Iterator[_Generated]:
    for count in (1, 2):
        yield from _target_comprehensions(count, _TARGET_ITERABLES, _TARGET_CONDITIONS)
    yield from _target_comprehensions(3, _TARGET_ITERABLES_OF_THREE, ("",))


@pytest.mark.hypothesis
def test_python_is_the_oracle_for_comprehension_targets() -> None:
    """R13-F1, R14-F6: every comprehension of the space above, not a sample.
    Where Python writes the member for an opaque runtime ``data`` in any
    variant (R18-F3, R19-F2), the guard fails; where every iterable is a runtime form
    and no target reads itself, the guard accepts. Python keeps the last binding of a target, the guard
    asks every iterable, so it may fail more often than Python writes."""

    write = "            updated_data[K2] = copy.deepcopy(registration)\n"
    tested = promised_count = 0
    written_counts = [0] * len(_VARIANTS)
    for case in _all_target_comprehensions():
        tested += 1
        comprehension = f"{{{case.key} + 'credentials': 1 {case.generators}}}"
        written_by = _python_writes(comprehension, {})
        written = any(written_by)
        if not (written or case.promised):
            continue
        line = (
            f"            other.update({{{case.key} + 'credentials': creds"
            f" {case.generators}}})\n"
        )
        source = _changed((write, write + "            other = {}\n" + line), "oracle")
        _writes, unclassified, shared = _snippet_problems(source)
        if written:
            written_counts = [
                n + w for n, w in zip(written_counts, written_by, strict=True)
            ]
            assert unclassified or shared, comprehension
        if case.promised:
            promised_count += 1
            assert not (unclassified or shared), comprehension
    assert (tested, *written_counts, promised_count) == (
        6614,
        3194,
        3194,
        1892,
        1892,
        398,
    )


#: R16-F6, R17-F5: iterables the element rule decides, each wrapped twice in
#: the shells below; a shell that keeps reads yields only what its operand yields.
_SHELL_ATOMS = ("data", "['fcm_']", "['x']", "[data, 'fcm_']")
_SHELLS: dict[str, bool] = {
    "{}": True,
    "sorted({})": True,
    "sorted({}, key=id, reverse=True)": True,
    "list({})": True,
    "tuple({})": True,
    "reversed(list({}))": True,
    "({} or [])": True,
    "([] or {})": True,
    "(data and {})": True,
    "[*{}]": True,
    "{{k: 1 for k in {}}}": True,
    "{{**{{k: 1 for k in {}}}}}": True,
    "(x for x in {} if x)": True,
    "(x or x for x in {})": True,
    "(data if data else {})": True,
    "({} or ['fcm_'])": False,
    "({} if data else ['fcm_'])": False,
    "({} and ['fcm_'])": False,
    "[*{}, 'fcm_']": False,
    "(list({}) + ['fcm_'])": False,
    "(['fcm_'] + list({}))": False,
    "sorted(*[{}])": False,
    "sorted({}, *[])": False,
}

#: R17-F5: an element taken from an atom, and the displays that hold it; a
#: display of keys is a dict display with the element as its key.
_SHELL_ELEMENTS: dict[str, bool] = {
    "{}[0]": True,
    "({}[0] or data)": True,
    "(data and {}[0])": True,
    "({}[0] if data else data)": True,
    "(data if data else {}[0])": True,
    "({}[0] or 'fcm_')": False,
}
_ELEMENT_DISPLAYS = ("[{}]", "({},)", "{{{}: 1}}")


def _shell_verdict(iterable: str, promised: bool, shadow: str = "") -> tuple[int, ...]:
    """Whether Python writes the member for a comprehension over the iterable,
    for each variant of ``data`` (R18-F3, R19-F2), and whether the guard is
    held to accept it; asserts the guard agrees. The shadow is a reordering
    builtin the snippet binds and Python calls as a function that returns the
    first part of the member."""

    comprehension = f"{{p + 'credentials': 1 for p in {iterable}}}"
    scope: dict[str, object] = {}
    if shadow:
        scope[shadow] = lambda *_args, **_keywords: ["fcm_"]
    written_by = _python_writes(comprehension, scope)
    written = any(written_by)
    if not (written or promised):
        return (0,) * (len(_VARIANTS) + 1)
    write = "            updated_data[K2] = copy.deepcopy(registration)\n"
    binding = f"            {shadow} = id\n" if shadow else ""
    line = f"            other.update({comprehension.replace(': 1', ': creds')})\n"
    source = _changed(
        (write, write + binding + "            other = {}\n" + line), "shell"
    )
    _writes, unclassified, shared = _snippet_problems(source)
    if written:
        assert unclassified or shared, (shadow, comprehension)
    if promised:
        assert not (unclassified or shared), (shadow, comprehension)
    return (*map(int, written_by), int(promised))


#: R17-F5, R18-F3, R19-F2: per outer shell, the programs Python writes per
#: variant in ``_VARIANTS``, and the programs the guard accepts.
_SHELL_COUNTS: dict[str, tuple[int, int, int, int, int]] = {
    "(['fcm_'] + list({}))": (92, 92, 92, 92, 0),
    "([] or {})": (52, 54, 52, 54, 15),
    "(data and {})": (52, 0, 52, 0, 15),
    "(data if data else {})": (0, 54, 0, 54, 15),
    "(list({}) + ['fcm_'])": (92, 92, 92, 92, 0),
    "(x for x in {} if x)": (52, 54, 52, 54, 15),
    "(x or x for x in {})": (52, 54, 52, 54, 15),
    "({} and ['fcm_'])": (92, 83, 83, 74, 0),
    "({} if data else ['fcm_'])": (52, 92, 52, 92, 0),
    "({} or ['fcm_'])": (52, 63, 61, 72, 0),
    "({} or [])": (52, 54, 52, 54, 15),
    "[*{}, 'fcm_']": (92, 92, 92, 92, 0),
    "[*{}]": (52, 54, 52, 54, 15),
    "list({})": (52, 54, 52, 54, 15),
    "reversed(list({}))": (52, 54, 52, 54, 15),
    "sorted(*[{}])": (52, 54, 52, 54, 0),
    "sorted({})": (52, 54, 52, 54, 15),
    "sorted({}, *[])": (52, 54, 52, 54, 0),
    "sorted({}, key=id, reverse=True)": (52, 54, 52, 54, 15),
    "tuple({})": (52, 54, 52, 54, 15),
    "{{**{{k: 1 for k in {}}}}}": (52, 54, 52, 54, 15),
    "{{k: 1 for k in {}}}": (52, 54, 52, 54, 15),
    "{}": (52, 54, 52, 54, 15),
}


@pytest.mark.parametrize("outer", sorted(_SHELLS))
def test_python_is_the_oracle_for_wrapped_iterables(outer: str) -> None:
    """R16-F6: every atom in every pair of shells, the outer one fixed per
    case. Where Python writes the member for an opaque runtime ``data`` in
    any variant, the guard fails; where the atom is ``data`` and both shells keep reads, the
    guard accepts."""

    counts = [
        _shell_verdict(
            outer.format(inner.format(atom)),
            atom == "data" and keeps and _SHELLS[outer],
        )
        for atom in _SHELL_ATOMS
        for inner, keeps in _SHELLS.items()
    ]
    assert tuple(map(sum, zip(*counts, strict=True))) == _SHELL_COUNTS[outer]


#: R17-F5, R18-F3, R19-F2: per outer shell, written programs per variant and
#: accepted programs over elements.
_ELEMENT_COUNTS: dict[str, tuple[int, int, int, int, int]] = {
    "(['fcm_'] + list({}))": (72, 72, 57, 60, 0),
    "([] or {})": (15, 18, 15, 15, 15),
    "(data and {})": (15, 0, 15, 0, 15),
    "(data if data else {})": (0, 18, 0, 15, 15),
    "(list({}) + ['fcm_'])": (72, 72, 57, 60, 0),
    "(x for x in {} if x)": (15, 18, 15, 15, 15),
    "(x or x for x in {})": (15, 18, 15, 15, 15),
    "({} and ['fcm_'])": (72, 72, 57, 60, 0),
    "({} if data else ['fcm_'])": (15, 72, 15, 72, 0),
    "({} or ['fcm_'])": (15, 18, 15, 15, 0),
    "({} or [])": (15, 18, 15, 15, 15),
    "[*{}, 'fcm_']": (72, 72, 57, 60, 0),
    "[*{}]": (15, 18, 15, 15, 15),
    "list({})": (15, 18, 15, 15, 15),
    "reversed(list({}))": (15, 18, 15, 15, 15),
    "sorted(*[{}])": (15, 18, 15, 15, 0),
    "sorted({})": (15, 18, 15, 15, 15),
    "sorted({}, *[])": (15, 18, 15, 15, 0),
    "sorted({}, key=id, reverse=True)": (15, 18, 15, 15, 15),
    "tuple({})": (15, 18, 15, 15, 15),
    "{{**{{k: 1 for k in {}}}}}": (15, 18, 15, 15, 15),
    "{{k: 1 for k in {}}}": (15, 18, 15, 15, 15),
    "{}": (15, 18, 15, 15, 15),
}


@pytest.mark.parametrize("outer", sorted(_SHELLS))
def test_python_is_the_oracle_for_wrapped_elements(outer: str) -> None:
    """R17-F5: ``and``/``or`` and a conditional as an element, in a list, a
    tuple and as a dict display key, inside every shell. The guard accepts
    where the atom is ``data`` and element and shell keep reads."""

    counts = [
        _shell_verdict(
            outer.format(display.format(element.format(atom))),
            atom == "data" and keeps and _SHELLS[outer],
        )
        for atom in _SHELL_ATOMS
        for element, keeps in _SHELL_ELEMENTS.items()
        for display in _ELEMENT_DISPLAYS
    ]
    assert tuple(map(sum, zip(*counts, strict=True))) == _ELEMENT_COUNTS[outer]


#: R17-F5, R18-F3, R19-F2: written programs per variant and accepted programs
#: per shadowed builtin.
_SHADOW_COUNTS: dict[str, tuple[int, int, int, int, int]] = {
    "list": (12, 14, 12, 14, 25),
    "reversed": (10, 12, 10, 12, 27),
    "sorted": (16, 18, 16, 18, 25),
    "tuple": (10, 12, 10, 12, 27),
}


@pytest.mark.parametrize("shadow", sorted(_REORDERING))
def test_python_is_the_oracle_for_shadowed_builtins(shadow: str) -> None:
    """R17-F5: a reordering builtin the snippet binds locally is no builtin;
    every shell around ``data`` on either side of ``{}`` is held to Python,
    which calls the local. The guard accepts only shells without the name."""

    pairs = {(outer, "{}") for outer in _SHELLS} | {("{}", inner) for inner in _SHELLS}
    counts = [
        _shell_verdict(
            outer.format(inner.format("data")),
            _SHELLS[outer] and _SHELLS[inner] and shadow not in outer + inner,
            shadow,
        )
        for outer, inner in sorted(pairs)
    ]
    assert tuple(map(sum, zip(*counts, strict=True))) == _SHADOW_COUNTS[shadow]


#: R17-F5, R18-F1, round 19: per binding, the class statements Python makes
#: instances of (the call writes); the others stay classes (the call raises).
_CLASS_BASE_COUNTS: dict[str, int] = {
    "annotated": 3,
    "assign": 3,
    "decorated": 3,
    "for": 3,
    "none": 0,
    "object": 1,
    "walrus": 3,
}


#: R17-F5: how a module binds ``A`` after ``class A: pass``, or ``object``
#: through a class statement (R18-F1).
_BASE_BINDINGS: dict[str, str] = {
    "none": "",
    "assign": "A = {r}\n",
    "annotated": "A: object = {r}\n",
    "for": "for A in [{r}]:\n    pass\n",
    "walrus": "(A := {r})\n",
    "decorated": "@lambda c: {r}\nclass A:\n    pass\n",
    "object": "@lambda c: {r}\nclass object:\n    pass\n",
}
#: Each class statement.
_STORE_DEFINITIONS = (
    "class Store(A)",
    "class Store(A, dict)",
    "class Store(dict, A)",
    "class Store(dict)",
    "class Store(object)",
)


@pytest.mark.parametrize("binding", sorted(_BASE_BINDINGS))
def test_python_is_the_oracle_for_class_bases(binding: str) -> None:
    """R17-F5, round 19: Python runs the module and the write. Where a base
    has a metaclass that returns a dict (``A = type.__new__(M, 'X', (), {})``),
    ``Store`` is an instance and the call writes or pops the member; otherwise
    ``Store`` is a class and the call raises. The guard fails both
    (``Store.__setitem__(C, creds)``), so it never has to tell them apart."""

    meta = "class M(type):\n    def __new__(mcs, *a):\n        return {}\n"
    rebind = _BASE_BINDINGS[binding].format(r="type.__new__(M, 'X', (), {})")
    instances = 0
    for store in _STORE_DEFINITIONS:
        module = f"{meta}class A:\n    pass\n{rebind}{store}:\n    pass\n"
        scope: dict[str, object] = {}
        exec(module, scope)  # noqa: S102 - generated test code only
        instance = isinstance(scope["Store"], dict)
        for write, member_after in (
            ("Store.pop(C)", None),
            ("Store.__setitem__(C, creds)", 1),
        ):
            run = {**scope, "C": _MEMBER, "creds": 1}
            if instance:
                scope["Store"][_MEMBER] = 0  # type: ignore[index]
            try:
                exec(write, run)  # noqa: S102 - generated test code only
            except (TypeError, AttributeError):
                assert not instance, (store, write)
            else:
                assert instance, (store, write)
                # R20-F4: the value the call leaves, not the presence of the key.
                after = scope["Store"].get(_MEMBER)  # type: ignore[attr-defined]
                assert after == member_after, (store, write)
            source = _changed(
                (
                    "K2 = K\n",
                    f"K2 = K\nC = 'FCM_CREDENTIALS'.lower()\n{module}",
                    "            updated_data[K2] = copy.deepcopy(registration)\n",
                    f"            updated_data[K2] = copy.deepcopy(registration)\n            {write}\n",
                ),
                "class oracle",
            )
            _writes, unclassified, shared = _snippet_problems(source)
            assert unclassified or shared, (store, write)
        instances += instance
    assert instances == _CLASS_BASE_COUNTS[binding]


#: Names the generated module statements bind; ``C`` is the key of the write.
_ORACLE_NAMES = ("A", "B", "C")
#: Parts only: a statement that spells the member is already caught as an
#: unclassified occurrence, so the member must arise at runtime to test anything.
_ORACLE_PARTS = ("fcm_", "credentials", "x")

#: Every way a module-level statement binds a name (Python reference, "Naming
#: and binding"), one template each; ``{n}`` is the name, ``{e}`` a string
#: expression. Each part of the label is a node the template must contain.
_ORACLE_FORMS: dict[str, str] = {
    "Assign": "{n} = {e}",
    "Assign/Tuple": "{n}, _unused = {e}, 0",
    "Assign/Starred": "*{n}, = ({e},)",
    "AnnAssign": "{n}: str = {e}",
    "AugAssign": "{n} += {e}",
    "For": "for {n} in ('x', {e}):\n    pass",
    "While": "while True:\n    {n} = {e}\n    break",
    "If": "if True:\n    {n} = {e}",
    "With": "with contextlib.nullcontext({e}) as {n}:\n    pass",
    "NamedExpr": "({n} := {e})",
    "NamedExpr/comprehension": "[({n} := {e}) for _ in (0,)]",
    "ExceptHandler": "try:\n    raise ValueError({e})\nexcept ValueError as {n}:\n    pass",
    "ExceptHandler/TryStar": (
        "try:\n    raise ExceptionGroup('g', [ValueError({e})])\n"
        "except* ValueError as {n}:\n    pass"
    ),
    "MatchAs": "match {e}:\n    case {n}:\n        pass",
    "MatchAs/MatchClass": "match {e}:\n    case str({n}):\n        pass",
    "MatchAs/MatchOr": "match [{e}]:\n    case [{n}] | ({n}, 0):\n        pass",
    "MatchStar": "match [{e}]:\n    case [*{n}]:\n        pass",
    "MatchMapping": "match {{'k': {e}}}:\n    case {{**{n}}}:\n        pass",
    "Import": "import string as {n}",
    "FunctionDef": "def {n}():\n    return {e}",
    "AsyncFunctionDef": "async def {n}():\n    return {e}",
    "ClassDef": "class {n}:\n    value = {e}",
    "TypeAlias": "type {n} = str",
    "Global": "def _bind():\n    global {n}\n    {n} = {e}\n_bind()",
    "Delete": "del {n}",
}

#: Binding constructs the oracle cannot run, with the reason; each has a pinned
#: ``_BROKEN`` or ``_ACCEPTED`` form instead.
_ORACLE_EXCLUDED: dict[str, str] = {
    "ImportFrom": "needs the package; 'name imported from const as key' pins it",
    "AsyncFor": "not allowed at module level",
    "AsyncWith": "not allowed at module level",
    "Nonlocal": "not allowed at module level",
    "TypeVar": (
        "a type parameter binds in its own annotation scope, never at module level;"
        " 'copy bound as a function type parameter' pins it"
    ),
}

#: The binding constructs of the grammar, from the language reference.
_BINDING_CONSTRUCTS = frozenset(
    {
        "Assign",
        "AnnAssign",
        "AugAssign",
        "For",
        "AsyncFor",
        "With",
        "AsyncWith",
        "NamedExpr",
        "ExceptHandler",
        "TryStar",
        "MatchAs",
        "MatchStar",
        "MatchMapping",
        "Import",
        "ImportFrom",
        "FunctionDef",
        "AsyncFunctionDef",
        "ClassDef",
        "TypeAlias",
        "TypeVar",
        "Global",
        "Nonlocal",
        "Delete",
    }
)


def _oracle_expression() -> st.SearchStrategy[str]:
    atom = st.one_of(
        st.sampled_from(_ORACLE_PARTS).map(repr), st.sampled_from(_ORACLE_NAMES)
    )
    return st.one_of(
        atom,
        st.tuples(atom, atom).map(lambda pair: f"{pair[0]} + {pair[1]}"),
        st.tuples(atom, atom).map(lambda pair: f"f'{{{pair[0]}}}{{{pair[1]}}}'"),
    )


def _oracle_statement(form: str, name: str, expression: str) -> str:
    return _ORACLE_FORMS[form].format(n=name, e=expression)


def _identity(part: str) -> object:
    return part


#: Operand templates of the ``expression`` family: ``{x}`` holds the stored value,
#: and the template turns it back into the part (``conversion r`` never does,
#: as a control).
_EXPRESSION_TEMPLATES: dict[str, tuple[str, Callable[[str], object]]] = {
    "plain": ("{x}", _identity),
    "subscript": ("{x}[0]", lambda part: (part, "x")),
    "slice": ("{x}[1:]", lambda part: "x" + part),
    "lower": ("{x}.lower()", str.upper),
    "strip": ("{x}.strip()", lambda part: f" {part} "),
    "percent": ("'%s' % ({x},)", _identity),
    "conversion s": ("f'{{{x}!s}}'", _identity),
    "conversion r": ("f'{{{x}!r}}'", _identity),
    "format s": ("f'{{{x}:s}}'", _identity),
    "format width": ("f'{{{x}:>4}}'", _identity),
}


@st.composite
def _expression_case(draw: st.DrawFn) -> list[str]:
    """Two operands built from expressions over a module name, a class
    attribute or a literal; the first is held by a name by construction."""

    statements: list[str] = []
    operands: list[str] = []
    for slot, wanted in enumerate(("fcm_", "credentials")):
        part = draw(st.sampled_from((wanted, wanted, wanted, "x")))
        template, encode = _EXPRESSION_TEMPLATES[
            draw(st.sampled_from(sorted(_EXPRESSION_TEMPLATES)))
        ]
        value = repr(encode(part))
        holder = draw(
            st.sampled_from(("name", "class") if slot == 0 else ("name", "literal"))
        )
        if holder == "name":
            name = "AB"[slot]
            statements.append(f"{name} = {value}")
            held = name
        elif holder == "class":
            statements.append(f"class NS:\n    v = {value}")
            held = "NS.v"
        else:
            held = value
        operands.append(template.format(x=held))
    return [*statements, f"C = {operands[0]} + {operands[1]}"]


#: Templates of the ``global`` family: a module name bound through ``global``
#: from a function, a class body or a nested function, with a value the module
#: does not see (a parameter default or a class-body name).
_GLOBAL_TEMPLATES: dict[str, str] = {
    "function": "def _set(a={p}):\n    global {n}\n    {n} = a\n_set()",
    "class body": "class _K:\n    part = {p}\n    global {n}\n    {n} = part",
    "nested function": (
        "def _outer():\n    def _inner(a={p}):\n        global {n}\n        {n} = a\n"
        "    _inner()\n_outer()"
    ),
}


@st.composite
def _global_case(draw: st.DrawFn) -> list[str]:
    statements: list[str] = []
    for name, wanted in (("A", "fcm_"), ("B", "credentials")):
        part = draw(st.sampled_from((wanted, wanted, wanted, "x")))
        template = draw(st.sampled_from([*sorted(_GLOBAL_TEMPLATES), "plain"]))
        if template == "plain":
            statements.append(f"{name} = {part!r}")
        else:
            statements.append(_GLOBAL_TEMPLATES[template].format(n=name, p=repr(part)))
    statements[1:1] = draw(
        st.lists(st.sampled_from(("A = 'x'", "B = 'x'")), max_size=1)
    )
    return [*statements, "C = A + B"]


#: Comprehensions the ``walrus`` family nests a ``:=`` in, as ``{x}`` the element.
_COMPREHENSIONS = (
    "[{x} for _ in (0,)]",
    "{{id({x}) for _ in (0,)}}",
    "{{0: {x} for _ in (0,)}}",
    "list({x} for _ in (0,))",
)


@st.composite
def _walrus_case(draw: st.DrawFn) -> list[str]:
    """``A := part`` one to three comprehensions deep; a lambda around it binds
    in the lambda (PEP 572) and is the control that never reaches the module."""

    part = draw(st.sampled_from(("fcm_", "fcm_", "fcm_", "x")))
    element = f"(A := {part!r})"
    if draw(st.booleans()) and draw(st.booleans()):
        element = f"(lambda: {element})()"
    for _level in range(draw(st.integers(min_value=1, max_value=3))):
        element = draw(st.sampled_from(_COMPREHENSIONS)).format(x=element)
    second = draw(st.sampled_from(("'credentials'", "B")))
    statements = [f"_ = {element}"]
    if second == "B":
        statements.insert(0, "B = 'credentials'")
    return [*statements, f"C = A + {second}"]


@st.composite
def _binding_case(draw: st.DrawFn) -> list[str]:
    """Some statements over all names, then ``A`` bound by any form and
    ``C = A + 'credentials'``."""

    noise = draw(
        st.lists(
            st.tuples(
                st.sampled_from(sorted(_ORACLE_FORMS)),
                st.sampled_from(_ORACLE_NAMES),
                _oracle_expression(),
            ),
            max_size=3,
        )
    )
    form = draw(st.sampled_from(sorted(_ORACLE_FORMS)))
    part = draw(st.sampled_from(("fcm_", "fcm_", "fcm_", "x")))
    return [
        *(_oracle_statement(*item) for item in noise),
        _oracle_statement(form, "A", repr(part)),
        "C = A + 'credentials'",
    ]


#: Rebindings drawn after every family's statements: the guard is flow-insensitive,
#: so a name rebound after the key was built must not hide the member.
_ORACLE_REBINDINGS = ("A = 'x'", "B = 'x'", "C = 'x'")

#: The oracle families (R7-F9, R7-F10): one test each, so that each reports its
#: own counterexample and its own statistics.
_ORACLE_FAMILIES: dict[str, st.SearchStrategy[list[str]]] = {
    "expression": _expression_case(),
    "global": _global_case(),
    "walrus": _walrus_case(),
    "binding": _binding_case(),
}


def _oracle_runs_to_the_member(statements: list[str]) -> bool:
    """Run ``statements`` one by one; True if ``C`` ever holds the member.

    A statement that raises is skipped; the guard folds every binding in any
    order, so it covers every subset as well.
    """

    namespace: dict[str, object] = {"contextlib": contextlib}
    exec("A = B = C = ''", namespace)  # noqa: S102 - generated test code only
    for statement in statements:
        try:
            exec(statement, namespace)  # noqa: S102 - generated test code only
        except Exception:  # noqa: BLE001 - a failing statement binds nothing
            continue
        if namespace.get("C") == _MEMBER:
            return True
    return False


def _oracle_source(statements: list[str]) -> str:
    """``_CLEAN`` with ``statements`` at module level and a write keyed by ``C``."""

    module = "\n".join(["A = B = C = ''", *statements])
    return _changed(
        (
            "K2 = K\n",
            f"K2 = K\nimport contextlib\n{module}\n",
            "            updated_data[K2] = copy.deepcopy(registration)\n",
            "            updated_data[C] = creds\n",
        ),
        "oracle",
    )


def test_the_oracle_covers_every_binding_construct() -> None:
    """Every binding construct is generated or excluded with a reason, and each
    template contains every node its label names."""

    generated = {part for label in _ORACLE_FORMS for part in label.split("/")}
    assert generated | set(_ORACLE_EXCLUDED) >= _BINDING_CONSTRUCTS
    for label, template in _ORACLE_FORMS.items():
        tree = ast.parse(template.format(n="C", e="'x'"))
        names = {type(node).__name__ for node in ast.walk(tree)}
        assert set(label.split("/")) <= names, label


@pytest.mark.parametrize("form", sorted(_ORACLE_FORMS))
def test_each_binding_form_of_a_key_part_is_seen(form: str) -> None:
    """Every form, once, binding the part ``A`` that ``C = A + 'credentials'``
    turns into the member: the guard must not pass the write keyed by ``C``."""

    statements = [_oracle_statement(form, "A", repr("fcm_")), "C = A + 'credentials'"]
    if not _oracle_runs_to_the_member(statements):
        pytest.skip(f"{form} binds no string that outlives the statement")
    _writes, unclassified, shared = _snippet_problems(_oracle_source(statements))
    assert unclassified or shared, form


@pytest.mark.hypothesis
@pytest.mark.parametrize("family", sorted(_ORACLE_FAMILIES))
@settings(max_examples=500, deadline=None, derandomize=True)
@given(data=st.data())
def test_python_is_the_oracle_for_module_keys(family: str, data: st.DataObject) -> None:
    """Whenever running the module binds the key ``C`` to the member, the guard fails.

    Python itself decides the value, so a form nobody listed is found as well.
    """

    statements = data.draw(_ORACLE_FAMILIES[family], label="statements")
    statements += data.draw(
        st.lists(st.sampled_from(_ORACLE_REBINDINGS), max_size=2), label="rebound"
    )
    assume(_MEMBER not in "\n".join(statements))
    reached = _oracle_runs_to_the_member(statements)
    event(f"tests the guard: {reached}")
    if not reached:
        return
    _writes, unclassified, shared = _snippet_problems(_oracle_source(statements))
    assert unclassified or shared, statements


#: Every form the docstrings of this file name, mapped to a pin.
_FORK_DOCUMENTED_FORMS: dict[str, str] = {
    "{p + 'credentials': creds for p in ('a', 'b')}": "comprehension target over constants",
    "{'x': p for p in ('a', 'b')}": "comprehension target over constants read only by the value",
    "{'x': {p + 'credentials': creds} for p in ('a', 'b')}": (
        "comprehension target over constants read by a key inside the value"
    ),
    "d.update(zip(...))": "pairs a call builds from a computed key",
    "dict(zip(...))": "pairs a call builds for dict",
    "{**dict(zip(...))}": "pairs a call builds for dict spread into a display",
    "p = 'FCM_CREDENTIALS'.lower(); updated_data[p] = creds": (
        "local bound by a method of a constant"
    ),
    "p = ''.join(['fcm_', 'credentials']); updated_data[p] = creds": (
        "local bound by a join of constants"
    ),
    "for p in ('FCM_CREDENTIALS'.lower(),): updated_data[p] = creds": (
        "statement loop over a method of a constant"
    ),
    "'fcm_credentials' in updated_data": "membership test of the whole member",
    "'fcm_credentials' not in creds": "negated membership test of the whole member",
    "p = 'fcm_credentials'": "local bound to the whole member",
    "for p in ('fcm_credentials',): updated_data[p] = creds": (
        "statement loop over the whole member"
    ),
    "for p in ('fcm_',): updated_data[p + 'credentials'] = creds": (
        "statement loop over constants binding a local"
    ),
    "for d2[[q for q in data][0]] in data": "module name rebound by a comprehension inside a subscript target",
    "[data, 'fcm_']": "comprehension target over a list of a local and a constant",
    "[data, bytes((102, 99, 109, 95)).decode()]": "comprehension target over a local and a decoded bytes constant",
    "data.get('x', ['fcm_'])": "comprehension target over a method result of a local",
    "data['items']": "comprehension target over a subscript of a parameter",
    "data.items()": "dict comprehension over a parameter",
    "sorted(data)": "comprehension target over sorted of a parameter",
    "data or []": "comprehension target over a parameter or an empty list",
    "{**data}": "comprehension target over a spread parameter",
    "dict(data)": "comprehension target over a call to dict of a local",
    "sorted(data, *[])": "comprehension target over sorted with an empty spread",
    "A = type.__new__(M, 'X', (), {})": "unknown key popped through a class whose base of the file is rebound",
    "data * 2": "comprehension target over a repeated local",
    "Store.__setitem__(C, creds)": "unknown key set through a class whose bases are the builtins dict and object",
    "Store.setdefault(C, creds)": "unknown key set through a classmethod of a class of the file",
    "['fcm_'] + data": "comprehension target over a list joined to a parameter",
    "Store.__setitem__(CACHE, 'x', creds)": "undecided mapping at the first position of a class call",
    "CACHE = Store()": "undecided mapping at the first position of a class call",
    "type(CACHE).__setitem__(CACHE, 'x', creds)": "undecided mapping at the first position of the type of a mapping",
    "dict().setdefault('x', CACHE)": "undecided mapping at the second position of a call receiver",
    "(x for x in data if x != 'skip')": "comprehension target over a filtered generator of a parameter",
    "for p in ['fcm_'] for p in data": "comprehension target bound by a first generator over constants",
    "+": "key concatenated from constants",
    "from ..const import X": "test_the_folding_guard_rejects_a_shared_or_unknown_form",
    "A = 'fcm_'; K = A + 'credentials'; A = 'x'": "key built from a name rebound later",
    "+=": "key built by augmented assignment",
    "for": "key constant bound in a module-level for block",
    "with": "With",
    "match": "MatchAs",
    ":=": "key part bound by walrus two comprehensions deep",
    "global": "key bound through global in a function",
    "A = A + 'a'": "key grown by a self-referencing binding",
    "B = A * 0": "key through a cycle over a multiplication",
    "_VALUE_LIMIT": "key with more values than the limit",
    "P[0]": "key part taken by subscript",
    "'%s' % A": "key formatted with percent",
    "f'{A!s}'": "key from an f-string with a conversion",
    "chr(X)": "key from a call over a module number",
    "X = 102": "key from a call over a module number",
    "NS.A": "key part from a class attribute",
    "'FCM_'.lower()": "key part from a method of a constant",
    "setdefault": "setdefault with the member",
    "pop": "test_the_folding_guard_rejects_a_shared_or_unknown_form",
    "del": "test_the_folding_guard_rejects_a_shared_or_unknown_form",
    "except": "ExceptHandler",
    "import": "Import",
    "A := part": "walrus",
    "C": "test_python_is_the_oracle_for_module_keys",
    "_ = [0 for K3 in ()]": "comprehension target does not hide a module key",
    "str(K3)": "unknown module key inside a call",
    "C = A + 'credentials'": "binding",
    "A = B; B = A": "alias cycle with a value other than the member",
    "list[Any]": "unknown module name only read by index",
    "x[i]": "unknown module name only read by index",
    "class Q: P = 'fcm_'": "key built from a name of a class body",
    "for p in ['']": "key built from a comprehension target over constants",
    "[{p + A + 'credentials': v} for p in ['']]": (
        "key built from a comprehension target over constants"
    ),
    "bundle[C]['gcm'] = v": "unknown key of a container written into",
    'bundle[C]["gcm"] = v': "unknown key of a container written into",
    "bundle[C]": "unknown key of a container written into",
    "operator.setitem": "unknown key written by operator.setitem",
    "dict": "unknown key in pairs handed to dict",
    "*(C, v)": "unknown key in starred setdefault arguments",
    "[(C, v)]": "unknown key in pairs handed to update",
    "class Q[fcm_]": "key built from a type parameter",
    "from operator import *": "unknown key written through a star import of operator",
    "[(C, v)] * 1": "unknown key in pairs behind an operator",
    "import operator as op": "unknown key written through an import alias of operator",
    "from operator import setitem": "unknown key written through setitem imported from operator",
    "for x in A for x in x": "comprehension target rebound from itself",
    "type(d).__setitem__(d, C, v)": "unknown key written through the type of the mapping",
    "s = d.__setitem__": "write method bound to a name",
    "getattr(d, '__setitem__')": "write method looked up by getattr",
    "__class__": "key built from __class__ in a method",
    "for p in data for p in ['fcm_']": (
        "comprehension target bound by a second generator over constants"
    ),
    "for x in data for y in x for x in y": (
        "comprehension target rebound through another target"
    ),
    "for d2[P] in data": "module name read inside a subscript comprehension target",
    "from operator import __setitem__": "write function imported as __setitem__",
    "op = operator": "write function through a local bound to operator",
    "operator.attrgetter": "write function through attrgetter",
    "getattr(d, M)": "write function looked up by getattr with a name",
    "__getattribute__": "write function through __getattribute__",
    "d.__class__": "write function of the class reached through __class__",
    "D = dict": "write function of dict bound to a local",
    "T = type(d)": "write function of a type bound to a local",
    "d |= [(C, v)]": "pairs merged by |=",
    "__ior__": "pairs merged by __ior__",
    "dict.__init__": "pairs handed to dict.__init__",
    "__P": "private name mangled in a class body",
    "class Q": "private name mangled in a class body",
    "_Q__P": "private name mangled in a class body",
    "t + 'credentials'": "test_python_is_the_oracle_for_comprehension_targets",
    "data": "test_python_is_the_oracle_for_comprehension_targets",
}


@pytest.mark.parametrize("label", sorted(_CHAINS))
def test_a_chain_of_any_length_is_followed_with_folded_keys(label: str) -> None:
    """R10-F1: the folding resolver builds ``_LiteralKeys`` too, so the chains
    the upstream guard follows without recursion are followed here as well."""

    changes, shared = _CHAINS[label]
    source = _changed(changes, label)
    with _shallow_stack(120):
        writes, unclassified, problems = _snippet_problems(source)
    assert (writes, unclassified, bool(problems)) == (2, [], shared), label
    # Each container of the chain is named once, at its own line.
    lines = source.splitlines()
    first = lines.index("        d0 = updated_data") + 1 if shared else 0
    expected = [
        f"line {first + i}: the container d{i} is not a fresh local"
        for i in range(len(problems))
    ]
    assert problems == sorted(expected), label


def test_a_place_both_resolvers_report_is_reported_once() -> None:
    """R10-F6: a key unknown to the folding that ``_LiteralKeys`` also reports
    (the package path) gives one problem, not two."""

    label = "const reached through the package path"
    problems = _snippet_problems(_changed(_BROKEN[label], label))[1]
    assert len(problems) == 1, problems


def test_the_fork_docstring_names_only_pinned_forms() -> None:
    """Each form the module docstring names has a pin."""

    pins = {*_ALL_BROKEN, *_ALL_ACCEPTED, *_ORACLE_FORMS, *_ORACLE_FAMILIES}
    pins |= set(_FORK_NOT_SEEN)
    pins |= {name for name in globals() if name.startswith("test_")}
    problems = _docstring_problems(pathlib.Path(__file__), _FORK_DOCUMENTED_FORMS, pins)
    assert not problems, problems


#: The forms the docstring declares as failing though the code runs, each frozen
#: with a control that writes the member through the same form: a form changed in
#: that paragraph has to run first. Only a form in double backticks is read.
_DECLARED_PRICE_FORMS = {
    "Store.__setitem__(CACHE, 'x', creds)": "Store.__setitem__(CACHE, 'fcm_credentials', creds)",
    "type(CACHE).__setitem__(CACHE, 'x', creds)": (
        "type(CACHE).__setitem__(CACHE, 'fcm_credentials', creds)"
    ),
    "dict().setdefault('x', CACHE)": "dict().setdefault('fcm_credentials', CACHE)",
    "{p + 'credentials': creds for p in ('a', 'b')}": (
        "{p + 'credentials': creds for p in ('fcm_', 'b')}"
    ),
    "{'x': {p + 'credentials': creds} for p in ('a', 'b')}": (
        "{'x': {p + 'credentials': creds} for p in ('b', 'fcm_')}"
    ),
    "s = d.__setitem__": "s = d.__setitem__; s('fcm_credentials', creds)",
    "getattr(d, '__setitem__')": "getattr(d, '__setitem__')('fcm_credentials', creds)",
}

#: Literals of the same paragraph that are no price form: the setup of a form and
#: the target that stays silent.
_PRICE_PARAGRAPH_OTHERS = {"CACHE = Store()", "{'x': p for p in ('a', 'b')}"}


def _double_backtick_literals(text: str) -> list[str]:
    """Each literal of ``text``, also one a line break cuts, with its space
    normalized as the sentence reader of the upstream guard does."""

    return re.findall(r"``(.+?)``", " ".join(text.split()))


def _keys_in(value: object) -> set[object]:
    """The keys of every dict reachable from ``value`` through dicts, lists and
    tuples; the frozen forms build no cycle."""

    if isinstance(value, dict):
        return set(value) | _keys_in(list(value.values()))
    if isinstance(value, list | tuple):
        return set().union(*map(_keys_in, value))
    return set()


def _keys_held_after(form: str) -> set[object]:
    """Run ``form`` and return the keys of each instance of ``Observed`` it
    created, copied or wrote and of each dict its result or a bound name reaches
    through dicts, lists and tuples. A mapping built by a display and dropped, or
    held by any other object, is not observed, see
    ``test_a_mapping_a_form_drops_or_hides_is_not_observed``; a form spelling the
    member is excluded by ``_holds_no_member`` instead."""

    created: list[dict[str, object]] = []

    class Observed(dict[str, object]):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, **kwargs)
            created.append(self)

        def copy(self) -> Observed:
            return Observed(self)

    class Store(Observed):
        pass

    scope: dict[str, object] = {
        "Store": Store,
        "dict": Observed,
        "CACHE": Store(),
        "d": Observed(),
        "creds": {"private": "x"},
    }
    is_expression = isinstance(ast.parse(form).body[0], ast.Expr)
    exec(f"result = {form}" if is_expression else form, scope)  # noqa: S102 - frozen forms only
    scope.pop("__builtins__")
    return _keys_in(created) | _keys_in(list(scope.values()))


def _constants(form: str) -> set[object]:
    """The constants ``form`` spells, adjacent string literals joined."""

    return {
        node.value
        for node in ast.walk(ast.parse(form))
        if isinstance(node, ast.Constant)
    }


def _holds_no_member(form: str) -> bool:
    """``form`` neither spells the member nor holds it under the observation."""

    return _MEMBER not in _constants(form) and _MEMBER not in _keys_held_after(form)


#: Forms that hold the member: the observation has to see each of them.
_HOLDING_FORMS = (
    "dict().copy().setdefault('fcm_credentials', CACHE)",
    "s = {'fcm_credentials': creds}",
    "dict(fcm_credentials=CACHE)",
    "dict.fromkeys(['fcm_credentials'])",
    "[{'fcm_credentials': 1}]",
    "{'x': {p + 'credentials': 1 for p in ('fcm_',)}}",
)


@pytest.mark.parametrize("form", _HOLDING_FORMS)
def test_the_observation_sees_a_form_that_holds_the_member(form: str) -> None:
    """Each frozen form that holds the member is observed."""

    assert _MEMBER in _keys_held_after(form)


@pytest.mark.parametrize(
    "form",
    (
        "[{p + 'credentials': 1 for p in ('fcm_',)}] and None",
        "s = iter([{p + 'credentials': 1 for p in ('fcm_',)}])",
        "s = type('T', (), {'a': {p + 'credentials': 1 for p in ('fcm_',)}})",
    ),
)
def test_a_mapping_a_form_drops_or_hides_is_not_observed(form: str) -> None:
    """The limit of ``_keys_held_after``: such a mapping is not observed."""

    assert _MEMBER not in _keys_held_after(form)


def test_a_form_spelling_the_member_and_dropping_it_is_excluded() -> None:
    """Negative control of the price test: ``_keys_held_after`` does not see this
    form, and ``_holds_no_member`` still refuses it for its literal."""

    form = "[{'fcm_' 'credentials': 1}] and None"
    assert _MEMBER not in _keys_held_after(form)
    assert not _holds_no_member(form)


def test_the_literal_reader_sees_a_literal_a_line_break_cuts() -> None:
    """A literal a line break cuts is read, and every marker belongs to one."""

    assert _double_backtick_literals("a ``1 +\n'x'`` b ``c``") == ["1 + 'x'", "c"]
    docstring = ast.get_docstring(ast.parse(pathlib.Path(__file__).read_text("utf-8")))
    assert docstring
    assert docstring.count("``") == 2 * len(_double_backtick_literals(docstring))


def test_every_declared_price_form_runs_without_the_member() -> None:
    """Python runs each form the docstring declares as a false alarm, the form
    holds no member while its control holds it under the same observation, and
    the case pinned to it contains the form."""

    docstring = ast.get_docstring(ast.parse(pathlib.Path(__file__).read_text("utf-8")))
    assert docstring
    text = " ".join(docstring.split())
    paragraph = text[text.index("The price:") : text.index("A string longer than")]
    assert set(_double_backtick_literals(paragraph)) == {
        *_DECLARED_PRICE_FORMS,
        *_PRICE_PARAGRAPH_OTHERS,
    }
    for form, control in _DECLARED_PRICE_FORMS.items():
        assert _holds_no_member(form), form
        assert "fcm_credentials" in _keys_held_after(control), control
        pin = _FORK_DOCUMENTED_FORMS[form]
        # The pins of the two method forms write through a local named ``other``.
        pinned = re.sub(r"\bd\b", "other", form)
        assert pinned in _changed(_ALL_BROKEN[pin], pin), form


def test_an_inherited_instance_method_raises_on_a_key_at_the_mapping_position() -> None:
    """The docstring: ``Store.__setitem__(C, creds)`` takes the mapping first."""

    class Store(dict[str, object]):
        pass

    with pytest.raises(TypeError):
        Store.__setitem__("fcm_credentials", {"private": "x"})  # type: ignore[call-arg,arg-type]


@pytest.mark.parametrize(
    "form",
    (
        "for p in ('fcm_',): updated_data[p + 'credentials'] = creds",
        "p = 'FCM_CREDENTIALS'.lower(); updated_data[p] = creds",
        "p = ''.join(['fcm_', 'credentials']); updated_data[p] = creds",
        "for p in ('FCM_CREDENTIALS'.lower(),): updated_data[p] = creds",
    ),
)
def test_the_statement_loop_limit_writes_the_member_in_python(form: str) -> None:
    """Each declared limit of a local is a real write: Python holds the member,
    the guard stays silent (``_FORK_NOT_SEEN``)."""

    scope: dict[str, object] = {"updated_data": {}, "creds": {"private": "x"}}
    exec(form, scope)  # noqa: S102 - frozen forms only
    assert "fcm_credentials" in scope["updated_data"]  # type: ignore[operator]
    pin = _FORK_DOCUMENTED_FORMS[form]
    assert form in _changed(_FORK_NOT_SEEN[pin], pin)


#: A key over each builtin whose result may yield a string, so that a target over
#: a builtin call stays unknown: Python writes the member with every one.
#: ``__import__``, ``breakpoint``, ``help``, ``input`` and ``open`` run against
#: the stubs of ``_stub_builtin_hooks``, the rest as they are.
_BUILTIN_ITERABLE_FORMS = {
    "__build_class__": (
        "{p + 'credentials': creds for p in "
        "__build_class__(lambda: None, 'T', metaclass=lambda *a: ('fcm_',))}"
    ),
    "__import__": "{p + 'credentials': creds for p in __import__('stub')}",
    "abs": (
        "{p + 'credentials': creds for p in "
        "abs(type('T', (), {'__abs__': lambda s: ('fcm_',)})())}"
    ),
    "aiter": (
        "{p + 'credentials': creds for p in "
        "aiter(type('T', (), {'__aiter__': lambda s: s, '__anext__': lambda s: 0, "
        "'__iter__': lambda s: iter(('fcm_',))})())}"
    ),
    "anext": (
        "{p + 'credentials': creds for p in "
        "anext(type('T', (), {'__anext__': lambda s: ('fcm_',)})())}"
    ),
    "ascii": "{p + 'cm_credentials': creds for p in ascii('f')}",
    "bin": "{chr(ord(p) + 53) + 'cm_credentials': creds for p in bin(1)}",
    "breakpoint": "{p + 'credentials': creds for p in breakpoint()}",
    "bytearray": "{chr(p) + 'cm_credentials': creds for p in bytearray((102,))}",
    "bytes": "{chr(p) + 'cm_credentials': creds for p in bytes((102,))}",
    "chr": "{p + 'cm_credentials': creds for p in chr(102)}",
    "dict": "{p + 'credentials': creds for p in dict(fcm_=1)}",
    "dir": "{p + 'credentials': creds for p in dir(type('T', (), {'fcm_': 1}))}",
    "divmod": "{chr(p) + 'cm_credentials': creds for p in divmod(102, 103)}",
    "enumerate": "{p + 'credentials': creds for _, p in enumerate(('fcm_',))}",
    "eval": "{p + 'credentials': creds for p in eval(\"('fcm_',)\")}",
    "filter": "{p + 'credentials': creds for p in filter(None, ('fcm_',))}",
    "format": "{p + 'cm_credentials': creds for p in format('f')}",
    "frozenset": "{p + 'credentials': creds for p in frozenset(('fcm_',))}",
    "getattr": (
        "{p + 'credentials': creds for p in "
        "getattr(type('T', (), {'a': ('fcm_',)}), 'a')}"
    ),
    "globals": "{p + 'credentials': creds for p in globals()}",
    "help": "{p + 'credentials': creds for p in help()}",
    "hex": "{chr(ord(p) + 53) + 'cm_credentials': creds for p in hex(1)}",
    "input": "{p + 'cm_credentials': creds for p in input()}",
    "iter": "{p + 'credentials': creds for p in iter(('fcm_',))}",
    "list": "{p + 'credentials': creds for p in list(('fcm_',))}",
    "locals": "{p + 'credentials': creds for p in locals()}",
    "map": "{p + 'credentials': creds for p in map(str, ('fcm_',))}",
    "max": "{p + 'credentials': creds for p in max((('fcm_',),))}",
    "memoryview": "{chr(p) + 'cm_credentials': creds for p in memoryview(b'f')}",
    "min": "{p + 'credentials': creds for p in min((('fcm_',),))}",
    "next": "{p + 'credentials': creds for p in next(iter((('fcm_',),)))}",
    "oct": "{chr(ord(p) + 53) + 'cm_credentials': creds for p in oct(1)}",
    "open": "{p + 'credentials': creds for p in open('x')}",
    "pow": (
        "{p + 'credentials': creds for p in "
        "pow(type('T', (), {'__pow__': lambda s, o: ('fcm_',)})(), 2)}"
    ),
    "range": "{chr(p) + 'cm_credentials': creds for p in range(102, 103)}",
    "repr": "{p + 'cm_credentials': creds for p in repr('f')}",
    "reversed": "{p + 'credentials': creds for p in reversed(('fcm_',))}",
    "round": (
        "{p + 'credentials': creds for p in "
        "round(type('T', (), {'__round__': lambda s: ('fcm_',)})())}"
    ),
    "set": "{p + 'credentials': creds for p in set(('fcm_',))}",
    "sorted": "{p + 'credentials': creds for p in sorted(('fcm_',))}",
    "str": "{p + 'cm_credentials': creds for p in str('f')}",
    "sum": "{p + 'credentials': creds for p in sum((('fcm_',),), ())}",
    "tuple": "{p + 'credentials': creds for p in tuple(('fcm_',))}",
    "type": (
        "{p + 'credentials': creds for p in "
        "type(type('M', (type,), {'__iter__': lambda c: iter(('fcm_',))})('C', (), {})())}"
    ),
    "vars": "{p + 'credentials': creds for p in vars()}",
    "zip": "{p + 'credentials': creds for p, in zip(('fcm_',))}",
}

#: Callable builtins named without a form: each returned nothing iterable under
#: the battery of ``_battery_arguments``, or does not return (``exit``, ``quit``).
#: That is a measurement of the battery, not of all arguments.
_BUILTINS_WITHOUT_A_FORM = frozenset(
    {
        "__loader__",
        "all",
        "any",
        "bool",
        "callable",
        "classmethod",
        "compile",
        "complex",
        "copyright",
        "credits",
        "delattr",
        "exec",
        "exit",
        "float",
        "hasattr",
        "hash",
        "id",
        "int",
        "isinstance",
        "issubclass",
        "len",
        "license",
        "object",
        "ord",
        "print",
        "property",
        "quit",
        "setattr",
        "slice",
        "staticmethod",
        "super",
    }
)


def _callable_builtins() -> list[str]:
    """Each callable name of the builtins module, read from the running Python."""

    return [name for name in dir(builtins) if callable(getattr(builtins, name))]


def _is_exception_class(name: str) -> bool:
    """An exception class: calling one builds an exception, which yields nothing."""

    value = getattr(builtins, name)
    return isinstance(value, type) and issubclass(value, BaseException)


def _stub_builtin_hooks(stub: pytest.MonkeyPatch) -> None:
    """Replace what the hook-driven builtins reach, so that each returns a value
    of the test and none waits on a terminal."""

    stub.setattr(builtins, "input", lambda: "f")
    stub.setattr(builtins, "open", lambda _path: iter(("fcm_",)))
    stub.setattr(sys, "stdin", io.StringIO())
    stub.setattr(sys, "breakpointhook", lambda *_a, **_k: ("fcm_",))
    stub.setitem(sys.modules, "stub", ("fcm_",))
    pydoc = types.SimpleNamespace(help=lambda *_a, **_k: ("fcm_",))
    stub.setitem(sys.modules, "pydoc", pydoc)


#: Protocol methods the battery's object answers with an iterable.
_BATTERY_PROTOCOLS = (
    "__abs__", "__anext__", "__bool__", "__bytes__", "__call__", "__complex__",
    "__dir__", "__float__", "__format__", "__hash__", "__index__", "__int__",
    "__len__", "__length_hint__", "__next__", "__repr__", "__reversed__",
    "__round__", "__str__", "__trunc__",
)  # fmt: skip


def _battery_arguments() -> tuple[tuple[object, ...], ...]:
    """Argument lists with an object that answers every protocol with an iterable
    and a class whose metaclass iterates."""

    methods: dict[str, object] = {
        name: lambda *_a, **_k: ("fcm_",) for name in _BATTERY_PROTOCOLS
    }
    methods["__iter__"] = lambda _s: iter(("fcm_",))
    methods["__aiter__"] = lambda s: s
    answering = type("Answering", (), methods)
    iterating = type("Iterating", (type,), {"__iter__": lambda _c: iter(("fcm_",))})
    klass = iterating("Klass", (), {})
    return (
        (),
        ("stub",),
        (("fcm_",),),
        (1,),
        ("f", "x", "exec"),
        (answering(),),
        (answering(), answering()),
        (klass(),),
        (klass,),
    )


def _returns_an_iterable(name: str) -> bool:
    """Whether a call of the builtin ``name`` under the battery returns an iterable."""

    for arguments in _battery_arguments():
        try:
            value = getattr(builtins, name)(*arguments)
        except (Exception, SystemExit):
            continue
        if hasattr(type(value), "__iter__") or hasattr(type(value), "__getitem__"):
            return True
    return False


@pytest.mark.parametrize("name", sorted(_BUILTINS_WITHOUT_A_FORM))
def test_a_named_builtin_returns_no_iterable_under_the_battery(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The partition is measured, not judged, as far as the battery reaches."""

    with monkeypatch.context() as stub:
        _stub_builtin_hooks(stub)
        assert not _returns_an_iterable(name)


@pytest.mark.parametrize(
    "name", ("__import__", "aiter", "anext", "breakpoint", "help", "type")
)
def test_the_battery_finds_the_builtins_that_answer_through_a_protocol(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Positive control: the battery finds each builtin a protocol or hook drives."""

    with monkeypatch.context() as stub:
        _stub_builtin_hooks(stub)
        assert _returns_an_iterable(name)


def test_every_callable_builtin_has_a_form_or_is_named() -> None:
    """A builtin a new Python adds, or a form struck here, breaks this partition."""

    others = {name for name in _callable_builtins() if not _is_exception_class(name)}
    assert set(_BUILTIN_ITERABLE_FORMS).isdisjoint(_BUILTINS_WITHOUT_A_FORM)
    assert others - _BUILTINS_WITHOUT_A_FORM == set(_BUILTIN_ITERABLE_FORMS)


@pytest.mark.parametrize("name", _callable_builtins())
def test_a_target_over_any_builtin_call_stays_unknown(name: str) -> None:
    """No callable builtin counts as a source of runtime values, whatever
    constant arguments it gets."""

    line = "            updated_data[K2] = copy.deepcopy(registration)\n"
    shapes = ("", "'fcm_'", "('fcm_',)", "1", "None")
    forms = "".join(
        f"            other = {{p + 'credentials': creds for p in {name}({shape})}}\n"
        for shape in shapes
    )
    _writes, unclassified, _shared = _snippet_problems(
        _changed((line, line + forms), name)
    )
    found = [message for message in unclassified if "unknown key" in message]
    assert len(found) == len(shapes), forms


@pytest.mark.parametrize("name", sorted(_BUILTIN_ITERABLE_FORMS))
def test_a_target_over_a_builtin_call_stays_unknown(
    name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Python holds the member through the form, and the guard reports the form."""

    form = _BUILTIN_ITERABLE_FORMS[name]
    with monkeypatch.context() as stub:
        _stub_builtin_hooks(stub)
        scope = {"creds": {"private": "x"}, "fcm_": 1}
        held = eval(form, scope)  # noqa: S307 - the frozen forms above only
    assert _MEMBER in held, form
    line = "            updated_data[K2] = copy.deepcopy(registration)\n"
    _writes, unclassified, _shared = _snippet_problems(
        _changed((line, f"{line}            other = {form}\n"), name)
    )
    assert any("unknown key" in message for message in unclassified), form


def _nested_comprehensions(depth: int) -> str:
    """A key of ``depth`` comprehensions, each the iterable of the next."""

    expression = "x0"
    for i in range(depth):
        expression = f"[x{i} for x{i} in {expression}]"
    write = "            updated_data[K2] = copy.deepcopy(registration)\n"
    nested = f"            other = {{}}\n            other[str({expression})] = creds\n"
    return _changed((write, write + nested), "nested comprehensions")


def _differences(work: list[int], order: int) -> list[list[int]]:
    """The differences of ``work`` up to ``order``, first differences first."""

    rows = [work]
    for _ in range(order):
        rows.append([b - a for a, b in zip(rows[-1], rows[-1][1:], strict=False)])
    return rows[1:]


def _is_at_most_square(work: list[int]) -> bool:
    """Whether the work of five equally spaced sizes grows at most with the square.

    Square work has third differences of zero up to a few events; a cubic part
    of c events per triple adds 6 c step^3 to each (R14-F9: the ratio
    used before had no positive control)."""

    first, _, third = _differences(work, 3)
    return 0 < first[0] and all(abs(d) <= first[0] // 1000 for d in third)


def _toy_work(size: int, cube: int) -> int:
    """Linear and square work plus ``cube`` hundredths of a loop step per triple."""

    def run() -> None:
        for _ in range(3000 * size + 30 * size * size):
            pass
        for _ in range(cube * size**3 // 100):
            pass

    return _executed_work(run)


def test_the_square_check_sees_a_small_cubic_part() -> None:
    """Positive control: the check tells a hundredth of a step per triple
    over a square part from the square part alone."""

    depths = (40, 60, 80, 100, 120)
    assert _is_at_most_square([_toy_work(d, 0) for d in depths])
    assert not _is_at_most_square([_toy_work(d, 1) for d in depths])


def test_nested_comprehensions_are_bound_in_square_work() -> None:
    """F-E: a walk up the parents per read, repeated for every iterable
    around it, grew with the cube of the nesting depth. Each iterable still
    reads the comprehensions inside it, so the work may grow with the square.
    Measured under pytest after round 17, depths 40 to 120 in steps of 20:
    third differences 0 and 0 (third differences 304000 and 304000 before
    F-E, measured after round 15). The first difference follows every change
    of the guard and is not stated (R17-F2). Below depth 40 a further part
    still grows: the first third difference is not 0 from depth 30 or depth 20
    (same steps; its value there depends on the order of the measurements), so
    the sizes start at 40."""

    def work(depth: int) -> int:
        snippet = _nested_comprehensions(depth)
        return _executed_work(functools.partial(_snippet_problems, snippet))

    # The first scan of a run also fills caches (532 events once under pytest).
    work(40)
    measured = [work(depth) for depth in (40, 60, 80, 100, 120)]
    assert _is_at_most_square(measured), _differences(measured, 3)


def test_a_concatenation_longer_than_the_member_is_dropped() -> None:
    """R8-F4: a string longer than the member never becomes it by
    concatenation, so ``_join`` drops it and a doubling name stays small."""

    assert _join(frozenset({"fcm_"}), frozenset({"credentials"})) == {_MEMBER}
    assert _join(frozenset({_MEMBER}), frozenset({"x"})) == frozenset()
    doubling = "".join(f"D{i} = D{i - 1} + D{i - 1}\n" for i in range(1, 9))
    env = _module_values(ast.parse("D0 = 'ab'\n" + doubling), {})
    assert env["D2"] == {"abababab"}
    assert env["D8"] == frozenset()


def _chain(n: int, back_edge: bool) -> ast.Module:
    lines = [f'N{i} = N{i - 1} + "a"' for i in range(n, 0, -1)] + ['N0 = "a"']
    return ast.parse("\n".join([*lines, f"N0 = N{n}"] if back_edge else lines))


def test_the_fixpoint_evaluates_each_binding_a_bounded_number_of_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R7-F5: dependencies first, each binding folded once on the acyclic chain
    written backwards, and a growing cycle is unknown without being folded."""

    calls: Counter[int] = Counter()
    fold = _values

    def counted(node: ast.AST, *args: object) -> Values:
        calls[id(node)] += 1
        return fold(node, *args)  # type: ignore[arg-type]

    monkeypatch.setattr(sys.modules[__name__], "_values", counted)
    env = _module_values(_chain(1000, back_edge=False), {})
    assert (sum(calls.values()), max(calls.values())) == (1001, 1)
    # R8-F4: a value longer than the member is dropped, the count stays.
    assert env["N14"] == frozenset({"a" * len(_MEMBER)})
    assert env["N15"] == env["N1000"] == frozenset()
    calls.clear()
    env = _module_values(_chain(1000, back_edge=True), {})
    assert max(calls.values(), default=0) <= 1
    assert env["N1000"] is _UNKNOWN
