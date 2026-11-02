# tests/test_guard_eval_exec_sites.py
"""Guard: every use of ``eval``/``exec`` is a listed exception of ``tests/AGENTS.md``.

``AGENTS.md`` section 11.2 says never to use ``eval``/``exec``, in the whole
repository. The section "``eval`` and ``exec`` in tests" of ``tests/AGENTS.md``
allows a fixed list of sites in tests. This guard keeps the code, ``ALLOWED`` and
that section in step: every reference to ``eval`` or ``exec`` in a Python file git
would commit (tracked, or untracked and not ignored) must be listed with its count
per enclosing function (qualified through classes and functions), every listed site
must still exist, and the section must list exactly ``ALLOWED``.

A reference is the bare name (a call, an alias such as ``run = exec``, an
assignment such as ``exec = print``, ``del eval``), any attribute named ``eval``
or ``exec`` whatever object carries it (``builtins.exec``, ``b.exec`` after
``import builtins as b``, ``__import__("builtins").exec``,
``sys.modules["builtins"].exec``), each keyword of a class pattern
(``case object(exec=run)`` reads the attribute), and each of the two names
imported from ``builtins``. Counting references rather than calls keeps an alias
of the function or of its module from hiding a later call. A name that only binds
or declares (``def exec``, a parameter, ``except E as exec``, ``case exec``,
``import os as exec``, ``global exec``) reads nothing and does not count; a later
use of it does.

A decorator, a default value, an annotation, a base class and a type parameter
(``def f[T: X]()``) belong to the scope that encloses the definition; a lambda and a comprehension belong to the function
they stand in. Two definitions with the same qualified name in one scope add up
(a redefinition, the two arms of an ``if``).

Limit: a lookup by string (``getattr(builtins, name)``,
``builtins.__dict__["exec"]``, ``vars(builtins)["exec"]``) is invisible; the section
names the one known case.
"""

from __future__ import annotations

import ast
import os
import re
import subprocess
from collections import Counter
from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]

_NAMES = frozenset({"eval", "exec"})

_SCOPES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

_SECTION_TITLE = "## `eval` and `exec` in tests"

_ROW = re.compile(r"^\| `(?P<file>[^`]+)` \| `(?P<owner>[^`]+)` \| (?P<count>\d+) \|$")

_FOLDING = "tests/test_guard_credential_key_folding.py"

#: ``(file, enclosing function) -> number of references``; mirrors the table in
#: ``tests/AGENTS.md``. ``<module>`` would stand for a reference at module level.
ALLOWED: dict[tuple[str, str], int] = {
    ("tests/helpers/ast_extract.py", "compile_class_method_from_module"): 1,
    (_FOLDING, "_python_writes"): 1,
    (_FOLDING, "test_python_is_the_oracle_for_class_bases"): 2,
    (_FOLDING, "_oracle_runs_to_the_member"): 2,
    (_FOLDING, "_keys_held_after"): 1,
    (_FOLDING, "test_the_statement_loop_limit_writes_the_member_in_python"): 1,
    (_FOLDING, "test_a_target_over_a_builtin_call_stays_unknown"): 1,
}


def _git_environment() -> dict[str, str]:
    """Return the environment without ``GIT_*``, so git looks at ``cwd`` only.

    A git hook exports ``GIT_DIR`` and ``GIT_INDEX_FILE``; inherited, they would
    point every git call of this guard at the repository of the caller.
    """
    return {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }


def _references(node: ast.AST) -> int:
    """Return how many references to ``eval``/``exec`` ``node`` itself makes."""
    if isinstance(node, ast.Name):
        return int(node.id in _NAMES)
    if isinstance(node, ast.Attribute):
        return int(node.attr in _NAMES)
    if isinstance(node, ast.MatchClass):
        # ``case K(exec=x)`` looks the attribute up with ``getattr``; the name is a
        # string in ``kwd_attrs``, not an ``ast.Attribute``.
        return sum(attribute in _NAMES for attribute in node.kwd_attrs)
    if isinstance(node, ast.ImportFrom) and node.module == "builtins":
        return sum(alias.name in _NAMES for alias in node.names)
    return 0


def _call_sites(source: str) -> Counter[str]:
    """Count the ``eval``/``exec`` references of ``source`` per enclosing scope."""
    found: Counter[str] = Counter()

    def walk(node: ast.AST, owner: str) -> None:
        count = _references(node)
        if count:
            found[owner] += count
        if not isinstance(node, _SCOPES):
            for child in ast.iter_child_nodes(node):
                walk(child, owner)
            return
        # Only the body runs in the new scope; decorators, defaults, annotations
        # and bases are evaluated where the definition stands.
        inner = node.name if owner == "<module>" else f"{owner}.{node.name}"
        for field, value in ast.iter_fields(node):
            for item in value if isinstance(value, list) else [value]:
                if isinstance(item, ast.AST):
                    walk(item, inner if field == "body" else owner)

    walk(ast.parse(source), "<module>")
    return found


def _python_files(repo: Path) -> list[str]:
    """Return every ``.py`` path git would commit in ``repo``, or raise.

    Tracked files and untracked files that no ignore rule covers; ``GIT_*`` of the
    environment is dropped. A missing git, a non-zero exit or an empty result
    raise: a silent fallback would look exactly like full coverage.
    """
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"]
        + ["--", "*.py"],
        cwd=repo,
        env=_git_environment(),
        capture_output=True,
        check=False,
        timeout=120,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", "replace").strip()
        raise AssertionError(f"git ls-files failed, nothing was scanned: {stderr}")
    names = {
        name
        for name in result.stdout.decode("utf-8", "surrogateescape").split("\0")
        if name
    }
    assert names, "git ls-files returned no Python file, which cannot be right"
    return sorted(names)


def _repository_call_sites(repo: Path = _REPO) -> dict[tuple[str, str], int]:
    """Collect every ``eval``/``exec`` reference in the Python files of ``repo``."""
    sites: dict[tuple[str, str], int] = {}
    for relative in _python_files(repo):
        path = repo / relative
        # A tracked file deleted in the working tree has no source left to run.
        if not path.is_file():
            continue
        for owner, count in _call_sites(path.read_text(encoding="utf-8")).items():
            sites[(relative, owner)] = count
    return sites


def _drift(
    found: Mapping[tuple[str, str], int], allowed: Mapping[tuple[str, str], int]
) -> list[str]:
    """Describe every difference between the found and the allowed references."""
    problems: list[str] = []
    for site in sorted(found.keys() | allowed.keys()):
        file, owner = site
        have, want = found.get(site, 0), allowed.get(site, 0)
        if want == 0:
            problems.append(f"{file}::{owner}: {have} unlisted reference(s)")
        elif have == 0:
            problems.append(f"{file}::{owner}: listed, but no reference is left")
        elif have != want:
            problems.append(f"{file}::{owner}: {have} reference(s), listed {want}")
    return problems


def _documented_sites(agents_text: str) -> dict[tuple[str, str], int]:
    """Read the table of the exception section from the text of ``tests/AGENTS.md``."""
    lines = agents_text.splitlines()
    assert lines.count(_SECTION_TITLE) == 1, f"{_SECTION_TITLE!r} must occur once"
    sites: dict[tuple[str, str], int] = {}
    for line in lines[lines.index(_SECTION_TITLE) + 1 :]:
        if line.startswith("## "):
            break
        match = _ROW.match(line)
        if match is not None:
            site = (match["file"], match["owner"])
            assert site not in sites, f"row listed twice: {site}"
            sites[site] = int(match["count"])
    return sites


def test_every_eval_or_exec_use_is_a_listed_exception() -> None:
    """Every use in the repository is listed with its count, and no more."""
    assert _drift(_repository_call_sites(), ALLOWED) == []


def test_the_contract_section_lists_exactly_the_allowed_sites() -> None:
    """The table in ``tests/AGENTS.md`` lists exactly ``ALLOWED``."""
    text = (_REPO / "tests" / "AGENTS.md").read_text(encoding="utf-8")
    assert _documented_sites(text) == ALLOWED


def test_the_root_rules_point_to_the_exception() -> None:
    """Both rules in ``AGENTS.md`` that forbid ``eval``/``exec`` name the exception."""
    text = (_REPO / "AGENTS.md").read_text(encoding="utf-8")
    rules = [
        line for line in text.splitlines() if re.search(r"\beval`?/`?exec\b", line)
    ]
    assert len(rules) == 2, rules
    assert all("tests/AGENTS.md" in rule for rule in rules), rules


def test_the_scanner_sees_each_written_form() -> None:
    """Each written form counts for its scope; a lookup by string does not (limit)."""
    source = (
        "import builtins\n"
        "from builtins import exec as run\n"
        "exec('a = 1')\n"
        "def one():\n"
        "    return eval('1')\n"
        "async def two():\n"
        "    builtins.exec('b = 2')\n"
        "    def inner():\n"
        "        __builtins__.eval('2')\n"
        "class Holder:\n"
        "    alias = exec\n"
        "    def method(self):\n"
        "        exec('c = 3'); exec('d = 4')\n"
        "def one_more():\n"
        "    e = eval\n"
        "    e('5')\n"
        "    f = lambda: exec('g = 6')\n"
        "    other.exec('f = 6')\n"
        "def unseen():\n"
        "    getattr(builtins, 'exec')('e = 5')\n"
        "    builtins.__dict__['exec']('h = 7')\n"
        "    vars(builtins)['eval']('8')\n"
        "    text = \"exec('g = 7')\"\n"
    )
    assert _call_sites(source) == Counter(
        {
            "<module>": 2,
            "one": 1,
            "two": 1,
            "two.inner": 1,
            "Holder": 1,
            "Holder.method": 2,
            "one_more": 3,
        }
    )


@pytest.mark.parametrize(
    "form",
    [
        "import builtins as b\ndef f():\n    b.exec('x = 1')\n",
        "import builtins\nB = builtins\ndef f():\n    B.exec('x = 1')\n",
        "def f():\n    __import__('builtins').exec('x = 1')\n",
        "import sys\ndef f():\n    sys.modules['builtins'].exec('x = 1')\n",
        "import importlib\n"
        "def f():\n    importlib.import_module('builtins').eval('1')\n",
    ],
)
def test_an_alias_of_the_module_cannot_hide_a_call(form: str) -> None:
    """``eval``/``exec`` reached through any binding of ``builtins`` counts once."""
    assert _call_sites(form) == Counter({"f": 1})


def test_a_binding_or_a_deletion_of_the_name_counts() -> None:
    """``exec = print`` and ``del eval`` are references, not only a load."""
    source = "def f():\n    exec = print\n    del eval\n"
    assert _call_sites(source) == Counter({"f": 2})


def test_each_name_imported_from_builtins_counts() -> None:
    """``from builtins import eval, exec`` makes two references, not one."""
    assert _call_sites("from builtins import eval, exec\n") == Counter({"<module>": 2})


def test_a_keyword_of_a_class_pattern_counts() -> None:
    """``case object(exec=run)`` reads ``exec`` through ``getattr``, so it counts."""
    source = (
        "def f(subject):\n"
        "    match subject:\n"
        "        case object(exec=run, eval=other, real=x):\n"
        "            return run, other, x\n"
    )
    assert _call_sites(source) == Counter({"f": 2})


#: Every field of the grammar that holds a name as a string, by what it does with
#: it. Only a reading field can reach ``eval``/``exec``; each sample uses ``exec``.
_READING_FIELDS: dict[str, str] = {
    "Name.id": "exec\n",
    "Attribute.attr": "x.exec\n",
    "MatchClass.kwd_attrs": "match x:\n    case K(exec=y):\n        pass\n",
    "alias.name": "from builtins import exec\n",
}
_NON_READING_FIELDS: dict[str, str] = {
    "FunctionDef.name": "def exec():\n    pass\n",
    "AsyncFunctionDef.name": "async def exec():\n    pass\n",
    "ClassDef.name": "class exec:\n    pass\n",
    "ExceptHandler.name": "try:\n    pass\nexcept E as exec:\n    pass\n",
    "Global.names": "global exec\n",
    "Nonlocal.names": "def f():\n    nonlocal exec\n",
    "MatchAs.name": "match x:\n    case exec:\n        pass\n",
    "MatchStar.name": "match x:\n    case [*exec]:\n        pass\n",
    "MatchMapping.rest": "match x:\n    case {**exec}:\n        pass\n",
    "TypeVar.name": "def f[exec]():\n    pass\n",
    "ParamSpec.name": "def f[**exec]():\n    pass\n",
    "TypeVarTuple.name": "def f[*exec]():\n    pass\n",
    "arg.arg": "def f(exec):\n    pass\n",
    "keyword.arg": "f(exec=1)\n",
    "alias.asname": "import os as exec\n",
    "ImportFrom.module": "from exec import x\n",
}
#: Never a name the guard sees: ``ast.parse`` leaves type comments out, and the
#: other two hold a tag and a string prefix.
_NEVER_A_NAME = frozenset(
    {
        "Assign.type_comment",
        "AsyncFor.type_comment",
        "AsyncWith.type_comment",
        "For.type_comment",
        "FunctionDef.type_comment",
        "AsyncFunctionDef.type_comment",
        "With.type_comment",
        "arg.type_comment",
        "TypeIgnore.tag",
        "Constant.kind",
    }
)


def _string_fields() -> set[str]:
    """Return ``Node.field`` for every field of the running grammar typed as a name."""
    kinds = (str, str | None, list[str])
    return {
        f"{name}.{field}"
        for name, node in vars(ast).items()
        if isinstance(node, type) and issubclass(node, ast.AST)
        for field, kind in getattr(node, "_field_types", {}).items()
        if kind in kinds
    }


def test_every_name_field_of_the_grammar_is_classified() -> None:
    """A new field in a later grammar fails here before it can hide a reference."""
    classified = [*_READING_FIELDS, *_NON_READING_FIELDS, *_NEVER_A_NAME]
    assert len(classified) == len(set(classified))
    assert _string_fields() == set(classified)


@pytest.mark.parametrize("field", sorted(_READING_FIELDS))
def test_each_reading_field_counts(field: str) -> None:
    """Each field that looks a name up counts once in its sample."""
    assert _call_sites(_READING_FIELDS[field]) == Counter({"<module>": 1})


@pytest.mark.parametrize("field", sorted(_NON_READING_FIELDS))
def test_a_field_that_only_binds_or_declares_does_not_count(field: str) -> None:
    """A binding, a declaration or a module name reads no builtin."""
    assert _call_sites(_NON_READING_FIELDS[field]) == Counter()


def test_a_definition_header_belongs_to_the_enclosing_scope() -> None:
    """Decorators, defaults, annotations, bases and type parameters run outside."""
    source = (
        "def outer():\n"
        "    @register(exec)\n"
        "    def deco(x=eval('1'), *, y: exec = None) -> eval:\n"
        "        return None\n"
        "    class Base(Holder[exec], meta=eval):\n"
        "        z = exec\n"
        "    def g[T: exec]():\n"
        "        return None\n"
        "    class K[T: eval]:\n"
        "        pass\n"
    )
    assert _call_sites(source) == Counter({"outer": 8, "outer.Base": 1})


def test_same_named_definitions_add_up() -> None:
    """Limit: a redefinition and the two arms of an ``if`` share one row."""
    source = (
        "def f():\n    exec('a = 1')\n"
        "def f():\n    exec('b = 2')\n"
        "if flag:\n"
        "    def g():\n        eval('1')\n"
        "else:\n"
        "    def g():\n        eval('2')\n"
    )
    assert _call_sites(source) == Counter({"f": 2, "g": 2})


def test_the_drift_report_names_each_kind_of_change() -> None:
    """A new site, an extra or a missing reference and a stale row each fail."""
    allowed = {("a.py", "f"): 1, ("a.py", "g"): 1}
    assert _drift(allowed, allowed) == []
    assert _drift({**allowed, ("b.py", "h"): 1}, allowed) == [
        "b.py::h: 1 unlisted reference(s)"
    ]
    assert _drift({("a.py", "f"): 2, ("a.py", "g"): 1}, allowed) == [
        "a.py::f: 2 reference(s), listed 1"
    ]
    assert _drift(
        {("a.py", "f"): 1, ("a.py", "g"): 1}, {**allowed, ("a.py", "f"): 2}
    ) == ["a.py::f: 1 reference(s), listed 2"]
    assert _drift({("a.py", "f"): 1}, allowed) == [
        "a.py::g: listed, but no reference is left"
    ]


def test_the_section_reader_stops_at_the_next_section() -> None:
    """Rows after the section belong to another section; a removed row is missing."""
    text = "\n".join(
        [
            _SECTION_TITLE,
            "| `a.py` | `f` | 1 |",
            "| `a.py` | `g` | 2 |",
            "## Next",
            "| `b.py` | `h` | 1 |",
        ]
    )
    assert _documented_sites(text) == {("a.py", "f"): 1, ("a.py", "g"): 2}
    shortened = text.replace("| `a.py` | `g` | 2 |\n", "")
    assert _documented_sites(shortened) == {("a.py", "f"): 1}
    last = "\n".join([_SECTION_TITLE, "| `a.py` | `f` | 1 |"])
    assert _documented_sites(last) == {("a.py", "f"): 1}


def test_a_row_listed_twice_fails() -> None:
    """A duplicate row would let the second count overwrite the first unnoticed."""
    text = "\n".join([_SECTION_TITLE, "| `a.py` | `f` | 1 |", "| `a.py` | `f` | 2 |"])
    with pytest.raises(AssertionError, match="row listed twice"):
        _documented_sites(text)


def _git(repo: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", *arguments],
        cwd=repo,
        env=_git_environment(),
        capture_output=True,
        check=True,
        timeout=120,
    ).stdout


@pytest.fixture
def foreign_git(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """Point ``GIT_*`` at a decoy, as a git hook or a caller's environment may.

    Beyond ``GIT_DIR`` and ``GIT_INDEX_FILE`` of a hook, ``GIT_WORK_TREE`` names
    another tree and ``GIT_CONFIG_*`` ignores every ``.py`` file; a filter that
    let any of them through would change what the guard scans.
    """
    decoy = tmp_path_factory.mktemp("decoy")
    _git(decoy, "init", "-q")
    ignore_all = decoy / "ignore-all-python"
    ignore_all.write_text("*.py\n", encoding="utf-8")
    monkeypatch.setenv("GIT_DIR", str(decoy / ".git"))
    monkeypatch.setenv("GIT_INDEX_FILE", str(decoy / ".git" / "index"))
    monkeypatch.setenv("GIT_WORK_TREE", str(decoy))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.excludesFile")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", str(ignore_all))
    yield
    assert _git(decoy, "ls-files") == b"", "a git call wrote into the decoy"


@pytest.mark.usefixtures("foreign_git")
def test_the_scan_covers_every_python_file_git_would_commit(tmp_path: Path) -> None:
    """Any directory counts, the root included; only an ignored file stays out."""
    files = ("tools/a.py", "b.py", "tests/c.py", "d/e.py", "build/f.py")
    for relative in files:
        (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / relative).write_text("exec('x = 1')\n", encoding="utf-8")
    (tmp_path / ".gitignore").write_text("build/\n", encoding="utf-8")
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "add", "tools/a.py", "b.py", "tests/c.py", ".gitignore")
    assert _repository_call_sites(tmp_path) == {
        (relative, "<module>"): 1 for relative in files[:4]
    }


def test_the_scan_reaches_beyond_the_integration_and_the_tests() -> None:
    """The two files the first scope (three directories) missed are scanned."""
    assert {"sitecustomize.py", "tools/analyze_fmdn_logs.py"} <= set(
        _python_files(_REPO)
    )


@pytest.mark.usefixtures("foreign_git")
def test_the_scan_fails_loudly_outside_a_repository(tmp_path: Path) -> None:
    """Without a repository nothing is scanned, and that must not pass as clean."""
    with pytest.raises(AssertionError, match="git ls-files failed"):
        _python_files(tmp_path)


@pytest.mark.usefixtures("foreign_git")
def test_a_repository_without_a_python_file_fails_loudly(tmp_path: Path) -> None:
    """An empty result is no proof of a clean repository."""
    (tmp_path / "notes.txt").write_text("exec\n", encoding="utf-8")
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "add", "notes.txt")
    with pytest.raises(AssertionError, match="returned no Python file"):
        _python_files(tmp_path)


@pytest.mark.usefixtures("foreign_git")
def test_a_tracked_file_deleted_in_the_working_tree_is_skipped(
    tmp_path: Path,
) -> None:
    """Git still lists it, but no source is left that could run."""
    for relative in ("kept.py", "gone.py"):
        (tmp_path / relative).write_text("exec('x = 1')\n", encoding="utf-8")
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "add", "kept.py", "gone.py")
    (tmp_path / "gone.py").unlink()
    assert "gone.py" in _python_files(tmp_path)
    assert _repository_call_sites(tmp_path) == {("kept.py", "<module>"): 1}
