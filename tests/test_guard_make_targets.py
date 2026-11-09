# tests/test_guard_make_targets.py
"""Fail when documentation or scripts invoke a ``make`` target the Makefile lacks.

The move to Poetry (``ba42a35f``) removed six Makefile targets
(``bootstrap-base-deps``, ``test-deps``, ``test-stubs``, ``install-ha-stubs``,
``wheelhouse``, ``clean-wheelhouse``), while ``AGENTS.md`` and the README kept
telling contributors to run them. Nothing failed: a dead ``make`` invocation in
prose is only noticed by the person who runs it.

This guard collects every ``make <target>`` invocation that stands in a code
context and compares the target names with the targets the root ``Makefile``
defines. Code context means exactly two places, because a plain search for
``make <word>`` mostly finds English prose ("make sure", "make the"):

* an inline code span in single backticks (`` `make lint` ``) or in the
  reStructuredText form with two backticks on each side, in any tracked text
  file;
* a whole line, but only in shell scripts, the Makefile itself, ``run:`` steps
  of workflow files (the value of the key and the lines of a ``run: |`` block)
  and fenced code blocks of Markdown files whose info string is empty or names a
  shell (``bash``, ``sh``, ``shell``, ``shell-session``, ``console``, ``zsh``;
  the first word of the info string counts, ``{.bash}`` and ``bash{1}`` too). A
  leading shell prompt ``$`` or a list marker ``-`` is dropped first, and a line
  that ends in a backslash is joined with the next one of the same block, unless
  it is a comment.

Such a text is split at the shell operators ``&&``, ``||``, ``;``, ``|``, ``&``
and parentheses, and every segment is read on its own. A segment counts when
its first word, after leading variable assignments (``CI=1``) and the recipe
prefixes ``@``, ``-`` and ``+``, is ``make`` or ``$(MAKE)``. From such a segment
the guard reads the words after ``make``: options are skipped together with
their value (``-j`` and ``-l`` take a value only when it is a number), variable
assignments (``PYTEST_ARGS=-x``) and redirections are skipped, and the remaining
words are targets until the first word that cannot be a target name. A segment
with ``-C``, ``-f``, ``--directory``, ``--file`` or ``--makefile`` reads a
different Makefile and is skipped. A word with a shell or template expansion
(``$``, ``<``, ``{``, ``*``, ``%``) is a placeholder, not a target, and is
skipped as well; a command substitution counts as one word (``-j$(nproc)``). The
word ``make`` among the arguments starts a new call.

Targets of the Makefile are the names before ``:`` on lines that start a rule.
Not targets: assignments of any flavour (``=``, ``:=``, ``::=``, ``?=``, ``+=``,
``!=``), lines inside ``define`` blocks, recipe lines, conditionals and other
directives (``ifeq``, ``vpath``, ``include`` and the like), special targets such
as ``.PHONY``, pattern rules (``%``), names built from variables (``$``) and any
other word that is not a plain target name (the ``&`` of grouped targets). If that
set comes out empty, the guard fails instead of passing: an empty set would
report every invocation, and a reader that silently returns nothing has already
happened once while this guard was prototyped (``make -pRrq`` yields no targets
here).

``ALLOWED_MENTIONS`` pins the places that name a removed target on purpose,
keyed by path and target name, and admits a mention only on a line that also
carries the pinned wording. A key without the wording would admit every mention
of the target in that file: measured against the tree before PR #1392, it hid
all twenty dead invocations in ``AGENTS.md``. A second test fails when a pinned
mention no longer occurs, so the list cannot keep an entry that would later hide
a real dead invocation.

Limits: the guard reads files tracked by git (``git ls-files``, run without the
caller's ``GIT_*`` environment) with the suffixes in ``_TEXT_SUFFIXES``, and only
the root ``Makefile``. It does not see a ``make`` call assembled from variables,
one behind another command word (``poetry run make``, ``if make``), one inside a
Python string passed to ``subprocess``, one in prose without code markup, a fence
inside a block quote, or an RST literal block (``.rst`` files are read for inline
code only), a nested code span, an HTML ``<code>`` element or a fenced block
written on a single line. A heredoc body in a script is read like any other line.
In workflow files every ``run:`` key counts, also one under ``with:``, and a
plain scalar ``run:`` value is read on its first line only.
Words that follow the targets inside the same code span are read as targets too
(`` `make "lint" twice` `` yields ``lint`` and ``twice``). A tracked
file that cannot be read fails the guard; one deleted in the working tree is
skipped. The guard skips its own file, whose fixtures name dead targets on
purpose. A Markdown fence opens with three or more backticks or tildes (a
backtick opener with a backtick later on the line is no fence) and closes only
with the same character, at least as long and with nothing after it; an unclosed
fence counts as code up to the end of the file.

``MAKE_TARGETS_REPO_ROOT`` overrides the repository root that is read; it exists
for red probes against an older or mutated checkout.
"""

from __future__ import annotations

import bisect
import functools
import os
import re
import shlex
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

_SELF = "tests/test_guard_make_targets.py"

REPO_ROOT = Path(
    os.environ.get("MAKE_TARGETS_REPO_ROOT", Path(__file__).resolve().parents[1])
)

# AGENTS.md, "Bootstrap helper": lists the targets removed with the move to
# Poetry so that readers of older instructions recognise them.
_REMOVED_WITH_POETRY = "were removed with the move to Poetry and no longer exist"

# Places that name a removed target on purpose. Key: (repo-relative path, target);
# value: wording that must stand on the same line for the mention to be admitted.
ALLOWED_MENTIONS: dict[tuple[str, str], str] = {
    ("AGENTS.md", "bootstrap-base-deps"): _REMOVED_WITH_POETRY,
    ("AGENTS.md", "test-deps"): _REMOVED_WITH_POETRY,
    ("AGENTS.md", "test-stubs"): _REMOVED_WITH_POETRY,
    ("AGENTS.md", "install-ha-stubs"): _REMOVED_WITH_POETRY,
}

_TEXT_SUFFIXES = frozenset(
    {
        ".md",
        ".rst",
        ".txt",
        ".py",
        ".sh",
        ".bash",
        ".yml",
        ".yaml",
        ".toml",
        ".cfg",
        ".ini",
    }
)
_LINE_SUFFIXES = frozenset({".sh", ".bash", ".yml", ".yaml"})
_MARKDOWN_SUFFIXES = frozenset({".md"})

_INLINE_RE = re.compile(
    r"(?<!`)(``?)(?!`)((?:(?!\n[ \t]*\n).)+?)(?<!`)\1(?!`)", re.DOTALL
)
_LINE_PREFIX_RE = re.compile(r"^\s*(?:\$\s+|-\s+)?")
_FENCE_OPEN_RE = re.compile(r"^\s*(`{3,}|~{3,})(.*)$")
_SHELL_FENCES = frozenset(
    {"", "bash", "sh", "shell", "shell-session", "console", "zsh"}
)
_FENCE_LANGUAGE_RE = re.compile(r"[{.\s]*([A-Za-z][A-Za-z0-9_-]*)?")
_YAML_RUN_RE = re.compile(r"^(\s*)(-\s+)?run:\s*(.*)$")
_SUBSTITUTION_RE = re.compile(r"\$\(\([^()]*\)\)|\$\([^()]*\)")
_FALLBACK_TOKEN_RE = re.compile(r"&&|\|\||[;|&()]|[^\s;|&()]+")
_MAKEFILE_DIRECTIVES = frozenset(
    {
        "ifeq",
        "ifneq",
        "ifdef",
        "ifndef",
        "else",
        "endif",
        "vpath",
        "include",
        "-include",
        "sinclude",
        "override",
        "export",
        "unexport",
        "undefine",
        "private",
    }
)
_ASSIGNMENT_WORD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
_TARGET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.+-]*$")
_MAKE_VARIABLE_RE = re.compile(r"\$\(MAKE\)|\$\{MAKE\}")

_SEPARATORS = frozenset({"&&", "||", ";", ";;", "|", "|&", "&", "(", ")"})
_REDIRECTIONS = frozenset({">", ">>", "<", ">&", "<&", "&>", "&>>", "<<", "<<<"})
_OTHER_MAKEFILE_OPTIONS = ("-C", "-f", "--directory", "--file", "--makefile")
_NUMERIC_VALUE_OPTIONS = frozenset({"-j", "-l", "--jobs", "--load-average"})
_VALUE_OPTIONS = frozenset(
    {
        "-o",
        "-W",
        "-I",
        "--old-file",
        "--assume-old",
        "--what-if",
        "--new-file",
        "--assume-new",
        "--include-dir",
    }
)
_PLACEHOLDER_CHARS = "$<{*%"


def makefile_targets(makefile: Path) -> set[str]:
    """Return the rule targets that ``makefile`` defines, without special targets."""
    targets: set[str] = set()
    in_define = False
    for line in makefile.read_text(encoding="utf-8").splitlines():
        words = line.split()
        if in_define:
            in_define = bool(words) and words[0] != "endef"
            continue
        if (
            words
            and words[0] in {"define", "override", "export"}
            and (words[0] == "define" or (len(words) > 1 and words[1] == "define"))
        ):
            in_define = True
            continue
        if not line or line.startswith(("\t", " ", "#")):
            continue
        if words[0] in _MAKEFILE_DIRECTIVES:
            continue
        colon = line.find(":")
        equals = line.find("=")
        if colon < 0 or (0 <= equals < colon):
            continue
        if line[colon:].lstrip(":").startswith("="):
            continue
        for name in line[:colon].split():
            if name.startswith(".") or not _TARGET_RE.match(name):
                continue
            targets.add(name)
    return targets


def tracked_text_files(root: Path) -> list[str]:
    """Return the tracked files under ``root`` that this guard reads."""
    env = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    result = subprocess.run(  # noqa: S603
        ["git", "ls-files", "-z"],  # noqa: S607
        cwd=root,
        env=env,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"git ls-files failed in {root} (rc={result.returncode}), so no file was "
        f"read: {result.stderr.decode(errors='replace').strip()}"
    )
    names = [name for name in result.stdout.decode().split("\0") if name]
    return [
        name
        for name in names
        if Path(name).name == "Makefile" or Path(name).suffix in _TEXT_SUFFIXES
    ]


def _tokens(command: str) -> list[str]:
    """Split ``command`` into words and shell operators."""
    command = _MAKE_VARIABLE_RE.sub("make", command)
    while True:
        substituted = _SUBSTITUTION_RE.sub("$SUB", command)
        if substituted == command:
            break
        command = substituted
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        return list(lexer)
    except ValueError:
        return _FALLBACK_TOKEN_RE.findall(command)


def _segments(tokens: list[str]) -> Iterator[list[str]]:
    """Yield the token runs between shell separators."""
    segment: list[str] = []
    for token in tokens:
        if token in _SEPARATORS:
            if segment:
                yield segment
            segment = []
        else:
            segment.append(token)
    if segment:
        yield segment


def _segment_targets(segment: list[str]) -> list[str]:
    """Return the targets of one segment, or nothing if it does not run make."""
    index = 0
    while index < len(segment) and _ASSIGNMENT_WORD_RE.match(segment[index]):
        index += 1
    if index >= len(segment) or segment[index].lstrip("@-+") != "make":
        return []
    targets: list[str] = []
    words = segment[index + 1 :]
    position = 0
    while position < len(words):
        word = words[position]
        position += 1
        if word in _REDIRECTIONS:
            position += 1
            continue
        if word.lstrip("@-+") == "make":
            return targets + _segment_targets(words[position - 1 :])
        if word.startswith(_OTHER_MAKEFILE_OPTIONS):
            return []
        if word.startswith("-"):
            if word in _VALUE_OPTIONS:
                position += 1
            elif (
                word in _NUMERIC_VALUE_OPTIONS
                and position < len(words)
                and words[position].isdigit()
            ):
                position += 1
            continue
        if "=" in word or any(char in word for char in _PLACEHOLDER_CHARS):
            continue
        if (
            word.isdigit()
            and position < len(words)
            and words[position] in _REDIRECTIONS
        ):
            continue
        name = word.rstrip(",.")
        if not _TARGET_RE.match(name):
            break
        targets.append(name)
    return targets


def invoked_targets(command: str) -> list[str]:
    """Return the target names of every ``make`` call in one shell text."""
    return [
        target
        for segment in _segments(_tokens(command))
        for target in _segment_targets(segment)
    ]


def invocations(path: str, text: str) -> Iterator[tuple[int, str, str]]:
    """Yield ``(line number, line, code text)`` for every code context of a file.

    Inline spans may run across a line break, as in Markdown, but not across a
    blank line or into a fence; their line is the line where the span starts.
    A whole line that ends in a backslash is joined with the lines after it and
    reported at its first line.
    """
    suffix = Path(path).suffix
    shell_lines = Path(path).name == "Makefile" or suffix in {".sh", ".bash"}
    yaml = suffix in {".yml", ".yaml"}
    markdown = suffix in _MARKDOWN_SUFFIXES
    lines = text.splitlines()
    found: list[tuple[int, str, str]] = []
    pending: list[tuple[int, str, str]] = []

    def emit(number: int, line: str, code: str) -> None:
        code = _LINE_PREFIX_RE.sub("", code, count=1)
        if pending:
            first, first_line, head = pending.pop()
            number, line, code = first, first_line, head + " " + code.strip()
        if code.rstrip().endswith("\\") and not code.lstrip().startswith("#"):
            pending.append((number, line, code.rstrip()[:-1]))
        else:
            found.append((number, line, code))

    def flush() -> None:
        if pending:
            found.append(pending.pop())

    outside_fences: list[str] = []
    fence: str | None = None
    shell_fence = False
    run_indent: int | None = None
    for number, line in enumerate(lines, start=1):
        if markdown:
            match = _FENCE_OPEN_RE.match(line)
            if (
                fence is None
                and match is not None
                and not (match.group(1)[0] == "`" and "`" in match.group(2))
            ):
                fence = match.group(1)
                language = _FENCE_LANGUAGE_RE.match(match.group(2))
                name = (language.group(1) or "") if language else ""
                shell_fence = name.lower() in _SHELL_FENCES
                outside_fences.append("")
                continue
            if fence is not None:
                closing = line.strip()
                if (
                    closing
                    and set(closing) == {fence[0]}
                    and len(closing) >= len(fence)
                ):
                    fence = None
                    flush()
                elif shell_fence:
                    emit(number, line, line)
                outside_fences.append("")
                continue
        outside_fences.append(line)
        if shell_lines:
            emit(number, line, line)
        elif yaml:
            indent = len(line) - len(line.lstrip())
            if run_indent is not None and (not line.strip() or indent > run_indent):
                if line.strip():
                    emit(number, line, line)
                continue
            run_indent = None
            flush()
            match = _YAML_RUN_RE.match(line)
            if match is not None:
                value = match.group(3).strip()
                if value[:1] in {"|", ">"}:
                    run_indent = len(match.group(1)) + len(match.group(2) or "")
                elif value:
                    emit(number, line, value.strip("\"'"))
    flush()
    masked = "\n".join(outside_fences)
    line_starts = [0] + [index + 1 for index, char in enumerate(masked) if char == "\n"]
    for match in _INLINE_RE.finditer(masked):
        number = bisect.bisect_right(line_starts, match.start(2))
        code = " ".join(match.group(2).split())
        found.append((number, lines[number - 1], code))
    yield from sorted(found, key=lambda item: item[0])


def check_text(
    path: str, text: str, targets: set[str]
) -> tuple[list[tuple[int, str]], set[tuple[str, str]], int]:
    """Return dead invocations, admitted mentions and call count of one file."""
    dead: list[tuple[int, str]] = []
    allowed_seen: set[tuple[str, str]] = set()
    calls = 0
    for number, line, code in invocations(path, text):
        for target in invoked_targets(code):
            calls += 1
            if target in targets:
                continue
            wording = ALLOWED_MENTIONS.get((path, target))
            if wording is not None and wording in line:
                allowed_seen.add((path, target))
                continue
            dead.append((number, target))
    return dead, allowed_seen, calls


def scan(
    root: Path,
) -> tuple[set[str], list[tuple[str, int, str]], set[tuple[str, str]], int, list[str]]:
    """Return targets, dead invocations, admitted mentions, call count, unreadable."""
    targets = makefile_targets(root / "Makefile")
    dead: list[tuple[str, int, str]] = []
    allowed_seen: set[tuple[str, str]] = set()
    calls = 0
    unreadable: list[str] = []
    for path in tracked_text_files(root):
        if path == _SELF:
            continue
        try:
            text = (root / path).read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        except (OSError, UnicodeDecodeError) as err:
            unreadable.append(f"{path} ({type(err).__name__})")
            continue
        file_dead, file_seen, file_calls = check_text(path, text, targets)
        dead.extend((path, number, target) for number, target in file_dead)
        allowed_seen |= file_seen
        calls += file_calls
    return targets, dead, allowed_seen, calls, unreadable


@pytest.mark.parametrize(
    ("path", "text", "expected"),
    [
        # prose and markup outside code contexts
        ("README.md", "Please make sure the docs build.", []),
        ("README.md", "make lint", []),
        ("custom_components/x.py", "make lint", []),
        ("README.md", "`make`", []),
        ("README.md", "`make <target>`", []),
        ("README.md", "`make_config_entry()`", []),
        # inline code spans
        ("README.md", "Run `make lint` before pushing.", ["lint"]),
        ("custom_components/x.py", "# Run `make lint` first.", ["lint"]),
        ("custom_components/x.py", "Run ``make lint`` first.", ["lint"]),
        ("README.md", "`make lint && make test-ha`", ["lint", "test-ha"]),
        ("README.md", "`make lint&&make test-ha 'x`", ["lint", "test-ha"]),
        ("README.md", "`make lint or make test-ha`", ["lint", "or", "test-ha"]),
        ("README.md", "a `span\nends` here, then `make lint`", ["lint"]),
        ("README.md", "`make\nlint`", ["lint"]),
        ("README.md", "an open ` tick\n\nthen `make lint`", ["lint"]),
        ("README.md", "`CI=1 make lint`", ["lint"]),
        ("README.md", "`make test-single TEST=tests/x.py`", ["test-single"]),
        ("README.md", "`poetry run make lint`", []),
        # options
        ("README.md", "`make -C other build`", []),
        ("README.md", "`make --file=other.mk build`", []),
        ("README.md", "`make -j lint`", ["lint"]),
        ("README.md", "`make -j 4 lint`", ["lint"]),
        ("README.md", "`make --jobs 4 lint`", ["lint"]),
        ("README.md", "`make -o lint test-ha`", ["test-ha"]),
        # whole lines in fences, scripts, workflows and the Makefile
        ("README.md", "```bash\nmake lint -j4 PYTEST_ARGS=-x\n```", ["lint"]),
        ("README.md", "```bash\n$ make test-ha && echo ok\n```", ["test-ha"]),
        ("README.md", "```bash\ncd sub && make lint\n```", ["lint"]),
        ("README.md", "```bash\nmake lint 2>&1 | tee lint.log\n```", ["lint"]),
        ("README.md", "```bash\nmake lint > lint.log\n```", ["lint"]),
        ("script/run.sh", "  make test-ha && echo ok", ["test-ha"]),
        ("script/run.sh", "make -j$(nproc) lint", ["lint"]),
        ("script/run.sh", "make -j$(echo $(nproc)) lint", ["lint"]),
        ("script/run.sh", "make -j$(( $(nproc)+1 )) lint", ["lint"]),
        ("script/run.sh", "# note \\\nmake lint", ["lint"]),
        ("README.md", "```bash\nmake lint \\\n```\n\n```bash\necho hi\n```", ["lint"]),
        ("README.md", "```{.bash}\nmake lint\n```", ["lint"]),
        ("README.md", "```bash,ignore\nmake lint\n```", ["lint"]),
        ("README.md", "```shell-session\n$ make lint\n```", ["lint"]),
        (
            ".github/workflows/ci.yml",
            "  - run: make lint \\\n  - run: echo hi",
            ["lint"],
        ),
        (
            "script/run.sh",
            "make lint \\\n  test-ha\nmake clean",
            ["lint", "test-ha", "clean"],
        ),
        ("README.md", "```text\nmake sure it works\n```", []),
        ("README.md", "```console\n$ make lint\n```", ["lint"]),
        ("README.md", "```make nosuch``` x\nmake sure, run `make lint`", ["lint"]),
        (".github/workflows/ci.yml", "description: |\n  make sure you read this\n", []),
        (".github/workflows/ci.yml", "      - run: make lint", ["lint"]),
        (
            ".github/workflows/ci.yml",
            "      - run: |\n          make lint\n\n          make test-ha\n"
            "      - name: x\n        make sure",
            ["lint", "test-ha"],
        ),
        (".github/workflows/ci.yml", "        run: make preflight", ["preflight"]),
        ("Makefile", "\t@$(MAKE) lint", ["lint"]),
        ("Makefile", "\t@make lint", ["lint"]),
        ("Makefile", '\t@echo "[make lint] running"', []),
        # fences close only with the same character and at least the same length
        ("README.md", "```\n~~~\nmake lint\n```\nmake test-ha", ["lint"]),
        (
            "README.md",
            "````\n```\nmake lint\n```\nmake test-ha\n````",
            ["lint", "test-ha"],
        ),
        ("README.md", "```\nmake lint\n``` x\nmake test-ha\n```", ["lint", "test-ha"]),
        # RST is read for inline code only
        ("docs/x.rst", "Title\n~~~~~\n\nmake lint", []),
        ("docs/x.rst", "Run ``make lint``.", ["lint"]),
    ],
)
def test_code_context_rule(path: str, text: str, expected: list[str]) -> None:
    """Only code spans, and whole lines in scripts and fences, are read."""
    found = [
        target
        for _, _, code in invocations(path, text)
        for target in invoked_targets(code)
    ]
    assert found == expected


def test_makefile_reader_skips_non_rules(tmp_path: Path) -> None:
    """Assignments, define blocks, recipes, patterns and variables are no targets."""
    makefile = tmp_path / "Makefile"
    makefile.write_text(
        "FOO ::= 1\n"
        "BAR := 2\n"
        "BAZ ?= 3\n"
        "QUX += 4\n"
        "CMD != echo\n"
        "export ENV = 5\n"
        "ifeq ($(A),b:c)\n"
        "vpath %.c a:b\n"
        "grouped one &: dep\n"
        "URL = http://localhost:8123\n"
        "%.o: %.c\n"
        "$(V)/bin/a: b\n"
        ".PHONY: real\n"
        "real: dep\n"
        "two names: dep\n"
        "define BLOCK\n"
        "inside: rule\n"
        "endef\n"
        "\trecipe: line\n"
        "after: real\n",
        encoding="utf-8",
    )
    assert makefile_targets(makefile) == {
        "real",
        "two",
        "names",
        "after",
        "grouped",
        "one",
    }


def test_scan_reports_unreadable_files_and_ignores_git_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unreadable tracked file fails loudly; a caller's ``GIT_DIR`` is ignored."""
    (tmp_path / "Makefile").write_text("lint:\n", encoding="utf-8")
    (tmp_path / "README.md").write_text("Run `make nosuch`.\n", encoding="utf-8")
    (tmp_path / "broken.md").write_bytes(b"\xff\xfe `make lint`\n")
    for command in (["git", "init", "-q"], ["git", "add", "-A"]):
        subprocess.run(command, cwd=tmp_path, check=True)  # noqa: S603
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "not-a-repository"))
    targets, dead, _, calls, unreadable = scan(tmp_path)
    assert targets == {"lint"}
    assert dead == [("README.md", 1, "nosuch")]
    assert calls == 1
    assert unreadable == ["broken.md (UnicodeDecodeError)"]


def test_allowed_mention_requires_its_wording() -> None:
    """A pinned target is admitted only on the line that carries the wording."""
    text = (
        f"* `make test-stubs` and `make test-deps` {_REMOVED_WITH_POETRY}.\n"
        "* Run `make test-stubs` after cloning.\n"
    )
    dead, seen, calls = check_text("AGENTS.md", text, {"install-dev"})
    assert calls == 3
    assert dead == [(2, "test-stubs")]
    assert seen == {("AGENTS.md", "test-stubs"), ("AGENTS.md", "test-deps")}


@functools.cache
def _repo_scan() -> tuple[
    set[str], list[tuple[str, int, str]], set[tuple[str, str]], int, list[str]
]:
    """Scan ``REPO_ROOT`` once for both repository tests."""
    return scan(REPO_ROOT)


def test_makefile_defines_targets() -> None:
    """An empty target set would make every other assertion meaningless."""
    targets = makefile_targets(REPO_ROOT / "Makefile")
    assert targets, (
        f"No target was read from {REPO_ROOT / 'Makefile'}; the rule reader is "
        "broken or the Makefile moved, so nothing was checked."
    )
    assert "install-dev" in targets, sorted(targets)


def test_no_dead_make_targets_in_docs_and_scripts() -> None:
    """Every ``make <target>`` in a code context names a defined target."""
    targets, dead, _, calls, unreadable = _repo_scan()
    assert targets, "Makefile yielded no targets; nothing was checked."
    assert calls > 0, "No make invocation was found; the scanner reads nothing."
    assert not unreadable, "These tracked files could not be read:\n" + "\n".join(
        f"  {entry}" for entry in unreadable
    )
    assert not dead, (
        "These make invocations name targets the root Makefile does not define "
        f"(defined: {', '.join(sorted(targets))}):\n"
        + "\n".join(f"  {path}:{line}: make {target}" for path, line, target in dead)
        + "\nUpdate the instruction, or pin a deliberate mention in ALLOWED_MENTIONS."
    )


def test_allowed_mentions_are_still_present() -> None:
    """A pinned mention that no longer occurs must leave ``ALLOWED_MENTIONS``."""
    _, _, allowed_seen, _, _ = _repo_scan()
    stale = sorted(set(ALLOWED_MENTIONS) - allowed_seen)
    assert not stale, (
        "ALLOWED_MENTIONS pins mentions that no longer occur; remove them:\n"
        + "\n".join(f"  {path}: make {target}" for path, target in stale)
    )
