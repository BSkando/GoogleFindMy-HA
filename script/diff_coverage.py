#!/usr/bin/env python3
# script/diff_coverage.py
"""Report statement coverage for the lines this branch changed.

Codecov reports a patch status on every pull request; this script answers the
same question locally, without adding ``diff-cover`` as a dependency. It reads a
Cobertura ``coverage.xml`` (the per-line hit list that ``pytest --cov-report=xml``
writes) and ``git diff --unified=0 <base>`` (the changed line numbers, merge base
against the working tree), then intersects the two.

Three decisions keep the number comparable to Codecov rather than merely
plausible:

* **Uninstrumented files are listed, never counted.** ``[tool.coverage.run]
  source`` covers ``custom_components/googlefindmy`` only, so ``tests/``,
  ``script/`` and ``docs/`` have no entry in the XML at all. Counting their
  changed lines as uncovered would report nonsense; dropping them silently would
  make the quotient unexplainable. They get their own counted line instead.
* **The base is the merge base, not a fixed commit.** ``git merge-base
  origin/main HEAD``, so the number does not wander when ``main`` moves. The
  other end of the diff is the working tree rather than ``HEAD``, because the
  coverage report comes from a run over the working tree; both sides of the
  intersection have to describe the same revision.
* **The measurement is statement based.** ``coverage.xml`` also carries a branch
  rate, and the terminal report of ``pytest --cov`` combines both. A patch
  coverage is per line ("executed or not"), so the branch rate is neither used
  nor mixed in. For the same reason the default threshold is the *statement*
  rate of the baseline run and not the combined project percentage.

Preview without a run of your own: point ``--coverage-xml`` at any existing
``coverage.xml`` and ``--base`` at any ref; nothing is written, the report goes
to stdout.

Exit status: ``0`` at or above the threshold, ``2`` below it (the measurement
itself succeeded), ``3`` when no instrumented statement changed at all, and ``1``
when the measurement could not be taken. Status ``3`` exists because "nothing to
measure" must not share an exit code with "measured and fine": a diff that only
touches tests and docs would otherwise report a passing patch coverage. Falling below
the threshold is deliberately not an abort: the report names the uncovered lines
so that the choice between "add a test" and "justify the line" is made at the
line, not here.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from defusedxml import DefusedXmlException, ElementTree

REPO_ROOT = Path(__file__).resolve().parent.parent

# Statement rate of the branch baseline, measured 2026-09-07 on commit e8e7aa16
# (the merge base of this branch): a full run there wrote a coverage.xml with
# line-rate="0.8221". Reproduce it by checking out that commit and reading the
# line-rate attribute of the <coverage> root element after
# `pytest -q --cov --cov-report=xml`.
#
# Two things this number is not. It is not the combined project percentage of
# the same run (79.23), which mixes statements and branches and is a different
# unit. And it is not the population being measured here: the threshold is a
# project-wide rate, the measurement is the rate over changed lines only. The
# comparison is the usual "do not fall below the base coverage" target that
# Codecov applies to a patch, not a like-for-like quotient.
DEFAULT_THRESHOLD = 82.21
DEFAULT_BASE_REF = "origin/main"

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_BELOW_THRESHOLD = 2
EXIT_NOT_MEASURABLE = 3

# A unified diff hunk header splits into exactly a pre-image and a post-image
# part around its "+"; anything else is not a hunk header we can read.
_HUNK_HEADER_PARTS = 2


class MeasurementError(RuntimeError):
    """Raised when the inputs of the measurement cannot be obtained."""


@dataclass(frozen=True)
class FileCoverage:
    """Coverage of the changed lines of a single instrumented file."""

    path: str
    covered: int
    missing: tuple[int, ...]

    @property
    def total(self) -> int:
        """Return the number of changed statements in this file."""

        return self.covered + len(self.missing)


@dataclass(frozen=True)
class DiffCoverage:
    """Result of intersecting a diff with a coverage report."""

    files: tuple[FileCoverage, ...]
    uninstrumented: tuple[tuple[str, int], ...]

    @property
    def covered(self) -> int:
        """Return the number of covered changed statements."""

        return sum(entry.covered for entry in self.files)

    @property
    def total(self) -> int:
        """Return the number of instrumented changed statements."""

        return sum(entry.total for entry in self.files)

    @property
    def uninstrumented_lines(self) -> int:
        """Return the number of changed lines outside the coverage source."""

        return sum(count for _, count in self.uninstrumented)

    @property
    def percent(self) -> float | None:
        """Return the patch coverage, or ``None`` when nothing was measurable."""

        if self.total == 0:
            return None
        return 100.0 * self.covered / self.total


def _repo_relative(filename: str, sources: Sequence[Path], repo_root: Path) -> str:
    """Return the repository-relative path for a Cobertura ``filename``.

    Several ``<source>`` elements are legal, and one of them can resolve to a
    path that merely looks plausible (a source that already ends in the package
    prefix doubles it). An existing file therefore wins over a merely resolvable
    one. If no source resolves under the repository at all, the report belongs
    to a different checkout and the caller has to hear about it rather than see
    every file quietly counted as uninstrumented.
    """

    resolvable: list[str] = []
    for source in sources:
        candidate = source / filename
        try:
            relative = candidate.relative_to(repo_root).as_posix()
        except ValueError:
            continue
        if candidate.is_file():
            return relative
        resolvable.append(relative)
    if resolvable:
        return resolvable[0]
    raise MeasurementError(
        f"coverage entry {filename!r} resolves under none of the declared "
        f"sources within {repo_root}; the report belongs to another checkout"
    )


def parse_coverage_xml(text: str, repo_root: Path) -> dict[str, dict[int, int]]:
    """Map repository-relative paths to their per-line hit counts."""

    try:
        root = ElementTree.fromstring(text)
    except DefusedXmlException as error:
        raise MeasurementError(
            f"coverage.xml declares an entity, which a coverage report "
            f"never needs: {error!r}"
        ) from error
    except ElementTree.ParseError as error:
        raise MeasurementError(f"coverage.xml is not parsable: {error}") from error

    sources = [
        Path(node.text.strip())
        for node in root.findall("./sources/source")
        if node.text and node.text.strip()
    ]
    hits: dict[str, dict[int, int]] = {}
    for class_node in root.iter("class"):
        filename = class_node.get("filename")
        if not filename:
            continue
        path = _repo_relative(filename, sources, repo_root)
        per_file = hits.setdefault(path, {})
        for line_node in class_node.iter("line"):
            number = line_node.get("number")
            hit_count = line_node.get("hits")
            if number is None or hit_count is None:
                continue
            try:
                per_file[int(number)] = int(hit_count)
            except ValueError as error:
                raise MeasurementError(
                    f"coverage entry {filename!r} carries a non-numeric line "
                    f"record (number={number!r}, hits={hit_count!r})"
                ) from error
    return hits


def _hunk_range(header: str) -> tuple[int, int]:
    """Return start line and length of the post-image range of a hunk header."""

    parts = header.split("+", 1)
    if len(parts) != _HUNK_HEADER_PARTS:
        raise MeasurementError(f"unified diff hunk header without a range: {header!r}")
    span = parts[1].split(" ", 1)[0].rstrip("@").strip()
    try:
        if "," in span:
            start_text, count_text = span.split(",", 1)
            return int(start_text), int(count_text)
        return int(span), 1
    except ValueError as error:
        raise MeasurementError(
            f"unified diff hunk header carries no line range: {header!r}"
        ) from error


# git prints a non-ASCII or control byte as a backslash and three octal digits.
_OCTAL_ESCAPE_DIGITS = 3

_C_ESCAPES = {
    "a": b"\a",
    "b": b"\b",
    "f": b"\f",
    "n": b"\n",
    "r": b"\r",
    "t": b"\t",
    "v": b"\v",
    "\\": b"\\",
    '"': b'"',
}


def _unquote_git_path(quoted: str) -> str:
    """Decode a path git printed in its C-style quoted form.

    With the default ``core.quotePath``, a path holding a double quote, a
    backslash, a control character or a non-ASCII byte arrives as
    ``"t\\303\\274r.py"``: surrounded by double quotes, with backslash
    escapes and octal byte values. A space alone does not trigger quoting.
    Stripping the quotes alone leaves the escapes in place, and such a name
    never matches the UTF-8 filename the coverage report carries, so the file
    would be listed as uninstrumented and its changed lines silently dropped.
    """

    raw = bytearray()
    body = quoted[1:-1]
    index = 0
    while index < len(body):
        char = body[index]
        if char != "\\":
            raw.extend(char.encode("utf-8"))
            index += 1
            continue
        index += 1
        if index >= len(body):
            raise MeasurementError(f"truncated escape in quoted diff path {quoted!r}")
        escape = body[index]
        if escape in _C_ESCAPES:
            raw.extend(_C_ESCAPES[escape])
            index += 1
            continue
        octal = body[index : index + _OCTAL_ESCAPE_DIGITS]
        if len(octal) == _OCTAL_ESCAPE_DIGITS and all(
            digit in "01234567" for digit in octal
        ):
            raw.append(int(octal, 8))
            index += _OCTAL_ESCAPE_DIGITS
            continue
        raise MeasurementError(f"unknown escape in quoted diff path {quoted!r}")
    return raw.decode("utf-8")


# The same table read the other way: byte value to escape letter.
_C_ESCAPE_LETTERS = {value[0]: letter for letter, value in _C_ESCAPES.items()}
# Below the first printable byte, from DEL upwards, plus the two that would
# be read as part of the quoting itself.
_FIRST_PRINTABLE, _DELETE = 0x20, 0x7F
_QUOTING_BYTES = frozenset(b'"\\')


def _needs_git_quoting(byte: int) -> bool:
    """Return whether git escapes this byte under the default ``core.quotePath``."""

    return byte < _FIRST_PRINTABLE or byte >= _DELETE or byte in _QUOTING_BYTES


def _quote_git_path(path: str) -> str:
    """Encode a path the way git prints it, the inverse of ``_unquote_git_path``.

    Codex finding on PR #1274: the synthetic diff for untracked files wrote
    the name raw into its ``+++`` header, while the parser expects git's form.
    A name holding a newline was therefore cut in two by ``splitlines`` and its
    hunk counted against a path that does not exist. git leaves a name alone
    unless it holds a double quote, a backslash, a control byte or a non-ASCII
    byte; then it wraps the name in double quotes, prints the C escapes
    ``\\a \\b \\t \\n \\v \\f \\r \\\\ \\"`` for their bytes and every
    other awkward byte as a backslash and three octal digits. A space does not
    trigger quoting; git marks such a header with a trailing tab instead, which
    the parser strips, so none is produced here.
    """

    raw = path.encode("utf-8")
    if not any(_needs_git_quoting(byte) for byte in raw):
        return path
    parts = ['"']
    for byte in raw:
        letter = _C_ESCAPE_LETTERS.get(byte)
        if letter is not None:
            parts.append(f"\\{letter}")
        elif _needs_git_quoting(byte):
            parts.append(f"\\{byte:03o}")
        else:
            parts.append(chr(byte))
    parts.append('"')
    return "".join(parts)


def _diff_path(target: str) -> str | None:
    """Return the repository-relative path a diff header names, if it names one."""

    if target == "/dev/null":
        return None
    # git quotes paths that contain quotes, backslashes, control or non-ASCII
    # bytes; neither the quotes nor the escapes are part of the name.
    if target.startswith('"') and target.endswith('"'):
        target = _unquote_git_path(target)
    return target[2:] if target.startswith(("a/", "b/")) else target


def parse_diff(text: str) -> dict[str, set[int]]:
    """Map file paths to the line numbers a unified diff adds or changes."""

    changed: dict[str, set[int]] = {}
    current: str | None = None
    awaiting_header = False
    for line in text.splitlines():
        # A file section starts at "diff --git" and nowhere else. Content lines
        # carry a "+"/"-" prefix, so a removed "-- note" reaches the parser as
        # "--- note" and an added "++ note" as "+++ note"; anchoring on the
        # "+++" line alone lets such a pair open a phantom file section and
        # swallow every hunk that belongs to the real one. No content line can
        # begin with "diff --git", because it would carry a prefix.
        if line.startswith("diff --git "):
            awaiting_header = True
            current = None
        elif line.startswith("+++ ") and awaiting_header:
            awaiting_header = False
            current = _diff_path(line[4:].strip())
            if current is not None:
                changed.setdefault(current, set())
        elif line.startswith("@@") and current is not None:
            start, count = _hunk_range(line)
            changed[current].update(range(start, start + count))
    return {path: lines for path, lines in changed.items() if lines}


def evaluate(
    changed: Mapping[str, set[int]],
    coverage: Mapping[str, Mapping[int, int]],
) -> DiffCoverage:
    """Intersect changed lines with the coverage report."""

    files: list[FileCoverage] = []
    uninstrumented: list[tuple[str, int]] = []
    for path in sorted(changed):
        lines = changed[path]
        per_file = coverage.get(path)
        if per_file is None:
            uninstrumented.append((path, len(lines)))
            continue
        covered = 0
        missing: list[int] = []
        for line in sorted(lines):
            if line not in per_file:
                continue
            if per_file[line] > 0:
                covered += 1
            else:
                missing.append(line)
        files.append(FileCoverage(path, covered, tuple(missing)))
    return DiffCoverage(tuple(files), tuple(uninstrumented))


def format_report(result: DiffCoverage, threshold: float) -> str:
    """Return the human-facing report for a measurement."""

    lines = ["Patch coverage (statements, changed lines only)", ""]
    for entry in result.files:
        if entry.total == 0:
            lines.append(f"  {entry.path}: no changed statement")
            continue
        share = 100.0 * entry.covered / entry.total
        lines.append(f"  {entry.path}: {entry.covered}/{entry.total} ({share:.2f}%)")
        if entry.missing:
            printable = ", ".join(str(number) for number in entry.missing)
            lines.append(f"    uncovered lines: {printable}")
    if result.uninstrumented:
        lines.append("")
        lines.append(
            "  not instrumented (outside [tool.coverage.run] source, "
            f"{result.uninstrumented_lines} changed lines in "
            f"{len(result.uninstrumented)} files):"
        )
        for path, count in result.uninstrumented:
            lines.append(f"    {path}: {count} changed lines")
    lines.append("")
    percent = result.percent
    if percent is None:
        lines.append(
            "  RESULT: no instrumented statement changed - no percentage exists. "
            "This is not 100%."
        )
        return "\n".join(lines)
    lines.append(
        f"  RESULT: {result.covered}/{result.total} ({percent:.2f}%), "
        f"threshold {threshold:.2f}%"
    )
    if percent + 1e-9 < threshold:
        lines.append(
            "  PATCH COVERAGE BELOW THRESHOLD - the uncovered lines are named "
            "above; decide per line whether to add a test or justify it."
        )
    return "\n".join(lines)


def _run_git(args: Sequence[str], repo_root: Path) -> str:
    """Run a git command inside the repository and return its stdout.

    The output is read as bytes and decoded here, not through ``text=True``:
    the text layer translates every carriage return into a newline, and the
    ``-z`` listing of untracked files carries names verbatim, so a file whose
    name holds ``\\r`` would be looked up under a name that does not exist.
    """

    # The parser decodes git's C-style quoting, so that quoting is pinned here
    # rather than left to the host's ``core.quotePath``. Git quotes control
    # bytes, quotes and backslashes under either setting; ``false`` only
    # prints bytes above ASCII raw. The pin keeps the real header in the same
    # form as the synthetic one from ``_quote_git_path``, so that both take
    # the same path through ``_diff_path``.
    result = subprocess.run(
        ["git", "-c", "core.quotePath=true", *args],
        check=False,
        capture_output=True,
        cwd=repo_root,
    )
    if result.returncode != 0:
        raise MeasurementError(
            f"git {' '.join(args)} failed with status {result.returncode}: "
            f"{result.stderr.decode('utf-8', errors='replace').strip()}"
        )
    return result.stdout.decode("utf-8")


def collect_diff(base_ref: str, repo_root: Path) -> tuple[str, str]:
    """Return the merge base and the unified diff against it."""

    base = _run_git(["merge-base", base_ref, "HEAD"], repo_root).strip()
    if not base:
        raise MeasurementError(f"no merge base between {base_ref} and HEAD")
    # The working tree, not `base..HEAD`. The coverage report this is intersected
    # with comes from a run over the current tree, so a diff that stops at HEAD
    # would describe a different revision: uncommitted lines would be missing
    # from the diff while their line numbers had already shifted the report.
    # Two sides of one intersection have to mean the same revision.
    # The header prefixes are pinned as well: ``_diff_path`` strips ``a/`` and
    # ``b/``, and a host with ``diff.mnemonicPrefix`` or ``diff.noprefix`` set
    # would print ``w/`` or nothing, leaving every file "uninstrumented".
    # ``--no-ext-diff`` keeps a configured ``diff.external`` out of the same way.
    # ``--no-color`` keeps ``color.diff=always`` from putting escape bytes in
    # front of every header, and ``--inter-hunk-context=0`` keeps a host's
    # ``diff.interHunkContext`` from merging separate hunks into one, which
    # would count every untouched line between them as changed.
    diff = _run_git(
        [
            "diff",
            "--unified=0",
            "--no-color",
            "--no-ext-diff",
            "--inter-hunk-context=0",
            "--src-prefix=a/",
            "--dst-prefix=b/",
            base,
        ],
        repo_root,
    )
    return base, diff + _untracked_diff(repo_root)


def _untracked_diff(repo_root: Path) -> str:
    """Return a synthetic unified diff that adds every untracked file in full.

    ``git diff <base>`` describes tracked files only. A file created in this
    working tree and not yet staged is still part of the revision the coverage
    run saw, and the report lists its statements; leaving it out of the diff
    would let the measurement report acceptable patch coverage over a tree
    that is smaller than the one the suite ran on. Only the headers and the
    hunk range are produced, which is all the parser reads.
    """

    listing = _run_git(["ls-files", "--others", "--exclude-standard", "-z"], repo_root)
    sections: list[str] = []
    for relative in listing.split("\0"):
        if not relative:
            continue
        try:
            count = len((repo_root / relative).read_bytes().splitlines())
        except OSError as error:
            raise MeasurementError(
                f"untracked file {relative!r} could not be read: {error}"
            ) from error
        # Quoted exactly as git would print the name, so the parser's decoding
        # step sees the same shape here as in a real ``git diff``.
        old = _quote_git_path(f"a/{relative}")
        new = _quote_git_path(f"b/{relative}")
        sections.append(
            f"diff --git {old} {new}\n--- /dev/null\n+++ {new}\n@@ -0,0 +1,{count} @@\n"
        )
    return "".join(sections)


def _build_parser() -> argparse.ArgumentParser:
    """Return the argument parser for the command."""

    parser = argparse.ArgumentParser(
        description=(
            "Report statement coverage of the lines changed against the merge "
            "base. Reads a Cobertura coverage.xml and git diff --unified=0; "
            "files outside the coverage source are listed, never counted."
        )
    )
    parser.add_argument(
        "--coverage-xml",
        default="coverage.xml",
        help=(
            "Cobertura report of the run over the current tree, which is the "
            "revision the diff is taken against (default: coverage.xml)."
        ),
    )
    parser.add_argument(
        "--base",
        default=DEFAULT_BASE_REF,
        help=f"Ref whose merge base with HEAD is the diff base (default: {DEFAULT_BASE_REF}).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=(
            "Statement percentage below which the run reports status 2 "
            f"(default: {DEFAULT_THRESHOLD}, the statement rate of the baseline run)."
        ),
    )
    parser.add_argument(
        "--repo-root",
        default=str(REPO_ROOT),
        help="Repository root used to resolve coverage source paths (default: this checkout).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Measure patch coverage and print the report."""

    args = _build_parser().parse_args(list(argv) if argv is not None else None)
    repo_root = Path(args.repo_root).resolve()
    xml_path = Path(args.coverage_xml)
    if not xml_path.is_absolute():
        xml_path = repo_root / xml_path
    try:
        if not xml_path.is_file():
            raise MeasurementError(f"coverage report not found: {xml_path}")
        coverage = parse_coverage_xml(xml_path.read_text(encoding="utf-8"), repo_root)
        base, diff_text = collect_diff(args.base, repo_root)
        result = evaluate(parse_diff(diff_text), coverage)
    except MeasurementError as error:
        print(f"diff coverage could not be measured: {error}", file=sys.stderr)
        return EXIT_ERROR

    print(f"base: {base}")
    print(f"coverage report: {xml_path}")
    print(format_report(result, args.threshold))
    percent = result.percent
    if percent is None:
        return EXIT_NOT_MEASURABLE
    if percent + 1e-9 < args.threshold:
        return EXIT_BELOW_THRESHOLD
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
