#!/usr/bin/env python3
# script/local_verify.py
"""Run the canonical local verification commands for Google Find My.

The script has two modes:

* Without arguments it runs ``ruff format --check`` followed by
  pytest-homeassistant-custom-component and the pycache cleanup, exactly as
  ``README.md`` and ``AGENTS.md`` describe it. The commands are the same as
  before; one thing did change, and it changed for both modes: they now run
  with the repository root as their working directory. Called from a
  subdirectory, ``ruff format --check`` used to inspect that subtree and now
  inspects the repository, and a relative path in ``--pytest-args`` is now
  resolved from the root.
* ``--all`` runs the full local preflight S1 to S8 and prints a report. Every
  stage reports one of ``OK``, ``FAILED``, ``NOTE`` or ``NOT CHECKED`` together
  with a reason and the tool version it used. A stage that could not run must
  never look like a stage that passed, and a version mismatch against the CI
  pins must be visible at once rather than after a red pipeline.

Preview without running anything expensive: ``--all --skip-pytest`` reports S5
and S6 as ``NOT CHECKED`` and still prints the static stages.

If command-line shims such as ``pytest-homeassistant-custom-component`` are
unavailable, use the module invocation fallbacks described in ``AGENTS.md``
(for example, ``python -m pytest_homeassistant_custom_component``) to mirror the
expected checks.
"""

from __future__ import annotations

import argparse
import hashlib
import shlex
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    # Direct invocation (`python script/local_verify.py`) puts `script/` on the
    # path, not the repository root, so the package import below fails. Both
    # documented callers use exactly that form, so the bootstrap belongs here.
    sys.path.insert(0, str(REPO_ROOT))

from script import diff_coverage  # noqa: E402
from script.clean_pycache import clean_pycache, format_summary  # noqa: E402

RUFF_FORMAT_SCRIPT = (
    Path(__file__).resolve().parent / "precommit_hooks" / "ruff_format.py"
)

STATUS_OK = "OK"
STATUS_FAILED = "FAILED"
STATUS_NOTE = "NOTE"
STATUS_SKIPPED = "NOT CHECKED"

# Versions pinned by .pre-commit-config.yaml. A local tool that is newer can
# report findings the CI will not, and the reverse; the report names both.
RUFF_PIN = "v0.17.3"
CODESPELL_PIN = "v2.4.3"

# Version queries and git calls are short. The suite is not, so only the
# capturing helper carries a deadline; a hanging tool must not hang the report.
CAPTURE_TIMEOUT_SECONDS = 120

# Exit statuses of --all. A stage that should have run and did not gets its own
# value, for the same reason diff_coverage separates "nothing to measure" from
# "measured and fine": a caller reads the status, not the prose.
EXIT_STAGE_FAILED = 1
EXIT_STAGE_ABSENT = 3

# codespell exits 65 when it found something. Any other non-zero status means
# the tool itself did not run.
CODESPELL_FINDINGS_STATUS = 65

# Sentinel of _tool_version: the tool did not answer, so it is not there.
UNKNOWN_VERSION = "version unknown"


@dataclass(frozen=True)
class StageResult:
    """Outcome of a single preflight stage.

    ``expected_absent`` separates the two reasons a stage can be NOT CHECKED.
    S7 and S8 cannot run on a developer machine at all, and demanding a non-zero
    exit for them would make every local run red, which is how a criterion gets
    ignored. A stage that was supposed to run and did not is a different matter
    and must reach the caller through the exit status, not only through prose.
    """

    key: str
    title: str
    status: str
    detail: str
    expected_absent: bool = False


def _echo_command(command: Sequence[str]) -> None:
    """Log the command in a shell-friendly format."""

    printable = shlex.join(command)
    print(f"+ {printable}")


def _run_command(command: Sequence[str]) -> int:
    """Execute a subprocess command and return its exit status."""

    _echo_command(command)
    # cwd is pinned: `ruff check .` in an inherited working directory finds no
    # Python file and exits 0, which would be a green stage that looked at
    # nothing. The documented call form works from anywhere.
    result = subprocess.run(command, check=False, cwd=str(REPO_ROOT))
    if result.returncode != 0:
        print(f"Command exited with status {result.returncode}")
    return result.returncode


def _build_ruff_command() -> list[str]:
    """Return the Ruff format --check command using repository defaults."""

    return [sys.executable, str(RUFF_FORMAT_SCRIPT), "--check"]


def _build_pytest_command(pytest_args: Sequence[str] | None) -> list[str]:
    """Return the HA-pytest command respecting optional overrides."""

    command = [
        sys.executable,
        "-m",
        "pytest_homeassistant_custom_component",
    ]
    if pytest_args:
        command.extend(pytest_args)
    return command


def _capture(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Run a command quietly and return the completed process."""

    try:
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=CAPTURE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            list(command), returncode=124, stdout="", stderr="timed out"
        )


def _capture_raw(command: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    """Run a command quietly and return its output untranslated.

    ``_capture`` reads through the text layer, which turns every carriage
    return into a newline; that is right for prose and wrong for a ``-z``
    path list, where the bytes are the name. A file called ``cr\\rx.py`` came
    back as ``cr\\nx.py``, and the digest clamp recorded a file that does not
    exist. Path listings come through here, everything read as prose through
    ``_capture``.
    """

    try:
        return subprocess.run(
            command,
            check=False,
            capture_output=True,
            cwd=str(REPO_ROOT),
            timeout=CAPTURE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            list(command), returncode=124, stdout=b"", stderr=b"timed out"
        )


def _tool_version(interpreter: str, module: str) -> str:
    """Return the reported version of a module-invoked tool."""

    completed = _capture([interpreter, "-m", module, "--version"])
    text = completed.stdout.strip() or completed.stderr.strip()
    if completed.returncode != 0 or not text:
        return UNKNOWN_VERSION
    return text.splitlines()[0].strip()


MISSING_DIGEST = "<missing>"


def _digest_of(paths: Sequence[str]) -> dict[str, str]:
    """Return a SHA-256 digest per path, relative to the repository.

    A path that does not exist is recorded as ``MISSING_DIGEST`` rather than
    left out. Leaving it out made a deletion during the run invisible: a file
    clean at the start is absent from the first snapshot, its deletion puts it
    on the second path list, and dropping it there again made both snapshots
    agree on "nothing changed" while the tree the suite ran on had lost a file.
    """

    digests: dict[str, str] = {}
    for name in paths:
        candidate = REPO_ROOT / name
        if candidate.is_file():
            digests[name] = hashlib.sha256(candidate.read_bytes()).hexdigest()
        else:
            digests[name] = MISSING_DIGEST
    return digests


def _nul_separated(stdout: bytes) -> list[str]:
    """Split ``-z`` output of git into paths, dropping the trailing empty entry.

    ``-z`` is the only form in which git hands over a path verbatim. Without it
    ``core.quotePath`` (on by default) C-quotes any name with a non-ASCII or
    control byte, ``"t\\303\\274r.py"`` for ``tür.py``, and a consumer that
    hashes or looks up that literal string is measuring a file that does not
    exist. ``script/diff_coverage.py`` decodes that quoting for the diff header
    it parses; here the quoting is avoided at the source instead. The input is
    bytes for the same reason: read as text, a carriage return in a name would
    already have become a newline.
    """

    return [entry.decode("utf-8") for entry in stdout.split(b"\0") if entry]


def _changed_paths() -> list[str]:
    """Return the working-tree paths git reports as changed or untracked.

    ``git diff HEAD`` names tracked files only. A file created in this working
    tree and not yet staged is part of the tree the suite runs on, so the digest
    clamp has to hold it as well; otherwise an edit to a new file during the
    run would go unnoticed, and the clamp would report one file fewer than the
    tree actually changed.
    """

    paths: list[str] = []
    for command in (
        ["git", "diff", "HEAD", "--name-only", "-z"],
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
    ):
        completed = _capture_raw(command)
        if completed.returncode != 0:
            continue
        paths.extend(_nul_separated(completed.stdout))
    return sorted(set(paths))


def _branch_paths(base: str) -> set[str]:
    """Return every path this branch touches, committed or not.

    The digest clamp asks about the working tree, this asks about the branch;
    they are different questions and a single helper for both would answer one
    of them wrongly whenever work is committed or still uncommitted.
    """

    merge_base = _capture(["git", "merge-base", base, "HEAD"])
    paths = set(_changed_paths())
    if merge_base.returncode != 0 or not merge_base.stdout.strip():
        return paths
    completed = _capture_raw(
        ["git", "diff", "--name-only", "-z", f"{merge_base.stdout.strip()}..HEAD"]
    )
    if completed.returncode == 0:
        paths.update(_nul_separated(completed.stdout))
    return paths


def _stage_static(
    key: str, title: str, interpreter: str, command: Sequence[str], module: str
) -> StageResult:
    """Run one static stage and classify its exit status."""

    version = _tool_version(interpreter, module)
    detail = version if version.lower().startswith(module) else f"{module} {version}"
    if version == UNKNOWN_VERSION:
        # The tool did not answer, so it is not installed for this interpreter.
        # Running it anyway produces a non-zero exit and a FAILED stage, and the
        # reader goes looking for lint errors that do not exist.
        return StageResult(
            key, title, STATUS_SKIPPED, f"{module} is not available for {interpreter}"
        )
    if module == "ruff":
        detail += f" (pin {RUFF_PIN.lstrip('v')})"
    if module == "mypy":
        detail += " (scope: [tool.mypy] files, which excludes script/ and tests/)"
    status = STATUS_OK if _run_command(command) == 0 else STATUS_FAILED
    return StageResult(key, title, status, detail)


def _stage_spelling(interpreter: str, base: str) -> StageResult:
    """Run codespell and report findings without ever blocking."""

    version = _tool_version(interpreter, "codespell_lib")
    completed = _capture([interpreter, "-m", "codespell_lib"])
    if completed.returncode not in (0, CODESPELL_FINDINGS_STATUS):
        stderr = completed.stderr.strip()
        last = stderr.splitlines()[-1] if stderr else "no output"
        return StageResult(
            "S4",
            "spelling",
            STATUS_SKIPPED,
            f"codespell did not run (exit {completed.returncode}): {last}",
        )
    findings = [line for line in completed.stdout.splitlines() if line.strip()]
    changed = _branch_paths(base)
    own = [line for line in findings if _normalise_finding_path(line) in changed]
    detail = (
        f"codespell {version} (pin {CODESPELL_PIN.lstrip('v')}); "
        f"{len(findings)} findings, "
        f"{len(own)} of them on the {len(changed)} files this branch touches. "
        f"The CI step carries continue-on-error, so this stage never blocks."
    )
    status = STATUS_OK if not findings else STATUS_NOTE
    return StageResult("S4", "spelling", status, detail)


def _normalise_finding_path(line: str) -> str:
    """Return the repository-relative path a codespell finding line names.

    codespell is invoked without a path argument and therefore walks ".", so
    every finding is printed as "./some/file:12: ...". git reports the same file
    as "some/file". Comparing the two raw makes the "findings on files this
    branch touches" count structurally zero, which is the quietest possible way
    for a counter to look reassuring.
    """

    raw = line.split(":", 1)[0]
    return raw[2:] if raw.startswith("./") else raw


def _run_suite(interpreter: str, xml_path: Path) -> tuple[int, str]:
    """Run the covered suite for one interpreter under a digest clamp."""

    paths = _changed_paths()
    before = _digest_of(paths)
    command = [
        interpreter,
        "-m",
        "pytest",
        # Exactly one -q, and it is this one. [tool.pytest.ini_options] addopts
        # in pyproject.toml carries none on purpose, and -q counts up: a second
        # one makes pytest drop its summary line, so the report this stage is
        # meant to hand a reviewer would name neither the number of tests nor
        # the number of failures. The verdict would stay correct (it reads the
        # exit status) while the evidence quietly disappeared.
        "-q",
        "--no-header",
        "-p",
        "no:cacheprovider",
        "--cov",
        "--cov-report=term",
        f"--cov-report=xml:{xml_path}",
        # No --cov-fail-under here on purpose. [tool.coverage.report] fail_under
        # in pyproject.toml already applies, and tests/test_platinum_compliance
        # clamps that value to the CI flag. A fourth copy of the number would
        # sit outside that clamp and would keep passing locally after the floor
        # was raised everywhere else.
    ]
    returncode = _run_command(command)
    # The path list is collected again: a file that was untouched when the run
    # started and was edited during it would be invisible to the first list.
    after = _digest_of(sorted(set(paths) | set(_changed_paths())))
    if before != after:
        return 1, (
            "source files changed while the suite ran; the run is discarded, "
            "not interpreted (a run measured 77.71% instead of 79.23% this way)"
        )
    return returncode, f"{len(before)} changed files digest-clamped, unchanged"


def _stage_suite(
    interpreters: Sequence[str], report_dir: Path
) -> tuple[StageResult, Path | None]:
    """Run the suite on every requested interpreter, sequentially."""

    details: list[str] = []
    status = STATUS_OK
    reference: Path | None = None
    for index, interpreter in enumerate(interpreters):
        xml_path = report_dir / f"coverage-{index}.xml"
        returncode, note = _run_suite(interpreter, xml_path)
        if returncode != 0:
            status = STATUS_FAILED
        if index == 0 and returncode == 0:
            # The first track, not the first green one: S6 has to name the
            # track it measured, and the documentation names this one.
            reference = xml_path
        details.append(f"{interpreter}: exit {returncode} ({note})")
    detail = "; ".join(details)
    if len(interpreters) == 1:
        detail += (
            "; only one interpreter was given, so this covers a single track - "
            "pass --python twice to cover both"
        )
    return StageResult("S5", "suite and project coverage", status, detail), reference


def _stage_patch_coverage(reference: Path | None, base: str) -> StageResult:
    """Measure patch coverage from the coverage report of the reference track."""

    if reference is None or not reference.is_file():
        return StageResult(
            "S6",
            "patch coverage",
            STATUS_SKIPPED,
            "no coverage report from S5, so there is nothing to intersect",
        )
    returncode = diff_coverage.main(
        [
            "--coverage-xml",
            str(reference),
            "--base",
            base,
            "--repo-root",
            str(REPO_ROOT),
        ]
    )
    status = {
        diff_coverage.EXIT_OK: STATUS_OK,
        diff_coverage.EXIT_BELOW_THRESHOLD: STATUS_NOTE,
        diff_coverage.EXIT_NOT_MEASURABLE: STATUS_SKIPPED,
    }.get(returncode, STATUS_FAILED)
    reason = {
        diff_coverage.EXIT_NOT_MEASURABLE: (
            "no instrumented statement changed, so there is no percentage; "
            "this is not full coverage"
        ),
        diff_coverage.EXIT_BELOW_THRESHOLD: (
            "below the threshold; the uncovered lines are named above"
        ),
    }.get(returncode, "")
    detail = f"diff_coverage exit {returncode}"
    if reason:
        detail += f" - {reason}"
    return StageResult("S6", "patch coverage", status, detail)


def _static_stages(
    interpreter: str, base: str, skip_format: bool = False
) -> list[StageResult]:
    """Return the results of the four static stages S1 to S4."""

    format_stage = (
        StageResult("S1", "format", STATUS_SKIPPED, "--skip-ruff was given")
        if skip_format
        else _stage_static(
            "S1",
            "format",
            interpreter,
            [interpreter, str(RUFF_FORMAT_SCRIPT), "--check"],
            "ruff",
        )
    )
    return [
        format_stage,
        _stage_static(
            "S2", "lint", interpreter, [interpreter, "-m", "ruff", "check", "."], "ruff"
        ),
        _stage_static(
            "S3",
            "types",
            interpreter,
            # --install-types --non-interactive is what AGENTS.md prescribes;
            # without them a fresh environment reports missing stubs as type
            # errors, which is indistinguishable from real ones.
            [
                interpreter,
                "-m",
                "mypy",
                "--strict",
                "--install-types",
                "--non-interactive",
            ],
            "mypy",
        ),
        _stage_spelling(interpreter, base),
    ]


def _module_present(interpreter: str, module: str) -> bool:
    """Return whether an interpreter can import a module."""

    completed = _capture(
        [
            interpreter,
            "-c",
            "import importlib.util,sys;"
            f"sys.exit(0 if importlib.util.find_spec({module!r}) else 1)",
        ]
    )
    return completed.returncode == 0


def _unavailable_stages(interpreter: str) -> list[StageResult]:
    """Return the two stages this preflight does not run, with a measured reason.

    The reason is measured rather than asserted: bandit and pip-audit are dev
    dependencies of this repository, so "not installed" is false on any machine
    that ran `make install-dev`. A report that prints an unverified environment
    claim as fact is defective by the same standard as a green stage that never
    ran.
    """

    present = [
        name
        for name in ("bandit", "semgrep", "pip_audit")
        if _module_present(interpreter, name)
    ]
    scans = (
        f"present but not run by this preflight: {', '.join(present)}; CI runs them"
        if present
        else "bandit, semgrep and pip-audit are not importable here; CI runs them"
    )
    docker = shutil.which("docker")
    manifest = (
        f"hassfest needs Docker; docker is at {docker} but this preflight does "
        "not drive it; CI runs it"
        if docker
        else "hassfest needs Docker, which is not on PATH here; CI runs it"
    )
    return [
        StageResult(
            "S7", "security scans", STATUS_SKIPPED, scans, expected_absent=True
        ),
        StageResult(
            "S8", "manifest (hassfest)", STATUS_SKIPPED, manifest, expected_absent=True
        ),
    ]


def _format_report(results: Sequence[StageResult]) -> str:
    """Return the printable preflight report."""

    lines = ["", "LOCAL PREFLIGHT REPORT", ""]
    for entry in results:
        lines.append(
            f"  {entry.key} {entry.title:<28} {entry.status:<11} {entry.detail}"
        )
    failed = [entry.key for entry in results if entry.status == STATUS_FAILED]
    absent = [entry.key for entry in results if entry.status == STATUS_SKIPPED]
    lines.append("")
    verdict = f"FAILED ({', '.join(failed)})" if failed else "no stage failed"
    if absent:
        verdict += f"; {len(absent)} of {len(results)} stages did not run ({', '.join(absent)})"
    lines.append(f"  VERDICT: {verdict}")
    lines.append(
        "  A stage reported NOT CHECKED did not run; it is not a passing stage."
    )
    return "\n".join(lines)


def run_preflight(args: argparse.Namespace) -> int:
    """Run the full preflight and print the report."""

    interpreters = list(args.python) or [sys.executable]
    # The static stages run on the first named track, not on whatever
    # interpreter happens to execute this file: `make preflight` deliberately
    # does not go through `poetry run`, so sys.executable is the one interpreter
    # that is guaranteed not to be one of the tracks.
    results = _static_stages(interpreters[0], args.base, skip_format=args.skip_ruff)
    if args.skip_pytest:
        reason = "--skip-pytest was given"
        results.append(
            StageResult("S5", "suite and project coverage", STATUS_SKIPPED, reason)
        )
        results.append(_stage_patch_coverage(None, args.base))
    else:
        with tempfile.TemporaryDirectory(prefix="preflight-") as raw_dir:
            report_dir = Path(raw_dir)
            suite_result, reference = _stage_suite(interpreters, report_dir)
            results.append(suite_result)
            results.append(_stage_patch_coverage(reference, args.base))
        directory_count, file_count = clean_pycache()
        print(f"Post-pytest cleanup: {format_summary(directory_count, file_count)}")
    results.extend(_unavailable_stages(interpreters[0]))
    print(_format_report(results))
    if any(entry.status == STATUS_FAILED for entry in results):
        return EXIT_STAGE_FAILED
    if any(
        entry.status == STATUS_SKIPPED and not entry.expected_absent
        for entry in results
    ):
        return EXIT_STAGE_ABSENT
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Return the argument parser for the command."""

    parser = argparse.ArgumentParser(
        description=(
            "Run Ruff format --check followed by pytest-homeassistant-custom-"
            "component using the repository\n"
            "defaults so contributors can quickly mirror the required local"
            " checks. If the CLI entry points are missing, use the module"
            " invocation fallbacks documented in AGENTS.md."
        )
    )
    parser.add_argument(
        "--skip-ruff",
        action="store_true",
        help="Skip running Ruff format --check (not recommended).",
    )
    parser.add_argument(
        "--skip-pytest",
        action="store_true",
        help="Skip running pytest-homeassistant-custom-component.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help=(
            "Run the full local preflight S1 to S8 and print the stage report "
            "instead of the two default commands (default: off)."
        ),
    )
    parser.add_argument(
        "--python",
        action="append",
        default=[],
        help=(
            "Interpreter to run the covered suite with under --all. Repeat it "
            "once per track; the first one supplies the coverage report that "
            "patch coverage is measured from (default: the running interpreter)."
        ),
    )
    parser.add_argument(
        "--base",
        default="origin/main",
        help="Ref whose merge base with HEAD bounds the patch coverage (default: origin/main).",
    )
    parser.add_argument(
        "--pytest-args",
        nargs=argparse.REMAINDER,
        help=(
            "Additional arguments to pass to pytest-homeassistant-custom-component "
            "after the repository defaults. Place this flag last and prefix "
            "pytest options with '--'."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the default checks, or the full preflight when ``--all`` is given."""

    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.all:
        if args.pytest_args is not None:
            parser.error(
                "--pytest-args has no effect under --all: S5 runs the covered "
                "suite with a fixed argument set so its numbers stay comparable"
            )
        return run_preflight(args)

    exit_code = 0

    if not args.skip_ruff:
        ruff_exit = _run_command(_build_ruff_command())
        exit_code = max(exit_code, ruff_exit)

    if not args.skip_pytest:
        pytest_exit = _run_command(_build_pytest_command(args.pytest_args or []))
        exit_code = max(exit_code, pytest_exit)

        directory_count, file_count = clean_pycache()
        summary = format_summary(directory_count, file_count)
        print(f"Post-pytest cleanup: {summary}")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
