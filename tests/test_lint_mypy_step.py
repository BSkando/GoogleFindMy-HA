# tests/test_lint_mypy_step.py
"""Behavioural tests for the ``Run mypy (strict)`` step of ``ci.yml``.

The step type-checks two targets in sequence: the integration package
(``-p custom_components.googlefindmy``) and the test suite (``-p tests``).
Both runs must always execute, both outputs must land in ``mypy_output.txt``,
and the step must fail as soon as either run fails. An earlier form wrapped
both runs in one ``{ ...; ...; } | tee`` group; ``${PIPESTATUS[0]}`` then held
the status of the group, which is the status of its LAST command, so a failing
package run behind a clean test run left the step green.

These tests execute the REAL ``run`` block, extracted with ``yaml.safe_load``
and started as ``bash -e`` like the GitHub runner does for ``run`` steps without
an explicit ``shell``. ``poetry`` is a stub on ``PATH`` that prints one marker
line per target and exits with a chosen code. Reach, stated so it is not
mistaken for more: the stub proves how the step passes the exit codes on, not
how mypy itself behaves.
"""

from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "ci.yml"
_STEP_NAME = "Run mypy (strict)"
_STEP_IF = "matrix.check == 'mypy'"

_MYPY = "run mypy --install-types --non-interactive --strict"
_PACKAGE_CALL = f"{_MYPY} -p custom_components.googlefindmy"
_TESTS_CALL = f"{_MYPY} -p tests"
_PACKAGE_MARKER = "STUB-MYPY package"
_TESTS_MARKER = "STUB-MYPY tests"

_POETRY_STUB = f"""#!/usr/bin/env bash
case "$*" in
  "{_PACKAGE_CALL}")
    echo "$*" >> "$STUB_POETRY_LOG"
    echo "{_PACKAGE_MARKER}"
    exit "$STUB_PACKAGE_RC" ;;
  "{_TESTS_CALL}")
    echo "$*" >> "$STUB_POETRY_LOG"
    echo "{_TESTS_MARKER}"
    exit "$STUB_TESTS_RC" ;;
  *)
    printf 'poetry %s\\n' "$*" >> "$STUB_UNEXPECTED_LOG"
    exit 99 ;;
esac
"""


def _mypy_step() -> dict[str, Any]:
    """Return the one mypy step of the ``lint`` job; refuse anything else."""

    workflow = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    job = workflow["jobs"]["lint"]
    # The test starts the block as `bash -e`, the runner's form for a run step
    # without an explicit shell; any shell setting would make that a different
    # interpreter from the one the runner uses.
    for scope, owner in (("workflow", workflow), ("job", job)):
        shell = (owner.get("defaults") or {}).get("run", {}).get("shell")
        assert shell is None, f"{scope} sets defaults.run.shell={shell!r}"
    steps = [step for step in job["steps"] if step.get("name") == _STEP_NAME]
    assert len(steps) == 1, (
        f"expected exactly one step named {_STEP_NAME!r}, got {len(steps)}"
    )
    step: dict[str, Any] = steps[0]
    assert "shell" not in step, f"the step sets shell={step['shell']!r}"
    assert step["if"] == _STEP_IF
    return step


def _mypy_run_block() -> str:
    """Return the ``run`` script of the mypy step, checked for runnability."""

    step = _mypy_step()
    run = step["run"]
    assert isinstance(run, str)
    # An expression would be substituted by the runner before bash sees the
    # script; executing it verbatim here would test something else.
    assert "${{" not in run, "the run block must not contain expressions"
    assert "env" not in step, "the step's env changed; pass it to the stub run"
    return run


def _log_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


@dataclass(frozen=True)
class _StepRun:
    """Outcome of one execution of the mypy step."""

    returncode: int
    output: str
    poetry_calls: list[str]
    unexpected_calls: list[str]
    github_output: list[str]
    mypy_output: str


def _run_step(tmp_path: Path, *, package_rc: int, tests_rc: int) -> _StepRun:
    """Execute the real mypy step under ``bash -e`` against the stub."""

    bash = shutil.which("bash")
    # Deliberately no skip: a skipped behaviour test would be a silent pass.
    assert bash is not None, "bash is required to execute the workflow step"

    script = tmp_path / "step.sh"
    script.write_text(_mypy_run_block(), encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "poetry"
    stub.write_text(_POETRY_STUB, encoding="utf-8")
    stub.chmod(0o755)
    workdir = tmp_path / "work"
    workdir.mkdir()
    poetry_log = tmp_path / "poetry.log"
    unexpected_log = tmp_path / "unexpected.log"
    github_output = tmp_path / "github_output"

    # Built from scratch, without os.environ, so nothing from the calling shell
    # or from CI (BASH_ENV, GITHUB_*) reaches the step.
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "HOME": str(tmp_path),
        "GITHUB_OUTPUT": str(github_output),
        "STUB_PACKAGE_RC": str(package_rc),
        "STUB_TESTS_RC": str(tests_rc),
        "STUB_POETRY_LOG": str(poetry_log),
        "STUB_UNEXPECTED_LOG": str(unexpected_log),
    }
    proc = subprocess.run(
        [bash, "-e", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
        check=False,
        cwd=workdir,
        env=env,
    )
    mypy_output = workdir / "mypy_output.txt"
    return _StepRun(
        returncode=proc.returncode,
        output=proc.stdout.decode("utf-8", errors="replace"),
        poetry_calls=_log_lines(poetry_log),
        unexpected_calls=_log_lines(unexpected_log),
        github_output=_log_lines(github_output),
        mypy_output=(
            mypy_output.read_text(encoding="utf-8") if mypy_output.exists() else ""
        ),
    )


@pytest.mark.parametrize(
    ("package_rc", "tests_rc"),
    [(0, 0), (1, 0), (0, 1), (1, 1), (2, 0), (1, 2)],
)
def test_mypy_step_fails_when_either_run_fails(
    tmp_path: Path, package_rc: int, tests_rc: int
) -> None:
    """Both runs execute in order; the step reports the worse exit code."""

    run = _run_step(tmp_path, package_rc=package_rc, tests_rc=tests_rc)
    expected = max(package_rc, tests_rc)

    assert not run.unexpected_calls, run.output
    assert run.poetry_calls == [_PACKAGE_CALL, _TESTS_CALL], run.output
    assert run.returncode == expected, run.output
    assert run.github_output == [f"exit_code={expected}"], run.output


@pytest.mark.parametrize(("package_rc", "tests_rc"), [(1, 0), (0, 1)])
def test_mypy_output_keeps_both_runs(
    tmp_path: Path, package_rc: int, tests_rc: int
) -> None:
    """``mypy_output.txt`` and the log carry the output of both runs."""

    run = _run_step(tmp_path, package_rc=package_rc, tests_rc=tests_rc)

    assert run.mypy_output.splitlines() == [_PACKAGE_MARKER, _TESTS_MARKER], (
        run.mypy_output
    )
    assert _PACKAGE_MARKER in run.output.splitlines(), run.output
    assert _TESTS_MARKER in run.output.splitlines(), run.output
