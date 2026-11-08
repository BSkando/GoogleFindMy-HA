# tests/test_lint_summary_step.py
"""Behavioural tests for the ``Write lint summary`` step of ``ci.yml``.

The step writes one verdict per lint check into ``$GITHUB_STEP_SUMMARY``. A
clean ruff, mypy or bandit run must show its green line, a failing run its
red or warning line. An earlier form counted with ``grep -c ... || echo "0"``;
``grep -c`` already prints ``0`` when nothing matches and then exits 1, so the
fallback added a second ``0`` and the comparison with ``"0"`` failed: green
mypy and bandit runs were reported as errors. The ruff branch counted every
output line, so the ``All checks passed!`` line of a clean run did the same.
Each branch now decides by the exit code of its step; the count only labels a
failure, and a run that fails without a counted line still shows red.

These tests execute the REAL ``run`` block under ``bash -e``. Unlike
``tests/test_release_preview_step.py``, the block contains ``${{ ... }}``
expressions, which the runner substitutes before bash starts. The tests do the
same textually, and only for the expressions in ``_EXPRESSIONS``; any other
expression fails the test instead of reaching bash verbatim. Reach, stated so
it is not mistaken for more: the fixture outputs are shortened examples of the
tools' output, and the textual substitution stands in for the runner's.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "ci.yml"
_STEP_NAME = "Write lint summary"
_STEP_IF = "always()"
_EXPRESSIONS = {
    "matrix.check",
    "steps.ruff.outputs.exit_code",
    "steps.mypy.outputs.exit_code",
    "steps.bandit.outputs.exit_code",
    "steps.precommit.outputs.exit_code",
}
_STEP_IDS = ("ruff", "mypy", "bandit", "precommit")

_RUFF_CLEAN = "All checks passed!\n42 files already formatted\n"
_RUFF_DIRTY = "F401 `os` imported but unused\nFound 1 error.\n"
_MYPY_CLEAN = (
    "Success: no issues found in 141 source files\n"
    "Success: no issues found in 487 source files\n"
)
_MYPY_DIRTY = (
    "custom_components/googlefindmy/a.py:1: error: Missing return statement\n"
    "custom_components/googlefindmy/b.py:2: error: Name 'x' is not defined\n"
    "Found 2 errors in 2 files (checked 141 source files)\n"
    "Success: no issues found in 487 source files\n"
)
_MYPY_CRASH = "pyproject.toml: [mypy]: invalid option\nmypy exited unexpectedly\n"
_MYPY_CRASH_LINE = "❌ **mypy failed without reporting a type error:**"
_BANDIT_CLEAN = "Test results:\n\tNo issues identified.\n"
_BANDIT_DIRTY = (
    "Test results:\n>> Issue: [B105:hardcoded_password_string] Possible\n"
    "   Severity: Low   Confidence: Medium\n"
)
_BANDIT_CRASH = "[main]\tERROR\tunable to load config pyproject.toml\n"
_BANDIT_CRASH_LINE = "❌ **bandit failed without reporting an issue:**"


def _summary_step() -> dict[str, Any]:
    """Return the one summary step of the ``lint`` job; refuse anything else."""

    workflow = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    job = workflow["jobs"]["lint"]
    # The test starts the block as `bash -e`, the runner's form for a run step
    # without an explicit shell.
    for scope, owner in (("workflow", workflow), ("job", job)):
        shell = (owner.get("defaults") or {}).get("run", {}).get("shell")
        assert shell is None, f"{scope} sets defaults.run.shell={shell!r}"
    steps = [step for step in job["steps"] if step.get("name") == _STEP_NAME]
    assert len(steps) == 1, (
        f"expected exactly one step named {_STEP_NAME!r}, got {len(steps)}"
    )
    step: dict[str, Any] = steps[0]
    assert "shell" not in step, f"the step sets shell={step['shell']!r}"
    assert "env" not in step, "the step's env changed; pass it to the run"
    assert step["if"] == _STEP_IF
    return step


def _summary_block(check: str, exit_codes: dict[str, str]) -> str:
    """Return the summary script with its expressions substituted."""

    run = _summary_step()["run"]
    assert isinstance(run, str)
    values = {"matrix.check": check}
    for step_id in _STEP_IDS:
        values[f"steps.{step_id}.outputs.exit_code"] = exit_codes.get(step_id, "")
    for expression in _EXPRESSIONS:
        run = run.replace("${{ " + expression + " }}", values[expression])
    unknown = re.findall(r"\$\{\{.*?\}\}", run)
    assert not unknown, (
        f"the summary uses expressions the test does not know: {unknown}"
    )
    return run


def _run_summary(
    tmp_path: Path,
    check: str,
    output: str,
    exit_codes: dict[str, str] | None = None,
) -> str:
    """Execute the summary for ``check`` and return the written summary."""

    bash = shutil.which("bash")
    # Deliberately no skip: a skipped behaviour test would be a silent pass.
    assert bash is not None, "bash is required to execute the workflow step"

    script = tmp_path / "step.sh"
    script.write_text(_summary_block(check, exit_codes or {}), encoding="utf-8")
    workdir = tmp_path / "work"
    workdir.mkdir()
    (workdir / f"{check}_output.txt").write_text(output, encoding="utf-8")
    summary = tmp_path / "step_summary.md"
    proc = subprocess.run(
        [bash, "-e", str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
        check=False,
        cwd=workdir,
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(tmp_path),
            "GITHUB_STEP_SUMMARY": str(summary),
        },
    )
    out = proc.stdout.decode("utf-8", errors="replace")
    assert proc.returncode == 0, out
    return summary.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("check", "output", "exit_codes", "expected"),
    [
        ("ruff", _RUFF_CLEAN, {"ruff": "0"}, "✅ **No issues found**"),
        ("mypy", _MYPY_CLEAN, {"mypy": "0"}, "✅ **No type errors found**"),
        ("bandit", _BANDIT_CLEAN, {"bandit": "0"}, "✅ **No security issues found**"),
    ],
    ids=["ruff", "mypy", "bandit"],
)
def test_summary_reports_clean_runs_as_clean(
    tmp_path: Path,
    check: str,
    output: str,
    exit_codes: dict[str, str],
    expected: str,
) -> None:
    """A clean run gets its green line and no red or warning line."""

    summary = _run_summary(tmp_path, check, output, exit_codes)

    assert expected in summary.splitlines(), summary
    assert "❌" not in summary, summary
    assert "⚠️" not in summary, summary


@pytest.mark.parametrize(
    ("check", "output", "exit_codes", "expected"),
    [
        ("ruff", _RUFF_DIRTY, {"ruff": "1"}, "❌ **Issues found:**"),
        ("mypy", _MYPY_DIRTY, {"mypy": "1"}, "❌ **2 type error(s) found:**"),
        (
            "bandit",
            _BANDIT_DIRTY,
            {"bandit": "1"},
            "⚠️ **1 potential security issue(s) found:**",
        ),
        # A crash prints no counted line; the exit code still makes it red.
        ("mypy", _MYPY_CRASH, {"mypy": "2"}, _MYPY_CRASH_LINE),
        ("bandit", _BANDIT_CRASH, {"bandit": "2"}, _BANDIT_CRASH_LINE),
        # A missing exit code (step cancelled mid-run) never reads as clean.
        ("ruff", _RUFF_CLEAN, {}, "❌ **Issues found:**"),
        ("mypy", _MYPY_CLEAN, {}, _MYPY_CRASH_LINE),
        ("bandit", _BANDIT_CLEAN, {}, _BANDIT_CRASH_LINE),
    ],
    ids=[
        "ruff",
        "mypy",
        "bandit",
        "mypy-crash",
        "bandit-crash",
        "ruff-no-exit-code",
        "mypy-no-exit-code",
        "bandit-no-exit-code",
    ],
)
def test_summary_reports_findings(
    tmp_path: Path,
    check: str,
    output: str,
    exit_codes: dict[str, str],
    expected: str,
) -> None:
    """A failing run gets its red or warning line and the output."""

    summary = _run_summary(tmp_path, check, output, exit_codes)

    assert expected in summary.splitlines(), summary
    assert "✅" not in summary, summary
    assert output.splitlines()[0] in summary.splitlines(), summary
