# tests/test_ci_approval_gate.py
"""Lock the approval gate of the CI workflow.

Pull request runs of ``ci.yml`` pass the job ``approval_gate``, bound to the
environment ``home-assistant-tests``, before any test job starts. Where the
environment has a required reviewer, the run waits for a manual approval, so the
Home Assistant test load only happens when it is wanted; where it has no
protection rules, the gate passes without waiting for approval. Pushes to ``main`` run
without the gate.
The contract is documented in the root ``AGENTS.md``
(bullet "PR CI passes an approval gate").

The gate only works as a whole. Each check below guards one link:

* the gate job exists, is bound to the environment and only runs on pull requests;
* every other job states its condition explicitly. Without that, a skipped gate
  on ``push`` would skip all jobs behind ``hassfest`` (a skipped ancestor skips
  its dependants under the default ``success()``);
* ``hassfest`` accepts a skipped gate only on ``push``, so a future mistake in the
  gate condition cannot let tests run on a pull request without approval;
* ``CI Success`` depends on the gate and fails a pull request run whose gate was
  not approved (second lock behind the required status check);
* one live run per pull request: a push that starts a new run cancels the
  previous one, push runs on ``main`` never share a group;
* the workflow triggers stay ``pull_request`` and ``push`` to ``main``, and the
  workflow sets no ``defaults`` or ``env`` that the check step would inherit.

Repository-neutral: the test checks the shape of ``ci.yml``, not the protection
rules of ``home-assistant-tests``, so it holds in every repository that carries
the workflow, with or without a required reviewer.

Conditions are compared as whole expressions (whitespace removed), not as
substrings: an appended ``|| always()`` or ``|| github.event_name == ...`` keeps
every substring but disables the gate.

``CI_YML_PATH`` overrides the workflow path; it exists for red probes against an
older or mutated copy of the file.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"

GATE = "approval_gate"
SUMMARY = "ci-success"
HASSFEST_CONDITION = (
    "${{ !cancelled() && (needs.approval_gate.result == 'success' || "
    "(github.event_name == 'push' && needs.approval_gate.result == 'skipped')) }}"
)
GATE_CHECK = '[[ "$EVENT" == "pull_request" && "$GATE" != "success" ]]'
CONCURRENCY_GROUP = (
    "${{ github.event_name == 'pull_request' && "
    "format('ci-pr-{0}', github.event.pull_request.number) || "
    "format('ci-run-{0}', github.run_id) }}"
)
# Keys outside these sets (continue-on-error, a step-level if, an inherited shell) could let
# a rejected run pass, so the jobs and the check step that carry the gate allow no others.
# Adding a key on purpose: first make sure it cannot turn a rejected run into success,
# then extend the set here.
# YAML reads the key on: as True.
WORKFLOW_KEYS = {"name", True, "permissions", "concurrency", "jobs"}
WORKFLOW_TRIGGERS = {"pull_request": None, "push": {"branches": ["main"]}}
GATE_JOB_KEYS = {"name", "if", "runs-on", "environment", "permissions", "steps"}
SUMMARY_JOB_KEYS = {"name", "runs-on", "needs", "if", "steps"}
CHECK_STEP_KEYS = {"name", "id", "shell", "env", "run"}
GATE_BLOCK = [
    f"if {GATE_CHECK}; then",
    'echo "CI approval gate not approved (result: $GATE)"',
    'echo "passed=false" >> "$GITHUB_OUTPUT"',
    "exit 1",
    "fi",
]


def _load_ci() -> dict[str, Any]:
    path = Path(os.environ.get("CI_YML_PATH", str(DEFAULT_CI_YML)))
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def _jobs() -> dict[str, dict[str, Any]]:
    jobs = _load_ci()["jobs"]
    assert isinstance(jobs, dict)
    return jobs


def _needs(job: dict[str, Any]) -> list[str]:
    needs = job.get("needs", [])
    if isinstance(needs, str):
        return [needs]
    assert isinstance(needs, list)
    return [str(item) for item in needs]


def _compact(text: str) -> str:
    return "".join(text.split())


def _condition(name: str, job: dict[str, Any]) -> str:
    condition = job.get("if")
    assert isinstance(condition, str), f"job {name} needs an explicit if condition"
    return condition


def test_gate_job_waits_for_environment_on_pull_requests() -> None:
    """The gate job is bound to home-assistant-tests and runs for pull requests only."""

    jobs = _jobs()
    assert GATE in jobs, "ci.yml must define the approval_gate job"
    gate = jobs[GATE]
    assert gate.get("environment") == "home-assistant-tests"
    assert (
        _condition(GATE, gate).replace(" ", "") == "github.event_name=='pull_request'"
    )
    assert gate.get("permissions") == {}, "the gate job needs no token permissions"
    assert set(gate) <= GATE_JOB_KEYS, (
        f"unexpected gate job keys: {set(gate) - GATE_JOB_KEYS}"
    )
    assert not _needs(gate), "the gate must be the first job"


def test_workflow_level_is_fixed() -> None:
    """No inherited defaults or env reach the check, and the triggers stay as gated."""

    data = _load_ci()
    assert set(data) <= WORKFLOW_KEYS, (
        f"unexpected workflow keys {set(data) - WORKFLOW_KEYS}: defaults or env would be "
        "inherited by the check step; extend WORKFLOW_KEYS only after checking that"
    )
    assert data.get(True) == WORKFLOW_TRIGGERS, (
        "ci.yml triggers changed; ungated push runs would add Home Assistant test load"
    )


def test_workflow_name_is_ci() -> None:
    """Helper tools of this fork find the CI checks by the workflow name."""

    assert _load_ci().get("name") == "CI", (
        "renaming the workflow breaks the approval tooling that matches name CI"
    )


def test_one_live_run_per_pull_request() -> None:
    """A push that starts a new PR run cancels the previous one; push runs never share a group."""

    concurrency = _load_ci().get("concurrency")
    assert isinstance(concurrency, dict), "ci.yml must define workflow concurrency"
    assert _compact(str(concurrency.get("group", ""))) == _compact(CONCURRENCY_GROUP)
    assert concurrency.get("cancel-in-progress") is True


def test_hassfest_runs_only_behind_an_approved_gate_or_on_push() -> None:
    """hassfest accepts a skipped gate only for push events."""

    hassfest = _jobs()["hassfest"]
    assert _needs(hassfest) == [GATE]
    condition = _condition("hassfest", hassfest)
    assert _compact(condition) == _compact(HASSFEST_CONDITION)


def test_every_job_requires_its_needs_explicitly() -> None:
    """Each job behind the gate checks every direct need for success."""

    jobs = _jobs()
    checked = 0
    for name, job in jobs.items():
        if name in (GATE, SUMMARY):
            continue
        needs = _needs(job)
        assert needs, f"job {name} must depend on the gate chain"
        if name != "hassfest":
            assert GATE not in needs, f"job {name} must reach the gate via hassfest"
            assert "hassfest" in needs, f"job {name} must need hassfest"
            required = " && ".join(
                f"needs.{need}.result == 'success'" for need in needs
            )
            expected = f"${{{{ !cancelled() && {required} }}}}"
            assert _compact(_condition(name, job)) == _compact(expected), (
                f"job {name}: condition must be exactly {expected}"
            )
        checked += 1
    assert checked >= 7, "expected hassfest and the six jobs behind it"


def test_ci_success_includes_the_gate() -> None:
    """CI Success covers every job and fails an unapproved pull request run."""

    jobs = _jobs()
    summary = jobs[SUMMARY]
    assert summary.get("if") == "always()"
    assert summary.get("name") == "CI Success", "the required check is named CI Success"
    others = [
        name
        for name, job in jobs.items()
        if name != SUMMARY and job.get("name") == "CI Success"
    ]
    assert not others, f"only {SUMMARY} may be named CI Success, also: {others}"
    assert set(summary) <= SUMMARY_JOB_KEYS, (
        f"unexpected CI Success job keys: {set(summary) - SUMMARY_JOB_KEYS}"
    )
    expected = {name for name in jobs if name != SUMMARY}
    assert expected <= set(_needs(summary))

    check = summary["steps"][0]
    assert check.get("id") == "check", (
        "the check must be the first step: an earlier step could change its environment"
    )
    assert check.get("shell") == "bash", "the check uses [[ and needs bash"
    assert set(check) <= CHECK_STEP_KEYS, (
        f"unexpected check step keys: {set(check) - CHECK_STEP_KEYS}"
    )
    env = check.get("env", {})
    assert env == {
        "EVENT": "${{ github.event_name }}",
        "GATE": "${{ needs.approval_gate.result }}",
    }, "the check step env must be exactly EVENT and GATE; extend this test on purpose"
    lines = [line.strip() for line in str(check["run"]).splitlines()]
    code = [line for line in lines if line and not line.startswith("#")]
    opening = f"if {GATE_CHECK}; then"
    assert code and code[0] == opening, "the gate check must be the first command"
    start = lines.index(opening)
    assert lines[start : start + len(GATE_BLOCK)] == GATE_BLOCK, (
        "the gate check block must be exactly GATE_BLOCK (no early exit 0 or trap)"
    )
    assert start < lines.index('echo "All CI jobs passed!"'), (
        "the gate check must run before the success message"
    )
