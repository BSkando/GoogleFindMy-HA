# tests/test_workflow_timeouts.py
"""Bound the run time of every working job in ``.github/workflows/ci.yml``.

Without ``timeout-minutes`` a hanging step (a package download that stops
making progress, a stuck test) holds the pull request until GitHub's platform
limit of 360 minutes. Each working job therefore carries a limit, and so does every step
that calls ``apt-get``.

The limits have two bounds. The lower bound is twice the longest duration
measured for the job, rounded up to whole minutes, so a normal run never hits
it. The upper bound is 30 minutes for a job, and the job limit for a step, so a
limit close to the platform default is caught as well.

The two gate jobs stay without a limit: ``approval_gate`` waits for a human
approval, and ``tests/test_ci_approval_gate.py`` pins the exact key sets of
``approval_gate`` and ``ci-success``.

Where the maximum durations come from (``MAX_JOB_SECONDS``): 30 successful runs
of the CI workflow on the fork, run ids 38914060261 to 39936583884
(2026-10-16 to 2026-10-23), job duration from ``startedAt`` to ``completedAt``
in ``gh run view <id> --json jobs``. For ``test`` the larger of the two
``Test (Python ...)`` matrix legs is used, for ``test_legacy_python`` the
larger of its legs. The ``lint`` value is provisional: the ``pre-commit``
matrix entry is new, and 200 s is a local cold run of it (installing the hook
environments, about 150 s, plus the run itself); the other lint entries took at
most 77 s. Replace it
with the first CI measurement when that is longer. The ``apt-get`` step took at
most 40 s.

The durations are literals on purpose. Deriving them from ``ci.yml`` would make
the test agree with whatever the workflow says.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "ci.yml"

UNLIMITED_JOBS = ("approval_gate", "ci-success")
MAX_JOB_SECONDS = {
    "hassfest": 63,
    "hacs": 60,
    "translations": 14,
    "vendored_assets": 11,
    "lint": 200,
    "test": 625,
    "test_legacy_python": 646,
}
MAX_APT_STEP_SECONDS = 40
JOB_LIMIT_CEILING_MINUTES = 30


def _jobs() -> dict[str, dict[str, Any]]:
    loaded = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    jobs = loaded["jobs"]
    assert isinstance(jobs, dict)
    return jobs


def _working_jobs() -> dict[str, dict[str, Any]]:
    return {name: job for name, job in _jobs().items() if name not in UNLIMITED_JOBS}


def _apt_steps() -> list[tuple[str, dict[str, Any]]]:
    return [
        (name, step)
        for name, job in _working_jobs().items()
        for step in job.get("steps", [])
        if "apt-get" in str(step.get("run", ""))
    ]


def _floor_minutes(seconds: int) -> int:
    return math.ceil(2 * seconds / 60)


def test_every_working_job_has_a_limit() -> None:
    """A job without ``timeout-minutes`` runs until the 360-minute default."""

    missing = [
        name for name, job in _working_jobs().items() if "timeout-minutes" not in job
    ]

    assert missing == []


def test_every_apt_step_has_a_limit() -> None:
    """The package mirror is the step that hung before; it gets its own limit."""

    steps = _apt_steps()
    missing = [name for name, step in steps if "timeout-minutes" not in step]

    assert steps, "no apt-get step found; the reader no longer sees them"
    assert missing == []


@pytest.mark.parametrize("name", UNLIMITED_JOBS)
def test_gate_jobs_have_no_limit(name: str) -> None:
    """The approval gate waits for a human; its key set is pinned elsewhere."""

    assert "timeout-minutes" not in _jobs()[name]


@pytest.mark.parametrize("name", sorted(MAX_JOB_SECONDS))
def test_job_limit_is_at_least_twice_the_longest_run(name: str) -> None:
    """A normal run must never reach the limit."""

    limit = _jobs()[name]["timeout-minutes"]

    assert limit >= _floor_minutes(MAX_JOB_SECONDS[name])


@pytest.mark.parametrize("name", sorted(MAX_JOB_SECONDS))
def test_job_limit_stays_below_the_ceiling(name: str) -> None:
    """A limit near the platform default protects nothing."""

    assert _jobs()[name]["timeout-minutes"] <= JOB_LIMIT_CEILING_MINUTES


def test_apt_step_limit_lies_between_twice_its_run_and_the_job_limit() -> None:
    """The step limit fires before the job limit and never on a normal run."""

    jobs = _jobs()
    out_of_bounds = [
        (name, step["timeout-minutes"])
        for name, step in _apt_steps()
        if not _floor_minutes(MAX_APT_STEP_SECONDS)
        <= step["timeout-minutes"]
        <= jobs[name]["timeout-minutes"]
    ]

    assert out_of_bounds == []


def test_every_working_job_has_a_measured_duration() -> None:
    """A new job needs a measured duration before its limit can be judged."""

    unmeasured = sorted(set(_working_jobs()) - set(MAX_JOB_SECONDS))

    assert unmeasured == []


def test_every_measured_job_exists() -> None:
    """A stale entry would let a renamed job escape the bounds."""

    stale = sorted(set(MAX_JOB_SECONDS) - set(_jobs()))

    assert stale == []
