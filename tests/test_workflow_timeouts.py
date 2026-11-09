# tests/test_workflow_timeouts.py
"""Bound the run time of every working job in ``.github/workflows/ci.yml``.

Without ``timeout-minutes`` a hanging step (a package download that stops
making progress, a stuck test) holds the pull request until GitHub's platform
limit of 360 minutes. Each working job therefore carries a limit, and so does every step
that calls ``apt-get`` or ``apt`` (the command word, not ``apt-cache`` or
``aptitude``).

The limits have two bounds. The lower bound is twice the longest duration
measured for the job or step, rounded up to whole minutes, so a normal run never
hits it. The upper bound is ``JOB_LIMIT_CEILING_MINUTES`` for a job, and the job
limit for a step, so a limit close to the platform default is caught as well.
Every step that carries ``timeout-minutes``, not only the package steps, needs a
measured duration in ``MAX_STEP_SECONDS``: a limit at a step nobody measured
cannot be judged, and one below twice the measured run fires on a normal run.

The two gate jobs stay without a limit: ``approval_gate`` waits for a human
approval, and ``tests/test_ci_approval_gate.py`` pins the exact key sets of
``approval_gate`` and ``ci-success``.

Where the maximum durations come from (``MAX_JOB_SECONDS``): the larger value of
two windows of successful runs of the CI workflow on the fork, 30 runs with ids
38914060261 to 39936583884 (2026-10-16 to 2026-10-23) and 12 runs with ids
42233715176 to 42474366885 (2026-11-06 to 2026-11-08), job duration from
``startedAt`` to ``completedAt`` in ``gh run view <id> --json jobs``. The test
legs grew from 646 s to 881 s (run 42280905514, ``Test (Python 3.14)``) and
880 s (run 42362012960); twice that, rounded up, is exactly 30 minutes. The
ceiling was therefore raised from 30 to 45 minutes on 2026-11-08, while the job
limits stay at 30 until a measured run longer than 900 s requires more. At that
date the longest test job in the six latest successful runs took 779 s (run
42424494992, ``declared floor`` leg), the others 473 s to 766 s. Splitting the
suite was set aside: it reaches into the Home Assistant fixtures and
``pytest-socket``. For ``test`` the larger of the two
``Test (Python ...)`` matrix legs is used, for ``test_legacy_python`` the
larger of its legs. The ``lint`` value is provisional: the ``pre-commit``
matrix entry is new, and 200 s is a local cold run of it (installing the hook
environments, about 150 s, plus the run itself); the other lint entries took at
most 77 s. Replace it
with the first CI measurement when that is longer. The two package steps
(``Install system dependencies``) took at most 40 s, which gives a lower bound
of 2 minutes for their limit.

The durations are literals on purpose. Deriving them from ``ci.yml`` would make
the test agree with whatever the workflow says.
"""

from __future__ import annotations

import math
import re
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
    "test": 881,
    "test_legacy_python": 880,
}
# Longest measured duration of each step that carries ``timeout-minutes``,
# keyed by step name.
MAX_STEP_SECONDS = {
    "Install system dependencies": 40,
}
JOB_LIMIT_CEILING_MINUTES = 45

# ``apt-get`` or ``apt`` as a command word; ``apt-cache``, ``aptitude`` and
# ``apt_pkg`` are other programs or names.
_APT_COMMAND = re.compile(r"(?<![\w.-])apt(?:-get)?(?![\w./-])")


def _jobs() -> dict[str, dict[str, Any]]:
    loaded = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    jobs = loaded["jobs"]
    assert isinstance(jobs, dict)
    return jobs


def _working_jobs(
    jobs: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    jobs = _jobs() if jobs is None else jobs
    return {name: job for name, job in jobs.items() if name not in UNLIMITED_JOBS}


def _apt_steps(
    jobs: dict[str, dict[str, Any]] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    return [
        (name, step)
        for name, job in _working_jobs(jobs).items()
        for step in job.get("steps", [])
        if _APT_COMMAND.search(str(step.get("run", "")))
    ]


def _limited_steps(
    jobs: dict[str, dict[str, Any]] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    return [
        (name, step)
        for name, job in _working_jobs(jobs).items()
        for step in job.get("steps", [])
        if "timeout-minutes" in step
    ]


def _apt_steps_without_limit(jobs: dict[str, dict[str, Any]]) -> list[str]:
    return [name for name, step in _apt_steps(jobs) if "timeout-minutes" not in step]


def _step_limits_out_of_bounds(
    jobs: dict[str, dict[str, Any]],
) -> list[tuple[str, str, object]]:
    """Return the limited steps without a measurement or outside their bounds."""

    out: list[tuple[str, str, object]] = []
    for name, step in _limited_steps(jobs):
        step_name = str(step.get("name", ""))
        limit = step["timeout-minutes"]
        measured = MAX_STEP_SECONDS.get(step_name)
        job_limit = jobs[name].get("timeout-minutes")
        if (
            measured is None
            or not isinstance(limit, int)
            or not isinstance(job_limit, int)
            or not _floor_minutes(measured) <= limit <= job_limit
        ):
            out.append((name, step_name, limit))
    return out


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

    assert steps, "no apt-get step found; the reader no longer sees them"
    assert _apt_steps_without_limit(_jobs()) == []


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


def test_step_limit_lies_between_twice_its_run_and_the_job_limit() -> None:
    """Every step limit fires before the job limit and never on a normal run."""

    assert _limited_steps(), "no limited step found; the reader no longer sees them"
    assert _step_limits_out_of_bounds(_jobs()) == []


def test_every_measured_step_exists() -> None:
    """A stale step entry would let a renamed step escape the bounds."""

    names = {str(step.get("name", "")) for _, step in _limited_steps()}

    assert sorted(set(MAX_STEP_SECONDS) - names) == []


def _job(*steps: dict[str, Any], limit: int = 10) -> dict[str, dict[str, Any]]:
    return {"lint": {"timeout-minutes": limit, "steps": list(steps)}}


@pytest.mark.parametrize(
    ("run", "is_apt"),
    [
        ("sudo apt-get update && sudo apt-get install -y jq", True),
        ("sudo apt install -y jq", True),
        ("apt update", True),
        ("sudo apt-cache policy jq", False),
        ("sudo aptitude install jq", False),
        ("python -c 'import apt_pkg'", False),
        ("cat /etc/apt/sources.list", False),
        ("sudo /usr/bin/apt install -y jq", True),
    ],
)
def test_apt_reader_sees_both_command_words(run: str, is_apt: bool) -> None:
    """Self-test: ``apt`` counts like ``apt-get``; other names do not."""

    jobs = _job({"name": "Install", "run": run})

    assert bool(_apt_steps(jobs)) is is_apt


def test_an_apt_install_step_without_limit_is_reported() -> None:
    """Self-test: an ``apt install`` step without a limit is a finding."""

    jobs = _job({"name": "Install system dependencies", "run": "sudo apt install x"})

    assert _apt_steps_without_limit(jobs) == ["lint"]


@pytest.mark.parametrize(
    ("step", "reported"),
    [
        ({"name": "Install system dependencies", "timeout-minutes": 0}, True),
        ({"name": "Install system dependencies", "timeout-minutes": 1}, True),
        ({"name": "Install system dependencies", "timeout-minutes": 2}, False),
        ({"name": "Install system dependencies", "timeout-minutes": 10}, False),
        ({"name": "Install system dependencies", "timeout-minutes": 11}, True),
        ({"name": "Run pytest", "timeout-minutes": 5}, True),
        ({"name": "Install system dependencies", "timeout-minutes": "5"}, True),
    ],
)
def test_step_limit_bounds_are_enforced(step: dict[str, Any], reported: bool) -> None:
    """Self-test: below twice the run, above the job limit, or unmeasured."""

    assert bool(_step_limits_out_of_bounds(_job(step))) is reported


def test_every_working_job_has_a_measured_duration() -> None:
    """A new job needs a measured duration before its limit can be judged."""

    unmeasured = sorted(set(_working_jobs()) - set(MAX_JOB_SECONDS))

    assert unmeasured == []


def test_every_measured_job_exists() -> None:
    """A stale entry would let a renamed job escape the bounds."""

    stale = sorted(set(MAX_JOB_SECONDS) - set(_jobs()))

    assert stale == []
