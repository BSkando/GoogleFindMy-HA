# tests/test_guard_workflow_environments.py
"""Pin every workflow environment name to a closed English allowlist.

A job that names an ``environment:`` shows that name in the approval dialog of a
waiting run ("<job> is waiting for <environment> deployment approval"). The name
is public, and the language policy in ``AGENTS.md`` asks for English. The approval
environment of ``ci.yml`` nevertheless once carried a German name: the policy
covered prose, comments and docstrings, and the prose guard looks for German
function words, so a single German noun without umlauts passed.

This guard therefore does not try to recognise German. It compares every
environment name in ``.github/workflows/*.yml`` and ``*.yaml`` against the closed
set ``ALLOWED_ENVIRONMENTS``. Any new name fails until it is added there on
purpose, with a reason; that holds for a German name as much as for a misspelt
English one. A second test fails when an allowed name is no longer referenced, so
the set cannot keep a stale entry that would later admit a name nobody reviewed.

Accepted forms (GitHub Actions documentation, "Using environments for jobs"): a
plain string, or a mapping whose ``name`` is a literal string; ``url`` and
``deployment`` are ignored. A name built from an expression (``${{ ... }}``)
or a value that is not a string fails, because it cannot be checked here. A
workflow file that does not parse, or that repeats a mapping key (a YAML loader
keeps only the last ``environment:`` of a job and would hide the first), fails
the guard instead of being skipped.

Limits: on a pull request this guard runs in CI only after the approval gate has
passed (pushes to ``main`` run it without a gate), so there it reports a
violation after the test load, not before it. It reads ``jobs.<id>.environment``
only. It does not see an environment passed as an action input
(``with: environment:``) or named in a ``run`` step, one set inside a reusable
workflow called from another repository (``jobs.<id>.uses``; a called workflow
of this repository is read like any other file here), or one created through the
API or the settings page.

``WORKFLOWS_DIR`` overrides the directory that is read; it exists for red probes
against an older or mutated copy of the workflows.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects a mapping key that appears twice.

    Merge keys (``<<``) are left to the safe constructor, which resolves them; a
    key that cannot be hashed fails with the same error class as a duplicate.
    """


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.Node) -> Any:
    assert isinstance(node, yaml.MappingNode)
    seen: set[Any] = set()
    for key_node, _ in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            continue
        key = loader.construct_object(key_node, deep=True)
        try:
            hash(key)
        except TypeError:
            raise yaml.constructor.ConstructorError(
                None, None, f"unhashable mapping key {key!r}", key_node.start_mark
            ) from None
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate mapping key {key!r}", key_node.start_mark
            )
        seen.add(key)
    return loader.construct_mapping(node)


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


def _load_workflow(path: Path) -> Any:
    """Parse one workflow file with the safe constructors and unique keys."""

    loader = _UniqueKeyLoader(path.read_text(encoding="utf-8"))
    try:
        return loader.get_single_data()
    finally:
        loader.dispose()


ALLOWED_ENVIRONMENTS = frozenset(
    {
        # ci.yml, job approval_gate: the approval dialog names the Home Assistant
        # test jobs of the pull request that wait behind the gate.
        "home-assistant-tests",
    }
)


def _workflows_dir() -> Path:
    override = os.environ.get("WORKFLOWS_DIR")
    return Path(override) if override else DEFAULT_WORKFLOWS_DIR


def _workflow_files() -> list[Path]:
    directory = _workflows_dir()
    return sorted([*directory.glob("*.yml"), *directory.glob("*.yaml")])


def _environment_references() -> list[tuple[str, str, Any]]:
    """Return (file, job, raw environment value) for every job that names one."""

    references: list[tuple[str, str, Any]] = []
    for path in _workflow_files():
        document = _load_workflow(path)
        assert isinstance(document, dict), f"{path.name} does not parse to a mapping"
        jobs = document.get("jobs") or {}
        assert isinstance(jobs, dict), f"{path.name}: 'jobs' is not a mapping"
        for job_id, job in jobs.items():
            if isinstance(job, dict) and "environment" in job:
                references.append((path.name, str(job_id), job["environment"]))
    return references


def _literal_name(value: Any) -> str | None:
    """Return the environment name if it is a literal string, else None."""

    name = value.get("name") if isinstance(value, dict) else value
    if not isinstance(name, str) or "${{" in name:
        return None
    return name


def test_environment_references_exist() -> None:
    """The guard reads at least the approval gate, so it cannot pass vacuously."""

    references = _environment_references()
    assert any(
        workflow == "ci.yml" and job == "approval_gate"
        for workflow, job, _ in references
    ), "no environment found for ci.yml job approval_gate; is WORKFLOWS_DIR right?"


def test_every_environment_is_allowed() -> None:
    """Every environment name is a literal string from ALLOWED_ENVIRONMENTS."""

    violations = []
    for workflow, job, value in _environment_references():
        name = _literal_name(value)
        if name is None:
            violations.append(f"{workflow} job {job}: not a literal string {value!r}")
        elif name not in ALLOWED_ENVIRONMENTS:
            violations.append(f"{workflow} job {job}: {name!r} is not allowed")
    assert not violations, (
        "environment names must come from ALLOWED_ENVIRONMENTS (English, added "
        "on purpose with a reason): " + "; ".join(violations)
    )


def test_allowlist_has_no_stale_entries() -> None:
    """Every allowed name is still referenced by at least one job."""

    used = {_literal_name(value) for _, _, value in _environment_references()}
    stale = sorted(ALLOWED_ENVIRONMENTS - used)
    assert not stale, f"ALLOWED_ENVIRONMENTS lists unused names: {stale}"
