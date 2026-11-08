# tests/test_precommit_config_contract.py
"""Keep the pre-commit hooks on the tool versions the CI runs.

The CI lint jobs install ruff, codespell and bandit from ``poetry.lock``; the
hooks in ``.pre-commit-config.yaml`` install them from their ``rev:``. When the
two differ, a local ``pre-commit run`` and the CI report different findings.
The ruff hook had drifted behind the lock twice before this test existed.
``script/local_verify.py`` prints the hook pins next to the local tool version,
so its ``RUFF_PIN`` and ``CODESPELL_PIN`` have to move with the revs as well.

The expected versions come from ``poetry.lock``, not from the hook file, so a
hook that is bumped alone fails just like a lock that is bumped alone. When a
dependency update raises one of the three tools, the same PR bumps the matching
``rev:`` and the pin in ``script/local_verify.py`` (root ``AGENTS.md``, bullet
"Configuration alignment").

Two further checks pin the configuration defects this file was written for: the
legacy ``ruff`` hook id, which received ``check`` twice and read it as a file
name, and ``additional_dependencies`` entries such as ``.[dev]``, which a
``repo: local`` hook cannot install because it has no project of its own. The
``mypy`` and ``pytest`` hooks therefore run from the project environment.

Blind spot: the CI skips the ``mypy`` and ``pytest`` hooks because both run in
jobs of their own, so any other defect in those two entries goes unnoticed
there; only the two forms checked here are covered.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml

from script import local_verify

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / ".pre-commit-config.yaml"
LOCK_PATH = REPO_ROOT / "poetry.lock"

# Hook repository of each tool whose rev must equal its version in poetry.lock.
LOCKED_HOOK_REPOS = {
    "ruff": "https://github.com/astral-sh/ruff-pre-commit",
    "bandit": "https://github.com/PyCQA/bandit",
}

# Pins in script/local_verify.py and the tool whose hook rev they mirror.
LOCAL_VERIFY_PINS = {
    "RUFF_PIN": "ruff",
    "CODESPELL_PIN": "codespell",
}

ALL_HOOK_REPOS = {
    **LOCKED_HOOK_REPOS,
    "codespell": "https://github.com/codespell-project/codespell",
}

SYSTEM_HOOKS = ("mypy", "pytest")


def _config() -> dict[str, Any]:
    loaded = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)
    return loaded


def _hooks() -> list[dict[str, Any]]:
    return [hook for repo in _config()["repos"] for hook in repo.get("hooks", [])]


def _rev(repo_url: str) -> str:
    revs = [str(repo["rev"]) for repo in _config()["repos"] if repo["repo"] == repo_url]
    assert len(revs) == 1, f"expected one entry for {repo_url}, found {len(revs)}"
    return revs[0]


def _locked_version(package: str) -> str:
    lock = tomllib.loads(LOCK_PATH.read_text(encoding="utf-8"))
    versions = [
        str(entry["version"]) for entry in lock["package"] if entry["name"] == package
    ]
    assert len(versions) == 1, f"expected one {package} entry in poetry.lock"
    return versions[0]


def drift_message(tool: str, found: str, expected: str) -> str:
    """Name both places a contributor has to change, so the failure is the fix."""

    return (
        f"{tool}: found {found}, poetry.lock has {expected}. Bump the `rev:` of "
        f"the {tool} hook in .pre-commit-config.yaml and its pin in "
        f"script/local_verify.py together."
    )


@pytest.mark.parametrize("tool", sorted(LOCKED_HOOK_REPOS))
def test_hook_rev_matches_the_locked_version(tool: str) -> None:
    """A hook rev and the lock entry of the same tool name the same release."""

    rev = _rev(LOCKED_HOOK_REPOS[tool])
    expected = _locked_version(tool)

    assert rev.lstrip("v") == expected, drift_message(tool, f"rev {rev}", expected)


@pytest.mark.parametrize("pin_name", sorted(LOCAL_VERIFY_PINS))
def test_local_verify_pin_matches_the_hook_rev(pin_name: str) -> None:
    """The pin local_verify prints is the rev the hook really installs."""

    tool = LOCAL_VERIFY_PINS[pin_name]
    pin = getattr(local_verify, pin_name)
    rev = _rev(ALL_HOOK_REPOS[tool])

    assert pin == rev, drift_message(tool, f"{pin_name} {pin}", f"rev {rev}")


def test_drift_message_names_both_places() -> None:
    """A red dependency update must say what to change and where."""

    message = drift_message("ruff", "rev v0.1.0", "0.2.0")

    assert "`rev:`" in message
    assert ".pre-commit-config.yaml" in message
    assert "script/local_verify.py" in message


def test_no_legacy_ruff_hook_id() -> None:
    """The ``ruff`` alias plus ``args: [check, ...]`` passed ``check`` as a file."""

    ids = [hook["id"] for hook in _hooks()]

    assert "ruff" not in ids
    assert "ruff-check" in ids


def test_no_local_install_of_the_project() -> None:
    """A ``repo: local`` hook has no project, so ``.[dev]`` installs nothing."""

    offending = [
        (hook["id"], dependency)
        for hook in _hooks()
        for dependency in hook.get("additional_dependencies", [])
        if str(dependency).startswith(".")
    ]

    assert offending == []


@pytest.mark.parametrize("hook_id", SYSTEM_HOOKS)
def test_environment_hooks_use_the_project_environment(hook_id: str) -> None:
    """mypy and pytest need the project and its plugins, so they run on PATH."""

    matches = [hook for hook in _hooks() if hook["id"] == hook_id]

    assert len(matches) == 1
    assert matches[0]["language"] == "system"
