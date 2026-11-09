# tests/test_pytest_config_smoke.py
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

_CHILD_ENV_VAR = "PYTEST_CONFIG_SMOKE_CHILD"


def test_pytest_config_emits_no_config_warnings(tmp_path: Path) -> None:
    """Ensure `pytest -q` completes without PytestConfigWarning output."""
    if importlib.util.find_spec("pytest_asyncio") is None:
        pytest.skip("pytest-asyncio is not installed")

    if os.environ.get(_CHILD_ENV_VAR) == "1":
        pytest.skip("child smoke invocation should not recurse")

    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env[_CHILD_ENV_VAR] = "1"
    # The child would otherwise erase the parent's coverage data file when the
    # suite runs under ``--cov``; give it a data file of its own.
    env["COVERAGE_FILE"] = str(tmp_path / ".coverage")

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "--fixtures"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, (
        "pytest --fixtures smoke run failed:\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )

    combined_output = "\n".join((result.stdout, result.stderr))
    assert "PytestConfigWarning" not in combined_output
