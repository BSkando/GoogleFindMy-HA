# tests/test_guard_asyncio_run_antipattern.py
"""Guard against using asyncio.run() inside the test suite."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# Legacy files still contain asyncio.run() usage and require refactoring.
# Do not add new occurrences outside this allowlist.
LEGACY_ALLOWLIST: set[str] = {
    "tests/test_adm_token_retrieval.py",
    "tests/test_api_fcm_token_scoping.py",
    "tests/test_cli_sound_request_entry_selection.py",
    "tests/test_config_flow_discovery.py",
    "tests/test_config_flow_initial_auth.py",
    "tests/test_device_identifier_normalization.py",
    "tests/test_duplicate_device_entities.py",
    "tests/test_e2ee_token_cache_propagation.py",
    "tests/test_fcm_receiver_guard.py",
    "tests/test_fcm_receiver_manual_locate.py",
    "tests/test_location_recorder.py",
    "tests/test_location_request_cache_isolation.py",
    "tests/test_manual_locate_resolution.py",
    "tests/test_options_flow_credentials_cache.py",
    "tests/test_reauth_manual_token.py",
    "tests/test_remove_entry_cleanup.py",
    "tests/test_services_forward_compat.py",
    "tests/test_services_refresh_device_urls.py",
    "tests/test_token_cache_namespace.py",
    "tests/test_token_cache_secrets.py",
}


def _find_asyncio_run_calls(path: Path) -> list[tuple[int, int]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    calls: list[tuple[int, int]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute):
            if (
                func.attr == "run"
                and isinstance(func.value, ast.Name)
                and func.value.id == "asyncio"
            ):
                calls.append((node.lineno, node.col_offset))
    return calls


def test_no_asyncio_run_in_tests() -> None:
    """Fail loudly if asyncio.run() is used in tests."""

    offenses: list[tuple[str, int, int]] = []
    tests_root = Path(__file__).parent
    for path in tests_root.rglob("*.py"):
        if path.name == Path(__file__).name:
            continue
        rel_path = path.relative_to(Path.cwd()).as_posix()
        if rel_path in LEGACY_ALLOWLIST:
            continue
        for lineno, col in _find_asyncio_run_calls(path):
            offenses.append((rel_path, lineno, col))

    if offenses:
        lines = []
        for file, lineno, col in offenses:
            lines.append(
                f"File: {file}\nLine: {lineno}\nColumn: {col}\nOffense: Found 'asyncio.run(...)'\n"
            )
        message = (
            "CRITICAL ARCHITECTURE VIOLATION DETECTED!\n"
            "------------------------------------------------------------\n"
            + "\n".join(lines)
            + "WHY THIS IS WRONG:\n"
            "Home Assistant tests run inside a managed event loop.\n"
            "Using asyncio.run() creates a competing loop, breaking fixtures.\n\n"
            "HOW TO FIX:\n"
            "1. Change 'def test_foo():' to 'async def test_foo():'.\n"
            "2. Replace 'result = asyncio.run(func())' with 'result = await func()'.\n"
            "3. Ensure pytest-asyncio provides the event loop (asyncio_mode = auto).\n"
            "------------------------------------------------------------\n"
        )
        pytest.fail(message)
