# tests/test_guard_no_private_event_loop.py
"""Guard: no test file drives an event loop of its own.

``tests/AGENTS.md`` (section "Async tests") says that pytest-asyncio manages
the event loop (``asyncio_mode = "auto"``), that ``asyncio.run()`` must never
be called inside tests because it creates a competing event loop and breaks
Home Assistant's managed loop fixtures, and that coroutine tests are written
as ``async def`` and ``await`` the coroutine directly. A loop created with
``asyncio.new_event_loop()`` and driven by ``loop.run_until_complete(...)`` is
the same competing loop by another name. The rule lived as prose only, so the
pattern of older files was copied into new ones, and a review found it only
after the push.

Reported in every ``test_*.py`` file under ``tests/``:

* ``asyncio.new_event_loop(...)`` and ``asyncio.run(...)``, also through
  ``import asyncio as aio`` and ``from asyncio import new_event_loop, run``
  (under any alias);
* ``<anything>.run_until_complete(...)``, whatever the loop object is called
  (``loop``, ``hass.loop``, ``asyncio.get_event_loop()``);
* ``getattr(<object>, "run_until_complete")`` and ``getattr(asyncio,
  "new_event_loop" | "run")`` with the name as a string literal.

The files that carried the pattern when this guard was written are frozen in
``LEGACY_ALLOWLIST``: no file may be added, and each entry must still exist and
still carry the pattern, so a refactored file leaves the list. The number is
pinned in ``LEGACY_COUNT``; it may only go down.

Not seen, stated on purpose: ``conftest.py`` and ``tests/helpers/`` (they host
the shared helpers ``drain_loop`` and ``run_loop_until`` that root
``AGENTS.md`` section 3.4 provides for the private loops that remain); a name
built at run time (``getattr(loop, name)``); a loop reached through another
module's helper; ``asyncio.Runner`` and ``loop.run_forever()`` (no file uses
them today). ``tests/test_guard_asyncio_run_antipattern.py`` is the older
guard for the literal ``asyncio.run``; this one also sees the aliases.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_TESTS_ROOT = Path(__file__).resolve().parent
_REPO_ROOT = _TESTS_ROOT.parent
_ASYNCIO_CALLS = frozenset({"new_event_loop", "run"})
_LOOP_METHODS = frozenset({"run_until_complete"})

# Files that drove a private event loop on 2026-11-08 (measured with this
# detector over every ``tests/**/test_*.py``). Do not add entries; convert a
# file to ``async def`` tests and remove it here instead.
LEGACY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "tests/test_adm_token_retrieval.py",
        "tests/test_api_basics.py",
        "tests/test_api_fcm_token_scoping.py",
        "tests/test_api_location_selection.py",
        "tests/test_button_service_registration.py",
        "tests/test_cache_accuracy_gate.py",
        "tests/test_cli_sound_request_entry_selection.py",
        "tests/test_config_flow_discovery.py",
        "tests/test_config_flow_initial_auth.py",
        "tests/test_coordinator_invalid_ts_split.py",
        "tests/test_coordinator_owner_index.py",
        "tests/test_coordinator_short_retry.py",
        "tests/test_coordinator_status.py",
        "tests/test_coordinator_timeout.py",
        "tests/test_coordinator_update_failed_translation.py",
        "tests/test_device_identifier_normalization.py",
        "tests/test_diagnostics_buffer_summary.py",
        "tests/test_diagnostics_p1_per_device.py",
        "tests/test_duplicate_device_entities.py",
        "tests/test_e2ee_token_cache_propagation.py",
        "tests/test_fcm_receiver_guard.py",
        "tests/test_fcm_receiver_manual_locate.py",
        "tests/test_fcm_receiver_thread_safety.py",
        "tests/test_hass_data_layout.py",
        "tests/test_helpers_asyncio.py",
        "tests/test_location_recorder.py",
        "tests/test_location_request_cache_isolation.py",
        "tests/test_location_request_not_accepted.py",
        "tests/test_manual_locate_resolution.py",
        "tests/test_multi_account_end_to_end.py",
        "tests/test_options_flow_credentials_cache.py",
        "tests/test_poll_timeout_hardening.py",
        "tests/test_reauth_manual_token.py",
        "tests/test_remove_entry_cleanup.py",
        "tests/test_services_forward_compat.py",
        "tests/test_services_refresh_device_urls.py",
        "tests/test_spot_grpc_client.py",
        "tests/test_spot_grpc_resilience.py",
        "tests/test_stats_sensor_updates.py",
        "tests/test_token_cache_namespace.py",
        "tests/test_token_cache_secrets.py",
        "tests/test_transient_owner_key_propagation.py",
    }
)
LEGACY_COUNT = 42


def private_loop_calls(source: str) -> list[tuple[int, str]]:
    """Return ``(line, form)`` for every private event loop call in ``source``."""

    tree = ast.parse(source)
    modules = {"asyncio"}
    functions: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "asyncio":
                    modules.add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module == "asyncio":
            for alias in node.names:
                if alias.name in _ASYNCIO_CALLS:
                    functions[alias.asname or alias.name] = alias.name
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute):
            if func.attr in _LOOP_METHODS:
                found.append((node.lineno, f"{func.attr}()"))
            elif (
                func.attr in _ASYNCIO_CALLS
                and isinstance(func.value, ast.Name)
                and func.value.id in modules
            ):
                found.append((node.lineno, f"asyncio.{func.attr}()"))
        elif isinstance(func, ast.Name):
            if func.id in functions:
                found.append((node.lineno, f"asyncio.{functions[func.id]}()"))
            elif func.id == "getattr" and len(node.args) >= 2:
                target, name = node.args[0], node.args[1]
                if not (isinstance(name, ast.Constant) and isinstance(name.value, str)):
                    continue
                if name.value in _LOOP_METHODS or (
                    name.value in _ASYNCIO_CALLS
                    and isinstance(target, ast.Name)
                    and target.id in modules
                ):
                    found.append((node.lineno, f"getattr {name.value}"))
    return found


def _test_files() -> list[Path]:
    return sorted(_TESTS_ROOT.rglob("test_*.py"))


def _relative(path: Path) -> str:
    return path.relative_to(_REPO_ROOT).as_posix()


def test_no_new_test_file_drives_a_private_event_loop() -> None:
    """Every file outside the frozen list awaits instead of driving a loop."""

    paths = _test_files()
    assert len(paths) > 100, f"only {len(paths)} test files found under {_TESTS_ROOT}"
    offenses = [
        f"{_relative(path)}:{line}: {form}"
        for path in paths
        if _relative(path) not in LEGACY_ALLOWLIST
        for line, form in private_loop_calls(path.read_text(encoding="utf-8"))
    ]

    assert offenses == [], (
        "Test code drives an event loop of its own (tests/AGENTS.md, Async "
        "tests): write the test as `async def` and `await` the coroutine; "
        "pytest-asyncio provides the loop.\n" + "\n".join(offenses)
    )


def test_the_frozen_list_neither_grows_nor_keeps_dead_entries() -> None:
    """The list only shrinks, and each entry still names a file with the pattern."""

    assert len(LEGACY_ALLOWLIST) <= LEGACY_COUNT
    dead = sorted(
        entry
        for entry in LEGACY_ALLOWLIST
        if not (_REPO_ROOT / entry).is_file()
        or not private_loop_calls((_REPO_ROOT / entry).read_text(encoding="utf-8"))
    )

    assert dead == [], f"remove these entries from LEGACY_ALLOWLIST: {dead}"


# The form the review found after the push in
# ``tests/test_poll_device_name_at_debug.py`` (rebuilt; that commit is gone).
_REVIEW_FINDING = """
import asyncio

def _run_cycle(coordinator, devices):
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(coordinator._async_start_poll_cycle(devices, force=True))
    finally:
        loop.close()
"""


@pytest.mark.parametrize(
    ("source", "forms"),
    [
        (_REVIEW_FINDING, ["asyncio.new_event_loop()", "run_until_complete()"]),
        ("import asyncio\nasyncio.run(main())", ["asyncio.run()"]),
        (
            "import asyncio as aio\nloop = aio.new_event_loop()",
            ["asyncio.new_event_loop()"],
        ),
        (
            "from asyncio import new_event_loop as fresh, run\nfresh()\nrun(main())",
            ["asyncio.new_event_loop()", "asyncio.run()"],
        ),
        ("hass.loop.run_until_complete(job())", ["run_until_complete()"]),
        (
            "import asyncio\nasyncio.get_event_loop().run_until_complete(job())",
            ["run_until_complete()"],
        ),
        ('getattr(loop, "run_until_complete")(job())', ["getattr run_until_complete"]),
        ('import asyncio\ngetattr(asyncio, "run")(job())', ["getattr run"]),
    ],
)
def test_detector_reports_every_form(source: str, forms: list[str]) -> None:
    """Self-test: each form is reported, including the review finding."""

    assert [form for _line, form in private_loop_calls(source)] == forms


@pytest.mark.parametrize(
    "source",
    [
        "import asyncio\nasync def test_x():\n    await asyncio.sleep(0)",
        "import asyncio\nloop = asyncio.get_running_loop()",
        "runner.run(job())",
        "from subprocess import run\nrun(['true'])",
        'getattr(asyncio_stub, "run")',
        "import asyncio\nasyncio.gather(*tasks)",
    ],
)
def test_detector_leaves_other_calls_alone(source: str) -> None:
    """Self-test: awaiting, other ``run`` functions and plain helpers pass."""

    assert private_loop_calls(source) == []
