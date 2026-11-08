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

Reported in every ``test_*.py`` file under ``tests/``, one finding per site:

* a loop factory reached through an asyncio module (``asyncio``, an alias such
  as ``import asyncio as aio``, a submodule such as ``asyncio.events``) or
  imported from one under any name: ``new_event_loop()``, ``run()``,
  ``Runner()`` and the loop classes ``EventLoop``, ``SelectorEventLoop``,
  ``ProactorEventLoop``, ``BaseEventLoop``, ``BaseSelectorEventLoop``,
  ``BaseProactorEventLoop``, ``_UnixSelectorEventLoop`` (taken from
  ``asyncio.__all__`` and the submodules of Python 3.14);
* ``new_event_loop()`` on any receiver, because the policy object returned by
  ``get_event_loop_policy()`` creates a loop the same way;
* ``run_until_complete`` and ``run_forever`` on any loop object, called or only
  referenced (``threading.Thread(target=loop.run_forever)``);
* ``getattr`` with one of these names as a string literal.

The files that carried the pattern when this guard was written are frozen in
``LEGACY_SITES`` with their number of sites: no file may be added, a listed
file must keep exactly its number (a new site fails, a removed one asks to
lower the number), and an entry for a missing file fails. ``LEGACY_FILES`` and
``LEGACY_TOTAL`` pin the size; both may only go down.

Not seen, stated on purpose: ``conftest.py`` and ``tests/helpers/`` (they host
the shared helpers ``drain_loop`` and ``run_loop_until`` that root
``AGENTS.md`` section 3.4 provides for the private loops that remain); a name
built at run time (``getattr(loop, name)``); a loop class reached through a
name not bound by an asyncio import (``loop_class = asyncio.SelectorEventLoop``
is a reference, not a call, and is not reported; the later ``loop_class()`` is
not either); a factory passed as a reference (``target=asyncio.run``,
``f = asyncio.new_event_loop`` and the later ``f()``); names reached through
``from asyncio import *``; loops of other libraries (``uvloop.new_event_loop()`` is reported
only through the any-receiver rule, ``uvloop.Loop()`` is not); a loop reached
through another module's helper. A file whose sites move without changing in
number passes. ``tests/test_guard_asyncio_run_antipattern.py`` is the older
guard for the literal ``asyncio.run``; this one also sees the aliases.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

_TESTS_ROOT = Path(__file__).resolve().parent
_REPO_ROOT = _TESTS_ROOT.parent
# The names were taken from the asyncio module of Python 3.14:
# ``asyncio.__all__`` lists ``new_event_loop``, ``run``, ``Runner`` and the
# loop classes ``EventLoop``, ``SelectorEventLoop``, ``BaseEventLoop`` (plus
# ``ProactorEventLoop`` on Windows); the submodules add ``BaseSelectorEventLoop``,
# ``BaseProactorEventLoop`` and ``_UnixSelectorEventLoop``. The policy object
# returned by ``get_event_loop_policy()`` creates a loop with ``new_event_loop``.
_LOOP_FACTORIES = frozenset(
    {
        "new_event_loop",
        "run",
        "Runner",
        "EventLoop",
        "SelectorEventLoop",
        "ProactorEventLoop",
        "BaseEventLoop",
        "BaseSelectorEventLoop",
        "BaseProactorEventLoop",
        "_UnixSelectorEventLoop",
    }
)
# Methods that drive a loop; a reference counts as well as a call
# (``threading.Thread(target=loop.run_forever)``).
_LOOP_DRIVERS = frozenset({"run_until_complete", "run_forever"})
# ``new_event_loop`` is also a policy method, so it counts on any receiver.
_ANY_RECEIVER = frozenset({"new_event_loop"})

# Private event loop sites per file on 2026-11-08 (measured with this detector
# over every ``tests/**/test_*.py``). Do not add files or sites; convert a test
# to ``async def`` and lower or remove its entry instead.
LEGACY_SITES: dict[str, int] = {
    "tests/test_adm_token_retrieval.py": 54,
    "tests/test_api_basics.py": 7,
    "tests/test_api_fcm_token_scoping.py": 3,
    "tests/test_api_location_selection.py": 3,
    "tests/test_button_service_registration.py": 3,
    "tests/test_cache_accuracy_gate.py": 6,
    "tests/test_cli_sound_request_entry_selection.py": 6,
    "tests/test_config_flow_discovery.py": 10,
    "tests/test_config_flow_initial_auth.py": 7,
    "tests/test_coordinator_invalid_ts_split.py": 1,
    "tests/test_coordinator_owner_index.py": 3,
    "tests/test_coordinator_short_retry.py": 6,
    "tests/test_coordinator_status.py": 44,
    "tests/test_coordinator_timeout.py": 6,
    "tests/test_coordinator_update_failed_translation.py": 3,
    "tests/test_device_identifier_normalization.py": 1,
    "tests/test_diagnostics_buffer_summary.py": 2,
    "tests/test_diagnostics_p1_per_device.py": 2,
    "tests/test_duplicate_device_entities.py": 1,
    "tests/test_e2ee_token_cache_propagation.py": 5,
    "tests/test_fcm_receiver_guard.py": 9,
    "tests/test_fcm_receiver_manual_locate.py": 3,
    "tests/test_fcm_receiver_thread_safety.py": 4,
    "tests/test_hass_data_layout.py": 37,
    "tests/test_helpers_asyncio.py": 3,
    "tests/test_location_recorder.py": 2,
    "tests/test_location_request_cache_isolation.py": 7,
    "tests/test_location_request_not_accepted.py": 1,
    "tests/test_manual_locate_resolution.py": 3,
    "tests/test_multi_account_end_to_end.py": 2,
    "tests/test_options_flow_credentials_cache.py": 5,
    "tests/test_poll_timeout_hardening.py": 19,
    "tests/test_reauth_manual_token.py": 1,
    "tests/test_remove_entry_cleanup.py": 3,
    "tests/test_services_forward_compat.py": 3,
    "tests/test_services_refresh_device_urls.py": 3,
    "tests/test_spot_grpc_client.py": 2,
    "tests/test_spot_grpc_resilience.py": 2,
    "tests/test_stats_sensor_updates.py": 8,
    "tests/test_token_cache_namespace.py": 1,
    "tests/test_token_cache_secrets.py": 1,
    "tests/test_transient_owner_key_propagation.py": 2,
}
LEGACY_FILES = 42
LEGACY_TOTAL = 294


def _asyncio_names(tree: ast.AST) -> tuple[set[str], dict[str, str]]:
    """Return names bound to asyncio modules and names imported from them."""

    modules: set[str] = set()
    imported: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "asyncio" or alias.name.startswith("asyncio."):
                    modules.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            if node.module == "asyncio" or node.module.startswith("asyncio."):
                for alias in node.names:
                    bound = alias.asname or alias.name
                    if alias.name in _LOOP_FACTORIES:
                        imported[bound] = alias.name
                    else:
                        # ``from asyncio import events`` binds a submodule.
                        modules.add(bound)
    return modules, imported


def _rooted_in(node: ast.expr, modules: set[str]) -> bool:
    while isinstance(node, ast.Attribute):
        node = node.value
    return isinstance(node, ast.Name) and node.id in modules


def private_loop_calls(source: str) -> list[tuple[int, str]]:
    """Return ``(line, form)`` for every private event loop site in ``source``."""

    tree = ast.parse(source)
    modules, imported = _asyncio_names(tree)
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Load):
            if node.attr in _LOOP_DRIVERS:
                found.append((node.lineno, node.attr))
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in _LOOP_FACTORIES:
            if func.attr in _ANY_RECEIVER or _rooted_in(func.value, modules):
                form = "asyncio.run()" if func.attr == "run" else f"{func.attr}()"
                found.append((node.lineno, form))
        elif isinstance(func, ast.Name) and func.id in imported:
            name = imported[func.id]
            form = "asyncio.run()" if name == "run" else f"{name}()"
            found.append((node.lineno, form))
        elif (
            isinstance(func, ast.Name) and func.id == "getattr" and len(node.args) >= 2
        ):
            target, name_node = node.args[0], node.args[1]
            if not (
                isinstance(name_node, ast.Constant) and isinstance(name_node.value, str)
            ):
                continue
            value = name_node.value
            if value in _LOOP_DRIVERS or (
                value in _LOOP_FACTORIES
                and (value in _ANY_RECEIVER or _rooted_in(target, modules))
            ):
                found.append((node.lineno, f"getattr {value}"))
    return sorted(found)


def _test_files() -> list[Path]:
    return sorted(_TESTS_ROOT.rglob("test_*.py"))


def _relative(path: Path) -> str:
    return path.relative_to(_REPO_ROOT).as_posix()


def _sites() -> dict[str, list[str]]:
    paths = _test_files()
    assert len(paths) > 100, f"only {len(paths)} test files found under {_TESTS_ROOT}"
    sites: dict[str, list[str]] = {}
    for path in paths:
        found = private_loop_calls(path.read_text(encoding="utf-8"))
        if found:
            sites[_relative(path)] = [f"{line}: {form}" for line, form in found]
    return sites


def test_no_new_test_file_drives_a_private_event_loop() -> None:
    """Every file outside the frozen list awaits instead of driving a loop."""

    offenses = [
        f"{path}:{site}"
        for path, found in _sites().items()
        if path not in LEGACY_SITES
        for site in found
    ]

    assert offenses == [], (
        "Test code drives an event loop of its own (tests/AGENTS.md, Async "
        "tests): write the test as `async def` and `await` the coroutine; "
        "pytest-asyncio provides the loop.\n" + "\n".join(offenses)
    )


def test_listed_files_keep_exactly_their_frozen_number_of_sites() -> None:
    """A listed file may not gain a site; a removed site lowers its number."""

    sites = _sites()
    drift = {
        path: (len(sites.get(path, [])), allowed)
        for path, allowed in LEGACY_SITES.items()
        if len(sites.get(path, [])) != allowed
    }

    assert drift == {}, (
        "update LEGACY_SITES (found, allowed): a higher count is a new private "
        f"loop, a lower or zero count must lower or remove the entry: {drift}"
    )


def test_the_frozen_list_neither_grows_nor_keeps_dead_entries() -> None:
    """The list only shrinks, and each entry still names an existing file."""

    assert len(LEGACY_SITES) <= LEGACY_FILES
    assert sum(LEGACY_SITES.values()) <= LEGACY_TOTAL
    dead = sorted(
        entry
        for entry, allowed in LEGACY_SITES.items()
        if allowed < 1 or not (_REPO_ROOT / entry).is_file()
    )

    assert dead == [], f"remove these entries from LEGACY_SITES: {dead}"


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
        (_REVIEW_FINDING, ["new_event_loop()", "run_until_complete"]),
        ("import asyncio\nasyncio.run(main())", ["asyncio.run()"]),
        (
            "import asyncio as aio\nloop = aio.new_event_loop()",
            ["new_event_loop()"],
        ),
        (
            "from asyncio import new_event_loop as fresh, run\nfresh()\nrun(main())",
            ["new_event_loop()", "asyncio.run()"],
        ),
        ("hass.loop.run_until_complete(job())", ["run_until_complete"]),
        (
            "import asyncio\nasyncio.get_event_loop().run_until_complete(job())",
            ["run_until_complete"],
        ),
        ('getattr(loop, "run_until_complete")(job())', ["getattr run_until_complete"]),
        ('import asyncio\ngetattr(asyncio, "run")(job())', ["getattr run"]),
        ("import asyncio\nloop = asyncio.SelectorEventLoop()", ["SelectorEventLoop()"]),
        ("import asyncio\nloop = asyncio.ProactorEventLoop()", ["ProactorEventLoop()"]),
        ("from asyncio import EventLoop\nloop = EventLoop()", ["EventLoop()"]),
        ("import asyncio\nwith asyncio.Runner() as runner:\n    pass", ["Runner()"]),
        (
            "import asyncio\nloop = asyncio.get_event_loop_policy().new_event_loop()",
            ["new_event_loop()"],
        ),
        (
            "import asyncio.events\nloop = asyncio.events.new_event_loop()",
            ["new_event_loop()"],
        ),
        (
            "from asyncio import events as ev\nloop = ev.new_event_loop()",
            ["new_event_loop()"],
        ),
        ("t = threading.Thread(target=loop.run_forever)", ["run_forever"]),
        ("loop.run_forever()", ["run_forever"]),
        (
            'import asyncio\ngetattr(asyncio, "SelectorEventLoop")()',
            ["getattr SelectorEventLoop"],
        ),
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
        "loop = other_library.EventLoop()",
        "import asyncio\nasyncio.set_event_loop_policy(None)",
        "loop.run_in_executor(None, work)",
    ],
)
def test_detector_leaves_other_calls_alone(source: str) -> None:
    """Self-test: awaiting, other ``run`` functions and plain helpers pass."""

    assert private_loop_calls(source) == []
