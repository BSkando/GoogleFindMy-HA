"""tests/conftest.py: Common fixtures and Home Assistant stubs for tests."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import json
import sys
import warnings
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest
import pytest_socket


def pytest_configure(config: pytest.Config) -> None:
    """Allow sockets globally and register asyncio markers for coroutine tests."""

    pytest_socket.enable_socket()
    config.option.disable_socket = False
    config.option.allow_unix_socket = True
    config.option.allow_host = ["127.0.0.1", "::1", "localhost"]
    config.addinivalue_line(
        "markers",
        "asyncio: execute the coroutine test using an isolated event loop",
    )
    _freeze_coordinator_identity_reference()


#: Frozen at ``pytest_configure`` time, before the first test can patch
#: anything.  ``None`` means the reference could not be taken; the guard then
#: stays silent rather than reporting a leak it cannot substantiate.
_COORDINATOR_IDENTITY_REFERENCE: type[Any] | None = None

#: Module that *defines* ``GoogleFindMyCoordinator``.  Deliberately not the
#: ``coordinator`` package: that one is the re-export other modules copy from
#: and the one test harnesses patch, so it cannot serve as its own reference.
_COORDINATOR_DEFINING_MODULE = "custom_components.googlefindmy.coordinator.main"

#: Modules that bind ``GoogleFindMyCoordinator`` into their own namespace at
#: import time (``from .coordinator import GoogleFindMyCoordinator`` at module
#: level).  Each copies whatever ``coordinator`` holds at the moment of its
#: first import, so an import that happens *inside* a patch window captures the
#: stub permanently: ``monkeypatch`` only undoes the assignments it made itself
#: and knows nothing about the copies.  Every harness that patches the symbol
#: therefore imports this list before it patches anything.
#:
#: Lives here rather than in one test module because three harnesses need it;
#: two of them used to carry hand-written, incomplete copies.  Derived by AST
#: scan, pinned by ``tests/test_guard_coordinator_identity.py``.
COORDINATOR_CONSUMER_MODULES: tuple[str, ...] = (
    "custom_components.googlefindmy.binary_sensor",
    "custom_components.googlefindmy.button",
    "custom_components.googlefindmy.device_tracker",
    "custom_components.googlefindmy.eid_resolver",
    "custom_components.googlefindmy.entity",
    "custom_components.googlefindmy.repairs",
    "custom_components.googlefindmy.sensor",
)


def import_coordinator_consumers() -> None:
    """Import every module of ``COORDINATOR_CONSUMER_MODULES``.

    One literal ``import`` statement per module instead of a loop with
    ``importlib.import_module``: the module set is fixed, so naming each module
    keeps the import visible to readers, linters and SAST tools.
    ``tests/test_guard_coordinator_identity.py`` pins that these statements name
    exactly the modules of the tuple, so the two cannot drift apart.
    """
    import custom_components.googlefindmy.binary_sensor  # noqa: F401
    import custom_components.googlefindmy.button  # noqa: F401
    import custom_components.googlefindmy.device_tracker  # noqa: F401
    import custom_components.googlefindmy.eid_resolver  # noqa: F401
    import custom_components.googlefindmy.entity  # noqa: F401
    import custom_components.googlefindmy.repairs  # noqa: F401
    import custom_components.googlefindmy.sensor  # noqa: F401


#: Package prefix the guard scans.  Compared as ``name == PREFIX`` or
#: ``name.startswith(PREFIX + ".")`` so a hypothetical sibling package such as
#: ``custom_components.googlefindmy_legacy`` is not swept in by accident.
_INTEGRATION_PACKAGE = "custom_components.googlefindmy"

#: Modules exempt from the identity check.  The integration package binds the
#: symbol lazily through ``_ensure_runtime_imports()`` (see the teardown hook
#: below), so a placeholder there is an expected, self-healing state rather
#: than a leak.  This exemption alone carries that case: the healing helper does
#: not influence it.
_COORDINATOR_IDENTITY_EXEMPT = frozenset({_INTEGRATION_PACKAGE})


def _freeze_coordinator_identity_reference() -> None:
    """Remember the production coordinator class before any test runs.

    Load-bearing side effect: importing ``coordinator.main`` also imports its
    parent package ``custom_components.googlefindmy.coordinator``.  That keeps
    the fallback branch in ``tests/test_button_setup.py`` (which would install a
    permanent, unpatched ``ModuleType`` stub under that name) unreachable.
    Making this reference lazy would re-open it, and the guard would then report
    an unhealable leak.
    """

    global _COORDINATOR_IDENTITY_REFERENCE

    try:
        module = importlib.import_module(_COORDINATOR_DEFINING_MODULE)
    except Exception as err:  # noqa: BLE001 - a missing reference disables the guard
        # Never silent: without a reference the guard returns "no leaks" for the
        # whole session, so the reason has to reach whoever wonders why
        # ``test_guard_reports_a_replaced_symbol`` went red.
        warnings.warn(
            "coordinator identity guard disabled: could not import "
            f"{_COORDINATOR_DEFINING_MODULE} ({err!r})",
            RuntimeWarning,
            stacklevel=2,
        )
        return
    _COORDINATOR_IDENTITY_REFERENCE = getattr(module, "GoogleFindMyCoordinator", None)


def detect_coordinator_identity_leaks() -> list[str]:
    """Return the modules whose ``GoogleFindMyCoordinator`` is not the real one.

    A module that does ``from .coordinator import GoogleFindMyCoordinator`` at
    import time takes a *copy* of whatever the source module holds right then.
    When that import happens inside a ``monkeypatch`` window, the copy is the
    test stub, and ``monkeypatch`` cannot undo a binding it never made: the
    stub then survives for the rest of the session and every later
    ``isinstance`` check against the production class fails somewhere else
    entirely.  Checking identity after each test names the culprit instead of
    the victim.

    Two blind spots, both accepted knowingly:

    * The reference comes from ``coordinator.main``.  A test that patches the
      class *there* would move the yardstick along with the measurement, which
      is why the reference is frozen at configure time rather than read on
      demand.
    * ``importlib.reload(coordinator.main)`` would build a fresh class object.
      Every consumer reloaded afterwards would then differ from the frozen
      reference permanently, without a leak being present.  No test reloads
      that module today.
    """

    reference = _COORDINATOR_IDENTITY_REFERENCE
    if reference is None:
        return []

    leaked: list[str] = []
    for name, module in list(sys.modules.items()):
        if name != _INTEGRATION_PACKAGE and not name.startswith(
            _INTEGRATION_PACKAGE + "."
        ):
            continue
        if name in _COORDINATOR_IDENTITY_EXEMPT:
            continue
        current = getattr(module, "GoogleFindMyCoordinator", None)
        if current is not None and current is not reference:
            leaked.append(name)
    return sorted(leaked)


def _restore_coordinator_identity(modules: list[str]) -> None:
    """Put the production class back into the modules a test poisoned.

    Reporting alone is not enough.  The stub stays in the consumer's namespace,
    so without this the *next* test, and every test after it, fails in teardown
    for a leak it did not cause: one culprit turns into a red session and the
    diagnosis gets worse, not better.  Healing keeps the failure attributable to
    exactly one test.
    """

    reference = _COORDINATOR_IDENTITY_REFERENCE
    if reference is None:
        return
    for name in modules:
        module = sys.modules.get(name)
        if module is not None:
            module.GoogleFindMyCoordinator = reference


@pytest.fixture(scope="session", autouse=True)
def enable_real_sockets() -> Iterable[None]:
    """Allow socket usage across the test session for network-marked tests."""

    pytest_socket.enable_socket()
    pytest_socket.socket_allow_hosts(["127.0.0.1", "::1", "localhost"])
    yield


@pytest.fixture(scope="session", autouse=True)
def _prime_leaflet_asset_cache() -> Iterable[None]:
    """Fill ``map_view``'s Leaflet cache once per session.

    In production the cache is filled by ``_async_prime_leaflet_cache`` in the
    executor, and ``_leaflet_asset`` raises on a miss instead of reading through
    (a read-through is the blocking call in the event loop that the indirection
    removes). Tests that call ``_generate_map_html`` directly bypass the request
    handler and would therefore hit that ``RuntimeError``; this fixture stands in
    for the handler. Reading here is safe because this is a *synchronous*
    fixture: no event loop is running while it executes. Keep it synchronous.

    Tests that need a cold cache clear it locally (``monkeypatch.setattr`` on a
    fresh dict), never by mutating this one.

    Because this fixture warms the cache for the whole session, the end-to-end
    map tests can no longer witness a return of the blocking read. The
    compensating watchdog is ``tests/test_map_view_blocking_io.py``, which
    clears the cache per test and asserts the reading thread. Do not delete that
    file without replacing that measurement, or this fixture turns into a
    cover-up.
    """

    from custom_components.googlefindmy import map_view

    try:
        map_view._LEAFLET_CACHE.update(map_view._read_leaflet_assets())
    except (OSError, UnicodeDecodeError):
        # An incomplete checkout must not error out every test in the session
        # during setup. Let the map tests fail on their own, where the message
        # points at the cause.
        pass
    yield


@pytest.fixture(scope="session", autouse=True)
def _redirect_ha_test_storage(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterable[None]:
    """Redirect the bundled HA test config dir to a writable tmp location.

    The HA test plugin ships ``testing_config/`` *inside* site-packages. On
    shutdown (``hass.async_stop``) the entity/device registry performs a final
    write into ``<testing_config>/.storage``. When site-packages is read-only
    (e.g. a root-owned venv driven by a non-root test runner), that write raises
    ``PermissionError`` and surfaces as a teardown error. Pointing
    ``get_test_config_dir`` at a writable copy makes the suite environment-
    independent, mirroring the intent of the plugin's own ``mock_storage``.

    ``common`` is imported lazily *inside* the fixture on purpose: importing it
    at module scope trips the plugin's ``patch_recorder`` assertion.
    """

    import os
    import shutil

    import pytest_homeassistant_custom_component.common as _ph

    original = _ph.get_test_config_dir
    bundled = original()
    target = str(tmp_path_factory.mktemp("ha_testing_config"))
    # Preserve the bundled assets (configuration.yaml, etc.) other tests read,
    # while making the directory — and thus .storage — writable.
    shutil.copytree(bundled, target, dirs_exist_ok=True)

    def _writable_test_config_dir(*add: str) -> str:
        return os.path.join(target, *add)

    _ph.get_test_config_dir = _writable_test_config_dir
    try:
        yield
    finally:
        _ph.get_test_config_dir = original


@pytest.hookimpl(trylast=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Re-enable sockets after other plugins run setup hooks."""

    pytest_socket.enable_socket()
    pytest_socket.socket_allow_hosts(["127.0.0.1", "::1", "localhost"])


from tests.helpers import deprecation_recorder as _deprecation_recorder
from tests.helpers import (
    install_homeassistant_core_callback_stub,
    install_homeassistant_network_stub,
)
from tests.helpers import single_owner_device_registry as _single_owner_registry
from tests.helpers.config_entries_stub import install_config_entries_stubs
from tests.helpers.constants import load_googlefindmy_const_module
from tests.helpers.core_shutdown_state import seed_core_shutdown_state

ConfigEntryAuthFailed: type[Exception] = Exception


if importlib.util.find_spec("homeassistant") is None:
    raise RuntimeError(
        "The real 'homeassistant' package must be installed for the Google Find My Device "
        "test suite. Run 'pip install homeassistant pytest-homeassistant-custom-component' "
        "before executing pytest."
    )

import homeassistant.components.http as ha_http

if not hasattr(ha_http, "start_http_server_and_save_config"):

    def start_http_server_and_save_config(*_: Any, **__: Any) -> None:
        """Fallback stub for HA builds without HTTP server helper."""

        return None

    ha_http.start_http_server_and_save_config = start_http_server_and_save_config

_PYTEST_HOMEASSISTANT_PLUGIN_AVAILABLE = (
    importlib.util.find_spec("pytest_homeassistant_custom_component") is not None
)

if not _PYTEST_HOMEASSISTANT_PLUGIN_AVAILABLE:
    raise RuntimeError(
        "Missing dependency: pytest-homeassistant-custom-component. Install it together "
        "with homeassistant before running the test suite."
    )


@pytest.fixture(autouse=True)
def disable_http_server() -> Iterable[None]:
    """Stub Home Assistant's HTTP server helper for plugin patches."""

    if not hasattr(ha_http, "start_http_server_and_save_config"):
        ha_http.start_http_server_and_save_config = start_http_server_and_save_config

    with patch(
        "homeassistant.components.http.start_http_server_and_save_config",
        create=True,
    ):
        yield


_FOREIGN_READING_TRACKER_MODULE = (
    "custom_components.googlefindmy.NovaApi.ExecuteAction.LocateTracker."
    "foreign_reading_tracker"
)


def _reset_foreign_reading_tracker() -> None:
    # Only when already imported: importing it here would pull the integration
    # into tests that install their own stubs before the first import.
    module = sys.modules.get(_FOREIGN_READING_TRACKER_MODULE)
    if module is not None:
        module.FOREIGN_READING_TRACKER.reset()


@pytest.fixture(autouse=True)
def reset_foreign_reading_tracker() -> Iterable[None]:
    """Isolate the process-wide foreign-reading memory between tests.

    The tracker keeps once-per-process log gates and remembered readings; a
    test that runs after another could otherwise see no log line at all.
    """
    _reset_foreign_reading_tracker()
    yield
    _reset_foreign_reading_tracker()


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown() -> None:
    """Heal the lazy symbol, then fail the culprit of a cross-test symbol leak.

    Two independent jobs share this hook because both need the same moment:
    after every fixture finalizer, ``monkeypatch`` included.

    **Part one** heals the lazy ``GoogleFindMyCoordinator`` symbol in the
    integration package (details below).

    **Part two** is the cross-test guard:
    :func:`detect_coordinator_identity_leaks` compares every integration
    module's ``GoogleFindMyCoordinator`` against the class frozen at configure
    time.  A mismatch means a stub outlived its ``monkeypatch`` window; the test
    that caused it fails here rather than an unrelated test file failing minutes
    later.  For that promise to hold, the symbol is *restored* before the
    failure is raised: reporting alone would leave the stub in place and redden
    every following test as well.

    The two parts are independent, and the order between them does not matter:
    part one only touches the integration package itself, and that package is
    exempt from part two's check anyway (see
    ``_COORDINATOR_IDENTITY_EXEMPT``).

    The integration package binds ``GoogleFindMyCoordinator`` lazily: it starts
    as a placeholder class and ``_ensure_runtime_imports()`` swaps in the real
    class while flipping ``_RUNTIME_IMPORTS_LOADED`` to ``True`` (a direct global
    assignment, invisible to ``monkeypatch``). Several tests ``monkeypatch`` that
    symbol while it is still the placeholder; if real runtime imports run during
    the test, teardown restores the *placeholder* as the symbol's value while
    ``_RUNTIME_IMPORTS_LOADED`` stays ``True``. The guard then never rebinds the
    real class, so every later ``isinstance(obj, GoogleFindMyCoordinator)`` check
    silently fails. Under xdist this leaks across tests in a worker and breaks
    unrelated ones (observed: ``test_device_identifier_normalization`` skipping
    its purge_device branch).

    This runs as a ``trylast`` teardown hook (not a fixture) so it executes after
    every fixture finalizer, in particular after ``monkeypatch`` has restored the
    placeholder. It detects the inconsistent state (guard set, yet any lazily
    bound symbol is still a placeholder) and clears the guard so the next
    ``_ensure_runtime_imports()`` rebinds the real classes. All four lazily bound
    classes are checked, not only the coordinator, because the same monkeypatch
    trap applies symmetrically to ``DiscoveryManager`` and the two map views.
    """

    _heal_lazy_runtime_imports()

    leaked = detect_coordinator_identity_leaks()
    if leaked:
        _restore_coordinator_identity(leaked)
        pytest.fail(
            "this test left a foreign GoogleFindMyCoordinator in "
            f"{leaked}. A module that imports the symbol at module level copies "
            "it; when that first import happens inside a monkeypatch window the "
            "copy is the stub and monkeypatch cannot undo it. Import those "
            "modules before patching, or patch them too.",
            pytrace=False,
        )


def _heal_lazy_runtime_imports() -> None:
    """Rebind the integration package's lazily loaded runtime classes."""

    integration = sys.modules.get("custom_components.googlefindmy")
    if integration is None:
        return
    if not getattr(integration, "_RUNTIME_IMPORTS_LOADED", False):
        return

    lazy_symbols = (
        "GoogleFindMyCoordinator",
        "DiscoveryManager",
        "GoogleFindMyMapView",
        "GoogleFindMyMapRedirectView",
        "GoogleFindMyMapTilesTokenView",
    )
    any_placeholder = any(
        "Placeholder" in getattr(getattr(integration, name, None), "__name__", "")
        for name in lazy_symbols
    )
    if not any_placeholder:
        return

    # Open the guard so the real classes get rebound. If rebinding fails (e.g. a
    # test left an incomplete submodule stub in sys.modules), the state stays
    # {loaded=False, placeholder}, which is consistent and self-heals on the next
    # _ensure_runtime_imports() call; swallowing avoids reddening an unrelated
    # test's teardown over that benign, recoverable condition.
    integration._RUNTIME_IMPORTS_LOADED = False
    try:
        integration._ensure_runtime_imports()
    except Exception:  # noqa: BLE001 - see comment above; left state is consistent
        integration._RUNTIME_IMPORTS_LOADED = False


_CONST_MODULE = load_googlefindmy_const_module()
_SERVICE_DEVICE_NAME: str = getattr(
    _CONST_MODULE, "SERVICE_DEVICE_NAME", "Google Find My Integration"
)
_SERVICE_DEVICE_MODEL: str = getattr(
    _CONST_MODULE, "SERVICE_DEVICE_MODEL", "Find My Device Integration"
)
_SERVICE_DEVICE_MANUFACTURER: str = getattr(
    _CONST_MODULE, "SERVICE_DEVICE_MANUFACTURER", "BSkando"
)
_SERVICE_DEVICE_TRANSLATION_KEY: str = getattr(
    _CONST_MODULE, "SERVICE_DEVICE_TRANSLATION_KEY", "google_find_hub_service"
)
_INTEGRATION_VERSION: str = getattr(_CONST_MODULE, "INTEGRATION_VERSION", "0.0.0")
_SERVICE_DEVICE_IDENTIFIER: Callable[[str], tuple[str, str]] = getattr(
    _CONST_MODULE, "service_device_identifier"
)


if _PYTEST_HOMEASSISTANT_PLUGIN_AVAILABLE:

    @pytest.fixture(autouse=True)
    def enable_event_loop_debug(
        request: pytest.FixtureRequest,
    ) -> Iterable[None]:
        """Ensure the pytest-homeassistant plugin sees an active event loop."""

        loop: asyncio.AbstractEventLoop
        created_loop = False
        try:
            loop = request.getfixturevalue("event_loop")
        except pytest.FixtureLookupError:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                # pytest-homeassistant-custom-component calls get_event_loop()
                # internally, so we must register the loop for compatibility.
                asyncio.set_event_loop(loop)  # noqa: ASYNC110
                created_loop = True
        loop.set_debug(True)
        try:
            yield
        finally:
            loop.set_debug(False)
            if created_loop:
                loop.close()
                asyncio.set_event_loop(None)  # noqa: ASYNC110


class _FakeIssueRegistry:
    """Lightweight repair issue registry used across tests."""

    def __init__(self) -> None:
        self.issues: dict[str, dict[str, Any]] = {}

    def async_get_issue(self, domain: str, issue_id: str) -> dict[str, Any] | None:
        issue = self.issues.get(issue_id)
        if issue and issue.get("domain") == domain:
            return issue
        return None


@dataclass(slots=True)
class IssueRegistryCapture:
    """Record repair issue interactions while stubbing the registry."""

    created: list[dict[str, Any]] = field(default_factory=list)
    deleted: list[tuple[str, str]] = field(default_factory=list)
    registry: _FakeIssueRegistry = field(default_factory=_FakeIssueRegistry)


@pytest.fixture
def credentialed_config_entry_data() -> Callable[..., dict[str, Any]]:
    """Return pre-populated config entry data that satisfies credential guards."""

    def _factory(
        *,
        email: str = "user@example.com",
        oauth_token: str = "oauth-token",
        aas_token: str | None = "aas-token",
        android_id: str = "0xC0FFEE",
        security_token: str = "0xFACEFEED",
        fcm_token: str = "fcm-token",
        extra_bundle_fields: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        bundle: dict[str, Any] = {
            "username": email,
            "Email": email,
            "oauth_token": oauth_token,
            "aas_token": aas_token,
            "fcm_credentials": {
                "gcm": {
                    "android_id": android_id,
                    "security_token": security_token,
                },
                "fcm": {"token": fcm_token},
            },
        }
        if extra_bundle_fields:
            bundle.update(extra_bundle_fields)

        sanitized_bundle = {
            key: value for key, value in bundle.items() if value is not None
        }

        const = load_googlefindmy_const_module()

        entry_data: dict[str, Any] = {
            getattr(const, "DATA_SECRET_BUNDLE"): sanitized_bundle,
            getattr(const, "CONF_GOOGLE_EMAIL"): email,
        }

        if oauth_token:
            entry_data[getattr(const, "CONF_OAUTH_TOKEN")] = oauth_token
        if aas_token:
            entry_data[getattr(const, "DATA_AAS_TOKEN")] = aas_token

        return entry_data

    return _factory


@pytest.fixture
def stub_homeassistant_network(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., ModuleType]:
    """Provide a configurable stub for Home Assistant network helpers."""

    def _install(
        *,
        url: str | None = "https://example.local",
        error: Exception | type[Exception] | None = None,
    ) -> ModuleType:
        network_module = install_homeassistant_network_stub(
            monkeypatch,
            get_url_result=url,
            get_url_error=error,
        )

        monkeypatch.setattr(
            "custom_components.googlefindmy.entity.get_url",
            network_module.get_url,
            raising=False,
        )
        monkeypatch.setattr(
            "custom_components.googlefindmy.entity.NoURLAvailableError",
            network_module.NoURLAvailableError,
            raising=False,
        )

        return network_module

    return _install


@pytest.fixture
def hass_executor_stub() -> Callable[..., SimpleNamespace]:
    """Return a factory that yields hass stubs with executor support."""

    async def _async_add_executor_job(
        func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        result = func(*args, **kwargs)
        if inspect.isawaitable(result):
            return await result
        return result

    def _factory(**attrs: Any) -> SimpleNamespace:
        return SimpleNamespace(
            async_add_executor_job=_async_add_executor_job,
            **attrs,
        )

    return _factory


@pytest.fixture
def issue_registry_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> IssueRegistryCapture:
    """Provide a patched issue registry and capture create/delete calls."""

    capture = IssueRegistryCapture()

    def _create_issue(
        hass: Any,
        domain: str,
        issue_id: str,
        *,
        translation_placeholders: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        payload = {
            "issue_id": issue_id,
            "domain": domain,
            "translation_placeholders": dict(translation_placeholders or {}),
            # Every other keyword (severity, breaks_in_ha_version,
            # translation_key, is_fixable, is_persistent, ...) as passed.
            **kwargs,
        }
        capture.created.append(payload)
        capture.registry.issues[issue_id] = payload

    def _delete_issue(hass: Any, domain: str, issue_id: str) -> None:
        capture.deleted.append((domain, issue_id))
        capture.registry.issues.pop(issue_id, None)

    monkeypatch.setattr(
        "custom_components.googlefindmy.ir.async_get", lambda hass: capture.registry
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.ir.async_create_issue", _create_issue
    )
    monkeypatch.setattr(
        "custom_components.googlefindmy.ir.async_delete_issue", _delete_issue
    )

    return capture


@pytest.fixture
def deterministic_config_subentry_id(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Any, str, str | None], str]:
    """Provide deterministic config_subentry_id fallbacks for platform setup."""

    modules = [
        importlib.import_module(path)
        for path in (
            "custom_components.googlefindmy.entity",
            "custom_components.googlefindmy.binary_sensor",
            "custom_components.googlefindmy.button",
            "custom_components.googlefindmy.sensor",
            "custom_components.googlefindmy.device_tracker",
        )
    ]

    assigned: dict[tuple[str, str], str] = {}

    def _assign(entry: Any, platform: str, candidate: str | None, **kwargs: Any) -> str:
        known_ids = kwargs.get("known_ids")
        if isinstance(known_ids, Iterable):
            known_ids = [
                value for value in known_ids if isinstance(value, str) and value
            ]
        else:
            known_ids = []

        if isinstance(candidate, str):
            normalized = candidate.strip()
            if normalized:
                return normalized

        if known_ids:
            return known_ids[0]

        entry_id = getattr(entry, "entry_id", "<unknown-entry>")
        if not isinstance(entry_id, str) or not entry_id:
            entry_id = "<unknown-entry>"

        key = (entry_id, platform)
        fallback = assigned.get(key)
        if fallback is None:
            fallback = f"{entry_id}:{platform}"
            assigned[key] = fallback
        return fallback

    for module in modules:
        monkeypatch.setattr(module, "ensure_config_subentry_id", _assign, raising=False)

    return _assign


@pytest.fixture
def subentry_support(monkeypatch: pytest.MonkeyPatch):
    """Toggle config subentry support between modern and legacy cores."""

    from custom_components.googlefindmy import config_flow

    original_flow = getattr(config_flow, "ConfigSubentryFlow", None)
    original_subentry = getattr(config_flow, "ConfigSubentry", None)
    fallback_flow = getattr(config_flow, "_FALLBACK_CONFIG_SUBENTRY_FLOW", None)

    class _SubentrySupportToggle:
        """Helper exposing modern and legacy subentry configurations."""

        def as_modern(self) -> object | None:
            """Restore the module to the original subentry-capable state."""

            monkeypatch.setattr(
                config_flow,
                "ConfigSubentryFlow",
                original_flow,
                raising=False,
            )
            monkeypatch.setattr(
                config_flow,
                "_FALLBACK_CONFIG_SUBENTRY_FLOW",
                fallback_flow,
                raising=False,
            )
            monkeypatch.setattr(
                config_flow,
                "ConfigSubentry",
                original_subentry,
                raising=False,
            )
            return original_flow

        def as_legacy(self) -> type[object]:
            """Simulate a core that lacks ConfigSubentry support."""

            legacy_flow = fallback_flow
            if legacy_flow is None:
                legacy_flow = type(
                    "_LegacyFallbackSubentryFlow",
                    (),
                    {"__doc__": "Fallback stub for legacy config subentries."},
                )

            monkeypatch.setattr(
                config_flow,
                "ConfigSubentryFlow",
                legacy_flow,
                raising=False,
            )
            monkeypatch.setattr(
                config_flow,
                "_FALLBACK_CONFIG_SUBENTRY_FLOW",
                legacy_flow,
                raising=False,
            )
            monkeypatch.setattr(
                config_flow,
                "ConfigSubentry",
                None,
                raising=False,
            )
            return legacy_flow

    toggle = _SubentrySupportToggle()
    toggle.as_modern()
    return toggle


# Ensure the package root is importable without installing the package.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

INTEGRATION_ROOT = ROOT / "custom_components" / "googlefindmy"

SERVICE_SUBENTRY_KEY: str = "service"
TRACKER_SUBENTRY_KEY: str = "tracker"


def pytest_addoption(parser: pytest.Parser) -> None:
    """Accept the asyncio mode ini option when pytest-asyncio is absent."""

    if importlib.util.find_spec("pytest_asyncio") is not None:
        return

    try:
        parser.addini(
            "asyncio_mode",
            "Default asyncio mode placeholder when pytest-asyncio is unavailable.",
        )
    except ValueError:
        # Another plugin already registered the option; reuse it.
        return


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    """Execute asyncio-marked coroutine tests without requiring pytest-asyncio."""

    marker = pyfuncitem.get_closest_marker("asyncio")
    if marker is None or not inspect.iscoroutinefunction(pyfuncitem.obj):
        return None

    loop = asyncio.new_event_loop()
    try:
        argnames = getattr(pyfuncitem._fixtureinfo, "argnames", ())  # noqa: SLF001 - pytest internals
        if any(
            param.kind is inspect.Parameter.VAR_KEYWORD
            for param in inspect.signature(pyfuncitem.obj).parameters.values()
        ):
            call_kwargs = pyfuncitem.funcargs
        else:
            call_kwargs = {
                name: pyfuncitem.funcargs[name]
                for name in argnames
                if name in pyfuncitem.funcargs
            }
        loop.run_until_complete(pyfuncitem.obj(**call_kwargs))
    finally:
        loop.close()
    return True


def _stub_homeassistant() -> None:
    """Install lightweight stubs for Home Assistant modules required at import time."""

    global ConfigEntryAuthFailed
    ha_pkg = sys.modules.setdefault("homeassistant", ModuleType("homeassistant"))
    ha_pkg.__path__ = getattr(ha_pkg, "__path__", [])  # mark as package

    try:
        from homeassistant.util import dt as dt_util  # type: ignore[import-not-found]

        dt_util.DEFAULT_TIME_ZONE = dt_util.UTC
    except Exception:  # pragma: no cover - defensive guard when HA is absent
        pass

    config_entries = ModuleType("homeassistant.config_entries")
    config_entries.SOURCE_DISCOVERY = "discovery"
    config_entries.SOURCE_RECONFIGURE = "reconfigure"

    install_config_entries_stubs(config_entries)
    ConfigEntryAuthFailed = getattr(config_entries, "ConfigEntryAuthFailed", Exception)
    sys.modules["homeassistant.config_entries"] = config_entries

    try:
        import voluptuous as vol_module  # type: ignore[import]
    except ImportError:
        vol_module = ModuleType("voluptuous")

        class _Schema:
            def __init__(self, schema, *args, **kwargs):
                self.schema = schema

            def __call__(self, value):  # pragma: no cover - defensive
                return value

        class _Marker:
            def __init__(self, key):
                self.key = key
                self.schema = {key}

            def __hash__(self) -> int:  # pragma: no cover - defensive
                return hash(self.key)

            def __eq__(self, other: object) -> bool:  # pragma: no cover - defensive
                if isinstance(other, _Marker):
                    return self.key == other.key
                return self.key == other

        class _Invalid(Exception):
            pass

        class _MultipleInvalid(_Invalid):
            pass

        def _identity(value):
            return value

        vol_module.Schema = _Schema  # type: ignore[attr-defined]
        vol_module.Invalid = _Invalid  # type: ignore[attr-defined]
        vol_module.MultipleInvalid = _MultipleInvalid  # type: ignore[attr-defined]
        vol_module.ALLOW_EXTRA = object()  # type: ignore[attr-defined]
        vol_module.PREVENT_EXTRA = object()  # type: ignore[attr-defined]

        def _optional(key, default=None, **_: object) -> _Marker:
            return _Marker(key)

        def _required(key, description=None, default=None, **_: object) -> _Marker:
            return _Marker(key)

        vol_module.Optional = _optional  # type: ignore[attr-defined]
        vol_module.Required = _required  # type: ignore[attr-defined]
        vol_module.Any = lambda *items, **kwargs: _identity  # type: ignore[attr-defined]
        vol_module.All = lambda *validators, **kwargs: _identity  # type: ignore[attr-defined]
        vol_module.In = lambda items: _identity  # type: ignore[attr-defined]
        vol_module.Range = lambda **kwargs: _identity  # type: ignore[attr-defined]
        vol_module.Coerce = lambda typ: _identity  # type: ignore[attr-defined]
        vol_module.Match = lambda pattern, msg=None: _identity  # type: ignore[attr-defined]
        vol_module.ExactSequence = lambda seq: _identity  # type: ignore[attr-defined]
        vol_module.Clamp = lambda **kwargs: _identity  # type: ignore[attr-defined]
        sys.modules["voluptuous"] = vol_module
    else:
        sys.modules["voluptuous"] = vol_module

    data_entry_flow = ModuleType("homeassistant.data_entry_flow")

    class _FlowResultType:
        """Enum-like container mirroring Home Assistant's flow result types."""

        ABORT = "abort"
        CREATE_ENTRY = "create_entry"
        FORM = "form"
        MENU = "menu"

    class _AbortFlow(Exception):
        """Stub AbortFlow matching Home Assistant's interface."""

        def __init__(self, reason: str) -> None:
            super().__init__(reason)
            self.reason = reason

    data_entry_flow.AbortFlow = _AbortFlow  # type: ignore[attr-defined]
    data_entry_flow.FlowResultType = _FlowResultType  # type: ignore[attr-defined]
    data_entry_flow.FlowResult = dict  # type: ignore[assignment]
    sys.modules["homeassistant.data_entry_flow"] = data_entry_flow

    const_module = ModuleType("homeassistant.const")

    class Platform:  # enum-like stub covering platforms used in __init__
        DEVICE_TRACKER = "device_tracker"
        BUTTON = "button"
        SENSOR = "sensor"
        BINARY_SENSOR = "binary_sensor"

    const_module.EVENT_HOMEASSISTANT_STARTED = "start"
    const_module.EVENT_HOMEASSISTANT_STOP = "stop"
    const_module.ATTR_LATITUDE = "latitude"
    const_module.ATTR_LONGITUDE = "longitude"
    const_module.ATTR_GPS_ACCURACY = "gps_accuracy"
    const_module.STATE_UNAVAILABLE = "unavailable"
    const_module.STATE_UNKNOWN = "unknown"
    const_module.STATE_HOME = "home"  # For FMDN Finder location filtering
    const_module.EVENT_STATE_CHANGED = (
        "state_changed"  # For FMDN Finder event listening
    )
    const_module.Platform = Platform
    const_module.PERCENTAGE = "%"
    sys.modules["homeassistant.const"] = const_module

    loader_module = ModuleType("homeassistant.loader")

    class _StubIntegration:
        """Minimal Integration stand-in that exposes component retrieval hooks."""

        def __init__(self, domain: str) -> None:
            self.domain = domain
            self.name = domain
            self.version = "0.0.0"
            self.requirements: list[str] = []
            self.dependencies: list[str] = []

        async def async_get_component(self) -> ModuleType | SimpleNamespace:
            domain = self.domain if isinstance(self.domain, str) else ""
            if domain == "googlefindmy":
                try:
                    return importlib.import_module(f"custom_components.{domain}")
                except ModuleNotFoundError:
                    return SimpleNamespace(__name__=domain)

            return SimpleNamespace(__name__=domain or "unknown_integration")

    async def _async_get_integration(_hass, _domain):  # pragma: no cover - stub
        return _StubIntegration(_domain)

    loader_module.async_get_integration = _async_get_integration
    sys.modules["homeassistant.loader"] = loader_module
    setattr(ha_pkg, "loader", loader_module)

    core_module = ModuleType("homeassistant.core")

    class CoreState:  # minimal CoreState stub
        running = "running"

    class Event(SimpleNamespace):
        """Minimal Event stub carrying type and data payload."""

        def __init__(
            self, event_type: str, data: Mapping[str, Any] | None = None
        ) -> None:
            super().__init__(event_type=event_type, data=data or {})

    class State(SimpleNamespace):
        """Minimal State stub for entity states."""

        def __init__(
            self,
            entity_id: str,
            state: str,
            attributes: Mapping[str, Any] | None = None,
        ) -> None:
            super().__init__(
                entity_id=entity_id, state=state, attributes=attributes or {}
            )

    class HomeAssistant:  # minimal HomeAssistant placeholder
        state = CoreState.running

    core_module.CoreState = CoreState
    core_module.HomeAssistant = HomeAssistant
    core_module.Event = Event
    core_module.State = State
    core_module.CALLBACK_TYPE = Callable[[], None]
    install_homeassistant_core_callback_stub(module=core_module, overwrite=True)

    exceptions_module = ModuleType("homeassistant.exceptions")

    class HomeAssistantError(Exception):
        """Stubbed HomeAssistantError accepting the core translation metadata.

        Mirrors ``homeassistant.exceptions.HomeAssistantError.__init__``: the
        three translation arguments are stored as attributes, and without a
        message but with domain and key the key becomes the only argument. The
        core then looks the key up in the translation cache of the running
        instance; the stub renders the raw key, which is what the core returns
        when nothing is cached. That is the least favourable case, so a wrapper
        that builds its text from such an exception shows the raw key in tests
        too. ``ServiceValidationError`` below keeps its own ``__str__``
        (``"domain:key"``); see its docstring.
        ``tests/test_exception_stub_translation_parity.py`` holds the mirror.
        The class defaults match the core, so a subclass that skips
        ``super().__init__`` still reads ``None`` and ``False``.
        """

        generate_message: bool = False
        translation_domain: str | None = None
        translation_key: str | None = None
        translation_placeholders: Mapping[str, Any] | None = None

        def __init__(
            self,
            *args: Any,
            translation_domain: str | None = None,
            translation_key: str | None = None,
            translation_placeholders: Mapping[str, Any] | None = None,
        ) -> None:
            if not args and translation_key and translation_domain:
                self.generate_message = True
                args = (translation_key,)
            super().__init__(*args)
            self.translation_domain = translation_domain
            self.translation_key = translation_key
            self.translation_placeholders = translation_placeholders

    class ConfigEntryNotReady(HomeAssistantError):
        """Stubbed ConfigEntryNotReady falling back to its cause like the core.

        The core's ``IntegrationError.__str__`` returns ``str(self.__cause__)``
        when the exception carries no message.
        """

        def __str__(self) -> str:
            return super().__str__() or str(self.__cause__)

    class ServiceValidationError(HomeAssistantError):
        """Stubbed ServiceValidationError carrying translation metadata.

        Unlike the base stub (and the core), a key without a message renders
        as ``"domain:key"`` here; tests assert on that form (``tests/AGENTS.md``,
        string representation contract), so it stays.
        """

        def __init__(
            self,
            *args: Any,
            translation_domain: str | None = None,
            translation_key: str | None = None,
            translation_placeholders: Mapping[str, Any] | None = None,
            **kwargs: Any,
        ) -> None:
            domain_fragment = translation_domain or ""
            key_fragment = translation_key or ""
            if domain_fragment and key_fragment:
                derived_message = f"{domain_fragment}:{key_fragment}"
            else:
                derived_message = (
                    key_fragment or domain_fragment or "Service validation error"
                )

            if args:
                super().__init__(*args)
                message = " ".join(str(arg) for arg in args)
            else:
                message = derived_message
                super().__init__(message)

            self.translation_domain = translation_domain
            self.translation_key = translation_key
            self.translation_placeholders = (
                None
                if translation_placeholders is None
                else dict(translation_placeholders)
            )
            self.message = message

        def __str__(self) -> str:  # pragma: no cover - deterministic for asserts
            return self.message

        def __repr__(self) -> str:  # pragma: no cover - mirrors __str__
            return self.message

    exceptions_module.HomeAssistantError = HomeAssistantError
    exceptions_module.ConfigEntryNotReady = ConfigEntryNotReady
    exceptions_module.ServiceValidationError = ServiceValidationError
    exceptions_module.ConfigEntryAuthFailed = ConfigEntryAuthFailed
    sys.modules["homeassistant.exceptions"] = exceptions_module
    from tests.helpers.service_doubles import install_service_auth_stubs

    install_service_auth_stubs(
        core_module=core_module, exceptions_module=exceptions_module
    )

    helpers_pkg = sys.modules.setdefault(
        "homeassistant.helpers", ModuleType("homeassistant.helpers")
    )
    helpers_pkg.__path__ = getattr(helpers_pkg, "__path__", [])

    for sub in (
        "device_registry",
        "entity_registry",
        "issue_registry",
        "update_coordinator",
    ):
        module_name = f"homeassistant.helpers.{sub}"
        module = ModuleType(module_name)
        sys.modules[module_name] = module
        setattr(helpers_pkg, sub, module)

    frame_module = ModuleType("homeassistant.helpers.frame")
    frame_module._configured_instances: list[Any] = []

    def frame_set_up(hass: Any) -> None:
        frame_module._configured_instances.append(hass)

    def frame_report(
        *args: Any, **kwargs: Any
    ) -> None:  # pragma: no cover - optional stub
        return None

    frame_module.set_up = frame_set_up
    frame_module.report = frame_report
    sys.modules["homeassistant.helpers.frame"] = frame_module
    setattr(helpers_pkg, "frame", frame_module)

    entity_module = ModuleType("homeassistant.helpers.entity")

    class DeviceInfo:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    entity_module.DeviceInfo = DeviceInfo

    def split_entity_id(entity_id: str) -> tuple[str, str]:
        if "." not in entity_id:
            raise ValueError(entity_id)
        domain, object_id = entity_id.split(".", 1)
        return domain, object_id

    entity_module.split_entity_id = split_entity_id

    class EntityCategory:
        CONFIG = "config"
        DIAGNOSTIC = "diagnostic"

    entity_module.EntityCategory = EntityCategory
    sys.modules["homeassistant.helpers.entity"] = entity_module
    setattr(helpers_pkg, "entity", entity_module)

    entity_component_module = ModuleType("homeassistant.helpers.entity_component")
    entity_component_module.split_entity_id = split_entity_id
    sys.modules["homeassistant.helpers.entity_component"] = entity_component_module
    setattr(helpers_pkg, "entity_component", entity_component_module)

    entity_platform_module = ModuleType("homeassistant.helpers.entity_platform")
    entity_platform_module.AddEntitiesCallback = Callable[[Iterable], None]
    sys.modules["homeassistant.helpers.entity_platform"] = entity_platform_module
    setattr(helpers_pkg, "entity_platform", entity_platform_module)

    restore_state_module = ModuleType("homeassistant.helpers.restore_state")

    class RestoreEntity:
        async def async_get_last_state(self):  # pragma: no cover - stub behaviour
            return None

    class RestoreStateData:  # pragma: no cover - storage shim
        def __init__(self) -> None:
            self.last_states: list[Any] | None = None

        async def async_setup(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def async_dump_states(self) -> None:
            return None

        async def async_setup_dump(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def async_save_persistent_states(self) -> None:
            return None

        async def async_load(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def async_get_stored_states(self) -> list[Any]:
            return []

        async def async_restore_entity_added(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        async def async_restore_entity_removed(
            self, *_args: Any, **_kwargs: Any
        ) -> None:
            return None

    restore_state_module.RestoreEntity = RestoreEntity
    restore_state_module.RestoreStateData = RestoreStateData

    async def _restore_state_start(*_args: Any, **_kwargs: Any) -> RestoreStateData:
        return RestoreStateData()

    _restore_state_start.async_at_start = True

    restore_state_module.start = _restore_state_start
    sys.modules["homeassistant.helpers.restore_state"] = restore_state_module
    setattr(helpers_pkg, "restore_state", restore_state_module)

    issue_registry_module = sys.modules["homeassistant.helpers.issue_registry"]
    # Every member of Core's IssueSeverity, so a WARNING issue does not fail on
    # the stub with an AttributeError.
    issue_registry_module.IssueSeverity = SimpleNamespace(
        CRITICAL="critical", ERROR="error", WARNING="warning", INFO="info"
    )

    if not hasattr(issue_registry_module, "IssueRegistryStore"):

        class IssueRegistryStore:  # pragma: no cover - storage shim
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.data: dict[str, Any] | None = None

            async def async_load(self) -> dict[str, Any] | None:
                return self.data

            async def async_save(self, data: dict[str, Any]) -> None:
                self.data = data

        issue_registry_module.IssueRegistryStore = IssueRegistryStore

    class _IssueRegistry:
        """Minimal in-memory Repairs issue registry used by tests."""

        def __init__(self) -> None:
            self._issues: dict[tuple[str, str], dict[str, object]] = {}

        def async_get_issue(
            self, domain: str, issue_id: str
        ) -> dict[str, object] | None:
            return self._issues.get((domain, issue_id))

        def async_create_issue(
            self,
            domain: str,
            issue_id: str,
            **data: object,
        ) -> None:
            self._issues[(domain, issue_id)] = {
                **data,
                "domain": domain,
                "issue_id": issue_id,
            }

        def async_delete_issue(self, domain: str, issue_id: str) -> None:
            self._issues.pop((domain, issue_id), None)

    def _issue_registry_for(hass) -> _IssueRegistry:
        registry = getattr(hass, "_issue_registry", None)
        if registry is None:
            registry = _IssueRegistry()
            setattr(hass, "_issue_registry", registry)
        return registry

    issue_registry_module.async_get = _issue_registry_for

    def _async_create_issue(hass, domain, issue_id, **data) -> None:
        _issue_registry_for(hass).async_create_issue(domain, issue_id, **data)

    def _async_delete_issue(hass, domain, issue_id) -> None:
        _issue_registry_for(hass).async_delete_issue(domain, issue_id)

    issue_registry_module.async_create_issue = _async_create_issue
    issue_registry_module.async_delete_issue = _async_delete_issue

    frame_module = ModuleType("homeassistant.helpers.frame")

    class _FrameHelper:
        """Trivial frame helper compatible with pytest-homeassistant expectations."""

        def __init__(self) -> None:
            self._is_setup = False
            self.hass: Any | None = None

        def set_up(self, hass: Any | None) -> None:
            self._is_setup = True
            if hass is not None:
                self.hass = hass

        async def async_set_up(self, hass: Any | None) -> None:
            self.set_up(hass)

        def report(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - no-op
            return None

        def report_usage(
            self, *args: Any, **kwargs: Any
        ) -> None:  # pragma: no cover - no-op
            return None

        def __getattr__(self, name: str) -> Any:
            if name.startswith("async_set_up") or name.startswith("async_setup"):

                async def _async_proxy(hass: Any | None) -> None:
                    result = self.async_set_up(hass)
                    if inspect.isawaitable(result):
                        await result

                return _async_proxy

            if name.startswith("set_up") and name != "set_up":

                def _setup_proxy(hass: Any | None) -> None:
                    self.set_up(hass)

                return _setup_proxy

            raise AttributeError(name)

    frame_helper = _FrameHelper()

    def frame_set_up(*args: Any, **kwargs: Any) -> None:
        frame_helper.set_up(
            kwargs.get("hass") if "hass" in kwargs else (args[0] if args else None)
        )

    async def frame_async_set_up(*args: Any, **kwargs: Any) -> None:
        result = frame_helper.async_set_up(
            kwargs.get("hass") if "hass" in kwargs else (args[0] if args else None)
        )
        if inspect.isawaitable(result):
            await result

    def frame_report(*args: Any, **kwargs: Any) -> None:
        frame_helper.report(*args, **kwargs)

    def frame_report_usage(*args: Any, **kwargs: Any) -> None:
        frame_helper.report_usage(*args, **kwargs)

    frame_module.frame_helper = frame_helper
    frame_module.set_up = frame_set_up
    frame_module.async_set_up = frame_async_set_up
    frame_module.report = frame_report
    frame_module.report_usage = frame_report_usage
    sys.modules["homeassistant.helpers.frame"] = frame_module
    setattr(helpers_pkg, "frame", frame_module)

    device_registry_module = sys.modules["homeassistant.helpers.device_registry"]
    device_registry_module.EVENT_DEVICE_REGISTRY_UPDATED = "device_registry_updated"

    if not hasattr(device_registry_module, "DeviceEntryType"):

        class DeviceEntryType:  # noqa: D401 - stub enum container
            SERVICE = "service"

        device_registry_module.DeviceEntryType = DeviceEntryType

    if not hasattr(device_registry_module, "DeviceRegistryStore"):

        class DeviceRegistryStore:  # pragma: no cover - storage shim
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.data: dict[str, Any] | None = None

            async def async_load(self) -> dict[str, Any] | None:
                return self.data

            async def async_save(self, data: dict[str, Any]) -> None:
                self.data = data

        device_registry_module.DeviceRegistryStore = DeviceRegistryStore

    class _StubDeviceEntry:
        """In-memory device entry capturing registry metadata."""

        _counter = 0

        def __init__(
            self,
            *,
            identifiers: set[tuple[str, str]],
            config_entry_id: str,
            name: str | None = None,
            manufacturer: str | None = None,
            model: str | None = None,
            sw_version: str | None = None,
            entry_type: object | None = None,
            configuration_url: str | None = None,
            translation_key: str | None = None,
            translation_placeholders: Mapping[str, str] | None = None,
            config_subentry_id: str | None = None,
            via_device_id: str | None = None,
            via_device: tuple[str, str] | None = None,
        ) -> None:
            type(self)._counter += 1
            self.id = f"device-{type(self)._counter}"
            self.identifiers = set(identifiers)
            self.config_entries = {config_entry_id}
            self.name = name
            self.name_by_user = None
            self.manufacturer = manufacturer
            self.model = model
            self.sw_version = sw_version
            self.entry_type = entry_type
            self.configuration_url = configuration_url
            self.translation_key = translation_key
            self.translation_placeholders = dict(translation_placeholders or {})
            self.config_subentry_id = config_subentry_id
            self.config_entries_subentries: dict[str, set[str | None]] = {
                config_entry_id: {config_subentry_id} if config_subentry_id else {None}
            }
            self.via_device_id = via_device_id
            self.via_device = via_device
            self.disabled_by = None

        def _sync_config_subentry_id(self, entry_id: str | None) -> None:
            """Keep legacy shortcut aligned with mapping-based links."""

            if not entry_id:
                return
            subentries = self.config_entries_subentries.get(entry_id)
            if not subentries:
                self.config_subentry_id = None
                return
            non_null = [item for item in subentries if item is not None]
            if len(subentries) == 1 and not non_null:
                self.config_subentry_id = None
            elif len(non_null) == 1:
                self.config_subentry_id = non_null[0]
            else:
                self.config_subentry_id = None

        def update(self, **changes: object) -> None:
            for key, value in changes.items():
                setattr(self, key, value)

    _MISSING = object()

    class _StubDeviceRegistry:
        """Minimal registry implementation retaining created/updated metadata."""

        def __init__(self) -> None:
            self.devices: dict[str, _StubDeviceEntry] = {}
            self.created: list[dict[str, object]] = []
            self.updated: list[dict[str, object]] = []
            self.removed: list[str] = []

        def async_get(self, device_id: str | None) -> _StubDeviceEntry | None:
            if not device_id:
                return None
            return self.devices.get(device_id)

        def async_get_device(
            self, *, identifiers: set[tuple[str, str]] | None = None
        ) -> _StubDeviceEntry | None:
            if not identifiers:
                return None
            for device in self.devices.values():
                if identifiers & device.identifiers:
                    return device
            return None

        def async_get_or_create(
            self,
            *,
            config_entry_id: str,
            identifiers: set[tuple[str, str]],
            manufacturer: str,
            model: str,
            name: str | None = None,
            via_device_id: str | None = None,
            via_device: tuple[str, str] | None = None,
            sw_version: str | None = None,
            entry_type: object | None = None,
            configuration_url: str | None = None,
            translation_key: str | None = None,
            translation_placeholders: Mapping[str, str] | None = None,
            config_subentry_id: str | None = None,
        ) -> _StubDeviceEntry:
            # Hub-collision regression tests rely on the mapping-aware
            # ``config_entries_subentries`` view staying in sync with the
            # legacy ``config_subentry_id`` shortcut. Keep both populated here
            # so downstream name-deduplication logic sees hub-linked devices.
            entry = _StubDeviceEntry(
                identifiers=identifiers,
                config_entry_id=config_entry_id,
                name=name,
                manufacturer=manufacturer,
                model=model,
                sw_version=sw_version,
                entry_type=entry_type,
                configuration_url=configuration_url,
                translation_key=translation_key,
                translation_placeholders=translation_placeholders,
                config_subentry_id=config_subentry_id,
                via_device_id=via_device_id,
                via_device=via_device,
            )
            self.devices[entry.id] = entry
            self.created.append(
                {
                    "config_entry_id": config_entry_id,
                    "identifiers": set(identifiers),
                    "manufacturer": manufacturer,
                    "model": model,
                    "name": name,
                    "via_device_id": via_device_id,
                    "via_device": via_device,
                    "sw_version": sw_version,
                    "entry_type": entry_type,
                    "configuration_url": configuration_url,
                    "translation_key": translation_key,
                    "translation_placeholders": dict(translation_placeholders or {}),
                    "config_subentry_id": config_subentry_id,
                    "config_entries_subentries": {
                        config_entry_id: {config_subentry_id}
                        if config_subentry_id
                        else {None}
                    },
                }
            )
            return entry

        def async_entries_for_config_entry(
            self, config_entry_id: str
        ) -> list[_StubDeviceEntry]:
            return [
                device
                for device in self.devices.values()
                if config_entry_id in getattr(device, "config_entries", set())
            ]

        def async_remove_device(self, device_id: str) -> None:
            if device_id in self.devices:
                self.devices.pop(device_id, None)
                self.removed.append(device_id)

        def async_update_device(
            self,
            *,
            device_id: str,
            new_identifiers: set[tuple[str, str]] | None = None,
            via_device_id: str | None = None,
            translation_key: str | None = None,
            translation_placeholders: Mapping[str, str] | None = None,
            config_subentry_id: object = _MISSING,
            add_config_subentry_id: object = _MISSING,
            config_entry_id: object = _MISSING,
            add_config_entry_id: object = _MISSING,
            remove_config_entry_id: object = _MISSING,
            remove_config_subentry_id: object = _MISSING,
            name: str | None = None,
            manufacturer: str | None = None,
            model: str | None = None,
            sw_version: str | None = None,
            entry_type: object | None = None,
            configuration_url: str | None = None,
        ) -> None:
            device = self.devices.get(device_id)
            if device is None:
                raise AssertionError(f"Unknown device_id {device_id}")
            if new_identifiers is not None:
                device.identifiers = set(new_identifiers)
            updates: dict[str, object | None] = {}
            if config_entry_id is not _MISSING:
                if config_entry_id is None:
                    device.config_entries = set()
                else:
                    device.config_entries = {cast(str, config_entry_id)}
                updates["config_entry_id"] = cast(str | None, config_entry_id)
            if add_config_entry_id is not _MISSING:
                if add_config_entry_id is None:
                    device.config_entries = set()
                    device.config_entries_subentries.clear()
                else:
                    entries = set(getattr(device, "config_entries", set()) or set())
                    entries.add(cast(str, add_config_entry_id))
                    device.config_entries = entries
                updates["add_config_entry_id"] = cast(str | None, add_config_entry_id)
            if remove_config_entry_id is not _MISSING:
                entries = set(getattr(device, "config_entries", set()) or set())
                if remove_config_entry_id is None:
                    entries.clear()
                else:
                    entries.discard(cast(str, remove_config_entry_id))
                device.config_entries = entries
                device.config_entries_subentries.pop(
                    cast(str, remove_config_entry_id), None
                )
            effective_subentry = _MISSING
            if config_subentry_id is not _MISSING:
                effective_subentry = config_subentry_id
            elif add_config_subentry_id is not _MISSING:
                effective_subentry = add_config_subentry_id
            elif remove_config_subentry_id is not _MISSING:
                effective_subentry = None
            if effective_subentry is not _MISSING:
                target_entries: set[str] = set()
                if (
                    add_config_entry_id is not _MISSING
                    and add_config_entry_id is not None
                ):
                    target_entries.add(cast(str, add_config_entry_id))
                elif config_entry_id is not _MISSING and config_entry_id is not None:
                    target_entries.add(cast(str, config_entry_id))
                if not target_entries:
                    target_entries.update(device.config_entries)
                for entry_id in target_entries:
                    bucket = device.config_entries_subentries.setdefault(
                        entry_id, set()
                    )
                    bucket.clear()
                    bucket.add(cast(str | None, effective_subentry))
                    device._sync_config_subentry_id(entry_id)
                updates["config_subentry_id"] = cast(str | None, effective_subentry)
            for attr, value in (
                ("via_device_id", via_device_id),
                ("translation_key", translation_key),
                ("config_subentry_id", config_subentry_id),
                ("name", name),
                ("manufacturer", manufacturer),
                ("model", model),
                ("sw_version", sw_version),
                ("entry_type", entry_type),
                ("configuration_url", configuration_url),
            ):
                if attr == "config_subentry_id":
                    continue
                if value is not None:
                    updates[attr] = value
            if translation_placeholders is not None:
                updates["translation_placeholders"] = dict(translation_placeholders)
            if updates:
                device.update(**updates)
            self.updated.append(
                {
                    "device_id": device_id,
                    "new_identifiers": None
                    if new_identifiers is None
                    else set(new_identifiers),
                    "via_device_id": via_device_id,
                    "translation_key": translation_key,
                    "translation_placeholders": None
                    if translation_placeholders is None
                    else dict(translation_placeholders),
                    "config_entry_id": None
                    if config_entry_id is _MISSING
                    else cast(str | None, config_entry_id),
                    "config_subentry_id": None
                    if effective_subentry is _MISSING
                    else cast(str | None, effective_subentry),
                    "name": name,
                    "manufacturer": manufacturer,
                    "model": model,
                    "sw_version": sw_version,
                    "entry_type": entry_type,
                    "configuration_url": configuration_url,
                    "add_config_subentry_id": None
                    if add_config_subentry_id is _MISSING
                    else cast(str | None, add_config_subentry_id),
                    "add_config_entry_id": None
                    if add_config_entry_id is _MISSING
                    else cast(str | None, add_config_entry_id),
                    "remove_config_entry_id": None
                    if remove_config_entry_id is _MISSING
                    else cast(str | None, remove_config_entry_id),
                    "remove_config_subentry_id": None
                    if remove_config_subentry_id is _MISSING
                    else cast(str | None, remove_config_subentry_id),
                }
            )

    def _device_registry_for(hass=None) -> _StubDeviceRegistry:
        if hass is not None:
            registry = getattr(hass, "_device_registry_stub", None)
            if registry is None:
                registry = _StubDeviceRegistry()
                setattr(hass, "_device_registry_stub", registry)
            return registry
        return _StubDeviceRegistry()

    device_registry_module.async_get = _device_registry_for

    def _async_device_entries_for_config_entry(
        registry: _StubDeviceRegistry, config_entry_id: str
    ) -> list[_StubDeviceEntry]:
        return registry.async_entries_for_config_entry(config_entry_id)

    device_registry_module.async_entries_for_config_entry = (
        _async_device_entries_for_config_entry
    )

    cv_module = ModuleType("homeassistant.helpers.config_validation")

    def _multi_select(choices):  # pragma: no cover - defensive
        return lambda value: value

    cv_module.multi_select = _multi_select
    sys.modules["homeassistant.helpers.config_validation"] = cv_module
    setattr(helpers_pkg, "config_validation", cv_module)

    aiohttp_client_module = ModuleType("homeassistant.helpers.aiohttp_client")
    aiohttp_client_module.async_get_clientsession = lambda hass: None

    async def _stub_async_make_resolver(*_args, **_kwargs):
        return None

    aiohttp_client_module._async_make_resolver = _stub_async_make_resolver
    sys.modules["homeassistant.helpers.aiohttp_client"] = aiohttp_client_module
    setattr(helpers_pkg, "aiohttp_client", aiohttp_client_module)

    storage_module = ModuleType("homeassistant.helpers.storage")

    class Store:  # minimal async Store stub
        def __init__(self, *args, **kwargs) -> None:
            self._data: dict[str, object] | None = None

        async def async_load(self) -> dict[str, object] | None:
            return self._data

        async def _async_load(self) -> dict[str, object] | None:
            """Alias used by HA storage helpers."""

            return await self.async_load()

        async def _async_write_data(self, _path: str, data: dict[str, object]) -> None:
            """Persist storage payloads during tests."""

            self._data = data

        def async_delay_save(self, *_args, **_kwargs) -> None:
            return None

        async def async_remove(self) -> None:
            self._data = None

    storage_module.Store = Store
    sys.modules["homeassistant.helpers.storage"] = storage_module
    setattr(helpers_pkg, "storage", storage_module)

    class UpdateFailed(HomeAssistantError):
        """Stubbed UpdateFailed; like the core it is a ``HomeAssistantError``."""

        def __init__(
            self,
            *args: Any,
            retry_after: float | None = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(*args, **kwargs)
            self.retry_after = retry_after

    update_coordinator_module = sys.modules["homeassistant.helpers.update_coordinator"]
    update_coordinator_module.UpdateFailed = UpdateFailed

    from typing import Generic, TypeVar

    _T = TypeVar("_T")

    class DataUpdateCoordinator(Generic[_T]):
        """Minimal stub for DataUpdateCoordinator supporting subclassing.

        ``async_shutdown`` and the two removers it calls mirror the core
        (``homeassistant/helpers/update_coordinator.py``, 2026.8.2, lines 210
        to 239) line for line, because ``GoogleFindMyCoordinator.async_shutdown``
        chains to them and the suite runs every shutdown path against this
        stub. ``tests/test_core_shutdown_stub_parity.py`` holds the mirror to
        the real class. The four attributes are the ones the core's
        ``__init__`` gives them; ``_debounced_refresh`` is a two-method
        stand-in for the ``Debouncer`` surface the core's shutdown uses.
        """

        def __init__(
            self, hass=None, logger=None, name: str | None = None, update_interval=None
        ):
            self.hass = hass
            self.logger = logger
            self.name = name or "coordinator"
            self.update_interval = update_interval
            self._shutdown_requested = False
            self._unsub_refresh: Callable[[], None] | None = None
            self._unsub_shutdown: Callable[[], None] | None = None
            self._debounced_refresh: Any = SimpleNamespace(
                async_shutdown=lambda: None, async_cancel=lambda: None
            )

        async def async_shutdown(self) -> None:
            """Cancel any scheduled call, and ignore new runs."""
            self._shutdown_requested = True
            self._async_unsub_refresh()
            self._async_unsub_shutdown()
            self._debounced_refresh.async_shutdown()

        def _async_unsub_refresh(self) -> None:
            """Cancel any scheduled call."""
            if self._unsub_refresh:
                self._unsub_refresh()
                self._unsub_refresh = None

        def _async_unsub_shutdown(self) -> None:
            """Cancel any scheduled call."""
            if self._unsub_shutdown:
                self._unsub_shutdown()
                self._unsub_shutdown = None

        async def async_request_refresh(
            self,
        ) -> None:  # pragma: no cover - stubbed behaviour
            return None

        async def async_config_entry_first_refresh(
            self,
        ) -> None:  # pragma: no cover - stubbed behaviour
            return None

    update_coordinator_module.DataUpdateCoordinator = DataUpdateCoordinator

    class CoordinatorEntity:
        def __init__(self, coordinator) -> None:
            self.coordinator = coordinator
            self.hass = getattr(coordinator, "hass", None)

        def async_write_ha_state(self) -> None:  # pragma: no cover - stub behaviour
            return None

        async def async_added_to_hass(
            self,
        ) -> None:  # pragma: no cover - stub behaviour
            return None

        def __class_getitem__(cls, _item):  # pragma: no cover - typing compatibility
            return cls

        @property
        def unique_id(self) -> str | None:
            return getattr(self, "_attr_unique_id", None)

    update_coordinator_module.CoordinatorEntity = CoordinatorEntity

    event_module = ModuleType("homeassistant.helpers.event")

    def _async_call_later(*_args, **_kwargs):  # pragma: no cover - stubbed behaviour
        return None

    event_module.async_call_later = _async_call_later

    def _async_track_time_interval(
        hass: Any,
        action: Callable[[Any], Any],
        _interval: timedelta,
        *_args: Any,
        **_kwargs: Any,
    ) -> Callable[[], None]:
        del hass, action, _interval, _args, _kwargs
        return lambda: None

    event_module.async_track_time_interval = _async_track_time_interval

    def _async_track_state_change_event(
        hass, entity_ids, action, **_kwargs
    ) -> Callable[[], None]:  # pragma: no cover - stubbed for FMDN Finder
        del hass, entity_ids, action, _kwargs
        return lambda: None

    event_module.async_track_state_change_event = _async_track_state_change_event
    sys.modules["homeassistant.helpers.event"] = event_module
    setattr(helpers_pkg, "event", event_module)

    network_module = install_homeassistant_network_stub(
        get_url_result="https://example.local",
    )
    setattr(helpers_pkg, "network", network_module)

    entity_registry_module = sys.modules["homeassistant.helpers.entity_registry"]

    if not hasattr(entity_registry_module, "EntityRegistryStore"):

        class EntityRegistryStore:  # pragma: no cover - storage shim
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.data: dict[str, Any] | None = None

            async def async_load(self) -> dict[str, Any] | None:
                return self.data

            async def async_save(self, data: dict[str, Any]) -> None:
                self.data = data

        entity_registry_module.EntityRegistryStore = EntityRegistryStore

    class _StubEntityRegistryEntry:
        """Entity registry entry capturing subentry assignments for assertions."""

        __slots__ = (
            "entity_id",
            "domain",
            "platform",
            "unique_id",
            "config_entry_id",
            "config_entry_subentry_id",
            "config_subentry_id",
            "device_id",
        )

        def __init__(
            self,
            *,
            entity_id: str,
            platform: str,
            unique_id: str | None,
            config_entry_id: str | None,
            config_entry_subentry_id: str | None,
        ) -> None:
            self.entity_id = entity_id
            # Home Assistant derives the entity domain from the entity_id;
            # ``platform`` names the integration that owns the entry.
            self.domain = entity_id.split(".", 1)[0]
            self.platform = platform
            self.unique_id = unique_id
            self.config_entry_id = config_entry_id
            self.config_entry_subentry_id = config_entry_subentry_id
            self.config_subentry_id = config_entry_subentry_id
            self.device_id: str | None = None

    class _StubEntityRegistry:
        """In-memory entity registry stub used by platform setup tests."""

        def __init__(self) -> None:
            self.entities: dict[str, _StubEntityRegistryEntry] = {}
            self.removed: list[str] = []

        def async_get(self, entity_id: str) -> _StubEntityRegistryEntry | None:
            return self.entities.get(entity_id)

        def async_get_entity_id(
            self, domain: str, platform: str, unique_id: str
        ) -> str | None:
            """Return the entity_id for ``(domain, platform, unique_id)`` like HA."""
            for entity_id, entry in self.entities.items():
                if (
                    entry.domain == domain
                    and entry.platform == platform
                    and entry.unique_id == unique_id
                ):
                    return entity_id
            return None

        def record_entity(
            self,
            entity_id: str,
            *,
            platform: str,
            unique_id: str | None,
            config_entry_id: str | None,
            config_entry_subentry_id: str | None,
        ) -> _StubEntityRegistryEntry:
            entry = _StubEntityRegistryEntry(
                entity_id=entity_id,
                platform=platform,
                unique_id=unique_id,
                config_entry_id=config_entry_id,
                config_entry_subentry_id=config_entry_subentry_id,
            )
            self.entities[entity_id] = entry
            return entry

        def async_entries_for_config_entry(
            self, config_entry_id: str
        ) -> list[_StubEntityRegistryEntry]:
            return [
                entry
                for entry in self.entities.values()
                if entry.config_entry_id == config_entry_id
            ]

        def async_remove(self, entity_id: str) -> None:
            if entity_id in self.entities:
                self.entities.pop(entity_id, None)
                self.removed.append(entity_id)

    def _entity_registry_for(
        hass: Any | None = None,
    ) -> _StubEntityRegistry:  # pragma: no cover - stub behaviour
        if hass is not None:
            registry = getattr(hass, "_entity_registry_stub", None)
            if registry is None:
                registry = _StubEntityRegistry()
                setattr(hass, "_entity_registry_stub", registry)
            return registry
        return _StubEntityRegistry()

    entity_registry_module.RegistryEntry = _StubEntityRegistryEntry
    entity_registry_module.async_get = _entity_registry_for

    def _async_entries_for_config_entry(
        registry: _StubEntityRegistry, config_entry_id: str
    ) -> list[_StubEntityRegistryEntry]:
        return registry.async_entries_for_config_entry(config_entry_id)

    entity_registry_module.async_entries_for_config_entry = (
        _async_entries_for_config_entry
    )

    def _async_entries_for_device(
        registry: _StubEntityRegistry,
        device_id: str,
        include_disabled_entities: bool = False,
    ) -> list[_StubEntityRegistryEntry]:
        """Get all entity registry entries for a device."""
        return [
            entry
            for entry in registry.entities.values()
            if getattr(entry, "device_id", None) == device_id
        ]

    entity_registry_module.async_entries_for_device = _async_entries_for_device

    util_pkg = sys.modules.setdefault(
        "homeassistant.util", ModuleType("homeassistant.util")
    )
    dt_module = ModuleType("homeassistant.util.dt")
    dt_module.UTC = UTC
    dt_module.utcnow = lambda: datetime.now(UTC)
    dt_module.now = dt_module.utcnow
    dt_module.as_local = lambda dt: dt
    sys.modules["homeassistant.util.dt"] = dt_module
    setattr(util_pkg, "dt", dt_module)

    components_pkg = sys.modules.setdefault(
        "homeassistant.components", ModuleType("homeassistant.components")
    )
    components_pkg.__path__ = getattr(components_pkg, "__path__", [])

    device_tracker_module = ModuleType("homeassistant.components.device_tracker")

    class SourceType:
        GPS = "gps"

    class TrackerEntity:  # pragma: no cover - stub behaviour
        _attr_has_entity_name = True

        def __init__(self, *_args, **_kwargs) -> None:
            pass

    device_tracker_module.DOMAIN = "device_tracker"
    device_tracker_module.SourceType = SourceType
    device_tracker_module.TrackerEntity = TrackerEntity
    sys.modules["homeassistant.components.device_tracker"] = device_tracker_module
    setattr(components_pkg, "device_tracker", device_tracker_module)

    def _entity_base() -> type:
        class _EntityBase:  # pragma: no cover - stub behaviour
            _attr_has_entity_name = True
            _context = None

            def __init__(self, *_args, **_kwargs) -> None:
                self.entity_id = None
                self.hass = None

            async def async_added_to_hass(self) -> None:
                return None

            async def async_will_remove_from_hass(self) -> None:
                return None

            def async_write_ha_state(self) -> None:
                return None

            @property
            def unique_id(self) -> str | None:
                return getattr(self, "_attr_unique_id", None)

        return _EntityBase

    button_module = ModuleType("homeassistant.components.button")

    class ButtonEntity(_entity_base()):  # pragma: no cover - stub
        pass

    class ButtonEntityDescription:  # pragma: no cover - stub
        def __init__(self, **kwargs) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    button_module.ButtonEntity = ButtonEntity
    button_module.ButtonEntityDescription = ButtonEntityDescription
    sys.modules["homeassistant.components.button"] = button_module
    setattr(components_pkg, "button", button_module)

    binary_sensor_module = ModuleType("homeassistant.components.binary_sensor")

    class BinarySensorEntity(_entity_base()):  # pragma: no cover - stub
        pass

    class BinarySensorEntityDescription:  # pragma: no cover - stub
        def __init__(self, **kwargs) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    class BinarySensorDeviceClass:  # pragma: no cover - stub values
        PROBLEM = "problem"

    binary_sensor_module.BinarySensorEntity = BinarySensorEntity
    binary_sensor_module.BinarySensorEntityDescription = BinarySensorEntityDescription
    binary_sensor_module.BinarySensorDeviceClass = BinarySensorDeviceClass
    sys.modules["homeassistant.components.binary_sensor"] = binary_sensor_module
    setattr(components_pkg, "binary_sensor", binary_sensor_module)

    sensor_module = ModuleType("homeassistant.components.sensor")

    class SensorEntity(_entity_base()):  # pragma: no cover - stub
        @property
        def native_unit_of_measurement(self) -> str | None:
            """Mirror ``SensorEntity.native_unit_of_measurement`` from HA."""
            if hasattr(self, "_attr_native_unit_of_measurement"):
                return self._attr_native_unit_of_measurement
            if hasattr(self, "entity_description"):
                return getattr(
                    self.entity_description, "native_unit_of_measurement", None
                )
            return None

    @dataclass
    class SensorExtraStoredData:  # pragma: no cover - stub
        """Mirror ``SensorExtraStoredData`` (value and unit) from HA."""

        native_value: Any
        native_unit_of_measurement: str | None

        def as_dict(self) -> dict[str, Any]:
            return {
                "native_value": self.native_value,
                "native_unit_of_measurement": self.native_unit_of_measurement,
            }

        @classmethod
        def from_dict(cls, restored: dict[str, Any]) -> Any:
            try:
                native_value = restored["native_value"]
                native_unit_of_measurement = restored["native_unit_of_measurement"]
            except KeyError:
                return None
            return cls(native_value, native_unit_of_measurement)

    class RestoreSensor(SensorEntity):  # pragma: no cover - stub
        @property
        def extra_restore_state_data(self) -> SensorExtraStoredData:
            """Mirror ``RestoreSensor.extra_restore_state_data`` from HA."""
            return SensorExtraStoredData(
                self.native_value, self.native_unit_of_measurement
            )

        async def async_get_last_sensor_data(self):
            return None

        async def async_get_last_extra_data(self):
            return None

        async def async_get_last_state(self):
            return None

    class SensorEntityDescription:  # pragma: no cover - stub
        def __init__(self, **kwargs) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

    class SensorDeviceClass:  # pragma: no cover - stub values
        TIMESTAMP = "timestamp"
        BATTERY = "battery"
        ENUM = "enum"

    class SensorStateClass:  # pragma: no cover - stub values
        TOTAL_INCREASING = "total_increasing"
        MEASUREMENT = "measurement"

    sensor_module.SensorEntity = SensorEntity
    sensor_module.RestoreSensor = RestoreSensor
    sensor_module.SensorExtraStoredData = SensorExtraStoredData
    sensor_module.SensorEntityDescription = SensorEntityDescription
    sensor_module.SensorDeviceClass = SensorDeviceClass
    sensor_module.SensorStateClass = SensorStateClass
    sys.modules["homeassistant.components.sensor"] = sensor_module
    setattr(components_pkg, "sensor", sensor_module)

    http_module = ModuleType("homeassistant.components.http")

    class HomeAssistantView:  # pragma: no cover - stub for imports
        requires_auth = False

        async def get(self, *_args, **_kwargs):
            return None

    http_module.HomeAssistantView = HomeAssistantView
    sys.modules["homeassistant.components.http"] = http_module
    setattr(components_pkg, "http", http_module)

    diagnostics_module = ModuleType("homeassistant.components.diagnostics")

    def _async_redact_data(data, _keys):  # pragma: no cover - stub behaviour
        return data

    diagnostics_module.async_redact_data = _async_redact_data
    sys.modules["homeassistant.components.diagnostics"] = diagnostics_module
    setattr(components_pkg, "diagnostics", diagnostics_module)

    recorder_module = ModuleType("homeassistant.components.recorder")
    recorder_module.get_instance = lambda *args, **kwargs: None
    history_module = ModuleType("homeassistant.components.recorder.history")

    def _no_history(*args, **kwargs):  # pragma: no cover - stub
        raise NotImplementedError

    history_module.get_significant_states = _no_history
    recorder_module.history = history_module
    sys.modules["homeassistant.components.recorder"] = recorder_module
    sys.modules["homeassistant.components.recorder.history"] = history_module
    setattr(components_pkg, "recorder", recorder_module)


@pytest.fixture(name="record_flow_forms")
def fixture_record_flow_forms() -> Callable[[Any], list[str | None]]:
    """Instrument a config flow to record the step IDs shown to the user."""

    def _apply(flow: Any) -> list[str | None]:
        recorded: list[str | None] = []

        async def _show_form(*_: Any, **kwargs: Any) -> dict[str, Any]:
            step_id = kwargs.get("step_id")
            recorded.append(step_id)
            response: dict[str, Any] = {"type": "form"}
            if step_id is not None:
                response["step_id"] = step_id
            return response

        flow.async_show_form = _show_form  # type: ignore[attr-defined]
        return recorded

    return _apply


@pytest.fixture(scope="session", name="integration_root")
def fixture_integration_root() -> Path:
    """Return the root path of the googlefindmy integration package."""

    assert INTEGRATION_ROOT.is_dir(), "integration package root must exist"
    return INTEGRATION_ROOT


@pytest.fixture(scope="session", name="integration_python_files")
def fixture_integration_python_files(integration_root: Path) -> list[Path]:
    """Return all Python files under the integration root (sorted)."""

    return sorted(integration_root.rglob("*.py"))


@pytest.fixture(scope="session", name="manifest")
def fixture_manifest(integration_root: Path) -> dict[str, object]:
    """Load and return the integration manifest."""

    manifest_path = integration_root / "manifest.json"
    manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert isinstance(manifest_data, dict)
    return manifest_data


@pytest.fixture(name="coordinator_teardown_defaults")
def fixture_coordinator_teardown_defaults() -> Callable[[Any], None]:
    """Populate coordinator teardown attributes with safe defaults."""

    def _apply(
        coordinator: Any, *, loop: asyncio.AbstractEventLoop | None = None
    ) -> None:
        seed_core_shutdown_state(coordinator)
        if getattr(coordinator, "_dr_unsub", None) is None:
            coordinator._dr_unsub = lambda: None
        if getattr(coordinator, "_short_retry_cancel", None) is None:
            coordinator._short_retry_cancel = lambda: None

        if getattr(coordinator, "_stats_save_task", None) is None:
            resolved_loop = loop
            if resolved_loop is None:
                try:
                    resolved_loop = asyncio.get_running_loop()
                except RuntimeError:
                    resolved_loop = None

            if resolved_loop is not None:
                stats_task = resolved_loop.create_future()
                stats_task.set_result(None)
            else:
                stats_task = None

            coordinator._stats_save_task = stats_task

    return _apply


@pytest.fixture(name="stub_coordinator_factory")
def fixture_stub_coordinator_factory() -> Callable[..., type[Any]]:
    """Return a factory that builds coordinator stubs with async helpers."""

    def _factory(
        *,
        data: Iterable[dict[str, Any]] | None = None,
        stats: Mapping[str, Any] | None = None,
        performance_metrics: Mapping[str, Any] | None = None,
        last_update_success: bool = True,
        subentry_key: str = TRACKER_SUBENTRY_KEY,
        service_subentry_key: str = SERVICE_SUBENTRY_KEY,
        metadata_for_feature: Mapping[str, str] | None = None,
        snapshot_callback: Callable[[str | None, str | None], Iterable[dict[str, Any]]]
        | None = None,
        init_hook: Callable[..., None] | None = None,
        methods: Mapping[str, Callable[..., Any]] | None = None,
        extra_attributes: Mapping[str, Any] | None = None,
    ) -> type[Any]:
        base_data = (
            list(data) if data is not None else [{"id": "device-1", "name": "Device"}]
        )
        base_stats = dict(stats or {"background_updates": 1})
        base_performance = dict(performance_metrics or {})
        feature_map = dict(metadata_for_feature or {})

        def _coerce_attribute(value: Any) -> Any:
            return value() if callable(value) else value

        class _CoordinatorStub:
            """Coordinator stub matching the runtime contract used in tests."""

            def __init__(self, hass: Any, *, cache: Any, **kwargs: Any) -> None:
                self.hass = hass
                self.cache = cache
                self.data = list(base_data)
                self.stats = dict(base_stats)
                self.performance_metrics = dict(base_performance)
                self.last_update_success = last_update_success
                self.config_entry: Any | None = None
                self._purged: list[str] = []
                self._listeners: list[Callable[[], None]] = []
                self._subentry_key = subentry_key
                self._service_subentry_key = service_subentry_key
                self._metadata_for_feature = dict(feature_map)
                self._snapshot_callback = snapshot_callback
                self._init_kwargs = dict(kwargs)
                self.subentry_manager: Any | None = None
                self._device_names: dict[str, str] = {}
                self._device_location_data: dict[str, Any] = {}
                self._device_last_good_location: dict[str, Any] = {}
                self._device_caps: dict[str, Any] = {}
                self._present_last_seen: dict[str, float] = {}
                self.first_refresh_calls = 0
                if extra_attributes:
                    for key, value in extra_attributes.items():
                        setattr(self, key, _coerce_attribute(value))
                if init_hook is not None:
                    init_hook(self, hass=hass, cache=cache, kwargs=kwargs)

            def async_add_listener(
                self, listener: Callable[[], None]
            ) -> Callable[[], None]:
                self._listeners.append(listener)
                return lambda: None

            def find_tracker_entity_entry(self, device_id: str) -> Any | None:
                del device_id
                return None

            def force_poll_due(self) -> None:  # pragma: no cover - default no-op
                return None

            async def async_setup(self) -> None:
                return None

            async def async_refresh(self) -> None:
                return None

            async def async_config_entry_first_refresh(self) -> None:
                self.first_refresh_calls += 1

            async def async_shutdown(self) -> None:
                return None

            async def async_request_refresh(
                self,
            ) -> None:  # pragma: no cover - optional
                return None

            def async_set_updated_data(
                self, _data: Any
            ) -> None:  # pragma: no cover - optional
                return None

            def push_updated(
                self, _ids: list[str]
            ) -> None:  # pragma: no cover - optional
                return None

            def purge_device(self, device_id: str) -> None:
                self._purged.append(device_id)

            def stable_subentry_identifier(
                self, *, key: str | None = None, feature: str | None = None
            ) -> str:
                if key is not None:
                    return key
                if feature is not None and feature in self._metadata_for_feature:
                    return self._metadata_for_feature[feature]
                if feature == "binary_sensor":
                    return self._service_subentry_key
                return self._subentry_key

            def get_subentry_metadata(
                self, *, key: str | None = None, feature: str | None = None
            ) -> Any:
                resolved = self.stable_subentry_identifier(key=key, feature=feature)
                return SimpleNamespace(key=resolved)

            def get_subentry_snapshot(
                self, key: str | None = None, *, feature: str | None = None
            ) -> list[dict[str, Any]]:
                if self._snapshot_callback is not None:
                    return list(self._snapshot_callback(key, feature))
                return list(self.data)

            def get_device_label_in_subentry(
                self, subentry_key: str | None, device_id: str
            ) -> str | None:
                """Mirror the coordinator's allocation-free label accessor.

                Reads the same source as ``get_subentry_snapshot`` but does not
                call it: tests asserting that the fast path never requests a
                snapshot copy would otherwise count this stub's own call.
                Rebuilt on every call, without a cache, because the coordinator
                contract does not promise one.  The production accessor takes no
                ``feature`` argument, so the callback is invoked with ``None``;
                a test whose ``_snapshot_callback`` branches on ``feature`` will
                see this method and ``get_subentry_snapshot`` answer from
                different rows.
                """

                rows = (
                    self._snapshot_callback(subentry_key, None)
                    if self._snapshot_callback is not None
                    else self.data
                )
                for row in rows:
                    if row.get("id") != device_id:
                        continue
                    name = row.get("name")
                    return name if isinstance(name, str) else None
                return None

            async def async_wait_subentry_visibility_updates(self) -> None:
                """Mirror the coordinator visibility wait helper."""

                manager = getattr(self, "subentry_manager", None)
                wait_visible = getattr(
                    manager, "async_wait_visible_device_updates", None
                )
                if callable(wait_visible):
                    await wait_visible()

            def is_device_visible_in_subentry(
                self, subentry_key: str, device_id: str
            ) -> bool:
                return True

            def get_display_location_data_for_subentry(
                self, subentry_key: str, device_id: str
            ) -> dict[str, Any] | None:
                # Mirror the real coordinator contract: return the display row
                # (current fix if it has usable accuracy, else the last-good),
                # not the raw current row, so display-semantics regressions are
                # not masked when this stub is used.
                from custom_components.googlefindmy.coordinator import (
                    select_display_row,
                )

                if not self.is_device_visible_in_subentry(subentry_key, device_id):
                    return None
                current = self._device_location_data.get(device_id)
                last_good = getattr(self, "_device_last_good_location", {}).get(
                    device_id
                )
                row = select_display_row(
                    current if isinstance(current, dict) else None, last_good
                )
                return dict(row) if row is not None else None

            def attach_subentry_manager(
                self, manager: Any, *, is_reload: bool = False
            ) -> None:
                self.subentry_manager = manager
                self._manager_is_reload = is_reload
                self._ensure_stub_service_device()

            def _ensure_stub_service_device(self) -> None:
                hass = getattr(self, "hass", None)
                entry = getattr(self, "config_entry", None)
                if hass is None or entry is None:
                    return

                entry_id = getattr(entry, "entry_id", None)
                if not isinstance(entry_id, str) or not entry_id:
                    return

                from homeassistant.helpers import device_registry as dr

                registry = dr.async_get(hass)
                create_device = getattr(registry, "async_get_or_create", None)
                if not callable(create_device):
                    return
                identifiers = {_SERVICE_DEVICE_IDENTIFIER(entry_id)}
                raw_title = getattr(entry, "title", None)
                if isinstance(raw_title, str) and raw_title.strip():
                    base_name = raw_title.strip()
                else:
                    base_name = _SERVICE_DEVICE_NAME
                service_name = f"{base_name} – Service"

                create_kwargs: dict[str, Any] = {
                    "config_entry_id": entry_id,
                    "identifiers": identifiers,
                    "manufacturer": _SERVICE_DEVICE_MANUFACTURER,
                    "model": _SERVICE_DEVICE_MODEL,
                    "sw_version": _INTEGRATION_VERSION,
                    "entry_type": dr.DeviceEntryType.SERVICE,
                    "name": service_name,
                    "translation_key": _SERVICE_DEVICE_TRANSLATION_KEY,
                    "translation_placeholders": {},
                    "configuration_url": "https://github.com/BSkando/GoogleFindMy-HA",
                }

                try:
                    create_device(**create_kwargs)
                except TypeError:
                    fallback_kwargs = dict(create_kwargs)
                    fallback_kwargs.pop("translation_key", None)
                    fallback_kwargs.pop("translation_placeholders", None)
                    create_device(**fallback_kwargs)

        if methods:
            for name, method in methods.items():
                setattr(_CoordinatorStub, name, method)

        return _CoordinatorStub

    return _factory


_stub_homeassistant()


def _load_integration_constant(attribute: str) -> str:
    """Resolve a constant from the integration module after stubs are ready."""

    const_module = importlib.import_module("custom_components.googlefindmy.const")
    return cast(str, getattr(const_module, attribute))


SERVICE_SUBENTRY_KEY = _load_integration_constant("SERVICE_SUBENTRY_KEY")
TRACKER_SUBENTRY_KEY = _load_integration_constant("TRACKER_SUBENTRY_KEY")

components_pkg = importlib.import_module("custom_components")
components_pkg.__path__ = [str(ROOT / "custom_components")]

gf_pkg = importlib.import_module("custom_components.googlefindmy")
setattr(components_pkg, "googlefindmy", gf_pkg)


@pytest.fixture
def use_real_homeassistant_modules() -> Iterable[None]:
    """Temporarily replace the stubbed Home Assistant modules with the real ones.

    Use this fixture in tests that require the real Home Assistant implementation
    provided by pytest-homeassistant-custom-component instead of the lightweight
    stubs installed by conftest.py.

    Example:
        @pytest.mark.asyncio
        async def test_integration_flow(
            hass: HomeAssistant,
            use_real_homeassistant_modules: None,
        ) -> None:
            # Test runs with real HA modules
            ...
    """
    saved_modules = {
        name: module
        for name, module in sys.modules.items()
        if name.startswith("homeassistant")
    }
    for name in list(sys.modules):
        if name.startswith("homeassistant"):
            del sys.modules[name]

    import homeassistant  # noqa: F401 - ensure the real package is loaded
    from homeassistant.helpers import aiohttp_client as _aiohttp_client
    from homeassistant.util import dt as dt_util

    # Ensure timezone is set for HA fixtures
    dt_util.DEFAULT_TIME_ZONE = dt_util.UTC

    # Some pytest-homeassistant-custom-component versions require this shim
    if not hasattr(_aiohttp_client, "_async_make_resolver"):

        async def _async_make_resolver(
            *args: Any, **kwargs: Any
        ) -> None:  # pragma: no cover
            return None

        _aiohttp_client._async_make_resolver = _async_make_resolver  # type: ignore[attr-defined]

    # The real modules were just re-imported, which discards every binding the
    # deprecation recorder made against the stubbed ones.  Re-install it here so
    # tests running under this fixture still observe ``report_usage`` calls;
    # without this the recorder can only stay silent, and silence looks exactly
    # like "no deprecation".  See tests/helpers/deprecation_recorder.py.
    _deprecation_recorder.rebind_active()

    try:
        yield
    finally:
        for name in list(sys.modules):
            if name.startswith("homeassistant"):
                del sys.modules[name]
        sys.modules.update(saved_modules)


@pytest.fixture(autouse=True)
def device_registry_deprecations(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterable[_deprecation_recorder.DeprecationRecorder]:
    """Record every ``report_usage`` call raised while a test runs.

    Autouse so that no test can opt out of being observed; the recorded list is
    only inspected where a test asks for this fixture by name.  The recorder
    delegates to the original callable, so behaviour is unchanged: a deprecated
    Core call made from outside ``custom_components/`` still raises, which two
    tests in this repository depend on.
    """
    recorder = _deprecation_recorder.DeprecationRecorder()
    _deprecation_recorder.install_recorder(monkeypatch, recorder)
    try:
        yield recorder
    finally:
        _deprecation_recorder.reset_active()


@pytest.fixture
def single_owner_device_registry() -> _single_owner_registry.SingleOwnerDeviceRegistry:
    """Return a device registry double implementing the Core 2026.8+ rules.

    Opt-in, not autouse: ``_StubDeviceRegistry`` still models the pre-2026.8
    multi-owner world and is what several hundred existing assertions expect.
    Switching everything at once would conflate a behaviour migration with a
    test-harness migration.  New assertions about single ownership use this
    fixture; ``tests/test_device_registry_single_owner_contract.py`` proves it
    matches real Core.
    """
    return _single_owner_registry.SingleOwnerDeviceRegistry()
