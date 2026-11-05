# AGENTS.md — Test scaffolding guidance

> **Scope & authority**
>
> Applies to the entire `tests/` tree.

## Cross-reference index

* [`custom_components/googlefindmy/AGENTS.md`](../custom_components/googlefindmy/AGENTS.md) — Config flow guidance describing optional `ConfigEntry` attributes and discovery safeguards that the shared stubs depend on.

## Flow helper return modes

The bundled Home Assistant stubs intentionally mix synchronous and
asynchronous helper signatures to mirror real-core behavior. The
`ConfigFlow.async_show_form` implementation in
[`tests/conftest.py`](tests/conftest.py) is defined as an `async def`, so
calling it yields an awaitable that resolves to the response dict. Tests
may temporarily replace that attribute with synchronous callables when
exercising legacy code paths, while the `OptionsFlow.async_show_form`
helper remains synchronous by default. When adding new tests or stubs,
keep this split explicit so flows under test can safely handle both
awaitable and immediate responses.

When a test or helper needs access to Home Assistant's config-entry
exception classes, call
`tests.helpers.config_entries_stub.install_config_entries_stubs(module)`
first and then reference the exceptions directly off the populated module
(for example, `module.ConfigEntryAuthFailed`, `module.OperationNotAllowed`).
The helper installs every config-entry related export in one place,
including the `UnknownEntry` and `UnknownSubEntry` stubs added for
2025.11 parity, so new tests only need to import the helper once rather
than duplicating definitions across `tests/conftest.py` or other
fixtures.

See the modern registry TypeError propagation reminder in
[`tests/test_coordinator_device_registry.py::test_modern_registry_typeerror_does_not_trigger_legacy_retry`](tests/test_coordinator_device_registry.py#L863-L885)
when updating registry expectations so new coverage stays aligned with
Home Assistant's current registry keyword surface.

### Canonical config-entry stub: `make_config_entry`

New tests **must** use
`tests.helpers.config_entries_stub.make_config_entry(...)` instead of
ad-hoc `SimpleNamespace(entry_id=..., data=..., options=...)` constructions.
The factory guarantees that `data` and `options` are always plain dicts
(never `None`), which is exactly the contract production helpers like
`_opt()` and `entry.data.get(...)` rely on. Ad-hoc stubs that omit
either field — or set them to `None` — are the structural cause of the
`AttributeError` failures fixed defensively in PR #172.

Use the full import path; do not re-export through
`tests/helpers/__init__.py` (mirrors the convention for
`install_config_entries_stubs`):

```python
from tests.helpers.config_entries_stub import make_config_entry

entry = make_config_entry(
    entry_id="test_entry",
    data={"oauth_token": "abc"},
    options={"poll_interval": 60},
)
```

Field rules:

- `entry_id`: explicit when the test asserts on the value; otherwise
  auto-generated (`entry-test-<n>`, not deterministic across runs).
- `data`/`options`: pass a dict literal; the factory copies it (in-place
  mutation by the test does not leak into the caller).
- Fields not in the factory signature (test-specific markers, exotic HA
  attributes): pass via `**extra`, ideally with an inline comment
  explaining why the field stays out of the canonical signature.
- Methods (`async_on_unload`, `add_update_listener`, …) are intentionally
  not part of the factory. Tests that need them attach them per-instance.

Wave-based migration of the existing call sites lands in this branch as
follow-up commits (waves 1–3); see the corresponding PR description for
the rollout order.

## Package layout

The test suite is a Python package (`tests/__init__.py`). Use
package-relative imports (for example, `from tests.helpers import foo`)
when sharing utilities across modules so mypy resolves the canonical
module paths consistently.

Scripts that a test runs as a real process (a probe chain, a deliberate
violation for a guard) live under `tests/fixtures/<name>/` with an
`__init__.py` that says what they are for, each file carrying the
repository-relative path header like every other test file. Keep the
`test_*.py` and `*_test.py` naming away from them: pytest collects nothing
there, `ruff` reads all of it. The three instances are `tests/fixtures/registry_guard_probe/`,
`tests/fixtures/kill_probe/` and `tests/fixtures/map_tiles_probe/`; source held in a string literal inside a
test is read by no linter, and a typo there surfaces only as a broken child at
run time.

When a test module depends on optional plugins such as
`pytest-homeassistant-custom-component`, wrap the import in
`pytest.skip(..., allow_module_level=True)` to keep import ordering
intact. The skip guard avoids littering files with inline import
fallbacks and ensures `ruff` continues to enforce top-level grouping.

When stubbing coordinators with asynchronous fixtures, reuse the shared
pytest event loop instead of creating ad-hoc loops so teardown waits do
not conflict with pending tasks.

Whenever the local environment installs the real `homeassistant`
package (for example via `pip install -r custom_components/googlefindmy/requirements-dev.txt`), also
install `pytest-homeassistant-custom-component` and run `pytest -q`.
The plugin ships the canonical Home Assistant stubs that our contract
tests depend on; skipping this pairing risks exercising stale or
partial interfaces.

Expect the paired `pip install --upgrade homeassistant
pytest-homeassistant-custom-component` command to run for roughly five
minutes in the hosted environment. Plan regression runs and shell work
around that window so long installs finish before invoking `pytest`.

When those stubs are active, remember that the entity registry fixture
populates `hass.data["entity_registry"]`. Integration helpers under test
should consult that cached instance before calling
`entity_registry.async_get(...)` so recovery logic inspects the same
registry object as the fixture.

When patching Home Assistant network helpers, patch the module-level
`get_url` import used by the integration (for example,
`custom_components.googlefindmy.__init__.get_url`) rather than the
function exported from `homeassistant.helpers.network` directly. Aligning
stubs with the integration's import pattern prevents drift when
resolving absolute URLs in tests.

### Tracker discovery gating expectations

Refer to the canonical tracker registry guidance in
[`custom_components/googlefindmy/agents/runtime_patterns/AGENTS.md#tracker-registry-gating`](../custom_components/googlefindmy/agents/runtime_patterns/AGENTS.md#tracker-registry-gating).
Keep new tests aligned with that runtime contract instead of duplicating wording here.

One consequence belongs here, because it is a property of the *doubles*: the
self-heal probe asks `entry_reload_gate.entry_reload_is_hopeless` before either
latch, and it passes the `ConfigEntry` it already holds. `_HassStub` in
`tests/test_device_tracker_scanner.py` therefore carries **no**
`async_get_entry` on purpose — its absence is what pins that the gate reaches a
verdict without an entry manager. A test that means to exercise the gate sets
`state` (as a `ConfigEntryState` member, not its string value) or `disabled_by`
on that entry; leaving both at the factory defaults lets the gate answer "not
hopeless" and the test exercises nothing.

### CoordinatorEntity stub overrides

The suite replaces `homeassistant.helpers.update_coordinator.CoordinatorEntity`
with a minimal stub in `tests/conftest.py` (see
`install_homeassistant_core_stubs`). The override removes availability
helpers such as `_handle_coordinator_update` and `_async_write_ha_state`
to keep tests independent of Home Assistant internals. When integration
code relies on those helpers, add explicit guards or local fallbacks so
entities remain importable under the stub; otherwise, `AttributeError`
failures will surface before the real coordinator features are present.

Keep the coordinator stubs aligned with runtime helpers introduced in
`custom_components.googlefindmy.coordinator` (for example, visibility-wait
helpers) so setup paths under test do not regress with missing attributes.
Update `tests/conftest.py` alongside new coordinator helpers to maintain
parity with production behavior.

When adding or adjusting tests, normalize imports with
`python -m ruff check --select I --fix` so `I001` findings resolve without
manual resorting.

### Config-entry mutation tracking lists

Config-entry manager doubles (for example, the `_ConfigEntries` stub in
`tests/test_config_flow_discovery.py`) expose `updated` tracking lists to
record every `async_update_entry` call. When tests emulate Home Assistant's
config-entry helpers, normalize those lists before asserting so duplicate
records introduced by legacy behaviors or task scheduling are collapsed to
the final update payload.

### Home Assistant callback decorator stub

Use `tests.helpers.install_homeassistant_core_callback_stub` whenever a
test needs to ensure `homeassistant.core.callback` exists. The helper
returns the resolved module, accepts an optional `monkeypatch` fixture so
pytest rolls the stub back automatically, and supports attaching to an
existing module via the `module=` argument (pass `overwrite=True` to
replace an existing decorator). Sharing the helper keeps the suite aligned
on the same identity-based implementation instead of scattering inline
`lambda` definitions across multiple tests.

Refer back to the root [`AGENTS.md`](../AGENTS.md#home-assistant-regression-helper)
section for the `make test-ha` shortcut and its `PYTEST_ARGS` override when
you need to install the full Home Assistant regression bundle before invoking
these tests with additional pytest markers or verbosity flags.

A guard in `tests/test_callback_stub_lint.py` fails if a new
``lambda func: func`` stub appears under `tests/`. Use the shared helper
instead of introducing inline decorators so the lint check stays green.

For a quicker bootstrap when you only need the options-flow regression
suite, run [`script/install_options_flow_test_deps.sh`](../script/install_options_flow_test_deps.sh).
The helper installs the minimal requirements bundle defined in
[`requirements-options-flow-tests.txt`](../requirements-options-flow-tests.txt)
so contributors can verify the targeted tests without recreating the
full development environment.

> **Note:** `pytest -q` buffers its trailing summary when the suite is
> quiet. Wait a moment for the command to exit cleanly, or rerun the
> module without `-q` if you need immediate progress output while
> verifying changes with the real stubs.

### Checklist — real Home Assistant stub pairing

Always review and update this checklist whenever modifying tests,
helpers, or fixtures. Every change must ensure the list stays current
and accurate so contributors can rely on it when the genuine
`homeassistant` package plus `pytest-homeassistant-custom-component`
are installed. **Review this checklist on every single change under
`tests/` and update the entries immediately if expectations shift.**

1. **Frame helper bootstrap** — confirm
   [`prepare_flow_hass_config_entries`](helpers/config_flow.py) still
   invokes the frame helper setup path and patches
   `homeassistant.helpers.frame` (including `report_usage`) so
   `ConfigFlow` instances and options flows operate without manual
   monkeypatching. The helper now registers an `importlib.reload`
   hook that re-applies the `OptionsFlow`/descriptor overrides after
   either `homeassistant.config_entries` or
   `custom_components.googlefindmy.config_flow` reloads; keep that
   hook intact so reloading modules during tests does not drop the
   frame helper patches. Refer to the table below for the regression
   tests that cover each patched helper.
2. **Handler default patch** — verify
   [`_ensure_flow_handler_default`](helpers/config_flow.py) continues to
   set a default `handler` on
   `custom_components.googlefindmy.config_flow.ConfigFlow` instances
   when the upstream implementation omits it.
3. **Service validation shim** — ensure the local
   [`_patch_service_validation_error`](helpers/config_flow.py)
   adjustments retain a descriptive `__str__`/`__repr__`
   implementation compatible with tests that assert translated
   messages.
4. **Config entries manager attachment** — double-check that
   [`prepare_flow_hass_config_entries`](helpers/config_flow.py) still
   assigns the fake manager to `hass.config_entries`, seeds
   `loop`/`loop_thread_id`, and works with the
   [`FakeConfigEntriesManager`](helpers/homeassistant.py).
5. **Options flow config-entry setter** — confirm the patched
   `config_entry` property on Home Assistant’s `OptionsFlow` stub keeps
   accepting assignment so options flow tests can store the entry
   reference (see [`helpers/config_flow.py`](helpers/config_flow.py)).

| Patched helper | Regression coverage |
| --- | --- |
| Frame setup proxy (`module.set_up` / `module.async_setup`) | `pytest tests/test_options_flow_* -q` (invoked via `tests/helpers/config_flow.py::prepare_flow_hass_config_entries` fixtures) |
| `report_usage` proxy (`module.report_usage`) | `pytest tests/test_options_flow_config_entry.py::test_options_flow_reuses_existing_hass` |
| Reload hook (`importlib.reload` patch) | `pytest tests/test_options_flow_reload.py::test_frame_helper_patches_survive_reload` |
6. **Module guards** — validate that tests importing optional Home
   Assistant components continue to wrap those imports in
   `pytest.skip(..., allow_module_level=True)` guards so the suite
   degrades gracefully when the plugin is absent. One named exception, because
   graceful degradation is the wrong default for it: a test whose *purpose* is
   to ground a stub's shape in the real core (point 10) imports the plugin
   inside the test body and fails loudly instead, since skipping it restores
   exactly the blindness it exists to remove. Such a test is expected to be the
   only casualty of a missing plugin in its module, which is why the import is
   function-local rather than at module scope.
7. **Cross-test symbol identity** — confirm
   `detect_coordinator_identity_leaks()` in `tests/conftest.py` still runs from
   `pytest_runtest_teardown` and that its reference is still frozen in
   `pytest_configure`. Reading the reference on demand instead would let a
   patched source module move the yardstick along with the measurement. See
   [Patching a symbol that other modules copy at import time](#patching-a-symbol-that-other-modules-copy-at-import-time).

8. **Options-flow reload doubles** — the options steps for semantic locations,
   subentry repairs, device visibility (`async_step_visibility`, whose reload
   is the only thing that gets an un-ignored device its entity back) and
   settings (`async_step_settings`, which asks for its own reload since the
   handler stopped inheriting one from `OptionsFlowWithReload`, and asks only
   when the submission actually changes the options)
   schedule their reload through
   `config_flow._schedule_claimed_reload`, so their manager doubles must expose
   a **synchronous** `async_schedule_reload` recorder (the core method is a
   `@callback`, an `async def` here would make the production call site look
   awaitable) and keep `async_reload` next to it as a tripwire, so an assertion
   can tell a silent fall back to the awaited variant from the intended path.
   Assert both sides (`scheduled_reloads == [entry_id]` **and** `reloads == []`);
   a one-sided assertion goes green on the "no lever" branch, which is how a
   missing recorder once slipped through. A double whose `async_reload` is
   *awaited* must return the core's `bool` (`return True` on success), not
   `None`: `_release_claim_when_reload_fails` reads the task's result as well
   as its exception, because the core reports a failed unload — and a component
   it could not set up — by returning `False` rather than by raising. A `None`
   therefore makes every successful reload look like a dead end, hands the
   latch back, and silently defeats the coalescing such tests assert.
   Entry stubs used by those tests must
   also carry `state`, `source` and `disabled_by`: `entry_reload_gate.entry_reload_is_hopeless`
   reads all three via `getattr(..., None)` and fails open, so a stub without
   them lets the state gate answer "not hopeless" unconditionally and a test
   that claims to exercise the gate exercises nothing. Use the submodule form
   `from homeassistant.config_entries import ConfigEntryState`, which is the
   module world the production comparison lives in. Note that
   `make_config_entry` defaults `state` to the **string** `"loaded"` while
   `entry_reload_gate.TERMINAL_ENTRY_RELOAD_STATES` is a `frozenset[ConfigEntryState]`: a string
   never matches, so a test meaning to exercise a terminal state has to pass the
   enum member rather than its value.
   One trap is worth naming for the *unchanged* case of `async_step_settings`,
   because it goes green for the wrong reason rather than failing: submitting the
   same form twice against one hass double leaves the shared latch claimed from
   the first run, so the second stands down as a foreign owner and proves nothing
   about the change detection. Derive the settled state on a throwaway entry,
   build a fresh entry and double from it, and assert the latch is still free
   (`_latch_is_free`) next to the two empty recorders — that assertion is what
   separates "never claimed" from "claimed and then not scheduled", so it is the
   guard for this trap rather than a separate rule.

9. **Direct-reload result doubles** — two paths evaluate the reload *result*,
   not only exceptions, and both ask
   `entry_reload_gate.falsy_reload_left_the_latch_behind` what a falsy result means (the config flow imports it under its former private name, so `config_flow._falsy_reload_left_the_latch_behind` still resolves and existing tests keep reaching it there):
   the non-interactive discovery update inline after its `await`, and
   **reconfigure** where its hand-off to the core scheduler was refused
   (`_give_up_after_a_falsy_reload`). The rule below therefore applies to
   reconfigure doubles as well, and it decides which branch a test reaches: with
   the domain **in** `components` the helper answers "a lifecycle hook already
   released the latch" and the path leaves a stranger's claim alone; with a
   readable list that lacks the domain it answers "still ours" and the path
   releases. A test that omits `hass.config` reaches neither branch on purpose,
   it only pins the fail-open default.
   That helper reads `hass.config.components`, so a hass double for those tests
   must carry a `config` object with a `components` container. Without it the
   helper fails open and reports every falsy reload as a dead end, which makes
   the "a lifecycle hook already released it" branch untestable and lets a test
   that claims to exercise it exercise nothing. It also reads the entry's
   `source` and `state`, because `ConfigEntry.async_setup` can bail before our
   hook for an ignored entry or a failed migration while the component stays
   loaded; entry doubles for those branches need both fields, and the state has
   to be compared against the **same** `ConfigEntryState` the production module
   imported (`from homeassistant.config_entries import ConfigEntryState`), not
   `config_entries.ConfigEntryState`, which is a different object under the stub. Their `async_reload` must be
   `async` and return the core's `bool`: a *synchronous* double never reaches
   the closure at all, because production guards it with
   `inspect.isawaitable`. Two of the module's older doubles are synchronous for
   that reason and are not a template for these tests.

10. **Read-only mapping attributes on entry doubles** — Home Assistant hands
    `ConfigEntry.subentries`, `.data` and `.options` out as `MappingProxyType`,
    never as a `dict`. An entry double that exposes a plain `dict` is therefore
    not a harmless simplification: production narrowing an `isinstance` guard
    to `dict` is false for every real entry and true for the double, so the
    guard silently skips instead of failing, and the test stays green.
    `config_flow._gather_subentry_options` carried exactly that guard from
    2025-10-27 until it was fixed, and the symptom was not an exception but a
    selection reduced to its synthetic fallback option, so no assignment could
    persist. A double that models one of these attributes should expose the
    read-only view and keep the mutable store private, with writes going
    through methods that mirror the core's own
    (`tests/test_options_flow_subentries.py::_EntryStub` shows the shape, but
    only for `subentries`: its `data` and `options` are still plain dicts, so
    copy the pattern rather than assuming the file already applies it
    everywhere). `make_config_entry` does not model `subentries` at all,
    so it is untouched by the fix, but its `data`/`options` are plain dicts and
    the core's are not (measured: all three attributes come back as
    `mappingproxy`). That divergence is deliberate and stays — the guarantee
    documented above is about those fields never being `None`, which is what
    `_opt()` and `entry.data.get(...)` rely on — but it is the same blind spot
    one attribute over, so a production `isinstance(..., dict)` on `data` or
    `options` would go unnoticed here too. A sweep at the time of writing found
    no such guard, only the `subentries` one that was fixed and one unreachable
    branch in `__init__.py`; treat a new one as a defect rather than as a style
    question. One caveat when grounding such a shape against the core:
    `conftest.py` installs a synthetic
    `homeassistant.config_entries` in `sys.modules`, so the
    `from homeassistant.config_entries import ...` form reaches the stub while
    `import homeassistant.config_entries as ...` reaches the real submodule
    through the package attribute. When a double must hold the *genuine* core
    class for its guarantees to mean anything (frozen writes,
    `MappingProxyType` fields), pick the import form deliberately **and assert
    the resolution at the construction site** (`is_dataclass`,
    `__dataclass_params__.frozen`) rather than trusting the form to have worked
    -- see `_ap1_subentry` in
    `tests/test_subentry_manager_registry_resolution.py`. Getting this wrong is
    not a red test: the assertions simply become vacuous and stay green, which
    is how two rounds of "fixes" there once landed without effect.

    The mirror of that mistake is assuming a double binds when it does not.
    `tests/helpers/config_entries_stub.py` defines `OptionsFlow` and
    `ConfigFlow` with no-op bodies (`add_suggested_values_to_schema` returns
    its argument unchanged), but those are the **fallback** for an absent core.
    With `homeassistant` installed — 2026.2.3 in the pinned environment —
    `config_flow.OptionsFlowHandler.__mro__` runs
    `OptionsFlowHandler → homeassistant.config_entries.OptionsFlow →
    ConfigEntryBaseFlow → homeassistant.data_entry_flow.FlowHandler`, and
    `add_suggested_values_to_schema` resolves to the genuine rebuilding
    implementation, which copies every marker, drops `advanced` fields and sets
    `description["suggested_value"]`. Reading the stub file and concluding "the
    suite neutralises this call" is therefore wrong in both directions: it
    understates what the test covers, and it hides that `suggested_value`
    outranks `default` in the rendered form. Resolve the binding
    (`cls.__mro__`, `inspect.getsourcefile`) before reasoning about which body
    runs; a stub file existing is not evidence that it is the one bound.
11. **Service registry, service call and authorization doubles** —
    `tests/test_service_doubles.py` pins the doubles in
    [`tests/helpers/service_doubles.py`](helpers/service_doubles.py) against
    the genuine core: the real `async_register_admin_service` registers
    through `FakeServiceRegistry` (the admin wrapper is unwrapped to the
    original handler, its schema and admin flag are recorded), a registry with
    the old three-argument `async_register` is rejected by that helper, and the
    stub `Unauthorized`/`UnknownUser` take the same keyword arguments as the
    real classes. When Home Assistant changes the wrapper shape,
    `unwrap_admin_handler` raises instead of passing a foreign `partial`
    through; update the doubles and this test together.
12. **Exception stubs with translation metadata** —
    `tests/test_exception_stub_translation_parity.py` builds
    `HomeAssistantError`, `UpdateFailed` and `ConfigEntryNotReady` from the
    `conftest` stubs and from the core and compares the stored
    `translation_domain`, `translation_key`, `translation_placeholders` and
    `retry_after` plus `str()`. Features an older core lacks (`retry_after`
    before 2026.1.3, class defaults of the translation fields before
    2026.9.3, as measured) are skipped with a reason there and fail on any
    core from that version on. A text argument wins, a key without text
    renders the raw key (the core's result when no translation is cached),
    and `ConfigEntryNotReady` without text falls back to `str(__cause__)`.
    The stub `UpdateFailed` derives from the stub `HomeAssistantError` as in
    the core, so `except HomeAssistantError` now also catches it in tests.
    `ServiceValidationError` keeps its own `"domain:key"` rendering (see the
    string representation contract below). Compare by construction, not by
    `inspect.signature`: the translation arguments live only in
    `HomeAssistantError.__init__`.

Treat this checklist as a living document: if a new helper or guard
becomes necessary, add it here and verify each item before completing
any change under `tests/`.

### Helper completeness reminder

When inserting new helper classes alongside existing stubs (for example,
`_PlatformRecorder` in `tests/helpers/homeassistant.py`), ensure the
updated class remains fully defined after edits. Partial insertions or
copy/paste adjustments can silently drop required stub attributes or
methods; always re-run the relevant tests after modifying helpers to
confirm their interfaces stay intact.

Contract tests under `tests/test_entity_device_info_contract.py` expect
`pytest-homeassistant-custom-component`'s bundled `homeassistant`
stubs to be installed. Without that optional dependency, pytest will
skip the module-level guard and report a missing `homeassistant`
package error when the test collection runs.

## Async tests

`pytest-asyncio` ships with the repository and `pytest` manages the event
loop according to the [`asyncio_mode = "auto"`](../pyproject.toml)
configuration. **Never call `asyncio.run()` inside tests.** It creates a
competing event loop and breaks Home Assistant’s managed loop fixtures.
Always author coroutine tests as `async def` with `@pytest.mark.asyncio`
(`pytestmark = pytest.mark.asyncio` at module scope is also fine) and
`await` the coroutine directly. This keeps event-loop handling centralized
and avoids duplicated scaffolding in each test module.

### Async setup entry harness expectations

The shared ``AsyncSetupEntryHarness`` in
``tests/test_hass_data_layout.py`` centralizes repeated patching for
``async_setup_entry`` coverage. Keep the documented attribute consumers in
sync with reality when refactoring:

* ``test_hass_data_layout`` relies on ``integration``, ``button_module``,
  ``map_view_module``, ``hass``, ``entry``, and ``cache``.
* ``test_async_setup_entry_propagates_subentry_registration`` consumes only
  ``integration``, ``hass``, and ``entry``.

If a test stops using an attribute, update both the harness comment and this
list so future cleanups can trim unused fields confidently.

### Patching a symbol that other modules copy at import time

A module that does `from .coordinator import GoogleFindMyCoordinator` at module
level takes a **copy** of whatever the source module holds at that moment.
`monkeypatch` only undoes the assignments it made itself, so a module whose
*first* import happens inside a patch window keeps the stub for the rest of the
session. The symptom appears far from the cause: an unrelated test file that
subclasses the poisoned symbol later fails with
`HomeAssistantError: googlefindmy coordinator not ready` from
`entity.resolve_coordinator`.

Whenever you patch a symbol that other modules bind at module level:

* **Import every consumer before the first `monkeypatch.setattr`**, so the copy
  they take is the production object. Call
  `tests.conftest.import_coordinator_consumers()`; it imports each module of the
  shared tuple `tests.conftest.COORDINATOR_CONSUMER_MODULES` with a literal
  `import` statement. The tuple was derived from an AST scan for module-level
  `ImportFrom` nodes naming the symbol, not from guesswork, and
  `tests/test_guard_coordinator_identity.py` pins that the helper imports
  exactly its modules. Do **not** write a local subset or a loop over
  `importlib.import_module`: two harnesses used to carry hand-written,
  incomplete copies, which is exactly the drift the shared helper prevents.
* **Or patch the consumers too**, if a test genuinely needs them to see the
  stub (this is why the harness pops and re-imports `map_view`).

Three harnesses patch the symbol and therefore call the helper:
`_prepare_async_setup_entry_harness` (`tests/test_hass_data_layout.py`),
`_patch_integration_runtime` (`tests/test_device_entity_registration.py`) and
`test_integration_device_info_uses_service_device`
(`tests/test_entity_device_info_contract.py`). A fourth site,
`tests/test_fcm_repair_reauth.py`, patches `repairs.GoogleFindMyCoordinator`
without driving a platform setup and is therefore not affected.

`tests/conftest.py` enforces the rule for `GoogleFindMyCoordinator`:
`detect_coordinator_identity_leaks()` runs in `pytest_runtest_teardown`,
**restores** the production class and then fails the **causing** test. The
restore is not cosmetic: reporting alone would leave the stub in place and
redden every following test as well, turning one attributable failure into a red
session. Its scope is deliberately one symbol, the one with a measured failure,
while an AST sweep over `tests/` found eleven further symbols with the same
structure (`ClientSession`, `Store`, `HomeAssistant`, `WebDriverWait`, and
others). Those are structural candidates, not known defects; extend the guard
when one of them actually bites.

`tests/test_guard_coordinator_identity.py` covers the detection logic and pins
the fix statically: the tuple must still equal the AST-derived set of
module-level binders, `import_coordinator_consumers()` must import exactly its
modules with plain, unconditional `import` statements, and in **each** of the
three harnesses a direct call to that helper must precede the first
`monkeypatch.setattr`. The teardown *wiring* is verified by mutation rather
than by a test: remove the early imports from
`_prepare_async_setup_entry_harness` and
`test_programmatic_subentry_creation_triggers_setup_and_entities` errors at
teardown naming `device_tracker`. Testing that wiring from inside the session
would require leaving a real leak behind. An earlier draft tried a runtime
reproduction (dropping the consumers from `sys.modules` and re-importing them
inside the window) and leaked `entity.get_url` into `test_metadata_helpers.py`
instead. Do not reintroduce that approach.

### No module stubs in `sys.modules` at import time

Do not write a stub for a `custom_components.googlefindmy.*` module into
`sys.modules` at module level of a test file, not even behind an
`if "<name>" not in sys.modules:` check. Whether the real module was already
imported depends only on collection order, and the stub then stays for the rest
of the session. `tests/test_manual_locate_resolution.py` used to do this for
`diagnostics` and `map_view`: the full run stayed green, while a run that
collected it before the first real `map_view` import errored in every test,
because the session fixture `_prime_leaflet_asset_cache` found no
`_LEAFLET_CACHE` on the stub. If a test genuinely needs a stub, install it with
`monkeypatch.setitem(sys.modules, ...)` inside the test or a fixture.
`tests/test_guard_import_time_module_stubs.py` checks every file under `tests/`
for import-time writes (subscript assignment, `|=`, `setdefault`, `update`,
`__setitem__`, also through `import sys as …`, `from sys import modules` or a
plain `name = sys.modules`); a key that is not a string literal counts as a
violation. Its docstring lists what it does not see, for example a write inside
a function that is called at import time, other alias forms and indirect access
such as `getattr(sys, "modules")`. The rule above covers those as well.

### Fresh import of an integration module under real Home Assistant modules

`custom_components.googlefindmy.*` is imported under the stubs and stays bound
to them; `use_real_homeassistant_modules` swaps in the real Home Assistant
modules but does not rebind integration modules that are already imported. A
test that needs a module bound to the real classes (real `Unauthorized`, real
registries, the real admin helper) imports it freshly inside a fixture, after
the swap, and hands every `custom_components.googlefindmy.*` key the import
creates to `monkeypatch`: pop the new module and `monkeypatch.setitem` it back,
so `monkeypatch` removes it on teardown and no module bound to the real
modules stays in `sys.modules` for later stub-world tests. A key that existed
before is removed through `monkeypatch.delitem` and restored on teardown. This
follows the rule above: `sys.modules` is only touched through `monkeypatch`, in
a fixture, never at import time. `tests/test_services_permissions.py`
(`_fresh_import`, `services_module`) is the reference, and
`tests/test_services_permissions_teardown.py` sorts after it and checks from the
stub world that `services` is bound to the stubs again.

The same file also builds Home Assistant itself from a freshly imported
`pytest_homeassistant_custom_component.common` instead of using the plugin's
`hass` fixture. The plugin's fixture builds Home Assistant from the modules the
plugin imported when it was loaded; after the swap the test and the freshly
imported module use other copies, so enums differ by identity
(`RegistryEntryDisabler`, `HassJobType`). Measured: an admin action registered
through the fresh `async_register_admin_service` is then never run, because the
plugin's core does not recognise the job type of the fresh `HassJob`. Build the
instance from the same copy (`ha_common` and `hass` fixtures there) when a test
crosses that boundary.

## No full-command-line process kills

`pkill -f <pattern>` selects by matching the **full command line** of every
process on the machine, so any ancestor of the calling process whose argv happens
to carry the pattern is a valid target. The tool spares only itself, never its
caller. (`killall` and `pkill -x` match the command *name* and are a different,
narrower class — see the guard section below.) Measured on 2026-07-28:
`chrome_driver.py` ran
`subprocess.run(["pkill", "-f", "chrome"])` in the Chrome auth flow, and a pytest
invocation that had `tests/test_chrome_driver.py` in its argv was killed by the
CLI subprocess it had started — exit 143 (SIGTERM), no traceback, no summary. The
A/B control differed only by a `-k` filter matching nothing, i.e. by the word in
argv alone: without it exit 0, with it exit 143.

Use `chrome_driver._terminate_matching_processes(pattern)` instead: it keeps the
`pgrep` selection (same matcher, so the same processes are found) but filters the
own PID and the whole ancestry before signalling. `taskkill /f /im chrome.exe` is
**not** affected — it matches the image name, not a command line.

`tests/test_guard_process_pattern_kill.py` pins the **literal** forms with an AST
scan over `custom_components/googlefindmy/**` and `tests/**`: an argv list, a
`shell=True` string, and `["sh", "-c", "…"]`. Its `LEGACY_ALLOWLIST` is empty
because both historical call sites were migrated in the same change.

Two deliberate narrowings keep it free of false positives, because a
repository-wide guard that cries wolf gets switched off rather than obeyed.
First, only the `-f`/`--full` form is rejected: `pkill --help` distinguishes
`-f, --full` ("use full process name to match") from `-x, --exact` ("match
exactly with the command name"), and the name-matching variants (`pkill -x`,
`pkill -P`, `killall`, `taskkill /im`) cannot hit a process that merely
*mentions* the pattern — a different failure class. Second, calls are resolved
against the module's own imports rather than matched by method name, so an
unrelated `runner.run([...])` stays invisible while `import subprocess as sp`
does not. Both narrowings came from Codex review findings on this PR.

The guard deliberately does *not* see commands assembled from variables or
`os.exec*` invocations — that is the remaining price, and it means the guard is a
ratchet, not a proof.

CLI subprocess tests carry the second half of the defence: `tests/test_main.py`
runs them through the `cli_sandbox` fixture. It shims `pkill`/`killall` onto
`PATH` into one sentinel file and `pgrep` (the sanctioned lookup) into a separate
one, so a kill attempt is a failed assertion while a legitimate lookup is not; it
points `GOOGLEFINDMY_SECRETS_PATH` at a throwaway file so the tests stop reading
and writing the developer's own `Auth/secrets.json`; and it *drops*
`GOOGLEFINDMY_CONTAINER_LOGIN`, `GOOGLEFINDMY_ASSUME_INTERACTIVE` and the other
branch-steering variables from the
inherited environment. That last part is not cosmetic: with the variable exported
in the developer's shell, the CLI takes the container branch, skips both the
desktop gate and the cleanup, and actually launches Chromium — the assertions
would pass while covering nothing. New tests that spawn the CLI must use the
fixture, assert `_assert_no_process_kill`, and assert positively *where* the CLI
stopped (`"attended terminal" in stderr`), so they cannot become vacuous when an
unrelated change makes the CLI exit earlier.

## ADM token retrieval contract

The ADM token helpers (`custom_components/googlefindmy/Auth/adm_token_retrieval.py`)
expect an **integer** `android_id`, matching the gpsoauth runtime contract. The
isolated config-flow path under test mirrors this requirement, so regression
tests (for example `tests/test_adm_token_retrieval.py::test_async_get_adm_token_isolated_*`)
must assert integer IDs sourced from secrets bundles, cache reads, or the
fallback constant. Keep expectations synchronized with the production helpers
to avoid drift between config-flow coverage and runtime behavior.

## Home Assistant stubs overview

The fixtures in `tests/conftest.py` provide lightweight stand-ins for key
Home Assistant modules so the integration can be imported and exercised in
isolation. The recent additions mirror enough of the `ConfigFlow` contract to
support discovery update tests and follow the real integration's behavior
closely enough for regression coverage.

### Service registry, service call and authorization stubs

Service registries that integration handlers are registered on, and service
calls they are invoked with, come only from
[`tests.helpers.service_doubles`](helpers/service_doubles.py); do not define a
local class with `async_register` or a local service call double in a test
module. The one exception is a deliberately incompatible registry that shows a
rejected signature, such as the three-argument registry in
`tests/test_service_doubles.py`. `FakeServiceRegistry.async_register` accepts the full Home Assistant
signature (schema, `supports_response`, keyword arguments), stores the
unwrapped handler in `handlers` (alias `registered`), the registered object in
`raw_handlers`, the schema in `schemas` and admin registrations in
`admin_services`. `FakeServiceCall(data=None, context=None, domain=..., service=...)`
defaults to a `FakeContext` without `user_id`, which is how an automation
calls a service. `tests/conftest.py` installs both as `homeassistant.core.ServiceCall`
and `homeassistant.core.Context`, adds the stub `Unauthorized` and
`UnknownUser` to `homeassistant.exceptions` through
`install_service_auth_stubs`, and gives the stub entity base a `_context`
attribute. Import the names from `tests.helpers.service_doubles` directly; apart
from `FakeServiceRegistry`, which `tests.helpers` and
`tests.helpers.homeassistant` keep exporting under its old name, they are not
exported through `tests.helpers`. In the stub world the admin wrapper in
`raw_handlers` raises the real `homeassistant.exceptions.Unauthorized`, not the
stub class; test admin rejections under real Home Assistant modules.

### `ServiceValidationError` string representation contract

The `ServiceValidationError` stub derives a descriptive message from
`translation_domain`/`translation_key` when positional arguments are absent and
stores the resolved string on the instance. Both `__str__` and `__repr__`
return that stored message so assertions see the derived text while
`translation_placeholders` and the other translation metadata remain intact for
tests that inspect them directly. When extending the stub, preserve this
behavior to keep message-focused assertions and translation checks aligned.

When fabricating bare `hass` objects outside the shared fixtures, import the
stubbed `homeassistant.helpers.frame` module from `tests.conftest` and call
`frame.set_up(hass)` before assigning `config_entry`. The guard mirrors Home
Assistant's runtime expectations and prevents spurious `ValueError` failures
during options-flow tests. Prefer routing new helpers through
`tests.helpers.config_flow.prepare_flow_hass_config_entries` so the frame setup
and manager attachment stay consistent across modules.

When updating `_StubConfigEntries` in
`tests/test_hass_data_layout.py`, keep its lookup and registration
semantics aligned with `tests.helpers.homeassistant.FakeConfigEntriesManager`.
The stub intentionally mirrors the shared helper so subentry registration
retries in layout tests exercise the same pathways guarded by the reusable
manager.

### Alignment reminder cross-references {#alignment-reminder-cross-references}

<!-- snippet: alignment-reminder-references -->
<details id="alignment-reminder-reference" open>
<summary><strong>Reusable list</strong> — link to this anchor instead of duplicating the entries</summary>

* `tests/helpers/homeassistant.py` (`FakeConfigEntriesManager` inline
  reminder)
* `tests/test_hass_data_layout.py` (`_StubConfigEntries` docstring and
  lookup comment)

</details>
<!-- end snippet: alignment-reminder-references -->

#### Transient `UnknownEntry` simulation helpers

`tests.helpers.homeassistant.FakeConfigEntriesManager` now ships with
first-class controls for reproducing transient `UnknownEntry` races during
tests. Pass a `transient_unknown_entries` mapping when constructing the manager
or call `set_transient_unknown_entry()` on an existing instance to declare how
many initial `async_get_entry` lookups (`lookup_misses`) or `async_setup`
attempts (`setup_failures`) should raise or return `None`. The helper also
tracks `lookup_attempts` so assertions can confirm the integration retried
before child entries became visible. Prefer these controls over ad-hoc
monkeypatching whenever a regression exercises delayed subentry registration or
setup retries.

### Quick reference — subentry registry race monkeypatch targets

Use these shortcuts when writing regression tests that need to emulate missing
or late subentry registrations:

* `"custom_components.googlefindmy._registered_subentry_ids"` — patch to
  return an empty set and simulate Home Assistant omitting the new subentry
  from its registry during setup.
* `FakeConfigEntriesManager.set_transient_unknown_entry(entry_id, *, lookup_misses=..., setup_failures=...)`
  — drive deterministic `UnknownEntry` responses for repeated
  `async_get_entry` or `async_setup` calls.
* `FakeConfigEntriesManager._registered_subentry_ids` — seed or clear the
  manager's cached registry snapshot before calling integration helpers that
  inspect the parent's registered subentries.

> **Reminder:** Because these helpers already model `UnknownEntry` behavior,
> new tests should avoid importing `custom_components.googlefindmy`'s
> `UnknownEntry` fallback directly. Use the provided knobs on
> `FakeConfigEntriesManager` instead so transient lookup coverage stays coupled
> to the shared stubs.

## Migration coverage expectations

`tests/test_config_entry_migration.py` exercises the integration's migration
helpers directly. Keep assertions focused on
`custom_components.googlefindmy.__init__.async_migrate_entry` and the associated
duplicate-detection utilities:

* Authoritative entries must soft-migrate known options before the version bump
  and clear any lingering duplicate-account repair issues.
* Non-authoritative duplicates should halt migration, leave options untouched,
  and refresh their `duplicate_account_*` repair issue with the provided
  `migration_duplicate` cause.
* When account metadata cannot provide a usable email (for example, the
  username is missing or whitespace only), the migration should succeed without
  creating duplicate repair issues. The `issue_registry_capture` fixture will
  therefore assert that `created` stays empty and that any stale
  `duplicate_account_<entry_id>` entry is removed during the run.
* `issue_registry_capture` patches the package-level `ir` module, which every
  platform module imports as the same object, so it also captures issues the
  platforms create. Each captured payload carries `issue_id`, `domain`,
  `translation_placeholders` and every other keyword argument as passed (for
  example `severity`, `breaks_in_ha_version`, `translation_key`, `is_fixable`,
  `is_persistent`).

### Duplicate disable fallbacks

Legacy-core scenarios are simulated in tests by monkeypatching
`hass.config_entries.async_set_disabled_by` to raise `TypeError`. This ensures
manual-action repair issues remain covered whenever the disable API is
unavailable.

### `ConfigFlow` helper methods

* `ConfigFlow.async_set_unique_id()` stores the provided ID on both
  `self.unique_id` and `self._unique_id` to match the attribute layout Home
  Assistant uses in production flows. This mirrors the real helper so tests can
  inspect either attribute without diverging from upstream behavior.
* `ConfigFlow.async_create_entry()` captures the created entry payload so tests
  can assert both the `title` and `data` returned by a flow. The helper records
  each call in order and returns the provided value unchanged to mimic Home
  Assistant's behavior.
* `_abort_if_unique_id_configured(*, updates: Mapping[str, Any] | None = None)`
  updates the matched config entry and triggers `hass.config_entries`
  `async_update_entry` followed by `async_reload`. When an entry is updated, the
  helper raises `AbortFlow` with the `already_configured` reason, maintaining the
  same contract as Home Assistant.
* `_async_invoke_discovery_update()` and the exposed
  `async_step_discovery_update_info()` wrap these helpers to drive the discovery
  update path: normal payloads update the entry, malformed payloads abort with
  `invalid_discovery_info`, and ingestion errors propagate the appropriate
  `AbortFlow` reason (for example `invalid_auth`).

### Exception and sentinel behavior

* `ConfigEntryAuthFailed` mirrors Home Assistant's auth failure exception so the
  integration can convert authentication issues into the correct abort reasons.
* The custom `UNDEFINED` sentinel behaves like the upstream constant and is
  available for any tests that need to assert default/optional handling.
* When helpers raise `ConfigEntryNotReady`, companion regression tests should
  assert both the warning log and the exception so behavior stays aligned with
  the integration's automatic retry expectations.

### Adding new helpers

Extend the stubs only when a test requires additional Home Assistant behavior,
and document any new helpers or contract nuances here so future contributors can
quickly understand the supported surface area.

#### Deferred subentry identifier assignment helper

Use :func:`tests.helpers.homeassistant.deferred_subentry_entry_id_assignment` to
model Home Assistant cores that publish a child subentry's global ``entry_id``
after the parent has already started setup. The helper schedules the
``entry_id`` update after an optional delay and automatically registers the
provided :class:`FakeConfigEntry` with the shared
``FakeConfigEntriesManager``. Reuse it instead of ad-hoc ``asyncio.sleep``
coroutines so regression tests remain concise and consistent. See
``tests/test_subentry_manager_registry_resolution.py`` for a concrete example of
coordinating the helper with provisional runtime objects.

#### Custom config entries manager subclasses

When a regression needs specialized lookup or creation behavior, subclass
``tests.helpers.homeassistant.FakeConfigEntriesManager`` and hand the custom
instance to the ``FakeHass`` fixture. The snippet below mirrors
``DeferredRegistryConfigEntriesManager``, which simulates Home Assistant cores
that lack ``async_create_subentry`` and delay child visibility until a later
lookup:

```python
from types import SimpleNamespace

from tests.helpers.homeassistant import (
    DeferredRegistryConfigEntriesManager,
    FakeConfigEntry,
    FakeHass,
)

parent_entry = FakeConfigEntry(entry_id="parent")
child = SimpleNamespace(entry_id="child", subentry_id="child-subentry", data={})
manager = DeferredRegistryConfigEntriesManager(parent_entry, child)
hass = FakeHass(config_entries=manager)
```

Attach the configured ``hass`` to the integration under test so registry
publication timing and lookup retries match the scenario being exercised.

A subclass that adds *subentry mutators* has four further core contracts to
honour, because the base class carries none of them:

1. ``ConfigSubentry`` is a frozen dataclass, so an update must write through
   ``object.__setattr__`` rather than assigning attributes.
2. ``async_update_subentry`` returns ``False`` when nothing changed, and
   production branches on that return.
3. It raises ``AbortFlow("already_configured")`` on a unique-id collision,
   which production catches and recovers from, so a double that cannot raise
   it silently excludes a live path.
4. Removal replaces ``entry.subentries`` with a fresh ``MappingProxyType``; an
   update does not touch it.

``_CoreLikeSubentryEntries`` in
``tests/test_subentry_manager_registry_resolution.py`` implements all four, but
read what that buys you before treating it as proof. Only contract 1 is pinned
by a test: reverting the ``object.__setattr__`` write turns four cases red.
Mutating away contracts 2, 3 and 4 individually leaves the whole file green
(measured). They are fidelity to the core, not guards, and a double copied from
there inherits that asymmetry.

Contract 1 also only bites once the double holds the *genuine* core class:
against the mutable ``conftest`` stand-in a frozen write is indistinguishable
from a plain assignment, so the same test passes while proving nothing. That is
point 10's import trap in its most expensive form -- see the assertions in
``_ap1_subentry`` for the shape of a check that fails loudly instead.

#### `config_entry_with_subentries` factory

The :func:`config_entry_with_subentries` helper in
``tests.helpers.homeassistant`` builds a :class:`FakeConfigEntry` prepopulated
with subentries. Provide keyword arguments mapping each subentry key to a
``ConfigSubentry`` payload dictionary—mirroring the runtime helper contract—so
tests can focus on their assertions instead of recreating the boilerplate
structure. The factory normalizes identifiers, attaches the entry to the fake
registry cache, and returns the configured ``FakeConfigEntry`` ready for use in
setup and reload assertions.

#### Device registry listing helper

The shared :func:`device_registry_async_entries_for_config_entry` helper in
``tests.helpers.homeassistant`` returns a deterministic list of
``FakeDeviceEntry`` instances for the provided config entry ID. Prefer importing
and patching this helper in registry-focused tests instead of reimplementing
ad-hoc stubs—doing so keeps device-registry expectations synchronized across
the suite and ensures new assertions automatically benefit from future
enhancements to the shared fake registry.

## Config flow subentry support fixture

The :func:`subentry_support` fixture in ``tests/conftest.py`` centralizes the
monkeypatch scaffolding required to toggle between modern Home Assistant cores
that provide ``ConfigSubentry``/``ConfigSubentryFlow`` and legacy builds that do
not. Request the fixture in a test and call ``toggle.as_modern()`` (the default)
or ``toggle.as_legacy()`` before invoking the config flow under test to assert
the correct behavior in each environment.

### Config flow unique_id helper

Use :func:`tests.helpers.config_flow.set_config_flow_unique_id` whenever a test
needs to assign ``unique_id`` on a ``ConfigFlow`` instance. Home Assistant's
metaclass exposes ``unique_id`` as a read-only descriptor on recent cores, and
the helper mirrors the runtime registration path by storing the identifier in
``flow.context``. Example usage:

```python
from tests.helpers.config_flow import set_config_flow_unique_id

flow = ConfigFlow()
set_config_flow_unique_id(flow, "test-id")
```

#### Config entries flow stub reminder

When fabricating config-entry manager stubs for tests, import
``tests.helpers.config_flow.config_entries_flow_stub`` and attach the returned
flow manager to the stub. The shared helper records flow initialization calls
so discovery-trigger tests (for example,
``tests/test_cloud_discovery_trigger.py``) continue to receive consistent
call-tracking behavior without recreating ad-hoc ``SimpleNamespace`` objects.
The helper returns a manager object exposing ``.flow`` for parity with Home
Assistant's runtime contract—reuse that attribute instead of constructing
inline wrappers in individual tests.

#### Cloud discovery discovery-key fallback coverage

``tests/test_cloud_discovery_trigger.py`` now asserts that
``custom_components.googlefindmy.discovery._trigger_cloud_discovery`` always
passes a structured discovery key to the helper, even when
``cf.DiscoveryKey`` is unavailable in stripped environments. The regression
uses the `_DiscoveryKeyCandidate` dataclass as the deterministic fallback and
asserts that ``namespace`` and ``stable_key`` still match the payload. When
updating the helper or its tests, keep the fallback object exposing a
``key`` attribute that returns the ``(namespace, stable_key)`` tuple so the
assertions remain valid without depending on Home Assistant internals.

### Config entries unique ID lookup helper

Stubs that need :meth:`ConfigEntries.async_entry_for_domain_unique_id` behavior
should import :func:`tests.helpers.config_flow.stub_async_entry_for_domain_unique_id`
and expose it on their manager shim. The helper already understands the variety
of storage layouts used across the test suite, so reusing it keeps new stubs
aligned with Home Assistant's matching semantics without reimplementing the
logic in each test module.

For convenience, ``tests.helpers.config_flow.ConfigEntriesDomainUniqueIdLookupMixin``
wraps the helper above and can be inherited by stubbed manager classes. Reuse
the mixin whenever possible so flow stubs across the suite converge on the same
lookup behavior without duplicating the glue code.

### Config subentry factory contract

``ConfigFlow.async_get_supported_subentry_types`` **must** expose zero-argument
factories. Each factory returns a fresh subentry flow instance and the
integration injects the active ``config_entry`` into the instance. Returning
handler classes instead of factories will raise ``TypeError: missing
'config_entry'`` when Home Assistant calls ``factory()`` during the "Add"
interaction. Factories must always return **new** instances (no caching); tests
assert two consecutive calls return distinct objects. Home Assistant constructs
update flows itself using ``(config_entry, subentry)`` and does not expect the
integration's factories to bind existing subentries.

### Subentry platform forwarding expectations

Core 2025.11.2 does **not** expose a ``config_subentry_id`` keyword on
``async_forward_entry_setups``. Integration setup must therefore let Home
Assistant schedule subentry platforms automatically after
``_async_setup_subentry`` returns ``True``. Tests must enforce the following
contract:

* Parent setup tests should assert that subentries are created and **not**
  manually forwarded; any helper resembling ``_async_ensure_subentries_are_setup``
  should remain inert.
* Platform tests should continue to assert that Home Assistant invokes
  ``async_setup_entry`` with a populated ``config_subentry_id`` (delivered by
  Core) and that entities/devices refuse to load when the identifier is missing
  while accepting valid IDs. Platform coverage should also verify that entity
  factories iterate ``entry.runtime_data`` and pass ``config_subentry_id`` into
  ``async_add_entities`` per child, matching the WAQI SSoT pattern.
* When a regression needs deterministic ``config_subentry_id`` fallbacks without
  repeating monkeypatch boilerplate, depend on the
  ``deterministic_config_subentry_id`` fixture from
  [`tests/conftest.py`](tests/conftest.py#L230-L275). Adding the fixture to a test
  function signature automatically patches the integration platforms (button,
  sensor, binary_sensor, device_tracker, and entity helpers) so they synthesize
  ``"<entry_id>:<platform>"`` identifiers whenever Home Assistant omits the
  value.

- Timebase helpers: `_clamp_and_mask_u32` masks directly to uint32 to preserve
  drift debugging signals (negative deltas wrap instead of clamping). Keep
  fixtures and assertions aligned with that contract when adjusting
  drift-related tests.

When parent-unload rollbacks are exercised (for example,
``tests/test_unload_subentry_cleanup.py::test_async_unload_entry_rolls_back_when_parent_unload_fails``),
the helper now expects retries **per platform** as part of the cleanup
scheduling. Guard the recorded platform set so regressions neither skip
forwarding nor double-schedule retries.

### Allow-list tests must rule out the unassigned-device merge

A test that pins what a group *may see* through its stored ``visible_device_ids``
**must** make sure the observed view comes from ``allow_filter`` and not from the
unassigned-device merge, which hands the tracker group every device no other
group already **owns or sees**. Both halves are load-bearing: in
``coordinator/subentry.py::_refresh_subentry_index`` the merge subtracts
``stored_assigned_ids`` *and* the ``visible_device_ids`` of every group's
metadata, so a group that stored no key at all sees everything and thereby
blocks the merge without owning anything. (The narrower ownership-only question
belongs to the branch that synthesises a missing tracker subentry; see
``agents/runtime_patterns/AGENTS.md``, which keeps the two apart deliberately.)

Otherwise the test measures the wrong mechanism, and that is not hypothetical:
``tests/test_coordinator_visibility_availability.py::test_refresh_returns_an_unowned_device_through_the_merge``
-- named ``test_refresh_recovers_devices_from_empty_visible_list`` before this
branch, and the old name carried exactly this confusion -- stayed green across
the reading change of ``ffde3fc6``, because its single device came back through
the merge either way.

Two shapes satisfy the requirement, and at least one **must** be present:

* Assert on a group whose stored key neither *is* nor *folds onto* a core key.
  "Not the tracker group" is **not** sufficient, and two measured shapes show
  why: a group storing ``SERVICE_SUBENTRY_KEY`` has its metadata visible ids
  forced to ``()`` regardless of any filter, so ``== ()`` there is vacuously
  true; and a ``tracker``-typed subentry storing ``SERVICE_SUBENTRY_KEY`` is
  folded onto ``TRACKER_SUBENTRY_KEY`` and *is* fed by the merge. So: not
  ``TRACKER_SUBENTRY_KEY``, not in ``NON_DEVICE_SUBENTRY_KEYS``, and not a
  mis-keyed twin that the fold moves onto a core key; or
* give a second group a **non-empty** stored list holding the device under test,
  so the merge cannot return it because that group both owns and sees it.

Prefer both when the assertion is about a shape rather than a group: pinning the
same stored shape on the default group *and* on a group the merge does not feed
is what separates "unrestricted" from "restricted to nothing"
(``tests/test_coordinator_subentry_visibility.py::test_ap3_a_stored_shape_decides_what_a_group_may_see``).
Where a fixture relies on either shape, guard it: assert that the owning group
still sees only its own device, and that the group under test is really not fed
by the merge, so a test that accidentally destroys the separation fails instead
of reporting a false positive
(``::test_ap4_the_record_is_deduplicated_per_subentry_not_per_coordinator``
carries both guards).

## `eval` and `exec` in tests

The security baseline in [`AGENTS.md`](../AGENTS.md) (section 11.2) says never
to use `eval`/`exec`. This section is the only exception, and it holds for
tests only. A test may reference `eval` or `exec` when all of the following hold:

* **Source of the code.** The code it runs is either repository source (an
  integration module inside the checkout, compiled without importing Home
  Assistant) or code the test builds itself from constants written in the test
  file (frozen forms, Hypothesis strategies over fixed parts). It never comes from
  outside the checkout: no network data, no environment variables, no files outside
  the repository, no user input.
* **Listed site.** The reference stands in the table below, with the number of
  references per function (qualified through classes and functions, as in
  `Outer.inner`). `tests/test_guard_eval_exec_sites.py` pins the table against
  every reference to `eval`/`exec` in every Python file git would commit (tracked,
  or untracked and not ignored), not only under `tests/`: a new site, an extra
  reference and a stale row each fail it. Adding a row needs a reason in the pull
  request that meets the first condition.
* **No `noqa`.** `S102` and `S307` are not enabled in this repository, so a
  `# noqa` on such a call suppresses nothing and only suggests a review that never
  took place. The reason lives in this section.

Why an exception: the credential folding guard uses Python itself as its oracle.
`ast.literal_eval` runs neither a comprehension nor a method, and an evaluator of
our own would replace the oracle by the code under test. A subprocess per oracle
call (permitted by section 11.2) was measured on 2026-11-02: one run of that file
makes 12,778 oracle calls (a call of `_python_writes`, `_oracle_runs_to_the_member`
or `_keys_held_after`, or an `eval`/`exec` in one of the listed tests) and takes
about two minutes, and an interpreter start takes 8.6 ms with `-I -S` and 19.2 ms
without flags (median of 30). A subprocess per oracle call would add about two to
four minutes, so the run would take two to three times as long.

| File | Function | References |
|---|---|---|
| `tests/helpers/ast_extract.py` | `compile_class_method_from_module` | 1 |
| `tests/test_guard_credential_key_folding.py` | `_python_writes` | 1 |
| `tests/test_guard_credential_key_folding.py` | `test_python_is_the_oracle_for_class_bases` | 2 |
| `tests/test_guard_credential_key_folding.py` | `_oracle_runs_to_the_member` | 2 |
| `tests/test_guard_credential_key_folding.py` | `_keys_held_after` | 1 |
| `tests/test_guard_credential_key_folding.py` | `test_the_statement_loop_limit_writes_the_member_in_python` | 1 |
| `tests/test_guard_credential_key_folding.py` | `test_a_target_over_a_builtin_call_stays_unknown` | 1 |

The guard counts references, not calls, so neither an alias of the function nor
one of its module can hide a later call. A reference is the bare name (`exec(…)`,
`run = exec`), any attribute named `eval` or `exec` whatever object carries it
(`builtins.exec`, `b.exec` after `import builtins as b`,
`__import__("builtins").exec`, `sys.modules["builtins"].exec`), each keyword of a
class pattern (`case object(exec=run)` reads the attribute), and each of the two
names imported from `builtins`. A name that only binds or declares (`def exec`, a
parameter, `except E as exec`, `case exec`, `import os as exec`) reads nothing and
does not count; the guard pins this split for every name field of the grammar,
so a new field fails it. It does not see a lookup by string such as
`getattr(builtins, name)`, `builtins.__dict__["exec"]` or `vars(builtins)["exec"]`.

One call is made without a reference and therefore stands outside the table:
the battery in `_returns_an_iterable` (`tests/test_guard_credential_key_folding.py`)
calls each builtin without a form through `getattr`, with constant stub arguments.
`exec` is one of them (nine argument sets per run); `eval` has a form and is not.
Frozen forms whose own text calls `eval` run through a listed site and add none.

## AST extraction helper

The :mod:`tests.helpers.ast_extract` module exposes
``compile_class_method_from_module`` for compiling individual methods from
integration modules without importing Home Assistant. Import the helper with
``from tests.helpers import compile_class_method_from_module`` and provide the
module path, class name, and method name to retrieve a standalone function that
can be bound using :class:`types.MethodType`.

## Shell fragment extraction

A test that pulls a block out of `docker-login/entrypoint.sh` does not merely
read it, it EXECUTES it, so a mis-grab is a side effect rather than a wrong
answer. Ending such an extraction on "the next line that is exactly `fi`" is not
an anchor: deleting the guard, or appending a comment to its `fi`, lets the
search run on. Measured twice in one change -- 63 lines including
`sudo chown -R ... /data`, and 74 including the backgrounded login CLI. The
second happened while fixing the first, which is why this belongs here and not
only in a comment. Extract through `_extract_shell_block` in
`tests/test_docker_login_hardening.py`, which ends on `if`/`fi` balance and
additionally refuses to return a block that fails `must_contain` or exceeds
`max_lines`; a mis-grab must raise, never run. Extent today, stated so it is not
mistaken for coverage: two callers go through it, `_extract_display_wait` and
`_extract_layout_call`. `_extract_layout_feature` is only half braked -- its
function half comes from the older `_extract_shell_function`, which anchors on
the first column-0 `}` and has neither `must_contain` nor `max_lines`. Most
fragments these tests execute therefore still contain an unbraked part; the
brakes cover the guard blocks, not everything that runs.

The same rule applies to GitHub workflow steps, which
`tests/test_release_propose_step.py` executes as well. There the anchor is
structural rather than textual: the step is selected with `yaml.safe_load` by
its exact `name`, and the test refuses to run unless exactly one step matches,
the `run` block contains no `${{` expression (the runner would substitute it, so
a verbatim run would test something else), the step's `env` keys equal the keys
of `_STEP_ENV`, from which the stub environment is built, and neither the step
nor `defaults.run` of the job or the workflow sets a `shell`. The block then
runs as `bash -e`, the runner's start form for `run` steps without an explicit
`shell`, with an environment built from scratch rather than from `os.environ`.
Stubs on `PATH` log their calls to files, not to stdout: inside `$(...)` a
stdout marker lands in the captured variable. A missing `bash` fails the test
instead of skipping it.

`tests/test_release_stamp_push_step.py` applies the same rule to the push step
of `release-stamp.yml`, with two differences. That step reads its inputs from
the job-level `env`, so the test pins the job's `env` keys (`_JOB_ENV_KEYS`) and
refuses a step-level `env`. And it runs the real `git` against a local bare
remote instead of a stub, so its environment isolates git explicitly: `HOME`
points into `tmp_path`, `GIT_CONFIG_NOSYSTEM=1`, `GIT_CONFIG_GLOBAL` names an
empty file, and the commit identity comes from `GIT_AUTHOR_*` and
`GIT_COMMITTER_*`. A new test that drives a workflow step with real git should
build its environment the same way.

## Device registry expectations

The coordinator device-registry tests retain Home Assistant's 2025.10
`via_device` tuple support for completeness even though tracker devices no
longer link to the service device. When extending the `_FakeDeviceRegistry`
implementation or adding new tests in `tests/test_coordinator_device_registry.py`,
ensure the helper continues to accept both the legacy `via_device_id` keyword
and the newer tuple form so regression tests can assert that the integration
leaves both fields unset for tracker entries.

For shared Home Assistant registry stubs referenced across multiple modules,
start with the `_StubDeviceRegistry` class in [`tests/conftest.py`](tests/conftest.py)
(search for `class _StubDeviceRegistry`; do not cite a line range here, it rots on
the next edit). The helper **must** model the **single-owner** device registry of
Home Assistant Core 2026.8 and newer: a device belongs to exactly one config entry
and exactly one config subentry, exposed as `config_entry_id` and
`config_subentry_id`. Check the class before you rely on that: at the time this
paragraph was written the helper in the tree still kept `config_entries` as a set
and `config_entries_subentries` as a dict of sets, and it knew none of the
replacement keywords. Converting it is a separate, pending change; until it lands,
read the rules below as the target and the class as the exception.
It records each payload so coordinator- and service-level tests observe the same
ownership behavior. Once converted it is to accept `new_config_entry_id` and
`new_config_subentry_id` on a core that carries the replacement API and apply them as
an immediate move, and to accept the legacy quadruple on a core at the declared
minimum (`2025.9.1`). Which of the two a test sees is a property of the capability
profile under test, so assert the resulting ownership, never the keyword.

Do **not** reintroduce a double that models the pre-2026.8 many-to-many mapping.
Such a double is what hid this migration: it accepted `add_config_entry_id`,
`remove_config_entry_id`, `add_config_subentry_id` and `remove_config_subentry_id`
and answered them as set operations, so a suite that was in fact calling a
superseded API stayed green. A double whose semantics differ from the supported
core does not test the integration, it tests the double. Background:
`docs/AI_DEPRECATIONS_GUIDE.md`, section VI.

### Deprecation recording

Tests that touch a device- or entity-registry write path are to use the
`device_registry_deprecations` fixture. It records every `report_usage` call raised
during the test instead of swallowing it, and a new recorded deprecation is a
**failure**, not noise. The fixture lives in `tests/conftest.py` (search for
`def device_registry_deprecations`) and is `autouse`, so every test already runs
under it. Two reasons this fixture exists rather than a log filter:
the stub in `tests/conftest.py` currently installs a no-op `report_usage` (search
for `def report_usage`), which makes this whole class of findings invisible, and the
fixture is what replaces it; and the interesting deprecations here carry
`core_behavior=ReportBehavior.ERROR`, so whether they raise or merely log depends
on whether a `custom_components/` frame is on the stack, which is a property of the
test rather than of the production code.

When expanding purge or cleanup coverage, mirror Home Assistant's registry
helper API surface (including `async_entries_for_config_entry` and
`async_remove`) instead of inventing bespoke stubs so new tests continue to use
the shared registry doubles.

Reuse `tests.helpers.service_device_stub` whenever a test needs a
`SimpleNamespace`-based service-device object with
`config_entries_subentries` metadata. The shared factory keeps identifier
defaults aligned across modules and avoids drift between coordinator and
registry-rebuild coverage as new scenarios are added.

### DeviceInfo assertion style

When asserting `DeviceInfo` contents, prefer attribute access (for example,
`info.config_entry_id`) over subscript-style lookups. Home Assistant models
`DeviceInfo` as a dataclass, so attribute reads reflect the production API and
avoid accidental reliance on the helper's mapping-like fallbacks.

### Update checklist for registry stub changes

When adjusting the registry stubs or adding new assertions, confirm the
following to preserve migration coverage:

1. **Identifier continuity** — Existing entries still match on the expected
   identifier tuples after the change.
2. **Config subentry alignment** — Legacy devices without a `config_subentry_id`
   are updated to reference the tracker subentry when migrations run.
3. **`device_id` verification** — Recorded updates are associated with the
   precise device entry under test so assertions catch accidental cross-device
   mutations.
4. **Dual service-device identifiers** — Tests exercising the service device
   must assert both the integration-level identifier and the derived
   service-subentry identifier (see `_service_subentry_identifier(...)`) so
   future expectations stay aligned with the coordinator's registry updates.
5. **Playbook parity** — Whenever a regression test targets creation, reload,
   or cleanup logic, stay consistent with Section VI
   ("Troubleshooting `ValueError` & Regressions") of
   `docs/CONFIG_SUBENTRIES_HANDBOOK.md`. Do not expect a large playbook there: it
   is two symptom/cause/fix entries, on repeated `async_forward_entry_setups` and
   on subentry devices that fail to delete. For device *ownership* questions the
   reference is `docs/AI_DEPRECATIONS_GUIDE.md`, section VI, not the handbook. Assert that registry helpers capture
   the `(entry_id, device_id, identifiers)` tuple, and assert the observed
   `OwnershipIntent` together with the operation sequence the planner derived
   from it. Do **not** assert that `add_config_entry_id` (or `config_entry_id`)
   is forwarded: that assertion pins a superseded API and would keep the old
   call alive. The keywords are an implementation detail of
   `plan_device_ownership`, chosen from the signature of the installed core; the
   intent is the contract. One exception, and only one: a test of an **executor**
   (`execute_ownership_plan`, `_call_device_registry_api`) may assert the
   keywords, because an executor picks none of its own, forwards what the planner
   already chose, and owns no resulting ownership that could be asserted instead.
   A test of a **call site** may not. See `docs/AI_DEPRECATIONS_GUIDE.md`,
   section VI.

## Translation alignment checks

`tests/test_service_device_translation_alignment.py` loads every locale (including
`strings.json`) and asserts each defines the translation key referenced by
`custom_components.googlefindmy.const.SERVICE_DEVICE_TRANSLATION_KEY`. Update this
test whenever new locales or service-device translation keys are introduced so it
continues to guard localized device names.

## Docstring semantics guards

`tests/test_uwt_mode_binary_sensor.py::TestUWTModeSensorDocumentationMatchesSpec`
introduces a guard family that pins *documentation* rather than behaviour: it
reads docstrings via `inspect.getdoc` and asserts that a withdrawn claim cannot
return, that the corrected statement stays present, and that the specification
reference stays discoverable. It exists because the UWT-mode sensor documented a
separation window as its own trigger, which produced misdirected automations and
bug reports (BSkando#210) without a single failing test.

**Pin the statement, not a token (correction of 2026-09-03).** The first version
of this family forbade the figure "8-24 hours" as such. That figure turned out to
be normative DULT (`T_(SEPARATED_UT_TIMEOUT)`), only attached to the wrong thing,
so the guard was pinning the *replacement* error in place: the docstring had gone
on to deny that the FMDN mode is the DULT separated state, which the Find Hub
specification asserts in so many words. A forbidden-token guard cannot tell a
wrong number from a misattributed one. The family now forbids the withdrawn
*statements* (normalised for case and line wrapping, each with its subject in the
pattern so a true sentence using the same words still passes) and requires the
corrected attribution in the same paragraph as the figure. Read that as the rule
for new members: name what must not be said and what must be said instead, and
never make a guard out of a term that a correct sentence would also need.

**Service-activation model (correction of 2026-10-11).** The 2026-09-03 text
described the mode as entered autonomously after 30 minutes of separation, with
Data ID `0x07` / `0x08` as an additional path. The Find Hub specification says
more about the mode itself: "The Find Hub service can relay an unwanted tracking
protection mode activation request through the Find Hub network". Turning the
mode on and off both use the unwanted tracking protection key; only turning it
off additionally requires that the hashed ephemeral identity key matches the
current one, so in practice the owner's phone turns it off (an inference, not a specification
statement). DULT's 30 minutes bound the separation criterion; Google's server
criteria are not published. Two patterns of the negative guard described this
model as an error and were retired (`command[-\s]driven`, `is entered and left
by command`); exclusivity (`only by command`) and `not by elapsed time` stay
forbidden. A new positive guard requires `0x07`, `0x08`, `relay` and `not
published` in the class docstring. The retirement left the withdrawn
autonomous transition without a pattern, so on 2026-10-12 the negative guard
gained five patterns, each scoped to a UWT subject whose list gained `that
state`, `this state`, `0x07` and `0x08`: `autonomously, not by
command`; `autonomously`, `on its own` or `by itself` next to a
separation time; `entered after 30 minutes`; `additional ... path`; and `no
command sent by anyone`. The words alone are not forbidden, because correct
sentences need them (the specification has the Seeker write `0x07`). The
control test gained one sentence per new pattern, each caught by that pattern
alone, and a second control lists correct sentences that must stay unmatched. Nothing above this
paragraph was deleted.

Use this pattern in one of exactly two situations, and keep four properties:

- **(a) Corrective.** A wrong docstring has already caused a real defect. This is
  the original case above.
- **(b) Load-bearing cross-reference.** The docstring carries the only in-repo
  statement that links an entity to an open upstream report, so deleting the prose
  silently deletes the link. `test_docstring_keeps_the_skip_ringing_authentication_caveat`
  is the first member of this kind: it pins the "Skip ringing authentication"
  control flag, which is what connects the UWT-mode sensor to the spurious-ring
  reports. Case (b) is narrower than it looks and is not a licence to freeze prose
  in general: without property 4 below it degenerates into exactly that.

1. **Match patterns, not one spelling.** The negative guard uses a regular
   expression covering hyphen, en-dash and spelled-out variants. A plain
   substring check is trivially evaded by rewording, which makes the guard read
   as protection while providing none.
2. **Do not paraphrase the banned claim in the guarded docstring.** If the
   prose needs to mention what was wrong, it must do so without restating the
   figure, otherwise the guard fires on its own debunking.
3. **State the blind spot in the test class docstring.** These are pattern and
   presence checks; they cannot distinguish good prose from a bag of the right
   keywords. Like the CLI kill guard, this is a ratchet, not a proof.
4. **Name the retirement condition.** Every guard states in its own docstring
   what would make it obsolete: for case (a) that the corrected claim has been
   stable across a release, for case (b) that the named upstream report has
   closed. A guard with no stated exit turns documentation into a permanent
   fixture and outlives the reason it was written.

## Prose contract guard (`tests/test_guard_prose_contract.py`)

`AGENTS.md` carries two rules about the prose a contributor writes: keep comments and
docstrings English (the "Language reminder" and "Language policy" paragraphs), and cite
only evidence a reader of this repository can open (section 2, "Mandatory evidence").
Both were plain text with no mechanism behind them, so a violation passed `ruff`,
`mypy`, `codespell` and the whole suite and only surfaced in review. That is the same gap `test_guard_path_header.py` was written
for, and the guard follows its shape.

Two narrowings, both measured, both load-bearing.

First, the guard reads **prose units**, not raw file text: module, class and function
docstrings via `ast.get_docstring`, comments via `tokenize`. String literals are out of
scope on purpose, because foreign language is legitimate data here
(`tests/test_translation_placeholders.py` asserts against the German address form,
`translations/*.json` is multilingual by contract). Measured over the 503 Python sources
of the sweep set (`.py` and `.pyi`), counting the guard file itself out because its own
comments name the words: the prose arm flags 12 of 29992 prose units, while the same word list over raw
file text additionally hits 11 occurrences in three further files, every one of them
translation data in a string literal.

Second, the German word list subtracts measured English homographs, each carrying its
hit count in the source. Counting unit throughout: a prose unit is one docstring or one
comment token, and a hit is a unit containing the word. Measured at the tip of the
branch, guard file excluded: `also` 231, `falls` 127, `probe` 121, `dies` 23, `der` 12
(the DER encoding in the credential tests), `mit` 5 (the MIT licence header), `wichtig` 1
(the `WICHTIG-2` ticket label). Six further words sit at zero today and are kept out
pre-emptively because each collides with an acronym this domain really uses: `des` (DES),
`ist` (IST), `dem` (DEM), `aus` (AUS), `als` (ambient light sensor) and `vor` (VHF
omnidirectional range). The subtraction happens in `_german_markers_in`, not merely in the comment: an
earlier draft documented the exclusion without performing it. Without those numbers the list gets tidied up later and the
guard starts crying wolf, which gets it switched off rather than obeyed. The cost is
stated rather than hidden: German prose built only from words outside the list passes.
The wider variants were tried and rejected, an umlaut character class flags the proper
name in every copyright header, and a two-distinct-words threshold only passed its
positive control because three nouns of one concrete incident had been added to the list.

The evidence arm covers Python prose plus Markdown and workflow files and matches paths
under an agent's private memory or home configuration. The private directories are a
named set (`_AGENT_PRIVATE_DIRS`, currently `.claude` and `.codex`) and this one is an
enumeration by necessity: no property of a directory name marks it as an assistant's
private tree, and matching any dot-directory under a home would swallow legitimate
documentation of a user's own configuration. Add a member when an assistant keeps its
notes, plans or instructions there; each member carries its own positive case, and a
member the pattern never reaches fails a test.

The directory ITSELF is a citation, not only a file under it. A home prefix followed by
one of those directories, with or without a trailing separator, names something no reader
can open, and requiring a non-empty tail let every such spelling through in silence. The
tail was doing two jobs at once, boundary and content, so the boundary is now explicit
(`_DIR_BOUNDARY`). It has to be, because the widening must not reach a bare mention of a
directory name in running prose - this contract and the guard's own comments are full of
those. Hence two shapes and no third: behind a home or path prefix the directory counts
on its own, and without a prefix a separator is still required. The boundary also
excludes the opening angle bracket, so the placeholder spelling this repository writes
for a file name stays quiet, and the percent sign, because an escape immediately after
the directory means the path continues in encoded form and the decoded pass is the one
that should read it.

Prose is normalised before the pattern sees it, because two spellings would otherwise be
judged by what they look like rather than by what they are. A public URL is openable by
any reader whatever its path component contains, so a link into someone else's
repository is a source, not a private citation; the scheme is what makes it public, so
this is a property and not a list of sites. A `file:` URI is the opposite case, a local
path in a different spelling, and its extra slashes hid it from the pattern entirely.
Both are replaced by spaces of the same length, so a citation next to a URL on the same
line survives. The URL body stops at the delimiters that close a link in prose, not
merely at whitespace: two Markdown links written back to back have no space between them,
and a run to the next space swallowed the second one whole, taking a private citation
with it. In exchange, a URL carrying a closing parenthesis is cut short there and its
tail can read as a citation; that direction fails loudly rather than silently, which is
the direction this guard should fail in.

Two properties make a URL openable, and both are checked. Its scheme is read by its
grammar (RFC 3986 3.1: a letter followed by letters, digits, plus, minus or dot) rather
than by a list of names: a list of four was written once, and each review round after it
found a spelling the list did not carry, first an uppercase scheme, then `git://`. But
the grammar alone is not enough, and getting that wrong cost a round of its own. Reading
*any* scheme as public blanked an editor URI naming a local file, and the citation
vanished in silence. The authority must therefore be a network host as well: a bracketed
IPv6 literal, a dotted name, or `localhost`. An unregistered scheme over a real host is
public; any scheme over something that is not a host is not.

A URI that fails the host test is not simply left alone either. Only its scheme and
authority are blanked, so the local path behind them is judged as what it is. Without
that step the path pattern cannot even see such a path: its own left boundary refuses to
start a match directly after a word character, and the last letter of the authority sits
exactly there.

The bracketed alternative in the host is not decoration. `]` is one of the delimiters
that close a link in prose, so without it an IPv6 literal cut the URL after its scheme
and the path of an openable source was reported as a private citation. The local schemes are a named set, `_LOCAL_URI_SCHEMES`, and
they are normalised first for exactly that reason: a rule that accepts any scheme would
otherwise swallow the one spelling that means a local path. The `file:` prefix consumes
an optional authority component, so `file:/path`, `file:///path` and
`file://localhost/path` are read alike; accepting only the middle one left the other two
with their scheme intact and reported nothing at all.

Percent escapes are decoded before the pattern judges a path. `%2Ecodex` is `.codex` to
every reader, and reading it literally let an unopenable citation through in silence.
The unit is matched twice, once as written and once decoded, and the second result is
appended to the first rather than replacing it, so a path the raw text already showed
cannot be lost and a path written twice is still reported twice. Decoding in place is
deliberately not done: it would shorten the text and move every later citation, which is
the property the space-padded blanking exists to protect.

Every shape the detector has been wrong about lives in `_DETECTOR_CORPUS` with its
expected result. Four review rounds went the same way, a narrowing leaving a gap and the
widening that closed it producing a false positive elsewhere, so a change to the pattern
is diffed against that table rather than against the cases its author happened to think
of. The home prefix is generic over
both path separators and does not have to be absolute, because a relative reference is
just as unopenable for a reader; the directory has to start a path segment, so a name
that merely ends in those letters does not trip. `/app/` is deliberately not part
of the pattern: both measured occurrences were `/app/requirements.txt`, this project's
own container path. Measured over 553 files (503 Python sources, 50
Markdown and workflow): zero hits, so `AGENT_LOCAL_PATH_ALLOWLIST` is empty and should
stay so.

The repository sweep is restricted to tracked files. Walking the working copy instead
made the result depend on whatever a developer happened to have lying around:
`.gitignore` reserves `.plans/`, `.bootstrap/` and `.wheelhouse/`, and an agent-local
citation in one of those would fail this suite for its author alone, over a file that
can never enter a commit. Pruning those three names would be the same enumeration this
guard has already lost several rounds to; asking `git` is not. The synthetic polarity
probes pass no restriction, because a temporary directory has no index to ask.

Excluded by file, not by directory: `ProtoDecoders`, `Auth/firebase_messaging/proto` and
`vendor` all hold tracked, hand-written Python next to generated or imported code, and
pruning those directories hid one such file per review round. The sweep drops `*_pb2.py`
and `*_pb2.pyi` by suffix instead, and `test_every_tracked_python_source_is_swept`
holds the sweep set against `git ls-files` so the next hidden file fails a test rather
than waiting for a reviewer. `test_every_tracked_text_source_is_swept` makes the same
claim for the Markdown and workflow half: asserting coverage for the Python half alone
would have left the original shape of the finding intact for prose files.

The language arm, by contrast, stops at Python. `AGENTS.md` itself quotes the German
translation guideline and would need an allowlist entry from day one, and an allowlist
that starts populated is the shape that erodes. The gap is stated rather than papered
over.

Polarity is proved in both directions and at two levels. At detector level a synthetic
docstring must trip and a clean one must not; at sweep level the same texts are written
under `tmp_path` so that extraction, iteration and the exclusion filter are exercised,
not just the regex. Every offending sample lives in a string literal or under `tmp_path`,
never planted in the working tree. One probe is explicitly anti-overfitting: the incident
phrase must still trip once the three incident nouns are stripped from the word list.

`test_every_swept_file_parses` is the interpreter guard. This tree uses PEP 695 syntax,
which needs Python 3.12 or newer; under 3.11 seven modules fail to parse and the language
arm would go silently blind on them. An unparsable file is reported by name instead of
counting as clean.

`LEGACY_ALLOWLIST` holds one entry, `tests/test_translation_placeholders.py`, because
that guard has to name the formal pronouns it rejects and quote the upstream translation
guideline verbatim. Do not add entries; translate the prose instead. The stale test turns
red once an entry stops violating.

One thing to know when editing the contract itself: writing the forbidden path shape out
literally in `AGENTS.md` would make this guard red, since the evidence arm reads
Markdown. The rule text therefore uses a placeholder, and both placeholder forms are
pinned by `test_documented_placeholder_form_does_not_trip`. The same reflex applies to
the guard's own docstring, which cannot spell out the German half of a typo pair for
the same reason.
