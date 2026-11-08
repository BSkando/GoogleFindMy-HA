<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [AGENTS.md — Operating Contract for `googlefindmy` (Home Assistant custom integration)](#agentsmd--operating-contract-for-googlefindmy-home-assistant-custom-integration)
  - [Start here — platinum essentials (front page)](#start-here--platinum-essentials-front-page)
      - [Quick reminder — path headers vs. future imports](#quick-reminder--path-headers-vs-future-imports)
  - [Linting reminders](#linting-reminders)
  - [Scoped guidance index](#scoped-guidance-index)
    - [Home Assistant helper signature changelog](#home-assistant-helper-signature-changelog)
    - [Documentation quick links](#documentation-quick-links)
      - [Deprecations & migrations](#deprecations--migrations)
    - [Runtime vs. type-checking import quick reference](#runtime-vs-type-checking-import-quick-reference)
  - [Environment verification](#environment-verification)
    - [Quick-reference (per-task checklist)](#quick-reference-per-task-checklist)
    - [Connectivity probe](#connectivity-probe)
    - [Quickstart checks (fast path before `pytest -q`)](#quickstart-checks-fast-path-before-pytest--q)
    - [Cleanup shortcuts](#cleanup-shortcuts)
    - [Module invocation primer](#module-invocation-primer)
    - [Shared pip cache for stub installs](#shared-pip-cache-for-stub-installs)
  - [1) What must be in **every** PR (lean checklist)](#1-what-must-be-in-every-pr-lean-checklist)
  - [Home Assistant version & dependencies](#home-assistant-version--dependencies)
    - [Poetry lock file management](#poetry-lock-file-management)
  - [Maintenance mode](#maintenance-mode)
    - [Config subentry maintenance helper](#config-subentry-maintenance-helper)
  - [Key material and resolver hypotheses](#key-material-and-resolver-hypotheses)
    - [Where key material comes from](#where-key-material-comes-from)
    - [Wrapped EIK hypothesis](#wrapped-eik-hypothesis)
    - [Timebase hypotheses](#timebase-hypotheses)
    - [Owner-key error taxonomy (classify only from positive evidence)](#owner-key-error-taxonomy-classify-only-from-positive-evidence)
  - [2) Roles (right-sized)](#2-roles-right-sized)
    - [2.1 Contributor (implementation) — **accountable for features/fixes/refactors**](#21-contributor-implementation--accountable-for-featuresfixesrefactors)
    - [2.2 Reviewer (maintainer/agent) — **accountable for correctness**](#22-reviewer-maintaineragent--accountable-for-correctness)
    - [2.3 History analysis best practices](#23-history-analysis-best-practices)
  - [3) Test policy — explicit AI actions](#3-test-policy--explicit-ai-actions)
    - [3.1 Automatic **Test Corrections** (MUST)](#31-automatic-test-corrections-must)
    - [3.2 Automatic **Regression Tests** for fixes (MUST)](#32-automatic-regression-tests-for-fixes-must)
    - [3.3 Opportunistic **Test Optimization** (SHOULD SUGGEST)](#33-opportunistic-test-optimization-should-suggest)
    - [3.4 Definition of Done for tests](#34-definition-of-done-for-tests)
    - [3.5 Temporary coverage dips](#35-temporary-coverage-dips)
    - [3.6 Entity-registry migration checklist (reload-centric fixes)](#36-entity-registry-migration-checklist-reload-centric-fixes)
    - [3.7 Device registry healing checklist](#37-device-registry-healing-checklist)
  - [4) **Token cache handling** (hard requirement; regression-prevention)](#4-token-cache-handling-hard-requirement-regression-prevention)
  - [5) Security & privacy guards](#5-security--privacy-guards)
  - [6) Expected **test types & coverage focus**](#6-expected-test-types--coverage-focus)
  - [7) Quality scale (practical, non-blocking)](#7-quality-scale-practical-non-blocking)
  - [8) Deprecation check (concise, per PR)](#8-deprecation-check-concise-per-pr)
    - [Deprecation shim checklist (Home Assistant)](#deprecation-shim-checklist-home-assistant)
    - [Integration-owned deprecations (register)](#integration-owned-deprecations-register)
  - [9) Docs & i18n (minimal but strict)](#9-docs--i18n-minimal-but-strict)
  - [10) Local commands (VERIFY)](#10-local-commands-verify)
    - [10.1 Type-checking policy — mypy strict on edited Python files](#101-type-checking-policy--mypy-strict-on-edited-python-files)
  - [11) Clean & Secure Coding Standard (Python 3.13 + Home Assistant 2025.10)](#11-clean--secure-coding-standard-python-313--home-assistant-202510)
    - [11.1 Language & style (self-documenting)](#111-language--style-self-documenting)
      - [Logging formatting (ruff G004)](#logging-formatting-ruff-g004)
    - [11.2 Security baseline (OWASP / NIST / BSI)](#112-security-baseline-owasp--nist--bsi)
    - [11.3 Async, concurrency & cancellation](#113-async-concurrency--cancellation)
    - [11.4 File system & I/O (safe & gentle)](#114-file-system--io-safe--gentle)
    - [11.5 Guard catalog & error messages](#115-guard-catalog--error-messages)
    - [11.6 Performance without feature loss](#116-performance-without-feature-loss)
    - [11.7 Home Assistant specifics (must-haves)](#117-home-assistant-specifics-must-haves)
    - [11.8 Release & operations](#118-release--operations)
    - [11.9 Machine-checkable acceptance checklist (for the agent)](#119-machine-checkable-acceptance-checklist-for-the-agent)
  - [REFERENCES](#references)
    - [1) Python 3.13 — Language, Style, Typing, Safety](#1-python-313--language-style-typing-safety)
    - [2) Home Assistant (Developer Docs, 2024–2025)](#2-home-assistant-developer-docs-20242025)
    - [3) Secure Development — Standards & Guidance](#3-secure-development--standards--guidance)
      - [OWASP Cheat Sheet Series (selected)](#owasp-cheat-sheet-series-selected)
    - [4) Software Supply Chain & Reproducibility](#4-software-supply-chain--reproducibility)
    - [5) Repo & Documentation Hygiene (GitHub/Markdown)](#5-repo--documentation-hygiene-githubmarkdown)
    - [6) In-repo Find My Device Network protocol reference](#6-in-repo-find-my-device-network-protocol-reference)
  - [High-confidence & sourcing standard for Codex](#high-confidence--sourcing-standard-for-codex)
    - [0. Scope](#0-scope)
    - [1. High-confidence mode (≥ 90 %)](#1-high-confidence-mode--90-)
    - [2. Mandatory evidence](#2-mandatory-evidence)
    - [3. Workflow for code changes](#3-workflow-for-code-changes)
      - [Home Assistant regression helper](#home-assistant-regression-helper)
      - [Config flow registration fallbacks](#config-flow-registration-fallbacks)
    - [4. User as data source](#4-user-as-data-source)
    - [5. Communicate uncertainty](#5-communicate-uncertainty)
    - [6. Consequences for commits, diffs, and reviews](#6-consequences-for-commits-diffs-and-reviews)
    - [7. Review obligations](#7-review-obligations)
    - [8. Required workflow summary](#8-required-workflow-summary)
    - [9. Rationale](#9-rationale)
    - [10. Post-task feedback obligations](#10-post-task-feedback-obligations)
  - [X. PROTOCOL: Runtime API Validation (Single Source of Truth - SSoT)](#x-protocol-runtime-api-validation-single-source-of-truth---ssot)
  - [pip-audit workflow guidance (CORRECTION — April 2025)](#pip-audit-workflow-guidance-correction--april-2025)
    - [7) Type checking (mypy)](#7-type-checking-mypy)
      - [Auto-installed stub packages](#auto-installed-stub-packages)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# AGENTS.md — Operating Contract for `googlefindmy` (Home Assistant custom integration)

* [Scoped guidance index](#scoped-guidance-index)
* [Environment verification](#environment-verification)
* [What must be in every PR](#1-what-must-be-in-every-pr-lean-checklist)
* [Mandatory evidence](#2-mandatory-evidence)
* [Workflow for code changes](#3-workflow-for-code-changes)
* [Communicate uncertainty](#5-communicate-uncertainty)
* [Type checking (mypy)](#7-type-checking-mypy)

> **Language reminder:** Keep all inline comments, docstrings, and documentation updates in English. When user-provided snippets
> include other languages, translate or adapt them so the committed code remains English-only.
> Enforced by `tests/test_guard_prose_contract.py`, which sweeps docstrings and comments of Python
> files (not string literals, so translation payloads and assertion data stay exempt) for German
> function words. Two limits, stated rather than implied: the detection keys on a measured word list
> from which the measured English homographs and domain acronyms are subtracted, so it catches ordinary
> German prose and not every
> fragment; and Markdown is out of its scope, because this file itself quotes a German translation
> guideline.

> **Environment reset reminder:** After a container or virtualenv reset, rerun `make install-dev` (or `poetry install --with dev,test`)
> so Home Assistant and the pytest plugin are reinstalled before invoking linting or pytest commands.

## Start here — platinum essentials (front page)

* **Quality scale in the manifest.** New or updated integrations must declare `"quality_scale": "platinum"` (with realistic
  `iot_class` values) and stick to async-friendly requirements; see [Quality scale (practical, non-blocking)](#7-quality-scale-
  practical-non-blocking) for the full checklist.
* **Runtime data placement.** Keep config entry state on `entry.runtime_data` (typed) instead of ad-hoc module globals or
  untyped `hass.data` buckets; the guardrails live under [Home Assistant specifics (must-haves)](#117-home-assistant-specifics-
  must-haves) and [Config subentry maintenance helper](#config-subentry-maintenance-helper).
* **Required local checks.** Before committing, run `python -m ruff check --fix`, `python -m mypy --strict --install-types --
  non-interactive`, and `python -m pytest --cov -q` (after `make install-dev` in new environments); see [Local commands
  (VERIFY)](#10-local-commands-verify) for context.

<a id="language-policy"></a>
> **Scope & authority**
>
> **Directory scope:** applies to the entire repository (with continued emphasis on `custom_components/googlefindmy/**` and tests under `tests/**`).
> **File headers:** Every Python file within scope must include a comment containing its repository-relative path (e.g., `# tests/test_example.py`). When a file has a shebang (`#!`), the shebang stays on the first line and the path comment immediately follows it. Some legacy files may still lack this header; reviewers should expect churn as contributors retrofit missing comments during unrelated fixes rather than blocking urgent work solely to add them.

#### Quick reminder — path headers vs. future imports

Always keep any `from __future__` imports immediately after the module docstring, even when the file starts with the repository-relative path header described above. This ordering prevents pytest's import hook from rejecting the file during rewrites.
> **Precedence:** (1) Official **Home Assistant Developer Docs** → (2) this AGENTS.md → (3) repository conventions. This file never overrides security/legal policies.
> **Language policy:** Keep the project consistently in English for documentation, inline code comments, docstrings, and configuration names such as workflow environments. (Translation files remain multilingual.)
> Guard: `tests/test_guard_prose_contract.py` (see the Language reminder above for its scope and limits); workflow environment names: `tests/test_guard_workflow_environments.py`.
> **Non-blocking:** Missing optional artifacts (README sections, `quality_scale.yaml`, CODEOWNERS, CI files) **must not block** urgent fixes. The agent proposes a minimal stub or follow-up task instead.
> **References:** This contract relies on the sources listed below; for a curated, extended list of links, see [BOOKMARKS.md](custom_components/googlefindmy/BOOKMARKS.md).
> **Upstream documentation hierarchy:** When consulting external guidance, prioritize Home Assistant's canonical domains in this order: developer portal (`https://developers.home-assistant.io`), user documentation (`https://www.home-assistant.io`), and the alerts/service bulletins site (`https://alerts.home-assistant.io`). If a required host is unreachable while the connectivity probe still confirms general internet access, pause implementation, request manual approval for that domain, and document the escalation before proceeding.
## Linting reminders

* After editing coordinator helpers (for example, `GoogleFindMyDataUpdateCoordinator` routines), rerun `ruff check` and `mypy --strict --install-types --non-interactive` before launching the full test suite so linting and typing regressions surface early.
* When moving entities between categories (for example, dropping `entity_category=EntityCategory.DIAGNOSTIC` to surface a control), re-run `ruff check --fix` to prune stale imports before the follow-up `ruff check` confirmation.
* Prefer importing container abstract base classes (for example, `Iterable`, `Mapping`, `Sequence`) from `collections.abc` instead of duplicating the names from `typing`. This mirrors the integration guidance under `custom_components/googlefindmy/AGENTS.md` (and the topical guides it references under `custom_components/googlefindmy/agents/`) and helps avoid ruff F811 redefinition warnings.
* Pytest warning filter: `pyproject.toml` configures `filterwarnings = ["ignore:Inheritance class HomeAssistantApplication from web.Application is discouraged:DeprecationWarning:homeassistant.components.http"]` to silence an upstream aiohttp deprecation emitted by the current Home Assistant release. Remove the filter only after Home Assistant stops subclassing `aiohttp.web.Application` (or drops the warning entirely) and a full `pytest` run verifies the warning no longer appears without the suppression.

## Scoped guidance index

> **Integration note:** Runtime, config-flow, and typing guidance for `custom_components/googlefindmy/**` now lives inside the topical files under `custom_components/googlefindmy/agents/`. The index file at `custom_components/googlefindmy/AGENTS.md` lists every available topic and should be the first stop before editing any integration module.

* [`tests/AGENTS.md`](tests/AGENTS.md) — Home Assistant config flow test stubs, helpers, discovery/update scaffolding details, **and** the package-layout note that requires package-relative imports now that `tests/` ships with an `__init__.py`. Also documents the coordinator device-registry expectations for `via_device` tuple handling so future stub updates remain aligned with Home Assistant 2025.10. The `_ensure_button_dependencies()` helper in `tests/test_button_setup.py` already prepares sufficient stubs to `import custom_components.googlefindmy.button`, so tests can reference coordinator attributes directly without reloading source snippets.
* [`docs/CONFIG_SUBENTRIES_HANDBOOK.md`](docs/CONFIG_SUBENTRIES_HANDBOOK.md) — Full Home Assistant 2025.7+ handbook covering the architecture, config flow factories, lifecycle routing, discovery patterns, translation rules, and peer-review checklist for configuration subentries. Keep code and tests aligned with this contract. When adding concise checklists or reminders beneath an existing subsection, anchor the new block with a `####` heading so the handbook's navigation keeps the guidance grouped with its parent topic. Mirror this heading-level rule in other documentation unless a directory-specific `AGENTS.md` states otherwise.
* [`docs/AI_DEPRECATIONS_GUIDE.md`](docs/AI_DEPRECATIONS_GUIDE.md) — Core 2025.10 through 2026.9 technical migration playbook for deprecations, breaking changes, and behavioral shifts. Treat its critical checklist as mandatory when touching affected APIs.
  * Section VI of [`docs/CONFIG_SUBENTRIES_HANDBOOK.md`](docs/CONFIG_SUBENTRIES_HANDBOOK.md) ("Troubleshooting `ValueError` & Regressions") carries the device/entity registry troubleshooting playbooks. Reference them whenever you touch `_async_setup_subentry`, registry rebuild services, or device cleanup helpers, and summarize the relevant diagnostics in your PR description. For device *ownership* changes read `docs/AI_DEPRECATIONS_GUIDE.md`, section VI, first: from Core 2026.8 a device belongs to a single config entry and a single subentry, and the old keywords changed meaning rather than name.
* **Self-healing helpers:** `_async_self_heal_duplicate_entities()` in `custom_components/googlefindmy/__init__.py` documents the existing duplicate-entity cleanup flow; review it alongside the new `EntityRecoveryManager` when designing additional recovery logic.
* **Architecture orientation (third party):** [GitDiagram](https://gitdiagram.com/bskando/googlefindmy-ha) draws an interactive map of `BSkando/GoogleFindMy-HA`, the upstream repository the HACS badge, `manifest.json` and `pyproject.toml` point at: the config flow, the coordinator, the `Auth` package, the `device_tracker` platform and the `NovaApi`/`SpotApi` clients, and how they connect. Handy before you open `custom_components/googlefindmy/` for the first time. An external service generates it from the upstream tree and serves it from a cache, so a first visit may have to build it, and work that has not landed upstream does not show up in it. Orientation only: the nested `AGENTS.md` files and the documents under `docs/` remain authoritative.

### Home Assistant helper signature changelog

* **`ConfigEntry.async_create_background_task(hass, target, *, name=None, eager_start=True)` (Home Assistant 2025.7+)** — pass
  the `hass` instance explicitly as the first argument, followed by the awaitable/awaitable-returning target. Stubs under
  `tests/` must forward the `target` callable/awaitable to `HomeAssistant.async_create_task` so lifecycle handling mirrors the
  runtime integration. See `custom_components/googlefindmy/__init__.py` for the in-tree usage reference and keep this section
  updated whenever Home Assistant revises helper signatures relied upon by the integration.

### Documentation quick links

* [Docs & i18n requirements (§9)](#9-docs--i18n-minimal-but-strict)
* [Rule §9.DOC (documentation accuracy)](#rule-9doc-canonical)
* [Language policy (English-only docs)](#language-policy)
* [Docstrings & typing expectations (§11.1)](#docstrings--typing)
* **README developer guidance:** Place any new development workflow notes (linting, testing, tooling commands) directly under the `### Continuous integration checks` section in `README.md`. Use a top-level `###` heading for repository-wide topics and `####` subheadings for individual command lists so contributor documentation stays grouped in one location.
* **Tracker device linkage:** Tracker entities rely on Home Assistant's automatic
  parent-device assignment for the `TRACKER_SUBENTRY_KEY`. Do not introduce manual
  `via_device` pointers for tracker `DeviceInfo` payloads; only service-level entities
  may declare `via_device` tuples when modelling nested hardware chains documented
  upstream.

  **Superseded instruction, do not restore.** Up to and including 1.7.15.14 this
  bullet demanded that
  every `async_update_device` call on a tracker device carry `add_config_entry_id`
  (or `config_entry_id` on legacy cores) whenever a tracker subentry identifier was
  present. That instruction is wrong from Home Assistant Core 2026.8 onwards. If you
  are about to add `add_config_entry_id` to a call because it "keeps the device
  associated with its config entry", stop: that is the superseded rule resurfacing.

  **Why it changed.** From Core 2026.8 a device belongs to exactly one config entry
  and exactly one config subentry. `add_config_entry_id` no longer attaches anything.
  It records a transient pending move which is only carried out by a later
  `remove_config_entry_id` issued by the same integration; on its own it is a no-op
  when the device already belongs to the entry. Symmetrically,
  `remove_config_entry_id` on the owning entry *deletes* the device when no pending
  move is armed (`homeassistant/helpers/device_registry.py` at tag `2026.8.0`, line
  2231). Both keywords are deprecated with `breaks_in_ha_version="2027.8.0"`, but the
  behavioural change is the part that bites today, not the warning: on 2026.8 the new
  behaviour ships **without** any log output, because the `report_usage` call for
  these keywords only arrives in Core 2026.9 (tag `2026.9.0`, line 3741). A quiet log
  is therefore not evidence that a call is still correct.

  **Current rule.** Express the intent, never the keywords. Call
  `RegistryOperations._apply_device_ownership(...)` with one of
  `OwnershipIntent.MOVE` (put the device into this subentry),
  `OwnershipIntent.ENSURE` (make sure the device sits where we think it does) or
  `OwnershipIntent.DETACH` (give the device up). The single translation point is
  `plan_device_ownership` in
  `custom_components/googlefindmy/coordinator/helpers/registry.py`. It maps the
  intent onto `new_config_subentry_id` (Core 2026.8.0 and newer, see
  `homeassistant/helpers/device_registry.py` at tag `2026.8.0`, lines 2063-2064),
  onto the `remove_*`/`add_*` quadruple (every core at or above our declared
  minimum: `add_config_subentry_id` is already present at tag `2025.9.1`, line
  1014), or onto `async_remove_device`. Write the intent at the call site, and
  let the planner choose the keywords.

  **Version clamp.** `hacs.json` and `pyproject.toml` declare `2025.9.1` as the
  minimum supported core. Raising it only helps at `2026.8.0`, the single release
  that introduces all four replacement APIs (`new_config_entry_id`,
  `new_config_subentry_id`, `async_get_device_by_identifier`, `async_get_devices`);
  every release up to and including `2026.7.0` has none of them. That raise is not
  permitted before **six months after the 2026.8.0 release date**; check the
  release date on the Home Assistant release notes before proposing it, and do
  not carry a hard-coded date forward. The new keywords do not exist on the
  declared minimum, so the switch is a runtime signature probe
  (`detect_device_registry_capabilities`), not a version string comparison. Do not
  replace it with a version check and do not delete the **legacy ownership branch**
  as "dead code": it is the only branch that runs on the declared minimum. It is not
  the same thing as the *subentry keyword naming* branch in
  `_device_registry_config_subentry_kwarg_name`, whose legacy side serves no
  supported core at all because that rename predates our minimum; see
  `docs/AI_DEPRECATIONS_GUIDE.md`, section VI.

  **Raising the floor: CI consequences.** The test suite runs against the
  declared minimum in exactly one place: the `declared floor` axis of the
  `test_legacy_python` job in `.github/workflows/ci.yml`, with its step `Verify
  the pinned floor still matches hacs.json`. The `test` job installs only what
  `poetry.lock` resolves. The `check-declared-minimum` job in
  `.github/workflows/ha-dependency-check.yml` and
  `tests/test_pip_audit_security.py` read the minimum, but neither runs the suite
  against it. Core `2026.8.0` requires Python `>=3.14.2`, so the raise makes the
  Python 3.13 job look obsolete. Deleting it removes the only run of the suite on
  the minimum, and the only check that notices is the job count in
  `test_every_job_requires_its_needs_explicitly`
  (`tests/test_ci_approval_gate.py`), which does not know which job carried the
  floor. Do not lower that count to make the deletion pass; move the axis
  instead. When the minimum is raised to `2026.8.0`, do all of the following in
  the same PR:

  * Move the `declared floor` axis, with its label unchanged, into a lock-free
    Python 3.14 job. Carry over everything the axis depends on: its matrix key
    `floor: true`, the `hacs.json` check, `Rewrite the stub constraints for this
    axis`, `Install dependencies without the Poetry lock` and `Report and verify
    the resolved Home Assistant and plugin versions`. The `hacs.json` check runs
    only under `if: matrix.floor`, so dropping the key together with the
    `pycares<5` cap would skip that check without failing. Pin
    `homeassistant==2026.8.0` and `pytest-homeassistant-custom-component==0.13.354`,
    the plugin release that requires exactly `2026.8.0`; releases `0.13.349` to
    `0.13.353` require the betas `2026.8.0b0` to `2026.8.0b5` instead. Confirm
    both pins on PyPI before committing.
  * Raise `hacs.json`, `pyproject.toml`, the `homeassistant>=` line in
    `custom_components/googlefindmy/requirements-dev.txt`, the axis pin and the
    plugin pin together. The `hacs.json` check compares only `hacs.json` with the
    axis pin, so it catches neither of the other two. Find every other mention of
    the old minimum with `git grep -n -F 2025.9.1`. Guards that pin it, such as
    `test_the_declared_minimum_is_still_the_one_this_guard_assumes` in
    `tests/test_guard_config_flow_reload_deprecation.py`, fail on purpose:
    re-check the premise their message names instead of only updating the
    version string.
  * Add the snapshot
    `tests/fixtures/pip_audit/ha_package_constraints_2026.8.0.txt` in the same
    PR; `tests/test_pip_audit_security.py` fails without it and names its source.
    That test audits every other `ha_package_constraints_*.txt` as an additional
    supported release, so remove the snapshots below the new minimum or keep
    them on purpose.
  * Drop the `last 3.13 line` axis and the `pycares<5` cap in `Rewrite the stub
    constraints for this axis`. Core `2026.8.0` requires `aiodns==4.0.4`, which
    requires `pycares>=5`, so the cap would make the floor axis unresolvable.
  * Keep the `CI Success` job in step with the job list: its `needs`, the result
    check in `Check all jobs passed` and the row in `Write CI summary`.
    `test_ci_success_includes_the_gate` requires every job in `needs`, but no
    test checks the per-job result checks, so a job without one can fail without
    failing `CI Success`.
  * Before deleting `custom_components/googlefindmy/constraints-test-stubs.txt`,
    list its readers with `git grep -n constraints-test-stubs` and update or
    remove each one.
  * Update every text that describes the Python 3.13 axes. Find them with
    `git grep -n -E 'test_legacy_python|3\.13'`, which also lists mentions of
    the language level that stay; among the axis texts are the CI paragraph in
    `README.md`, the `Pinned stub constraints` bullet, the "Two separate axes"
    note at the start of section 11, the `test_legacy_python` pin paragraph in
    section 11.2, the header comment of `constraints-test-stubs.txt` and the
    matrix comment of the `test` job. The language level (`py313` for ruff and
    mypy) is a separate decision, as that section 11 note states.

  **Before and after.**

  ```python
  # WRONG from Core 2026.8 on. What this call does depends on the device's current
  # ownership, which is exactly why it is unsafe. Device already in that subentry:
  # pure no-op. Device in a different subentry of the same entry: the add only arms
  # a pending move and the remove condition does not match, so nothing is applied.
  # Device sitting on the entry with no subentry: the remove condition matches and
  # the move IS carried out. One call, three outcomes, no warning before Core 2026.9.
  dev_reg.async_update_device(
      device_id=device.id,
      add_config_entry_id=entry.entry_id,
      add_config_subentry_id=tracker_subentry_id,
      remove_config_entry_id=entry.entry_id,
      remove_config_subentry_id=None,
  )

  # RIGHT: state the intent, let the planner pick the keywords per core version
  self._apply_device_ownership(
      dev_reg,
      intent=OwnershipIntent.MOVE,
      device_id=device.id,
      entry_id=entry.entry_id,
      target_subentry_id=tracker_subentry_id,
      device=device,
  )
  ```

  Background and deadlines: `docs/AI_DEPRECATIONS_GUIDE.md`, section VI.
* **Config entry version source of truth:** Use `custom_components/googlefindmy/const.py::CONFIG_ENTRY_VERSION` whenever flows, migrations, or tests need the integration's config-entry version. Do not introduce duplicate version constants in other modules.
* [`docs/AI_MAINTENANCE_TASKLIST.md`](docs/AI_MAINTENANCE_TASKLIST.md) — Continuous QA checklist for the agent covering manifest hygiene, translation discipline, blocking I/O guards, and service consistency.
* [`tests/helpers/README.md`](tests/helpers/README.md) — Quick index of ad-hoc debugging helpers (for example, the stub coordinator builder). Skim before adding new utilities so helper guidance stays centralized.
* [`custom_components/googlefindmy/AGENTS.md`](custom_components/googlefindmy/AGENTS.md) — Top-level index for integration guidance. It links to the per-topic AGENT files under `custom_components/googlefindmy/agents/` (currently `config_flow`, `runtime_patterns`, and `typing_guidance`). Always consult the relevant topical file before editing `custom_components/googlefindmy/**` so you pick up the right runtime vs. typing rules (for example, tuple-based parent unloads versus per-platform subentry forwarding).
* [`custom_components/googlefindmy/ProtoDecoders/AGENTS.md`](custom_components/googlefindmy/ProtoDecoders/AGENTS.md) — Protobuf overlay structure requirements, including the mandate that generated message classes remain nominal subclasses of `google.protobuf.message.Message` so helper utilities typed against the concrete base keep accepting them.
* [`custom_components/googlefindmy/FMDNCrypto/AGENTS.md`](custom_components/googlefindmy/FMDNCrypto/AGENTS.md) — Cryptography helper typing contract that documents concrete `int` expectations for modular arithmetic and coordinate normalization across the decompression helpers.
* **Config entry reload states:** `SERVICE_REBUILD_REGISTRY` in `custom_components/googlefindmy/services.py` reloads every target entry unfiltered inside a `try`/`except` that logs the failure. In the default mode `rebuild` it does so in two halves: `hass.config_entries.async_unload(entry_id)`, then the purge of the entry's entities and devices (`purge_entry_registrations`, because unloading keeps registry entries), then `hass.config_entries.async_setup(entry_id)`. A failed unload purges nothing; a failed purge still sets an active entry up again (it is unloaded already); a disabled entry is purged but not set up again, which is what a reload of a disabled entry does as well. The purge covers what is registered to the entry's subentries. Mode `migrate` and a missing purge helper keep the plain `async_reload(entry_id)`. There is no allowed reload state set to keep in sync: one was added in `7ab87a2e` and removed again in `faafc7e7`, and the file has carried no `ConfigEntryState` reference since. Do not reintroduce one. The core already gates: `async_reload` goes through `ConfigEntries.async_unload`, which raises `OperationNotAllowed` while `entry.state.recoverable` is false, so an entry in `FAILED_UNLOAD` or `MIGRATION_ERROR` is **not** recoverable through this service; the call produces a logged error, not a repair. A local filter would only move that verdict earlier and drift from the enum, which is what the two commits above already demonstrated. This is the same verdict `entry_reload_gate.entry_reload_is_hopeless` reaches for the scheduling paths (see `agents/runtime_patterns/AGENTS.md`), so both sides answer alike: a terminal entry needs a restart or a targeted repair, not another reload. When Home Assistant introduces a state, review it there, in the positive terminal list, not here.
* **Platform subentry quick reference:**
  * Shared runtime data lives under `hass.data[DOMAIN]["entries"][entry_id]`. Always rebuild this mapping before child setup resumes and populate `entry.runtime_data` from the shared bucket instead of direct dictionary lookups.
  * Keep the Home Assistant service registration contract aligned with Section IV.I of `docs/CONFIG_SUBENTRIES_HANDBOOK.md`: register platform entity services inside `async_setup` and catch `ImplementationUnavailableError` during parent setup to raise `ConfigEntryNotReady`.
  * Service-level diagnostics (`binary_sensor`, diagnostic `sensor` entities, repairs counters, etc.) **must** pass `SERVICE_SUBENTRY_KEY` when constructing entities **and** expose `device_info = service_device_info(include_subentry_identifier=True)`. This guarantees every diagnostic entity binds to the same service device + config subentry pair so Home Assistant keeps the hub grouped in the device registry.
  * Per-device platforms (`device_tracker`, per-device `sensor` entities such as `last_seen`, `button` actions, future tracker-scoped entities) **must** pass `TRACKER_SUBENTRY_KEY`, publish tracker-specific identifiers in `device_info`, and rely on Home Assistant's automatic device association—do **not** add manual `via_device` tuples for tracker devices.
  * Device-scoped entities of **any** domain stay on their tracker device, binary sensors included. The relink (`_async_relink_subentry_entities` in `custom_components/googlefindmy/__init__.py`) keeps every other `binary_sensor` on the service device and recognises a device-scoped one only by its `unique_id` suffix in `_DEVICE_SCOPED_BINARY_SENSOR_SUFFIXES` (today `_uwt_mode`). Whoever adds a device-scoped binary sensor adds its suffix there; otherwise the relink moves it to the service device on every start and the duplicate cleanup deletes it as a sibling of the other trackers' sensors. `tests/test_relink_uwt_mode_anchor.py::test_relink_knows_the_device_scoped_binary_sensors` fails when a suffix is missing, for the classes within the guard's scope. The guard cannot see every form; its docstrings name examples, pinned by `test_suffix_guard_declared_blind_spots` and `test_suffix_guard_blind_to_a_symlinked_directory`.
  * The integration removes a tracker device itself only when it is missing from two consecutive fresh, non-empty, complete device lists (`_remove_stale_devices` in `coordinator/registry.py`, decided by `plan_stale_device_removals` in `coordinator/helpers/registry.py`, called from the update cycle in `coordinator/polling.py`). Only devices of the own config entry count, never the service device, never a device without a readable Google id, and never a device while any of its Google ids is still listed. A cached, deferred or empty list, a fetch that dropped candidates without a valid id, and a canonicless `warn` drop all count as no list; a device whose name the decoder dropped from the fetch for lack of a canonical id, whatever its class, counts as listed (`decoder.get_canonicless_name_counts`), compared through `normalize_device_name` and without the hub collision suffix, and only while that name has more drops of one kind (`android`, `no_information`, `tracker`, from the device type Google reports) than in the last counting list that still carried the device with its id (a phone of the same name that was dropped before does not keep a departed tracker; a phone and a tracker of one name that swap roles in one listing do, because the drop changes kind; two devices of one name and one kind stay indistinguishable, different names in the Find Hub app tell them apart; after a restart the name alone protects). For this match the registry sync keeps the integration `name` of a tracker in step with Google also under a user name, as Core does; `name_by_user` is never touched. Removal runs through `_apply_device_ownership` with DETACH on the device's subentry link, counts a device that is gone from the registry or has no link to the entry afterwards, retries a refused or failing removal once and then leaves the device alone until the next setup (one WARNING), and does **not** add the device to `ignored_devices`, so a tracker added back to the account returns. Its entities return with it only through a reload, because each platform keeps the ids it has set up for the lifetime of its setup: the coordinator remembers the removed Google ids and reloads the entry once, through the shared latch `claim_pending_entry_reload`, when a fresh list carries one again (`tests/test_stale_device_return_platform.py` pins the platform side). Its log records run on the poll path: INFO names the Home Assistant `device_id`, the device name stays at DEBUG (section 5, class (b)); an exception is logged at WARNING with its class only and at DEBUG through `describe_exception`, never with its text. This is the Quality Scale rule `stale-devices`; `tests/test_stale_device_removal.py` holds one test per protection rule (S-R1 to S-R9, S-R3d).
  * Unloading an entry keeps its registry entries. The integration purges the entities and devices of a subentry on unload only when its platforms demonstrably never loaded (`ValueError` from the platform unload, `_async_unload_subentry` and the legacy branch of `_unload_config_subentry` in `custom_components/googlefindmy/__init__.py`), never otherwise. Removing a subentry or the entry is cleaned up by Core (`ConfigEntries.async_remove_subentry`, entry removal); devices that leave the Google account are removed by the stale-device rule above, not by the unload. The `rebuild` mode of `googlefindmy.rebuild_registry` removes entities and devices explicitly between unload and setup through `purge_entry_registrations` in the service context. `tests/test_unload_keeps_registry.py` covers reload, never-loaded subentries and the Core path; `tests/test_services_rebuild_registry.py` covers the service.
  * Subentry setup validation now lives alongside `_async_setup_new_subentries` in `custom_components/googlefindmy/__init__.py` with coverage in `tests/test_subentry_setup_trigger.py`; skim those guards when adjusting child setup or registry checks.
  * Registry updates **must not** pass `add_config_entry_id`,
    `add_config_subentry_id`, `remove_config_entry_id` or
    `remove_config_subentry_id`. The former rule on this line ("include the child
    `entry_id` in every `async_update_device` call, `add_config_entry_id` for
    2025.7+, `config_entry_id` for legacy cores") is superseded and must not be
    reintroduced. From Core 2026.8 a device has a single owning entry and a single
    owning subentry, so there is nothing to add: `add_config_entry_id` on its own
    changes nothing but arms a deferred move that a later
    `remove_config_entry_id` completes, and `remove_config_entry_id` on the
    owning entry deletes the device unless such a move is armed. Never drop or
    add just one half of that pair: it turns a working move into a deletion.
    Route every ownership change through
    `_apply_device_ownership` / `plan_device_ownership`, which selects
    `new_config_subentry_id`, the legacy quadruple or `async_remove_device`
    depending on the signature of the installed core (minimum `2025.9.1`, so the
    legacy branch stays). `plan_device_ownership` is the one translation point
    and has no alternative; the executor depends on where you are. With a
    coordinator in hand, call `_apply_device_ownership`. Without one (`services.py`,
    `config_flow.py`), build the plan and run it through `execute_ownership_plan`
    in the same helpers module. Never name an ownership keyword at a call site
    either way. Keep logging the `(entry_id, device_id, identifiers)`
    tuple in debug builds to catch mismatches early, and mirror the troubleshooting
    playbooks in `docs/CONFIG_SUBENTRIES_HANDBOOK.md`, Section VI, when triaging
    stuck or orphaned devices. A static guard,
    `tests/test_guard_device_registry_kwargs.py`, fails the build if the
    superseded keywords reappear outside the legacy translator.
  * **Walking devices: pick the question, not the shortest expression.** When the
    answer is "the devices of this config entry" -- which it almost always is --
    call `dr.async_entries_for_config_entry(dev_reg, entry_id)`. It is not
    deprecated on any supported core and it says what it means. Only where the
    answer is genuinely "every device in the registry" (a collision check across
    entries, a one-time normalisation pass) use `iter_all_devices(dev_reg)` from
    `custom_components/googlefindmy/coordinator/helpers/registry.py`. Then decide
    per call site, and write the decision down. Three shapes occur, and the
    examples are the ones in the tree:
    (a) a pass that acts **for one entry** filters with `device_belongs_to_entry`
    (the service-device fallback scan in `__init__.py`);
    (b) a pass that is **entry-agnostic but needs the owner** reads it per device
    with `device_owning_entry_ids` and does not filter (`_async_refresh_device_urls`,
    which seeds a per-entry map token for devices of every entry);
    (c) a pass that is **entry-agnostic and owner-blind** does neither and says so
    (`_async_normalize_device_names`, the cross-entry collision check).
    `RegistryOperations._ensure_registry_for_devices` belongs to (b) and shows why
    the two are worth telling apart: it *collects* the names the hub's children
    carry without any filter, because a sibling hanging off our hub may belong to
    any entry and filtering there would miss exactly the names to avoid -- but it
    then reads ownership per device in `_resolve_hub_name`, which is what decides
    between reusing a device of ours and appending a suffix for a stranger's.
    Owner-blind while gathering is not owner-blind while deciding.
    Adding an ownership filter to (b) or (c) silently stops the pass doing its job
    for every other entry, which is why the choice is written down rather than
    left to the next reader. **Superseded instruction, do not restore:**
    `dev_reg.devices.values()` and any other mapping access on `devices`
    (`[...]`, `.get(...)`, `.keys()`) are deprecated from Core 2026.9
    (`breaks_in_ha_version="2027.9.0"`, tag `2026.9.0`, line 1560). Plain
    iteration does not report -- but it means two different things: on `2026.9`
    it yields `DeviceEntry` objects, on the declared minimum `2025.9.1` it yields
    device **ids**. That is why the wrapper exists and why writing
    `list(dev_reg.devices)` at a call site is wrong even though it silences the
    deprecation. The same static guard fails the build if a `devices` access
    reappears outside the translator.
  * When `manifest.json` sets `"integration_type": "hub"`, expose an `async_step_hub` handler and register a `"hub"` mapping in `ConfigFlow.async_get_supported_subentry_types()` that points at the service/hub subentry flow handler. This keeps Home Assistant's "Add hub" button functional without custom UI patches.
  * Iterating `entry.subentries.items()` yields `(subentry_id, subentry)` tuples. Always select the child object's global `entry_id` when calling lifecycle helpers or emitting debug logs so identifiers stay aligned across unload fallbacks and cleanup paths.
    * Lifecycle helper checklist:
      - [`async_unload` parent workflow](docs/CONFIG_SUBENTRIES_HANDBOOK.md#f-parent-unload)
      - [`async_remove_subentry` cascade rules](docs/CONFIG_SUBENTRIES_HANDBOOK.md#h-cascading-removal)
  * Home Assistant's subentry helpers (`async_add_subentry`, `async_update_subentry`, `async_remove_subentry`) may return a `ConfigSubentry`, a sentinel boolean/`None`, or an awaitable yielding one of those values. Always normalize results through the shared `_await_subentry_result` helper (or an equivalent `inspect.isawaitable` guard) so asynchronous responses update the manager cache correctly.
  * **Race-condition mitigation:** When programmatically creating subentries, immediately yield control back to the event loop (for example, `await asyncio.sleep(0)`) before invoking `hass.config_entries.async_setup(...)`. This prevents `homeassistant.config_entries.UnknownEntry` errors while the config-entry registry finalizes the new child entry. Leave a short inline comment describing the yield so future contributors retain the guard.

#### Deprecations & migrations

* [`docs/AI_DEPRECATIONS_GUIDE.md`](docs/AI_DEPRECATIONS_GUIDE.md) — Core 2025.10 through 2026.9 migration playbook for breaking changes, API removals, and checklist-driven refactors.
* [`custom_components/googlefindmy/NovaApi`](custom_components/googlefindmy/NovaApi) — Nova helpers, request builders, and protobuf serializers. Keep protobuf imports inside `if TYPE_CHECKING:` guards when the dependency is only needed for type annotations so startup stays fast on constrained devices.
* `custom_components/googlefindmy/Auth/firebase_messaging/**` — Firebase messaging modules share the `JSONDict` and `MutableJSONMapping` aliases defined alongside the implementations. Prefer these aliases over ad-hoc `dict[str, Any]`/`Mapping[str, object]` annotations when describing payloads, responses, or task metadata. When new helpers consume nested JSON, extend the aliases or introduce additional `TypeAlias` definitions in the same module so every constructor or attribute keeps explicit container parameters for mypy strict runs. When referencing stub-only overlays (for example, `protobuf_typing`), wrap the import inside an `if TYPE_CHECKING:` guard and alias to the runtime class otherwise so production code never depends on non-existent modules.
  * **FCM HTTP fatal classifier (PR #169 / #1086):** When `fcm_install`, `fcm_register`, `fcm_refresh_install_token`, `gcm_check_in`, or `gcm_register` encounter a status in `_FATAL_HTTP_STATUSES` (401, 404) that persists past their retry budget, they must raise `FcmRegisterHTTPError(status=N)` instead of returning `None`. The caller side in `Auth/fcm_receiver_ha.py::_raise_if_fatal_http_error` mirrors `_raise_if_fatal_client_error`: the `except FcmRegisterHTTPError` block in `_register_for_fcm_entry` MUST appear before the generic `except (TimeoutError, RuntimeError, Exception)` block so the 401 path triggers `_invalidate_fcm_tokens()` and the 404 retry budget instead of the transient-RuntimeError fallthrough. Helpers that follow this status surface must opt into the same classifier by populating a numeric status cache (see `gcm_register`'s `last_fatal_status: int | None`) and re-raising from it — do not introduce alternative escalation paths such as substring matching against the logger output, which couples the defense to the log message format. The numeric cache MUST follow **last-wins** semantics, and the classification SSOT MUST be the **HTTP status** of each response, not the branch identity or the response body shape. Every alternative response branch in the retry loop re-assigns the cache to `int(status) if int(status) in _FATAL_HTTP_STATUSES else None` (the network-exception branch is the only exception: no HTTP status is available, so the cache is cleared to `None`). This mirrors the long-standing `last_error` update discipline. Two related failure modes have surfaced and are now pinned by regression tests in `tests/test_fcm_register.py`: (a) **Stale fatal across non-fatal followups** — a retry that gets a non-fatal followup (transient 5xx, structured `Error=` body, network exception) after an earlier 401/404 MUST clear the cache so the post-loop classifier reflects the FINAL attempt, not the first fatal seen (Codex finding on PR #1087 commit `559afde82f`); (b) **Body-shape misclassification** — the structured `Error=...` body branch MUST NOT unconditionally clear the cache. The server CAN return a structured error body together with a fatal HTTP status (401/404); the body shape is orthogonal to the auth/endpoint classification used by the caller. Classify from the HTTP status, not from "this is the error_code branch" (Codex finding on PR #1087 commit `7a89e2e321`).
* `custom_components/googlefindmy/NovaApi/**` — Prefer type annotations that reference protobuf-generated classes (for example, `DeviceUpdate_pb2.ExecuteActionRequest`) but import those modules exclusively under `if TYPE_CHECKING:` guards. At runtime, alias to the concrete protobuf package shipped with Home Assistant. This keeps Nova helpers importable during Home Assistant startup without incurring the protobuf import cost unless type checking is running.

### Runtime vs. type-checking import quick reference

Use the following patterns whenever a module only exists as a `.pyi` stub or when the runtime dependency must stay optional:

1. **Guard the stub import** so production code never tries to import the missing module:

   ```python
   from typing import TYPE_CHECKING

   if TYPE_CHECKING:
       from custom_components.googlefindmy.protobuf_typing import MessageProto
   else:
       from google.protobuf.message import Message as MessageProto
   ```

2. **Provide a runtime alias** to the concrete implementation (or a graceful fallback) so the rest of the module can use the shared name (`MessageProto` above) without knowing whether it came from the stub or the runtime module.

3. **Avoid work in the `TYPE_CHECKING` block.** Limit the guarded section to imports and type-only definitions; execute all runtime logic outside the guard so mypy and the interpreter share the same behavior.
4. **Catch only `ImportError` when providing runtime fallbacks.** Optional integration helpers should surface unexpected runtime exceptions immediately instead of masking them behind broad `except Exception:` guards. This keeps startup failures debuggable and prevents silent misconfiguration when a dependency is present but broken for other reasons.
* `google/protobuf/**` — Local type stub overlays that model the minimal subset of `google.protobuf` used by the integration. These stubs unblock strict mypy runs without depending on the upstream package’s incomplete type hints. Update them when generated protobuf code begins to reference additional APIs or when upstream ships first-party stubs that supersede these local helpers.
* **Documentation alignment:** When describing config entries or subentries in `docs/`, keep `docs/CONFIG_SUBENTRIES_HANDBOOK.md` as the canonical source of truth. Sync other documentation with the handbook and add inline citations back to the relevant sections whenever you reference the `homeassistant.config_entries` interface snapshot or platform-forwarding rules.

>
> | Domain | Primary use cases |
> | --- | --- |
> | `https://developers.home-assistant.io` | Integration architecture, helper APIs, config flow patterns, data update coordinator guidance, and other developer-facing contracts. |
> | `https://www.home-assistant.io` | User-facing behavior references, feature overviews, release notes, and configuration examples that affect documentation or UX communication. |
> | `https://alerts.home-assistant.io` | Service advisories, breaking changes, security or outage bulletins that may require mitigation steps or temporary workarounds. |

## Environment verification

### Quick-reference (per-task checklist)

* Connectivity probe: follow the [connectivity probe](#connectivity-probe) before each implementation cycle.
* Dependency bootstrap: run `make install-dev` (or `poetry install --with dev,test`) before linting or tests; it installs `homeassistant`, `pytest-homeassistant-custom-component` and the rest of the dev/test toolchain from `poetry.lock`.
* Dependency primer (fresh environments): `make install-dev` already installs core extras such as `httpx[http2]`, `ecdsa`, and `selenium` (runtime dependencies in `pyproject.toml`); install them by hand only in pip-based setups so coverage and flow tests do not stall on missing HTTP/crypto/browser drivers.
* Cache accelerators: preserve the download caches (for example, mount/retain `~/.cache/pypoetry` for Poetry and `.cache/pip` for pip) so repeated `make install-dev` and `pip install -r custom_components/googlefindmy/requirements-dev.txt` runs reuse the built wheels instead of redownloading them on every validation cycle.
* Optional driver preload: `make install-dev` installs the runtime dependencies from `pyproject.toml` together with the `dev` and `test` groups, so optional clients (for example, `gpsoauth`) are ready for `pytest --cov -q` without extra commands.
* Required checks (in order): `python -m ruff check --fix`, `python -m mypy --strict`, `python -m pytest --cov -q`.
* Registry helpers: when adjusting device/entity relink logic, reuse the shared helper contract documented near `_async_relink_entities_for_entry` instead of adding new lookup paths so registry fallbacks stay consistent.

> **`make install-dev` refresh (fresh checkout quick how-to):**
> 1. Run `make install-dev` once after cloning to install Home Assistant, `pytest-homeassistant-custom-component` and the dev/test toolchain into the Poetry environment.
> 2. If the command reports a missing `poetry`, install it first (`pip install poetry`, see the README) and rerun it.
> 3. Reuse the same command whenever mypy/pytest import errors resurface after environment resets; Poetry skips packages that are already installed at the locked version.

> **Quickstart:** Run `make install-dev` once per fresh environment to install `homeassistant` and `pytest-homeassistant-custom-component` before `pytest -q`, `mypy --strict`, or `ruff check`; the download/build step typically finishes in about five minutes, so plan lint/test runs with that buffer in mind.

### Connectivity probe

* **Canonical HTTP/HTTPS check:** Run `python -m pip install --dry-run --no-deps pip` before each implementation cycle and record the output chunk or log path for citations.
* **Citation helper:** Note the exact chunk identifier or log file path containing the probe output so future summaries can reference it without rescanning the terminal history.
* **Alternate probes:** Prefer the [connectivity probe](#connectivity-probe) and, if you need to confirm package channels more broadly, supplement with commands such as `pip index versions pip` or a metadata refresh like `apt-get update`. Avoid ICMP-only checks (for example, `ping 8.8.8.8`), which are blocked and do not reflect HTTP/HTTPS reachability.
* **Module-path reminder:** When a tool installs entry points outside the current `PATH`, invoke it as `python -m <module>` so the connectivity probe also confirms module availability despite interpreter differences.

### Quickstart checks (fast path before `pytest -q`)

* **Cache cleanup:** Run `make clean` to prune `__pycache__` directories and stale bytecode before rerunning tests.
* **Connectivity probe:** Follow the [connectivity probe](#connectivity-probe) and capture the output before running tests.
* **Test dependency install:** Use `make install-dev` to install `homeassistant`, `pytest-homeassistant-custom-component` and the remaining dev/test packages before `pytest -q`. Plan for several minutes of download time in the hosted environment because the Home Assistant wheels are large.
* **Lint-in-test requirement:** Every testing cycle must include `python -m ruff check --fix .` so lint autofixes run alongside the rest of the suite.
* **Pinned stub constraints:** `make install-dev` resolves Home Assistant and the pytest plugin from `poetry.lock`. `custom_components/googlefindmy/constraints-test-stubs.txt` only applies to pip-based installs: `custom_components/googlefindmy/requirements-dev.txt` includes it via `-c`, and the `test_legacy_python` job in `.github/workflows/ci.yml` rewrites its pins for each Home Assistant axis. Reuse it when installing Home Assistant with pip outside Poetry.
* **DocToc preinstall:** Run `make bootstrap-doctoc` once per environment to install the DocToc dev dependency non-interactively (cached under `.npm-cache`); subsequent `make doctoc` calls reuse the installation to refresh the AGENTS.md table of contents.
> **Connectivity Probes in Final Reports:** Clarify that a "connectivity probe" (manual verification of API/FCM reachability) counts as a valid required "test" step for the final report when automated integration tests cannot simulate the specific network outage scenario. This reduces ambiguity regarding documentation of probes versus traditional test runs.
* **Strict mypy prep:** In a fresh environment, run `make install-dev` before `mypy --install-types --non-interactive --strict` so the Home Assistant stubs are already present and strict type checking doesn't trip over missing packages.
* **Coverage reminder:** After adjusting registry helper fallbacks, rerun `pytest --cov -q` to confirm coverage stays intact before committing.
* **Pytest coverage guard:** `pytest -q --cov` will raise the conftest bootstrap error if the Home Assistant stubs are missing. Run `make install-dev` first so coverage runs complete without runtime import failures.
* **Pytest-homeassistant HTTP stub:** When `pytest-homeassistant-custom-component` is installed (for example via `make install-dev` or `pip install -r custom_components/googlefindmy/requirements-dev.txt`), keep the autouse `disable_http_server` fixture in `tests/conftest.py` enabled. The plugin accesses `homeassistant.helpers.http` during startup in HA 2025.12.0; the fixture patches that helper so imports succeed under the lightweight stub environment.
* **Import normalization:** Run `python -m ruff check --select I --fix` to auto-sort import blocks (especially in `tests/`) instead of rearranging them manually. Import coroutine annotations (for example, `Coroutine`) from `collections.abc` alongside other container ABCs to avoid duplicate-definition lint errors when `typing` re-exports overlap.
* **Pytest log capture for citations:** Pipe full coverage runs to a log file (for example, `python -m pytest --cov -q | tee pytest-full.log`) and cite the relevant tail snippet instead of rerunning the suite when only the output is needed.

* **Cache hygiene helper.** Run `make clean` from the repository root to prune `__pycache__` directories and stray `*.pyc` files after tests or whenever caches need to be refreshed. If you need to tidy up manually (for example after executing helper scripts), remove the bytecode caches with `find . -type d -name '__pycache__' -prune -exec rm -rf {} +` before committing.

* **Generated protobuf sampling.** Use `script/rg_proto_snippet.sh` to preview hits when searching massive generated files (for example, `script/rg_proto_snippet.sh encryptedMetadata custom_components/googlefindmy/ProtoDecoders`). The helper wraps `rg --max-count` and truncates each line via `cut` so shell output stays within the limit documented in this guide.

* **Dependency bootstrap timing:** Expect `make install-dev` to run for roughly five minutes in the hosted environment (downloads + wheel builds). Plan shell work accordingly so lengthy installs finish before kicking off lint or test runs.
* **Dev dependency install guard:** Run `make install-dev` after cloning or rebasing to install the full dev/test stack (including Home Assistant). This avoids skipped or failed `pytest`/`mypy` runs due to missing packages and keeps the resolver checks green without manual pip retries. **Always run this (or an equivalent `python -m pip install -r custom_components/googlefindmy/requirements-dev.txt`) before any lint/test invocation**, especially when exercising Moto Tag regression suites that import optional crypto/backoff libraries.
* **Rotation-window debug toggle:** The resolver's rotation window helper now accepts an `allow_negative` flag to log and scan negative windows when debugging drift-heavy anchors. When investigating timebase drift, enable the toggle in the resolver debug path instead of re-reading the code to remember how negative offsets are surfaced.
* **Bootstrap helper:** `make install-dev` is the single bootstrap target. The former targets `make bootstrap-base-deps`, `make test-deps`, `make test-stubs` and `make install-ha-stubs` were removed with the move to Poetry and no longer exist.
* **Stub refresh after upgrades:** When dependency versions change (for example, after rebasing or updating requirements), rerun `make install-dev` so Home Assistant and the pytest plugin stay in sync before invoking `pytest -q` or `mypy --strict`.
* **Fallback reminder:** If a CLI helper such as `pre-commit` is not yet on the PATH, rerun the command via its module form (for example, `python -m pre_commit run --all-files`) so the initial check still succeeds.
* **Online mode:** When a network connection is available you may install or update missing development tooling (for example, `pip`, `pip-tools`, `pre-commit`, `rustup`, `node`, `jq`) whenever it is necessary for maintenance or local verification.
* **Offline mode:** When no connection is available, limit the work to local analysis only and call out any follow-up actions that must happen once connectivity is restored.

### Cleanup shortcuts

* **DocToc reinstall:** Run `make clean` to delete `.bootstrap/doctoc-preinstall.stamp` when you need to force a fresh DocToc install before rerunning `make doctoc`.
* **Home Assistant reinstall:** After a dependency change, rerun `make install-dev`; Poetry reinstalls the changed `homeassistant`/`pytest-homeassistant-custom-component` versions from `poetry.lock`. There is no sentinel file to delete.

### Module invocation primer

Some developer tools register entry points inside isolated Python environments that differ from the shell `PATH`. When a command is missing, rerun it in module form to guarantee it resolves the correct interpreter:

* **Python packaging:** `python -m pip ...` (install, inspect, or run connectivity probes).
* **Pre-commit hooks:** `python -m pre_commit ...` (install, list, or run hooks when `pre-commit` is not yet on the PATH).
* **Formatters and linters:** Module-friendly tools (`python -m ruff`, `python -m pytest`, etc.) keep working even when scripts are shadowed by other environments.
* **Repository utilities:** Project helpers with `__main__` shims (`python -m script.sync_translations`, for example) follow the same pattern.

Prefer the executable name when it is available; fall back to the module form whenever onboarding, switching interpreters, or recovering from environment churn.

### Shared pip cache for stub installs

* Export `PIP_CACHE_DIR=/workspace/.cache/pip` (or reuse the default `~/.cache/pip`) and `POETRY_CACHE_DIR` (default `~/.cache/pypoetry`) before invoking `make install-dev` or pip installs so wheel downloads persist across sessions. Reusing the same caches keeps the Home Assistant and pytest plugin installs fast and avoids repeated dependency fetches in fresh shells.
* If you routinely rebuild the stub environment, keep a reusable virtualenv or wheelhouse around to sidestep the lengthy `make install-dev` bootstrap. For example, cache a pre-populated `.venv` or store `pip download` artifacts in a shared directory that `PIP_FIND_LINKS` or `pip --no-index --find-links` can consume on the next run.

---

## 1) What must be in **every** PR (lean checklist)

* **PR template alignment.** Complete [.github/PULL_REQUEST_TEMPLATE.md](.github/PULL_REQUEST_TEMPLATE.md) and keep the responses synchronized with the items listed below.
* **AGENTS upkeep.** Before opening a PR, review all applicable `AGENTS.md` files and update them when improvements or corrections are evident.
* **Translation schema guardrails.** Home Assistant option/config flow translations must stick to the documented schema. Under each `options.step.<id>` or `config.step.<id>` block, only use `title`, `description`, `data`, `data_description`, `menu_options`, `error`, `abort`, and `progress`. Avoid nested `select` objects or other ad-hoc keys—translation validation will reject them and the options UI will render placeholders instead of labels.
  * `data_description` entries must be **strings**. Validators reject dictionaries at that path, so encode select choice labels in plain text (for example, list the choices in the field description) and reserve option-specific labels for the selector definition in `config_flow.py`.
  * Keep localized translations in sync with the base `strings.json`. If you flatten a `data_description` value to a string in the source strings, mirror the same shape in every `translations/<lang>.json` file; a single lingering dictionary (for example, per-locale `options`) will cause hassfest to fail.
  * **Consistent form of address (formality).** Each `translations/<lang>.json` file must use a single, consistent form of address throughout, matching the register Home Assistant uses for that locale. Never mix formal and informal address within one file. Home Assistant mandates formality only for German: use the informal *du*, never the formal *Sie* (per the Home Assistant [translation guidelines](https://developers.home-assistant.io/docs/translations/), German section: "Duze in den Übersetzungen, und verwende nicht das formale 'Sie'."). For every other locale, follow the register that locale already establishes. In this repository that means: French formal (*vous*); European Portuguese (`pt`) courtesy third person (courtesy imperative, no explicit *você* pronoun); Brazilian Portuguese (`pt-BR`) informal *você*; Dutch informal (*je*); Italian informal (*tu*); Polish informal second person (*ty*); Spanish informal (*tú*). Hebrew has no formal/informal *you* distinction, and the English base (`strings.json`, `translations/en.json`) is register-neutral. When adding or editing a localized string, match the existing register of that file. <!-- codespell:ignore -->
* **Voluptuous defaults:** When schema defaults need to reference runtime values (for example, Home zone coordinates), prefer callable defaults rather than eagerly captured constants. Tests that validate option-flow defaults should coerce callables to their resolved values before asserting to avoid brittle comparisons across environments.
* **Contributor guidance hygiene.** Verify that root and scoped `AGENTS.md` files remain accurate. When code or tests touch related automation or guidance, review and update the impacted `.github` workflows/templates, shared test utilities, and documentation so they stay current.
  * **Mypy override ordering.** Append new strictly-typed modules to the override list in `pyproject.toml` in alphabetical order so future reviews can spot additions quickly.
  * **Codespell ignore list.** Add a word to `[tool.codespell] ignore-words-list` in `pyproject.toml` when the reports it causes outweigh the detection lost tree-wide; `tests/test_guard_prose_contract.py` carries that trade and its measured counts.
  * **Test scaffolding reference.** The Home Assistant config flow stubs and helper behaviors for tests are documented in [`tests/AGENTS.md`](tests/AGENTS.md); point future contributors there whenever discovery/update helpers change.
  * **Task scheduling helpers.** Home Assistant-style test doubles for `async_create_task` may accept only `(coro)` without keyword arguments like `name`. Design scheduling wrappers so they gracefully handle both signatures and still attach error-handling callbacks when a task object is returned.
  * **Discovery callbacks.** Reuse `ha_typing.callback` for new discovery helper callbacks so strict mypy keeps enforcing the typed decorator instead of drifting back to untyped shims.
* **pre-commit hooks.** Run the hooks from the project environment (`poetry run pre-commit run --all-files`); the `mypy` and `pytest` hooks use `language: system` and fail when the environment is not active. CI runs every other hook as the `pre-commit` entry of the `lint` matrix. Nothing pushes fixes to PR branches, because no hosted pre-commit service is connected; keep `.pre-commit-config.yaml` aligned with the enforced checks.
* **TOC upkeep.** Generate the root `AGENTS.md` overview with the in-repo hook `script/precommit_hooks/update_doctoc.py`: run `pre-commit run doctoc --files AGENTS.md`, or `make doctoc` as a shorthand for the same script. The hook derives anchors the way GitHub renders heading ids, so every link of the overview resolves on GitHub.
* **Hassfest auto-sort workflow.** `.github/workflows/hassfest-auto-fix.yml` must remain present and operational so manifest key ordering issues are auto-corrected and pushed back to PR branches; update the workflow when hassfest or `git-auto-commit-action` inputs change upstream.
* **Action pinning.** Every third-party `uses:` line in `.github/workflows/`, except the two branch-tracking actions named below, references a full 40-character commit SHA followed by a comment naming the most specific release tag that points at it (`uses: actions/checkout@<sha> # v7.0.1`); a tag such as `@v7` can be moved, a commit SHA cannot. Dependabot's `github-actions` ecosystem (`.github/dependabot.yml`) updates such pins and the version comment together. The only exceptions are `home-assistant/actions/hassfest` and `hacs/action`: they track their default branch on purpose because their newest release tags are far behind it, and a pin would freeze the validation rules until someone refreshes it by hand. Each exception carries a comment line above it pointing here. `tests/test_workflow_action_pins.py` enforces the form in the regular `pytest` run, including that the exception list has no stale entries and that one action resolves to one SHA everywhere. The test runs offline, so it does not check that the tag in the comment really points at the SHA: verify that when writing or changing a pin (`gh api repos/<owner>/<repo>/commits/<tag> --jq .sha` must print the pinned SHA). Extend its `BRANCH_TRACKING_ACTIONS` only together with this paragraph.
* **Purpose & scope.** PR title/description state *what* changes and *why*, and which user scenarios are affected.
* **Tests — creation & update (MUST).** Any code change ships unit/integration tests that cover the change; every bug fix includes a **regression test** (§3.2). Never reduce existing coverage without a follow-up to restore it.

  * **Auto-corrections applied:** trivial test failures (syntax, imports, obvious assertion drift) are automatically fixed when unambiguous (§3.1).
  * **Regression test added:** for `fix:` commits (or `fix/...` branches), add a minimal regression test if none existed (§3.2).
* **Ruff linting parity.** Treat `ruff check` as co-equal with `pytest -q` and `mypy --strict`; run it before presenting results and resolve every reported issue in-tree.
* **Deprecation remediation.** Investigate and resolve every `DeprecationWarning` observed during implementation, local verification, or CI. Prefer code changes over warning filters; if a warning must persist, document the upstream blocker in the PR description with a follow-up issue reference.
* **Coverage targets (aspirational target vs. effective floor).** The long-term targets stay **config flow at 100 %** and repo total **≥ 95 %** (`COVERAGE_PLATINUM_TARGET` in `tests/test_platinum_compliance.py`). They are *targets*, not the gate that fails a PR. The **effective floor is 80 %**, declared identically on both sides — `pyproject.toml` `[tool.coverage.report] fail_under` (engine) and `.github/workflows/ci.yml` `--cov-fail-under` (CI) — and `tests/test_platinum_compliance.py::test_coverage_threshold_enforced` fails the PR when the two drift apart or leave the `[60, 95]` band. Measured on `c972fd34` (recorded in the `pyproject.toml` comment block): repo total **80.33 %**. Earlier, on `04a66fc2` with the floor at 78 %: repo total **78.36 %**, `custom_components/googlefindmy/config_flow.py` **79 %**; there is no separate config-flow gate, so the 100 % figure is a target only. Treat the floor as a **ratchet**: raise it (both declarations in the same commit, with the measured total and its commit recorded in the `pyproject.toml` comment block) whenever a series locks in new coverage, and never lower it. If necessary code removal pushes the total below the floor, **open a follow-up issue** to restore coverage and reference it in the PR (§3.5).
* **Behavioral safety.** No secrets/PII in logs; user-visible errors use translated `translation_key`s; entities report `unavailable` on communication failures.
  * **Log hygiene for scanners.** Code scanning on the upstream repository reports CodeQL (`py/clear-text-logging-sensitive-data`) and Semgrep (`python-logger-credential-disclosure`) results for pull requests, and it counts a result as open even when the line carries `# nosemgrep` (Semgrep marks the result suppressed in the SARIF, code scanning ignores that) or a `codeql[...]` comment (the CodeQL setup here evaluates no such comments). Fix such a finding by changing the code. This is separate from the job gate in `.github/workflows/semgrep.yml`, which does honour `# nosemgrep: <rule-id>` for deliberate test fixtures. Semgrep matches the format string alone: a logger call whose format string literal contains `api key`, `secret`, `credential`, `token` or `password` (any case, also inside a word such as `TokenCache`) followed by a `%s`, both within the first source line of the literal, is reported, whatever the argument is (measured with Semgrep 1.180.0: a word on the first line and `%s` on a continuation line is not matched); name the component in neutral words (`cache instance` instead of `TokenCache`). CodeQL follows data flow from names and values that look like secrets (`*_token`, `*secret*`, `*password*`) into the log call; do not log a cache key name, because per-account keys embed the account e-mail (`owner_key_<email>`, `shared_key_<email>` in `__init__.py`). Log the value type, a count, or a class (a)/(b) identifier from section 5 ("Never log") instead. CodeQL has no sanitizer for this query: a value derived from a sensitive source stays sensitive after `_mask_email_for_logs`. A local or parameter whose name CodeQL's name heuristics (`SensitiveDataHeuristics.qll`) classify as secret, such as `secrets_bundle`, is such a source, so everything read through it counts as a secret; names with `account` are classified as identifiers, which this query ignores. Read an account e-mail from a bundle into a neutrally named local, and name an account in a log line by the label the caller derives from the config entry (`_label_entry_for_log(entry)`: the masked account e-mail, else the entry ID, never the entry title), not by a clear-text value read from the bundle. Log a bundle field name only if it is a fixed name taken from a literal list (`_LOGGABLE_BUNDLE_FIELDS`); other field names can embed the account e-mail (`adm_token_<email>`). To log a name for a credential key, choose a literal by comparison (`_optional_credential_label`) instead of logging the key. `tests/test_log_hygiene_pilot.py` pins both rules for every logger call in `Auth/token_cache.py`, `tests/test_log_hygiene_auth.py` pins the Semgrep form and credential-named constants for every logger call in `Auth/`, and `tests/test_log_hygiene_init.py` pins the Semgrep form for every logger call in `__init__.py`, the neutral local in `_resolve_entry_email`, the caller-side account label of the credential seed, both fallbacks of `_label_entry_for_log`, and what `_async_save_secrets_data` logs; its docstring lists what it does not cover. `tests/test_log_hygiene_entry_title.py` pins that no logger call in any module of the package passes a config entry title or the entry object itself, whose `repr` carries the title (name the entry by its ID or by `_label_entry_for_log(entry)`); its docstring lists the forms it does not follow. `tests/test_log_hygiene_flow_discovery.py` pins, for every logger call in `config_flow.py` and `discovery.py`, the Semgrep form and credential-named constants, that token candidate sources are logged only through `_probe_source_label` (a fixed name from `_PROBE_SOURCE_NAMES`, `<name>_n` for an indexed name such as `tokens_0`, else `other`) and discovery namespaces only through `_namespace_label`, that `_redact_account_for_log` without an e-mail argument names only the kind of key, and that no logger call in `config_flow.py` puts an `email` field or a `_mask_email_for_logs` value into `extra` (the address there can come from the secrets bundle, and Home Assistant's log format does not print `extra`); its docstring lists what it does not cover. The name heuristics apply to non-secret values as well: a timestamp in a local named `secrets_creation_date` or a length limit in a constant named `_SECRETS_STRUCT_LEN_THRESHOLD` is reported, so give such values a neutral name (`anchor_date`, `_STRUCTURED_PAYLOAD_LEN_THRESHOLD`). CodeQL also classifies `lat`/`lon` values as private data; a rejected coordinate is no less precise than an accepted one, so describe each half by a fixed label (`ok`, the kind from `_coordinate_kind`, or `ok`/`invalid` chosen by `_coordinate_in_range` in `coordinator/locate.py`), and name the device of a rejected fix only at DEBUG (section 5 (b)). `tests/test_log_hygiene_coordinator_nova.py` pins, for the logger calls it detects in `coordinator/identity.py`, `coordinator/locate.py`, `NovaApi/ExecuteAction/LocateTracker/decrypt_locations.py`, `NovaApi/ExecuteAction/LocateTracker/location_request.py`, `NovaApi/ListDevices/nbe_list_devices.py` and `NovaApi/nova_request.py`, the Semgrep form, that no argument reads a raw coordinate or a name with `secret` in it, that `identity.py` passes none of the key-material names listed in the test (for these two name checks, arguments of `len`, `type` and `isinstance` are excepted), and that the device-list CLI prints a location only in `_print_locations`, whose purpose is to show it; its docstring lists what it does not cover. `tests/test_log_hygiene_nova_account.py` pins that no logger call in `NovaApi/nova_request.py` passes the account name (`self.username`), a per-account key property (`self.k_*`), a `_key_variants` call or a local named for a key built from the account, neither among the arguments after the format string, nor as a keyword value such as `extra`, nor inside a computed format argument (f-string or `%`), that the two namespaced-mirror records in `_get_initial_token_async` pass no `exc_info` and log only the exception type, and that the AAS lifetime records name no account at runtime; its docstring lists what it does not cover.
* **Docs/i18n (when user-facing behavior changes).** Update `README.md` and every relevant translation; avoid hard-coded UI strings in Python. Make sure placeholders/keys referenced in Python files match `strings.json` and `translations/en.json`. Afterwards synchronize every file in `custom_components/googlefindmy/translations/*.json` (for example, via `python script/sync_translations.py`, `pre-commit run translations-sync`, or `git diff -- custom_components/googlefindmy/translations`). The helper at [`script/sync_translations.py`](script/sync_translations.py) (callable with `python script/sync_translations.py`) will overwrite the base language with `strings.json` and backfill other locales (use `--check` during CI-style verification). Follow **Rule §9.DOC** so documentation and docstrings stay intact. Document any CI/test guidance adjustments directly in the PR description so automation notes remain accurate.
  * **Config subentry doctrine.** When touching README or docs about setup flows, reference Section 0 of `docs/CONFIG_SUBENTRIES_HANDBOOK.md` so guidance reflects the parent–child enforcement model, the March 2025 `_get_entry()` rename, and the expectation that `async_setup_entry` (not `async_setup`) owns instance bootstrap.
  * **Reference-style footnotes.** When introducing new reference-style links or numbered footnotes in Markdown, append the matching `[label]: URL` definitions so every callout resolves to a valid target.
  * **Pre-commit ready translations.** Run `python script/sync_translations.py` before committing any translation updates so the local pre-commit hook (`script/sync_translations.py --check`) passes without extra iterations.
* **Deprecation check (concise).** Add 2–4 bullets with links to HA release notes/dev docs that might affect this change (§8).
* **Quality-scale evidence (lightweight).** If a Quality-Scale rule is touched, append one evidence bullet in `quality_scale.yaml` (or propose adding the file). **Do not block** if it’s missing—note this in the PR.
* **Historical context.** For regressions, reference implementations, or suspected newly introduced bugs, inspect the relevant commit history (e.g., `git log -- <file>`, `git show <commit>`, `git diff <old>..<new>`).

> **Local run (VERIFY)**
> **Offline mode:**
> – pre-commit run --all-files *(mandatory)*
> – ruff format --check *(mandatory; capture the outcome in the "Testing" section)*
> – ruff check . *(mandatory; you may run `ruff check --fix` beforehand, but finish with a non-fixing run and document the result)*
> – mypy --strict --explicit-package-bases custom_components/googlefindmy tests *(mandatory for Python changes; capture the outcome)*
> – pytest -q *(mandatory; investigate and resolve every `DeprecationWarning`; capture the outcome)*
> – If Home Assistant stubs are missing, run `make install-dev` before rerunning pytest (see [README — Installing Home Assistant test dependencies on demand](README.md#installing-home-assistant-test-dependencies-on-demand)).
>
> **Online mode (in addition to the offline steps):**
> – pip install -r custom_components/googlefindmy/requirements-dev.txt *(install or update missing dependencies)*
> – pre-commit install *(make sure hooks are installed)*
> – pre-commit run --all-files *(run again if new hooks were installed)*
> – ruff format --check *(reconfirm that formatting is correct)*
> – ruff check . *(reconfirm that linting passes without autofixes; run `ruff check --fix` beforehand if needed and rerun without `--fix` before recording results)*
> – pytest -q *(reconfirm that the tests pass)*
> – mypy --strict --explicit-package-bases custom_components/googlefindmy tests *(full run across the entire codebase and tests)*
> – ruff check --fix --exit-non-zero-on-fix && ruff check *(optional helper when additional linting fixes are necessary; still conclude with the standalone `ruff check .` command above)*
> – pip-compile custom_components/googlefindmy/requirements-dev.in && pip-compile custom_components/googlefindmy/requirements.in *(when the corresponding `*.in` inputs exist; otherwise, record that no compile targets are present and skip without creating ad-hoc `requirements.txt` files)*
> – pip-audit -r custom_components/googlefindmy/requirements-dev.txt -r custom_components/googlefindmy/requirements.txt *(security scan; exit code 1 is acceptable but must be noted)* — prefer `python -m pip_audit` so the tool resolves even when the entry point directory is absent from `$PATH`
> – review package/version updates and synchronize lock files/manifests as needed (see the "Home Assistant version & dependencies" section)
> – rerun the relevant tests/linters after dependency updates
>
> *(The helper `python script/local_verify.py` covers Ruff + Pytest as a quick pass; run it in addition to—but never instead of—the mandatory steps above. `--all` extends it to the full local preflight and marks every stage it could not run as `NOT CHECKED` rather than silently skipping it, see `script/AGENTS.md`. Document every component you run manually.)*
> *Hassfest validation now runs in CI via `.github/workflows/hassfest-auto-fix.yml`; rely on that workflow and re-run it from the PR UI whenever you need a fresh manifest check.*
>
> **optional escalation:** `PYTHONWARNINGS=error::DeprecationWarning pytest -q` *(turns new deprecations into hard failures so they cannot be overlooked—clear the root cause or document the upstream blocker before retrying without the flag).*

## Home Assistant version & dependencies

* **Compatibility target:** Keep the integration working with the latest Home Assistant stable release. When older releases become incompatible, document the oldest supported version in the README or release notes.
* **Synchronization points:** Keep `custom_components/googlefindmy/manifest.json`, `custom_components/googlefindmy/requirements.txt`, `pyproject.toml`, and `custom_components/googlefindmy/requirements-dev.txt` aligned. When bumping versions, check whether other files (for example, `hacs.json` or helpers under `script/`) must change as well.
  * **One deliberate asymmetry:** `selenium` and `undetected_chromedriver` are listed in `pyproject.toml`, `custom_components/googlefindmy/requirements.txt` (the `docker-login` image installs from it) and the dev requirements, but **not** in `manifest.json`. Home Assistant installs `manifest.json` `requirements` into every setup, and no code path Home Assistant executes on its own imports a browser package — the browser is used only by the manual, user-started credential extraction. Do not "repair" this by adding them back; `tests/test_browser_deps_boundary.py` fails if a Home Assistant entry point starts reaching them.
  * **Second deliberate asymmetry:** `aiohttp`, `cryptography` and `httpx` are listed in `pyproject.toml` and `custom_components/googlefindmy/requirements.txt`, but **not** in `manifest.json`. Home Assistant pins all three (2025.9.1, the minimum in `hacs.json`, ships versions above our former floors), and hassfest rejects a custom integration that lists a package Home Assistant itself requires or constrains. The integration code does not import `httpx` at all. Do not add them back. `tests/test_hacs_validation.py::test_manifest_lists_no_home_assistant_core_dependency` is a local early warning that reads only the installed `homeassistant` distribution's requirements; hassfest also reads core's `requirements.txt`, `requirements_all.txt` and `package_constraints.txt` and stays authoritative. With this change the pip-audit gate (`script/audit_manifest.py`) no longer receives `aiohttp` and `httpx` as input; `cryptography` still reaches it as a dependency of `http-ece`. Findings for packages Home Assistant pins never blocked the gate.
* **Upgrade workflow:** With internet access, perform dependency maintenance via `pip install`, `pip-compile`, `pip-audit`, `poetry update` (if relevant), and `python -m pip list --outdated`. Afterwards rerun tests/linters and document the outcomes.
* **Change notes:** Record adjusted minimum versions or dropped legacy releases in the PR description and, when needed, in `CHANGELOG.md` or `README.md`.

### Poetry lock file management

**Critical:** After ANY change to `pyproject.toml`, regenerate `poetry.lock` with `poetry lock` before committing. CI will fail with "pyproject.toml changed significantly since poetry.lock was last generated" if the content-hash doesn't match.

**Correct workflow:**
```bash
# 1. Edit pyproject.toml (e.g., change dependency version)
# 2. Regenerate lock file
poetry lock

# 3. Verify lock is in sync
poetry check

# 4. Commit BOTH files together
git add pyproject.toml poetry.lock
git commit -m "chore: update dependency X to version Y"
```

**Common mistakes to avoid:**
- Committing `pyproject.toml` without regenerating `poetry.lock`
- Running `poetry install` without first running `poetry lock` after `pyproject.toml` changes
- Using `--no-update` flag when dependencies need updating

**CI failure pattern:**
```
pyproject.toml changed significantly since poetry.lock was last generated.
Run `poetry lock` to fix the lock file.
```

* **Manifest compatibility (Jan 2025):** The shared CI still ships a `script.hassfest` build that rejects the `homeassistant` manifest key. Until upstream relaxes the schema for custom integrations, do **not** add `"homeassistant": "<version>"` to `custom_components/googlefindmy/manifest.json` or `hacs.json`. Track the minimum supported Home Assistant core release in documentation/tests instead.

## Maintenance mode

* **Activation:** Maintenance mode is triggered explicitly through a maintainer request, an issue label, or a direct user request. Confirm in your response or PR description that maintenance mode is active.
* **Required checks:** Run the full suite—`mypy --strict` across the entire repository (with no exclusions), complete test suites (`pytest` plus any integration/end-to-end tests), `pytest --cov` (or another `pytest-cov` invocation) to capture coverage whenever maintenance mode is active so my maintenance passes produce actionable data, `ruff check`, `ruff format --check`, `pre-commit run --all-files`, `pip-compile`, `pip-audit`, configuration/manifest synchronization (see "Home Assistant version & dependencies"), and documentation alignment. Launch CLI utilities with `python -m` (for example, `python -m pre_commit run --all-files`, `python -m piptools compile`, `python -m pip_audit`) to avoid PATH-related "command not found" failures when the virtualenv bin directory is not exported by default.
* **Coverage-driven follow-up:** Treat the maintenance-run coverage data as the prioritization source for test improvements, and schedule exactly one follow-up task per cycle that targets the highest-risk coverage gap highlighted by the report so the next increment closes the most critical hole first.
* **Error-to-test mandate:** When either I or the user encounters a defect, misbehavior, or regression during maintenance, treat it as a mandatory opportunity to extend an existing pytest or add a new one that captures the scenario before the cycle closes.
* **Connectivity caveat:** The managed environment occasionally blocks TLS handshakes for PyPI-hosted metadata, which causes `pip-audit` (and other HTTPS-dependent checks) to fail with `SSLError: CERTIFICATE_VERIFY_FAILED`. When this happens, document the failure in the PR/testing summary, capture the offending package URL, and proceed with the remaining maintenance tasks instead of repeatedly retrying the audit.
* **Configuration alignment:** Ensure configuration files (`pyproject.toml`, `.pre-commit-config.yaml`, `manifest.json`, requirement files) reflect the same dependency versions and document the synchronization. Ruff, codespell and bandit follow `poetry.lock`: when a dependency update raises one of them, the same PR bumps the matching `rev:` in `.pre-commit-config.yaml` and the pin in `script/local_verify.py`, and `tests/test_precommit_config_contract.py` fails until both match. Dependabot's `pre-commit` ecosystem stays disabled for these three hooks.
* **Completion notice:** When finished, report whether another maintenance run is required (for example, due to pending upstream fixes) or maintenance mode can be closed. Capture any remaining TODOs or follow-up tasks.

### Config subentry maintenance helper

* `custom_components/googlefindmy/__init__.py::ConfigEntrySubEntryManager._deduplicate_subentries()` removes redundant config subentries while preserving a single canonical group/member pair. Call it when migrations or recovery paths encounter Home Assistant's `AbortFlow("already_configured")` errors to converge on a stable state before retrying updates. **Convergence is not guaranteed for every colliding shape, and a caller must not treat a second abort as impossible.** Since PR #1236 the helper removes a duplicate only where the survivor and the loser share a `subentry_type`: a foreign type sharing an identifier is a group of its own and is left standing, because `async_remove_subentry` clears its device and entity registry bindings irreversibly (the rule and its measurements live in `custom_components/googlefindmy/agents/config_flow/AGENTS.md`). Renaming a colliding holder is the flow's job, through `_claim_unique_id`, which this manager never calls. **The guard has a cost, and an earlier version of this paragraph claimed it away as "measured".** It is not the retry loop being untouched; a spared twin can be the very subentry that then takes a core slot, and the write-back does change an identifier in that case. Reproduced: a `hub` storing the service key on `e1-core_tracking` beside a `tracker` on the same identifier is spared, holds the service slot through its stored key, and `async_sync` then tries to write `e1-service` onto it while a third subentry holds that -- the core raises, the retry deduplicates to no effect, and the second abort lands in `_async_adopt_existing_unique_id`. With the guard neutralised the same shape removes the hub and reaches the right holder directly. Both paths end at the same holder, so the trade is an irreversible removal exchanged for a logged fallback, which is the direction this repository wants; it is not a free change. Pinned by `tests/test_subentry_manager_registry_resolution.py::test_dedup_sparing_a_twin_can_route_a_sync_through_the_adoption_exit`. **"Left standing" is a statement about this helper, not about the call.** `async_sync` runs a type-blind stale sweep of its own afterwards, which removes a spared `hub` that stores a legacy or absent key (`::test_ap1_stale_sweep_is_decided_by_the_resolved_key`); the survival claim is verified end to end only for the production collision shape, by `::test_dedup_a_spared_hub_survives_a_whole_sync_not_just_the_helper`.
* The helper is **idempotent** and refreshes the manager's internal `_managed` mapping after cleanup. Avoid creating new subentries inside the helper; it only removes duplicates reported by Home Assistant.

---

## Key material and resolver hypotheses

### Where key material comes from

* The integration stores the uploaded secret bundle under `DATA_SECRET_BUNDLE` on the config entry; it originates from the user's `secrets.json` input.
* Owner key derivation runs through the Spot API helper and is exposed at runtime via `async_get_owner_key(token_cache)`; the helper records the bytes and version in the entry-scoped token cache.
* Shared-key retrieval is handled by `async_get_shared_key(cache=token_cache)`, which normalizes the value and stores it in the same entry-scoped cache under the canonical `shared_key` key (falling back to migrated per-user keys when present).
* Per-component keys (`derive_component_key`, `docs/FMDN.md` S3.4.1) are only derived from the EIK, in memory: never persisted, never logged, never promoted to an identity key.

### Wrapped EIK hypothesis

* Some trackers appear to return a 60-byte encrypted identity key envelope shaped as `nonce(12) + ciphertext(32) + tag(16)`; AES-GCM verification on that layout rejects invalid tags so unwrap attempts do not misclassify random blobs as valid identity keys.

### Timebase hypotheses

* Resolver scans currently seed three candidates: `ABSOLUTE` (wall-clock), `REL_PAIR` (pair-date anchored), and `REL_SECRETS` (secrets-creation anchored). Once a candidate yields a match, the resolver caches the winning epoch/offset per device to lock onto that timebase and narrow future rotation windows.
* Runtime code outside the resolver does not re-derive these candidates or pick one of them as "the" counter (the CLI tools in `SpotApi/UploadPrecomputedPublicKeyIds` and `SpotApi/CreateBleDevice` derive counters from `pair_date` for precomputed IDs and device registration and are not consumers of the resolver). The FMDN finder reports what a match observed (`GoogleFindMyEIDResolver.last_confirmed_sighting`: variant, window counter, sighting time, and the observed unwanted tracking protection mode from `_effective_uwt_mode()`) and sends nothing without a recent sighting or while that mode is unknown, because the upload field cannot express "unknown" and a `0` would report the mode as off (field data: `docs/UWT_FIELD_DATA.md`), nor when none of the device's current candidate identity keys is the key the sighting was matched under (`ConfirmedSighting.key_fingerprint`), because the window counter belongs to that key; the owner resolves a report over the recent past and near future of the counter (FMDN specification, "Decryption of values encrypted with EID").
* A sighting of a multi-component device also names its component series (`ConfirmedSighting.component`, `docs/FMDN.md` S3.4.1). The finder derives the per-component key from the identity key its fingerprint selects and reports that EID; the fingerprint always stays that of the EIK. The derivation is measured on one device and not published by Google.
* The finder's upload path itself is unverified, independent of the sending conditions above. The disabled code sends own-device reports in a shape that does not match Spot's owner path (`SpotReportingService/UploadOwnerScans`), and this integration's own decrypt path cannot read them back (text payload for `CROWDSOURCED`, unread payload for `SEMANTIC`). Before touching `fmdn_finder/location_uploader.py` or `fmdn_finder/google_uploader.py`, read `docs/FMDN_UPLOAD_LIMITATION.md`, section "Owner location reports". Do not fix single rows of its comparison table; change the upload path only together with a live probe of one own device, and keep `FEATURE_FMDN_FINDER_ENABLED` and `FMDN_UPLOAD_ENABLED` off until then.

### Owner-key error taxonomy (classify only from positive evidence)

The owner-key decrypt chain is the source of the most user-confusing error messages, so the classifier must name the right layer and never guess a credential defect from a transient failure. The grounded facts:

* **L1 - Key chain (directed):** `recovery -> application -> security_domain -> shared -> owner -> EIK`, where each stage decrypts the next (`KeyBackup/cloud_key_decryptor.py:443-449`). A fault low in the chain (EIK no longer matches server reports) is NOT the same as a fault high in the chain (wrong shared key); messages must name the layer, not collapse them.
* **L2 - Owner key arrives encrypted from the server:** `DeviceUpdate.EncryptedOwnerKeyAndMetadata{encryptedOwnerKey, ownerKeyVersion, securityDomain}`, decrypted client-side with the `shared_key` via AES-GCM (`SpotApi/GetEidInfoForE2eeDevices/get_owner_key.py:181-197`, `ProtoDecoders/DeviceUpdate.proto:13-16`).
* **L3 - InvalidTag == wrong shared key:** an AES-GCM `InvalidTag` at the owner-key decrypt is the ONLY positive signal that the shared key is wrong/stale; it propagates unchanged to `SharedKeyMismatchError` (account-wide; a fresh `secrets.json` is required).
* **L4 - ownerKeyVersion mismatch == per-tracker:** a single tracker encrypted with an older owner-key version raises `StaleOwnerKeyError` and needs that one tracker re-paired, NOT an account re-authentication (`NovaApi/ExecuteAction/LocateTracker/decrypt_locations.py:636-641`).
* **L5 - Classifier discipline:** classify only from positive evidence. A transient or partial server response is NOT a credential defect, and the `context` labels ("initial lookup" / "forced refresh") are internal phase markers, never a first-setup indicator (`NovaApi/ExecuteAction/LocateTracker/decrypt_locations.py:288-312`). Unknown/transient owner-key lookup misses map to `OwnerKeyLookupTransientError` (base `Exception`, not a `DecryptionError`), so they never reach the account-wide reauth path.
* **L6 - Own vs. foreign reports:** an own report (`publicKeyRandom == b""`) is decrypted with `SHA256(EIK)` via AES-GCM; if ALL own reports of a device fail, the result is `OwnReportIdentityMismatchError` (per-device, a fresh `secrets.json` does not help) (`NovaApi/ExecuteAction/LocateTracker/decrypt_locations.py:1568-1574`).

---

## 2) Roles (right-sized)

### 2.1 Contributor (implementation) — **accountable for features/fixes/refactors**

* Deliver code **with matching tests** (new/updated) for the changed behavior.
* **Auto-correct trivial test failures** flagged by CI/lint/static checks (§3.1).
* Use `DataUpdateCoordinator`; raise `UpdateFailed` (transient) and `ConfigEntryAuthFailed` (auth).
* Keep runtime objects on `entry.runtime_data` (typed); avoid module-global singletons.
* Inject the session via `async_get_clientsession(hass)`; never create raw `ClientSession`.
* Entities: stable `unique_id`, `_attr_has_entity_name = True`, correct `device_info` (identifiers/model), proper device classes & categories; noisy defaults disabled.
<a id="docstrings--typing"></a>
* **Docstrings & typing.** English docstrings for public classes/functions; full type hints; track the **current HA core baseline** for Python/typing strictness.
* **Strict typing for touched modules.** Run the repository mypy command (see "Local run") and resolve all `--strict` findings for every modified Python file, including tests, before finalizing a PR.

### 2.2 Reviewer (maintainer/agent) — **accountable for correctness**

* Verify the PR checklist, **test adequacy** (depth & quality), and token-cache safety (§4).
* **Test adequacy includes:** presence **and** coverage of **core logic, edge cases, and error paths** relevant to the change (not just line coverage).
* Provide **actionable feedback** on missing/inadequate tests. The contributor fixes failing tests; reviewers may add minimal tests directly if quicker/obvious.
* May request **follow-ups** when repo-wide targets (coverage/typing/docs) are momentarily below goals, without blocking urgent bugfixes.

*(If you use “Code / QA / Docs” sub-roles internally, map them onto this Contributor/Reviewer model; do not require three separate formal sign-offs.)*

### 2.3 History analysis best practices

* Review commit messages surrounding the affected modules to understand intent and regression windows.
* Compare diffs across the relevant commits or branches to pinpoint behavioral changes before coding fixes.
* Correlate suspect changes with coverage in `tests/`, updating or extending tests alongside code adjustments.

---

## 3) Test policy — explicit AI actions

### 3.1 Automatic **Test Corrections** (MUST)

The agent **must automatically fix** tests within the PR when failures are **unambiguous**:

* **Examples:** syntax errors, import/module path mistakes, straightforward assertion updates after renamed parameters/return types, typographical mistakes in test names or markers.
* **Boundaries:** Do **not** auto-change tests when behavior/requirements are unclear (e.g., semantic disagreements, flaky timing without a clear fix). In those cases, request targeted reviewer guidance.

### 3.2 Automatic **Regression Tests** for fixes (MUST)

When a change is a bug fix (**commit type** `fix:` or **branch** `fix/...`) and **no existing test** covers the failure mode:

* Create a **minimal regression test** that **fails without** the fix and **passes with** the fix, isolating the precise scenario (no extra scope).
* Prefer the closest relevant file/naming: `tests/test_<area>_...py`. For config-flow fixes, put it in `test_config_flow.py`; for token/cache issues, `test_token_cache.py`.
* If multiple permutations exist, cover the **single most representative** one; add more only if they catch distinct behaviors.

> **Reviewer reminder:** When service payload handling changes (for example, normalizing `entry_id` inputs from strings vs. lists), explicitly confirm or request regression coverage for each supported payload shape before approving the PR.

### 3.3 Opportunistic **Test Optimization** (SHOULD SUGGEST)

* Suggest improvements **only as a by-product** of other work (no dedicated optimization sweep): e.g., use `pytest.mark.parametrize`, simplify redundant mocks/fixtures, remove unreachable branches, replace sleeps with time freezing.
* Offer suggestions as **optional** PR comments/notes; avoid churn unless the gain is **significant** (e.g., **> 20 % speedup**, major readability/flake reduction).

### 3.4 Definition of Done for tests

* **Deterministic:** no sleeps/time-races; use time freezing/monkeypatching.
  * Private event loops: close them with `tests.helpers.drain_loop`, which joins the default executor (a leftover `asyncio_*` thread fails Home Assistant's `verify_cleanup`), and wait for work that crosses an executor thread with `tests.helpers.run_loop_until` instead of a fixed `loop.run_until_complete(asyncio.sleep(...))`.
* **Isolated:** no live network; inject HA web sessions; mock external I/O at the boundary.
* **Readable:** clear arrange/act/assert, meaningful names, minimal fixture magic.
* **Value-dense:** each test protects a distinct behavior; avoid near-duplicates.
* **Fast:** prefer coordinator plumbing and targeted mocks over slow end-to-end paths.

### 3.5 Temporary coverage dips

If necessary changes reduce coverage below target, **open a follow-up issue** to restore it and reference it in the PR; do not allow repeated dips.

### 3.6 Entity-registry migration checklist (reload-centric fixes)

For any work that migrates entity-registry records during reload/startup flows, validate the following items before shipping:

1. **Device linkage** — assert that migrated entities reference the expected `device_id` and that identifier lookups exercise all candidate formats.
2. **Service exclusion** — include coverage showing service devices remain excluded (e.g., entries with `entry_type == SERVICE` are not used as targets).
3. **via_device integrity** — when helpers touch parent linkage, assert the fake registry preserves or updates `via_device` / `via_device_id` appropriately.
4. **Multi-entry safety** — demonstrate that entities belonging to other config entries stay untouched by the migration helper.
5. **Idempotence** — run the helper twice or on already-correct data to prove no extra registry updates occur.

### 3.7 Device registry healing checklist

1. **Trigger conditions.** Only heal when a registry lookup returns an existing `DeviceEntry` with an incorrect or missing `config_subentry_id`, name, or support flag. Read the latest object immediately before scheduling the update so debug logs can reference the authoritative identifiers. Use `dev_reg.async_get(device_id)` when you already hold the registry id, and `resolve_device_by_identifiers(dev_reg, candidates, entry_id=...)` from `custom_components/googlefindmy/coordinator/helpers/registry.py` when you only hold identifiers. **Superseded instruction, do not restore:** up to and including 1.7.15.14 this bullet named `dev_reg.async_get_device` and "list iteration". `async_get_device` is deprecated from Core 2026.9 (`breaks_in_ha_version="2027.8.0"`) and is already ambiguous from 2026.8 on, because identifiers are only unique *within* a config entry. "List iteration" meant iterating `dev_reg.devices` as a mapping, which is deprecated separately (`"2027.9.0"`); iterating the result of `async_entries_for_config_entry(...)` is not deprecated and stays the right way to walk this entry's devices. Reaching for the deprecated pair because "the healing checklist says so" is that superseded rule resurfacing; see `docs/AI_DEPRECATIONS_GUIDE.md`, section VI.
2. **Apply updates via `async_update_device`.** Do not rely on `async_get_or_create` to mutate existing devices. Instead, state the ownership as an intent (`plan_device_ownership(...)` + `execute_ownership_plan(...)` from `custom_components/googlefindmy/coordinator/helpers/registry.py`, see bullet 1) and pass only the non-ownership fields to `dev_reg.async_update_device(device.id, **extra_fields)`; `async_update_device` takes no `config_subentry_id` keyword on any supported core (2026.8.2 knows `add_`/`new_`/`remove_config_subentry_id` only, and those are exactly the keywords the intent layer exists to hide). Always capture the returned entry (the helper returns a *new* object). Treat the previous `device` reference as stale and overwrite it with the returned instance before continuing.
3. **Bookkeeping and metrics.** Count each successful heal exactly once—after `async_update_device` returns—and log the `(entry_id, device_id, identifiers, config_subentry_id)` tuple at debug level. Surface aggregate counters in telemetry or diagnostics helpers when applicable so regression tests can assert how many devices were corrected.
4. **`_heal_tracker_device_subentry` contract.** The helper returns a tuple `(device_entry, healed)` where `device_entry` is the freshest registry object (even if no heal occurred) and `healed` flags whether an update was applied. Always propagate the refreshed `device_entry` for downstream work (name updates, via-device clearing, identifier merges) and base counters/logs on the boolean flag so heals do not masquerade as extra creations.

---

## 4) **Token cache handling** (hard requirement; regression-prevention)

**WARNING: Incorrect token cache handling can lead to severe security vulnerabilities (cross-account data exposure). Strict adherence to these rules is mandatory.**

* **Single source of truth:** Keep an **entry-scoped** `TokenCache` (HA Store if applicable). No extra globals.
* **Pass explicitly:** Thread the `TokenCache` through every call chain (`__init__` → API/clients → coordinator → entities). **No implicit lookups** or module-level fallbacks.
* **Refresh strategy:**

  * Detect expiry proactively; refresh **synchronously on the calling path** or fail fast with a translated error.
  * No background refreshes that can race with requests.
  * On refresh failure, raise `ConfigEntryAuthFailed` to trigger reauth (avoid infinite loops).
* **Auditability:** All cache writes go through one adapter with structured debug logs (never secrets).
* **Removals are one-directional:** `entry.data` is the source of truth for credentials and the cache mirrors it, so a credential key that is **absent** from `entry.data` must be **dropped** from the cache, never recovered from it. Replacing credentials expresses "this account has no bundle / no AAS token any more" by leaving the key out (`const.OPTIONAL_CREDENTIAL_KEYS`, written by `config_flow._merge_credential_updates`), because Home Assistant's unique-id guard merges flat and cannot remove anything. A cache-first fallback that recovers such a key hands the integration back the credentials the user just replaced. The single exception is the migration gap the credential seed exists for: an entry that carries **no** credentials of its own, where the cache is read back in full. Applies to every writing surface (discovery overwrite, watched-file import, reauth, options login) and is enforced centrally in the credential seed of `__init__.async_setup_entry`, not per flow step. The same rule holds outside Home Assistant, where it has to be enforced per site: `main._clear_stale_tokens_for_reauth` snapshots the credentials `--reauth` removes and restores them only if the login stored none of `main._REAUTH_CHAIN_ANCHORS`. Restoring the derived values key by key beside a fresh anchor is the same one-directional violation in CLI clothing: `_ensure_aas_token` returns early on any cached `aas_token`, so the next run would never exchange the new token and would keep the previous account's vault keys (Codex review, PR #1261).
* **Tests:** Include regressions for stale tokens, cross-account bleed, and refresh races.

---

## 5) Security & privacy guards

* **Never log** tokens, email addresses, precise coordinates, raw API payloads, or class (c) identifiers in clear text. Device identifiers are graded by one question: *can somebody who holds only the log file use the value without owning this Home Assistant instance or this Google account?*
  * (a) **Rotating identifiers** (EID, the `request_uuid` this integration generates, the BLE MAC of an FMDN advertisement that resolved to one of the user's own trackers, which rotates with the EID, or once per 24 h while unwanted-tracking mode is active, `docs/FMDN.md` S3.5): allowed at any level, in full or truncated. The MAC of an advertisement that did not resolve belongs to somebody else's tracker until proven otherwise and is class (c); a caller-supplied value (the `stop_sound` service accepts a `request_uuid`) is graded by its content, not by the parameter name; the EID is truncated by convention (`EID_LOG_PREFIX_LENGTH` in `fmdn_finder/`: 8 hex chars of `eid_hex`, 8 bytes of the raw `eid`). Truncation does not turn a stable value into class (a): a prefix or suffix of a class (b) or (c) identifier keeps its class, and class (c) may only appear in the masked form defined under (c).
  * (b) **Registry identifiers of this instance** (`entry_id`, `config_subentry_id`, Home Assistant `device_id`) and the Google canonical ids of the user's own trackers: allowed at any level, they carry no meaning outside this instance and this account. User-provided device names are derived information (see "Redact rigorously" below). A record that can repeat unattended (polling, transport recovery, background sweeps, state-change listeners) keeps the name out of the record at INFO and above and carries it only at DEBUG; a count, an index or a class (a)/(b) identifier may stand in its place, none of them is required. The locate transport pins the Name@DEBUG half at WARNING as "Count@WARNING, Name@DEBUG" (`test_location_request_r6_name_sweep.py`), and `tests/test_guard_logging_entity_ids.py` pins the `entity_id` form package-wide at INFO and above. `tests/test_guard_unattended_device_names.py` pins the Name@DEBUG half for the unattended device-location and registry-heal paths (`api.async_get_device_location`, `ProtoDecoders/decoder.get_devices_with_location`, the subentry heal in `coordinator/registry._ensure_registry_for_devices`); inside these three functions it checks the shape of each logger call above DEBUG; its docstring names the call forms it reads as a closed list, and the blind spots of its name check. A record bound to one invocation (a manual locate, a button press, a service call) may name the device at any level, because the caller asked for that device by name and has to see which one failed. An automation can repeat an invocation, so a new invocation-bound record prefers the count-or-index form unless the name is what the operator has to act on; the name-carrying records above DEBUG that exist today, on the locate transport and on the poll path alike, are a documented exception, not a template, and are reduced as they are touched. A device name is the operator's own label inside the operator's own instance; the PII ban (section 1, "Behavioral safety") covers data about persons that the integration handles (e-mail addresses, account identifiers, coordinates), it does not turn every label into PII. A label that names a person other than the operator is class (c) once the log leaves the instance; the operator redacts it before attaching a log, because the integration cannot know which labels are personal. An `entity_id` is a slug of that name and follows the same rule.
  * (c) **Hardware addresses** (BLE or Wi-Fi MAC of scanners, proxies, phones), stable identifiers of third parties (foreign trackers, other accounts) and clear-text identifiers of anybody who is not the operator: never in clear text. The permitted masked forms are the masking helpers for e-mail and account values (`_mask_email_for_logs`, `_redact_account_for_log`) and, for addresses, the last four characters (`AA:BB:CC:DD:EE:FF` becomes `...E:FF`, `_mask_address_for_logs` in `fmdn_finder/location_uploader.py`); any longer part of the value is clear text.
  * When in doubt, treat a value as (c): a log file leaves the operator's control the moment it is attached to an issue, and a stable identifier is still valid years later, while a rotating one is stale before anybody reads it.
* **Diagnostics redaction:** use a central `TO_REDACT` list in `diagnostics.py`.
* **HTTP views & map tokens:** no secrets in URLs; server-side validation; short-lived, entry-scoped tokens.
* **Data minimization:** store only what is necessary (HA Store); document retention in README.
* **Network:** set timeouts; use backoff; fail closed on uncertainty.
* **Redact rigorously:** ensure not only direct secrets but also potentially identifying **derived information** (e.g., user-provided device names if sensitive, correlated external IDs) are redacted from diagnostics and from log records that can repeat unattended at INFO and above (the name at DEBUG only, see the graded rule above); a record bound to one invocation may name the device.

---

## 6) Expected **test types & coverage focus**

Prioritize a small but protective suite:

1. **Config flow** — user flow (success/invalid), duplicate abort (`async_set_unique_id` + `_abort_if_unique_id_configured`), connectivity pre-check, **reauth** (success/failure → reload on success), **reconfigure** step.
2. **Lifecycle** — `async_setup_entry`, `async_unload_entry`, **reload** (no zombie listeners; entities reattach cleanly).
3. **Coordinator & availability** — happy path; transient errors raise `UpdateFailed`; entities flip to `unavailable`; single “down/back” log.
4. **Diagnostics** — `diagnostics.py` returns data with strict **redaction** (no tokens/emails/locations; device, canonical and EID keys go through `TO_REDACT`, the instance's own `entry_id` stays, section 5 class (b)).
5. **Services** — success/error paths with localized messages; throttling/rate-limits where applicable.
6. **Discovery & dynamic devices** (if supported) — announcement, IP update, add/remove devices post-setup.
7. **Token cache** — expiry detection, refresh, failure propagation, no hidden fallbacks (§4).

---

## 7) Quality scale (practical, non-blocking)

* Maintain a **light** `custom_components/googlefindmy/quality_scale.yaml` to record **rule IDs** touched and a short **evidence** pointer (file+line / test name / PR link).
* If the file is missing, **do not block**—propose adding a minimal stub or open a follow-up task.
* Reviewers decide the final level when relevant; `hassfest` validates presence/schema in CI.

**Platinum hot-spots to double-check**

* Async dependency; injected web session; strict typing.
* Config flow (user/duplicate/reauth/reconfigure); unload/reload robustness.
* Diagnostics redaction; discovery/network info updates (if used).

---

## 8) Deprecation check (concise, per PR)

Add to the PR description:

* **Versions scanned:** current HA ± 2 releases.
* **Notes found:** 2–4 bullets with links to relevant release notes/developer docs (or “none found”).
* **Impact here:** “none”, or one-liners on code/tests/docs you adjusted.

*(When unsure, add a follow-up item; don’t block urgent fixes.)*

### Deprecation shim checklist (Home Assistant)

- [ ] `custom_components/googlefindmy/__init__.py` — `CONFIG_SCHEMA` retains
  the `config_entry_only_config_schema`/`no_yaml_config_schema` fallback chain,
  and `_apply_update_entry_fallback()` mirrors legacy `async_update_entry`
  keyword handling. Drop these once the minimum supported Core guarantees the
  modern helpers/signatures.
- [ ] `custom_components/googlefindmy/system_health.py` —
  `async_get_system_health_info()` still supports the pre-2024 signature of
  `ConfigEntries.async_entries()` that omitted the domain filter. Remove this
  guard when compatibility with that Core series is no longer required.

### Integration-owned deprecations (register)

Deprecations this integration announces to its users (attributes, options,
entities), as opposed to the Core deprecations above. Each row names the
constant that holds the removal version, the surface, the replacement, the
repair issue and the guard.

* The removal ships in the first integration release on or after the Home
  Assistant version in the constant.
* The constant may be raised (extended) with a reason in the PR description,
  never lowered.
* When the guard fails, remove the surface, the repair issue, the constant and
  the row in one PR, or extend the constant.

| Constant (`const.py`) | Surface | Replacement | Repair issue | Guard |
| --- | --- | --- | --- | --- |
| `TRACKER_ATTRS_BREAKS_IN_HA_VERSION = "2027.6.0"` | `device_tracker` attributes `location_age`, `location_status`, `coarse_*` (main tracker) | per-device sensors with the unique_id suffixes `_location_status`, `_last_seen`, `_coarse_fix` | `tracker_attributes_frozen_<entry_id>` | `tests/test_guard_integration_deprecations.py` |

---

## 9) Docs & i18n (minimal but strict)

* No hard-coded UI strings in Python.
* Translation hygiene (MUST) — keep `strings.json` and `translations/*.json` in sync:
  * [ ] Update **every** locale file in `translations/*.json` **and** `strings.json` whenever translation keys or formatted placeholders change.
  * [ ] Cross-check locale files for missing keys, placeholder mismatches, or obsolete entries before submitting the PR.
  * [ ] Run the documented helper command (if available) to compare locales; otherwise perform a manual diff review to confirm parity and note the method in the PR.
  * ⚠️ Hassfest rejects `config.menu` objects. When a flow shows a menu, translate the options under `config.step.<step_id>.menu_options` (or the matching `options.*`/`config_subentries.*` section) instead of adding a top-level `menu` block.
* Translate service and exception texts (`translation_key`). Every `UpdateFailed` in the package keeps its English message and passes `translation_domain=DOMAIN` and a literal `translation_key`: the integration card shows the key of the cause as setup-retry reason. Newer cores copy it there themselves; on older ones (2025.9.1 and 2026.1.3 measured) `_initial_refresh_not_ready` in `__init__.py` takes it from the cause, and only from a cause with `translation_domain=DOMAIN`. Placeholders are limited to `_ALLOWED_PLACEHOLDERS` (IDs, status, counts, the error class name) and never carry a device name. `tests/test_guard_update_failed_translation.py` enforces both; extend its allow list together with the template when a new placeholder is needed.
* Update README only when user-visible behavior/options change.
* Program output quoted verbatim in a guide is a claim like any other, and one message per code path is not the same as one message for several: `docker-login/README.md` once printed the closed-window line for the timeout path too, so a user diagnosing a timeout was told to look for words that path cannot emit. Guard such a quote by rendering it through the real code path and locating it in the fenced block that directly follows its own lead-in sentence; a "the text appears somewhere in the file" check passes on a guide that files two messages under one heading, and a "somewhere after the lead-in" check passes when the message sits under a later one. Extent today, stated so it is not mistaken for coverage: three such guards exist. `tests/test_auth_flow.py::test_the_docker_login_guide_quotes_each_abort_message` covers the two `LoginAborted` messages under `## Cancelling a login`, each located in the fenced block that follows its own lead-in. `tests/test_auth_flow.py::test_the_guide_describes_the_driver_timeout_path_it_can_actually_produce` covers the driver-timeout bullet in `## Troubleshooting`, where the quotation is inline rather than fenced: it renders the failure through the flow, asserts the guide names the type that came out, and takes the denied exit status from `main._EXIT_LOGIN_ABORTED` instead of from the prose. Its reach stops at `_run_oauth_flow_or_exit`; a blanket handler further out in `main._main` could still falsify the bullet without turning it red. Every other quotation of program output in that guide, fenced or inline, is unguarded. `tests/test_main_cli_exit_paths.py::test_the_guide_quotes_each_reauth_restore_message` covers two of the four messages `main._restore_cleared_tokens` can print (the successful and the declined restore), in the `--reauth` paragraph of the same `## Cancelling a login` section: it renders each one through the real function (both interpolate — a token count, the anchor name) and locates it in the fenced block after its own lead-in. Its other two messages -- the unreadable file and the failed write -- stay unguarded, because the guide paraphrases them rather than quoting them. `tests/test_docker_login_hardening.py` couples README prose to code as well, but by asserting phrases, not by rendering messages. Extend the guard when you quote a new message there.
* A setting the user can switch off needs an off switch **every supported launcher can type**, and `cmd.exe` cannot type an empty one: `set VAR=` deletes the name there, so bash's empty-but-present state has no Windows equivalent and `if defined` is the counterpart of `${VAR+set}`, not of `${VAR:-}`. `GFMY_LOCALE` therefore takes the POSIX `C`/`POSIX` locale as a stated absence of a preference and drops it without the warning a typo earns, which gives `login.sh`, `login.cmd` and a bare `docker compose run` one spelling in common. Reading a shell locale with `:-` cannot tell "said nothing" from "said no preference" and silently overwrites the second; use `+set` wherever an empty value is a documented answer. The rule lives twice on purpose, as a `case` arm in `login.sh` (host locale) and as `_NO_LOCALE_PATTERN` in `chrome_driver.py` (explicit value); extend both together. Extent today, stated so it is not mistaken for coverage: `tests/test_docker_login_hardening.py::test_both_halves_agree_on_what_counts_as_no_preference` pins the pair over twelve values, six POSIX spellings and six real locales, and nothing outside that set. What carries the container side is the ABSENCE OF A WARNING; the `None` is asserted too but proves nothing on its own: `C` and `POSIX` are no language tags either way, so a `None` check is satisfied by the warning branch too and would not notice the rule being deleted.
<a id="rule-9doc-canonical"></a>
* **Rule §9.DOC (canonical):** Keep documentation and docstrings accurate for existing features by correcting errors and augmenting missing details without shortening or deleting content. When functionality is intentionally removed or deprecated, remove or reduce the corresponding documentation to reflect that change while preserving historical clarity.
  * **Decision algorithm:** IF functionality is intentionally removed/deprecated → update or remove the related documentation to match the removal (include deprecation context as needed); ELSE → correct/augment the documentation without shortening it.
* Documentation-only updates must still record any prerequisite bootstraps performed during the edit (for example, `python -m pip install --upgrade homeassistant`) so reviewers can trace required environment steps alongside the content changes.

---

## 10) Local commands (VERIFY)

* `pre-commit run --all-files`
* `ruff check`
* `pytest -q`
* `mypy --strict --install-types --non-interactive custom_components/googlefindmy tests`

  > **Non-interactive flag REQUIRED.** Running mypy with
  > `--install-types --non-interactive` pre-approves the stub installations strict
  > mode requires, eliminating interactive prompts during local or CI runs. When
  > invoking mypy against a subset of files, append
  > `--install-types --non-interactive` as well (for example,
  > `mypy path/to/file.py --install-types --non-interactive`). The CI mypy job always
  > passes these flags (`.github/workflows/ci.yml`) so strict runs never stall on an
  > interactive stub-install prompt; no separate step scans the logs for the flag.

> **Hassfest runs in CI.** The `.github/workflows/hassfest-auto-fix.yml` workflow
> validates manifests on every PR, auto-committing any key
> ordering fixes (the blocking manifest gate is the `hassfest` job in `ci.yml`).
> Review the workflow output instead of attempting a local run; when you need a
> fresh validation, re-run the job from the PR UI (the workflow has no
> `workflow_dispatch` trigger).

### 10.1 Type-checking policy — mypy strict on edited Python files

**Applicability:** Only the Python files touched by the current edit/PR (generated
artifacts remain exempt when explicitly flagged by repo configuration).

1. **Identify Python paths.** Use `git diff --name-only --relative -- '*.py'`
   against `HEAD` and narrow the list to files actually modified in this edit. If
   Git metadata is unavailable, derive the list directly from the edit context.
2. **Respect local configuration.** When `pyproject.toml`, `mypy.ini`,
   `.mypy.ini`, or `setup.cfg` exists, invoke `mypy` without extra overrides so the
   repository configuration remains authoritative.
   *Fresh environment reminder:* Run
   `mypy --install-types --non-interactive --strict` once before the checks below
   to auto-install required stub packages without interactive prompts.
3. **Run the strict check.** Execute `mypy -q <changed-files>` when a
   configuration file is present; otherwise run `mypy -q --strict <changed-files>`.
   Install/upgrade mypy locally (`python -m pip install --upgrade mypy`) if it is
   missing.
   *Practical note:* invoking `mypy --strict` at the repository root can trip over
   namespace-package collisions between the integration and the `tests/`
   package. Prefer the package-targeted invocation
   `mypy --strict --install-types --non-interactive custom_components/googlefindmy tests`
   so the strict run completes reliably in CI and local shells.
4. **Resolve every diagnostic.** Add precise type annotations, tighten `Optional`
   handling, and avoid blanket `Any`. Use `# type: ignore[...]` only when the
   specific error code cannot be eliminated and document the rationale nearby.
5. **Re-run until clean.** Repeat the command in step 3 until mypy exits with code
   0 for all edited files.

**Acceptance criteria:**

* Clean mypy run (exit code 0) covering every touched Python file.
* No new broad or unscoped `# type: ignore` directives.
* Repository-level mypy configuration is honored; fallback strict mode is used
  only when no configuration file exists.

---

## 11) Clean & Secure Coding Standard (Python 3.13 + Home Assistant 2025.10)

> **Two separate axes, do not conflate them.** The *language level* this code is
> written against is Python 3.13, enforced by `ruff target-version = "py313"` and
> `mypy python_version = "3.13"`; the integration has to keep running on Home
> Assistant Core 2025.9.1, which ships 3.13. The *development and test
> environment* resolves for Python 3.14.2 and above, because Core requires that
> from 2026.3.0 onwards and pins its dependencies exactly, so a 3.13 resolution
> would permanently hold nine packages at vulnerable versions. The 3.13 runtime
> axis is kept under test by the lock-free `test_legacy_python` CI job. Raising
> the language level is therefore a separate decision from the lock constraint,
> and neither implies the other.

### 11.1 Language & style (self-documenting)

#### Logging formatting (ruff G004)

* **Default:** Use lazy logging with `%`-style placeholders so expensive formatting is deferred until the log level is enabled.
* **Permitted `# noqa: G004` usage:** Only on `_LOGGER.debug(...)` calls where the f-string formats existing variables without invoking additional functions.
* **Prohibited:** Never suppress `G004` for `_LOGGER.info()`, `_LOGGER.warning()`, `_LOGGER.error()`, or `_LOGGER.critical()`; these levels must always use lazy formatting.

* **PEP 8/PEP 257 mandatory.** Consistent formatting, clear docstrings; meaningful names.
* **Historic phase markers stay English.** When preserving multi-phase progress notes (for example, `Phase 1`, `Phase 2`, legacy cleanup steps), keep both the marker and its description in English so maintenance cycles do not reintroduce untranslated fragments.
* **Typing is strict.** Use Python typing everywhere; prefer **PEP 695** generics where helpful.
* **Exceptions.** Raise precise types; use **`raise … from …`** to preserve causal chains; avoid broad `except:`; never swallow errors silently.
* **Docstrings.** Every public function/class has an English docstring (purpose first, then Args/Returns/Raises, short example) and follows **Rule §9.DOC**.
* **File header path line (REQUIRED).** Every Python file must begin with a single-line comment containing its repository path. Example first line: `# custom_components/googlefindmy/binary_sensor.py`

### 11.2 Security baseline (OWASP / NIST / BSI)

**Input validation & injection**

* **Never** use `eval`/`exec`. For literals, use `ast.literal_eval`. The only exception is for tests and is narrow: section "`eval` and `exec` in tests" of [`tests/AGENTS.md`](tests/AGENTS.md) lists every allowed call site, and `tests/test_guard_eval_exec_sites.py` pins that list.
* Subprocess: **no `shell=True`** for untrusted data; pass argv lists; use `shlex.quote` if you must touch shell.
* Use parameterized queries for SQL/LDAP/XML; sanitize file names and paths.

**(De)serialization**

* **Do not** use `pickle`/`marshal`/`yaml.load` on untrusted data; prefer JSON or `yaml.safe_load`.
* On archive extraction (`tarfile`/`zipfile`), normalize and validate target paths to prevent traversal.

**Cryptography & secrets**

* Use the `secrets` module for tokens/keys; never `random` for security.
* Follow **BSI TR-02102-1** for algorithms and key sizes; prefer library defaults that meet these constraints.

**Logging & privacy**

* **Redact** tokens, PII, coordinates, and class (c) identifiers (section 5); class (b) identifiers may appear at any level.
* Use a central redaction list in diagnostics; keep logs actionable yet non-sensitive.

**Supply chain**

* The **entire runtime stack** uses lower-bound (`>=`) floors rather than exact
  pins or `--require-hashes`: every entry in
  `custom_components/googlefindmy/requirements.txt` and every `manifest.json`
  requirement uses a `>=` floor (aiohttp, cryptography, protobuf, gpsoauth and the
  rest, alongside Selenium and undetected-chromedriver). What is **scoped to the
  browser-facing** packages (undetected-chromedriver, Selenium and their
  transitive stack) is the **Chrome-currency rationale**: those must track the
  current Chrome/ChromeDriver upstream by design and therefore must not be
  exact-pinned; the remaining runtime packages simply follow the same floor
  convention (upgrade-friendly, not hash-locked).
* **Most test and tooling** dependencies use the same lower-bound (`>=`) ranges as
  the runtime stack (for example `bandit>=1.7`, `mypy>=1.11`, `pytest>=8.3` and
  `ruff>=0.14.1` in `custom_components/googlefindmy/requirements-dev.txt`, and the
  Poetry `dev`/`test` groups in `pyproject.toml`), so they are **not** reproducibly
  pinned either. Only a small **explicitly constrained** subset is exact-pinned and
  must not be relaxed under the Chrome-currency rationale:
  `custom_components/googlefindmy/requirements-dev.txt` pins `pytest-asyncio==1.4.0`,
  and `custom_components/googlefindmy/constraints-test-stubs.txt` pins
  `homeassistant==2026.10.3` and `pytest-homeassistant-custom-component==0.13.374`
  (the `poetry.lock` versions; bump both together, since each plugin release
  requires one exact Home Assistant release). The `test_legacy_python` job pins
  its two Python 3.13 axes exactly (`homeassistant==2025.9.1` with plugin
  `0.13.278`, and `homeassistant==2026.2.3` with plugin `0.13.316`) and adds the
  cap `pycares<5` for the declared-floor axis only (an upper bound, not an exact
  pin; the runtime floor `pycares>=4.4.0` lives in `requirements-dev.txt`).
* Actual coverage today (each claim cites the workflow proving its *effective*
  behaviour):
  * **PR CI passes an approval gate.** In
    `.github/workflows/ci.yml` the first job, `approval_gate`, runs only on
    `pull_request` and is bound to the environment `home-assistant-tests`;
    `hassfest` needs it, and every other job needs `hassfest`. Where
    `home-assistant-tests` has a required reviewer (in
    `jleinenbach/GoogleFindMy-HA`), that reviewer must approve the run before
    any job starts; where it has no protection rules (GitHub creates a missing
    environment without rules on the first run that references it), the gate
    passes without waiting for approval. Pushes to `main` skip the gate and
    run all jobs (each job states an explicit `if:` so the skipped
    gate does not skip them). A run that is not approved, is rejected or
    expires never turns `CI Success` green: `CI Success` depends on the gate
    and fails a pull request run whose gate result is not `success`. Workflow
    `concurrency` keeps one live `CI` run per pull request; a push that starts
    a new run cancels the previous one (a push that starts no run, such as a
    `[skip ci]` commit, leaves the waiting run in place, so approve only the
    run for the current head). The gate is only as strong as the environment: GitHub
    creates a missing environment without protection rules when a job
    references it, so the approval is enforced only while
    `gh api repos/<owner>/<repo>/environments/home-assistant-tests --jq '.protection_rules[].type'`
    prints `required_reviewers` for that repository. Name it explicitly:
    without `GH_REPO`, gh resolves the `{owner}/{repo}` placeholders from the
    checkout's git remotes, and in a fork checkout with an `upstream` remote
    they resolve to the upstream repository. The gate is a load control, not a security
    boundary, and the guarantees above hold only for the unmodified
    `ci.yml`: a `pull_request` run uses the `ci.yml` of the pull request's
    merge commit, so a pull request that edits or removes the gate job (or
    the guard test, which itself only runs once the gate has passed) runs `CI` without
    approval. This is accepted. Do not move the gate to `pull_request_target`
    or `workflow_run`: those run in the base repository's context, where the
    token can be write-scoped and repository secrets are available, and the
    test jobs would check out the pull request's code there. The other pull
    request workflows are not gated: CodeQL, Semgrep (SAST), pip-audit,
    hassfest (auto-fix), Semantic PR and the Home Assistant Dependency
    Compatibility Check
    (`.github/workflows/ha-dependency-check.yml`, which installs
    `homeassistant` on every pull request). `tests/test_ci_approval_gate.py`
    locks the shape of `ci.yml`.
    `tests/test_guard_workflow_environments.py` pins every `environment:` in
    `.github/workflows/` to an English allowlist; a new name is added there
    deliberately, with a reason. Gate, guard tests and this bullet are
    repository-neutral and may go upstream: where `home-assistant-tests` has no
    protection rules, the gate passes without approval, and nothing has to be
    configured there.
  * **`pip-audit` on PRs is report-only.** `.github/workflows/pip-audit.yml`
    guards the PR job with `if: github.event_name == 'pull_request'` and runs it
    as "Audit requirements (report-only; keep CI green)", failing only on a
    pip-audit *tool* error (`exit "$status"`), never on a discovered advisory:
    findings surface as job-summary warnings, they do not fail the PR.
  * **A weekly `pip-audit` auto-fix job** (`schedule: cron '23 3 * * 2'`, job
    guarded by `if: github.event_name != 'pull_request'`) opens automated
    security-update PRs via `peter-evans/create-pull-request` for fixable
    advisories.
  * **Semgrep SAST runs on PRs only.** `.github/workflows/semgrep.yml` triggers
    on `pull_request` alone and scans the PR head against its base commit
    (`--baseline-commit`). Semgrep resolves the baseline to
    `git merge-base <base> HEAD`, which on the `pull_request` merge ref equals
    `base.sha`, so commits that reached the base branch after the PR branched
    are not attributed to the PR. The full-tree scan of `main` is CodeQL's job
    (next bullet). Its gate step fails the job on any new finding of severity
    `ERROR`, `HIGH` or `CRITICAL` (both Semgrep severity scales); `WARNING`,
    `MEDIUM` and below are uploaded to the Security tab but do not fail the job.
    The gate only counts what the scan produced: a scan that itself fails
    (Semgrep exit code 2, e.g. registry unreachable) uploads an empty artifact
    with a `::notice` and leaves the job green. The workflow writes its own
    `.semgrepignore` at run time (a committed one would be overwritten); it
    excludes `custom_components/googlefindmy/vendor/leaflet/`, an unmodified
    third-party copy pinned by the hashes in its `VERSION` file.
    `vendor/openlocationcode` is a modified extract and stays in the scan.
  * **CodeQL scans the full tree.** `.github/workflows/codeql.yml` (advanced
    setup) analyzes `python` and `actions` on the merge ref of every
    `pull_request`, on every `push` to `main`, and on a weekly `schedule`;
    an inline `paths-ignore` excludes generated `*_pb2*.py` modules and
    `custom_components/googlefindmy/vendor/`. Results upload under the SARIF
    categories `/language:python` and `/language:actions`. The pass/fail
    verdict of the PR check is not decided by the workflow file: it comes from
    the repository's code scanning setting "check failure" (newly introduced
    alerts of error level or high/critical security severity). Merging into
    `main` IS blocked on CodeQL: the repository ruleset on `main` (`gh api
    repos/<owner>/<repo>/rules/branches/main`) carries a `code_scanning` rule
    for the tool `CodeQL` (`security_alerts_threshold: high_or_higher`,
    `alerts_threshold: errors`), which blocks the merge while the analysis is
    pending, when no CodeQL analysis exists for the merge ref, or when the PR
    introduces alerts at or above those thresholds (repository admins can
    bypass via the PR bypass path, like every rule of that ruleset). Measured
    on 2026-09-17 with probe PR #1299 (two new CodeQL alerts, `error`/`high`;
    `CI Success` green; merge state `BLOCKED`) against control PR #1300 (no
    new alerts; `CLEAN`).
  * **Not every change is human-reviewed.** `.github/workflows/release-stamp.yml`
    can push a version stamp directly to the owning branch (or, when branch
    rules reject the direct push, step "Resolve the owning branch and push the
    stamp" opens a stamp PR that a maintainer merges by hand; no review
    requirement), and
    `.github/workflows/hassfest-auto-fix.yml` commits manifest key-sorts via
    `stefanzweifel/git-auto-commit-action`. Human review is the norm for feature
    PRs, not a guarantee on every commit. Known limit: when
    `hassfest-auto-fix.yml` pushes a sort commit onto a PR head with
    `GITHUB_TOKEN`, GitHub creates the `pull_request` runs for that commit "in an
    approval-required state" (docs: actions/concepts/security/github_token), so
    `CI Success` is missing until a maintainer approves the runs in the Actions
    UI or the author pushes again.
  * **A narrow manifest CVE gate does block PRs.** The `test` job
    (`.github/workflows/ci.yml`, `poetry run pytest`), aggregated into the
    required `CI Success` check (job `ci-success`, `needs:` all CI jobs;
    required by the repository ruleset on `main`), runs
    `tests/test_pip_audit_security.py::TestManifestOnlyPipAuditGate::test_no_fixable_integration_owned_vulnerability`,
    which fails the PR when `script/audit_manifest.py` finds an actionable,
    fixable, integration-owned manifest or transitive-dependency vulnerability
    (HA-governed and unfixable advisories do not block). What is **absent** is a
    **broad, full-dependency-tree CVE scan** and any **SBOM scan** blocking a PR.
* Hardening targets (not yet implemented): generate a CycloneDX **SBOM** and scan
  it (e.g., Dependency-Track); fail CI on known critical vulnerabilities.
* **Control claims must be grounded (no aspirational controls), in every
  direction.** Grounding is symmetric across three claim polarities, because a
  claim can drift from reality by asserting too much, too little, or the wrong
  extent:
  * **Positive** ("control X *exists* / *blocks* / *runs daily* / *fails the
    PR*"): cite the workflow file and the line proving its *effective*
    behaviour, not its declared intent.
  * **Negative** ("there is **no** X", "not enforced", "does not block"): before
    asserting an absence, search for the counterexample that would falsify it,
    and treat a **gate embedded in the `test` job behind the required
    `CI Success` check** (a pytest test that fails the PR) as a real gate even
    when no dedicated workflow exists. A false "no gate" is the same drift as
    a false "gate exists".
  * **Scope / quantifier** ("scoped to X", "**only** X", "**every** / **all**
    X"): enumerate the full set (e.g. grep every entry in `requirements.txt`,
    not just the named packages) and check the stated boundary against it. State
    the mechanism's true extent separately from any narrower *rationale*.
  Two recurring traps for positive claims: (a) `report-only` /
  `continue-on-error` / "keep CI green" is a report, not a blocking gate; (b) a
  declared `schedule:` / `push:` trigger whose only job is guarded by
  `if: github.event_name == 'pull_request'` gives no scheduled or push coverage.
  Describe what runs, not what is aspired to; if a control is only planned, list
  it under "Hardening targets", never as current coverage.

### 11.3 Async, concurrency & cancellation

* **Async-first**: no event-loop blocking. In new code that has a `hass` in reach, prefer `await hass.async_add_executor_job(...)`: it registers the job in the caller's task bucket, so shutdown waits for it only when the caller is itself a tracked task: the job lands in `hass._tasks` only if `asyncio.current_task()` is in `hass._tasks`, otherwise in `hass._background_tasks`, which Home Assistant cancels on stop instead of awaiting (`homeassistant/core.py`, 2026.9.3, `async_add_executor_job` and `async_stop`). A job started from the poll cycle is such a case, because the cycle runs as a background task (`coordinator/polling.py`). The thread pool is the same either way (Home Assistant installs it as the loop's default executor), so `asyncio.to_thread` stays correct where no `hass` is available, which is the case for the crypto and token paths that use it today. This mirrors the remediation in [`docs/AI_MAINTENANCE_TASKLIST.md`](docs/AI_MAINTENANCE_TASKLIST.md) § 4.
* Use **`asyncio.TaskGroup`** for structured concurrency where suitable.
* Cancel correctly (`task.cancel(); await task`) and handle `CancelledError`. Use `asyncio.shield` only for small critical sections.

### 11.4 File system & I/O (safe & gentle)

* Use `pathlib`; validate roots; prefer atomic writes (temp → `replace`).
* Batch/dedupe writes via coalescing; avoid chatty flush patterns.
* Cache pure computations with `functools.lru_cache`; define invalidation/TTL strategy where relevant.

### 11.5 Guard catalog & error messages

* **Existence/type/range** guards before access (`is None`, `isinstance`, length/bounds).
* **Path guards** (`Path.resolve()`, `is_relative_to`) to prevent traversal.
* **Network guards**: sane timeouts, retry with backoff/jitter, TLS verification enabled.
* **Deserialization guards**: format allow-list, schemas, safe loaders.
* **Error messages**: specific cause + actionable hint; no vague “failed”.
* **Runtime payload invariants**: crypto helpers (`_ensure_bytes`, `WrappedLocation`) must enforce decrypted payloads remain `bytes`; coerce `bytearray` and drop non-byte responses before instantiating wrappers so downstream protobuf parsing stays type-safe.

### 11.6 Performance without feature loss

* Avoid busy-waiting; use backoff/jitter; coalesce duplicate work.
* Prefer **coordinator-based** data fetch (one fetch per resource/account per tick).
* Stream large I/O; avoid unnecessary (de)serialization; minimize filesystem churn.

### 11.7 Home Assistant specifics (must-haves)

* Network: **inject** the web session (`async_get_clientsession(hass)`); do not create ad-hoc sessions.
* Instance URL: use `homeassistant.helpers.network.get_url(hass, …)`.
* Data: centralize periodic fetch in a **DataUpdateCoordinator**; push > poll when available.
* Config flow: **test before configure**; localized errors; duplicate-account abort; reauth & reconfigure paths.
* Config subentries: `ConfigFlow.async_get_supported_subentry_types` must keep the `ServiceSubentryFlowHandler` and `TrackerSubentryFlowHandler` buckets aligned with the service/tracker feature constant sets so UI actions remain accurate.
* Config subentries (stub compatibility): `custom_components/googlefindmy/config_flow.py` ships a fallback `ConfigSubentryFlow` stub whose `async_update_and_abort(*, data, ...)` signature intentionally omits the upstream `(entry, subentry, *, data=...)` parameters; revisit when the core API evolves to avoid diverging behavior.
* Coordinator metadata probes: coordinator helpers that call `_refresh_subentry_index(...)` during early setup must pass `skip_manager_update=True` so Home Assistant stubs stay untouched while feature visibility is indexed. This avoids mutating the stubbed `ConfigEntrySubEntryManager` before the real manager is bound and keeps the visibility tests deterministic.
* Repairs/Diagnostics: provide both; redact aggressively.
* Storage: use `helpers.storage.Store` for tokens/state; throttle writes (batch/merge).
* System health: prefer the `SystemHealthRegistration` helper (`homeassistant.components.system_health.SystemHealthRegistration`) when available and keep the legacy component import only as a guarded fallback.
* **Entity naming** (HA Best Practice, ref: [Adopting a new way to name entities](https://developers.home-assistant.io/blog/2022/07/10/entity_naming/)):
  - Always set `_attr_has_entity_name = True` on entity classes.
  - **Primary entity** (represents the device itself): set `_attr_name = None` so it inherits only the device name (e.g., "Galaxy S25 Ultra").
  - **Secondary entities** (additional features): use `translation_key` with a `name` in translations; HA auto-composes the friendly name as "Device Name + Translation" (e.g., "Galaxy S25 Ultra Last location").
  - **Translation files**: for the primary entity's `translation_key`, **omit** the `"name"` key entirely (presence of `"name"` would append a suffix); for secondary entities, **include** the `"name"` key with the suffix text.
  - Never set `_attr_name` dynamically at runtime (e.g., in coordinator update callbacks) when using `has_entity_name=True`—the device registry is the single source of truth for the device name.
  - **CRITICAL: `_attr_name = None` vs. attribute not set** — These behave differently with `has_entity_name=True`:
    - `_attr_name = None` (explicitly set) → entity inherits **only** the device name, no suffix
    - `_attr_name` **not set** (attribute doesn't exist) → name comes from `translation_key`
    - If a parent class sets `_attr_name = None` in `__init__()` and a child class needs the translation-based name, the child must **delete** the attribute after `super().__init__()`: `del self._attr_name`

### 11.8 Release & operations

* CI **security gate**: lint/type/tests must pass and roll up into `CI Success`, a required status check of the `main` ruleset (together with the `code_scanning` rule for CodeQL); the `test` job enforces a **narrow** manifest CVE gate (the `test_no_fixable_integration_owned_vulnerability` pytest gate over `audit_manifest`), while the separate `pip-audit` workflow runs report-only; a broad full-tree CVE scan and an SBOM scan remain hardening targets, not yet enforced.
* Logs are **incident-ready** but privacy-preserving (use OWASP vocabulary).
* All doc updates comply with **Rule §9.DOC**.

### 11.9 Machine-checkable acceptance checklist (for the agent)

* [ ] PEP 8/257 compliance; complete docstrings maintained per **Rule §9.DOC**.
* [ ] Strict typing incl. PEP 695 where relevant; no implicit `Any` in public APIs.
* [ ] No `eval/exec` (tests: only the call sites listed in `tests/AGENTS.md`); subprocess without `shell=True`; parameterized I/O; safe loaders.
* [ ] Archive extraction is traversal-safe; paths validated with `pathlib`.
* [ ] `secrets` used for tokens; cryptography aligns with BSI TR-02102-1 guidance.
* [ ] Logs redact tokens, PII, coordinates and class (c) identifiers (section 5) at every level, and derived identifiers (user-provided names, `entity_id`) from records that can repeat unattended at INFO and above; diagnostics redact device, canonical and EID keys through `TO_REDACT` (the instance's own `entry_id` is class (b) and stays).
* [ ] The **whole runtime stack** uses `>=` floors (the Chrome/ChromeDriver-currency rationale is what is scoped to the browser packages, not hard pins across the stack); **most test/tooling** deps also use `>=` floors, only a constrained subset is exact-pinned (`pytest-asyncio==1.4.0`, `constraints-test-stubs.txt`); the `test` job (behind the required `CI Success` check of the `main` ruleset) enforces a **narrow** manifest CVE gate (`test_no_fixable_integration_owned_vulnerability`), the separate `pip-audit` workflow runs report-only on PRs + weekly auto-update PRs; Semgrep SAST runs on PRs only (workflow triggers on `pull_request` alone, baseline scan against the PR base, gate fails on new `ERROR`/`HIGH`/`CRITICAL` findings); CodeQL scans the full tree (PR merge ref, push to `main`, weekly) and blocks merges into `main` via the ruleset's `code_scanning` rule; not every change is human-reviewed (release-stamp/hassfest-auto-fix auto-commit); a broad full-tree CVE scan and an SBOM scan remain hardening targets.
* [ ] Async: no loop blockers; `to_thread`/`TaskGroup`; proper cancel handling.
* [ ] I/O optimized (batch/atomic); caches with clear TTL/invalidations.
* [ ] HA-specific: Coordinator, injected session, `get_url`, config-flow test, Repairs/Diagnostics, HA Store.
* [ ] Tests: cover happy/edge/error paths; regressions for fixes; deterministic/time-safe.
* [ ] Local verify commands passed (`pre-commit run --all-files`, `ruff check`, `pytest -q`, `mypy --strict --install-types --non-interactive custom_components/googlefindmy tests`); hassfest continues to run in CI.

---

## REFERENCES

### 1) Python 3.13 — Language, Style, Typing, Safety

* PEP 8 – Style Guide: [https://peps.python.org/pep-0008/](https://peps.python.org/pep-0008/)
* PEP 257 – Docstring Conventions: [https://peps.python.org/pep-0257/](https://peps.python.org/pep-0257/)
* PEP 695 – Type Parameter Syntax (Generics): [https://peps.python.org/pep-0695/](https://peps.python.org/pep-0695/)
* What’s New in Python 3.13: [https://docs.python.org/3/whatsnew/3.13.html](https://docs.python.org/3/whatsnew/3.13.html)
* Exceptions & `raise … from …` (tutorial): [https://docs.python.org/3/tutorial/errors.html](https://docs.python.org/3/tutorial/errors.html)
* `asyncio.TaskGroup` (structured concurrency): [https://docs.python.org/3/library/asyncio-task.html#taskgroups](https://docs.python.org/3/library/asyncio-task.html#taskgroups)
* `subprocess` — security considerations / avoid `shell=True`: [https://docs.python.org/3/library/subprocess.html#security-considerations](https://docs.python.org/3/library/subprocess.html#security-considerations)
* Shell escaping via `shlex.quote`: [https://docs.python.org/3/library/shlex.html#shlex.quote](https://docs.python.org/3/library/shlex.html#shlex.quote)
* `pickle` — security limitations (avoid for untrusted data): [https://docs.python.org/3/library/pickle.html#security-limitations](https://docs.python.org/3/library/pickle.html#security-limitations)
* Safe literal parsing via `ast.literal_eval`: [https://docs.python.org/3/library/ast.html#ast.literal_eval](https://docs.python.org/3/library/ast.html#ast.literal_eval)
* `tarfile` — extraction & path traversal note: [https://docs.python.org/3/library/tarfile.html#tarfile.TarFile.extractall](https://docs.python.org/3/library/tarfile.html#tarfile.TarFile.extractall)
* `zipfile` — untrusted archives & traversal note: [https://docs.python.org/3/library/zipfile.html#zipfile-objects](https://docs.python.org/3/library/zipfile.html#zipfile-objects)

### 2) Home Assistant (Developer Docs, 2024–2025)

* Fetching data (DataUpdateCoordinator): [https://developers.home-assistant.io/docs/integration_fetching_data/](https://developers.home-assistant.io/docs/integration_fetching_data/)
* Inject web session (`async_get_clientsession`/httpx): [https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/inject-websession/](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/inject-websession/)
* Test connection before configure (Config Flow): [https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/test-before-configure/](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/test-before-configure/)
* Blocking operations (keep event loop clean): [https://developers.home-assistant.io/docs/asyncio_blocking_operations](https://developers.home-assistant.io/docs/asyncio_blocking_operations)
* Appropriate polling intervals: [https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/appropriate-polling/](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/appropriate-polling/)
* Entity unavailable on errors: [https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/entity-unavailable/](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/entity-unavailable/)
* Integration setup failures & reauth: [https://developers.home-assistant.io/docs/integration_setup_failures/](https://developers.home-assistant.io/docs/integration_setup_failures/)
* Integration file structure (coordinator.py, entity.py, …): [https://developers.home-assistant.io/docs/creating_integration_file_structure/](https://developers.home-assistant.io/docs/creating_integration_file_structure/)
* Diagnostics (redact sensitive data): [https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/diagnostics/](https://developers.home-assistant.io/docs/core/integration-quality-scale/rules/diagnostics/)
* Integration diagnostics (`async_redact_data`): [https://developers.home-assistant.io/docs/core/integration_diagnostics/](https://developers.home-assistant.io/docs/core/integration_diagnostics/)
* Repairs platform (issue registry & flows): [https://developers.home-assistant.io/docs/core/platform/repairs/](https://developers.home-assistant.io/docs/core/platform/repairs/)
  * **Reminder for contributors:** Home Assistant's issue registry helper names use the `async_*` prefix even though they are synchronous callbacks. Call `issue_registry.async_get(hass)` and the returned registry's `async_*` methods without `await` in both production code and tests.
* Repairs (user docs): [https://www.home-assistant.io/integrations/repairs/](https://www.home-assistant.io/integrations/repairs/)
* Secrets (`!secret`): [https://www.home-assistant.io/docs/configuration/secrets/](https://www.home-assistant.io/docs/configuration/secrets/)

### 3) Secure Development — Standards & Guidance

* NIST SP 800-218 — Secure Software Development Framework (SSDF): [https://nvlpubs.nist.gov/nistpubs/specialpublications/nist.sp.800-218.pdf](https://nvlpubs.nist.gov/nistpubs/specialpublications/nist.sp.800-218.pdf)
* BSI TR-02102-1 — Cryptographic mechanisms & key lengths: [https://www.bsi.bund.de/SharedDocs/Downloads/DE/BSI/Publikationen/TechnischeRichtlinien/TR02102/BSI-TR-02102-1.pdf](https://www.bsi.bund.de/SharedDocs/Downloads/DE/BSI/Publikationen/TechnischeRichtlinien/TR02102/BSI-TR-02102-1.pdf)

#### OWASP Cheat Sheet Series (selected)

* Injection Prevention Cheat Sheet: [https://cheatsheetseries.owasp.org/cheatsheets/Injection_Prevention_Cheat_Sheet.html](https://cheatsheetseries.owasp.org/cheatsheets/Injection_Prevention_Cheat_Sheet.html)
* OS Command Injection Defense: [https://cheatsheetseries.owasp.org/cheatsheets/OS_Command_Injection_Defense_Cheat_Sheet.html](https://cheatsheetseries.owasp.org/cheatsheets/OS_Command_Injection_Defense_Cheat_Sheet.html)
* Deserialization Cheat Sheet: [https://cheatsheetseries.owasp.org/cheatsheets/Deserialization_Cheat_Sheet.html](https://cheatsheetseries.owasp.org/cheatsheets/Deserialization_Cheat_Sheet.html)
* Logging Cheat Sheet: [https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html)
* Application Logging Vocabulary: [https://cheatsheetseries.owasp.org/cheatsheets/Logging_Vocabulary_Cheat_Sheet.html](https://cheatsheetseries.owasp.org/cheatsheets/Logging_Vocabulary_Cheat_Sheet.html)
* OWASP Top 10 ↔ Cheat Sheets index: [https://cheatsheetseries.owasp.org/IndexTopTen.html](https://cheatsheetseries.owasp.org/IndexTopTen.html)

### 4) Software Supply Chain & Reproducibility

* pip — Secure installs (`--require-hashes`, `--only-binary`): [https://pip.pypa.io/en/stable/topics/secure-installs/](https://pip.pypa.io/en/stable/topics/secure-installs/)
* pip-tools — `pip-compile`: [https://pip-tools.readthedocs.io/en/latest/cli/pip-compile/](https://pip-tools.readthedocs.io/en/latest/cli/pip-compile/)
* CycloneDX Python SBOM Tool: [https://cyclonedx-bom-tool.readthedocs.io/](https://cyclonedx-bom-tool.readthedocs.io/)
* Dependency-Track (SBOM/SCA): [https://docs.dependencytrack.org/](https://docs.dependencytrack.org/)

### 5) Repo & Documentation Hygiene (GitHub/Markdown)

* GitHub — Community health files overview: [https://docs.github.com/en/communities/setting-up-your-project-for-healthy-contributions/about-community-profiles-for-public-repositories](https://docs.github.com/en/communities/setting-up-your-project-for-healthy-contributions/about-community-profiles-for-public-repositories)
* Default community health files (`.github` repo): [https://docs.github.com/en/communities/setting-up-your-project-for-healthy-contributions/creating-a-default-community-health-file](https://docs.github.com/en/communities/setting-up-your-project-for-healthy-contributions/creating-a-default-community-health-file)
* Organization-wide health files (GitHub changelog): [https://github.blog/changelog/2019-02-21-organization-wide-community-health-files/](https://github.blog/changelog/2019-02-21-organization-wide-community-health-files/)
* Relative links in Markdown (GitHub blog): [https://github.blog/news-insights/product-news/relative-links-in-markup-files/](https://github.blog/news-insights/product-news/relative-links-in-markup-files/)
* CommonMark spec (current) — link reference definitions: [https://spec.commonmark.org/current/](https://spec.commonmark.org/current/)

### 6) In-repo Find My Device Network protocol reference

* [`docs/FMDN.md`](docs/FMDN.md) — canonical reference detailing cryptography, provisioning flows, BLE behavior, and failure modes underpinning modules such as [`custom_components/googlefindmy/api.py`](custom_components/googlefindmy/api.py), [`custom_components/googlefindmy/coordinator.py`](custom_components/googlefindmy/coordinator.py), and the BLE parsers in [`custom_components/googlefindmy/ProtoDecoders/`](custom_components/googlefindmy/ProtoDecoders/).

See also: [BOOKMARKS.md](custom_components/googlefindmy/BOOKMARKS.md) for additional, curated reference URLs.

## High-confidence & sourcing standard for Codex

This section originates from the maintainer request and governs every answer, patch, refactor, review, commit, diff proposal, and architectural recommendation produced by the AI (“Codex”) inside this repository. It supplements existing quality requirements (for example, mypy strict for touched files, Home Assistant best practices) and is mandatory.

### 0. Scope

The rules apply to the entire repository and every interaction where Codex proposes or reviews code.

### 1. High-confidence mode (≥ 90 %)

*Codex must only deliver concrete code when it is at least 90 % confident that:*

1. the change is syntactically valid;
2. the change matches the project’s architecture, patterns, and compatibility expectations;
3. the change respects all known constraints (Home Assistant Core guidelines, existing conventions, runtime behavior).

If confidence drops below 90 %:

* do **not** change code;
* do **not** invent new function signatures, imports, or APIs;
* respond with “I do not have reliable information for that—I do not know.”;
* immediately request the missing evidence from the user (see §4 “User as data source”).

Confidence is < 90 % whenever, for example:

* the relevant API version is unclear;
* a breaking change is suspected but cannot be demonstrated;
* project conventions (exception classes, logging patterns, helper utilities) are unknown;
* backward compatibility requirements are uncertain.

### 2. Mandatory evidence

Every recommendation—code, architecture, migration, best practice—must reference a verifiable source inside the project reality.

*Verifiable* means a reader of this repository can open it. A path inside an agent's private memory or
home configuration (`memory/<scope>/<file>.md`, `~/.claude/<file>`) is not a source, however real the
underlying measurement was: commit a redacted artefact under `docs/` and cite that instead. Enforced by
`tests/test_guard_prose_contract.py` over Python prose, Markdown and workflow files.

Acceptable sources include:

* existing repository code (including tests);
* previously established project standards (documented in `AGENTS.md`, `CONTRIBUTING.md`, docstrings, comments);
* explicit, user-confirmed instructions (for example, “Use `homeassistant.helpers.network.get_url(self.hass)` instead of the deprecated `async_get_url()`”);
* official upstream interfaces (Home Assistant helpers, `DataUpdateCoordinator`, config flow contracts) **when** Codex has direct access to them.

When the evidence lives inside this repository, cite the exact file and symbol. If the evidence only exists upstream (Home Assistant developer docs, changelog, breaking changes) and is not available here, name it explicitly and refrain from modifying code until the user supplies the relevant excerpt (see §4).

Assertions without sources imply confidence < 90 % → no code changes.

### 3. Workflow for code changes

Before proposing a change Codex must:

1. **Analyze the problem.** Summarize what must change (bug fix, refactor, API migration, new test) and highlight the affected files or sections.
2. **Compare with project standards.** Identify existing patterns to reuse and ensure no established rule is violated (for example, deprecated helpers, translation handling, config flow ordering).
3. **Propose only with ≥ 90 % confidence.** Provide a full, executable patch backed by the cited evidence.
4. **Verify consistency.** Explain how the patch remains backward compatible and why it should pass mypy strict for the modified files.

#### Home Assistant regression helper

* Use `make test-ha` to provision the `.venv` environment, install `custom_components/googlefindmy/requirements-dev.txt` (including `homeassistant` and `pytest-homeassistant-custom-component`), and execute the regression suite in one step.
* Append custom pytest flags with `make test-ha PYTEST_ARGS="…"` when you need markers, selection filters, or verbosity tweaks without editing the recipe.
* The `README.md` section ["Running Home Assistant integration tests locally"](#running-home-assistant-integration-tests-locally) mirrors this workflow so external contributors can follow the same command.

#### Config flow registration fallbacks

* Prefer the Home Assistant config flow metaclass to register flows. Manual assignments to `HANDLERS` are legacy fallbacks that should only be reintroduced when Home Assistant removes the metaclass hook or when a regression in upstream releases prevents automatic registration.
* If a fallback becomes necessary, document the affected core version, link to the upstream issue or breaking change notice, and mark the block with a TODO referencing the removal criteria so the workaround is pruned once the regression is resolved.
* When strict typing requires forwarding metaclass keywords (for example, `domain=DOMAIN`), reuse the `_DomainAwareConfigFlow` helper in `custom_components/googlefindmy/config_flow.py`—or introduce an analogous keyword-aware shim in the same module—so `mypy --strict` accepts the keyword without reintroducing manual `HANDLERS` fallbacks.

### 4. User as data source

When external information is missing (for example, an up-to-date Home Assistant helper signature, a breaking change notice, an upstream commit), Codex must:

1. Explicitly request the missing snippet from the user (“Please provide the current definition of `homeassistant.helpers.network.get_url`”, etc.).
2. Treat user-supplied excerpts as valid sources once delivered.
3. Resume high-confidence mode only after receiving the evidence.
4. If the user cannot provide the evidence, stay below 90 % confidence and avoid speculative code.

### 5. Communicate uncertainty

State uncertainty early and precisely: “I am <90 % confident because I lack the signature of `X` in your target Home Assistant version. I do not have reliable information—please provide the definition of `X`.” Do **not** rely on guesses or invented APIs.

### 6. Consequences for commits, diffs, and reviews

*With ≥ 90 % confidence:* Codex may deliver complete diffs, refactors, tests, migrations—always citing the supporting evidence.

*With < 90 % confidence:* Codex must not supply diffs. Instead, list the open information gaps and label any hypotheses as unverified. Do not commit or rely on speculative changes.

### 7. Review obligations

All critiques of existing code must also cite evidence. If a concern depends on an unverified deprecation or breaking change, immediately request the source instead of rewriting code blindly.

### 8. Required workflow summary

1. **Confidence check.** Confirm ≥ 90 % certainty for syntax, semantics, compatibility. If not, state the lack of information and request sources.
2. **Cite evidence.** Every change references a concrete repository location or confirmed directive.
3. **Deliver patch.** Provide a complete, runnable patch that follows all conventions (type hints, English docstrings, no deprecated APIs, mypy strict readiness).
4. **Post-verification.** Tell the user which commands to run locally (for example, `pytest -q`, `mypy --strict`, `ruff check`).

### 9. Rationale

The objective is reliability and auditability: every change must document its source, justification, and any unresolved assumptions. This protects code quality, compatibility, and long-term maintainability.

### 10. Post-task feedback obligations

After completing each task, Codex must finish with a brief, constructive note proposing improvements to this environment—preferably actionable suggestions for refining `AGENTS.md` or adjacent tooling—based on the experience gained during the most recent assignment.

## X. PROTOCOL: Runtime API Validation (Single Source of Truth - SSoT)

**Core Principle:** AI training data is inherently outdated. Test stubs (e.g., in `conftest.py` or `tests/helpers/`) may be incorrect or obsolete. The only immutable "Single Source of Truth" (SSoT) for API contracts is the **runtime behavior of the packages installed in the `.venv`** (e.g., `homeassistant`, `pytest-homeassistant-custom-component`).

**Mandate:** Runtime errors encountered during validation (`TypeError`, `AttributeError`, `ImportError`, `NotImplementedError`) that contradict the AI's internal model *must* be treated as a failure of the AI's model, not a failure of the code or environment.

**MANDATORY SSoT VALIDATION WORKFLOW:**

If a patch fails validation with an error that implies a mismatch in API contracts (e.g., `TypeError: unexpected keyword argument 'config_subentry_id'`, `TypeError: 'Platform' object is not iterable`):

1.  **FORBIDDEN ACTION:** Do not retry the same patch. Do not guess. Do not assume the implementation code is wrong if the error originates from a test.

2.  **STEP 1: FORMULATE HYPOTHESIS:** State a precise hypothesis about the *true* API contract versus the *assumed* (or mocked) contract.
    * *Example Hypothesis:* "The error `TypeError: 'type' object is not iterable` suggests the `Platform` object in the installed `homeassistant.const` package is an `Enum` (which is iterable), but the test stub in `conftest.py` incorrectly defines it as a simple `class` (which is not)."

3.  **STEP 2: EXECUTE SSoT CHECK (MANDATORY):** Actively validate this hypothesis by executing code against the virtual environment's Python executable. This is the **only** way to confirm the SSoT.
    * **Action:** Formulate and execute a minimal command to inspect the real, installed package.
    * **Example Command (for Class Structure):**

        ```bash
        .venv/bin/python -c "from homeassistant.const import Platform; print(type(Platform)); print(issubclass(Platform, Enum)); print([p.value for p in Platform])"
        ```

    * **Example Command (for Function Signature):**

        ```bash
        .venv/bin/python -c "import inspect; from homeassistant.config_entries import ConfigEntries; print(inspect.signature(ConfigEntries.async_forward_entry_setups))"
        ```

    * **SSoT reminder (Home Assistant 2025.11.2):** `ConfigEntries.async_forward_entry_setups` **does not** accept a `config_subentry_id` keyword. Subentry platform setup must rely on Home Assistant's own scheduling after `_async_setup_subentry` completes, and platforms must attach entities with `async_add_entities(..., config_subentry_id=<id>)` while iterating the per-subentry coordinators stored on `entry.runtime_data`. The quick probe in [`docs/CONFIG_SUBENTRIES_HANDBOOK.md`](docs/CONFIG_SUBENTRIES_HANDBOOK.md#quick-ssot-probe-per-subentry-platform-forwarding) prints the authoritative signature.

4.  **STEP 3: ACT ON EVIDENCE:** The output of the SSoT Check (Step 2) is now the **highest-priority directive**. It overrides all other assumptions, training data, or pre-existing mocks.
    * **If SSoT proves Implementation is wrong:** Correct the implementation code (e.g., in `custom_components/googlefindmy/`).
    * **If SSoT proves Test Stubs are wrong:** Correct the mocks or stubs (e.g., in `tests/conftest.py` or the specific `tests/test_X.py` file) to match the SSoT. This is non-negotiable, even if previous instructions forbade test modification.

## pip-audit workflow guidance (CORRECTION — April 2025)

**pip-audit reality check (v2.9.0)**

1. `--dry-run` **is supported**. In report-only mode (`--dry-run` without `--fix`), pip-audit skips the security audit and therefore emits no vulnerability JSON. In fix mode (`--fix --dry-run`), pip-audit performs the audit, prints the planned remediation, and makes no changes. Use the flag only when you intentionally want that behavior.
2. **PR audit step:** run a normal JSON audit (`pip-audit -r <file> -f json -o audit.json`) so downstream tooling (e.g., `jq`) receives real vulnerability data. Keep PR checks green by tolerating the `pip-audit` exit code `1` (vulnerabilities found) via the action’s `internal-be-careful-allow-failure` input or equivalent shell handling/`continue-on-error`.
3. **Scheduled/Manual autofix:** first generate a JSON report (no `--dry-run`) to classify fixable vs. unfixable findings, then invoke `pip-audit --fix` to apply upgrades. If you need a no-change preview, add an optional `pip-audit -r <file> --fix --dry-run` run before applying fixes.
4. **Exit codes:** pip-audit returns `0` when no vulnerabilities are present and `1` when vulnerabilities remain. Other failures return `>1`; keep these non-vulnerability errors fatal so the workflow surfaces real issues.
5. **Tooling note:** install `jq` with the runner package manager (for example, `sudo apt-get install -y jq`) so the CLI is available in the shell environment.

Sources: [pip-audit · PyPI](https://pypi.org/project/pip-audit/), [gh-action-pip-audit · GitHub](https://github.com/pypa/gh-action-pip-audit).

### 7) Type checking (mypy)

* mypy — Command line reference: [https://mypy.readthedocs.io/en/stable/command_line.html](https://mypy.readthedocs.io/en/stable/command_line.html)
* mypy — Configuration reference: [https://mypy.readthedocs.io/en/stable/config_file.html](https://mypy.readthedocs.io/en/stable/config_file.html)
* mypy — Getting started & strict mode: [https://mypy.readthedocs.io/en/stable/getting_started.html](https://mypy.readthedocs.io/en/stable/getting_started.html)

#### Auto-installed stub packages

Running `mypy --install-types --strict` against the integration will prompt
pip to fetch a small set of stub distributions automatically. In the hosted
tooling environment this consistently includes:

* `types-pyOpenSSL`
* `types-cffi` (pulled in by `types-pyOpenSSL`)
* `types-setuptools` (pulled in by `types-cffi`)

Contributors working offline can preinstall these packages (alongside
`types-requests` from `custom_components/googlefindmy/requirements-dev.txt`) to
avoid repeated network downloads during strict mypy runs.

The first invocation of `mypy --strict` after creating a fresh environment may
display an interactive prompt asking to install the missing type stubs listed
above. When scripting or running in CI, pass `--install-types --non-interactive`
so the command exits cleanly without waiting for user input.
