<!--
This is the operational checklist for PRs in this repository.
It implements the spirit of AGENTS.md without blocking urgent fixes.

Notes for AI/automation:
- You MAY pre-check boxes you have satisfied in this PR.
- Only mark items you have actually performed or verified.
-->

<!-- ================================================================
     PR TITLE FORMAT (Required)
     ================================================================
     Your PR title MUST follow Conventional Commits format:

       <type>(<scope>): <subject>

     Examples:
       feat(auth): add OAuth2 refresh token support
       fix(coordinator): resolve race condition in polling
       docs: update installation instructions
       ci: add bandit security scanning
       refactor(config-flow): simplify error handling

     Types:
       feat     - New feature (triggers minor version bump)
       fix      - Bug fix (triggers patch version bump)
       docs     - Documentation only changes
       style    - Formatting, missing semicolons, etc
       refactor - Code change that neither fixes a bug nor adds a feature
       perf     - Performance improvement
       test     - Adding or correcting tests
       build    - Changes to build system or dependencies
       ci       - Changes to CI configuration
       chore    - Other changes that don't modify src or test files
       revert   - Reverts a previous commit

     Scopes (optional):
       auth, api, coordinator, config-flow, entities, services,
       diagnostics, discovery, fmdn, spot, nova, translations, ci, release

     Subject rules:
       - Use imperative mood ("add" not "added", "fix" not "fixed")
       - Don't capitalize first letter
       - No period at the end
     ================================================================ -->

# Summary
<!-- 1–3 sentences: What changed and why it matters for users/reviewers. -->

- Type: ☐ fix ☐ feat ☐ refactor ☐ docs ☐ chore ☐ ci ☐ test ☐ perf
- Scope/Area: …
- Linked issues: Fixes #…

## Motivation
<!-- Why are we doing this? Problem statement, user impact, context. Keep it crisp. -->

---

## Change Log (human-readable)
<!-- Keep this high-signal, mirroring your commit intent. -->

### Added
<!-- New behavior, services, entities, helpers, flags. -->
- …

### Changed
<!-- Modifications to existing behavior; migrations; clarified guarantees. -->
- …

### Fixed
<!-- Bugs/addressed regressions; include 1–2 bullets per root cause if known. -->
- …

### Removed
<!-- Deleted code/paths, legacy options; note if replacement exists. -->
- …

### Performance
<!-- Notable latency/CPU/memory reductions; micro-optimizations with rationale. -->
- …

### Security/Privacy
<!-- Redaction hardening, token handling, diagnostics safety, etc. -->
- …

### Compatibility
<!-- User-facing compatibility: breaking changes (expected none), migrations, deprecations. -->
- …

---

## Evidence (optional but helpful)
<!-- Logs, screenshots, sample diagnostics (redacted), before/after metrics, etc. -->
<details>
<summary>Expand for screenshots/logs</summary>

</details>

---

## Tests (MUST)
- ☐ `pytest -q` passes locally
- ☐ New/updated **unit/integration tests** cover the change
- ☐ For **bugfixes**: a **minimal regression test** is included that fails without the fix and passes with it
- Coverage targets:
  - ☐ `config_flow.py` remains **100%**
  - ☐ Total ≥ **95%**
    ☐ Temporary dip due to necessary code removal — **follow-up issue** opened: #___

### Expected areas (check all that apply)
- ☐ **Config flow**: user/invalid, duplicate abort (`async_set_unique_id` + `_abort_if_unique_id_configured`), connectivity pre-check, **reauth** (success/failure → reload), **reconfigure**
- ☐ **Lifecycle**: `async_setup_entry`, **reload**, `async_unload_entry` (no zombie listeners)
- ☐ **Coordinator & availability**: `UpdateFailed` on transient errors; entities flip to `unavailable`; single "down/back" log
- ☐ **Diagnostics**: strictly redacted (no tokens/emails/locations/IDs)
- ☐ **Services**: success/error paths with localized messages; rate limiting where applicable
- ☐ **Discovery & dynamic devices** (if supported)
- ☐ **Token cache**: expiry detection, refresh, failure propagation; **no hidden fallbacks**

---

## Token Cache Safety (MUST)
- ☐ Single **entry-scoped** `TokenCache` (no globals)
- ☐ `TokenCache` is **passed explicitly** through call chains (`__init__` → API/clients → coordinator → entities)
- ☐ Refresh is **synchronous** on the calling path or fails fast with a translatable error
- ☐ On refresh failure: raises `ConfigEntryAuthFailed`
- ☐ Regression tests for stale tokens / cross-account bleed / refresh races

---

## Docs & i18n
- ☐ No hard-coded UI strings in Python
- ☐ `translations/*.json` updated (services & exceptions via `translation_key`)
- ☐ README updated (only if user-visible behavior/options changed)
- ☐ Missing files are **non-blocking**; propose stub/follow-up if needed

---

## Quality Scale (lightweight, non-blocking)
- Touched rule IDs & evidence (file+line / test name / PR link):
  - e.g. `strict-typing`: attach mypy log or CI link
  - e.g. `diagnostics`: `tests/test_diagnostics.py::test_redaction`

---

## Deprecation Check (concise)
- Versions scanned: current HA ± 2 releases
- Notes found (links): …
- Impact here: …

---

## Risk & Rollback
- Risk level: ☐ low ☐ medium ☐ high
- Rollback plan: how to revert safely if needed (and whether data/entity IDs remain consistent)

---

## Local Verification (run before pushing)
```bash
pre-commit run --all-files
python3 -m script.hassfest
pytest -q
poetry run bandit -r custom_components script -c pyproject.toml
```

---

## Reviewer Notes (optional)

* Edge cases to double-check:
* Follow-up tasks (if any):
