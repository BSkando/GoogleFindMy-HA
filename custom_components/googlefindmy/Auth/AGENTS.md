# AGENTS.md — Authentication helpers (gpsoauth integration)

> **Scope:** `custom_components/googlefindmy/Auth/**`
>
> Applies to synchronous and asynchronous token retrieval helpers, cache utilities, and any additional gpsoauth wrappers added under this folder.

## gpsoauth stub expectations

* `gpsoauth.perform_oauth` requires the third positional argument (`android_id`) to be an **integer**. Helpers must resolve or convert IDs into the numeric representation (for example, `int(hex_value, 16)` for cached values stored as hexadecimal strings) before invoking the API. Regression tests (for example, `tests/test_adm_token_retrieval.py::test_async_request_token_uses_cached_android_id`) assert this behavior. When editing `gpsoauth.pyi`, update the stub signature to `perform_oauth(email: str, aas_token: str, android_id: int, *, service: str, app: str, client_sig: str) -> dict[str, Any]` so type checkers reflect the runtime and test contract.
* `gpsoauth.perform_master_login` mirrors the same positional types (`email: str`, `password: str`, `android_id: int`). Provide the integer form of the identifier directly—do **not** stringify the value. Stored `"0x"`-prefixed IDs must be normalized into their integer value ahead of time so downstream helpers stay consistent, and mirror the signature change in the stub when updating it.
* Both functions return a `dict[str, Any]` containing response keys like `"Token"`, `"Auth"`, and `"Error"`. Persist this annotation so mypy strict remains satisfied when parsing the response payload.

When the upstream stubs change, update this file and adjust the affected call sites so that future type-checking runs remain stable.

## Classifying selenium failures in the login flow

`auth_flow._describe_lost_session` decides whether a `WebDriverException` means
"the user closed the window" (a finished run, `LoginAborted`, CLI status 130) or
"the driver broke" (a defect, own type and traceback). Two rules hold it
together. The first lives in `_describe_lost_session`, the second in
`request_oauth_account_token_flow`; keep both when editing either. Everything
stated below about selenium was measured against selenium 4.40.

* **Chromedriver is not known to use the `timeout` error code to *report* a
  gone window,** and the rule is written so that being wrong about that is the
  cheap direction: a genuine cancellation would then be reported as a driver
  fault with a traceback, rather than a driver fault being reported as a
  cancellation the user never made. so a window phrase inside a `TimeoutException` message is not
  evidence of one and the type is checked before any phrase matching. Note the
  direction: a window that goes away mid-command often *causes* a timeout
  ("Timed out receiving message from renderer"), which is exactly why the text
  cannot be trusted there. Only two W3C codes map to `TimeoutException`
  (`timeout`, `script timeout`); a gone window arrives as
  `NoSuchWindowException` or `InvalidSessionIdException`. Without the type check
  running first, the navigation, the poll and the cookie read can each sell a
  stalled driver to the user as a cancellation they never made. (The e-mail
  extraction cannot: it swallows its own failures and returns `None`.) The check
  uses `isinstance`, so a future selenium subclass of `TimeoutException` is
  covered; no test pins that, because no such subclass exists today.
* **The wait's own deadline and a driver timeout raised by the predicate share
  one type,** so `request_oauth_account_token_flow` records the predicate's
  exception and compares by object identity. `WebDriverWait.until` lets a
  predicate's exception propagate unchanged and builds a *new* one for the
  deadline, which is what makes identity exact rather than a heuristic. A
  weaker test ("a failure was recorded") passes today and stops being correct
  the moment `ignored_exceptions` is widened.

## Linting reminder

Keep `TYPE_CHECKING` aliases only when the alias is referenced in the module. Remove stale aliases during cleanups so linting runs stay predictable and reviewers can confirm no runtime imports are hidden behind unused guards.

## Shared helper preference

When multiple modules need the same small utility (for example, `_mask_email_for_logs`), define it once in a shared helper module and import it at module scope rather than re-importing inside functions. Centralizing helpers avoids circular-import traps and prevents Ruff from flagging inline imports.

### FCM activity health helper

The FCM supervisor uses a shared activity-health helper (freshness window + snapshot structure) to decide when a client is healthy, stale, or needs a restart. Reuse the existing helper and timestamp fields (`last_activity_monotonic`, freshness window constants, snapshot shape) rather than introducing new window values or parallel health calculations so diagnostics and coordinator state remain consistent across entries.

## Token lifecycle & standalone persistence

### Token hierarchy

The integration uses a three-tier token chain derived from Google Play Services authentication:

```
Chrome Cookie (oauth2_4/***, single-use, consumed after first exchange)
  → gpsoauth.exchange_token()
  → AAS / Master Token (aas_et/***, quasi-permanent)
    → gpsoauth.perform_oauth(service=...) — repeatable indefinitely
    → ADM / Scoped Token (ya29.***, ~1 hour TTL)
      → Nova API requests
```

* **Chrome OAuth cookie:** Obtained once via `accounts.google.com/EmbeddedSetup` (see `auth_flow.py`). It is **single-use** — after `exchange_token()` consumes it, the cookie is worthless. Never overwrite `CONF_OAUTH_TOKEN` with an AAS token; `adm_token_retrieval.py:450-454` guards the OAuth-fallback path with `not startswith("aas_et/")`.
* **AAS master token:** Long-lived. Only invalidated by password change, manual revocation in Google account settings, or Google security heuristics. Can generate unlimited scoped ADM tokens via `perform_oauth()`.
* **ADM scoped token:** Short-lived (~1 hour, measured adaptively by `TTLPolicy` in `nova_request.py`). Regenerated automatically from the AAS token on expiry.

### Architecture note: OAuth fallback lives in the ADM layer

When `gpsoauth.perform_oauth()` rejects the AAS token (`InvalidAasTokenError`), the retry logic in `adm_token_retrieval.py:449-482` attempts an **OAuth fallback**: it reads `CONF_OAUTH_TOKEN`, switches `DATA_AUTH_METHOD` to `"individual_tokens"`, and retries — which causes the next iteration to call `async_get_aas_token()` → `_generate_aas_token()` → `gpsoauth.exchange_token(oauth_cookie)` to produce a fresh AAS token from the Chrome cookie.

This is architecturally an **AAS regeneration** triggered from the ADM layer. It works for HA where the Chrome cookie may still be fresh (e.g., during initial config validation), but is ineffective in standalone mode where the cookie is already consumed. A future refactor could move this fallback into `aas_token_retrieval.py` where it logically belongs; for now it is documented here so contributors understand the cross-layer dependency.

### HA vs standalone persistence

| Layer | HA mode | Standalone CLI (`main.py`) |
|-------|---------|----------------------------|
| **Volatile** | `TokenCache` (in-memory dict) | `_FileCache._data` (in-memory dict) |
| **Persistent** | `entry.data` (HA database) — survives `cache.set(key, None)` and is re-seeded on every restart (credential seed in `__init__.async_setup_entry`) | `secrets.json` — **only store**; a naive `set(key, None)` would permanently delete the value |

The seed runs in one direction only, and deliberately so: `entry.data` is the
source of truth, the cache is its mirror. A credential key the entry *lacks* is
therefore not recovered from the cache but dropped from it, because credentials
that replace older ones express "this account has no bundle / no AAS token any
more" by leaving the key out (`const.OPTIONAL_CREDENTIAL_KEYS`, written by
`config_flow._merge_credential_updates`). The one exception is an entry that
carries no credentials at all: that is the migration gap the seed exists for, and
there the cache is read back in full, with one exception. A bundle field the
runtime renews in place (`_RUNTIME_REFRESHED_BUNDLE_FIELDS` in `__init__.py`,
today only `fcm_credentials`) keeps its non-empty top-level value in the cache
instead of being replaced by the copy inside the cached bundle. Reason: that copy
predates every renewal since the bundle was stored, and the receiver's entry
write never reaches an entry without a bundle, so the older registration would
come back on every setup. An empty top-level value is still filled from the
bundle, so a cold start keeps its registration (Issue #152). Enforced by
`tests/test_credential_seed_cache_branch.py`.

The legacy migration in `async_setup_entry` relies on that exception: when it
moves the bundle into the cache, it removes **every** credential key from the
entry (`DATA_SECRET_BUNDLE`, `scanned_data`, `CONF_OAUTH_TOKEN` and
`DATA_AAS_TOKEN`), so the seed that runs right after it sees an entry without
credentials and reads the cache back. Leaving one of them behind turns the
migrated entry into "the user replaced the bundle", and the seed would drop the
bundle the migration just stored, together with the key material derived from
it. Enforced by `tests/test_legacy_migration_aas_token.py` (behaviour and an AST
guard over the migration's `pop` calls).

### Credential ownership across receiver, cache and entry

* **One place for the FCM registration in the entry.** The registration lives
  in the secrets bundle (`entry.data["secrets_data"]["fcm_credentials"]`) and
  in the token cache, never as a top-level key of `entry.data`. Earlier config
  flows stored a second copy there; nothing read it, it went stale after every
  renewal, and it kept one more exemplar of the device credential in the
  entry. No flow writes it any more, and a setup-time repair
  (`_async_strip_entry_fcm_credentials_copy` in `__init__.py`, no version
  bump, run before the legacy migration) removes a copy an older version
  stored.
  Enforced by `tests/test_fcm_credentials_entry_copy.py`, including an AST
  guard that lets `config_flow.py` name the key only to read it.

* **Detached copies.** A credential value that crosses between the FCM
  receiver, the `TokenCache` and `entry.data` is handed over as a deep copy,
  and `TokenCache` stores and returns a copy of every value that is not an
  immutable scalar, in `all()` and the module-level sync facades as well.
  Reason: the receiver edits its registration and the nested `gcm` block in
  place (`_invalidate_fcm_tokens`); a shared container would rewrite
  `entry.data` without `async_update_entry`, so neither the store nor the
  update listener would see the change. Enforced by
  `tests/test_guard_credential_entry_writes.py` (every `fcm_credentials` write
  in `fcm_receiver_ha.py` is a `copy.deepcopy` written once, and every other
  read of it only hashes it or goes through the token cache),
  `tests/test_fcm_registration_entry_sync.py` and
  `tests/test_token_cache_value_isolation.py`.
* **The entry is the durable store for a renewed registration.** The receiver
  writes only the member it owns, `fcm_credentials` in the secrets bundle,
  after leaving a
  self-write receipt (`entry_self_write`, see
  `agents/runtime_patterns/AGENTS.md`, "One owner per entry reload"). When the
  bundle holds a registration the receiver neither registered nor wrote
  itself, it leaves the entry as it is; an entry that had no bundle when the
  receiver registered counts as holding no registration. Reason: that value
  comes from a user change, usually an imported `secrets.json`, whose reload
  is already on its way; overwriting it would undo the import. An
  invalidation is written to the entry as well, before the cache. Reason: an
  entry with a bundle re-seeds the cache from it on every setup, so a
  cache-only invalidation would bring the stale tokens back on the next
  start. A failing entry write is logged as a warning and never stops the
  cache write. Enforced by `tests/test_fcm_entry_self_write_receipt.py` and
  `tests/test_fcm_registration_entry_sync.py`.

### Soft invalidation (standalone)

To bridge the architectural gap, `_FileCache` applies **soft invalidation** for the `aas_token` key (see `_SOFT_INVALIDATE_KEYS`). When `cache.set("aas_token", None)` is called during a runtime 401 recovery:

1. The key is added to `_soft_invalidated` (in-memory set).
2. `get("aas_token")` returns `None` for the remainder of the process — the runtime sees the token as invalidated and can attempt regeneration or fail fast.
3. `_save()` is **not** called — `secrets.json` still contains the AAS token.
4. On the next process start, `_soft_invalidated` is empty, so `get("aas_token")` returns the value from the file, effectively recovering the token.

This mirrors HA's two-layer model where `entry.data` preserves the AAS token even after the volatile `TokenCache` is cleared.

### When re-authentication is required

If Google genuinely revokes the AAS master token (password change, security event), no cached value can recover it. The standalone CLI logs an explicit error:

```
AAS token exchange failed with a likely-expired OAuth cookie.
The Chrome OAuth cookie is single-use and cannot be reused.
Please re-authenticate: python -m custom_components.googlefindmy --reauth
```

In HA mode, the equivalent path raises `ConfigEntryAuthFailed`, which triggers the reconfiguration UI.

## Cache key conventions

Android IDs and related identifiers stored in `TokenCache` must follow predictable, per-user keys so helpers avoid collisions between accounts:

* **AAS/FCM Android IDs:** `android_id_<username>` — always the normalized username string (entry-scoped) as the suffix.
* **FCM credential bundle:** `fcm_credentials` — contains the `gcm.android_id` value that downstream helpers normalize and cache under the key above.
* **AAS token:** `aas_token` (`DATA_AAS_TOKEN`) — the master token. In standalone mode this key is **soft-invalidated** (hidden in memory but preserved in `secrets.json`). Do not add it to hard-delete flows.
* **OAuth token:** `oauth_token` (`CONF_OAUTH_TOKEN`) — the original Chrome cookie. Must remain as-is after initial storage; never overwrite with an AAS token value.
* **Shared key:** `shared_key` — the 32-byte vault-backed key used to decrypt the owner key. In HA mode, pre-populated from the secrets bundle as `shared_key_<email>` and migrated to `shared_key` on first access. In standalone CLI mode, obtained via the interactive browser flow on first run.
* **Owner key:** `owner_key_<username>` — the decrypted owner key (hex string). Cleared during `--reauth` and during blind-refresh retries when EIK decryption fails.

When adding new cache-backed helpers under this directory, reuse these patterns (prefix + username suffix) so multi-account setups remain isolated and future helpers can retrieve existing values without guessing.

## Cookie handling

When reading cookies from external authentication flows (for example, Selenium-managed sessions), always validate both the presence and the expected type of each field before use. In particular, confirm that the `"value"` entry resolves to a `str` and raise a descriptive exception if validation fails so helpers consuming the data can rely on strict return contracts.

## Logging guardrails

* Do not attach a traceback in this package: no `exc_info=` value other than `False` or `None` (a name, `True`, an alias bound outside the handler, a tuple, `sys.exc_info()`) and no `logger.exception(...)`. The last rendered line of a traceback is `str(err)`, which for `gpsoauth`, `requests` or `aiohttp` may echo a token or a response body, and an exception this package raises `from` a foreign one renders the chained cause as well. Log `describe_exception(err)` and, for the location, `exception_origin(err)` instead.
* When referencing account identifiers in logs, always mask them via `_mask_email_for_logs` (available from `aas_token_retrieval`) instead of embedding raw usernames or email addresses.

### Preferred logger pattern

Use structured extras plus `describe_exception` to keep tokens and raw error text out of records:

```python
_LOGGER.debug(
    "Token probe failed; mapped error key (%s at %s).",
    describe_exception(err),
    exception_origin(err),
    extra={
        "token_source": _probe_source_label(source),
        "error_key": key,
    },
)
```

**Quick template for reviewers/authors:**

```python
_LOGGER.info(
    "<short summary without secrets>: %s at %s",
    describe_exception(err),  # type plus error_kind, errno or withheld length
    exception_origin(err),  # innermost frame, no text; omit when the location adds nothing
    extra={
        "user": _mask_email_for_logs(username),
        "context_key": context_value,
    },
)
```

Keep sensitive strings (tokens, response bodies, raw exception text) out of the
message itself and prefer short context keys in `extra` so log processing stays
consistent and Semgrep does not flag credential leaks.

The first template carries no account address on purpose: in the token probe of
`config_flow.py` the address can be read from the secrets bundle, so CodeQL reported
it even masked, and Home Assistant's log format does not print `extra` fields,
so the address never reached `home-assistant.log` or a downloaded debug log.
Add an account to `extra` only where a reader needs it, and never from a value
read through the bundle (root `AGENTS.md`, "Log hygiene for scanners").

The classified gpsoauth error code (`classify_gpsoauth_error`) stays out of
`extra` in `_exchange_oauth_for_aas` for the same reason: the kind is already on
the raised error (the `error_kind` attribute and `kind=` in its message), and
`async_get_aas_token` logs it from there via `describe_exception`. CodeQL
reported the copy in `extra` as sensitive data, although Home Assistant's log
format does not print it. The literal kinds `auth_error` and `exchange_error` in
the other two warnings of that function stay in `extra`; CodeQL reports no flow
for them.

Inside an `except` handler whose types are not all defined in this package
(`except Exception as exc`, `except (OSError, ssl.SSLError) as err`, ...), pass
the exception through `Auth.log_safety.describe_exception(exc)` instead of
`exc`, `str(exc)` or `_clip(exc)`: it prints the type plus `error_kind` or
`errno` when present, the message of exceptions raised by this package, the bare
type name for an empty message, `(unprintable)` when `str()`, a metadata
property (`error_kind`, `errno`) or the class's own name or module itself
fails, whatever it raises (`<unnamed>` when the class name is not a plain
`str`), and otherwise the withheld character count. A class whose
`__module__` is not an exact `str` counts as foreign rather than as this
package's own, so its message stays withheld. `exception_origin(exc)` names the innermost frame when a location
is needed. The same applies to a parameter annotated with such a type
(`def _classify(entry_id: str, err: BaseException)`): the callee logs an
exception it did not catch, and the name is bound for the whole function;
so is a name assigned from `task.exception()` in a done callback, and so is
a name the same function derives from one of those in the forms the guard
follows (an alias, attribute or subscript target, `detail = err`,
`self.last = err`; its text or a derivation of it, `shown = str(err)`,
`msg += str(err)`, `"x %s" % err`, `str(err) or ""`, `str(err).lower()`,
`err.strerror`, `getattr(err, "msg")`, a container display, element or
built-in copy such as `list(errors)`, a container mutated by `append`; a
tuple or starred unpacking by position, or as a whole from `err.args`; a
`for` target whose iterable mentions the name, `for i, err in
enumerate(errors)`; a `match` capture such as `case ClientError() as err`
or `case ClientError(args=[first])`). It makes no difference whether the
annotation names the type outright or reaches it
`through a local type alias defined in the same module`
(`type X = ClientError`, `X: TypeAlias = ClientError`, or a plain
`X = ClientError`): the alias name carries no information about the type, so
the guard resolves it before it classifies. Inside a handler the same holds for
the handler name (`except OSError as exc: text = str(exc)` binds `text`
there). Which further forms the guard follows and which it does not is
listed in one place only, the module docstring of
`tests/test_guard_logging_payloads.py`; this file does not repeat that
list. A form the docstring names as not followed remains a review matter,
not a guard one.
`tests/test_guard_logging_payloads.py` (shape (I)) fails the suite
on a bare exception in such a record under `Auth/`, on an own exception
constructed from one (`raise OwnError(str(err)) from err`: its message is
logged one hop later), and on any `exc_info=`
value other than `False`/`None` or any `logger.exception(...)` under `Auth/`,
whatever the handler. A record whose value only looks like exception text
(the key of a `KeyError` raised by a literal lookup on the package's own
dict, `fcm_refresh_install_token`) is pinned in the guard's `_REVIEWED`
set with its reason; a pin names one call by path, leaf and format-string
prefix and fails the suite when the call is copied or disappears.
